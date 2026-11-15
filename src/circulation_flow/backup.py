"""Local multi-generation backup store for circulation SQLite deliverables.

The store keeps a persistent catalog (``catalog.sqlite``) plus immutable
content-addressed database objects (``objects/<sha256>.db``) and offers three
operations, both as a Python API and as ``python3 -m circulation_flow.backup``
subcommands:

  backup(store, source, request_id)   snapshot one SQLite database into the store
  restore(store, generation, target)  materialize one committed generation
  prune(store, keep)                  drop all but the newest ``keep`` generations

Design contract (full operator documentation in docs/backup.md):

* A backup request is idempotent on ``request_id``: a committed generation is
  returned as-is and the source is never re-opened, so retrying a request
  after the source changed still yields the original generation.
* The snapshot is taken with the SQLite online backup API, which copies the
  committed state of the source *including* transactions that only live in
  its WAL; restore is a pure byte copy and never recomputes business data.
* Commit order is: object file written and fsynced -> object renamed into
  ``objects/`` and directory fsynced -> catalog row committed in one
  transaction (synchronous=FULL). The catalog row is the commit point: after
  a SIGKILL a restart sees either the full generation (row present, object
  guaranteed present) or garbage (unreferenced objects / staging files) that
  only this store's own sweep patterns remove. Restore never observes a
  half-published generation.
* prune keeps the newest ``keep`` committed generations but skips any
  generation currently protected by a restore hold. Holds are ``flock`` locks
  on per-generation files, so a crashed restore releases protection
  automatically (the kernel releases the lock) with no timeout guessing.
  Long restores never take the publish lock or a catalog write lock, so
  backups and prune of other generations complete concurrently.
* restore verifies size + SHA-256 and runs ``PRAGMA integrity_check`` on a
  staging file next to the target before atomically replacing it; the target
  must be offline and must not live inside the store. Any failure before the
  atomic replace keeps the old target bytes untouched.

Fault-injection test hooks (inert unless the environment variables are set):
``CIRCULATION_BACKUP_KILL_AT`` names a comma-separated list of commit-boundary
points where the process kills itself with SIGKILL, and
``CIRCULATION_BACKUP_RV_DIR`` names a rendezvous directory used to pause at
``after_snapshot``/``after_hold`` until a ``<point>.go`` file appears.
"""
import argparse
import contextlib
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import tempfile
import time
import uuid

STORE_FORMAT = '1'
CATALOG_NAME = 'catalog.sqlite'
OBJECT_SUFFIX = '.db'
_TMP_PREFIX = 'obj-'
_TMP_SUFFIX = '.part'
_STAGE_SUFFIX = '.circbak'
_CHUNK = 1024 * 1024
_BUSY_MS = 30000
_OBJECT_RE = re.compile(r'[0-9a-f]{64}\.db\Z')
_REQUEST_ID_MAX = 200


class BackupError(ValueError):
    """Every backup-store failure: bad arguments, store corruption, unavailable generation."""


# ---------------------------------------------------------------------------
# Fault injection for crash/concurrency tests (no-op unless env vars are set)
# ---------------------------------------------------------------------------

def _fault_point(point):
    kill = os.environ.get('CIRCULATION_BACKUP_KILL_AT')
    if kill and point in kill.split(','):
        os.kill(os.getpid(), signal.SIGKILL)
    rv = os.environ.get('CIRCULATION_BACKUP_RV_DIR')
    if rv and point in ('after_snapshot', 'after_hold'):
        Path(rv, point + '.ready').touch()
        go = Path(rv, point + '.go')
        deadline = time.monotonic() + 60
        while not go.exists():
            if time.monotonic() > deadline:
                raise BackupError('test rendezvous timed out at %s' % point)
            time.sleep(0.02)


# ---------------------------------------------------------------------------
# Small filesystem / locking helpers
# ---------------------------------------------------------------------------

def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _try_lock(fd):
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False


@contextlib.contextmanager
def _publish_lock(store):
    """Store-wide critical section: object publish/unlink vs catalog commit."""
    fd = os.open(store / 'locks' / 'publish.lock', os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)  # closing the fd releases the flock


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(_CHUNK), b''):
            h.update(chunk)
    return h.hexdigest()


def _utcnow():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def _open_catalog(store, *, create):
    catalog = Path(store) / CATALOG_NAME
    if not create and not catalog.is_file():
        raise BackupError('backup store not found (no %s): %s' % (CATALOG_NAME, store))
    con = sqlite3.connect(str(catalog), timeout=_BUSY_MS / 1000)
    con.isolation_level = None  # explicit BEGIN IMMEDIATE / COMMIT control
    con.execute('PRAGMA busy_timeout=%d' % _BUSY_MS)
    con.execute('PRAGMA synchronous=FULL')
    if create:
        con.execute('BEGIN IMMEDIATE')
        con.execute('CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS generations('
                    'generation_id INTEGER PRIMARY KEY,'
                    'request_id TEXT NOT NULL UNIQUE,'
                    'object_sha256 TEXT NOT NULL,'
                    'size_bytes INTEGER NOT NULL,'
                    'source_path TEXT NOT NULL,'
                    'created_utc TEXT NOT NULL)')
        con.execute("INSERT OR IGNORE INTO meta VALUES('store_format',?)", (STORE_FORMAT,))
        con.execute("INSERT OR IGNORE INTO meta VALUES('store_id',?)", (str(uuid.uuid4()),))
        con.execute('COMMIT')
    row = con.execute("SELECT value FROM meta WHERE key='store_format'").fetchone()
    if row is None or row[0] != STORE_FORMAT:
        con.close()
        raise BackupError('unsupported backup store format in %s (expected store_format=%s)'
                          % (catalog, STORE_FORMAT))
    return con


def _init_store(store):
    store = Path(store)
    fresh = not (store / CATALOG_NAME).exists()
    store.mkdir(parents=True, exist_ok=True)
    for sub in ('objects', 'tmp', 'holds', 'locks'):
        (store / sub).mkdir(exist_ok=True)
    _open_catalog(store, create=True).close()
    if fresh:
        _fsync_dir(store)


def _row_dict(r):
    return dict(generation_id=r[0], request_id=r[1], object_sha256=r[2],
                size_bytes=r[3], source_path=r[4], created_utc=r[5])


_COLUMNS = 'generation_id,request_id,object_sha256,size_bytes,source_path,created_utc'


def _by_request(con, request_id):
    r = con.execute('SELECT %s FROM generations WHERE request_id=?' % _COLUMNS,
                    (request_id,)).fetchone()
    return _row_dict(r) if r else None


def _all_generations(con):
    return [_row_dict(r) for r in con.execute(
        'SELECT %s FROM generations ORDER BY generation_id DESC' % _COLUMNS)]


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------

def _check_request_id(request_id):
    if not isinstance(request_id, str) or not request_id \
            or len(request_id) > _REQUEST_ID_MAX or '\x00' in request_id:
        raise BackupError('request_id must be a non-empty string of at most %d '
                          'characters without NUL' % _REQUEST_ID_MAX)
    return request_id


def _check_generation(generation):
    if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
        raise BackupError('generation must be a positive integer')
    return generation


def _check_keep(keep):
    if not isinstance(keep, int) or isinstance(keep, bool) or keep < 1:
        raise BackupError('keep must be a positive integer (at least 1 retained generation)')
    return keep


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

def _snapshot(source, staging):
    """Copy the committed state of source (including committed WAL content)."""
    uri = Path(source).resolve().as_uri() + '?mode=ro'
    src = sqlite3.connect(uri, uri=True, timeout=_BUSY_MS / 1000)
    try:
        dst = sqlite3.connect(str(staging))
        try:
            src.backup(dst)  # online backup API: consistent committed snapshot
        finally:
            dst.close()
    finally:
        src.close()


# ---------------------------------------------------------------------------
# Public operations
# ---------------------------------------------------------------------------

def backup(store, source, request_id):
    """Snapshot ``source`` into ``store`` exactly once for ``request_id``.

    Returns the committed generation record plus ``deduplicated``: True when
    the request was already committed and the stored generation is returned
    without re-opening the (possibly changed) source.
    """
    store = Path(store)
    request_id = _check_request_id(request_id)
    _init_store(store)
    con = _open_catalog(store, create=False)
    try:
        row = _by_request(con, request_id)
        if row is not None:
            return dict(row, deduplicated=True)
        source = Path(source)
        if not source.is_file():
            raise BackupError('source database not found: %s' % source)
        fd, tmp_name = tempfile.mkstemp(prefix=_TMP_PREFIX, suffix=_TMP_SUFFIX,
                                        dir=store / 'tmp')
        staging = Path(tmp_name)
        fcntl.flock(fd, fcntl.LOCK_EX)  # marks the staging file as in-use for the tmp sweep
        try:
            _snapshot(source, staging)
            _fault_point('after_snapshot')
            digest = _sha256_file(staging)
            size = staging.stat().st_size
            os.fsync(fd)
            obj = store / 'objects' / (digest + OBJECT_SUFFIX)
            created_utc = _utcnow()
            with _publish_lock(store):
                # 1. durable immutable object, 2. catalog commit: the row is the
                # commit point, so a crash can only leave collectable garbage.
                if obj.exists():
                    staging.unlink()
                else:
                    os.rename(staging, obj)
                    _fsync_dir(store / 'objects')
                _fault_point('after_object_publish')
                try:
                    con.execute('BEGIN IMMEDIATE')
                    new_id = con.execute('SELECT COALESCE(MAX(generation_id),0)+1 '
                                         'FROM generations').fetchone()[0]
                    con.execute('INSERT INTO generations VALUES(?,?,?,?,?,?)',
                                (new_id, request_id, digest, size, str(source), created_utc))
                    con.execute('COMMIT')
                except sqlite3.IntegrityError:
                    # A concurrent process committed this request_id first:
                    # return its generation; our duplicate object stays an
                    # orphan for the next prune sweep.
                    con.execute('ROLLBACK')
                    row = _by_request(con, request_id)
                    if row is None:
                        raise
                    return dict(row, deduplicated=True)
                _fault_point('after_catalog_commit')
            return dict(generation_id=new_id, request_id=request_id,
                        object_sha256=digest, size_bytes=size,
                        source_path=str(source), created_utc=created_utc,
                        deduplicated=False)
        finally:
            os.close(fd)
            staging.unlink(missing_ok=True)
    finally:
        con.close()


@contextlib.contextmanager
def hold_generation(store, generation):
    """Protect a committed generation against prune for the duration of a restore.

    The hold is an flock on ``holds/<generation>.hold``: it is released by the
    kernel on process exit or crash, never by timeout. The catalog row is
    re-checked after the lock is taken, so a generation pruned just before the
    hold is reported as unavailable instead of being read half-deleted.
    """
    store = Path(store)
    generation = _check_generation(generation)
    con = _open_catalog(store, create=False)  # clean error when the store is missing
    try:
        fd = os.open(store / 'holds' / ('%d.hold' % generation), os.O_RDWR | os.O_CREAT, 0o644)
    except OSError:
        con.close()
        raise
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            row = con.execute('SELECT %s FROM generations WHERE generation_id=?'
                              % _COLUMNS, (generation,)).fetchone()
        finally:
            con.close()
        if row is None:
            raise BackupError('generation %d is not committed in this store '
                              '(unknown or already pruned)' % generation)
        _fault_point('after_hold')
        yield _row_dict(row)
    finally:
        os.close(fd)


def _check_target(store, target):
    target = Path(target)
    parent = target.parent
    if not parent.is_dir():
        raise BackupError('target directory does not exist: %s' % parent)
    if target.is_dir():
        raise BackupError('restore target is a directory: %s' % target)
    store_real = os.path.realpath(store)
    target_real = os.path.realpath(target)
    if target_real == store_real or target_real.startswith(store_real + os.sep):
        raise BackupError('restore target must not be inside the backup store: %s' % target)
    return target, parent


def _sweep_staging(parent, target_name):
    """Remove this tool's own stale restore-staging files for one target.

    A live restore holds an flock on its staging file, so only files left
    behind by a crashed restore are removed; foreign files are never matched
    by the name pattern.
    """
    prefix = '.%s%s-' % (target_name, _STAGE_SUFFIX)
    for f in parent.iterdir():
        if not f.name.startswith(prefix):
            continue
        try:
            fd = os.open(f, os.O_RDWR)
        except OSError:
            continue
        try:
            if _try_lock(fd):
                f.unlink(missing_ok=True)
        finally:
            os.close(fd)


def _integrity_check(path):
    uri = Path(path).resolve().as_uri() + '?mode=ro'
    con = sqlite3.connect(uri, uri=True)
    try:
        rows = con.execute('PRAGMA integrity_check').fetchall()
    except sqlite3.Error as exc:
        raise BackupError('integrity_check failed on staged restore: %s' % exc) from None
    finally:
        con.close()
    if rows != [('ok',)]:
        raise BackupError('integrity_check failed on staged restore: %s'
                          % '; '.join(str(r[0]) for r in rows[:5]))


def restore(store, generation, target):
    """Materialize committed ``generation`` at ``target`` (atomically replaced).

    The object is verified (size, SHA-256, integrity_check) on a staging file
    before the replace; any failure keeps the old target bytes. The target
    must be offline and outside the store.
    """
    store = Path(store)
    generation = _check_generation(generation)
    target, parent = _check_target(store, target)
    with hold_generation(store, generation) as gen:
        obj = store / 'objects' / (gen['object_sha256'] + OBJECT_SUFFIX)
        if not obj.is_file():
            raise BackupError('object %s for generation %d is missing from the store'
                              % (gen['object_sha256'], generation))
        _sweep_staging(parent, target.name)
        fd, staging_name = tempfile.mkstemp(prefix='.%s%s-' % (target.name, _STAGE_SUFFIX),
                                            dir=parent)
        staging = Path(staging_name)
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            h = hashlib.sha256()
            total = 0
            with open(obj, 'rb') as src, os.fdopen(os.dup(fd), 'wb') as out:
                for chunk in iter(lambda: src.read(_CHUNK), b''):
                    out.write(chunk)
                    h.update(chunk)
                    total += len(chunk)
            if total != gen['size_bytes'] or h.hexdigest() != gen['object_sha256']:
                raise BackupError('object digest mismatch for generation %d: store object '
                                  'is corrupt or tampered; old target left untouched' % generation)
            os.fsync(fd)
            _integrity_check(staging)
            _fault_point('after_stage')
            # Point of no return. Stale sidecars of the previous target
            # incarnation are removed first so they can never be replayed onto
            # the new file; the replace itself is one atomic rename.
            for suffix in ('-wal', '-shm'):
                Path(str(target) + suffix).unlink(missing_ok=True)
            replaced = target.exists()
            os.replace(staging, target)
            _fsync_dir(parent)
        finally:
            os.close(fd)
            staging.unlink(missing_ok=True)
    return dict(generation_id=generation, object_sha256=gen['object_sha256'],
                size_bytes=gen['size_bytes'], target=str(target), replaced=replaced)


def _sweep_orphan_objects(store, con):
    removed = 0
    with _publish_lock(store):
        referenced = {r[0] for r in con.execute('SELECT object_sha256 FROM generations')}
        objects = store / 'objects'
        for f in objects.iterdir():
            if _OBJECT_RE.match(f.name) and f.name[:-len(OBJECT_SUFFIX)] not in referenced:
                f.unlink(missing_ok=True)
                removed += 1
        if removed:
            _fsync_dir(objects)
    return removed


def _sweep_tmp(store):
    removed = 0
    tmp = store / 'tmp'
    for f in tmp.iterdir():
        if not (f.name.startswith(_TMP_PREFIX) and f.name.endswith(_TMP_SUFFIX)):
            continue
        try:
            fd = os.open(f, os.O_RDWR)
        except OSError:
            continue
        try:
            if _try_lock(fd):
                f.unlink(missing_ok=True)
                removed += 1
        finally:
            os.close(fd)
    if removed:
        _fsync_dir(tmp)
    return removed


def prune(store, keep):
    """Drop all but the newest ``keep`` committed generations.

    Generations protected by a live restore hold are skipped (reported in
    ``skipped_locked``). Unreferenced objects and stale staging files left by
    killed processes are swept; only files matching this store's own naming
    patterns are ever removed.
    """
    store = Path(store)
    keep = _check_keep(keep)
    con = _open_catalog(store, create=False)
    try:
        candidates = _all_generations(con)[keep:]
        pruned = []
        skipped_locked = []
        for row in candidates:
            g = row['generation_id']
            hold = store / 'holds' / ('%d.hold' % g)
            fd = None
            if hold.exists():
                fd = os.open(hold, os.O_RDWR)
                if not _try_lock(fd):
                    os.close(fd)
                    skipped_locked.append(g)
                    continue
            try:
                # Unpublish first: after this commit the generation is gone for
                # new restores; the object unlink is a repeatable cleanup.
                con.execute('BEGIN IMMEDIATE')
                con.execute('DELETE FROM generations WHERE generation_id=?', (g,))
                con.execute('COMMIT')
                with _publish_lock(store):
                    refs = con.execute('SELECT COUNT(*) FROM generations '
                                       'WHERE object_sha256=?', (row['object_sha256'],)).fetchone()[0]
                    if refs == 0:
                        (store / 'objects' / (row['object_sha256'] + OBJECT_SUFFIX)).unlink(missing_ok=True)
                        _fsync_dir(store / 'objects')
                hold.unlink(missing_ok=True)
                pruned.append(g)
            finally:
                if fd is not None:
                    os.close(fd)
        orphans_removed = _sweep_orphan_objects(store, con)
        tmp_removed = _sweep_tmp(store)
        remaining = [r['generation_id'] for r in _all_generations(con)]
        return dict(keep=keep, pruned=pruned, skipped_locked=skipped_locked,
                    orphans_removed=orphans_removed, tmp_removed=tmp_removed,
                    remaining=remaining)
    finally:
        con.close()


def list_generations(store):
    """All committed generations, newest first."""
    con = _open_catalog(Path(store), create=False)
    try:
        return _all_generations(con)
    finally:
        con.close()


def check_store(store):
    """Verify catalog <-> object consistency without changing anything."""
    store = Path(store)
    con = _open_catalog(store, create=False)
    try:
        rows = _all_generations(con)
    finally:
        con.close()
    missing = []
    mismatched = []
    referenced = set()
    for row in rows:
        sha = row['object_sha256']
        referenced.add(sha)
        obj = store / 'objects' / (sha + OBJECT_SUFFIX)
        if not obj.is_file():
            missing.append(row['generation_id'])
        elif obj.stat().st_size != row['size_bytes'] or _sha256_file(obj) != sha:
            mismatched.append(row['generation_id'])
    objects = store / 'objects'
    orphans = sorted(f.name for f in objects.iterdir()
                     if _OBJECT_RE.match(f.name)
                     and f.name[:-len(OBJECT_SUFFIX)] not in referenced) if objects.is_dir() else []
    tmp = store / 'tmp'
    stray_tmp = sorted(f.name for f in tmp.iterdir()
                       if f.name.startswith(_TMP_PREFIX)
                       and f.name.endswith(_TMP_SUFFIX)) if tmp.is_dir() else []
    return dict(ok=not missing and not mismatched, generations=len(rows),
                missing=missing, mismatched=mismatched,
                orphan_objects=orphans, stray_tmp=stray_tmp)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    p = argparse.ArgumentParser(
        prog='circulation_flow.backup',
        description='Local multi-generation backup store for circulation SQLite deliverables')
    sub = p.add_subparsers(dest='command', required=True)
    b = sub.add_parser('backup', help='snapshot a SQLite database into the store')
    b.add_argument('--store', required=True)
    b.add_argument('--source', required=True)
    b.add_argument('--request-id', required=True)
    r = sub.add_parser('restore', help='atomically replace a target with a committed generation')
    r.add_argument('--store', required=True)
    r.add_argument('--generation', required=True, type=int)
    r.add_argument('--target', required=True)
    pr = sub.add_parser('prune', help='keep only the newest N committed generations')
    pr.add_argument('--store', required=True)
    pr.add_argument('--keep', required=True, type=int)
    li = sub.add_parser('list', help='list committed generations (newest first)')
    li.add_argument('--store', required=True)
    ck = sub.add_parser('check', help='verify catalog/object consistency')
    ck.add_argument('--store', required=True)
    a = p.parse_args(argv)
    try:
        if a.command == 'backup':
            out = backup(a.store, a.source, a.request_id)
        elif a.command == 'restore':
            out = restore(a.store, a.generation, a.target)
        elif a.command == 'prune':
            out = prune(a.store, a.keep)
        elif a.command == 'list':
            out = dict(generations=list_generations(a.store))
        else:
            out = check_store(a.store)
    except (ValueError, OSError, sqlite3.Error) as exc:
        p.exit(2, str(exc) + '\n')
    print(json.dumps(out, sort_keys=True))


if __name__ == '__main__':
    main()

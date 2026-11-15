"""Two-process crash/concurrency tests for the multi-generation backup store.

Covers, always against real CLI-built deliverables (plain JSONL pipeline
database and vendor package intake database with BLOB provenance):

  1. Round-trip of both artifact kinds: tables, views and BLOB bytes survive
     backup/restore untouched (pure byte copy, no business recomputation).
  2. WAL sources: transactions committed only in the source WAL are captured.
  3. Idempotent requests: a retry after the source changed returns the
     original generation without re-reading the source; two concurrent
     processes racing one request_id while the source changes commit exactly
     one generation.
  4. SIGKILL at every persistent commit boundary (snapshot, object publish,
     catalog commit, restore hold, restore stage): after restart the catalog
     distinguishes committed generations from orphans, restore never sees a
     half generation, and prune collects only the store's own garbage.
  5. prune keeps the newest N committed generations, skips generations
     protected by a live restore hold, and proceeds immediately once the
     holding process dies (kernel-released flock, no timeout).
  6. Object tampering/loss: restore fails before touching the target and the
     old target bytes survive; check_store reports the damage.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
sys.path.insert(0, str(ROOT / 'tools'))

from circulation_flow.backup import (backup, restore, prune, list_generations,  # noqa: E402
                                     check_store, hold_generation, BackupError)
from circulation_flow.pipeline import run as pipeline_run  # noqa: E402
from circulation_flow.intake import collect  # noqa: E402
from generate_package import build_package, load_jsonl_records  # noqa: E402

FIX = ROOT / 'fixtures/events.jsonl'
AS_OF = '2026-01-12T00:00:00Z'
SIGKILL_RC = -signal.SIGKILL


def db_fingerprint(path):
    """Order-independent digest of schema + every row of every table/view."""
    con = sqlite3.connect(str(path))
    h = hashlib.sha256()
    relations = []
    for typ, name, sql in con.execute("SELECT type,name,sql FROM sqlite_master "
                                      "WHERE name NOT LIKE 'sqlite_%' ORDER BY name"):
        h.update(repr((typ, name, sql)).encode())
        if typ in ('table', 'view'):
            relations.append(name)
    for name in relations:
        for r in sorted(repr(row) for row in con.execute('SELECT * FROM "%s"' % name)):
            h.update(r.encode())
    con.close()
    return h.hexdigest()


def build_plain(path):
    pipeline_run(FIX, AS_OF, path)


def build_intake(path, zip_path):
    records = load_jsonl_records(FIX)
    data, _m, _e = build_package(records, ['csv/part-1.csv', 'csv/part-2.csv', 'csv/part-3.csv'])
    zip_path.write_bytes(data)
    collect(zip_path, AS_OF, path)


def mutate(path, tag, payload=b'x'):
    """Change a source database in a visible, fingerprintable way."""
    con = sqlite3.connect(str(path))
    con.execute('CREATE TABLE %s(tag TEXT,b BLOB)' % tag)
    con.execute('INSERT INTO %s VALUES(?,?)' % tag, (tag, payload))
    con.commit()
    con.close()


class Case(unittest.TestCase):
    def setUp(self):
        t = tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.root = Path(t.name)
        self.store = self.root / 'store'

    # -- CLI helpers ---------------------------------------------------------
    def _env(self, extra):
        env = dict(os.environ)
        pp = str(ROOT / 'src')
        if env.get('PYTHONPATH'):
            pp += os.pathsep + env['PYTHONPATH']
        env['PYTHONPATH'] = pp
        env.update(extra or {})
        return env

    def cli(self, args, env_extra=None):
        return subprocess.run([sys.executable, '-m', 'circulation_flow.backup'] + args,
                              capture_output=True, text=True, env=self._env(env_extra),
                              timeout=120)

    def cli_ok(self, args, env_extra=None):
        p = self.cli(args, env_extra)
        self.assertEqual(p.returncode, 0, 'stderr: %s' % p.stderr)
        return json.loads(p.stdout)

    def cli_popen(self, args, env_extra=None):
        return subprocess.Popen([sys.executable, '-m', 'circulation_flow.backup'] + args,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, env=self._env(env_extra))

    def wait_ready(self, path, proc=None):
        deadline = time.monotonic() + 30
        while not path.exists():
            if proc is not None and proc.poll() is not None:
                self.fail('child exited %s before rendezvous: %s'
                          % (proc.returncode, proc.stderr.read()))
            if time.monotonic() > deadline:
                self.fail('timed out waiting for %s' % path)
            time.sleep(0.02)

    # -- fixture builders ----------------------------------------------------
    def plain_source(self, name='plain.sqlite'):
        src = self.root / name
        build_plain(src)
        return src

    def intake_source(self, name='intake.sqlite'):
        src = self.root / name
        build_intake(src, self.root / (name + '.zip'))
        return src


class RoundTripTest(Case):
    def test_real_cli_artifacts_end_to_end(self):
        """Both deliverables built by their real CLIs survive backup/restore."""
        env = self._env({})
        plain = self.root / 'cli-plain.sqlite'
        p = subprocess.run([sys.executable, '-m', 'circulation_flow.cli',
                            '--source', str(FIX), '--as-of', AS_OF,
                            '--database', str(plain)],
                           capture_output=True, text=True, env=env, timeout=120)
        self.assertEqual(p.returncode, 0, p.stderr)
        zip_path = self.root / 'export.zip'
        p = subprocess.run([sys.executable, str(ROOT / 'tools' / 'generate_package.py'),
                            str(zip_path), '--source', str(FIX),
                            '--parts', '3', '--seed', '20261113'],
                           capture_output=True, text=True, env=env, timeout=120)
        self.assertEqual(p.returncode, 0, p.stderr)
        intake = self.root / 'cli-intake.sqlite'
        p = subprocess.run([sys.executable, '-m', 'circulation_flow.intake',
                            '--package', str(zip_path), '--as-of', AS_OF,
                            '--database', str(intake)],
                           capture_output=True, text=True, env=env, timeout=120)
        self.assertEqual(p.returncode, 0, p.stderr)
        for i, src in enumerate((plain, intake), start=1):
            out = self.cli_ok(['backup', '--store', str(self.store),
                               '--source', str(src), '--request-id', 'cli-%d' % i])
            self.assertEqual(out['generation_id'], i)
            target = self.root / ('restored-%d.sqlite' % i)
            self.cli_ok(['restore', '--store', str(self.store),
                         '--generation', str(i), '--target', str(target)])
            self.assertEqual(db_fingerprint(target), db_fingerprint(src))
        # The restored package database still carries every archived byte.
        con = sqlite3.connect(str(self.root / 'restored-2.sqlite'))
        self.assertEqual(con.execute('SELECT package_bytes FROM intake_runs').fetchone()[0],
                         zip_path.read_bytes())
        con.close()

    def test_roundtrip_both_artifacts(self):
        plain = self.plain_source()
        intake = self.intake_source()
        g_plain = backup(self.store, plain, 'req-plain')
        g_intake = backup(self.store, intake, 'req-intake')
        self.assertEqual((g_plain['generation_id'], g_intake['generation_id']), (1, 2))
        out_plain = self.root / 'restore-plain.sqlite'
        out_intake = self.root / 'restore-intake.sqlite'
        restore(self.store, 1, out_plain)
        restore(self.store, 2, out_intake)
        # Byte-faithful copy: schema, tables, views and BLOBs all identical.
        self.assertEqual(db_fingerprint(plain), db_fingerprint(out_plain))
        self.assertEqual(db_fingerprint(intake), db_fingerprint(out_intake))
        for db in (out_plain, out_intake):
            con = sqlite3.connect(str(db))
            self.assertEqual(con.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            con.close()
        # Package provenance BLOBs are preserved byte for byte: the captured
        # ZIP rebuilt from the restored database equals the archived original.
        src = sqlite3.connect(str(intake))
        dst = sqlite3.connect(str(out_intake))
        self.assertEqual(src.execute('SELECT package_bytes,manifest_bytes FROM intake_runs').fetchone(),
                         dst.execute('SELECT package_bytes,manifest_bytes FROM intake_runs').fetchone())
        self.assertEqual(src.execute('SELECT raw_bytes FROM intake_parts ORDER BY part_id').fetchall(),
                         dst.execute('SELECT raw_bytes FROM intake_parts ORDER BY part_id').fetchall())
        self.assertEqual(dst.execute('SELECT package_bytes FROM intake_runs').fetchone()[0],
                         (self.root / 'intake.sqlite.zip').read_bytes())
        self.assertEqual(dst.execute('SELECT * FROM status_summary').fetchall(),
                         [('closed', 1, 1, 2, 1), ('open', 1, 1, 4, 0)])
        src.close()
        dst.close()

    def test_restore_replaces_target_and_drops_stale_sidecars(self):
        src = self.plain_source()
        backup(self.store, src, 'req-1')
        target = self.root / 't.sqlite'
        r1 = restore(self.store, 1, target)
        self.assertFalse(r1['replaced'])
        # Simulate leftovers of a previous WAL incarnation of the target.
        Path(str(target) + '-wal').write_bytes(b'junk-wal')
        Path(str(target) + '-shm').write_bytes(b'junk-shm')
        r2 = restore(self.store, 1, target)
        self.assertTrue(r2['replaced'])
        self.assertFalse(Path(str(target) + '-wal').exists())
        self.assertFalse(Path(str(target) + '-shm').exists())
        self.assertEqual(db_fingerprint(src), db_fingerprint(target))

    def test_cli_roundtrip_and_json(self):
        plain = self.plain_source()
        intake = self.intake_source()
        g1 = self.cli_ok(['backup', '--store', str(self.store),
                          '--source', str(plain), '--request-id', 'cli-plain'])
        g2 = self.cli_ok(['backup', '--store', str(self.store),
                          '--source', str(intake), '--request-id', 'cli-intake'])
        self.assertEqual((g1['generation_id'], g2['generation_id']), (1, 2))
        listed = self.cli_ok(['list', '--store', str(self.store)])
        self.assertEqual([g['generation_id'] for g in listed['generations']], [2, 1])
        out = self.root / 'cli-out.sqlite'
        self.cli_ok(['restore', '--store', str(self.store),
                     '--generation', '2', '--target', str(out)])
        self.assertEqual(db_fingerprint(intake), db_fingerprint(out))
        chk = self.cli_ok(['check', '--store', str(self.store)])
        self.assertTrue(chk['ok'])
        self.assertEqual(chk['generations'], 2)
        # CLI errors exit non-zero with the reason on stderr.
        p = self.cli(['restore', '--store', str(self.store),
                      '--generation', '99', '--target', str(self.root / 'x.sqlite')])
        self.assertEqual(p.returncode, 2)
        self.assertIn('generation 99', p.stderr)


class WalSourceTest(Case):
    def test_committed_wal_content_captured_across_processes(self):
        src = self.plain_source()
        blob = hashlib.sha256(b'wal-payload').digest() * 16384  # 512 KiB deterministic BLOB
        con = sqlite3.connect(str(src))
        con.isolation_level = None  # autocommit: explicit transaction control
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('CREATE TABLE wal_marker(tag TEXT,b BLOB)')
        con.execute('INSERT INTO wal_marker VALUES(?,?)', ('wal', blob))
        wal = Path(str(src) + '-wal')
        self.assertTrue(wal.exists() and wal.stat().st_size > 0)
        # An uncommitted transaction must not leak into the snapshot.
        con.execute('BEGIN')
        con.execute('CREATE TABLE wal_pending(x)')
        con.execute('INSERT INTO wal_marker VALUES(?,?)', ('pending', b'p'))
        # Backup runs in a second process while the WAL connection stays open.
        g = self.cli_ok(['backup', '--store', str(self.store),
                         '--source', str(src), '--request-id', 'wal-req'])
        out = self.root / 'wal-out.sqlite'
        self.cli_ok(['restore', '--store', str(self.store),
                     '--generation', str(g['generation_id']), '--target', str(out)])
        con.execute('ROLLBACK')
        fp_src = db_fingerprint(src)
        con.close()
        self.assertEqual(db_fingerprint(out), fp_src)
        chk = sqlite3.connect(str(out))
        self.assertEqual(chk.execute('SELECT b FROM wal_marker').fetchone()[0], blob)
        self.assertEqual(chk.execute('SELECT count(*) FROM wal_marker').fetchone()[0], 1)
        self.assertEqual(chk.execute("SELECT count(*) FROM sqlite_master "
                                     "WHERE name='wal_pending'").fetchone()[0], 0)
        chk.close()


class IdempotencyTest(Case):
    def test_retry_after_source_change_returns_original_generation(self):
        src = self.plain_source()
        fp_v1 = db_fingerprint(src)
        g1 = backup(self.store, src, 'req-1')
        mutate(src, 'mark_v2', b'v2')
        self.assertNotEqual(db_fingerprint(src), fp_v1)
        g2 = backup(self.store, src, 'req-1')
        self.assertEqual(g2['generation_id'], g1['generation_id'])
        self.assertTrue(g2['deduplicated'])
        out = self.root / 'out.sqlite'
        restore(self.store, g1['generation_id'], out)
        self.assertEqual(db_fingerprint(out), fp_v1)  # the changed source was not re-read

    def test_validation(self):
        src = self.plain_source()
        for bad in ('', 'x' * 201, 'a\x00b', 12, None):
            with self.assertRaises(BackupError):
                backup(self.store, src, bad)
        for bad in (0, -1, True, 1.5, '2'):
            with self.assertRaises(BackupError):
                prune(self.store, bad)
        for bad in (0, -3, True, '1'):
            with self.assertRaises(BackupError):
                restore(self.store, bad, self.root / 't.sqlite')
        with self.assertRaises(BackupError):
            backup(self.store, self.root / 'missing.sqlite', 'req-x')
        with self.assertRaises(BackupError):
            list_generations(self.root / 'no-such-store')

    def test_store_format_mismatch_rejected(self):
        src = self.plain_source()
        backup(self.store, src, 'req-1')
        con = sqlite3.connect(str(self.store / 'catalog.sqlite'))
        con.execute("UPDATE meta SET value='0' WHERE key='store_format'")
        con.commit()
        con.close()
        with self.assertRaisesRegex(BackupError, 'store format'):
            backup(self.store, src, 'req-2')
        with self.assertRaisesRegex(BackupError, 'store format'):
            list_generations(self.store)


class ConcurrencyTest(Case):
    def test_concurrent_same_request_with_changed_source(self):
        src = self.plain_source()
        rv = self.root / 'rv'
        rv.mkdir()
        # Process A snapshots v1, then pauses before publishing.
        proc_a = self.cli_popen(['backup', '--store', str(self.store),
                                 '--source', str(src), '--request-id', 'shared'],
                                {'CIRCULATION_BACKUP_RV_DIR': str(rv)})
        self.wait_ready(rv / 'after_snapshot.ready', proc_a)
        # The source changes; process B commits the same request first.
        mutate(src, 'mark_v2', b'v2')
        fp_v2 = db_fingerprint(src)
        out_b = self.cli_ok(['backup', '--store', str(self.store),
                             '--source', str(src), '--request-id', 'shared'])
        self.assertEqual(out_b['generation_id'], 1)
        self.assertFalse(out_b['deduplicated'])
        (rv / 'after_snapshot.go').touch()
        out_a_text, err_a = proc_a.communicate(timeout=60)
        self.assertEqual(proc_a.returncode, 0, err_a)
        out_a = json.loads(out_a_text)
        # Both processes agree on the single committed generation.
        self.assertEqual(out_a['generation_id'], 1)
        self.assertTrue(out_a['deduplicated'])
        gens = list_generations(self.store)
        self.assertEqual(len(gens), 1)
        out = self.root / 'out.sqlite'
        restore(self.store, 1, out)
        self.assertEqual(db_fingerprint(out), fp_v2)
        # A's snapshot lost the race: its object is an orphan, swept by prune.
        chk = check_store(self.store)
        self.assertEqual(len(chk['orphan_objects']), 1)
        result = prune(self.store, 1)
        self.assertEqual(result['orphans_removed'], 1)
        self.assertEqual(check_store(self.store)['orphan_objects'], [])

    def test_concurrent_different_requests_on_fresh_store(self):
        src = self.plain_source()
        procs = [self.cli_popen(['backup', '--store', str(self.store),
                                 '--source', str(src), '--request-id', 'req-%d' % i])
                 for i in (1, 2)]
        outs = []
        for p in procs:
            text, err = p.communicate(timeout=60)
            self.assertEqual(p.returncode, 0, err)
            outs.append(json.loads(text))
        self.assertEqual(sorted(o['generation_id'] for o in outs), [1, 2])
        # Identical content shares one immutable object.
        self.assertEqual(len(list((self.store / 'objects').iterdir())), 1)
        self.assertTrue(check_store(self.store)['ok'])


class CrashBoundaryTest(Case):
    def test_backup_kill_boundaries(self):
        for point, expect in (('after_snapshot', 'tmp'),
                              ('after_object_publish', 'orphan'),
                              ('after_catalog_commit', 'committed')):
            with self.subTest(point=point):
                store = self.root / ('store-' + point)
                src = self.plain_source('src-%s.sqlite' % point)
                fp_src = db_fingerprint(src)
                p = self.cli(['backup', '--store', str(store), '--source', str(src),
                              '--request-id', 'req-1'],
                             {'CIRCULATION_BACKUP_KILL_AT': point})
                self.assertEqual(p.returncode, SIGKILL_RC, p.stderr)
                chk = check_store(store)
                self.assertTrue(chk['ok'])  # no committed generation is ever broken
                if expect == 'tmp':
                    self.assertEqual(chk['generations'], 0)
                    self.assertEqual(len(chk['stray_tmp']), 1)
                elif expect == 'orphan':
                    self.assertEqual(chk['generations'], 0)
                    self.assertEqual(len(chk['orphan_objects']), 1)
                    # A half-published generation is invisible to restore.
                    p2 = self.cli(['restore', '--store', str(store), '--generation', '1',
                                   '--target', str(self.root / 'half.sqlite')])
                    self.assertEqual(p2.returncode, 2)
                else:
                    self.assertEqual(chk['generations'], 1)
                swept = prune(store, 1)
                if expect == 'tmp':
                    self.assertEqual(swept['tmp_removed'], 1)
                elif expect == 'orphan':
                    self.assertEqual(swept['orphans_removed'], 1)
                # Retrying the same request converges to exactly one generation.
                g = backup(store, src, 'req-1')
                self.assertEqual(g['generation_id'], 1)
                self.assertEqual(g['deduplicated'], expect == 'committed')
                self.assertEqual(len(list_generations(store)), 1)
                out = self.root / ('out-%s.sqlite' % point)
                restore(store, 1, out)
                self.assertEqual(db_fingerprint(out), fp_src)
                self.assertEqual(check_store(store)['orphan_objects'], [])

    def test_restore_kill_after_hold_releases_protection(self):
        src = self.plain_source()
        backup(self.store, src, 'req-1')
        mutate(src, 'mark_v2', b'v2')
        backup(self.store, src, 'req-2')
        target = self.root / 'target.sqlite'
        shutil.copyfile(src, target)
        old_bytes = target.read_bytes()
        p = self.cli(['restore', '--store', str(self.store), '--generation', '1',
                      '--target', str(target)],
                     {'CIRCULATION_BACKUP_KILL_AT': 'after_hold'})
        self.assertEqual(p.returncode, SIGKILL_RC, p.stderr)
        # Nothing was touched and the dead holder no longer protects gen 1:
        # prune proceeds immediately, without any timeout.
        self.assertEqual(target.read_bytes(), old_bytes)
        result = prune(self.store, 1)
        self.assertEqual(result['pruned'], [1])
        self.assertEqual(result['skipped_locked'], [])
        self.assertEqual(result['remaining'], [2])

    def test_restore_kill_after_stage_preserves_old_target(self):
        src = self.plain_source()
        backup(self.store, src, 'req-1')
        target = self.root / 'target.sqlite'
        build_intake(target, self.root / 'old.zip')  # different old content
        old_bytes = target.read_bytes()
        p = self.cli(['restore', '--store', str(self.store), '--generation', '1',
                      '--target', str(target)],
                     {'CIRCULATION_BACKUP_KILL_AT': 'after_stage'})
        self.assertEqual(p.returncode, SIGKILL_RC, p.stderr)
        self.assertEqual(target.read_bytes(), old_bytes)  # old target intact
        # The crashed restore's staging file is own garbage, swept on retry.
        self.assertEqual(len(list(self.root.glob('.target.sqlite.circbak-*'))), 1)
        restore(self.store, 1, target)
        self.assertEqual(db_fingerprint(target), db_fingerprint(src))
        self.assertEqual(list(self.root.glob('.target.sqlite.circbak-*')), [])


class PruneProtectionTest(Case):
    def test_prune_skips_held_generation_while_backup_completes(self):
        src = self.plain_source()
        fps = {}
        for i in (1, 2, 3):
            fps[i] = db_fingerprint(src)
            backup(self.store, src, 'req-%d' % i)
            mutate(src, 'mark_v%d' % (i + 1), b'v%d' % i)
        rv = self.root / 'rv'
        rv.mkdir()
        target = self.root / 'held.sqlite'
        proc_r = self.cli_popen(['restore', '--store', str(self.store),
                                 '--generation', '1', '--target', str(target)],
                                {'CIRCULATION_BACKUP_RV_DIR': str(rv)})
        self.wait_ready(rv / 'after_hold.ready', proc_r)
        # During the long restore a new backup commits and prune runs.
        g4 = backup(self.store, src, 'req-4')
        self.assertEqual(g4['generation_id'], 4)
        result = prune(self.store, 1)
        self.assertEqual(result['pruned'], [3, 2])
        self.assertEqual(result['skipped_locked'], [1])
        self.assertEqual(result['remaining'], [4, 1])
        self.assertTrue(check_store(self.store)['ok'])
        # Release the restore: the protected generation is still restorable.
        (rv / 'after_hold.go').touch()
        text, err = proc_r.communicate(timeout=60)
        self.assertEqual(proc_r.returncode, 0, err)
        self.assertEqual(db_fingerprint(target), fps[1])
        # With the hold gone, prune collects generation 1 as well.
        result = prune(self.store, 1)
        self.assertEqual(result['pruned'], [1])
        self.assertEqual(result['remaining'], [4])
        out = self.root / 'gen4.sqlite'
        restore(self.store, 4, out)
        self.assertEqual(db_fingerprint(out), db_fingerprint(src))

    def test_prune_retention_numbering_and_foreign_files(self):
        src = self.plain_source()
        fps = {}
        for i in (1, 2, 3, 4, 5):
            fps[i] = db_fingerprint(src)
            backup(self.store, src, 'req-%d' % i)
            mutate(src, 'mark_v%d' % (i + 1), b'v%d' % i)
        result = prune(self.store, 2)
        self.assertEqual(result['pruned'], [3, 2, 1])
        self.assertEqual(result['remaining'], [5, 4])
        # Retained older generations stay restorable.
        out4 = self.root / 'g4.sqlite'
        restore(self.store, 4, out4)
        self.assertEqual(db_fingerprint(out4), fps[4])
        # Numbering keeps increasing; ids are not reused.
        g6 = backup(self.store, src, 'req-6')
        self.assertEqual(g6['generation_id'], 6)
        # Only the store's own garbage is collected: foreign files survive,
        # valid-looking orphans and stale tmp files are swept.
        objects = self.store / 'objects'
        (objects / 'notes.txt').write_text('foreign')
        (objects / 'not-a-sha.db').write_text('foreign')
        (objects / ('0' * 64 + '.db')).write_text('orphan')
        (self.store / 'tmp' / 'foreign.bin').write_text('foreign')
        (self.store / 'tmp' / 'obj-stale.part').write_text('stale')
        result = prune(self.store, 2)
        self.assertEqual(result['orphans_removed'], 1)
        self.assertEqual(result['tmp_removed'], 1)
        self.assertTrue((objects / 'notes.txt').exists())
        self.assertTrue((objects / 'not-a-sha.db').exists())
        self.assertTrue((self.store / 'tmp' / 'foreign.bin').exists())
        self.assertFalse((objects / ('0' * 64 + '.db')).exists())
        self.assertTrue(check_store(self.store)['ok'])

    def test_tmp_sweep_respects_live_backup_lock(self):
        src = self.plain_source()
        backup(self.store, src, 'req-1')
        live = self.store / 'tmp' / 'obj-live.part'
        live.write_text('in-flight')
        fd = os.open(live, os.O_RDWR)
        fcntl.flock(fd, fcntl.LOCK_EX)  # a live backup holds its staging file
        try:
            result = prune(self.store, 1)
            self.assertEqual(result['tmp_removed'], 0)
            self.assertTrue(live.exists())
        finally:
            os.close(fd)
        result = prune(self.store, 1)
        self.assertEqual(result['tmp_removed'], 1)
        self.assertFalse(live.exists())


class TamperTest(Case):
    def test_object_tampered_restore_fails_old_target_kept(self):
        src = self.plain_source()
        g = backup(self.store, src, 'req-1')
        target = self.root / 'target.sqlite'
        build_intake(target, self.root / 'old.zip')
        old_bytes = target.read_bytes()
        obj = self.store / 'objects' / (g['object_sha256'] + '.db')
        data = bytearray(obj.read_bytes())
        data[len(data) // 2] ^= 0xFF  # flip one byte, keep the size
        obj.write_bytes(bytes(data))
        with self.assertRaisesRegex(BackupError, 'digest mismatch'):
            restore(self.store, 1, target)
        self.assertEqual(target.read_bytes(), old_bytes)
        p = self.cli(['restore', '--store', str(self.store), '--generation', '1',
                      '--target', str(target)])
        self.assertEqual(p.returncode, 2)
        self.assertIn('digest mismatch', p.stderr)
        self.assertEqual(target.read_bytes(), old_bytes)
        chk = check_store(self.store)
        self.assertFalse(chk['ok'])
        self.assertEqual(chk['mismatched'], [1])

    def test_object_deleted_restore_fails_old_target_kept(self):
        src = self.plain_source()
        g = backup(self.store, src, 'req-1')
        target = self.root / 'target.sqlite'
        target.write_bytes(b'previous target')
        (self.store / 'objects' / (g['object_sha256'] + '.db')).unlink()
        with self.assertRaisesRegex(BackupError, 'missing'):
            restore(self.store, 1, target)
        self.assertEqual(target.read_bytes(), b'previous target')
        chk = check_store(self.store)
        self.assertFalse(chk['ok'])
        self.assertEqual(chk['missing'], [1])


class TargetSafetyTest(Case):
    def test_target_inside_store_rejected(self):
        src = self.plain_source()
        backup(self.store, src, 'req-1')
        for target in (self.store / 'inside.sqlite',
                       self.store / 'objects' / 'nested.sqlite',
                       self.store / 'catalog.sqlite'):
            with self.assertRaisesRegex(BackupError, 'inside the backup store'):
                restore(self.store, 1, target)
        p = self.cli(['restore', '--store', str(self.store), '--generation', '1',
                      '--target', str(self.store / 'inside.sqlite')])
        self.assertEqual(p.returncode, 2)
        self.assertIn('inside the backup store', p.stderr)

    def test_target_shape_and_unknown_generation(self):
        src = self.plain_source()
        backup(self.store, src, 'req-1')
        with self.assertRaisesRegex(BackupError, 'directory'):
            restore(self.store, 1, self.root)  # a directory
        with self.assertRaisesRegex(BackupError, 'does not exist'):
            restore(self.store, 1, self.root / 'no-dir' / 't.sqlite')
        with self.assertRaisesRegex(BackupError, 'generation 99'):
            restore(self.store, 99, self.root / 't.sqlite')
        # A pruned generation is gone for restore.
        mutate(src, 'mark_v2', b'v2')
        backup(self.store, src, 'req-2')
        prune(self.store, 1)
        with self.assertRaisesRegex(BackupError, 'generation 1'):
            restore(self.store, 1, self.root / 't.sqlite')


if __name__ == '__main__':
    unittest.main()

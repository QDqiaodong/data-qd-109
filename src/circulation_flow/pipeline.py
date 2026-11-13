from contextlib import closing
from datetime import date as _date
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from .events import instant,iter_events

WORK_MEMORY_MB_MIN=4
WORK_MEMORY_MB_MAX=256
WORK_MEMORY_MB_DEFAULT=16

_TEMP_PREFIX='.circulation-'
_TEMP_SUFFIX='.tmp'

#
# Bounded-memory pipeline
# -----------------------
# The large-input path never keeps an event or loan collection in a Python
# container. One sequential pass over the JSONL feeds a temporary on-disk SQLite
# staging database; a covering index provides global (event_at,event_id) ordering
# and duplicate detection. A server-side cursor then feeds the state machine,
# whose state (active copies, loans, used loan IDs, simultaneous-event guard)
# lives in SQLite tables. Final overdue/status materialization is one SQL pass.
# The same temporary database is atomically renamed onto the target only after
# every validation and write has succeeded.
#

def _check_work_memory_mb(work_memory_mb):
    if not isinstance(work_memory_mb,int) or isinstance(work_memory_mb,bool) \
            or not (WORK_MEMORY_MB_MIN<=work_memory_mb<=WORK_MEMORY_MB_MAX):
        raise ValueError('work_memory_mb must be an integer between %d and %d'
                         %(WORK_MEMORY_MB_MIN,WORK_MEMORY_MB_MAX))
    return work_memory_mb

def _resolve_temp_dir(temp_dir,target_parent):
    """Return a writable temp directory guaranteed to share the target's volume."""
    if temp_dir is None:
        return Path(target_parent)
    temp_dir=Path(temp_dir)
    temp_dir.mkdir(parents=True,exist_ok=True)
    if not temp_dir.is_dir():
        raise ValueError('temp dir is not a directory: %s'%temp_dir)
    # Check the volume first (stat only, no write) so a foreign filesystem fails
    # with a precise error even when the directory itself is not writable.
    if os.stat(temp_dir).st_dev!=os.stat(target_parent).st_dev:
        raise ValueError('temp dir must be on the same filesystem as the target database '
                         'so the final publish can be an atomic rename')
    # Create a real file (not a probe directory) so write permission is tested on
    # the same volume that will hold the staging database.
    probe=tempfile.NamedTemporaryFile(prefix=_TEMP_PREFIX,suffix=_TEMP_SUFFIX,
                                      dir=temp_dir,delete=False)
    try:
        probe.write(b'ok');probe.flush();probe.close()
    except OSError:
        Path(probe.name).unlink(missing_ok=True)
        raise
    Path(probe.name).unlink(missing_ok=True)
    return temp_dir

def _apply_limits(db,work_memory_mb):
    """Bind the SQLite memory budget and keep sorting/spilling on disk.

    Constrained by work_memory_mb:
      * cache_size       - page cache ceiling (negative value = KiB);
      * temp_store=FILE  - ORDER BY/index/sort materialization never grows the
                           SQLite in-memory allocator, it spills into temp files
                           under SQLITE_TMPDIR;
      * mmap_size=0      - no memory-mapped I/O budget is taken.
    Not guaranteed to be precisely capped by the budget:
      * on-disk staging, rollback journal and spill-file size (bounded by event
        count and available disk, not by the memory budget);
      * fixed interpreter/runtime overhead and short-lived per-row allocations
        (one line / one row at a time);
      * OS page cache outside the process.
    """
    budget_kib=work_memory_mb*1024
    db.execute('PRAGMA journal_mode=DELETE')
    db.execute('PRAGMA synchronous=NORMAL')
    db.execute('PRAGMA temp_store=FILE')
    db.execute('PRAGMA mmap_size=0')
    db.execute('PRAGMA cache_size=%d'%-budget_kib)

def _stage_events(db,source):
    """One sequential JSONL pass -> on-disk unique-event staging table."""
    db.execute('CREATE TABLE stage_events('
               'event_id TEXT PRIMARY KEY,'
               'event_at TEXT NOT NULL,'
               'payload TEXT NOT NULL)')
    source_rows=0;duplicate_rows=0;saw_any=False
    insert=db.execute
    for event in iter_events(source):
        saw_any=True;source_rows+=1
        event_id=event['event_id'];event_at=event['at']
        payload=json.dumps(event,sort_keys=True)
        row=insert('SELECT payload FROM stage_events WHERE event_id=?',(event_id,)).fetchone()
        if row is None:
            insert('INSERT INTO stage_events(event_id,event_at,payload) VALUES (?,?,?)',
                   (event_id,event_at,payload))
        elif row[0]==payload:
            duplicate_rows+=1
        else:
            raise ValueError('conflicting event ID')
    if not saw_any:
        raise ValueError('empty event stream')
    # Covering index: global (event_at,event_id) order is served from disk;
    # any index build beyond the page-cache budget spills via temp_store=FILE.
    db.execute('CREATE INDEX stage_order ON stage_events(event_at,event_id,payload)')
    unique_events=db.execute('SELECT count(*) FROM stage_events').fetchone()[0]
    return source_rows,duplicate_rows,unique_events

def _streaming_rebuild(db,as_of):
    """Drive the state machine from a server-side cursor, one row at a time.

    All lifecycle state is disk-backed in seen_times (the same-copy/same-second
    guard), active_loans (active copy state) and loans (every loan ID ever used,
    which also enforces the no-reuse rule). No sorted result is materialized in
    Python: the cursor yields exactly one row per iteration.
    """
    db.execute('CREATE TABLE seen_times('
               'copy_id TEXT NOT NULL,event_at TEXT NOT NULL,'
               'PRIMARY KEY(copy_id,event_at)) WITHOUT ROWID')
    db.execute('CREATE TABLE active_loans('
               'copy_id TEXT PRIMARY KEY,loan_id TEXT NOT NULL,'
               'borrowed_at TEXT NOT NULL,due TEXT NOT NULL) WITHOUT ROWID')
    # Internal state table; the published table is created later as `loans`.
    db.execute('CREATE TABLE state_loans('
               'loan_id TEXT PRIMARY KEY,copy_id TEXT NOT NULL,'
               'borrowed_at TEXT NOT NULL,due TEXT NOT NULL,'
               'returned_at TEXT,renewals INTEGER NOT NULL) WITHOUT ROWID')
    future_events=0
    # Single statement, covering index, default cursor (arraysize=1): no fetchall.
    cursor=db.execute('SELECT event_at,payload FROM stage_events ORDER BY event_at,event_id')
    for event_at,payload_text in cursor:
        event=json.loads(payload_text)
        kind=event['kind'];copy_id=event['copy_id'];loan_id=event['loan_id']
        if event_at>as_of:
            # Future events are retained and marked applied=0; they must not take
            # part in active-state validation.
            future_events+=1
            continue
        if db.execute('SELECT 1 FROM seen_times WHERE copy_id=? AND event_at=?',
                      (copy_id,event_at)).fetchone() is not None:
            raise ValueError('ambiguous simultaneous events for copy')
        db.execute('INSERT INTO seen_times(copy_id,event_at) VALUES (?,?)',(copy_id,event_at))
        if kind=='borrow':
            if db.execute('SELECT 1 FROM active_loans WHERE copy_id=?',(copy_id,)).fetchone() is not None:
                raise ValueError('copy already on loan')
            if db.execute('SELECT 1 FROM state_loans WHERE loan_id=?',(loan_id,)).fetchone() is not None:
                raise ValueError('loan ID reused')
            db.execute('INSERT INTO state_loans(loan_id,copy_id,borrowed_at,due,returned_at,renewals)'
                       ' VALUES (?,?,?,?,NULL,0)',
                       (loan_id,copy_id,event_at,event['due']))
            db.execute('INSERT INTO active_loans(copy_id,loan_id,borrowed_at,due) VALUES (?,?,?,?)',
                       (copy_id,loan_id,event_at,event['due']))
        else:
            row=db.execute('SELECT loan_id,due FROM active_loans WHERE copy_id=?',(copy_id,)).fetchone()
            if row is None or row[0]!=loan_id:
                raise ValueError('no matching active loan')
            if kind=='renew':
                if event['due']<=row[1]:
                    raise ValueError('renewal must extend due date')
                db.execute('UPDATE active_loans SET due=? WHERE copy_id=?',(event['due'],copy_id))
                db.execute('UPDATE state_loans SET due=?,renewals=renewals+1 WHERE loan_id=?',
                           (event['due'],loan_id))
            else:  # return
                db.execute('UPDATE state_loans SET returned_at=? WHERE loan_id=?',(event_at,loan_id))
                db.execute('DELETE FROM active_loans WHERE copy_id=?',(copy_id,))
    return future_events

def _materialize_outputs(db,as_of,source_rows,duplicate_rows,unique_events,future_events):
    """Build the published schema and compute status/overdue in one SQL pass."""
    db.execute('CREATE TABLE events(event_id TEXT PRIMARY KEY,event_at TEXT NOT NULL,'
               'applied INTEGER NOT NULL,payload TEXT NOT NULL)')
    db.execute('CREATE TABLE loans(loan_id TEXT PRIMARY KEY,copy_id TEXT NOT NULL,'
               'borrowed_at TEXT NOT NULL,due TEXT NOT NULL,returned_at TEXT,'
               'renewals INTEGER NOT NULL,overdue_days INTEGER NOT NULL,status TEXT NOT NULL)')
    db.execute('CREATE TABLE run_metadata(as_of TEXT NOT NULL,source_rows INTEGER,'
               'duplicate_rows INTEGER,future_events INTEGER)')
    db.execute('CREATE VIEW status_summary AS SELECT status,count(*) loans,'
               'sum(overdue_days>0) overdue_loans,sum(overdue_days) overdue_days,'
               'sum(renewals) renewals FROM loans GROUP BY status')

    db.execute('INSERT INTO events(event_id,event_at,applied,payload)'
               ' SELECT event_id,event_at,CASE WHEN event_at<=? THEN 1 ELSE 0 END,payload'
               ' FROM stage_events ORDER BY event_at,event_id',(as_of,))

    def day_diff(end_text,due_text):
        # Natural-day rule, identical to reconstruct.rebuild: the due day itself
        # is not overdue and the difference is calendar days, not a 24h window.
        return max(0,(_date.fromisoformat(end_text)-_date.fromisoformat(due_text)).days)
    db.create_function('day_diff',2,day_diff,deterministic=True)

    # End day = return day for closed loans, the as_of day for open loans.
    db.execute('INSERT INTO loans(loan_id,copy_id,borrowed_at,due,returned_at,'
               'renewals,overdue_days,status)'
               ' SELECT loan_id,copy_id,borrowed_at,due,returned_at,renewals,'
               'day_diff(COALESCE(substr(returned_at,1,10),?),due),'
               " CASE WHEN returned_at IS NULL THEN 'open' ELSE 'closed' END"
               ' FROM state_loans ORDER BY loan_id',(as_of[:10],))
    db.execute('INSERT INTO run_metadata VALUES (?,?,?,?)',
               (as_of,source_rows,duplicate_rows,future_events))
    summary=[dict(zip(('status','loans','overdue_loans','overdue_days','renewals'),r))
             for r in db.execute('SELECT * FROM status_summary ORDER BY status')]
    return {
        'as_of':as_of,
        'source_rows':source_rows,
        'duplicate_rows':duplicate_rows,
        'unique_events':unique_events,
        'future_events':future_events,
        'summary':summary,
    }

def run(source,as_of,target,work_memory_mb=WORK_MEMORY_MB_DEFAULT,temp_dir=None):
    """Rebuild the SQLite snapshot with bounded process memory.

    Public positional call and the returned result are identical to the original
    implementation; work_memory_mb and temp_dir are optional controls for the
    large-input staging path.
    """
    as_of=instant(as_of)
    work_memory_mb=_check_work_memory_mb(work_memory_mb)
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    work_dir=_resolve_temp_dir(temp_dir,target.parent)

    fd,temp_path=tempfile.mkstemp(prefix=_TEMP_PREFIX,suffix=_TEMP_SUFFIX,dir=work_dir)
    os.close(fd)
    temp=Path(temp_path)
    old_tmpdir=os.environ.get('SQLITE_TMPDIR')
    os.environ['SQLITE_TMPDIR']=str(work_dir)
    try:
        # isolation_level=None -> explicit transaction control; executescript is
        # avoided entirely because it force-commits any open transaction.
        with closing(sqlite3.connect(temp,isolation_level=None)) as db:
            _apply_limits(db,work_memory_mb)
            try:
                db.execute('BEGIN')
                source_rows,duplicate_rows,unique_events=_stage_events(db,source)
                future_events=_streaming_rebuild(db,as_of)
                result=_materialize_outputs(db,as_of,source_rows,duplicate_rows,
                                            unique_events,future_events)
                db.execute('COMMIT')
                # Shrink to the published schema; VACUUM must run outside a
                # transaction and spills via the same bounded temp store.
                for name in ('stage_events','seen_times','active_loans','state_loans'):
                    db.execute('DROP TABLE IF EXISTS '+name)
                db.execute('VACUUM')
            except BaseException:
                db.rollback()
                raise
        # Publish only after a clean close; the same-volume rename is atomic.
        os.replace(temp,target)
        return result
    finally:
        temp.unlink(missing_ok=True)
        if old_tmpdir is None:
            os.environ.pop('SQLITE_TMPDIR',None)
        else:
            os.environ['SQLITE_TMPDIR']=old_tmpdir

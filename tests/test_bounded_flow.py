"""Tests for the bounded-memory streaming pipeline.

Coverage groups:
  1. Fixture contract preserved (counts, applied flags, loan status/overdue, view).
  2. Ordering, duplicate/conflict and future-field semantics.
  3. State-machine rejections, all driven through the on-disk cursor path.
  4. Bounded-memory growth measured with tracemalloc and RUSAGE ru_maxrss at two
     scales, relative thresholds only (see test_memory_stays_bounded).
  5. Injected SQLite write/sort/publish failures: target bytes preserved, no
     temp residue, successful re-run afterwards.
  6. End-to-end CLI subprocess on a large shuffled input, compared row-by-row on
     stable keys against the independent in-memory reference.
"""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'tools'))

from circulation_flow.events import read_events  # noqa: E402
from circulation_flow.reconstruct import rebuild  # noqa: E402
from circulation_flow.pipeline import run  # noqa: E402
from generate_events import generate  # noqa: E402

FIX=ROOT/'fixtures/events.jsonl'
AS_OF='2026-01-12T00:00:00Z'
SEED=109001


def snapshot(db_path):
    with sqlite3.connect(db_path) as d:
        return {
            'events':d.execute('SELECT * FROM events ORDER BY event_id').fetchall(),
            'loans':d.execute('SELECT * FROM loans ORDER BY loan_id').fetchall(),
            'run_metadata':d.execute('SELECT * FROM run_metadata').fetchall(),
            'status_summary':d.execute('SELECT * FROM status_summary ORDER BY status').fetchall(),
            'schema':sorted((r[0],r[1]) for r in d.execute(
                "SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")),
        }


def reference_snapshot(source,as_of):
    """Independent in-memory reference via the unchanged public helpers."""
    events,count,duplicates=read_events(source)
    loans,future=rebuild(events,as_of)
    return {
        'events':sorted((e['event_id'],e['at'],int(e['at']<=as_of),
                         json.dumps(e,sort_keys=True)) for e in events),
        'loans':sorted((l['loan_id'],l['copy_id'],l['borrowed_at'],l['due'],
                        l['returned_at'],l['renewals'],l['overdue_days'],l['status'])
                       for l in loans),
        'run_metadata':[(as_of,count,duplicates,future)],
        'status_summary':None,  # compared via SQL of the produced database
    }


class FixtureContractTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'

    def test_fixture_counts_and_status(self):
        r=run(FIX,AS_OF,self.db)
        # 6 non-empty input lines, 1 duplicate, 5 unique events, 1 future event.
        self.assertEqual((r['source_rows'],r['duplicate_rows'],
                          r['unique_events'],r['future_events']),(6,1,5,1))
        # L1 closed and overdue by two days; L2 open and four days overdue at cutoff.
        with sqlite3.connect(self.db) as d:
            l1=d.execute("SELECT * FROM loans WHERE loan_id='L1'").fetchone()
            l2=d.execute("SELECT * FROM loans WHERE loan_id='L2'").fetchone()
            self.assertEqual(l1,('L1','B1','2026-01-01T00:00:00Z','2026-01-10',
                                 '2026-01-12T00:00:00Z',1,2,'closed'))
            self.assertEqual(l2,('L2','B2','2026-01-02T00:00:00Z','2026-01-08',
                                 None,0,4,'open'))
            self.assertEqual(d.execute('SELECT event_id,applied FROM events ORDER BY event_id').fetchall(),
                             [('E1',1),('E2',1),('E3',1),('E4',1),('E5',0)])
            self.assertEqual(d.execute('SELECT * FROM status_summary ORDER BY status').fetchall(),
                             [('closed',1,1,2,1),('open',1,1,4,0)])
            self.assertEqual(d.execute('SELECT * FROM run_metadata').fetchone(),
                             (AS_OF,6,1,1))
            # Published schema contains only the documented objects.
            self.assertEqual(sorted((r[0],r[1]) for r in d.execute(
                "SELECT type,name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")),
                [('table','events'),('table','loans'),
                 ('table','run_metadata'),('view','status_summary')])

    def test_matches_in_memory_reference_row_for_row(self):
        run(FIX,AS_OF,self.db)
        got=snapshot(self.db);ref=reference_snapshot(FIX,AS_OF)
        self.assertEqual(got['events'],ref['events'])
        self.assertEqual(got['loans'],ref['loans'])
        self.assertEqual(got['run_metadata'],ref['run_metadata'])


class OrderingAndDuplicationTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'
        self.events,_,_=read_events(FIX)

    def test_shuffled_input_produces_identical_business_rows(self):
        lines=FIX.read_text(encoding='utf-8').splitlines()
        shuffled=self.root/'shuffled.jsonl'
        shuffled.write_text('\n'.join(reversed(lines))+'\n',encoding='utf-8')
        run(FIX,AS_OF,self.db)
        db2=self.root/'out2.sqlite'
        run(shuffled,AS_OF,db2)
        self.assertEqual(snapshot(self.db),snapshot(db2))

    def test_canonical_duplicate_and_conflict_rules(self):
        # Byte-identical duplicate and an offset spelling normalizing to the same
        # UTC payload are both accepted and counted once.
        e0=self.events[0]
        spelled=dict(e0,at=_plus_one_spelling(e0['at']))
        dup_path=self.root/'dup.jsonl'
        dup_path.write_text(
            '\n'.join((json.dumps(e0),json.dumps(e0),json.dumps(spelled)))+'\n')
        r=run(dup_path,AS_OF,self.db)
        self.assertEqual((r['source_rows'],r['duplicate_rows'],r['unique_events']),(3,2,1))
        # Same event_id with a different normalized payload must fail immediately.
        conflict=dict(e0,due='2026-02-01')
        bad=self.root/'conflict.jsonl'
        bad.write_text(json.dumps(e0)+'\n'+json.dumps(conflict)+'\n')
        with self.assertRaisesRegex(ValueError,'conflicting event ID'):
            run(bad,AS_OF,self.db)

    def test_future_event_still_field_validated(self):
        future=dict(self.events[-1])  # E5 is already after the cutoff
        bad=dict(future,kind='return',due='2026-02-01')  # return must have null due
        p=self.root/'future-bad.jsonl'
        p.write_text(json.dumps(bad)+'\n')
        with self.assertRaisesRegex(ValueError,'return has no due date'):
            run(p,AS_OF,self.db)
        # Unknown kind and bad timestamps are likewise rejected before cutoff logic.
        for mutate,message in (
            (dict(future,kind='recall'),'unknown event kind'),
            (dict(future,at='2026-01-13T00:00:00'),'timestamp'),
            (dict(future,at='2026-01-13T00:00:00.5Z'),'timestamp'),
        ):
            p.write_text(json.dumps(mutate)+'\n')
            with self.assertRaisesRegex(ValueError,message):
                run(p,AS_OF,self.db)

    def test_blank_lines_and_empty_stream(self):
        p=self.root/'blanks.jsonl'
        p.write_text('\n  \n'+FIX.read_text(encoding='utf-8')+'\n\n')
        r=run(p,AS_OF,self.db)
        self.assertEqual(r['source_rows'],6)
        empty=self.root/'empty.jsonl';empty.write_text('\n  \n')
        with self.assertRaisesRegex(ValueError,'empty event stream'):
            run(empty,AS_OF,self.db)


def _plus_one_spelling(utc_iso):
    from datetime import datetime,timedelta
    dt=datetime.fromisoformat(utc_iso.replace('Z','+00:00'))+timedelta(hours=1)
    return dt.strftime('%Y-%m-%dT%H:%M:%S+01:00')


class StateMachineRejectionTest(unittest.TestCase):
    """Every rejection here flows through the disk-backed cursor state machine."""

    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'
        self.events,_,_=read_events(FIX)

    def _write(self,name,*events):
        p=self.root/name
        p.write_text('\n'.join(json.dumps(e) for e in events)+'\n')
        return p

    def test_double_borrow_same_copy(self):
        e0=self.events[0]
        other=dict(e0,event_id='EX1',loan_id='L9',at='2026-01-02T00:00:00Z')
        p=self._write('double.jsonl',e0,other)
        with self.assertRaisesRegex(ValueError,'copy already on loan'):
            run(p,AS_OF,self.db)

    def test_simultaneous_events_same_copy_same_second(self):
        e0=self.events[0]
        other=dict(e0,event_id='EX2',kind='renew',due='2026-01-09')
        p=self._write('simul.jsonl',e0,other)
        with self.assertRaisesRegex(ValueError,'ambiguous simultaneous events for copy'):
            run(p,AS_OF,self.db)

    def test_loan_id_reuse_on_borrow(self):
        # Reuse L1 on a different copy while L1 is still active (before its return).
        e0=self.events[0]
        reuse=dict(e0,event_id='EX3',copy_id='B9',at='2026-01-02T00:00:00Z',due='2026-01-20')
        p=self._write('reuse.jsonl',e0,reuse)
        with self.assertRaisesRegex(ValueError,'loan ID reused'):
            run(p,AS_OF,self.db)

    def test_renew_must_extend_due_date(self):
        e0=self.events[0]
        weak=dict(e0,event_id='EX4',kind='renew',at='2026-01-02T00:00:00Z',due='2026-01-05')
        p=self._write('weak.jsonl',e0,weak)
        with self.assertRaisesRegex(ValueError,'renewal must extend due date'):
            run(p,AS_OF,self.db)

    def test_return_without_matching_active_loan(self):
        ret=next(e for e in self.events if e['kind']=='return')
        # Return referencing an unknown loan id while the copy is out; a different
        # second keeps this on the matching path rather than the simultaneity guard.
        wrong=dict(ret,event_id='EX5',loan_id='LX',at='2026-01-12T01:00:00Z')
        p=self._write('wrong.jsonl',ret,wrong)
        with self.assertRaisesRegex(ValueError,'no matching active loan'):
            run(p,AS_OF,self.db)
        # A second return: placed one second before the real return it closes the
        # loan first, so the original return then finds no active loan.
        again=dict(ret,event_id='EX6',at='2026-01-11T23:59:59Z')
        p2=self._write('twice.jsonl',*self.events,again)
        with self.assertRaisesRegex(ValueError,'no matching active loan'):
            run(p2,AS_OF,self.db)

    def test_future_events_do_not_drive_state(self):
        # Two mutually conflicting borrows both after the cutoff: field-valid,
        # stored with applied=0, and no state-machine error is raised.
        a=dict(self.events[-1])
        b=dict(a,event_id='EF2',loan_id='L4',at='2026-01-14T00:00:00Z',due='2026-01-21')
        p=self._write('future.jsonl',a,b)
        r=run(p,AS_OF,self.db)
        self.assertEqual(r['future_events'],2)
        with sqlite3.connect(self.db) as d:
            self.assertEqual(d.execute('SELECT count(*) FROM events WHERE applied=0').fetchone()[0],2)
            self.assertEqual(d.execute('SELECT count(*) FROM loans').fetchone()[0],0)


class MemoryBoundTest(unittest.TestCase):
    """Relative growth test; thresholds are deliberately generous and machine-local."""

    def test_memory_stays_bounded(self):
        import resource
        import tracemalloc
        # Two scales with identical event density. Generation is seeded, so the
        # shape is deterministic; only the memory numbers are environment-specific.
        small=generate(self._path('small.jsonl'),2000,seed=SEED)
        large=generate(self._path('large.jsonl'),8000,seed=SEED+1)
        growth=large['unique_events']/small['unique_events']
        self.assertGreater(growth,3.5)  # confirm the ~4x input scale actually happened

        def measure(source):
            tracemalloc.start()
            rss0=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            run(source,'2027-01-01T00:00:00Z',self._path(source.stem+'.sqlite'),
                work_memory_mb=8)
            _,py_peak=tracemalloc.get_traced_memory()
            tracemalloc.stop()
            rss1=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return py_peak,max(0,rss1-rss0)

        py_small,rss_small=measure(self._path('small.jsonl'))
        py_large,rss_large=measure(self._path('large.jsonl'))
        # Python allocator peak must not scale with the input: the ~4x larger run
        # stays within a small constant slack of the smaller one.
        self.assertLessEqual(py_large,py_small*1.5+512*1024)
        # RSS (includes interpreter baseline and bounded SQLite cache) grows far
        # less than the 4x input growth. This is a relative slope assertion, not a
        # portable absolute RSS ceiling; absolute RSS varies by libc/SQLite build.
        self.assertLess(rss_large,rss_small*1.6+32*1024)

    def _path(self,name):
        if not hasattr(self,'_dir'):
            t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self._dir=Path(t.name)
        return self._dir/name


class FailureInjectionTest(unittest.TestCase):
    """Staging write, on-disk sort and atomic publish failures."""

    class _FailingConnection:
        """Proxy that raises on a chosen statement text; everything else delegates."""

        def __init__(self,real,fail_on,message):
            self._real=real;self._fail_on=fail_on;self._message=message

        def execute(self,sql,params=()):
            if self._fail_on in sql:
                raise sqlite3.OperationalError(self._message)
            return self._real.execute(sql,params)

        def __getattr__(self,name):
            return getattr(self._real,name)

    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'
        run(FIX,AS_OF,self.db)
        self.good_bytes=self.db.read_bytes()
        self.big=self.root/'big.jsonl'
        generate(self.big,400,seed=SEED)

    def _temp_residue(self):
        return [p.name for p in self.root.iterdir()
                if p.name.startswith('.circulation-') and p.name.endswith('.tmp')]

    def _fail_with(self,fail_on,message):
        real_connect=sqlite3.connect
        def patched(path,*args,**kwargs):
            conn=real_connect(path,*args,**kwargs)
            if str(path).startswith(str(self.root)) and str(path)!=str(self.db):
                return self._FailingConnection(conn,fail_on,message)
            return conn
        return patched

    def test_staging_write_failure_preserves_target(self):
        old=sqlite3.connect
        sqlite3.connect=self._fail_with('INSERT INTO stage_events','injected disk full')
        try:
            with self.assertRaisesRegex(sqlite3.Error,'injected disk full'):
                run(self.big,AS_OF,self.db)
        finally:
            sqlite3.connect=old
        self.assertEqual(self.db.read_bytes(),self.good_bytes)
        self.assertEqual(self._temp_residue(),[])
        # A corrected re-run succeeds directly.
        self.assertEqual(run(self.big,AS_OF,self.db)['source_rows'],
                         sum(1 for line in self.big.read_text().splitlines() if line.strip()))

    def test_sort_failure_preserves_target(self):
        old=sqlite3.connect
        sqlite3.connect=self._fail_with('CREATE INDEX stage_order','injected sort failure')
        try:
            with self.assertRaisesRegex(sqlite3.Error,'injected sort failure'):
                run(self.big,AS_OF,self.db)
        finally:
            sqlite3.connect=old
        self.assertEqual(self.db.read_bytes(),self.good_bytes)
        self.assertEqual(self._temp_residue(),[])
        run(self.big,AS_OF,self.db)  # direct re-run works

    def test_publish_failure_preserves_target(self):
        old_replace=os.replace
        def boom(src,dst):raise OSError('injected rename failure')
        os.replace=boom
        try:
            with self.assertRaisesRegex(OSError,'injected rename failure'):
                run(self.big,AS_OF,self.db)
        finally:
            os.replace=old_replace
        self.assertEqual(self.db.read_bytes(),self.good_bytes)
        self.assertEqual(self._temp_residue(),[])
        run(self.big,AS_OF,self.db)


class WorkMemoryAndTempDirTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'

    class _SpyConnection:
        """Records PRAGMA calls while delegating every operation to the real DB."""

        def __init__(self,real,record):
            self._real=real;self._record=record

        def execute(self,sql,params=()):
            cur=self._real.execute(sql,params)
            if sql.upper().startswith('PRAGMA'):
                self._record[sql]=cur.fetchall()
            return cur

        def __getattr__(self,name):
            return getattr(self._real,name)

    def test_budget_bounds_and_pragmas_are_applied(self):
        for bad in (3,257,0,12.5,True,'16',None):
            with self.assertRaises(ValueError):
                run(FIX,AS_OF,self.db,work_memory_mb=bad)
        applied={}
        real=sqlite3.connect
        def spy(path,*args,**kwargs):
            return self._SpyConnection(real(path,*args,**kwargs),applied)
        sqlite3.connect=spy
        try:
            run(FIX,AS_OF,self.db,work_memory_mb=12)
        finally:
            sqlite3.connect=real
        self.assertIn('PRAGMA cache_size=-12288',applied)
        self.assertIn('PRAGMA temp_store=FILE',applied)
        self.assertIn('PRAGMA mmap_size=0',applied)

    def test_temp_dir_defaults_next_to_target_and_accepts_explicit(self):
        work=self.root/'work';work.mkdir()
        run(FIX,AS_OF,self.db,temp_dir=work)
        self.assertTrue(self.db.exists())
        # Explicit temp dir on another filesystem is rejected before any work.
        if Path('/dev').exists() and os.stat('/dev').st_dev!=os.stat(self.root).st_dev:
            with self.assertRaisesRegex(ValueError,'same filesystem'):
                run(FIX,AS_OF,self.db,temp_dir='/dev')
        with self.assertRaises(OSError):
            run(FIX,AS_OF,self.db,temp_dir='/nonexistent-root-cannot-create/xyz')

    def test_repeatable_results(self):
        first=run(FIX,AS_OF,self.db,work_memory_mb=4)
        second=run(FIX,AS_OF,self.db,work_memory_mb=256)
        self.assertEqual(first,second)


class CliEndToEndTest(unittest.TestCase):
    def test_cli_subprocess_large_input_matches_reference(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        root=Path(t.name)
        source=root/'big.jsonl';db=root/'out.sqlite';work=root/'work';work.mkdir()
        stats=generate(source,1500,seed=SEED)
        env=dict(os.environ,PYTHONPATH=str(ROOT/'src'))
        # No -I here: the package is exercised from the source tree via PYTHONPATH;
        # the input itself is this test's own deterministic synthetic data.
        proc=subprocess.run(
            [sys.executable,'-m','circulation_flow.cli',
             '--source',str(source),'--as-of','2027-01-01T00:00:00Z',
             '--database',str(db),'--work-memory-mb','8','--temp-dir',str(work)],
            cwd=ROOT,env=env,capture_output=True,text=True,check=True)
        printed=json.loads(proc.stdout)
        self.assertEqual(printed['source_rows'],stats['source_rows'])
        self.assertEqual(printed['duplicate_rows'],stats['duplicate_rows'])
        self.assertEqual(printed['unique_events'],stats['unique_events'])
        ref=reference_snapshot(source,'2027-01-01T00:00:00Z')
        with sqlite3.connect(db) as d:
            self.assertEqual(d.execute('SELECT * FROM events ORDER BY event_id').fetchall(),
                             ref['events'])
            self.assertEqual(d.execute('SELECT * FROM loans ORDER BY loan_id').fetchall(),
                             ref['loans'])
            self.assertEqual(d.execute('SELECT * FROM run_metadata').fetchall(),
                             ref['run_metadata'])
            # View ordering and columns remain the documented contract.
            view=d.execute('SELECT * FROM status_summary ORDER BY status').fetchall()
            self.assertEqual([r[0] for r in view],sorted(r[0] for r in view))
            self.assertEqual(len(view[0]),5)
            self.assertEqual(d.execute('SELECT count(*) FROM events WHERE applied=0').fetchone()[0],
                             printed['future_events'])
        # No temp files are left behind in either directory.
        self.assertEqual([p.name for p in root.rglob('.circulation-*.tmp')],[])

    def test_cli_failure_exit_code_and_stderr(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        root=Path(t.name)
        bad=root/'bad.jsonl';bad.write_text('not-json\n')
        env=dict(os.environ,PYTHONPATH=str(ROOT/'src'))
        proc=subprocess.run(
            [sys.executable,'-m','circulation_flow.cli','--source',str(bad),
             '--as-of',AS_OF,'--database',str(root/'x.sqlite')],
            cwd=ROOT,env=env,capture_output=True,text=True)
        self.assertNotEqual(proc.returncode,0)
        # The decode failure is surfaced on stderr rather than swallowed.
        self.assertIn('Expecting value',proc.stderr)
        self.assertFalse((root/'x.sqlite').exists())


if __name__=='__main__':
    unittest.main()

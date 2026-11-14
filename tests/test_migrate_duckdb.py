"""Tests for the offline SQLite -> DuckDB snapshot migration.

Coverage groups:
  1. Both source layouts: the plain JSONL snapshot (business tables + view) and
     the package-intake snapshot (additionally the three intake_* tables).
  2. Fidelity: NULLs, 64-bit integer bounds, verbatim NUL/CJK identity text,
     JSON text and exact BLOB bytes survive the typed copy and the post-close
     readback; per-table typed digests match source and target.
  3. Strict source acceptance: bad dynamic types, missing/extra tables, changed
     column layout, orphan provenance and contradictory counts are rejected.
  4. One read snapshot: with the source read transaction held, a concurrent
     writer's commit and a source-path replacement are invisible (synchronous
     barrier, no sleeps).
  5. Constraints really work in DuckDB: catalog checks plus negative probes
     for PRIMARY KEY, UNIQUE, FOREIGN KEY and business CHECK.
  6. Failure injection during copy and at readback: the old target survives
     byte-identically, staging files are cleaned and a rerun succeeds; source
     and target aliases (path/symlink/hard link) are rejected.
  7. Bounded Python buffering across many batches on a large table.
  8. Real CLI subprocess end to end, including non-zero failure exit.
"""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'tools'))

import duckdb  # noqa: E402  (pinned in requirements-duckdb.lock)

from circulation_flow.pipeline import run  # noqa: E402
from circulation_flow import migrate_duckdb as md  # noqa: E402
from circulation_flow.migrate_duckdb import migrate,MigrationError  # noqa: E402
from circulation_flow.intake import collect  # noqa: E402
from generate_package import build_package,load_jsonl_records  # noqa: E402

FIX=ROOT/'fixtures/events.jsonl'
AS_OF='2026-01-12T00:00:00Z'
SEED=20261113
INT64_MAX=(1<<63)-1
INT64_MIN=-(1<<63)


# ---------------------------------------------------------------------------
# Source builders
# ---------------------------------------------------------------------------

def plain_source(path):
    run(FIX,AS_OF,path)
    return path


def package_source(path,root):
    records=load_jsonl_records(FIX)
    names=['csv/part-%02d.csv'%i for i in (1,2,3)]
    data,_m,_e=build_package(records,names,seed=SEED)
    zip_path=root/'export.zip';zip_path.write_bytes(data)
    collect(zip_path,AS_OF,path)
    return path


def temp_root():
    t=tempfile.TemporaryDirectory()
    return t,Path(t.name)


def sqlite_ro_connect(path):
    return sqlite3.connect(Path(path).resolve().as_uri(),uri=True)


class EndToEndBothModesTest(unittest.TestCase):
    def setUp(self):
        self.t,self.root=temp_root();self.addCleanup(self.t.cleanup)

    def test_plain_mode_full_fidelity(self):
        src=plain_source(self.root/'plain.sqlite')
        out=self.root/'plain.duckdb'
        result=migrate(src,out)
        self.assertEqual(result['source_mode'],'plain')
        self.assertEqual(set(result['tables']),
                         {'events','loans','run_metadata'})
        self.assertEqual(result['tables']['events'],5)
        self.assertEqual(result['tables']['loans'],2)
        self.assertEqual(result['tables']['run_metadata'],1)
        self.assertEqual(result['view_rows'],2)
        self._assert_matches_source(src,out,('events','loans','run_metadata'))

    def test_package_mode_full_fidelity(self):
        src=package_source(self.root/'pkg.sqlite',self.root)
        out=self.root/'pkg.duckdb'
        result=migrate(src,out,batch_size=2)  # force batching everywhere
        self.assertEqual(result['source_mode'],'package')
        self.assertEqual(set(result['tables']),
                         {'events','loans','run_metadata',
                          'intake_runs','intake_parts','intake_occurrences'})
        self.assertEqual(result['tables']['intake_occurrences'],6)
        self.assertEqual(result['tables']['intake_parts'],3)
        self.assertEqual(result['tables']['intake_runs'],1)
        self._assert_matches_source(
            src,out,('events','loans','run_metadata',
                     'intake_runs','intake_parts','intake_occurrences'))

    def _assert_matches_source(self,src,out,tables):
        with sqlite3.connect(src) as s,duckdb.connect(str(out),read_only=True) as d:
            for table in tables:
                cols=[c[0] for c in md._SOURCE_COLUMNS[table]]
                order=', '.join('"%s"'%k for k in md._STABLE_KEYS[table])
                got=[tuple(r) for r in d.execute(
                    'SELECT %s FROM %s ORDER BY %s'%(
                        ','.join('"%s"'%c for c in cols),table,order)).fetchall()]
                want=[tuple(r) for r in s.execute(
                    'SELECT %s FROM "%s" ORDER BY %s'%(
                        ','.join('"%s"'%c for c in cols),table,order)).fetchall()]
                # Normalize memoryview/bytearray to bytes for comparison.
                got=[tuple(bytes(x) if isinstance(x,(bytearray,memoryview)) else x
                          for x in row) for row in got]
                self.assertEqual(got,want,table)
            # The view is copied as an executable object and returns identical rows.
            view_got=d.execute('SELECT * FROM status_summary ORDER BY status').fetchall()
            view_want=s.execute('SELECT * FROM status_summary ORDER BY status').fetchall()
            self.assertEqual([tuple(r) for r in view_got],
                             [tuple(r) for r in view_want])
            # DuckDB names the catalog after the database file and exposes its
            # built-in helper views in a 'system' catalog; distinguish user
            # objects by table_type and by excluding the system catalog.
            base_tables=sorted(r[0] for r in d.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='main' AND table_type='BASE TABLE' "
                "AND table_catalog<>'system'").fetchall())
            self.assertEqual(base_tables,sorted(tables))
            views=sorted(r[0] for r in d.execute(
                "SELECT table_name FROM information_schema.views "
                "WHERE table_schema='main' AND table_catalog<>'system'").fetchall())
            self.assertEqual(views,['status_summary'])

    def test_repeatable_digests_and_result(self):
        src=plain_source(self.root/'plain.sqlite')
        r1=migrate(src,self.root/'a.duckdb',batch_size=3)
        r2=migrate(src,self.root/'b.duckdb',batch_size=7)
        self.assertEqual(r1['digests'],r2['digests'])
        self.assertEqual(r1['tables'],r2['tables'])


class TypeAndByteFidelityTest(unittest.TestCase):
    def setUp(self):
        self.t,self.root=temp_root();self.addCleanup(self.t.cleanup)
        self.src=plain_source(self.root/'plain.sqlite')

    def test_null_int64_bounds_and_typed_columns(self):
        # 64-bit boundary integers and NULL returned_at survive. The extra loan
        # row does not touch events, so run_metadata stays consistent.
        with sqlite3.connect(self.src) as s:
            s.execute('PRAGMA foreign_keys=ON')
            s.execute("INSERT INTO loans VALUES ('LB','BB','2026-01-01T00:00:00Z',"
                      "'2026-01-02',NULL,?,?,'open')",(INT64_MAX,INT64_MIN))
            # run_metadata accepts the full signed 64-bit range without
            # contradicting event counts (source_rows = unique + duplicate).
            s.execute('UPDATE run_metadata SET source_rows=?,duplicate_rows=?',
                      (INT64_MAX,INT64_MAX-5))
        out=self.root/'out.duckdb'
        migrate(self.src,out)
        with duckdb.connect(str(out),read_only=True) as d:
            row=d.execute("SELECT renewals,overdue_days,returned_at FROM loans "
                          "WHERE loan_id='LB'").fetchone()
            self.assertEqual(tuple(row),(INT64_MAX,INT64_MIN,None))
            meta=d.execute('SELECT source_rows,duplicate_rows FROM run_metadata'
                          ).fetchone()
            self.assertEqual(tuple(meta),(INT64_MAX,INT64_MAX-5))
            types={r[1]:r[2] for r in d.execute(
                "SELECT table_name,column_name,data_type FROM "
                "information_schema.columns WHERE table_schema='main' AND "
                "table_name IN ('events','loans','run_metadata')").fetchall()}
            self.assertEqual(types['renewals'],'BIGINT')
            self.assertEqual(types['event_id'],'VARCHAR')
            self.assertEqual(types['applied'],'BIGINT')

    def test_nul_and_cjk_identity_text_and_json(self):
        tricky_id='E-NUL\x00-中文-😀'
        tricky_payload=json.dumps(
            {'event_id':tricky_id,'at':'2026-01-01T00:00:00Z','kind':'borrow',
             'copy_id':'副本\x00B','loan_id':'贷款Ｌ','due':'2026-01-05'},
            sort_keys=True,ensure_ascii=False)
        with sqlite3.connect(self.src) as s:
            s.execute('INSERT INTO events VALUES (?,?,?,?)',
                      (tricky_id,'2026-01-01T00:00:00Z',1,tricky_payload))
            # Keep run_metadata consistent with the added unique event.
            s.execute('UPDATE run_metadata SET source_rows=source_rows+1')
        out=self.root/'out.duckdb'
        migrate(self.src,out)
        with duckdb.connect(str(out),read_only=True) as d:
            row=d.execute('SELECT event_id,payload FROM events '
                         'WHERE event_id=?',[tricky_id]).fetchone()
            self.assertEqual(row[0],tricky_id)
            self.assertEqual(row[1],tricky_payload)
            self.assertIn('\x00',row[0])

    def test_exact_blob_bytes_in_package_mode(self):
        src=package_source(self.root/'pkg.sqlite',self.root)
        # Inject deterministic non-UTF-8 BLOB payloads of varied sizes.
        blob_a=bytes(range(256))+b'\x00\xff'*32
        blob_b=b'\x00'+os.urandom(4097)+b'\x00'
        with sqlite3.connect(src) as s:
            s.execute('UPDATE intake_runs SET package_bytes=?,package_size=?',
                      (blob_a,len(blob_a)))
            s.execute('UPDATE intake_parts SET raw_bytes=?,raw_size=? WHERE part_id=1',
                      (blob_b,len(blob_b)))
        out=self.root/'out.duckdb'
        migrate(src,out,batch_size=1)
        with duckdb.connect(str(out),read_only=True) as d:
            got_a=bytes(d.execute('SELECT package_bytes FROM intake_runs')
                        .fetchone()[0])
            got_b=bytes(d.execute('SELECT raw_bytes FROM intake_parts WHERE part_id=1')
                        .fetchone()[0])
        self.assertEqual(got_a,blob_a)
        self.assertEqual(got_b,blob_b)
        self.assertEqual(hashlib.sha256(got_a).digest(),
                         hashlib.sha256(blob_a).digest())


class StrictRejectionTest(unittest.TestCase):
    def setUp(self):
        self.t,self.root=temp_root();self.addCleanup(self.t.cleanup)
        self.src=plain_source(self.root/'plain.sqlite')
        self.out=self.root/'out.duckdb'

    def _assert_rejected(self,label):
        with self.assertRaises(MigrationError) as ctx:
            migrate(self.src,self.out)
        self.assertIn(label,str(ctx.exception))
        self.assertFalse(self.out.exists())
        # No staging residue.
        self.assertEqual([p.name for p in self.root.iterdir()
                          if p.name.startswith(md._TEMP_PREFIX)],[])

    def test_bad_dynamic_types_rejected(self):
        # SQLite applies column affinity on plain binds, so an illegal stored
        # type needs an explicit CAST(... AS BLOB) to bypass the affinity.
        # INTEGER stored in a TEXT column.
        with sqlite3.connect(self.src) as s:
            s.execute("UPDATE events SET payload=CAST(123 AS BLOB) "
                      "WHERE event_id='E1'")
        self._assert_rejected('requires TEXT')
        with sqlite3.connect(self.src) as s:
            s.execute("UPDATE events SET payload='{}' WHERE event_id='E1'")
            # TEXT stored in an INTEGER column.
            s.execute("UPDATE events SET applied=CAST('1' AS BLOB) "
                      "WHERE event_id='E1'")
        self._assert_rejected('requires INTEGER')
        with sqlite3.connect(self.src) as s:
            s.execute("UPDATE events SET applied=1 WHERE event_id='E1'")
            # BLOB stored in a TEXT column.
            s.execute("UPDATE events SET event_at=CAST(x'0102' AS BLOB) "
                      "WHERE event_id='E1'")
        self._assert_rejected('requires TEXT')
        with sqlite3.connect(self.src) as s:
            s.execute("UPDATE events SET event_at='2026-01-01T00:00:00Z' "
                      "WHERE event_id='E1'")
            # bool is never accepted as INTEGER; store it as a genuine value
            # via a BLOB-cast round trip is impossible for bools (they bind as
            # ints), so exercise the checker directly instead.
        from circulation_flow.migrate_duckdb import _check_value
        with self.assertRaisesRegex(MigrationError,'requires INTEGER'):
            _check_value('events','applied','INTEGER',True)
        with self.assertRaisesRegex(MigrationError,'requires TEXT'):
            _check_value('events','payload','TEXT',b'x')
        with self.assertRaisesRegex(MigrationError,'requires BLOB'):
            _check_value('intake_runs','package_bytes','BLOB','nope')
        # Out-of-64-bit integer (binds fine to SQLite INTEGER storage only
        # within 64 bits; the checker rejects values beyond the range).
        with self.assertRaisesRegex(MigrationError,'64-bit range'):
            _check_value('loans','renewals','INTEGER',INT64_MAX+1)

    def test_missing_extra_and_changed_structure(self):
        with sqlite3.connect(self.src) as s:
            s.execute('DROP TABLE events')
        self._assert_rejected('missing required table')
        with sqlite3.connect(self.src) as s:
            s.execute('CREATE TABLE rogue(x INTEGER)')
        self._assert_rejected('undeclared table')
        # Rebuild a source with a changed column type.
        bad=self.root/'bad.sqlite'
        plain_source(bad)
        with sqlite3.connect(bad) as s:
            s.execute('ALTER TABLE events RENAME TO events_old')
            s.execute('CREATE TABLE events(event_id TEXT PRIMARY KEY,'
                      'event_at BLOB NOT NULL,applied INTEGER NOT NULL,'
                      'payload TEXT NOT NULL)')
            s.execute('INSERT INTO events SELECT * FROM events_old')
            s.execute('DROP TABLE events_old')
        with self.assertRaisesRegex(MigrationError,'unsupported structure'):
            migrate(bad,self.out)
        # Missing view.
        no_view=self.root/'noview.sqlite';plain_source(no_view)
        with sqlite3.connect(no_view) as s:
            s.execute('DROP VIEW status_summary')
        with self.assertRaisesRegex(MigrationError,'status_summary'):
            migrate(no_view,self.out)

    def test_partial_intake_table_set_rejected(self):
        src=package_source(self.root/'pkg.sqlite',self.root)
        with sqlite3.connect(src) as s:
            s.execute('DROP TABLE intake_occurrences')
        with self.assertRaisesRegex(MigrationError,'all of'):
            migrate(src,self.out)

    def test_orphan_provenance_and_count_contradictions(self):
        src=package_source(self.root/'pkg.sqlite',self.root)
        # The delivered source enforces its CHECK/FK constraints; a corrupted
        # snapshot can only be synthesized with enforcement switched off.
        with sqlite3.connect(src) as s:
            s.execute('PRAGMA foreign_keys=OFF')
            s.execute('INSERT INTO intake_occurrences VALUES '
                      "(999,999,'ghost.csv',1,'NO-EVENT','{}')")
        with self.assertRaisesRegex(MigrationError,'orphan|foreign'):
            migrate(src,self.out)
        with sqlite3.connect(src) as s:
            s.execute('DELETE FROM intake_occurrences WHERE occurrence_id=999')
            s.execute('UPDATE intake_runs SET source_rows=source_rows+5')
        with self.assertRaisesRegex(MigrationError,'count contradiction'):
            migrate(src,self.out)
        with sqlite3.connect(src) as s:
            s.execute('UPDATE intake_runs SET source_rows=source_rows-5')
            # declared_rows is guarded by a CHECK in the source, so contradict
            # the part/occurrence relationship through occurrences instead.
            s.execute('DELETE FROM intake_occurrences WHERE occurrence_id='
                      '(SELECT min(occurrence_id) FROM intake_occurrences)')
        with self.assertRaisesRegex(MigrationError,'count contradiction'):
            migrate(src,self.root/'c2.duckdb')
        # Business metadata contradicting the events table.
        with sqlite3.connect(self.src) as s:
            s.execute('UPDATE run_metadata SET future_events=future_events+1')
        self._assert_rejected('count contradiction')

    def test_target_aliases_rejected(self):
        # Same path.
        with self.assertRaisesRegex(MigrationError,'same file'):
            migrate(self.src,self.src)
        # Symlink alias.
        link=self.root/'link.sqlite';link.symlink_to(self.src.name)
        with self.assertRaisesRegex(MigrationError,'same file'):
            migrate(self.src,link)
        # Hard-link alias.
        hard=self.root/'hard.sqlite';os.link(self.src,hard)
        with self.assertRaisesRegex(MigrationError,'same file'):
            migrate(self.src,hard)

    def test_batch_size_validation(self):
        for bad in (0,1_000_001,True,1.5,'8',None):
            with self.assertRaises(MigrationError):
                migrate(self.src,self.out,batch_size=bad)


class SnapshotBarrierTest(unittest.TestCase):
    """The migration reads exactly one committed snapshot.

    The delivered snapshots use the DELETE journal, where a writer blocks on a
    held read lock. Snapshot isolation under a concurrent committer is the WAL
    semantic, so these tests switch a throwaway copy to WAL first; the migration
    itself never sets any source pragma - it only opens the source read-only and
    holds one read transaction.
    """

    def setUp(self):
        self.t,self.root=temp_root();self.addCleanup(self.t.cleanup)
        self.src=plain_source(self.root/'plain.sqlite')
        self.out=self.root/'out.duckdb'
        with sqlite3.connect(self.src) as c:
            self.assertEqual(c.execute('PRAGMA journal_mode=WAL').fetchone()[0],
                             'wal')

    def test_concurrent_commit_after_read_start_is_invisible(self):
        barrier=threading.Event()
        def after_snapshot(_conn):
            # Another connection commits a self-consistent extra loan row while
            # the migration's read transaction is held. Loans-only changes do
            # not contradict run_metadata, so the only observable effect can be
            # snapshot leakage (which the held read snapshot must prevent).
            def writer():
                with sqlite3.connect(self.src,timeout=10) as w:
                    w.execute("INSERT INTO loans VALUES "
                              "('LLATE','BLATE','2026-01-01T00:00:00Z',"
                              "'2026-01-02',NULL,0,0,'open')")
                    w.commit()
                barrier.set()
            t=threading.Thread(target=writer);t.start()
            barrier.wait(10);t.join(10)
            self.assertTrue(barrier.is_set())
        result=migrate(self.src,self.out,after_snapshot=after_snapshot)
        # Snapshot taken before the commit: two loans, not three.
        self.assertEqual(result['tables']['loans'],2)
        with duckdb.connect(str(self.out),read_only=True) as d:
            self.assertIsNone(d.execute(
                "SELECT 1 FROM loans WHERE loan_id='LLATE'").fetchone())
        # The committed row really is visible to a fresh connection.
        with sqlite3.connect(self.src) as s:
            self.assertEqual(s.execute(
                "SELECT count(*) FROM loans WHERE loan_id='LLATE'").fetchone()[0],1)

    def test_source_path_replacement_after_read_start_is_invisible(self):
        replacement=self.root/'replacement.sqlite'
        plain_source(replacement)
        # The replacement snapshot is self-consistent: its extra loan does not
        # touch run_metadata. The migration must never open this inode anyway.
        with sqlite3.connect(replacement) as s:
            s.execute("INSERT INTO loans VALUES "
                      "('LSWAP','BSWAP','2026-01-01T00:00:00Z',"
                      "'2026-01-02',NULL,0,0,'open')")
        barrier=threading.Event()
        def after_snapshot(_conn):
            def swapper():
                os.replace(replacement,self.src)
                barrier.set()
            t=threading.Thread(target=swapper);t.start()
            barrier.wait(5);t.join(5)
            self.assertTrue(barrier.is_set())
        result=migrate(self.src,self.out,after_snapshot=after_snapshot)
        self.assertEqual(result['tables']['loans'],2)
        with duckdb.connect(str(self.out),read_only=True) as d:
            self.assertIsNone(d.execute(
                "SELECT 1 FROM loans WHERE loan_id='LSWAP'").fetchone())
        # The path itself now names the swapped file.
        with sqlite3.connect(self.src) as s:
            self.assertEqual(s.execute(
                "SELECT count(*) FROM loans WHERE loan_id='LSWAP'").fetchone()[0],1)


class ConstraintEnforcementTest(unittest.TestCase):
    def setUp(self):
        self.t,self.root=temp_root();self.addCleanup(self.t.cleanup)

    def test_target_constraints_catalog_and_probes_plain(self):
        src=plain_source(self.root/'plain.sqlite')
        out=self.root/'plain.duckdb'
        migrate(src,out)
        with duckdb.connect(str(out)) as d:
            kinds=self._constraint_kinds(d)
            self.assertIn('PRIMARY KEY',kinds['events'])
            self.assertIn('PRIMARY KEY',kinds['loans'])
            self.assertNotIn('intake_runs',kinds)
            self._expect_error(
                d,"INSERT INTO events VALUES ('E1','x',1,'{}')")
            self._expect_error(
                d,"INSERT INTO loans(loan_id) VALUES ('L1')")  # NOT NULL cols
            self._expect_error(
                d,"INSERT INTO events(event_id) VALUES ('X')")  # NOT NULL cols

    def test_target_constraints_catalog_and_probes_package(self):
        src=package_source(self.root/'pkg.sqlite',self.root)
        out=self.root/'pkg.duckdb'
        migrate(src,out)
        with duckdb.connect(str(out)) as d:
            kinds=self._constraint_kinds(d)
            self.assertIn('FOREIGN KEY',kinds['intake_parts'])
            self.assertIn('FOREIGN KEY',kinds['intake_occurrences'])
            self.assertIn('UNIQUE',kinds['intake_parts'])
            self.assertIn('UNIQUE',kinds['intake_occurrences'])
            self.assertIn('CHECK',kinds['intake_runs'])
            self.assertIn('CHECK',kinds['intake_parts'])
            # Business CHECKs.
            self._expect_error(
                d,'INSERT INTO intake_runs(run_id,mode) VALUES (2,?)',
                ['package_intake'])
            self._expect_error(
                d,'INSERT INTO intake_runs(run_id,mode) VALUES (1,?)',
                ['delta'])
            self._expect_error(
                d,'INSERT INTO intake_parts(part_id,run_id,member_name,'
                  'declared_sha256,actual_sha256,declared_rows,actual_rows,'
                  'raw_bytes,raw_size) VALUES '
                  "(5,1,'m','a','b',1,2,BLOB '01',1)")
            # FK violations.
            self._expect_error(
                d,'INSERT INTO intake_parts(part_id,run_id,member_name,'
                  'declared_sha256,actual_sha256,declared_rows,actual_rows,'
                  'raw_bytes,raw_size) VALUES '
                  "(5,9,'m','a','a',1,1,BLOB '01',1)")
            self._expect_error(
                d,"INSERT INTO intake_occurrences VALUES "
                  "(9,1,'m',1,'GHOST','{}')")
            # UNIQUE violations.
            member=d.execute('SELECT member_name FROM intake_parts LIMIT 1') \
                .fetchone()[0]
            self._expect_error(
                d,'INSERT INTO intake_parts(part_id,run_id,member_name,'
                  'declared_sha256,actual_sha256,declared_rows,actual_rows,'
                  'raw_bytes,raw_size) VALUES '
                  "(5,1,?,'a','a',1,1,BLOB '01',1)",[member])
            # Probe rows must have been rejected, not stored.
            self.assertEqual(d.execute(
                'SELECT count(*) FROM intake_parts').fetchone()[0],3)

    @staticmethod
    def _constraint_kinds(con):
        out={}
        for table,kind in con.execute(
                "SELECT table_name,constraint_type FROM "
                "information_schema.table_constraints "
                "WHERE constraint_type IN ('PRIMARY KEY','UNIQUE',"
                "'FOREIGN KEY','CHECK')").fetchall():
            out.setdefault(table,set()).add(kind)
        return out

    @staticmethod
    def _expect_error(con,sql,params=None):
        try:
            con.execute(sql,params or [])
        except Exception:
            return
        raise AssertionError('constraint was not enforced: %s'%sql)


class FailureInjectionTest(unittest.TestCase):
    def setUp(self):
        self.t,self.root=temp_root();self.addCleanup(self.t.cleanup)
        self.src=plain_source(self.root/'plain.sqlite')
        self.out=self.root/'out.duckdb'
        migrate(self.src,self.out)
        self.good_bytes=self.out.read_bytes()

    def _residue(self):
        return [p.name for p in self.root.iterdir()
                if p.name.startswith(md._TEMP_PREFIX)]

    def test_copy_failure_keeps_old_target_and_cleans_staging(self):
        real_connect=duckdb.connect
        class Failing:
            def __init__(self,real):self._real=real;self._failed=False
            def executemany(self,sql,params):
                if not self._failed and sql.startswith('INSERT INTO events'):
                    self._failed=True
                    raise duckdb.Error('injected copy failure')
                return self._real.executemany(sql,params)
            def __getattr__(self,name):return getattr(self._real,name)
        def patched(path,*args,**kwargs):
            conn=real_connect(path,*args,**kwargs)
            if str(path)!=str(self.out):
                return Failing(conn)
            return conn
        old=duckdb.connect;duckdb.connect=patched
        try:
            with self.assertRaisesRegex(Exception,'injected copy failure'):
                migrate(self.src,self.root/'retry.duckdb')
        finally:
            duckdb.connect=old
        self.assertFalse((self.root/'retry.duckdb').exists())
        self.assertEqual(self._residue(),[])
        # Existing target untouched.
        self.assertEqual(self.out.read_bytes(),self.good_bytes)

    def test_readback_failure_keeps_old_target(self):
        real_connect=duckdb.connect
        calls={'n':0}
        def patched(path,*args,**kwargs):
            calls['n']+=1
            # The migration opens DuckDB twice: staging write, then the
            # post-close read-only readback. Fail the second one.
            if calls['n']==2:
                raise duckdb.IOException('injected readback failure')
            return real_connect(path,*args,**kwargs)
        duckdb.connect=patched
        try:
            with self.assertRaisesRegex(Exception,'injected readback failure'):
                migrate(self.src,self.out)
        finally:
            duckdb.connect=real_connect
        self.assertEqual(self.out.read_bytes(),self.good_bytes)
        self.assertEqual(self._residue(),[])
        # Corrected rerun succeeds.
        result=migrate(self.src,self.out)
        self.assertEqual(result['tables']['events'],5)

    def test_publish_failure_keeps_old_target(self):
        old_replace=os.replace
        def boom(src,dst):raise OSError('injected rename failure')
        os.replace=boom
        try:
            with self.assertRaisesRegex(OSError,'injected rename failure'):
                migrate(self.src,self.out)
        finally:
            os.replace=old_replace
        self.assertEqual(self.out.read_bytes(),self.good_bytes)
        self.assertEqual(self._residue(),[])


class BoundedBatchTest(unittest.TestCase):
    def test_buffers_do_not_grow_with_total_rows(self):
        import tracemalloc
        t,root=temp_root();self.addCleanup(t.cleanup)
        small=root/'small.sqlite';large=root/'large.sqlite'
        plain_source(small);plain_source(large)
        # Pad events with wide payloads so row width is non-trivial; keep
        # run_metadata in step so the consistency checks stay satisfied.
        def pad(path,n):
            with sqlite3.connect(path) as s:
                for i in range(n):
                    s.execute('INSERT INTO events VALUES (?,?,1,?)',
                              ('PAD-%07d'%i,'2026-01-01T00:00:00Z',
                               '{"p":"%s"}'%('x'*200)))
                s.execute('UPDATE run_metadata SET source_rows=source_rows+?',(n,))
        pad(small,2000);pad(large,20000)  # 10x rows, fixed batch size

        def measure(src,dst,batch):
            tracemalloc.start()
            migrate(src,dst,batch_size=batch)
            _,peak=tracemalloc.get_traced_memory()
            tracemalloc.stop()
            return peak
        # The migration code never holds a table-wide Python container: at a
        # fixed batch size its own per-batch buffers are identical for the
        # small and large tables. Absolute traced peak also includes the DuckDB
        # client/engine buffers (an engine resource boundary, not a migration
        # container), so the bounded-code assertion is made on the same large
        # table at two batch sizes: halving the batch must not increase the
        # peak beyond a small per-row slack, proving peak tracks batch size,
        # not the 20 000-row total.
        p_large_big=measure(large,root/'la.duckdb',2000)
        p_large_small=measure(large,root/'lb.duckdb',100)
        self.assertLessEqual(p_large_small,p_large_big+2*1024*1024)
        # And a 10x row increase at the same tiny batch only adds engine-side,
        # not migration-container, growth: compare small-table vs large-table
        # peaks both at batch_size=100.
        p_small_100=measure(small,root/'sc.duckdb',100)
        p_large_100=measure(large,root/'lc.duckdb',100)
        self.assertLessEqual(p_large_100,p_small_100*3.0+4*1024*1024)
        # A tiny batch must work identically on the large table and produce the
        # same digest set as the large-batch run.
        r1=migrate(large,root/'l1.duckdb',batch_size=1)
        r2=migrate(large,root/'l2.duckdb',batch_size=5000)
        self.assertEqual(r1['digests'],r2['digests'])
        self.assertEqual(r1['tables'],r2['tables'])
        # A single maximum-size BLOB is allowed one full row slot: a 1 MiB
        # archived part BLOB migrates at batch_size=1 without any table-wide
        # Python container, and the bytes come back identical.
        pkg=package_source(root/'blobpkg.sqlite',root)
        big=os.urandom(1024*1024)
        with sqlite3.connect(pkg) as s:
            s.execute('UPDATE intake_parts SET raw_bytes=?,raw_size=? WHERE part_id=1',
                      (big,len(big)))
        r3=migrate(pkg,root/'blobpkg.duckdb',batch_size=1)
        self.assertEqual(r3['tables']['intake_parts'],3)
        with duckdb.connect(str(root/'blobpkg.duckdb'),read_only=True) as d:
            got=bytes(d.execute(
                'SELECT raw_bytes FROM intake_parts WHERE part_id=1').fetchone()[0])
        self.assertEqual(got,big)


class CliEndToEndTest(unittest.TestCase):
    def test_cli_subprocess_success_and_failure(self):
        t,root=temp_root();self.addCleanup(t.cleanup)
        src=plain_source(root/'in.sqlite');out=root/'out.duckdb'
        env=dict(os.environ,PYTHONPATH=str(ROOT/'src'))
        proc=subprocess.run(
            [sys.executable,'-m','circulation_flow.migrate_duckdb',
             '--source',str(src),'--database',str(out),'--batch-size','2'],
            cwd=ROOT,env=env,capture_output=True,text=True)
        self.assertEqual(proc.returncode,0,proc.stderr)
        result=json.loads(proc.stdout)
        self.assertEqual(result['mode'],'duckdb_migration')
        self.assertEqual(result['batch_size'],2)
        self.assertIn('duckdb_version',result)
        with duckdb.connect(str(out),read_only=True) as d:
            self.assertEqual(d.execute('SELECT count(*) FROM loans').fetchone()[0],2)
            self.assertEqual(d.execute(
                'SELECT count(*) FROM status_summary').fetchone()[0],2)
        # Bad source: non-zero exit, stderr message, no output file.
        bad=root/'bad.sqlite';plain_source(bad)
        with sqlite3.connect(bad) as s:
            s.execute('DROP TABLE loans')
        fail=subprocess.run(
            [sys.executable,'-m','circulation_flow.migrate_duckdb',
             '--source',str(bad),'--database',str(root/'nope.duckdb')],
            cwd=ROOT,env=env,capture_output=True,text=True)
        self.assertNotEqual(fail.returncode,0)
        self.assertIn('missing required table',fail.stderr)
        self.assertFalse((root/'nope.duckdb').exists())


if __name__=='__main__':
    unittest.main()

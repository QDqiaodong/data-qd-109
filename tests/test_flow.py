import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from circulation_flow.events import read_events,instant
from circulation_flow.reconstruct import rebuild
from circulation_flow.pipeline import run
FIX=Path(__file__).resolve().parents[1]/'fixtures/events.jsonl'
class FlowTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.root=Path(t.name);self.db=self.root/'out.sqlite'
        self.events,_,_=read_events(FIX)
    def test_business(self):
        r=run(FIX,'2026-01-12T00:00:00Z',self.db)
        self.assertEqual((r['source_rows'],r['duplicate_rows'],r['unique_events'],r['future_events']),(6,1,5,1))
        self.assertEqual(r['summary'],[dict(status='closed',loans=1,overdue_loans=1,overdue_days=2,renewals=1),dict(status='open',loans=1,overdue_loans=1,overdue_days=4,renewals=0)])
        with sqlite3.connect(self.db) as d:self.assertEqual(d.execute('SELECT sum(applied) FROM events').fetchone()[0],4)
    def test_repeat(self):
        self.assertEqual(run(FIX,'2026-01-12T00:00:00Z',self.db),run(FIX,'2026-01-12T00:00:00Z',self.db))
    def test_conflicting_event(self):
        p=self.root/'bad.jsonl';p.write_text(FIX.read_text()+json.dumps(dict(self.events[0],due='2026-02-01'))+'\n')
        with self.assertRaisesRegex(ValueError,'conflicting'):read_events(p)
    def test_return_without_borrow(self):
        with self.assertRaisesRegex(ValueError,'matching'):rebuild([e for e in self.events if e['kind']=='return'],'2026-01-12T00:00:00Z')
    def test_double_borrow(self):
        e=dict(self.events[0],event_id='XX',loan_id='L9',at='2026-01-02T00:00:00Z')
        with self.assertRaisesRegex(ValueError,'already'):rebuild([self.events[0],e],'2026-01-12T00:00:00Z')
    def test_simultaneous(self):
        e=dict(self.events[0],event_id='XX',kind='renew')
        with self.assertRaisesRegex(ValueError,'simultaneous'):rebuild([self.events[0],e],'2026-01-12T00:00:00Z')
    def test_timezone_and_subseconds(self):
        self.assertEqual(instant('2026-01-01T01:00:00+01:00'),'2026-01-01T00:00:00Z')
        with self.assertRaises(ValueError):instant('2026-01-01T00:00:00')
        with self.assertRaises(ValueError):instant('2026-01-01T00:00:00+00:00:00.5')
        with self.assertRaises(ValueError):instant('2026-01-01T00:00:00+01:00:00.5')
        with self.assertRaises(ValueError):instant('2026-01-01T00:00:00.5Z')
        with self.assertRaises(ValueError):instant('2026-01-01T00:00:00+01:99')
        with self.assertRaises(ValueError):instant('2026-01-01T00:00:00-24:00')
    def test_failure_preserves_database(self):
        run(FIX,'2026-01-12T00:00:00Z',self.db);before=self.db.read_bytes()
        p=self.root/'bad.jsonl';p.write_text(json.dumps(next(e for e in self.events if e['kind']=='return'))+'\n')
        with self.assertRaises(ValueError):run(p,'2026-01-12T00:00:00Z',self.db)
        self.assertEqual(self.db.read_bytes(),before)
    def test_before_any_events(self):
        self.assertEqual(run(FIX,'2025-01-01T00:00:00Z',self.db)['summary'],[])

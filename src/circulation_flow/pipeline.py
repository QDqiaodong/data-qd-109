from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from .events import read_events,instant
from .reconstruct import rebuild

def run(source,as_of,target):
    as_of=instant(as_of);events,count,duplicates=read_events(source)
    loans,future=rebuild(events,as_of)
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    fd,temp=tempfile.mkstemp(prefix='.circulation-',suffix='.tmp',dir=target.parent);os.close(fd)
    try:
        with closing(sqlite3.connect(temp)) as db, db:
            db.executescript('''
            CREATE TABLE events(event_id TEXT PRIMARY KEY,event_at TEXT NOT NULL,applied INTEGER NOT NULL,payload TEXT NOT NULL);
            CREATE TABLE loans(loan_id TEXT PRIMARY KEY,copy_id TEXT NOT NULL,borrowed_at TEXT NOT NULL,due TEXT NOT NULL,
              returned_at TEXT,renewals INTEGER NOT NULL,overdue_days INTEGER NOT NULL,status TEXT NOT NULL);
            CREATE TABLE run_metadata(as_of TEXT NOT NULL,source_rows INTEGER,duplicate_rows INTEGER,future_events INTEGER);
            CREATE VIEW status_summary AS SELECT status,count(*) loans,sum(overdue_days>0) overdue_loans,
              sum(overdue_days) overdue_days,sum(renewals) renewals FROM loans GROUP BY status;
            ''')
            db.executemany('INSERT INTO events VALUES (?,?,?,?)',[(e['event_id'],e['at'],int(e['at']<=as_of),json.dumps(e,sort_keys=True)) for e in events])
            db.executemany('INSERT INTO loans VALUES (?,?,?,?,?,?,?,?)',[(l['loan_id'],l['copy_id'],l['borrowed_at'],l['due'],l['returned_at'],l['renewals'],l['overdue_days'],l['status']) for l in loans])
            db.execute('INSERT INTO run_metadata VALUES (?,?,?,?)',(as_of,count,duplicates,future))
            result={'as_of':as_of,'source_rows':count,'duplicate_rows':duplicates,'unique_events':len(events),'future_events':future,'summary':[dict(zip(['status','loans','overdue_loans','overdue_days','renewals'],r)) for r in db.execute('SELECT * FROM status_summary ORDER BY status')]}
        os.replace(temp,target);return result
    finally:Path(temp).unlink(missing_ok=True)

"""Peak-memory benchmark for the bounded pipeline versus the full-container path.

Run as a *separate subprocess* per configuration so each measurement starts from
a fresh interpreter and only one implementation is resident at a time:

    python3 -I tools/memory_benchmark.py pipeline  /tmp/big.jsonl <as_of> /tmp/o.sqlite
    python3 -I tools/memory_benchmark.py reference /tmp/big.jsonl <as_of> /tmp/o.sqlite

Two instruments are reported:
  * tracemalloc peak - Python-allocator allocations still owned by the process;
    this captures the growing event/loan dictionaries and lists directly;
  * resource.RUSAGE_SELF ru_maxrss - process RSS peak (KiB on Linux); this also
    includes the interpreter baseline and the SQLite page cache, so it is
    platform-dependent and is used only for relative comparison, never as a
    universal absolute limit.

The "reference" mode intentionally replays the original unbounded design: every
unique event is held in a dict and the full loan state in Python containers.
"""
import json
import os
import resource
import sys
import tempfile
import tracemalloc
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))


def _measure(fn):
    tracemalloc.start()
    rss_before=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    result=fn()
    _,py_peak=tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        'result':result,
        'python_peak_bytes':py_peak,
        'rss_peak_kib':rss_after,
        'rss_delta_kib':max(0,rss_after-rss_before),
    }


def run_pipeline(source,as_of,target,work_memory_mb):
    from circulation_flow.pipeline import run
    return _measure(lambda: run(source,as_of,target,work_memory_mb=work_memory_mb))


def run_reference(source,as_of,target):
    """Original unbounded design: full Python containers, then SQLite write."""
    import sqlite3
    from circulation_flow.events import read_events
    from circulation_flow.reconstruct import rebuild

    def _fn():
        events,count,duplicates=read_events(source)
        loans,future=rebuild(events,as_of)
        Path(target).parent.mkdir(parents=True,exist_ok=True)
        fd,tmp=tempfile.mkstemp(prefix='.ref-',suffix='.tmp',dir=Path(target).parent)
        os.close(fd)
        try:
            with sqlite3.connect(tmp) as db:
                db.executescript('''
                CREATE TABLE events(event_id TEXT PRIMARY KEY,event_at TEXT NOT NULL,applied INTEGER NOT NULL,payload TEXT NOT NULL);
                CREATE TABLE loans(loan_id TEXT PRIMARY KEY,copy_id TEXT NOT NULL,borrowed_at TEXT NOT NULL,due TEXT NOT NULL,
                  returned_at TEXT,renewals INTEGER NOT NULL,overdue_days INTEGER NOT NULL,status TEXT NOT NULL);
                CREATE TABLE run_metadata(as_of TEXT NOT NULL,source_rows INTEGER,duplicate_rows INTEGER,future_events INTEGER);
                CREATE VIEW status_summary AS SELECT status,count(*) loans,sum(overdue_days>0) overdue_loans,
                  sum(overdue_days) overdue_days,sum(renewals) renewals FROM loans GROUP BY status;
                ''')
                db.executemany('INSERT INTO events VALUES (?,?,?,?)',
                    [(e['event_id'],e['at'],int(e['at']<=as_of),json.dumps(e,sort_keys=True)) for e in events])
                db.executemany('INSERT INTO loans VALUES (?,?,?,?,?,?,?,?)',
                    [(l['loan_id'],l['copy_id'],l['borrowed_at'],l['due'],l['returned_at'],
                      l['renewals'],l['overdue_days'],l['status']) for l in loans])
                db.execute('INSERT INTO run_metadata VALUES (?,?,?,?)',(as_of,count,duplicates,future))
            os.replace(tmp,target)
            return {'source_rows':count,'duplicate_rows':duplicates,
                    'unique_events':len(events),'future_events':future}
        finally:
            Path(tmp).unlink(missing_ok=True)
    return _measure(_fn)


def main():
    mode,source,as_of,target=sys.argv[1:5]
    work_memory_mb=int(sys.argv[5]) if len(sys.argv)>5 else 16
    if mode=='pipeline':
        report=run_pipeline(source,as_of,target,work_memory_mb)
    elif mode=='reference':
        report=run_reference(source,as_of,target)
    else:
        raise SystemExit('mode must be pipeline or reference')
    print(json.dumps(report,sort_keys=True))


if __name__=='__main__':
    main()

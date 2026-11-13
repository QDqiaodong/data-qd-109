import argparse
import json
import sqlite3
from .pipeline import run,WORK_MEMORY_MB_MIN,WORK_MEMORY_MB_MAX,WORK_MEMORY_MB_DEFAULT

def main():
    p=argparse.ArgumentParser(description='Reconstruct copy circulation from a synthetic event stream')
    p.add_argument('--source',required=True)
    p.add_argument('--as-of',required=True)
    p.add_argument('--database',required=True)
    p.add_argument('--work-memory-mb',type=int,default=WORK_MEMORY_MB_DEFAULT,
                   help='SQLite page-cache/sort memory budget in MiB (integer %d..%d); '
                        'staging and spill space stays on disk'
                        %(WORK_MEMORY_MB_MIN,WORK_MEMORY_MB_MAX))
    p.add_argument('--temp-dir',default=None,
                   help='directory for the temporary staging database and SQLite spill files; '
                        'must be writable and on the same filesystem as --database '
                        '(defaults to the database directory)')
    a=p.parse_args()
    try:
        r=run(a.source,a.as_of,a.database,
              work_memory_mb=a.work_memory_mb,temp_dir=a.temp_dir)
    except (ValueError,OSError,sqlite3.Error) as e:
        p.exit(2,str(e)+'\n')
    print(json.dumps(r,sort_keys=True))
if __name__=='__main__':main()

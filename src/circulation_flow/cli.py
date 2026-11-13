import argparse
import json
import sqlite3
from .pipeline import run

def main():
    p=argparse.ArgumentParser(description='Reconstruct copy circulation from a synthetic event stream')
    p.add_argument('--source',required=True);p.add_argument('--as-of',required=True);p.add_argument('--database',required=True)
    a=p.parse_args()
    try:r=run(a.source,a.as_of,a.database)
    except (ValueError,OSError,sqlite3.Error) as e:p.exit(2,str(e)+'\n')
    print(json.dumps(r,sort_keys=True))
if __name__=='__main__':main()

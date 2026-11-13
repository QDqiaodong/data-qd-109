from datetime import datetime,timezone,date
import json
import re
from pathlib import Path

def instant(value):
    if not isinstance(value,str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])',value):
        raise ValueError('timestamp requires whole seconds and Z or +/-HH:MM offset')
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('timezone required')
    dt=dt.astimezone(timezone.utc)
    if dt.microsecond:raise ValueError('whole UTC seconds required')
    return dt.isoformat().replace('+00:00','Z')

def read_events(path):
    unique={};duplicate=0;count=0
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        if not line.strip():continue
        count+=1;r=json.loads(line)
        if not isinstance(r,dict) or set(r)!={'event_id','at','kind','copy_id','loan_id','due'}:raise ValueError('unexpected event fields')
        for k in ('event_id','copy_id','loan_id'):
            if not isinstance(r[k],str) or not r[k].strip():raise ValueError('empty event identity')
        r['at']=instant(r['at'])
        if r['kind'] not in ('borrow','renew','return'):raise ValueError('unknown event kind')
        if r['kind']=='return':
            if r['due'] is not None:raise ValueError('return has no due date')
        else:
            if not isinstance(r['due'],str) or date.fromisoformat(r['due']).isoformat()!=r['due']:raise ValueError('invalid due date')
            if r['due']<r['at'][:10]:raise ValueError('due before event day')
        key=r['event_id']
        if key in unique:
            if unique[key]!=r:raise ValueError('conflicting event ID')
            duplicate+=1
        else:unique[key]=r
    if not unique:raise ValueError('empty event stream')
    return sorted(unique.values(),key=lambda r:(r['at'],r['event_id'])),count,duplicate

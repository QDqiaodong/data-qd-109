from datetime import datetime,timezone,date
import json
import re
from pathlib import Path

_EVENT_FIELDS=frozenset(('event_id','at','kind','copy_id','loan_id','due'))
_IDENTITY_FIELDS=('event_id','copy_id','loan_id')
_KINDS=('borrow','renew','return')

def instant(value):
    if not isinstance(value,str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])',value):
        raise ValueError('timestamp requires whole seconds and Z or +/-HH:MM offset')
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('timezone required')
    dt=dt.astimezone(timezone.utc)
    if dt.microsecond:raise ValueError('whole UTC seconds required')
    return dt.isoformat().replace('+00:00','Z')

def decode_event(line):
    """Strictly decode and validate one JSONL line into a normalized event dict.

    Validation rules and error messages match the original read_events contract:
    exact field set, non-empty textual identities, whole-second ISO timestamps with
    Z or +/-HH:MM offsets, known kind and the due-date rules per kind.
    """
    r=json.loads(line)
    if not isinstance(r,dict) or set(r)!=_EVENT_FIELDS:raise ValueError('unexpected event fields')
    for k in _IDENTITY_FIELDS:
        if not isinstance(r[k],str) or not r[k].strip():raise ValueError('empty event identity')
    r['at']=instant(r['at'])
    if r['kind'] not in _KINDS:raise ValueError('unknown event kind')
    if r['kind']=='return':
        if r['due'] is not None:raise ValueError('return has no due date')
    else:
        if not isinstance(r['due'],str) or date.fromisoformat(r['due']).isoformat()!=r['due']:raise ValueError('invalid due date')
        if r['due']<r['at'][:10]:raise ValueError('due before event day')
    return r

def iter_events(path):
    """Yield normalized events from a JSONL file with a single sequential pass.

    Blank lines are ignored exactly as in read_events. The file handle and every
    parsed object are bounded to one input line; no container accumulates events.
    """
    with open(Path(path),encoding='utf-8') as fh:
        for line in fh:
            if not line.strip():continue
            yield decode_event(line)

def read_events(path):
    unique={};duplicate=0;count=0
    for r in iter_events(path):
        count+=1
        key=r['event_id']
        old=unique.get(key)
        if old is not None:
            if old!=r:raise ValueError('conflicting event ID')
            duplicate+=1
        else:unique[key]=r
    if not unique:raise ValueError('empty event stream')
    return sorted(unique.values(),key=lambda r:(r['at'],r['event_id'])),count,duplicate

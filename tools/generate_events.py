"""Deterministic synthetic JSONL generator for the bounded-memory pipeline.

This helper is deliberately outside the ``circulation_flow`` package: it only
produces fixture data for tests and the memory benchmark. Generation is seeded,
so the same arguments always produce the same bytes. Every emitted record is a
valid event under docs/contract.md; duplicates use either an identical line or
an equivalent timezone spelling that normalizes to the same UTC payload.

Typical use::

    python3 tools/generate_events.py /tmp/big.jsonl --copies 8000 --seed 109001
"""
from datetime import date,timedelta,datetime
import argparse
import json
import random

_BASE=date(2026,1,1)
_DEFAULT_AS_OF=date(2027,1,1)


def _iso(day,hour,minute,second):
    return '%sT%02d:%02d:%02dZ'%(day.isoformat(),hour,minute,second)


def _offset_spelling(utc_iso):
    """Rewrite a ...Z UTC timestamp as the same instant written with +01:00."""
    shifted=datetime.fromisoformat(utc_iso.replace('Z','+00:00'))+timedelta(hours=1)
    return shifted.strftime('%Y-%m-%dT%H:%M:%S+01:00')


def build_records(copies,*,seed=109001,future_copies=None,as_of=_DEFAULT_AS_OF):
    """Return (records, as_of) where records are validated event dictionaries."""
    rng=random.Random(seed)
    if future_copies is None:
        future_copies=max(1,copies//20)
    regular=copies-future_copies
    records=[]
    event_seq=0

    def next_id():
        nonlocal event_seq
        event_seq+=1
        return 'E%08d'%event_seq

    for index in range(copies):
        is_future=index>=regular
        copy_id='B%07d'%(index+1)
        loan_id='L%07d'%(index+1)
        if is_future:
            # All events of future copies lie strictly after the cutoff, so they
            # are stored with applied=0 and never touch the state machine.
            day=as_of+timedelta(days=rng.randrange(2,40))
        else:
            # Many copies share a day (distinct random clock times), so the whole
            # population fits inside the cutoff window and the on-disk loan state
            # grows with the input size rather than the timeline.
            day=_BASE+timedelta(days=rng.randrange(0,300))
        hour,minute,second=rng.randrange(24),rng.randrange(60),rng.randrange(60)
        due=day+timedelta(days=5)
        records.append({'event_id':next_id(),'at':_iso(day,hour,minute,second),
                        'kind':'borrow','copy_id':copy_id,'loan_id':loan_id,
                        'due':due.isoformat()})
        current_due=due
        for _ in range(rng.randrange(0,4)):
            day=day+timedelta(days=2)
            current_due=current_due+timedelta(days=rng.randrange(1,6))
            records.append({'event_id':next_id(),
                            'at':_iso(day,rng.randrange(24),rng.randrange(60),rng.randrange(60)),
                            'kind':'renew','copy_id':copy_id,'loan_id':loan_id,
                            'due':current_due.isoformat()})
        if not is_future and rng.random()<0.8:
            day=day+timedelta(days=rng.randrange(1,8))
            records.append({'event_id':next_id(),
                            'at':_iso(day,rng.randrange(24),rng.randrange(60),rng.randrange(60)),
                            'kind':'return','copy_id':copy_id,'loan_id':loan_id,'due':None})
    return records,as_of


def write_jsonl(path,records,*,seed=109001,duplicate_fraction=0.1,blank_fraction=0.02):
    """Shuffle, inject canonical duplicates and blank lines, then write JSONL."""
    rng=random.Random(seed^0x9E3779B9)
    ordered=list(records)
    rng.shuffle(ordered)
    lines=[json.dumps(r,separators=(',',':')) for r in ordered]
    unique=len(lines)
    duplicates=0
    chosen=rng.sample(range(unique),max(1,int(unique*duplicate_fraction)))
    extra=[]
    for pos,idx in enumerate(chosen):
        record=ordered[idx]
        if pos%2==0 and record['at'].endswith('Z'):
            spelled=dict(record)
            spelled['at']=_offset_spelling(record['at'])
            extra.append(json.dumps(spelled,separators=(',',':')))
        else:
            extra.append(lines[idx])  # byte-identical re-emission
        duplicates+=1
    rng.shuffle(extra)
    lines.extend(extra)
    rng.shuffle(lines)
    blanks=max(1,int((len(lines))*blank_fraction))
    for _ in range(blanks):
        lines.insert(rng.randrange(len(lines)+1),'')
    with open(path,'w',encoding='utf-8') as fh:
        fh.write('\n'.join(lines)+'\n')
    return {'unique_events':unique,'duplicate_rows':duplicates,
            'source_rows':unique+duplicates,'blank_lines':blanks}


def generate(path,copies,*,seed=109001,duplicate_fraction=0.1,blank_fraction=0.02,
             future_copies=None,as_of=_DEFAULT_AS_OF):
    records,cutoff=build_records(copies,seed=seed,future_copies=future_copies,as_of=as_of)
    stats=write_jsonl(path,records,seed=seed,duplicate_fraction=duplicate_fraction,
                      blank_fraction=blank_fraction)
    stats['as_of']=cutoff.isoformat()+'T00:00:00Z'
    stats['copies']=copies
    return stats


def main():
    p=argparse.ArgumentParser(description='Generate a deterministic large JSONL event file')
    p.add_argument('path')
    p.add_argument('--copies',type=int,default=8000)
    p.add_argument('--seed',type=int,default=109001)
    p.add_argument('--duplicate-fraction',type=float,default=0.1)
    p.add_argument('--blank-fraction',type=float,default=0.02)
    a=p.parse_args()
    stats=generate(a.path,a.copies,seed=a.seed,
                   duplicate_fraction=a.duplicate_fraction,
                   blank_fraction=a.blank_fraction)
    print(json.dumps(stats,sort_keys=True))


if __name__=='__main__':
    main()

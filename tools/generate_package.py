"""Deterministic synthetic vendor export-package generator.

Builds an original ZIP containing a root manifest.json and several borrow/return
CSV parts, the vendor format consumed by ``circulation_flow.intake``. The
records are synthesized from the same six-event JSONL fixture by default (or
from any JSONL file), shuffled across parts in an order that does not reflect
event chronology. Everything is deterministic given the seed: same inputs and
arguments always produce the same logical package and the same bytes.

The generator only writes synthetic data; it never reads real borrower records.

Typical use::

    python3 -I tools/generate_package.py run/export.zip \\
        --source fixtures/events.jsonl --parts 3 --seed 20261113
"""
import argparse
import csv
import hashlib
import io
import json
import random
import zipfile
from pathlib import Path

KIND_TO_ACTION={'borrow':'CHECKOUT','renew':'EXTEND','return':'CHECKIN'}
CSV_HEADER=['source_event_id','occurred_at','action','barcode',
            'loan_reference','due_on']
FIXED_DATE_TIME=(2026,1,1,0,0,0)


def load_jsonl_records(source):
    """Read JSONL event dicts, preserving the original timestamp spellings."""
    records=[]
    for line in Path(source).read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        r=json.loads(line)
        records.append({
            'source_event_id':r['event_id'],
            'occurred_at':r['at'],
            'action':KIND_TO_ACTION[r['kind']],
            'barcode':r['copy_id'],
            'loan_reference':r['loan_id'],
            'due_on':'' if r['due'] is None else r['due'],
        })
    return records


def csv_bytes(rows):
    buf=io.StringIO(newline='')
    writer=csv.writer(buf,lineterminator='\n',quoting=csv.QUOTE_MINIMAL)
    writer.writerow(CSV_HEADER)
    for row in rows:
        writer.writerow([row[h] for h in CSV_HEADER])
    return buf.getvalue().encode('utf-8')


def assign_parts(records,part_names,seed):
    """Shuffle records and distribute them across parts (parts may be empty)."""
    rng=random.Random(seed)
    ordered=list(records)
    rng.shuffle(ordered)
    buckets={name:[] for name in part_names}
    for index,record in enumerate(ordered):
        buckets[part_names[index%len(part_names)]].append(record)
    return buckets


def build_package(records,part_names,*,seed=20261113,
                  source_system='synthetic-vendor',export_id='EXP-SYNTH-0001'):
    """Return the complete ZIP bytes for one synthetic full export."""
    buckets=assign_parts(records,part_names,seed)
    part_entries=[]
    payloads=[]
    for name in part_names:
        data=csv_bytes(buckets[name])
        digest=hashlib.sha256(data).hexdigest()
        # record_index counts non-empty logical data records, excluding header.
        rows=len(buckets[name])
        part_entries.append({'path':name,'sha256':digest,'rows':rows})
        payloads.append((name,data))
    manifest={
        'schema_version':1,
        'source_system':source_system,
        'export_id':export_id,
        'export_kind':'full',
        'parts':part_entries,
    }
    manifest_bytes=(json.dumps(manifest,ensure_ascii=False,separators=(',',':'))
                    .encode('utf-8'))
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,'w',compression=zipfile.ZIP_DEFLATED) as zf:
        # Write members in the given (arbitrary) order; the manifest is a plain
        # root member like every other and carries fixed deterministic metadata.
        for name,data in payloads:
            info=zipfile.ZipInfo(name,date_time=FIXED_DATE_TIME)
            info.compress_type=zipfile.ZIP_DEFLATED
            info.create_system=3
            info.external_attr=0o100644<<16  # regular file, rw-r--r--
            zf.writestr(info,data)
        info=zipfile.ZipInfo('manifest.json',date_time=FIXED_DATE_TIME)
        info.compress_type=zipfile.ZIP_DEFLATED
        info.create_system=3
        info.external_attr=0o100644<<16
        zf.writestr(info,manifest_bytes)
    return buf.getvalue(),manifest,part_entries


def add_tricky_identity_record(records):
    """Append a record whose identity contains comma, quote and newline.

    It uses a fresh event id and its own loan so the existing lifecycle stays
    valid; the awkward characters only exercise quoting/escaping fidelity.
    """
    records=list(records)
    records.append({
        'source_event_id':'E6, "tricky"\nid',
        'occurred_at':'2026-01-12T00:00:00Z',
        'action':'CHECKOUT',
        'barcode':'B4, "quote"\nbarcode',
        'loan_reference':'L4, "ref"\nloan',
        'due_on':'2026-01-19',
    })
    return records


def main(argv=None):
    p=argparse.ArgumentParser(
        description='Generate a deterministic synthetic vendor export ZIP')
    p.add_argument('path',help='output ZIP path')
    p.add_argument('--source',
                   default=str(Path(__file__).resolve().parents[1]
                               /'fixtures/events.jsonl'),
                   help='JSONL events to convert into vendor CSV rows')
    p.add_argument('--parts',type=int,default=3,
                   help='number of CSV parts (records are distributed round-robin; '
                        'empty parts are possible)')
    p.add_argument('--seed',type=int,default=20261113)
    p.add_argument('--subdir',default='csv',
                   help='subdirectory prefix for CSV member names')
    p.add_argument('--tricky',action='store_true',
                   help='add a record whose identities contain comma, quote and newline')
    a=p.parse_args(argv)
    records=load_jsonl_records(a.source)
    if a.tricky:
        records=add_tricky_identity_record(records)
    part_names=['%s/part-%02d.csv'%(a.subdir,i+1) for i in range(a.parts)]
    data,manifest,entries=build_package(records,part_names,seed=a.seed)
    Path(a.path).parent.mkdir(parents=True,exist_ok=True)
    Path(a.path).write_bytes(data)
    print(json.dumps({
        'path':str(a.path),
        'package_sha256':hashlib.sha256(data).hexdigest(),
        'package_size':len(data),
        'parts':entries,
        'export_id':manifest['export_id'],
        'source_system':manifest['source_system'],
    },sort_keys=True))


if __name__=='__main__':
    main()

"""Vendor full-export package intake.

A vendor delivers one complete export as a ZIP instead of normalized JSONL:
a root ``manifest.json`` declaring several borrow/return CSV parts. This module
captures the ZIP bytes exactly once, validates the package as an all-or-nothing
unit, maps every vendor record onto the existing event contract, and publishes
a self-contained SQLite snapshot that keeps both the business tables and the
byte-level provenance needed to trace every event back to its source record.

Only the Python standard library and SQLite are used. There is no HTTP, no
cross-run ledger, no partial acceptance: any missing, malformed, conflicting or
over-budget member rejects the whole package and leaves the previous target
untouched.
"""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import re
import sqlite3
import zipfile
import zlib

from .events import decode_event,instant
from .pipeline import _build_database

DEFAULT_MAX_BYTES=67108864            # 64 MiB
MAX_BYTES_LIMIT=1073741824            # 1 GiB hard ceiling
MANIFEST_NAME='manifest.json'
CSV_HEADER=['source_event_id','occurred_at','action','barcode',
            'loan_reference','due_on']
_ACTION_MAP={'CHECKOUT':'borrow','EXTEND':'renew','CHECKIN':'return'}
_HEX64=re.compile(r'[0-9a-f]{64}\Z')
_DRIVE=re.compile(r'[A-Za-z]:')


class IntakeError(ValueError):
    """Package-level rejection; messages always name the member/record at fault."""


# ---------------------------------------------------------------------------
# Parameter and path guards
# ---------------------------------------------------------------------------

def _check_max_bytes(max_bytes):
    if isinstance(max_bytes,bool) or not isinstance(max_bytes,int) \
            or max_bytes<=0 or max_bytes>MAX_BYTES_LIMIT:
        raise IntakeError('max_bytes must be a positive integer not exceeding %d'
                          %MAX_BYTES_LIMIT)
    return max_bytes


def _reject_target_alias(package,target):
    """Fail when the target database could overwrite the input ZIP itself.

    Covers the plain path, symlink aliases (lexical realpath comparison, which
    also works when the target does not exist yet) and hard-link aliases (same
    device/inode after following links).
    """
    package=Path(package);target=Path(target)
    if not package.is_file():
        raise IntakeError('package is not a regular file: %s'%package)
    pkg_real=os.path.realpath(package)
    pkg_st=os.stat(package)
    if os.path.lexists(target):
        if os.path.realpath(target)==pkg_real:
            raise IntakeError('target database must not be the same file as the '
                              'input package (path or symlink alias)')
        try:
            t_st=os.stat(target)
        except OSError:
            t_st=None
        if t_st is not None and (t_st.st_dev,t_st.st_ino)==(pkg_st.st_dev,pkg_st.st_ino):
            raise IntakeError('target database must not be the same file as the '
                              'input package (hard-link alias)')


# ---------------------------------------------------------------------------
# Single capture
# ---------------------------------------------------------------------------

def read_package_bytes(package,max_bytes):
    """Capture the whole ZIP in one read, enforcing the raw-byte budget.

    The returned bytes are the only copy of the package ever inspected: the ZIP
    directory, member payloads, hashes and archived raw bytes all derive from
    them, so replacing the source path after capture cannot mix two versions.
    """
    max_bytes=_check_max_bytes(max_bytes)
    chunks=[];size=0
    with open(package,'rb') as fh:
        while True:
            chunk=fh.read(1024*1024)
            if not chunk:break
            chunks.append(chunk);size+=len(chunk)
            if size>max_bytes:
                raise IntakeError('package raw size %d exceeds max_bytes %d'
                                  %(size,max_bytes))
    return b''.join(chunks)


# ---------------------------------------------------------------------------
# Member-name rules: canonical relative POSIX paths only, never normalized away
# ---------------------------------------------------------------------------

def safe_member_name(name):
    if not isinstance(name,str) or not name:
        raise IntakeError('empty or non-text member name')
    if '\x00' in name:
        raise IntakeError('NUL byte in member name: %r'%name)
    if '\\' in name:
        raise IntakeError('backslash is not allowed in member name: %r'%name)
    if name.startswith('/'):
        raise IntakeError('absolute member name is not allowed: %r'%name)
    if _DRIVE.match(name):
        raise IntakeError('drive-letter member name is not allowed: %r'%name)
    for segment in name.split('/'):
        if segment in ('','.','..'):
            raise IntakeError('member name has empty, "." or ".." segment: %r'%name)
    return name


def _unix_mode(info):
    """Return the st_mode high bits for Unix entries, else 0 for DOS entries.

    ZIP creator system 3 is Unix; creator 0 is DOS, whose high bits hold no
    st_mode (DOS attributes live in the low byte), so DOS entries are regular
    files and cannot encode a symlink.
    """
    if info.create_system==3:
        return info.external_attr>>16
    return 0


def _is_symlink_member(info):
    mode=_unix_mode(info)
    return mode!=0 and (mode&0o170000)==0o120000


def _is_regular_member(info):
    mode=_unix_mode(info)
    if mode==0:
        # DOS entries, and Unix entries that store permission bits without the
        # S_IFREG type bit (as Python's convenience writestr does, mode 0o600),
        # are ordinary files.
        return True
    file_type=mode&0o170000
    # Explicit regular files and permission-only modes are accepted; any other
    # special type (directory, device, fifo, socket) is rejected.
    return file_type in (0o100000,0)


# ---------------------------------------------------------------------------
# Manifest parsing with strict typing and duplicate-key rejection
# ---------------------------------------------------------------------------

def _reject_duplicate_keys(pairs):
    seen=set()
    for key,_ in pairs:
        if key in seen:
            raise IntakeError('duplicate JSON object key: %r'%key)
        seen.add(key)
    return dict(pairs)


def _strict_int(value,label):
    if isinstance(value,bool) or not isinstance(value,int):
        raise IntakeError('manifest %s must be an integer'%label)
    return value


def _non_empty_text(value,label):
    if not isinstance(value,str) or not value or value!=value.strip():
        raise IntakeError('manifest %s must be a non-empty string without '
                          'leading or trailing whitespace'%label)
    return value


def parse_manifest(manifest_bytes):
    """Validate the manifest bytes and return (manifest dict, fingerprint)."""
    if manifest_bytes.startswith(b'\xef\xbb\xbf'):
        raise IntakeError('manifest must be UTF-8 without a BOM')
    try:
        text=manifest_bytes.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise IntakeError('manifest is not valid UTF-8: %s'%exc) from None
    try:
        manifest=json.loads(text,object_pairs_hook=_reject_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise IntakeError('manifest is not valid JSON: %s'%exc) from None
    if not isinstance(manifest,dict) or set(manifest)!={
            'schema_version','source_system','export_id','export_kind','parts'}:
        raise IntakeError('manifest top-level fields must be exactly '
                          'schema_version, source_system, export_id, '
                          'export_kind, parts')
    schema_version=_strict_int(manifest['schema_version'],'schema_version')
    if schema_version!=1:
        raise IntakeError('unsupported schema_version: %r'%schema_version)
    source_system=_non_empty_text(manifest['source_system'],'source_system')
    export_id=_non_empty_text(manifest['export_id'],'export_id')
    if manifest['export_kind']!='full':
        raise IntakeError("export_kind must be 'full'")
    parts=manifest['parts']
    if not isinstance(parts,list) or not parts:
        raise IntakeError('manifest parts must be a non-empty array')
    seen_paths=set()
    clean_parts=[]
    for index,part in enumerate(parts):
        where='manifest parts[%d]'%index
        if not isinstance(part,dict) or set(part)!={'path','sha256','rows'}:
            raise IntakeError('%s must have exactly path, sha256 and rows'%where)
        path=safe_member_name(part['path'])
        if path==MANIFEST_NAME:
            raise IntakeError('%s cannot name the manifest itself'%where)
        if path in seen_paths:
            raise IntakeError('duplicate part path in manifest: %s'%path)
        seen_paths.add(path)
        sha256=part['sha256']
        if not isinstance(sha256,str) or not _HEX64.fullmatch(sha256):
            raise IntakeError('%s sha256 must be 64 lowercase hexadecimal digits'%where)
        rows=_strict_int(part['rows'],'%s rows'%where)
        if rows<0:
            raise IntakeError('%s rows must be non-negative'%where)
        clean_parts.append({'path':path,'sha256':sha256,'rows':rows})
    # Logical manifest fingerprint: parts sorted by path, sorted object keys,
    # compact UTF-8 JSON with no trailing newline. It describes the logical
    # manifest only and never replaces the raw ZIP/manifest byte digests.
    canonical=[{'path':p['path'],'rows':p['rows'],'sha256':p['sha256']}
               for p in sorted(clean_parts,key=lambda p:p['path'])]
    fingerprint_text=json.dumps(canonical,sort_keys=True,ensure_ascii=False,
                                separators=(',',':'))
    fingerprint=hashlib.sha256(fingerprint_text.encode('utf-8')).hexdigest()
    manifest['parts']=clean_parts
    return manifest,fingerprint


# ---------------------------------------------------------------------------
# ZIP structural validation and one-pass extraction from captured bytes
# ---------------------------------------------------------------------------

def extract_members(captured,max_bytes):
    """Return (members_by_name, ordered infos) from the captured ZIP bytes."""
    try:
        zf=zipfile.ZipFile(io.BytesIO(captured))
    except zipfile.BadZipFile as exc:
        raise IntakeError('not a valid ZIP package: %s'%exc) from None
    members={}
    declared_names=set()
    infos=zf.infolist()
    # Structural/name/flag checks first, before any payload is decompressed.
    for info in infos:
        name=safe_member_name(info.filename)
        if name.endswith('/'):
            raise IntakeError('explicit directory entries are not allowed: %r'%name)
        if name in declared_names:
            raise IntakeError('duplicate ZIP member name: %s'%name)
        declared_names.add(name)
        if info.flag_bits&0x1:
            raise IntakeError('encrypted ZIP member is not allowed: %s'%name)
        if info.compress_type not in (zipfile.ZIP_STORED,zipfile.ZIP_DEFLATED):
            raise IntakeError('only STORE and DEFLATE compression are allowed: %s'%name)
        if _is_symlink_member(info):
            raise IntakeError('symbolic-link ZIP member is not allowed: %s'%name)
        if not _is_regular_member(info):
            raise IntakeError('only regular file ZIP members are allowed: %s'%name)
    if MANIFEST_NAME not in declared_names:
        raise IntakeError('package must contain a root %s member'%MANIFEST_NAME)
    total=0
    info=None
    try:
        for info in infos:
            name=info.filename
            # zf.open verifies the local header; read() verifies CRC-32 and the
            # compressed size, so truncation and corruption fail here rather
            # than trusting the central directory's declared sizes.
            with zf.open(info) as fh:
                data=fh.read()
            total+=len(data)
            if total>max_bytes:
                raise IntakeError('total decompressed member size %d exceeds '
                                  'max_bytes %d'%(total,max_bytes))
            members[name]=data
    except (zipfile.BadZipFile,zlib.error,EOFError,OSError) as exc:
        raise IntakeError('corrupt ZIP member (%s): %s'
                          %(getattr(info,'filename','?'),exc)) from None
    finally:
        zf.close()
    return members,infos


# ---------------------------------------------------------------------------
# CSV parsing and vendor-record -> event mapping
# ---------------------------------------------------------------------------

def _validate_quoting(text,member_name):
    """Reject malformed quoting that csv.reader itself may tolerate.

    A quote is legal only as the first character of a field (then the field is
    quoted and every internal quote is doubled) or as the doubled closing pair
    followed by a comma or end-of-file. A bare quote inside an unquoted field,
    an undoubled quote and trailing junk after a closing quote are rejected.
    Newlines inside quoted fields are part of the field and do not end records.
    """
    i=0;n=len(text);logical=1
    while i<n:
        # one logical record
        field_start=True;in_quotes=False
        while i<n:
            c=text[i]
            if in_quotes:
                if c=='"':
                    if i+1<n and text[i+1]=='"':
                        i+=2;continue
                    in_quotes=False;i+=1
                    if i<n and text[i] not in (',','\r','\n'):
                        raise IntakeError('%s: malformed CSV quoting at logical '
                                          'record %d'%(member_name,logical))
                    continue
                i+=1;continue
            if c=='"':
                if not field_start:
                    raise IntakeError('%s: bare quote in unquoted field at logical '
                                      'record %d'%(member_name,logical))
                in_quotes=True;i+=1;continue
            if c==',':
                field_start=True;i+=1;continue
            if c in ('\r','\n'):
                if c=='\r' and i+1<n and text[i+1]=='\n':
                    i+=1
                i+=1;logical+=1
                break
            field_start=False;i+=1
        else:
            if in_quotes:
                raise IntakeError('%s: unterminated quoted field at logical '
                                  'record %d'%(member_name,logical))
            return


def parse_csv_part(member_name,data):
    """Parse one CSV part into (records, row_count).

    Records are true CSV logical records: quoting, doubled quote escaping and
    newlines inside quoted fields are handled by the csv module; malformed
    quoting raises. Empty logical records and all-empty rows are ignored.
    record_index counts only non-empty logical data records, starting at 1.
    """
    if data.startswith(b'\xef\xbb\xbf'):
        raise IntakeError('%s: CSV must be UTF-8 without a BOM'%member_name)
    try:
        text=data.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise IntakeError('%s: CSV is not valid UTF-8: %s'%(member_name,exc)) from None
    _validate_quoting(text,member_name)
    reader=csv.reader(io.StringIO(text),strict=True)
    try:
        header=next(reader)
    except StopIteration:
        raise IntakeError('%s: missing CSV header'%member_name) from None
    except csv.Error as exc:
        raise IntakeError('%s: malformed CSV header: %s'%(member_name,exc)) from None
    if header!=CSV_HEADER:
        raise IntakeError('%s: CSV header must be exactly %s'
                          %(member_name,','.join(CSV_HEADER)))
    records=[];row_count=0
    try:
        for row in reader:
            if not row or all(cell=='' for cell in row):
                continue
            row_count+=1
            if len(row)!=6:
                raise IntakeError('%s: logical record %d must have exactly 6 columns'
                                  %(member_name,row_count))
            records.append((row_count,row))
    except csv.Error as exc:
        raise IntakeError('%s: malformed CSV quoting at logical record %d: %s'
                          %(member_name,row_count,exc)) from None
    return records,row_count


def map_record(member_name,record_index,row):
    """Map one vendor CSV row onto the existing normalized event contract."""
    source_event_id,occurred_at,action,barcode,loan_reference,due_on=row
    where='%s record %d'%(member_name,record_index)
    kind=_ACTION_MAP.get(action)
    if kind is None:
        raise IntakeError('%s: unknown action %r'%(where,action))
    if kind=='return':
        if due_on!='':
            raise IntakeError('%s: CHECKIN due_on must be empty'%where)
        due=None
    else:
        if due_on=='':
            raise IntakeError('%s: %s requires a due_on date'%(where,action))
        due=due_on
    raw={
        'event_id':source_event_id,
        'at':occurred_at,
        'kind':kind,
        'copy_id':barcode,
        'loan_id':loan_reference,
        'due':due,
    }
    # Reuse the JSONL validator unchanged: exact identities, whole-second
    # timestamp with timezone, per-kind due-date and lifecycle-input rules keep
    # exactly the old strictness. Errors are re-cast with source location.
    try:
        return decode_event(json.dumps(raw))
    except ValueError as exc:
        raise IntakeError('%s (event %r): %s'
                          %(where,source_event_id,exc)) from None


# ---------------------------------------------------------------------------
# Whole-package validation
# ---------------------------------------------------------------------------

def validate_package(captured,max_bytes):
    """Validate captured ZIP bytes end to end and return all intake context.

    Nothing here touches the source path again; the captured bytes are the
    single source of truth for parsing, hashing and archived raw data.
    """
    members,_infos=extract_members(captured,max_bytes)
    manifest_bytes=members[MANIFEST_NAME]
    manifest,manifest_fingerprint=parse_manifest(manifest_bytes)
    declared={p['path'] for p in manifest['parts']}
    actual=set(members)-{MANIFEST_NAME}
    missing=sorted(declared-actual)
    extra=sorted(actual-declared)
    if missing:
        raise IntakeError('package is missing declared CSV part(s): %s'
                          %', '.join(missing))
    if extra:
        raise IntakeError('package contains undeclared member(s): %s'
                          %', '.join(extra))
    package_sha256=hashlib.sha256(captured).hexdigest()
    manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest()

    # Part order is irrelevant to event semantics; sort by member name so every
    # generated identifier and byte layout is stable across repackagings.
    occurrences=[]     # (part order, record_index, event_id, payload, member)
    part_rows=[]       # provenance rows in stable order
    payloads={}        # event_id -> canonical payload for cross-part conflict check
    source_rows=0
    for order,part in enumerate(sorted(manifest['parts'],key=lambda p:p['path']),start=1):
        name=part['path']
        data=members[name]
        actual_sha256=hashlib.sha256(data).hexdigest()
        if actual_sha256!=part['sha256']:
            raise IntakeError('%s: sha256 mismatch: declared %s actual %s'
                              %(name,part['sha256'],actual_sha256))
        records,actual_rows=parse_csv_part(name,data)
        if actual_rows!=part['rows']:
            raise IntakeError('%s: row count mismatch: declared %d actual %d'
                              %(name,part['rows'],actual_rows))
        part_rows.append({
            'part_id':order,'member_name':name,
            'declared_sha256':part['sha256'],'actual_sha256':actual_sha256,
            'declared_rows':part['rows'],'actual_rows':actual_rows,
            'raw_bytes':data,'raw_size':len(data),
        })
        for record_index,row in records:
            event=map_record(name,record_index,row)
            payload=json.dumps(event,sort_keys=True)
            event_id=event['event_id']
            prior=payloads.get(event_id)
            if prior is not None and prior!=payload:
                raise IntakeError('conflicting event ID %r: same id appears with '
                                  'different normalized payloads across parts'
                                  %event_id)
            payloads.setdefault(event_id,payload)
            occurrences.append({
                'part_id':order,'member_name':name,
                'record_index':record_index,'event_id':event_id,
                'payload':payload,
            })
            source_rows+=1
    if source_rows==0:
        raise IntakeError('empty event stream: every CSV part has zero records')

    # Events are fed in stable (member name, record index) order; the staging
    # layer re-applies the same global dedup/conflict rules as the JSONL path.
    events=[json.loads(o['payload']) for o in occurrences]
    return {
        'manifest':manifest,
        'manifest_fingerprint':manifest_fingerprint,
        'manifest_bytes':manifest_bytes,
        'manifest_sha256':manifest_sha256,
        'package_sha256':package_sha256,
        'package_size':len(captured),
        'package_bytes':captured,
        'parts':part_rows,
        'occurrences':occurrences,
        'events':events,
        'source_rows':source_rows,
        'unique_events':len(payloads),
        'duplicate_rows':source_rows-len(payloads),
        'part_count':len(part_rows),
    }


# ---------------------------------------------------------------------------
# Provenance schema (written inside the staging transaction)
# ---------------------------------------------------------------------------

def _write_provenance(db,context):
    db.execute('CREATE TABLE intake_runs('
               'run_id INTEGER PRIMARY KEY CHECK(run_id=1),'
               "mode TEXT NOT NULL CHECK(mode='package_intake'),"
               'source_system TEXT NOT NULL,export_id TEXT NOT NULL,'
               'schema_version INTEGER NOT NULL,export_kind TEXT NOT NULL,'
               'as_of TEXT NOT NULL,'
               'package_bytes BLOB NOT NULL,package_sha256 TEXT NOT NULL,'
               'package_size INTEGER NOT NULL,'
               'manifest_bytes BLOB NOT NULL,manifest_sha256 TEXT NOT NULL,'
               'manifest_fingerprint TEXT NOT NULL,'
               'source_rows INTEGER NOT NULL,duplicate_rows INTEGER NOT NULL,'
               'unique_events INTEGER NOT NULL,part_count INTEGER NOT NULL)')
    db.execute('CREATE TABLE intake_parts('
               'part_id INTEGER PRIMARY KEY,'
               'run_id INTEGER NOT NULL REFERENCES intake_runs(run_id),'
               'member_name TEXT NOT NULL UNIQUE,'
               'declared_sha256 TEXT NOT NULL,actual_sha256 TEXT NOT NULL,'
               'declared_rows INTEGER NOT NULL,actual_rows INTEGER NOT NULL,'
               'raw_bytes BLOB NOT NULL,raw_size INTEGER NOT NULL,'
               'CHECK(declared_sha256=actual_sha256),'
               'CHECK(declared_rows=actual_rows))')
    db.execute('CREATE TABLE intake_occurrences('
               'occurrence_id INTEGER PRIMARY KEY,'
               'part_id INTEGER NOT NULL REFERENCES intake_parts(part_id),'
               'member_name TEXT NOT NULL,record_index INTEGER NOT NULL,'
               'event_id TEXT NOT NULL REFERENCES events(event_id),'
               'payload TEXT NOT NULL,'
               'UNIQUE(part_id,record_index))')
    db.execute('CREATE INDEX intake_occurrences_event_idx '
               'ON intake_occurrences(event_id)')
    m=context['manifest']
    run_params=(
        'package_intake',
        m['source_system'],m['export_id'],m['schema_version'],m['export_kind'],
        context['as_of'],
        sqlite3.Binary(context['package_bytes']),context['package_sha256'],
        context['package_size'],
        sqlite3.Binary(context['manifest_bytes']),context['manifest_sha256'],
        context['manifest_fingerprint'],
        context['source_rows'],context['duplicate_rows'],
        context['unique_events'],context['part_count'],
    )
    placeholders=','.join('?'*len(run_params))
    db.execute(
        'INSERT INTO intake_runs(run_id,mode,source_system,export_id,'
        'schema_version,export_kind,as_of,package_bytes,package_sha256,'
        'package_size,manifest_bytes,manifest_sha256,manifest_fingerprint,'
        'source_rows,duplicate_rows,unique_events,part_count) VALUES (1,'
        +placeholders+')',
        run_params)
    for part in context['parts']:
        db.execute('INSERT INTO intake_parts(part_id,run_id,member_name,'
                   'declared_sha256,actual_sha256,declared_rows,actual_rows,'
                   'raw_bytes,raw_size) VALUES (?,1,?,?,?,?,?,?,?)',
                   (part['part_id'],part['member_name'],part['declared_sha256'],
                    part['actual_sha256'],part['declared_rows'],
                    part['actual_rows'],sqlite3.Binary(part['raw_bytes']),
                    part['raw_size']))
    for occurrence_id,occ in enumerate(context['occurrences'],start=1):
        db.execute('INSERT INTO intake_occurrences(occurrence_id,part_id,'
                   'member_name,record_index,event_id,payload)'
                   ' VALUES (?,?,?,?,?,?)',
                   (occurrence_id,occ['part_id'],occ['member_name'],
                    occ['record_index'],occ['event_id'],occ['payload']))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def collect(package,as_of,target,max_bytes=DEFAULT_MAX_BYTES):
    """Collect one offline vendor full-export ZIP into a self-contained SQLite.

    The ZIP is captured a single time and every later step (parsing,
    validation, digest calculation, raw-byte archival) uses that capture. The
    business snapshot and full provenance are built in a private staging
    database and the target is replaced only after everything has been written,
    checked and closed. Any failure raises and leaves input and old target
    bytes untouched.
    """
    max_bytes=_check_max_bytes(max_bytes)
    as_of=instant(as_of)
    _reject_target_alias(package,target)
    captured=read_package_bytes(package,max_bytes)
    context=validate_package(captured,max_bytes)
    context['as_of']=as_of

    def before_publish(db):
        _write_provenance(db,context)

    try:
        result=_build_database(target,as_of,iter(context['events']),
                               before_publish=before_publish,
                               enable_foreign_keys=True)
    except IntakeError:
        raise
    except (ValueError,sqlite3.Error,OSError) as exc:
        # Keep the original strict JSONL/state-machine messages, but every
        # package-collection failure surfaces as the intake error type.
        raise IntakeError(str(exc)) from None
    m=context['manifest']
    result.update({
        'mode':'package_intake',
        'source_system':m['source_system'],
        'export_id':m['export_id'],
        'export_kind':m['export_kind'],
        'schema_version':m['schema_version'],
        'package_sha256':context['package_sha256'],
        'package_size':context['package_size'],
        'manifest_sha256':context['manifest_sha256'],
        'manifest_fingerprint':context['manifest_fingerprint'],
        'part_count':len(context['parts']),
    })
    return result


def main(argv=None):
    p=argparse.ArgumentParser(
        description='Collect a vendor full-export ZIP package into a circulation SQLite snapshot')
    p.add_argument('--package',required=True,help='vendor export ZIP to collect')
    p.add_argument('--as-of',required=True,help='cutoff instant (same ISO format as the JSONL CLI)')
    p.add_argument('--database',required=True,help='output SQLite file (must not alias the package)')
    p.add_argument('--max-bytes',type=int,default=DEFAULT_MAX_BYTES,
                   help='positive integer byte budget (<= %d) applied separately to the '
                        'captured ZIP size and to total decompressed member bytes'
                        %MAX_BYTES_LIMIT)
    a=p.parse_args(argv)
    try:
        result=collect(a.package,a.as_of,a.database,max_bytes=a.max_bytes)
    except (ValueError,OSError,sqlite3.Error) as exc:
        p.exit(2,str(exc)+'\n')
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':
    main()

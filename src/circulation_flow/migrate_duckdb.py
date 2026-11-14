"""Offline migration of a delivered circulation SQLite snapshot to DuckDB.

The two production entry points (``circulation_flow.cli`` for JSONL and
``circulation_flow.intake`` for vendor ZIP packages) stay unchanged and keep
publishing SQLite. This module adds a *read-only, offline* one-shot migration
that copies an already delivered SQLite snapshot into a self-contained DuckDB
analytical database. It never re-interprets events and never recomputes loans:
events, loans, run_metadata, the status_summary view and, in package mode, the
three intake_* provenance tables are copied as stored, including NULLs, 64-bit
integers, verbatim identity text, JSON text and raw BLOB bytes.

Guarantees:

* one source snapshot. The SQLite source is opened read-only through a URI and
  one explicit read transaction is held for the whole migration, so commits
  landing after the read started (or a replacement of the source path) cannot
  mix into the result;
* explicit, strict type mapping. Bad dynamic types, missing/extra/changed
  tables, orphan provenance rows and contradictory counts reject the run;
* constraints really exist in the target. PRIMARY KEY, UNIQUE, FOREIGN KEY and
  business CHECK constraints are declared on the DuckDB objects and probed;
* bounded Python buffering. Tables are streamed in batches with a server-side
  cursor; no Python container grows with the total row count, while a single
  maximum-size BLOB is allowed to occupy one row slot;
* verify then publish. Per-table typed digests are computed from the snapshot
  and recomputed from a freshly reopened DuckDB connection; the target is
  atomically renamed into place only after verification and clean close. Any
  failure keeps the old target bytes and removes staging files;
* fully offline. DuckDB is imported lazily; no extension is installed or
  loaded and no network/service is used.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile

from .events import instant

DEFAULT_BATCH_SIZE=1000
BATCH_SIZE_MIN=1
BATCH_SIZE_MAX=1_000_000
_TEMP_PREFIX='.circulation-migrate-'
_TEMP_SUFFIX='.tmp'
_DUCKDB_STORAGE='.duckdb-storage'

# Source layouts that are accepted. The plain (JSONL) snapshot owns exactly the
# three business tables plus the status_summary view. A package snapshot adds
# the three intake_* provenance tables; they appear all together or not at all.
_PLAIN_TABLES=('events','loans','run_metadata')
_INTAKE_TABLES=('intake_runs','intake_parts','intake_occurrences')
_REQUIRED_VIEW='status_summary'
_KNOWN_TABLES=frozenset(_PLAIN_TABLES+_INTAKE_TABLES)

# Stable copy/order keys per table. They are explicit, never "ORDER BY all
# columns", so BLOB-heavy tables do not need BLOB comparison during sorting.
_STABLE_KEYS={
    'events':('event_id',),
    'loans':('loan_id',),
    'run_metadata':('as_of',),
    'intake_runs':('run_id',),
    'intake_parts':('part_id',),
    'intake_occurrences':('occurrence_id',),
}

# Accepted source column layout: (column name, declared SQLite type, notnull).
# The declared type is part of the acceptable-structure contract: an accepted
# source must carry the exact published schema, not just compatible values.
# Note a SQLite quirk: PRAGMA table_info reports notnull=0 for TEXT/BLOB
# PRIMARY KEY columns even though a PRIMARY KEY is NOT NULL semantically (only
# INTEGER PRIMARY KEY columns carry the auto notnull flag); the key declaration
# itself is checked separately in _validate_table_keys, so the expected flags
# below mirror SQLite's actual catalog output for the published schema.
_SOURCE_COLUMNS={
    'events':(
        ('event_id','TEXT',0),('event_at','TEXT',1),
        ('applied','INTEGER',1),('payload','TEXT',1)),
    'loans':(
        ('loan_id','TEXT',0),('copy_id','TEXT',1),
        ('borrowed_at','TEXT',1),('due','TEXT',1),('returned_at','TEXT',0),
        ('renewals','INTEGER',1),('overdue_days','INTEGER',1),
        ('status','TEXT',1)),
    'run_metadata':(
        ('as_of','TEXT',1),('source_rows','INTEGER',0),
        ('duplicate_rows','INTEGER',0),('future_events','INTEGER',0)),
    'intake_runs':(
        ('run_id','INTEGER',0),('mode','TEXT',1),
        ('source_system','TEXT',1),('export_id','TEXT',1),
        ('schema_version','INTEGER',1),('export_kind','TEXT',1),
        ('as_of','TEXT',1),
        ('package_bytes','BLOB',1),('package_sha256','TEXT',1),
        ('package_size','INTEGER',1),
        ('manifest_bytes','BLOB',1),('manifest_sha256','TEXT',1),
        ('manifest_fingerprint','TEXT',1),
        ('source_rows','INTEGER',1),('duplicate_rows','INTEGER',1),
        ('unique_events','INTEGER',1),('part_count','INTEGER',1)),
    'intake_parts':(
        ('part_id','INTEGER',0),('run_id','INTEGER',1),
        ('member_name','TEXT',1),
        ('declared_sha256','TEXT',1),('actual_sha256','TEXT',1),
        ('declared_rows','INTEGER',1),('actual_rows','INTEGER',1),
        ('raw_bytes','BLOB',1),('raw_size','INTEGER',1)),
    'intake_occurrences':(
        ('occurrence_id','INTEGER',0),('part_id','INTEGER',1),
        ('member_name','TEXT',1),('record_index','INTEGER',1),
        ('event_id','TEXT',1),('payload','TEXT',1)),
}


class MigrationError(ValueError):
    """A rejected migration; the message names the object/row at fault."""


# ---------------------------------------------------------------------------
# Lazy DuckDB import: the production JSONL/intake entry points do not need it
# ---------------------------------------------------------------------------

def _duckdb():
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - exercised via CLI tests
        raise MigrationError(
            'duckdb is required for the offline migration; install the pinned '
            'dependency from requirements-duckdb.lock') from None
    return duckdb


def _check_batch_size(batch_size):
    if isinstance(batch_size,bool) or not isinstance(batch_size,int) \
            or not (BATCH_SIZE_MIN<=batch_size<=BATCH_SIZE_MAX):
        raise MigrationError('batch_size must be an integer between %d and %d'
                             %(BATCH_SIZE_MIN,BATCH_SIZE_MAX))
    return batch_size


# ---------------------------------------------------------------------------
# Path guards: the target must never alias the source database
# ---------------------------------------------------------------------------

def _reject_target_alias(source,target):
    """Reject plain, symlink and hard-link aliases between source and target."""
    source=Path(source);target=Path(target)
    if not source.is_file():
        raise MigrationError('source database is not a regular file: %s'%source)
    src_real=os.path.realpath(source)
    src_st=os.stat(source)
    if os.path.lexists(target):
        if os.path.realpath(target)==src_real:
            raise MigrationError('target database must not be the same file as '
                                 'the source database (path or symlink alias)')
        try:
            t_st=os.stat(target)
        except OSError:
            t_st=None
        if t_st is not None and (t_st.st_dev,t_st.st_ino)==(src_st.st_dev,src_st.st_ino):
            raise MigrationError('target database must not be the same file as '
                                 'the source database (hard-link alias)')


def _resolve_work_dir(temp_dir,target_parent):
    """Return a writable work directory on the target's filesystem."""
    if temp_dir is None:
        return Path(target_parent)
    work=Path(temp_dir)
    work.mkdir(parents=True,exist_ok=True)
    if not work.is_dir():
        raise MigrationError('temp dir is not a directory: %s'%work)
    if os.stat(work).st_dev!=os.stat(target_parent).st_dev:
        raise MigrationError('temp dir must be on the same filesystem as the '
                             'target database so publishing is an atomic rename')
    probe=tempfile.NamedTemporaryFile(prefix=_TEMP_PREFIX,suffix=_TEMP_SUFFIX,
                                      dir=work,delete=False)
    try:
        probe.write(b'ok');probe.flush();probe.close()
    except OSError:
        Path(probe.name).unlink(missing_ok=True)
        raise
    Path(probe.name).unlink(missing_ok=True)
    return work


# ---------------------------------------------------------------------------
# Source snapshot: one read-only connection, one held read transaction
# ---------------------------------------------------------------------------

def _open_source_snapshot(source):
    """Open the SQLite source read-only and take one fixed read snapshot.

    The ``mode=ro`` URI refuses every write, ``query_only=1`` blocks accidental
    write statements at the connection level, and an explicit ``BEGIN`` starts
    one read transaction that is held until close. Every later read therefore
    sees the same committed snapshot: commits landing after this point, or a
    replacement of the source path on disk, cannot mix into the migration.
    """
    uri=Path(source).resolve().as_uri()  # file:// URI, percent-escaped
    try:
        db=sqlite3.connect(uri,uri=True,isolation_level=None)
    except sqlite3.Error as exc:
        raise MigrationError('cannot open source SQLite read-only: %s'%exc) from None
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')  # one deferred read transaction held until close
    except sqlite3.Error as exc:
        db.close()
        raise MigrationError('cannot establish source read transaction: %s'%exc) from None
    return db


# ---------------------------------------------------------------------------
# Acceptable source structure
# ---------------------------------------------------------------------------

def _validate_structure(db):
    """Validate object set, column layouts and key/foreign-key declarations.

    Returns the ordered list of tables to copy and the source mode label.
    """
    objects=db.execute(
        "SELECT type,name FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    # Tables and the one documented view are the only supported object kinds;
    # indexes are implementation details of constraints and are checked via
    # PRAGMA index_list rather than object enumeration.
    found_other=sorted((kind,name) for kind,name in objects
                       if kind not in ('table','view','index'))
    if found_other:
        raise MigrationError('source contains unsupported object(s): %s'
                             %', '.join('%s %s'%x for x in found_other))
    found_tables=sorted(name for kind,name in objects if kind=='table')
    found_views=sorted(name for kind,name in objects if kind=='view')
    extra=sorted(set(found_tables)-_KNOWN_TABLES)
    if extra:
        raise MigrationError('source contains undeclared table(s): %s'
                             %', '.join(extra))
    missing_plain=[t for t in _PLAIN_TABLES if t not in found_tables]
    if missing_plain:
        raise MigrationError('source is missing required table(s): %s'
                             %', '.join(missing_plain))
    intake_present=frozenset(t for t in _INTAKE_TABLES if t in found_tables)
    if intake_present and intake_present!=frozenset(_INTAKE_TABLES):
        raise MigrationError('package source must contain all of %s together, '
                             'found only: %s'%(', '.join(_INTAKE_TABLES),
                                               ', '.join(sorted(intake_present))))
    mode='package' if intake_present else 'plain'
    if found_views!=[_REQUIRED_VIEW]:
        raise MigrationError("source must contain exactly the view "
                             "'status_summary', found views: %s"
                             %(', '.join(found_views) or 'none'))

    tables=list(_PLAIN_TABLES)+(list(_INTAKE_TABLES) if mode=='package' else [])
    for table in tables:
        cols=db.execute('PRAGMA table_info("%s")'%table).fetchall()
        expected=_SOURCE_COLUMNS[table]
        actual=tuple((c[1],c[2].upper(),c[3]) for c in cols)
        if actual!=expected:
            raise MigrationError(
                'table %s has an unsupported structure; expected columns %r, '
                'got %r'%(table,expected,actual))
        _validate_table_keys(db,table,mode)
    # A view must expose the documented five columns in order.
    view_cols=tuple(c[1] for c in db.execute(
        'PRAGMA table_info(%s)'%_REQUIRED_VIEW).fetchall())
    if view_cols!=('status','loans','overdue_loans','overdue_days','renewals'):
        raise MigrationError('status_summary must expose columns '
                             '(status, loans, overdue_loans, overdue_days, '
                             'renewals), got %r'%(view_cols,))
    return tables,mode


def _validate_table_keys(db,table,mode):
    """Require the primary/unique/foreign-key declarations of the contract."""
    info=db.execute('PRAGMA table_info("%s")'%table).fetchall()
    pk_cols=[c[1] for c in sorted((c for c in info if c[5]>0),key=lambda c:c[5])]
    expected_pk={
        'events':['event_id'],'loans':['loan_id'],
        'run_metadata':[],
        'intake_runs':['run_id'],'intake_parts':['part_id'],
        'intake_occurrences':['occurrence_id'],
    }[table]
    if pk_cols!=expected_pk:
        raise MigrationError('table %s primary key must be %r, got %r'
                             %(table,expected_pk,pk_cols))
    index_rows=db.execute('PRAGMA index_list("%s")'%table).fetchall()
    unique_indexes={r[1]:r[2] for r in index_rows}  # name -> unique flag
    index_columns={name:[c[2] for c in db.execute(
        'PRAGMA index_info("%s")'%name).fetchall()]
        for name in unique_indexes}
    def has_unique(cols):
        return any(flag and index_columns[name]==cols
                   for name,flag in unique_indexes.items())
    expected_unique={
        'events':[],'loans':[],'run_metadata':[],
        'intake_runs':[],
        'intake_parts':[['member_name']],
        'intake_occurrences':[['part_id','record_index']],
    }[table]
    for cols in expected_unique:
        if not has_unique(cols):
            raise MigrationError('table %s is missing UNIQUE constraint on %s'
                                 %(table,', '.join(cols)))
    fks=db.execute('PRAGMA foreign_key_list("%s")'%table).fetchall()
    fk_map={(f[3],f[2],f[4]):f for f in fks}  # (from, to-table, to-col)
    expected_fk={
        'events':[],'loans':[],'run_metadata':[],
        'intake_runs':[],
        'intake_parts':[('run_id','intake_runs','run_id')],
        'intake_occurrences':[('part_id','intake_parts','part_id'),
                              ('event_id','events','event_id')],
    }[table]
    for spec in expected_fk:
        if spec not in fk_map:
            raise MigrationError('table %s is missing FOREIGN KEY %s -> %s(%s)'
                                 %(table,spec[0],spec[1],spec[2]))
    extra_fk=[spec for spec in fk_map if spec not in expected_fk]
    if extra_fk:
        raise MigrationError('table %s has undeclared foreign key(s): %s'
                             %(table,extra_fk))
    if mode=='package':
        # SQLite only enforces FKs when the pragma was on at write time; verify
        # the delivered snapshot is internally consistent regardless.
        violations=db.execute('PRAGMA foreign_key_check("%s")'%table).fetchall()
        if violations:
            raise MigrationError('table %s contains orphan rows violating its '
                                 'foreign keys: %r'%(table,violations[:3]))


# ---------------------------------------------------------------------------
# Strict dynamic type checking and typed, unambiguous digest encoding
# ---------------------------------------------------------------------------

# Tags make the per-value encoding unambiguous even for NULL, empty strings,
# empty BLOBs and numeric edge values. Every row is framed by its column count.
_TAG_NULL=b'N'
_TAG_INT=b'I'
_TAG_TEXT=b'T'
_TAG_BLOB=b'B'
_INT64_MIN=-(1<<63)
_INT64_MAX=(1<<63)-1


def _check_value(table,column,declared,value):
    """Return the value only when its runtime type matches the declared type."""
    if value is None:
        return None
    if declared=='INTEGER':
        # bool is an int subclass in Python but never a legal stored value.
        if isinstance(value,bool) or not isinstance(value,int):
            raise MigrationError('table %s column %s requires INTEGER, got '
                                 'dynamic type %s'%(table,column,
                                                    type(value).__name__))
        if not (_INT64_MIN<=value<=_INT64_MAX):
            raise MigrationError('table %s column %s integer %d is outside the '
                                 'signed 64-bit range'%(table,column,value))
        return value
    if declared=='TEXT':
        if not isinstance(value,str):
            raise MigrationError('table %s column %s requires TEXT, got dynamic '
                                 'type %s'%(table,column,type(value).__name__))
        return value
    if declared=='BLOB':
        if not isinstance(value,(bytes,bytearray,memoryview)):
            raise MigrationError('table %s column %s requires BLOB, got dynamic '
                                 'type %s'%(table,column,type(value).__name__))
        return bytes(value)
    raise MigrationError('table %s column %s has unmapped declared type %r'
                         %(table,column,declared))  # pragma: no cover


def _encode_row(declared_types,values):
    parts=[str(len(values)).encode('ascii'),b'|']
    for declared,value in zip(declared_types,values):
        if value is None:
            parts.append(_TAG_NULL)
            continue
        if declared=='INTEGER':
            body=str(value).encode('ascii');tag=_TAG_INT
        elif declared=='TEXT':
            body=value.encode('utf-8');tag=_TAG_TEXT
        else:
            body=value;tag=_TAG_BLOB
        parts.append(tag+str(len(body)).encode('ascii')+b':'+body)
    return b''.join(parts)


# ---------------------------------------------------------------------------
# Target DuckDB schema: explicit DDL so every constraint actually exists
# ---------------------------------------------------------------------------

_DUCKDB_DDL=[
    'CREATE TABLE events('
    'event_id VARCHAR PRIMARY KEY,event_at VARCHAR NOT NULL,'
    'applied BIGINT NOT NULL,payload VARCHAR NOT NULL)',
    'CREATE TABLE loans('
    'loan_id VARCHAR PRIMARY KEY,copy_id VARCHAR NOT NULL,'
    'borrowed_at VARCHAR NOT NULL,due VARCHAR NOT NULL,returned_at VARCHAR,'
    'renewals BIGINT NOT NULL,overdue_days BIGINT NOT NULL,status VARCHAR NOT NULL)',
    'CREATE TABLE run_metadata('
    'as_of VARCHAR NOT NULL,source_rows BIGINT,'
    'duplicate_rows BIGINT,future_events BIGINT)',
    'CREATE TABLE intake_runs('
    'run_id BIGINT PRIMARY KEY CHECK(run_id=1),'
    "mode VARCHAR NOT NULL CHECK(mode='package_intake'),"
    'source_system VARCHAR NOT NULL,export_id VARCHAR NOT NULL,'
    'schema_version BIGINT NOT NULL,export_kind VARCHAR NOT NULL,'
    'as_of VARCHAR NOT NULL,'
    'package_bytes BLOB NOT NULL,package_sha256 VARCHAR NOT NULL,'
    'package_size BIGINT NOT NULL,'
    'manifest_bytes BLOB NOT NULL,manifest_sha256 VARCHAR NOT NULL,'
    'manifest_fingerprint VARCHAR NOT NULL,'
    'source_rows BIGINT NOT NULL,duplicate_rows BIGINT NOT NULL,'
    'unique_events BIGINT NOT NULL,part_count BIGINT NOT NULL)',
    'CREATE TABLE intake_parts('
    'part_id BIGINT PRIMARY KEY,'
    'run_id BIGINT NOT NULL REFERENCES intake_runs(run_id),'
    'member_name VARCHAR NOT NULL UNIQUE,'
    'declared_sha256 VARCHAR NOT NULL,actual_sha256 VARCHAR NOT NULL,'
    'declared_rows BIGINT NOT NULL,actual_rows BIGINT NOT NULL,'
    'raw_bytes BLOB NOT NULL,raw_size BIGINT NOT NULL,'
    'CHECK(declared_sha256=actual_sha256),'
    'CHECK(declared_rows=actual_rows))',
    'CREATE TABLE intake_occurrences('
    'occurrence_id BIGINT PRIMARY KEY,'
    'part_id BIGINT NOT NULL REFERENCES intake_parts(part_id),'
    'member_name VARCHAR NOT NULL,record_index BIGINT NOT NULL,'
    'event_id VARCHAR NOT NULL REFERENCES events(event_id),'
    'payload VARCHAR NOT NULL,'
    'UNIQUE(part_id,record_index))',
    'CREATE INDEX intake_occurrences_event_idx '
    'ON intake_occurrences(event_id)',
    'CREATE VIEW status_summary AS SELECT status,count(*) loans,'
    'sum(CAST(overdue_days>0 AS BIGINT)) overdue_loans,'
    'sum(overdue_days) overdue_days,sum(renewals) renewals '
    'FROM loans GROUP BY status',
]


_DDL_BY_OBJECT={
    'events':_DUCKDB_DDL[0],
    'loans':_DUCKDB_DDL[1],
    'run_metadata':_DUCKDB_DDL[2],
    'intake_runs':_DUCKDB_DDL[3],
    'intake_parts':_DUCKDB_DDL[4],
    'intake_occurrences':_DUCKDB_DDL[5],
}
_DDL_INDEX=_DUCKDB_DDL[6]
_DDL_VIEW=_DUCKDB_DDL[7]


def _create_target_schema(con,mode):
    """Create the target objects for the mode; FKs are always enforced."""
    wanted=list(_PLAIN_TABLES)
    if mode=='package':
        wanted+=list(_INTAKE_TABLES)
    for name in wanted:
        con.execute(_DDL_BY_OBJECT[name])
    if mode=='package':
        con.execute(_DDL_INDEX)
    con.execute(_DDL_VIEW)


# ---------------------------------------------------------------------------
# Cross-table consistency: counts and business summaries must not contradict
# ---------------------------------------------------------------------------

def _scan_dynamic_types(db,tables):
    """Type-check every stored value before any cross-table logic runs.

    SQLite is dynamically typed and applies affinity on plain binds, but
    BLOB-cast or legacy files can still hold values whose runtime storage class
    contradicts the declared column type. Rejecting them here guarantees the
    later consistency SQL and the typed digests never meet a bad value first.
    """
    for table in tables:
        columns=[c[0] for c in _SOURCE_COLUMNS[table]]
        declared=[c[1] for c in _SOURCE_COLUMNS[table]]
        select='SELECT %s FROM "%s"'%(', '.join('"%s"'%c for c in columns),table)
        for row in db.execute(select):
            for col,decl,val in zip(columns,declared,row):
                _check_value(table,col,decl,val)


def _check_consistency(db,tables):
    """Reject contradictory counts/orphans inside the fixed source snapshot."""
    def scalar(sql,params=()):
        return db.execute(sql,params).fetchone()[0]
    if 'intake_runs' in tables:
        run_count=scalar('SELECT count(*) FROM intake_runs')
        if run_count!=1 or scalar('SELECT run_id FROM intake_runs')!=1:
            raise MigrationError('intake_runs must contain exactly one row with '
                                 'run_id=1, found %d row(s)'%run_count)
        run=db.execute('SELECT source_rows,duplicate_rows,unique_events,'
                       'part_count FROM intake_runs').fetchone()
        source_rows,duplicate_rows,unique_events,part_count=run
        occ_total=scalar('SELECT count(*) FROM intake_occurrences')
        part_total=scalar('SELECT count(*) FROM intake_parts')
        if occ_total!=source_rows:
            raise MigrationError('count contradiction: intake_runs.source_rows='
                                 '%d but intake_occurrences holds %d rows'
                                 %(source_rows,occ_total))
        if source_rows-duplicate_rows!=unique_events:
            raise MigrationError('count contradiction: source_rows-duplicate_rows '
                                 '!= unique_events in intake_runs')
        if part_total!=part_count:
            raise MigrationError('count contradiction: intake_runs.part_count=%d '
                                 'but intake_parts holds %d rows'
                                 %(part_count,part_total))
        distinct_events=scalar('SELECT count(DISTINCT event_id) '
                               'FROM intake_occurrences')
        if distinct_events!=unique_events:
            raise MigrationError('count contradiction: distinct occurrence '
                                 'event_id=%d != intake_runs.unique_events=%d'
                                 %(distinct_events,unique_events))
        # Part row counts and raw sizes must match their provenance columns.
        for part_id,declared_rows,actual_rows,raw_size in db.execute(
                'SELECT part_id,declared_rows,actual_rows,raw_size '
                'FROM intake_parts ORDER BY part_id'):
            if declared_rows!=actual_rows:
                raise MigrationError('count contradiction: intake_parts part_id=%d '
                                     'declared/actual rows differ'%(part_id,))
            occ=scalar('SELECT count(*) FROM intake_occurrences WHERE part_id=?',
                       (part_id,))
            if occ!=actual_rows:
                raise MigrationError('count contradiction: part_id=%d declares %d '
                                     'rows but occurrences hold %d'
                                     %(part_id,actual_rows,occ))
            if raw_size<0:
                raise MigrationError('intake_parts part_id=%d has negative '
                                     'raw_size'%part_id)
        # Every occurrence must trace to an existing part (FK covers it too, but
        # the explicit message names the business problem).
        orphan_parts=scalar(
            'SELECT count(*) FROM intake_occurrences o WHERE NOT EXISTS '
            '(SELECT 1 FROM intake_parts p WHERE p.part_id=o.part_id)')
        if orphan_parts:
            raise MigrationError('orphan provenance: %d occurrence row(s) have no '
                                 'matching intake_parts row'%orphan_parts)
        orphan_events=scalar(
            'SELECT count(*) FROM intake_occurrences o WHERE NOT EXISTS '
            '(SELECT 1 FROM events e WHERE e.event_id=o.event_id)')
        if orphan_events:
            raise MigrationError('orphan provenance: %d occurrence row(s) have no '
                                 'matching events row'%orphan_events)
    # run_metadata vs events and the status_summary view vs loans.
    meta=db.execute('SELECT as_of,source_rows,duplicate_rows,future_events '
                    'FROM run_metadata').fetchall()
    if len(meta)!=1:
        raise MigrationError('run_metadata must contain exactly one row, found %d'
                             %len(meta))
    as_of,source_rows,duplicate_rows,future_events=meta[0]
    try:
        instant(as_of)
    except ValueError:
        raise MigrationError('run_metadata.as_of %r is not a valid cutoff '
                             'instant'%as_of) from None
    event_count=scalar('SELECT count(*) FROM events')
    if source_rows is not None and duplicate_rows is not None \
            and source_rows-duplicate_rows!=event_count:
        raise MigrationError('count contradiction: run_metadata '
                             'source_rows-duplicate_rows=%d but events holds %d'
                             %(source_rows-duplicate_rows,event_count))
    applied_zero=scalar('SELECT count(*) FROM events WHERE applied=0')
    if future_events is not None and applied_zero!=future_events:
        raise MigrationError('count contradiction: run_metadata.future_events=%d '
                             'but %d event rows have applied=0'
                             %(future_events,applied_zero))
    if scalar("SELECT count(*) FROM events WHERE applied NOT IN (0,1)"):
        raise MigrationError('events.applied must be 0 or 1')
    # View and table must describe the same loan population.
    view_rows=db.execute('SELECT status,loans,overdue_loans,overdue_days,renewals '
                        'FROM status_summary ORDER BY status').fetchall()
    table_rows=db.execute(
        "SELECT status,count(*),sum(overdue_days>0),sum(overdue_days),"
        "sum(renewals) FROM loans GROUP BY status ORDER BY status").fetchall()
    if [tuple(r) for r in view_rows]!=[tuple(r) for r in table_rows]:
        raise MigrationError('count contradiction: status_summary view %r does '
                             'not match loans aggregation %r'
                             %(view_rows,table_rows))
    bad_status=scalar("SELECT count(DISTINCT status) FROM loans "
                      "WHERE status NOT IN ('open','closed')")
    if bad_status:
        raise MigrationError("loans.status must be 'open' or 'closed'")


# ---------------------------------------------------------------------------
# Batched copy with typed source digests
# ---------------------------------------------------------------------------

def _stream_table(src,dst,table,batch_size):
    """Copy one table in bounded batches, returning (row_count, digest).

    A read-only server-side cursor (arraysize=1, default SQLite cursor) feeds
    fixed-size batches via fetchmany; each batch is one executemany. The only
    Python buffers that grow are one batch of rows and one batch of encoded
    digest fragments: both are O(batch_size * row width), never O(total rows).
    A single BLOB may occupy its full size in one row slot; nothing larger is
    accumulated.
    """
    declared=[c[1] for c in _SOURCE_COLUMNS[table]]
    columns=[c[0] for c in _SOURCE_COLUMNS[table]]
    keys=_STABLE_KEYS[table]
    order=', '.join('"%s"'%k for k in keys)
    select='SELECT %s FROM "%s" ORDER BY %s'%(', '.join('"%s"'%c for c in columns),
                                              table,order)
    placeholders=', '.join(['?']*len(columns))
    insert='INSERT INTO %s(%s) VALUES (%s)'%(
        table,', '.join(columns),placeholders)
    cursor=src.execute(select)
    hasher=hashlib.sha256()
    total=0
    while True:
        batch=cursor.fetchmany(batch_size)
        if not batch:
            break
        checked=[]
        for row in batch:
            checked_row=tuple(_check_value(table,col,decl,val)
                              for col,decl,val in zip(columns,declared,row))
            hasher.update(_encode_row(declared,checked_row))
            checked.append(checked_row)
        dst.executemany(insert,checked)
        total+=len(checked)
        # Drop batch references before the next fetch; no cross-batch container.
        del batch,checked
    return total,hasher.hexdigest()


def _view_digest(src):
    """Digest the view output using the same typed encoding as tables."""
    declared=('TEXT','INTEGER','INTEGER','INTEGER','INTEGER')
    hasher=hashlib.sha256()
    count=0
    for row in src.execute('SELECT status,loans,overdue_loans,overdue_days,'
                           'renewals FROM status_summary ORDER BY status'):
        checked=tuple(_check_value('status_summary',c,d,v)
                      for c,d,v in zip(declared,declared,row))
        hasher.update(_encode_row(declared,checked))
        count+=1
    return count,hasher.hexdigest()


# ---------------------------------------------------------------------------
# Reopen DuckDB and verify every copied item
# ---------------------------------------------------------------------------

def _readback_digests(path,tables,batch_size):
    """Reopen the finished DuckDB file read-only and recompute every digest.

    The reopened connection streams through server-side cursors and
    fetchmany(batch_size): like the copy path, no table-wide Python container
    is materialized, so verification memory stays bounded by the batch size
    regardless of total rows.
    """
    duckdb=_duckdb()
    con=duckdb.connect(str(path),read_only=True)
    try:
        result={}
        for table in tables:
            columns=[c[0] for c in _SOURCE_COLUMNS[table]]
            declared=[c[1] for c in _SOURCE_COLUMNS[table]]
            order=', '.join('"%s"'%k for k in _STABLE_KEYS[table])
            sql='SELECT %s FROM %s ORDER BY %s'%(
                ', '.join('"%s"'%c for c in columns),table,order)
            cursor=con.execute(sql)
            hasher=hashlib.sha256();count=0
            while True:
                batch=cursor.fetchmany(batch_size)
                if not batch:
                    break
                for row in batch:
                    # DuckDB returns BLOBs as bytes/buffer and BIGINTs as int;
                    # the same strict checker rejects any unexpected Python type.
                    checked=tuple(_check_value(table,col,decl,val)
                                  for col,decl,val in zip(columns,declared,row))
                    hasher.update(_encode_row(declared,checked))
                    count+=1
            result[table]=(count,hasher.hexdigest())
        # View: values come from aggregation, so INTEGER-affinity columns may
        # arrive as int already; check them under the view's declared types.
        declared=('TEXT','INTEGER','INTEGER','INTEGER','INTEGER')
        hasher=hashlib.sha256();count=0
        view_cursor=con.execute('SELECT status,loans,overdue_loans,overdue_days,'
                                'renewals FROM status_summary ORDER BY status')
        while True:
            batch=view_cursor.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                checked=tuple(_check_value('status_summary',c,d,v)
                              for c,d,v in zip(declared,declared,row))
                hasher.update(_encode_row(declared,checked))
                count+=1
        result[_REQUIRED_VIEW]=(count,hasher.hexdigest())
        return result
    finally:
        con.close()


def _verify_constraint_catalog(con,mode):
    """Check the DuckDB catalog actually carries every required constraint.

    DuckDB exposes constraints through the duckdb_constraints() table function;
    NOT NULL is represented as a CHECK constraint, so the business CHECK probes
    below match on the constraint expression text.
    """
    catalog=[tuple(r) for r in con.execute(
        "SELECT table_name,constraint_type,constraint_text,referenced_table "
        "FROM duckdb_constraints() WHERE schema_name='main' "
        "ORDER BY table_name,constraint_type").fetchall()]
    constraints={}
    fk_targets={}
    check_texts={}
    for table,kind,text,ref_table in catalog:
        constraints.setdefault(table,set()).add(kind)
        check_texts.setdefault(table,[]).append(text or '')
        if kind=='FOREIGN KEY':
            fk_targets.setdefault(table,set()).add(ref_table)
    required_pk=('events','loans')
    if mode=='package':
        required_pk+=('intake_runs','intake_parts','intake_occurrences')
    for table in required_pk:
        if 'PRIMARY KEY' not in constraints.get(table,set()):
            raise MigrationError('target table %s lost its PRIMARY KEY'%table)
    if mode=='package':
        if 'FOREIGN KEY' not in constraints.get('intake_parts',set()):
            raise MigrationError('target intake_parts lost its FOREIGN KEY')
        if 'FOREIGN KEY' not in constraints.get('intake_occurrences',set()):
            raise MigrationError('target intake_occurrences lost its FOREIGN KEYs')
        if fk_targets.get('intake_parts')!={'intake_runs'}:
            raise MigrationError('intake_parts FOREIGN KEY must reference intake_runs')
        if fk_targets.get('intake_occurrences')!={'events','intake_parts'}:
            raise MigrationError('intake_occurrences FOREIGN KEYs must reference '
                                 'events and intake_parts, got %r'
                                 %(fk_targets.get('intake_occurrences'),))
        if 'UNIQUE' not in constraints.get('intake_parts',set()) or \
                'UNIQUE' not in constraints.get('intake_occurrences',set()):
            raise MigrationError('target provenance tables lost UNIQUE constraints')
        def norm(text):
            # DuckDB normalizes CHECK expressions (extra spaces, quoted
            # identifiers, CHECK((...)) wrapping); compare canonical forms.
            return (text or '').replace('"','').replace(' ','').lower()
        joined=' '.join(norm(t) for t in check_texts.get('intake_runs',[]))
        if 'run_id=1' not in joined or "mode='package_intake'" not in joined:
            raise MigrationError('intake_runs lost its business CHECK constraints')
        joined=' '.join(norm(t) for t in check_texts.get('intake_parts',[]))
        if 'declared_sha256=actual_sha256' not in joined or \
                'declared_rows=actual_rows' not in joined:
            raise MigrationError('intake_parts lost its business CHECK constraints')
    # DuckDB enforces constraints by default. Probes run in autocommit: a
    # rejected statement rolls itself back, and a probe that unexpectedly
    # succeeds raises MigrationError before any later statement runs, so no
    # probe row can survive in the staging database.
    _probe_violations(con,mode)


def _probe_violations(con,mode):
    """Each probe must be rejected by an actually enforced constraint."""
    def must_fail(sql,params=()):
        try:
            con.execute(sql,params)
        except Exception:
            return
        raise MigrationError('constraint probe unexpectedly succeeded: %s'%sql)
    # events NOT NULL enforcement (payload is declared NOT NULL).
    must_fail("INSERT INTO events(event_id,event_at,applied,payload) "
              "VALUES('PROBE-NN','2026-01-01T00:00:00Z',1,NULL)")
    # events PRIMARY KEY enforcement: reinserting an existing event id fails.
    existing_event=con.execute('SELECT event_id,event_at,applied,payload '
                               'FROM events LIMIT 1').fetchone()
    if existing_event is not None:
        must_fail('INSERT INTO events(event_id,event_at,applied,payload) '
                  'VALUES (?,?,?,?)',existing_event)
    else:
        must_fail("INSERT INTO events(event_id,event_at,applied,payload) "
                  "VALUES('PROBE-NN',NULL,1,'{}')")  # NOT NULL enforcement
    # loans NOT NULL enforcement, then PRIMARY KEY enforcement via a duplicate.
    must_fail("INSERT INTO loans(loan_id,copy_id,borrowed_at,due,"
              "returned_at,renewals,overdue_days,status) VALUES "
              "('PROBE-NL',NULL,'2026-01-01T00:00:00Z','2026-01-02',NULL,0,0,"
              "'open')")
    existing_loan=con.execute('SELECT loan_id,copy_id,borrowed_at,due,'
                              'returned_at,renewals,overdue_days,status '
                              'FROM loans LIMIT 1').fetchone()
    if existing_loan is not None:
        must_fail('INSERT INTO loans(loan_id,copy_id,borrowed_at,due,'
                  'returned_at,renewals,overdue_days,status) VALUES '
                  '(?,?,?,?,?,?,?,?)',existing_loan)
    else:
        must_fail("INSERT INTO loans(loan_id,copy_id,borrowed_at,due,"
                  "returned_at,renewals,overdue_days,status) VALUES "
                  "('PROBE-NL','B','2026-01-01T00:00:00Z','2026-01-02',NULL,0,"
                  "0,NULL)")
    # loans has no business CHECK in the source contract; its enforceable
    # invariants are NOT NULL and the PRIMARY KEY, both probed above.
    if mode=='package':
        must_fail("INSERT INTO intake_runs(run_id,mode,source_system,export_id,"
                  "schema_version,export_kind,as_of,package_bytes,package_sha256,"
                  "package_size,manifest_bytes,manifest_sha256,"
                  "manifest_fingerprint,source_rows,duplicate_rows,"
                  "unique_events,part_count) VALUES "
                  "(2,'package_intake','s','e',1,'full','2026-01-01T00:00:00Z',"
                  "BLOB '01','h',1,BLOB '02','h','f',0,0,0,0)")
        must_fail("INSERT INTO intake_occurrences(occurrence_id,part_id,"
                  "member_name,record_index,event_id,payload) VALUES "
                  "(999999,999999,'x',999999,'NO-SUCH-EVENT','{}')")


# ---------------------------------------------------------------------------
# Public migration entry point
# ---------------------------------------------------------------------------

def migrate(source,target,*,batch_size=DEFAULT_BATCH_SIZE,temp_dir=None,
            after_snapshot=None,before_readback=None):
    """Copy one delivered SQLite snapshot into a verified DuckDB database.

    Parameters
    ----------
    source: path to the read-only SQLite snapshot (plain or package mode).
    target: output ``.duckdb`` path; must not alias the source.
    batch_size: rows per fetchmany/executemany batch (1..1_000_000); bounds the
        Python buffers independently of the total row count.
    temp_dir: staging directory; defaults to the target directory and must be on
        the same filesystem so the final rename is atomic.
    after_snapshot: optional hook invoked with the open read-only source
        connection right after the read transaction starts; used by the
        consistency-barrier tests to commit writes / swap the source path while
        the snapshot is held.
    before_readback: optional hook ``(staging_path) -> None`` invoked after the
        staging DuckDB is closed and before reopening it; used to inject
        readback-side failure scenarios.

    Any failure raises MigrationError (or the underlying sqlite/OS error),
    leaves any pre-existing target byte-identical and removes staging files.
    """
    duckdb=_duckdb()
    batch_size=_check_batch_size(batch_size)
    target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
    _reject_target_alias(source,target)
    work_dir=_resolve_work_dir(temp_dir,target.parent)

    # Reserve a unique staging *path* rather than a pre-created file: DuckDB
    # refuses to open an existing non-database file, so the database file is
    # created by duckdb.connect itself.
    fd,staging_name=tempfile.mkstemp(prefix=_TEMP_PREFIX,suffix=_TEMP_SUFFIX,
                                     dir=work_dir)
    os.close(fd)
    staging=Path(staging_name)
    staging.unlink()
    storage_dir=Path(staging_name+_DUCKDB_STORAGE)
    storage_dir.mkdir()
    source_digests={}
    src=None;dst=None
    try:
        # ---- 1. One fixed read snapshot of the whole source ---------------
        src=_open_source_snapshot(source)
        tables,mode=_validate_structure(src)
        # The first reads above have now pinned the read snapshot; external
        # commits or a source-path swap happening at this barrier cannot enter.
        if after_snapshot is not None:
            after_snapshot(src)
        _scan_dynamic_types(src,tables)
        _check_consistency(src,tables)

        # ---- 2. Build the target schema in a private staging DuckDB -------
        dst=duckdb.connect(str(staging))
        try:
            # Built-in pragmas only: no extension repository, no network.
            dst.execute("SET temp_directory='%s'"%str(storage_dir).replace("'", "''"))
            dst.execute('SET autoinstall_known_extensions=false')
            dst.execute('SET autoload_known_extensions=false')
            _create_target_schema(dst,mode)
            _verify_constraint_catalog(dst,mode)

            # ---- 3. Batched copy with per-table typed digests -------------
            for table in tables:
                count,digest=_stream_table(src,dst,table,batch_size)
                source_digests[table]=(count,digest)
            view_count,view_digest=_view_digest(src)
            source_digests[_REQUIRED_VIEW]=(view_count,view_digest)
        finally:
            # Checkpoint so the single staging file holds everything, then
            # close cleanly before the independent readback connection opens.
            if dst is not None:
                try:
                    dst.execute('CHECKPOINT')
                finally:
                    dst.close()
                    dst=None
        src.close();src=None
        if before_readback is not None:
            before_readback(staging)

        # ---- 4. Reopen and verify counts/digests item by item -------------
        got=_readback_digests(staging,tables,batch_size)
        if got!=source_digests:
            details={k:{'source':source_digests.get(k),'target':got.get(k)}
                     for k in sorted(set(source_digests)|set(got))
                     if source_digests.get(k)!=got.get(k)}
            raise MigrationError('readback verification failed: %s'
                                 %json.dumps(details,sort_keys=True))

        # ---- 5. Atomic publish only after verification --------------------
        os.replace(staging,target)
    finally:
        if dst is not None:
            try:
                dst.close()
            except Exception:
                pass
        if src is not None:
            src.close()
        staging.unlink(missing_ok=True)
        # DuckDB may leave a WAL/sidecar next to the staging file on failure.
        for suffix in ('.wal',):
            Path(str(staging)+suffix).unlink(missing_ok=True)
        if storage_dir.exists():
            for child in storage_dir.glob('*'):
                child.unlink(missing_ok=True)
            storage_dir.rmdir()

    result={
        'mode':'duckdb_migration',
        'source_mode':mode,
        'tables':{name:source_digests[name][0] for name in tables},
        'view':_REQUIRED_VIEW,
        'view_rows':source_digests[_REQUIRED_VIEW][0],
        'batch_size':batch_size,
        'digests':{name:source_digests[name][1]
                   for name in tables+[_REQUIRED_VIEW]},
        'duckdb_version':duckdb.__version__,
    }
    return result


def main(argv=None):
    p=argparse.ArgumentParser(
        description='Offline migration of a circulation SQLite snapshot to a '
                    'self-contained DuckDB analytical database')
    p.add_argument('--source',required=True,
                   help='input SQLite snapshot (plain or package-intake mode)')
    p.add_argument('--database',required=True,
                   help='output DuckDB file (must not alias the source)')
    p.add_argument('--batch-size',type=int,default=DEFAULT_BATCH_SIZE,
                   help='rows per copy batch, integer %d..%d; bounds Python '
                        'buffering independently of total rows'
                        %(BATCH_SIZE_MIN,BATCH_SIZE_MAX))
    p.add_argument('--temp-dir',default=None,
                   help='staging directory on the same filesystem as --database '
                        '(defaults to the database directory)')
    a=p.parse_args(argv)
    try:
        result=migrate(a.source,a.database,batch_size=a.batch_size,
                       temp_dir=a.temp_dir)
    except (MigrationError,OSError) as exc:
        p.exit(2,str(exc)+'\n')
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__':
    main()

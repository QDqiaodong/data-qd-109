"""Tests for the vendor full-export package intake (circulation_flow.intake).

Coverage groups:
  1. End-to-end collection of a shuffled multi-part package: the six fixture
     events become six traced occurrences (one duplicate, five unique, one
     future), and every published business row matches the JSONL entry key for
     key rather than just the summary.
  2. Byte-level provenance: capture ZIP, manifest and every part can be rebuilt
     from the output alone, digests and the logical manifest fingerprint verify,
     and every occurrence (duplicates and future rows included) joins back to
     its unique business event.
  3. CSV fidelity: quoting, escaped quotes and newlines inside identities,
     empty parts, empty-record handling, record_index semantics.
  4. Determinism: ZIP entry order, parts order and compression metadata do not
     change business/provenance semantics; identical inputs give identical
     stdout bytes and stable logical content.
  5. Whole-package rejection: structural ZIP faults, unsafe names, symlinks,
     encryption/unsupported compression, duplicate JSON keys, manifest typing,
     digest/row mismatches, UTF-8/BOM and CSV header/column/quoting errors.
  6. Byte-budget boundaries for both the captured ZIP and the decompressed
     total; lifecycle and future-row field rejections stay as strict as JSONL.
  7. Failure injection after business materialization and at the publish
     boundary: old target and source bytes survive, temp files are cleaned and
     a corrected re-run succeeds; target aliasing (symlink/hard-link) rejected.
  8. A real CLI subprocess generates a package, collects it, verifies business
     rows over SQL and rebuilds every archived source byte.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'tools'))

from circulation_flow.pipeline import run  # noqa: E402
from circulation_flow import intake  # noqa: E402
from circulation_flow.intake import collect,IntakeError  # noqa: E402
from generate_package import build_package,load_jsonl_records,csv_bytes  # noqa: E402

FIX=ROOT/'fixtures/events.jsonl'
AS_OF='2026-01-12T00:00:00Z'
SEED=20261113
HEADER='source_event_id,occurred_at,action,barcode,loan_reference,due_on\n'


# ---------------------------------------------------------------------------
# Package construction helpers
# ---------------------------------------------------------------------------

def fixture_records():
    return load_jsonl_records(FIX)


def make_zip_bytes(members,*,compression=zipfile.ZIP_DEFLATED):
    """members: ordered list of (name, bytes[, ZipInfo kwargs])."""
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,'w',compression=compression) as zf:
        for item in members:
            name,data=item[0],item[1]
            kw=item[2] if len(item)>2 else {}
            info=zipfile.ZipInfo(name,date_time=(2026,1,1,0,0,0))
            info.compress_type=kw.get('compress_type',compression)
            info.create_system=kw.get('create_system',3)
            info.external_attr=kw.get('external_attr',0o100644<<16)
            if 'flag_bits' in kw:
                info.flag_bits=kw['flag_bits']
            zf.writestr(info,data)
    return buf.getvalue()


def manifest_bytes(parts,**overrides):
    manifest={
        'schema_version':1,
        'source_system':'synthetic-vendor',
        'export_id':'EXP-SYNTH-0001',
        'export_kind':'full',
        'parts':parts,
    }
    manifest.update(overrides)
    return json.dumps(manifest,ensure_ascii=False,separators=(',',':')).encode('utf-8')


def part_entry(name,data,*,rows=None):
    if rows is None:
        # Non-empty logical data records per the intake's CSV semantics.
        import csv as _csv
        parsed=list(_csv.reader(io.StringIO(data.decode('utf-8')),strict=True))
        rows=sum(1 for r in parsed[1:] if r and any(c!='' for c in r))
    return {'path':name,'sha256':hashlib.sha256(data).hexdigest(),'rows':rows}


def valid_package(tmp_path,name='export.zip',records=None,parts=3,seed=SEED):
    records=fixture_records() if records is None else records
    names=['csv/part-%02d.csv'%i for i in range(1,parts+1)]
    data,_manifest,_entries=build_package(records,names,seed=seed)
    path=tmp_path/name
    path.write_bytes(data)
    return path


def replace_member(zip_bytes,name,new_data):
    """Rebuild a ZIP replacing one member's payload, keeping member order."""
    out=io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zin, \
            zipfile.ZipFile(out,'w') as zout:
        for info in zin.infolist():
            data=new_data if info.filename==name else zin.read(info.filename)
            new_info=zipfile.ZipInfo(info.filename,date_time=info.date_time)
            new_info.compress_type=info.compress_type
            new_info.create_system=info.create_system
            new_info.external_attr=info.external_attr
            new_info.flag_bits=info.flag_bits
            zout.writestr(new_info,data)
    return out.getvalue()


def manifest_fingerprint_of(manifest):
    canonical=[{'path':p['path'],'rows':p['rows'],'sha256':p['sha256']}
               for p in sorted(manifest['parts'],key=lambda p:p['path'])]
    text=json.dumps(canonical,sort_keys=True,ensure_ascii=False,
                    separators=(',',':'))
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def logical_snapshot(db_path):
    with sqlite3.connect(db_path) as d:
        return {
            'events':d.execute('SELECT * FROM events ORDER BY event_id').fetchall(),
            'loans':d.execute('SELECT * FROM loans ORDER BY loan_id').fetchall(),
            'run_metadata':d.execute('SELECT * FROM run_metadata').fetchall(),
            'runs':d.execute('SELECT run_id,mode,source_system,export_id,'
                             'schema_version,export_kind,manifest_fingerprint,'
                             'source_rows,duplicate_rows,unique_events,part_count '
                             'FROM intake_runs').fetchall(),
            'raw_digests':d.execute('SELECT package_sha256,manifest_sha256 '
                                    'FROM intake_runs').fetchall(),
            'parts':d.execute('SELECT part_id,member_name,declared_sha256,'
                              'actual_sha256,declared_rows,actual_rows,raw_size '
                              'FROM intake_parts ORDER BY part_id').fetchall(),
            'occ':d.execute('SELECT occurrence_id,part_id,member_name,'
                            'record_index,event_id,payload '
                            'FROM intake_occurrences ORDER BY occurrence_id').fetchall(),
        }


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'

    def test_fixture_package_counts_and_business_rows(self):
        package=valid_package(self.root,parts=3)
        result=collect(package,AS_OF,self.db)
        # Six source records across three unordered parts: 1 duplicate, 5
        # unique events, 1 future event - exactly the JSONL fixture numbers.
        self.assertEqual((result['mode'],result['source_rows'],
                          result['duplicate_rows'],result['unique_events'],
                          result['future_events'],result['part_count']),
                         ('package_intake',6,1,5,1,3))
        self.assertEqual(result['summary'],[
            dict(status='closed',loans=1,overdue_loans=1,overdue_days=2,renewals=1),
            dict(status='open',loans=1,overdue_loans=1,overdue_days=4,renewals=0)])
        with sqlite3.connect(self.db) as d:
            # Direct key-by-key queries on all four business objects.
            l1=d.execute("SELECT * FROM loans WHERE loan_id='L1'").fetchone()
            l2=d.execute("SELECT * FROM loans WHERE loan_id='L2'").fetchone()
            self.assertEqual(l1,('L1','B1','2026-01-01T00:00:00Z','2026-01-10',
                                 '2026-01-12T00:00:00Z',1,2,'closed'))
            self.assertEqual(l2,('L2','B2','2026-01-02T00:00:00Z','2026-01-08',
                                 None,0,4,'open'))
            self.assertEqual(
                d.execute('SELECT event_id,event_at,applied FROM events ORDER BY event_id').fetchall(),
                [('E1','2026-01-01T00:00:00Z',1),('E2','2026-01-03T00:00:00Z',1),
                 ('E3','2026-01-12T00:00:00Z',1),('E4','2026-01-02T00:00:00Z',1),
                 ('E5','2026-01-13T00:00:00Z',0)])
            self.assertEqual(d.execute('SELECT * FROM status_summary ORDER BY status').fetchall(),
                             [('closed',1,1,2,1),('open',1,1,4,0)])
            self.assertEqual(d.execute('SELECT as_of,source_rows,duplicate_rows,'
                                       'future_events FROM run_metadata').fetchone(),
                             (AS_OF,6,1,1))

    def test_business_rows_match_jsonl_entry_key_for_key(self):
        package=valid_package(self.root,parts=2)
        collect(package,AS_OF,self.db)
        jsonl_db=self.root/'jsonl.sqlite'
        run(FIX,AS_OF,jsonl_db)
        with sqlite3.connect(self.db) as got,sqlite3.connect(jsonl_db) as ref:
            for table in ('events','loans','run_metadata'):
                self.assertEqual(
                    got.execute('SELECT * FROM %s ORDER BY 1,2,3,4'%table).fetchall(),
                    ref.execute('SELECT * FROM %s ORDER BY 1,2,3,4'%table).fetchall())
            self.assertEqual(
                got.execute('SELECT * FROM status_summary ORDER BY status').fetchall(),
                ref.execute('SELECT * FROM status_summary ORDER BY status').fetchall())
        # The JSONL database must not fabricate package provenance.
        with sqlite3.connect(jsonl_db) as ref:
            names={r[0] for r in ref.execute(
                "SELECT name FROM sqlite_master WHERE name LIKE 'intake_%'")}
            self.assertEqual(names,set())

    def test_equivalent_timezone_spelling_merges_but_keeps_every_occurrence(self):
        # The fixture already contains E1 twice: ...Z and +01:00 of the same
        # instant. Both occurrences must be preserved and merge into one event.
        package=valid_package(self.root,parts=2)
        collect(package,AS_OF,self.db)
        with sqlite3.connect(self.db) as d:
            rows=d.execute("SELECT member_name,record_index,payload "
                           "FROM intake_occurrences WHERE event_id='E1' "
                           "ORDER BY occurrence_id").fetchall()
            self.assertEqual(len(rows),2)
            payloads={r[2] for r in rows}
            self.assertEqual(len(payloads),1)  # identical canonical payload
            self.assertEqual(d.execute("SELECT count(*) FROM events WHERE event_id='E1'").fetchone()[0],1)

    def test_same_event_id_conflicting_payload_rejects_whole_package(self):
        records=fixture_records()
        # Clone an existing CHECKOUT identity with a different due date: the
        # id is the same but the normalized payload conflicts.
        source=next(r for r in records if r['action']=='CHECKOUT')
        conflict=dict(source,due_on='2026-02-01')
        records.append(conflict)
        names=['csv/part-%02d.csv'%i for i in (1,2)]
        data,_m,_e=build_package(records,names,seed=SEED)
        package=self.root/'conflict.zip';package.write_bytes(data)
        with self.assertRaisesRegex(IntakeError,'conflicting event ID'):
            collect(package,AS_OF,self.db)
        self.assertFalse(self.db.exists())

    def test_empty_parts_allowed_but_all_empty_package_rejected(self):
        empty=csv_bytes([])
        one=csv_bytes([fixture_records()[3]])  # E1 borrow: a valid lifecycle start
        good=make_zip_bytes([
            ('csv/empty.csv',empty),
            ('csv/one.csv',one),
            ('manifest.json',manifest_bytes([
                part_entry('csv/empty.csv',empty,rows=0),
                part_entry('csv/one.csv',one)])),
        ])
        good_path=self.root/'with-empty.zip';good_path.write_bytes(good)
        r=collect(good_path,AS_OF,self.db)
        self.assertEqual((r['source_rows'],r['unique_events']),(1,1))
        # Every part empty: declared rows are zero, no data records at all.
        blank=make_zip_bytes([
            ('csv/a.csv',empty),('csv/b.csv',empty),
            ('manifest.json',manifest_bytes([
                part_entry('csv/a.csv',empty,rows=0),
                part_entry('csv/b.csv',empty,rows=0)])),
        ])
        blank_path=self.root/'blank.zip';blank_path.write_bytes(blank)
        with self.assertRaisesRegex(IntakeError,'empty event stream'):
            collect(blank_path,AS_OF,self.root/'blank.sqlite')

    def test_csv_with_comma_quote_and_newline_identities(self):
        tricky_id='E6, "tricky"\nid'
        tricky_barcode='B4, "q"\ncode'
        tricky_loan='L4, "r"\nloan'
        row=','.join([
            '"E6, ""tricky""\nid"',
            '2026-01-12T00:00:00Z','CHECKOUT',
            '"B4, ""q""\ncode"','"L4, ""r""\nloan"','2026-01-19'])
        data=(HEADER+row+'\n').encode('utf-8')
        package_bytes=make_zip_bytes([
            ('csv/tricky.csv',data),
            ('manifest.json',manifest_bytes([part_entry('csv/tricky.csv',data)])),
        ])
        package=self.root/'tricky.zip';package.write_bytes(package_bytes)
        collect(package,AS_OF,self.db)
        with sqlite3.connect(self.db) as d:
            occ=d.execute("SELECT member_name,record_index,event_id,payload "
                          "FROM intake_occurrences").fetchone()
            # One logical record despite the embedded newline; index starts at 1.
            self.assertEqual((occ[0],occ[1],occ[2]),('csv/tricky.csv',1,tricky_id))
            payload=json.loads(occ[3])
            self.assertEqual(payload['copy_id'],tricky_barcode)
            self.assertEqual(payload['loan_id'],tricky_loan)
            self.assertEqual(d.execute("SELECT count(*) FROM intake_occurrences").fetchone()[0],1)
            self.assertEqual(d.execute("SELECT count(*) FROM intake_parts").fetchone()[0],1)


class ProvenanceTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'
        self.package=valid_package(self.root,parts=3)
        self.original_bytes=self.package.read_bytes()
        collect(self.package,AS_OF,self.db)

    def test_rebuild_capture_zip_manifest_and_parts_from_output(self):
        with zipfile.ZipFile(self.package) as zf:
            original={info.filename:zf.read(info.filename)
                      for info in zf.infolist()}
        with sqlite3.connect(self.db) as d:
            run_row=d.execute('SELECT package_bytes,package_sha256,manifest_bytes,'
                              'manifest_sha256 FROM intake_runs').fetchone()
            zip_bytes,zip_sha,manifest_raw,manifest_sha=run_row
            self.assertEqual(hashlib.sha256(zip_bytes).hexdigest(),zip_sha)
            self.assertEqual(zip_bytes,self.original_bytes)
            self.assertEqual(hashlib.sha256(manifest_raw).hexdigest(),manifest_sha)
            parts=d.execute('SELECT member_name,raw_bytes,actual_sha256,'
                            'declared_sha256,declared_rows,actual_rows '
                            'FROM intake_parts ORDER BY member_name').fetchall()
            archived={}
            for name,raw,actual,declared,dr,ar in parts:
                self.assertEqual(hashlib.sha256(raw).hexdigest(),actual)
                self.assertEqual(actual,declared)
                self.assertEqual(dr,ar)
                archived[name]=bytes(raw)
        # Remove the source ZIP entirely; the output must be self-contained.
        self.package.unlink()
        self.assertFalse(self.package.exists())
        self.assertEqual(set(archived)|{'manifest.json'},set(original))
        self.assertEqual(archived,{k:v for k,v in original.items() if k!='manifest.json'})
        self.assertEqual(
            hashlib.sha256(original['manifest.json']).hexdigest(),manifest_sha)

    def test_manifest_fingerprint_matches_independent_canonicalization(self):
        with zipfile.ZipFile(io.BytesIO(self.original_bytes)) as zf:
            manifest=json.loads(zf.read('manifest.json').decode('utf-8'))
        expected=manifest_fingerprint_of(manifest)
        with sqlite3.connect(self.db) as d:
            stored=d.execute('SELECT manifest_fingerprint FROM intake_runs').fetchone()[0]
        self.assertEqual(stored,expected)
        # Fingerprint is stable under parts-order permutation even though the
        # raw manifest digest changes.
        shuffled=dict(manifest,parts=list(reversed(manifest['parts'])))
        self.assertEqual(manifest_fingerprint_of(shuffled),expected)

    def test_every_occurrence_joins_to_its_unique_event(self):
        with sqlite3.connect(self.db) as d:
            # FK enforcement is part of the schema definition.
            fks=d.execute('PRAGMA foreign_key_list(intake_occurrences)').fetchall()
            self.assertTrue(any(f[3]=='event_id' and f[2]=='events' for f in fks))
            total=d.execute('SELECT count(*) FROM intake_occurrences').fetchone()[0]
            joined=d.execute(
                'SELECT count(*) FROM intake_occurrences o JOIN events e '
                'ON e.event_id=o.event_id').fetchone()[0]
            self.assertEqual(joined,total)
            # Duplicate and future occurrences are both retained, not just the
            # winning first appearance.
            dup=d.execute("SELECT count(*) FROM intake_occurrences WHERE event_id='E1'").fetchone()[0]
            self.assertEqual(dup,2)
            future=d.execute(
                "SELECT count(*) FROM intake_occurrences o JOIN events e "
                "ON e.event_id=o.event_id WHERE e.applied=0").fetchone()[0]
            self.assertEqual(future,1)
            # A complete trace row for each appearance.
            trace=d.execute(
                'SELECT o.member_name,o.record_index,o.event_id,e.event_at,e.applied '
                'FROM intake_occurrences o JOIN events e ON e.event_id=o.event_id '
                'ORDER BY o.occurrence_id').fetchall()
            self.assertEqual(len(trace),6)

    def test_capture_uses_one_snapshot_source_replace_is_ineffective(self):
        # Controlled hook, no sleeps: replace the source path after capture by
        # monkeypatching the single reader the intake uses. Everything
        # downstream must keep describing the originally captured bytes.
        original=intake.read_package_bytes
        captured_holder={}

        def spy(package,max_bytes):
            data=original(package,max_bytes)
            captured_holder['data']=data
            # Simulate the vendor swapping the file on disk after capture.
            Path(package).write_bytes(b'COMPLETELY DIFFERENT BYTES')
            return data
        intake.read_package_bytes=spy
        try:
            rebuilt=self.root/'rebuilt.sqlite'
            collect(self.package,AS_OF,rebuilt)
        finally:
            intake.read_package_bytes=original
        self.assertEqual(self.package.read_bytes(),b'COMPLETELY DIFFERENT BYTES')
        with sqlite3.connect(rebuilt) as d:
            stored=d.execute('SELECT package_bytes,package_sha256 FROM intake_runs').fetchone()
        self.assertEqual(bytes(stored[0]),captured_holder['data'])
        self.assertEqual(hashlib.sha256(stored[0]).hexdigest(),stored[1])
        self.assertEqual(logical_snapshot(self.db),logical_snapshot(rebuilt))


class DeterminismTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name)

    def _package(self,*,reverse_entries=False,reverse_parts=False,store=False):
        # Build the canonical parts once; every variant reuses the exact same
        # per-part bytes, so part digests/rows/sizes must stay identical.
        records=fixture_records()
        names=['csv/part-%02d.csv'%i for i in (1,2,3)]
        base_data,manifest,_entries=build_package(records,names,seed=SEED)
        with zipfile.ZipFile(io.BytesIO(base_data)) as zf:
            payload={info.filename:zf.read(info.filename)
                     for info in zf.infolist()}
        part_order=list(reversed(manifest['parts'])) if reverse_parts \
            else manifest['parts']
        new_manifest=dict(manifest,parts=part_order)
        payload['manifest.json']=json.dumps(
            new_manifest,ensure_ascii=False,separators=(',',':')).encode('utf-8')
        member_order=list(reversed(list(payload))) if reverse_entries \
            else list(payload)
        out=io.BytesIO()
        compression=zipfile.ZIP_STORED if store else zipfile.ZIP_DEFLATED
        with zipfile.ZipFile(out,'w',compression=compression) as zout:
            for name in member_order:
                ni=zipfile.ZipInfo(name,date_time=(2026,2,2,2,2,2))
                ni.compress_type=compression
                ni.external_attr=0o100644<<16
                zout.writestr(ni,payload[name])
        return out.getvalue()

    def test_entry_order_parts_order_and_compression_keep_semantics(self):
        base=self.root/'base.zip';base.write_bytes(self._package())
        shuffled=self.root/'shuffled.zip'
        shuffled.write_bytes(self._package(reverse_entries=True,reverse_parts=True,
                                           store=True))
        db1=self.root/'a.sqlite';db2=self.root/'b.sqlite'
        r1=collect(base,AS_OF,db1);r2=collect(shuffled,AS_OF,db2)
        snap1=logical_snapshot(db1);snap2=logical_snapshot(db2)
        # Raw ZIP/manifest digests honestly differ and are compared separately.
        self.assertNotEqual(snap1.pop('raw_digests'),snap2.pop('raw_digests'))
        self.assertEqual(snap1,snap2)
        # Raw ZIP/manifest digests honestly differ; the logical fingerprint
        # (parts order independent) stays the same.
        self.assertNotEqual(r1['package_sha256'],r2['package_sha256'])
        self.assertNotEqual(r1['manifest_sha256'],r2['manifest_sha256'])
        self.assertEqual(r1['manifest_fingerprint'],r2['manifest_fingerprint'])

    def test_identical_inputs_give_identical_stdout_bytes_and_logic(self):
        p1=self.root/'a.zip';p2=self.root/'b.zip'
        data=self._package()
        p1.write_bytes(data);p2.write_bytes(data)
        env=dict(os.environ,PYTHONPATH=str(ROOT/'src'))
        def run_cli(pkg,db):
            proc=subprocess.run(
                [sys.executable,'-m','circulation_flow.intake','--package',str(pkg),
                 '--as-of',AS_OF,'--database',str(db)],
                cwd=ROOT,env=env,capture_output=True,check=True)
            return proc.stdout
        out1=run_cli(p1,self.root/'a.sqlite')
        out2=run_cli(p2,self.root/'b.sqlite')
        self.assertEqual(out1,out2)  # byte-identical stdout JSON
        self.assertEqual(logical_snapshot(self.root/'a.sqlite'),
                         logical_snapshot(self.root/'b.sqlite'))


class PackageRejectionTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'
        self.good=valid_package(self.root)
        self.good_bytes=self.good.read_bytes()

    def _collect_bytes(self,data,name='bad.zip'):
        path=self.root/name;path.write_bytes(data)
        with self.assertRaises(IntakeError):
            collect(path,AS_OF,self.root/(name+'.sqlite'))

    def test_not_a_zip(self):
        self._collect_bytes(b'plain text, not a zip at all')

    def test_truncated_zip_and_crc_corruption(self):
        self._collect_bytes(self.good_bytes[:len(self.good_bytes)//2],'trunc.zip')
        # Flip a payload byte after the local header: CRC verification fails.
        corrupted=bytearray(self.good_bytes)
        corrupted[60]^=0xFF
        self._collect_bytes(bytes(corrupted),'crc.zip')

    def test_missing_part_extra_part_and_duplicate_member(self):
        with zipfile.ZipFile(io.BytesIO(self.good_bytes)) as zf:
            manifest=json.loads(zf.read('manifest.json'))
            members=[(i.filename,zf.read(i.filename)) for i in zf.infolist()]
        # Missing: manifest declares a part the ZIP does not contain.
        missing_manifest=dict(manifest,parts=manifest['parts']+[
            {'path':'csv/ghost.csv','sha256':'0'*64,'rows':1}])
        self._collect_bytes(make_zip_bytes(
            [(n,d) for n,d in members]+[
                ('manifest.json',manifest_bytes(missing_manifest['parts']))]),
            'missing.zip')
        # Extra: an undeclared member.
        self._collect_bytes(make_zip_bytes(
            members+[('csv/rogue.csv',csv_bytes([]))]),'extra.zip')
        # Duplicate member name.
        import warnings
        dup=io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore',UserWarning)  # zipfile duplicate-name warning
            with zipfile.ZipFile(dup,'w') as zout:
                for n,d in members:
                    zout.writestr(n,d)
                zout.writestr(members[0][0],b'duplicate payload')
        self._collect_bytes(dup.getvalue(),'dup.zip')

    def test_unsafe_member_names(self):
        one=csv_bytes(fixture_records()[:1])
        cases=[
            '/abs/path.csv','back\\slash.csv','C:/drive.csv','C:drive.csv',
            'seg//empty.csv','dot/./x.csv','dot/../x.csv','../escape.csv',
            'trailing/','x/\x00.csv',
        ]
        for bad in cases:
            with self.subTest(bad=bad):
                data=make_zip_bytes([
                    (bad,one),
                    ('manifest.json',manifest_bytes([
                        {'path':bad,'sha256':hashlib.sha256(one).hexdigest(),
                         'rows':1}]))])
                self._collect_bytes(data,'unsafe.zip')

    def test_directory_entry_symlink_encryption_and_compression(self):
        one=csv_bytes(fixture_records()[:1])
        digest=hashlib.sha256(one).hexdigest()
        # Explicit directory entry.
        self._collect_bytes(make_zip_bytes([
            ('csv/',b''),('csv/one.csv',one),
            ('manifest.json',manifest_bytes([
                {'path':'csv/one.csv','sha256':digest,'rows':1}]))]),
            'dir.zip')
        # Symlink member (Unix symlink mode).
        self._collect_bytes(make_zip_bytes([
            ('link.csv',b'target',{'create_system':3,
             'external_attr':(0o120777<<16)}),
            ('manifest.json',manifest_bytes([
                {'path':'link.csv','sha256':hashlib.sha256(b'target').hexdigest(),
                 'rows':1}]))]),'symlink.zip')
        # Encrypted flag set. ZipFile.writestr clears flag_bits, so set the
        # encryption bit (0x0001) directly in the local and central headers.
        enc=bytearray(make_zip_bytes([
            ('csv/one.csv',one),
            ('manifest.json',manifest_bytes([
                {'path':'csv/one.csv','sha256':digest,'rows':1}]))]))
        local=enc.find(b'PK\x03\x04')
        enc[local+6]|=0x01
        central=enc.find(b'PK\x01\x02')
        enc[central+8]|=0x01
        self._collect_bytes(bytes(enc),'encrypted.zip')
        # Unsupported compression type (bzip2 id).
        self._collect_bytes(make_zip_bytes([
            ('csv/one.csv',one,{'compress_type':12}),
            ('manifest.json',manifest_bytes([
                {'path':'csv/one.csv','sha256':digest,'rows':1}]))]),
            'bz2.zip')

    def test_duplicate_json_keys_and_manifest_typing(self):
        with zipfile.ZipFile(io.BytesIO(self.good_bytes)) as zf:
            parts=json.loads(zf.read('manifest.json'))['parts']
        dup_key=('{"schema_version":1,"schema_version":1,'
                 '"source_system":"s","export_id":"e","export_kind":"full",'
                 '"parts":[]}').encode('utf-8')
        self._collect_bytes(make_zip_bytes([('manifest.json',dup_key)]),
                            'dupkey.zip')
        def with_part(**change):
            return manifest_bytes([dict(parts[0],**change)])
        bad_manifests=[
            manifest_bytes(parts,schema_version=True),
            manifest_bytes(parts,schema_version=2),
            manifest_bytes(parts,export_kind='delta'),
            manifest_bytes(parts,source_system=' spaced '),
            manifest_bytes(parts,export_id=''),
            json.dumps({'schema_version':1,'source_system':'s','export_id':'e',
                        'export_kind':'full','parts':[]},
                       separators=(',',':')).encode('utf-8'),
            with_part(rows=True),
            with_part(rows=-1),
            with_part(sha256='ABCDEF'),
            with_part(path='manifest.json'),
        ]
        for i,raw in enumerate(bad_manifests):
            with self.subTest(manifest=i):
                with zipfile.ZipFile(io.BytesIO(self.good_bytes)) as zf:
                    members=[(info.filename,
                              raw if info.filename=='manifest.json'
                              else zf.read(info.filename))
                             for info in zf.infolist()]
                self._collect_bytes(make_zip_bytes(members),'mtype.zip')

    def test_hash_and_row_mismatch(self):
        with zipfile.ZipFile(io.BytesIO(self.good_bytes)) as zf:
            manifest=json.loads(zf.read('manifest.json'))
            first=manifest['parts'][0]['path']
            original_part=zf.read(first)
        wrong_sha=dict(manifest)
        wrong_sha['parts'][0]=dict(wrong_sha['parts'][0],sha256='0'*64)
        self._collect_bytes(replace_member(
            self.good_bytes,'manifest.json',manifest_bytes(wrong_sha['parts'])),
            'sha.zip')
        wrong_rows=dict(manifest)
        wrong_rows['parts'][0]=dict(wrong_rows['parts'][0],
                                    rows=wrong_rows['parts'][0]['rows']+1)
        self._collect_bytes(replace_member(
            self.good_bytes,'manifest.json',manifest_bytes(wrong_rows['parts'])),
            'rows.zip')
        # Actual CSV content changed without the manifest digest updating.
        tampered=replace_member(self.good_bytes,first,original_part+b'x\n')
        self._collect_bytes(tampered,'tamper.zip')

    def test_utf8_bom_and_csv_structure_errors(self):
        good_header=HEADER.encode('utf-8')
        valid_line=b'E1,2026-01-01T00:00:00Z,CHECKOUT,B1,L1,2026-01-05\n'
        cases={
            'bom.csv':b'\xef\xbb\xbf'+good_header+valid_line,
            'badutf8.csv':good_header+b'E1,\xff\xfe,CHECKOUT,B1,L1,2026-01-05\n',
            'header.csv':b'wrong,header\n'+valid_line,
            'cols.csv':good_header+b'E1,2026-01-01T00:00:00Z,CHECKOUT,B1\n',
            'quote.csv':good_header+b'E1,x,CHECKOUT,B"ad,L1,2026-01-05\n',
            'unterm.csv':good_header+b'"E1,x,CHECKOUT,B1,L1,2026-01-05\n',
            'trailing.csv':good_header+b'"E1"junk,x,CHECKOUT,B1,L1,2026-01-05\n',
            'empty.csv':b'',
        }
        for name,data in cases.items():
            with self.subTest(name=name):
                pkg=make_zip_bytes([
                    ('csv/'+name,data),
                    ('manifest.json',manifest_bytes([
                        {'path':'csv/'+name,
                         'sha256':hashlib.sha256(data).hexdigest(),
                         'rows':1}]))])
                self._collect_bytes(pkg,'csvbad.zip')
        # Manifest BOM and non-UTF8 manifest.
        self._collect_bytes(make_zip_bytes([
            ('manifest.json',b'\xef\xbb\xbf{}')]),'mbom.zip')
        self._collect_bytes(make_zip_bytes([
            ('manifest.json',b'\xff\xfe{}')]),'mutf8.zip')


class ByteBudgetTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'

    def test_max_bytes_parameter_validation(self):
        package=valid_package(self.root)
        for bad in (0,-1,1073741825,True,1.5,'16',None):
            with self.assertRaises(IntakeError):
                collect(package,AS_OF,self.db,max_bytes=bad)

    def test_raw_capture_boundary(self):
        # STORE makes raw size close to the decompressed total; pick a budget
        # exactly one byte below and exactly at the captured package size.
        records=fixture_records()
        names=['csv/p.csv']
        data,_m,_e=build_package(records,names,seed=SEED)
        # Rebuild with STORE for a predictable raw size.
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            members=[(i.filename,zf.read(i.filename)) for i in zf.infolist()]
        stored=make_zip_bytes(members,compression=zipfile.ZIP_STORED)
        path=self.root/'stored.zip';path.write_bytes(stored)
        size=len(stored)
        with self.assertRaisesRegex(IntakeError,'raw size'):
            collect(path,AS_OF,self.db,max_bytes=size-1)
        r=collect(path,AS_OF,self.root/'ok.sqlite',max_bytes=size)
        self.assertEqual(r['package_size'],size)

    def test_decompressed_total_boundary(self):
        # Highly compressible payload: raw ZIP is tiny while decompressed bytes
        # are large, so the second (decompressed) budget binds first.
        # Big quoted cell keeps column count at six; the decompressed budget is
        # checked before CSV field parsing.
        csv_data=(HEADER.encode()+
                  b'E1,2026-01-01T00:00:00Z,CHECKOUT,B1,L1,"2026-01-05'
                  +b'A'*20000+b'"\n')
        manifest=manifest_bytes([part_entry('csv/big.csv',csv_data)])
        pkg=make_zip_bytes([('csv/big.csv',csv_data),('manifest.json',manifest)])
        raw_size=len(pkg);decompressed=len(csv_data)+len(manifest)
        self.assertLess(raw_size,decompressed)  # compression actually worked
        path=self.root/'big.zip';path.write_bytes(pkg)
        # Budget above raw but below decompressed total must fail on extraction.
        budget=raw_size+10
        self.assertLess(budget,decompressed)
        with self.assertRaisesRegex(IntakeError,'decompressed'):
            collect(path,AS_OF,self.db,max_bytes=budget)
        # Budget at the decompressed total passes extraction; the giant
        # due_on string then fails CSV->event date validation instead.
        with self.assertRaisesRegex(IntakeError,'isoformat|due date'):
            collect(path,AS_OF,self.root/'edge.sqlite',max_bytes=decompressed)


class LifecycleAndFutureTest(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'

    def _package_from_rows(self,rows):
        data=HEADER.encode()+b''.join(rows)
        pkg=make_zip_bytes([
            ('csv/p.csv',data),
            ('manifest.json',manifest_bytes([part_entry('csv/p.csv',data)]))])
        path=self.root/'p.zip';path.write_bytes(pkg)
        return path

    def test_future_row_bad_field_rejected(self):
        # Future relative to AS_OF but structurally invalid (return with due).
        pkg=self._package_from_rows(
            [b'E9,2026-02-01T00:00:00Z,CHECKIN,B9,L9,2026-02-01\n'])
        with self.assertRaisesRegex(IntakeError,'CHECKIN due_on must be empty'):
            collect(pkg,AS_OF,self.db)

    def test_simultaneous_same_copy_and_bad_lifecycle_rejected(self):
        same_sec=b'E1,2026-01-01T00:00:00Z,CHECKOUT,B1,L1,2026-01-05\n' \
                 b'E2,2026-01-01T00:00:00Z,EXTEND,B1,L1,2026-01-09\n'
        with self.assertRaisesRegex(IntakeError,'simultaneous'):
            collect(self._package_from_rows([same_sec]),AS_OF,self.db)
        # Return without an active loan.
        orphan=b'E3,2026-01-01T00:00:00Z,CHECKIN,B1,L1,\n'
        with self.assertRaisesRegex(IntakeError,'no matching active loan'):
            collect(self._package_from_rows([orphan]),AS_OF,self.db)
        # Renewal that does not extend the due date.
        weak=(b'E1,2026-01-01T00:00:00Z,CHECKOUT,B1,L1,2026-01-05\n'
              b'E4,2026-01-02T00:00:00Z,EXTEND,B1,L1,2026-01-05\n')
        with self.assertRaisesRegex(IntakeError,'renewal must extend'):
            collect(self._package_from_rows([weak]),AS_OF,self.db)

    def test_identity_contract_is_not_trimmed_or_casefolded(self):
        rows=(b' E1 ,2026-01-01T00:00:00Z,CHECKOUT,B1,L1,2026-01-05\n',)
        # Whitespace inside an identity is preserved text (non-empty), so the
        # stored event_id must be the exact source spelling.
        pkg=self._package_from_rows(rows)
        collect(pkg,AS_OF,self.db)
        with sqlite3.connect(self.db) as d:
            self.assertEqual(
                d.execute('SELECT event_id FROM events').fetchone()[0],' E1 ')


class FailureInjectionTest(unittest.TestCase):
    class _FailingConnection:
        def __init__(self,real,fail_on,message):
            self._real=real;self._fail_on=fail_on;self._message=message
            self._failed=False

        def execute(self,sql,params=()):
            if not self._failed and self._fail_on in sql:
                self._failed=True
                raise sqlite3.OperationalError(self._message)
            return self._real.execute(sql,params)

        def __getattr__(self,name):
            return getattr(self._real,name)

    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        self.root=Path(t.name);self.db=self.root/'out.sqlite'
        self.package=valid_package(self.root)
        collect(self.package,AS_OF,self.db)
        self.good_db=self.db.read_bytes()
        self.good_zip=self.package.read_bytes()

    def _temp_residue(self):
        return [p.name for p in self.root.iterdir()
                if p.name.startswith('.circulation-') and p.name.endswith('.tmp')]

    def test_failure_after_business_materialization_during_provenance(self):
        real_connect=sqlite3.connect
        def patched(path,*args,**kwargs):
            conn=real_connect(path,*args,**kwargs)
            if str(path)!=str(self.db):
                return self._FailingConnection(
                    conn,'INSERT INTO intake_occurrences','injected provenance failure')
            return conn
        sqlite3.connect=patched
        try:
            with self.assertRaisesRegex(IntakeError,'injected provenance failure'):
                collect(self.package,AS_OF,self.db)
        finally:
            sqlite3.connect=real_connect
        self.assertEqual(self.db.read_bytes(),self.good_db)
        self.assertEqual(self.package.read_bytes(),self.good_zip)
        self.assertEqual(self._temp_residue(),[])
        # Corrected re-run succeeds directly.
        r=collect(self.package,AS_OF,self.db)
        self.assertEqual(r['source_rows'],6)

    def test_publish_boundary_failure_preserves_everything(self):
        old_replace=os.replace
        def boom(src,dst):
            raise OSError('injected rename failure')
        os.replace=boom
        try:
            with self.assertRaisesRegex(IntakeError,'injected rename failure'):
                collect(self.package,AS_OF,self.db)
        finally:
            os.replace=old_replace
        self.assertEqual(self.db.read_bytes(),self.good_db)
        self.assertEqual(self.package.read_bytes(),self.good_zip)
        self.assertEqual(self._temp_residue(),[])
        collect(self.package,AS_OF,self.db)

    def test_target_aliases_rejected(self):
        # Symlink alias pointing at the source ZIP.
        link=self.root/'link.zip'
        link.symlink_to(self.package.name)
        with self.assertRaisesRegex(IntakeError,'same file'):
            collect(self.package,AS_OF,link)
        # Target path that resolves to the package through a directory symlink.
        alias_dir=self.root/'alias';alias_dir.symlink_to(self.root,target_is_directory=True)
        with self.assertRaisesRegex(IntakeError,'same file'):
            collect(self.package,AS_OF,alias_dir/self.package.name)
        # Hard-link alias.
        hard=self.root/'hard.zip'
        os.link(self.package,hard)
        with self.assertRaisesRegex(IntakeError,'same file'):
            collect(self.package,AS_OF,hard)
        # Input bytes untouched after all rejections.
        self.assertEqual(self.package.read_bytes(),self.good_zip)
        self.assertEqual(self.db.read_bytes(),self.good_db)


class CliEndToEndTest(unittest.TestCase):
    def test_real_cli_subprocess_generate_collect_verify_rebuild(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        root=Path(t.name)
        package=root/'export.zip';db=root/'out.sqlite'
        env=dict(os.environ,PYTHONPATH=str(ROOT/'src'))
        # 1. Generate an original synthetic package through the tool CLI.
        gen=subprocess.run(
            [sys.executable,str(ROOT/'tools/generate_package.py'),str(package),
             '--source',str(FIX),'--parts','3','--seed',str(SEED),'--tricky'],
            cwd=ROOT,env=env,capture_output=True,text=True,check=True)
        gen_stats=json.loads(gen.stdout)
        self.assertTrue(package.exists())
        # 2. Collect through the real intake CLI.
        proc=subprocess.run(
            [sys.executable,'-m','circulation_flow.intake','--package',str(package),
             '--as-of',AS_OF,'--database',str(db)],
            cwd=ROOT,env=env,capture_output=True,text=True,check=True)
        summary=json.loads(proc.stdout)
        self.assertEqual(summary['mode'],'package_intake')
        self.assertEqual(summary['part_count'],3)
        self.assertEqual(summary['package_sha256'],gen_stats['package_sha256'])
        # 3. SQL business verification against the JSONL entry side by side.
        jsonl_db=root/'jsonl.sqlite'
        subprocess.run(
            [sys.executable,'-m','circulation_flow.cli','--source',str(FIX),
             '--as-of',AS_OF,'--database',str(jsonl_db)],
            cwd=ROOT,env=env,capture_output=True,check=True)
        with sqlite3.connect(db) as got,sqlite3.connect(jsonl_db) as ref:
            # as_of, duplicates and future counts match; source_rows has one
            # extra tricky record only in the package.
            self.assertEqual(
                got.execute('SELECT as_of,duplicate_rows,future_events '
                            'FROM run_metadata').fetchone(),
                ref.execute('SELECT as_of,duplicate_rows,future_events '
                            'FROM run_metadata').fetchone())
            self.assertEqual(
                got.execute('SELECT source_rows FROM run_metadata').fetchone()[0],7)
            # The six fixture events match key for key; the extra tricky record
            # (7th occurrence, its own loan L4) is preserved alongside them with
            # the exact quoted identity spelling.
            fixture_events=ref.execute('SELECT * FROM events ORDER BY event_id').fetchall()
            got_events=got.execute('SELECT * FROM events ORDER BY event_id').fetchall()
            self.assertEqual(got_events[:len(fixture_events)],fixture_events)
            fixture_loans=ref.execute('SELECT * FROM loans ORDER BY loan_id').fetchall()
            got_loans=got.execute('SELECT * FROM loans ORDER BY loan_id').fetchall()
            self.assertEqual(got_loans[:len(fixture_loans)],fixture_loans)
            tricky=got.execute(
                "SELECT event_id,applied,payload FROM events "
                "WHERE event_id LIKE 'E6%'").fetchone()
            tricky_payload=json.loads(tricky[2])
            self.assertEqual(tricky[0],'E6, "tricky"\nid')
            self.assertEqual(tricky_payload['copy_id'],'B4, "quote"\nbarcode')
            self.assertEqual(tricky_payload['loan_id'],'L4, "ref"\nloan')
            self.assertEqual(
                got.execute("SELECT count(*) FROM intake_occurrences").fetchone()[0],7)
        # 4. Rebuild every archived source byte and verify digests independently.
        rebuild_dir=root/'rebuilt';rebuild_dir.mkdir()
        with sqlite3.connect(db) as d:
            zip_bytes=d.execute('SELECT package_bytes FROM intake_runs').fetchone()[0]
            (rebuild_dir/'export.zip').write_bytes(bytes(zip_bytes))
        with zipfile.ZipFile(rebuild_dir/'export.zip') as zf:
            zf.extractall(rebuild_dir/'unzipped')
            for info in zf.infolist():
                with sqlite3.connect(db) as d:
                    if info.filename=='manifest.json':
                        stored=d.execute('SELECT manifest_bytes,manifest_sha256 '
                                         'FROM intake_runs').fetchone()
                    else:
                        stored=d.execute('SELECT raw_bytes,actual_sha256 FROM intake_parts '
                                         'WHERE member_name=?',(info.filename,)).fetchone()
                self.assertEqual(bytes(stored[0]),zf.read(info.filename))
                self.assertEqual(hashlib.sha256(bytes(stored[0])).hexdigest(),stored[1])
        # 5. A malformed package exits non-zero and leaves no database behind.
        bad=root/'bad.zip';bad.write_bytes(b'not a zip')
        fail=subprocess.run(
            [sys.executable,'-m','circulation_flow.intake','--package',str(bad),
             '--as-of',AS_OF,'--database',str(root/'nope.sqlite')],
            cwd=ROOT,env=env,capture_output=True,text=True)
        self.assertNotEqual(fail.returncode,0)
        self.assertIn('ZIP',fail.stderr)
        self.assertFalse((root/'nope.sqlite').exists())


if __name__=='__main__':
    unittest.main()

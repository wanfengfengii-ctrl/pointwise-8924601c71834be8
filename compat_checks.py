"""Cross-implementation differential: original commit vs current bounded pipeline.

Both implementations run in separate processes against identical streams.  The
comparison covers receipts, full record bytes, source_order/sequence, error
types for rejected streams, batch generation, conflict handling and reopening a
database written by the other implementation.

The original sources are read from a pristine archive of the initial commit
(``git archive`` of the root commit; its SHA256 is printed with the report),
never by importing the working tree's modules.

Run: python3.11 -B compat_checks.py
"""
import base64
import hashlib
import json
import os
import random
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent
BASELINE = Path(tempfile.gettempdir()) / 'catalogue-baseline-original'


def ensure_baseline():
    """Extract the initial (root) commit's sources into an isolated directory.

    Using the root commit rather than HEAD keeps the reference implementation
    fixed once this working tree is committed.
    """
    if (BASELINE / 'catalogue' / 'archive.py').exists():
        return BASELINE
    root = subprocess.check_output(
        ['git', 'rev-list', '--max-parents=0', 'HEAD'], cwd=ROOT,
        text=True).split()[0]
    packed = subprocess.check_output(['git', 'archive', root], cwd=ROOT)
    BASELINE.mkdir(parents=True, exist_ok=True)
    subprocess.run(['tar', '-x', '-C', str(BASELINE)], input=packed, check=True)
    return BASELINE


# ------------------------------------------------------------------ streams --

def simple(body=b'old', record_id='r'):
    return [('begin_group', 'g'), ('begin_record', record_id), ('body', body),
            ('end_record', True), ('verdict', True), ('end_group',), ('end_batch',)]


def random_catalogue(seed):
    """Random tree with repeated tags, early/late verdicts, arbitrary chunks."""
    rng = random.Random(seed)
    serial = 0

    def walk(depth):
        nonlocal serial
        events = []
        for _ in range(rng.randint(1, 4)):
            serial += 1
            if depth and rng.randrange(3) == 0:
                events.append(('begin_group', 'repeated'))
                early = rng.choice([True, False])
                approved = bool(rng.randrange(2))
                if early:
                    events.append(('verdict', approved))
                events.extend(walk(depth - 1))
                if not early:
                    events.append(('verdict', approved))
                events.append(('end_group',))
            else:
                identity = 'n' + str(serial)
                raw = rng.choice([b'', b'same', b'\x00\xff', bytes(range(37)),
                                  b'same'])
                events.append(('begin_record', identity))
                events.append(('body', b''))
                offset = 0
                while offset < len(raw):
                    width = rng.randint(1, 9)
                    events.append(('body', raw[offset:offset + width]))
                    offset += width
                events.append(('end_record', bool(rng.randrange(2))))
        return events
    return walk(4) + [('end_batch',)]


def rejected_catalogues():
    return [
        ('missing-end-batch', simple()[:-1]),
        ('event-after-end', simple() + [('end_group',)]),
        ('nested-record', [('begin_record', 'a'), ('begin_group', 'g'),
                           ('end_batch',)]),
        ('duplicate-id', [('begin_record', 'a'), ('end_record', True),
                          ('begin_record', 'a'), ('end_record', True),
                          ('end_batch',)]),
        ('group-without-verdict', [('begin_group', 'g'), ('end_group',),
                                   ('end_batch',)]),
        ('double-verdict', [('begin_group', 'g'), ('verdict', True),
                            ('verdict', True), ('end_group',), ('end_batch',)]),
        ('empty-id', [('begin_record', ''), ('end_record', True), ('end_batch',)]),
        ('body-outside-record', [('body', b'x'), ('end_batch',)]),
        ('int-verdict', [('begin_group', 'g'), ('verdict', 1),
                         ('end_group',), ('end_batch',)]),
        ('bad-arity', [('end_batch', 'extra')]),
    ]


# ------------------------------------------------------------ child driver --

def emit(archive, batch_id, stream):
    from catalogue import Archive  # noqa: F401  (ensures the module path)
    try:
        receipt = archive.ingest_batch(batch_id, iter(stream))
    except Exception as exc:  # report the observed category per stream
        print(json.dumps({'batch': batch_id, 'error': type(exc).__name__,
                          'message': str(exc)}, sort_keys=True))
        return
    with archive.read_records(batch_id) as records:
        rows = [{'sequence': r.sequence, 'source_order': r.source_order,
                 'id': r.id, 'sha256': r.sha256,
                 'body_b64': base64.b64encode(r.body).decode('ascii')}
                for r in records]
    print(json.dumps({'batch': batch_id, 'receipt': {
        'batch_id': receipt.batch_id, 'input_sha256': receipt.input_sha256,
        'generation': receipt.generation, 'record_count': receipt.record_count,
        'total_bytes': receipt.total_bytes,
        'manifest_sha256': receipt.manifest_sha256},
        'records': rows}, sort_keys=True))


def driver(payload_path):
    """Run one implementation's ingest sequence from a JSON payload.

    The payload uses a compact tagged encoding because JSON cannot carry bytes;
    bodies are base64 and event names are explicit.
    """
    spec = json.loads(Path(payload_path).read_text(encoding='utf-8'))
    from catalogue import Archive
    database = spec['database']
    outputs = []
    with Archive(database) as archive:
        for item in spec['batches']:
            stream = decode_events(item['events'])
            if item.get('conflict'):
                try:
                    archive.ingest_batch(item['batch'], iter(stream))
                except Exception as exc:
                    outputs.append({'batch': item['batch'],
                                    'error': type(exc).__name__,
                                    'message': str(exc)})
                    continue
            capture = []

            def run():
                try:
                    receipt = archive.ingest_batch(item['batch'], iter(stream))
                except Exception as exc:
                    capture.append({'batch': item['batch'],
                                    'error': type(exc).__name__,
                                    'message': str(exc)})
                    return
                with archive.read_records(item['batch']) as records:
                    rows = [{'sequence': r.sequence, 'source_order': r.source_order,
                             'id': r.id, 'sha256': r.sha256,
                             'body_b64': base64.b64encode(r.body).decode('ascii')}
                            for r in records]
                capture.append({'batch': item['batch'], 'receipt': {
                    'batch_id': receipt.batch_id,
                    'input_sha256': receipt.input_sha256,
                    'generation': receipt.generation,
                    'record_count': receipt.record_count,
                    'total_bytes': receipt.total_bytes,
                    'manifest_sha256': receipt.manifest_sha256},
                    'records': rows})
            run()
            outputs.extend(capture)
        reopen = spec.get('reopen', [])
        if reopen:
            with Archive(database) as again:
                for batch_id in reopen:
                    value = again.receipt(batch_id)
                    current = again.current()
                    outputs.append({'reopened': batch_id,
                                    'receipt': None if value is None else {
                                        'batch_id': value.batch_id,
                                        'input_sha256': value.input_sha256,
                                        'generation': value.generation,
                                        'record_count': value.record_count,
                                        'total_bytes': value.total_bytes,
                                        'manifest_sha256': value.manifest_sha256},
                                    'current': None if current is None else current.batch_id})
    for output in outputs:
        print(json.dumps(output, sort_keys=True))


def decode_events(encoded):
    result = []
    for item in encoded:
        kind = item[0]
        if kind == 'body':
            result.append((kind, base64.b64decode(item[1])))
        elif kind in ('begin_group', 'begin_record'):
            result.append((kind, item[1]))
        elif kind in ('end_record', 'verdict'):
            result.append((kind, bool(item[1])))
        else:
            result.append((kind,))
    return result


def encode_events(stream):
    encoded = []
    for event in stream:
        kind = event[0]
        if kind == 'body':
            encoded.append([kind, base64.b64encode(event[1]).decode('ascii')])
        elif kind in ('end_record', 'verdict'):
            encoded.append([kind, bool(event[1])])
        elif kind in ('begin_group', 'begin_record'):
            encoded.append([kind, event[1]])
        else:
            encoded.append([kind])
    return encoded


# ----------------------------------------------------------- orchestration --

def build_specs(database):
    """A sequence of batches that stresses identity, conflicts and generations."""
    batches = []
    for seed in range(12):
        batches.append({'batch': 'random-' + str(seed),
                        'events': encode_events(random_catalogue(seed))})
    sample_path = ROOT / 'samples' / 'directory.jsonl'
    sample_events = []
    for line in sample_path.read_text(encoding='utf-8').splitlines():
        item = json.loads(line)
        kind = item['event']
        if kind == 'body':
            sample_events.append(('body', base64.b64decode(item['data'])))
        elif kind in ('begin_group', 'begin_record'):
            sample_events.append((kind, item['id']))
        elif kind in ('end_record', 'verdict'):
            sample_events.append((kind, item['approved']))
        else:
            sample_events.append((kind,))
    batches.append({'batch': 'sample', 'events': encode_events(sample_events)})
    # Same input re-chunked differently -> identical receipt.
    rechunked = []
    rng = random.Random(3)
    for event in sample_events:
        if event[0] == 'body' and event[1]:
            data = event[1]
            offset = 0
            while offset < len(data):
                width = rng.randint(1, len(data) - offset)
                rechunked.append(('body', data[offset:offset + width]))
                offset += width
        else:
            rechunked.append(event)
    batches.append({'batch': 'sample', 'events': encode_events(rechunked)})
    for name, stream in rejected_catalogues():
        batches.append({'batch': name, 'events': encode_events(stream)})
    # Conflicting content for a committed ID.
    other = [('begin_group', 'g'), ('begin_record', 'r'), ('body', b'changed'),
             ('end_record', True), ('verdict', True), ('end_group',),
             ('end_batch',)]
    batches.append({'batch': 'random-0', 'events': encode_events(other),
                    'conflict': True})
    # Rejected bodies still contribute identity.
    rejected_body = [('begin_group', 'g'), ('begin_record', 'r'),
                     ('body', b'not published'), ('end_record', False),
                     ('verdict', True), ('end_group',), ('end_batch',)]
    batches.append({'batch': 'veto-identity',
                    'events': encode_events(rejected_body)})
    changed = list(rejected_body)
    changed[2] = ('body', b'also not published')
    batches.append({'batch': 'veto-identity', 'events': encode_events(changed),
                    'conflict': True})
    # A fully rejected batch publishes zero records.
    batches.append({'batch': 'zero', 'events': encode_events(
        [('begin_group', 'g'), ('begin_record', 'x'), ('body', b'x'),
         ('end_record', True), ('verdict', False), ('end_group',),
         ('end_batch',)])})
    return {'database': str(database), 'batches': batches,
            'reopen': ['sample', 'zero', 'random-0', 'veto-identity', 'missing']}


def run_implementation(sources, payload, label):
    environment = dict(os.environ, PYTHONPATH=os.pathsep.join(
        [str(sources), str(ROOT)]))
    process = subprocess.run(
        [sys.executable, '-B', '-c',
         'import sys; from compat_checks import driver; '
         'driver(sys.argv[1])', str(payload)],
        cwd=str(Path(payload).parent), env=environment, text=True,
        capture_output=True)
    if process.returncode != 0:
        raise AssertionError(label + ' failed:\n' + process.stderr)
    return [json.loads(line) for line in process.stdout.splitlines() if line]


def compare():
    baseline = ensure_baseline()
    with tempfile.TemporaryDirectory(prefix='catalogue-compat-') as folder:
        folder = Path(folder)
        original_payload = folder / 'original.json'
        current_payload = folder / 'current.json'
        original_spec = build_specs(folder / 'original.sqlite')
        current_spec = build_specs(folder / 'current.sqlite')
        original_payload.write_text(json.dumps(original_spec), encoding='utf-8')
        current_payload.write_text(json.dumps(current_spec), encoding='utf-8')
        original = run_implementation(baseline, original_payload, 'original')
        current = run_implementation(ROOT, current_payload, 'current')
        if original != current:
            for index, (left, right) in enumerate(zip(original, current)):
                if left != right:
                    print('first difference at output', index)
                    print('original:', json.dumps(left, sort_keys=True)[:1500])
                    print('current :', json.dumps(right, sort_keys=True)[:1500])
                    break
            raise AssertionError('implementations disagree')
        count = len(original)

        # Cross-read: a database written by the original is readable by the
        # current implementation, and vice versa (v1 schema compatibility).
        original_db = folder / 'original.sqlite'
        current_db = folder / 'current.sqlite'
        reopen_ids = current_spec['reopen']
        read_only = folder / 'original-read.json'
        read_only.write_text(json.dumps(
            {'database': str(original_db), 'batches': [], 'reopen': reopen_ids}),
            encoding='utf-8')
        reopened = run_implementation(ROOT, read_only, 'current-on-original')
        assert reopened == original[-len(reopen_ids):], 'cross-read disagreement'

        # The current implementation's database also opens under the original.
        check_payload = folder / 'check.json'
        check_payload.write_text(json.dumps(
            {'database': str(current_db), 'batches': [], 'reopen': reopen_ids}),
            encoding='utf-8')
        original_view = run_implementation(baseline, check_payload,
                                           'original-on-current')
        assert original_view == current[-len(reopen_ids):], (
            'original cannot read current database')
        return {'compared_outputs': count,
                'cross_read_both_directions': True,
                'batches': len(original_spec['batches']),
                'baseline_sources_sha256': baseline_digest()}


def baseline_digest():
    """Identity of the pristine reference sources used for the comparison."""
    digest = hashlib.sha256()
    for name in sorted(p.name for p in (BASELINE / 'catalogue').glob('*.py')):
        digest.update(name.encode('utf-8'))
        digest.update((BASELINE / 'catalogue' / name).read_bytes())
    return digest.hexdigest()


if __name__ == '__main__':
    print(json.dumps(compare(), sort_keys=True))

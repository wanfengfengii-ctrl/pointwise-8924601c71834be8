"""Independent differential and staging-fault checks for the bounded pipeline.

This entry is a development deliverable, not part of the frozen benchmark.  It
does not re-implement the production tree walk: the expected result comes from
tests/reference.py's declarative oracle, and it exercises

  * differential equality over random trees (repeated group tags, verdicts
    before/between/after children, arbitrary body chunking, empty bodies),
  * input identity across re-chunking and identity of rejected bodies,
  * injected faults at every *new* staging write/read boundary (append body,
    end record, seal, chunked read-back), observing live cleanup while the
    Archive object stays open,
  * bounded memory for large individual bodies (the incremental blob path).

Run: python3.11 -B streaming_checks.py
"""
import gc
import hashlib
import json
from pathlib import Path
import random
import tempfile
import tracemalloc

from catalogue import Archive, BatchConflict, StructureError
from catalogue import qualification
from catalogue.qualification import STAGE_CHUNK
from tests import reference

ROOT = Path(__file__).resolve().parent


# ---------------------------------------------------------------- fixtures --

def explicit_catalogue():
    """Repeated tags, early inner verdict, late outer verdict, re-chunked same body."""
    raw = b'\x00\xffA'
    return [
        ('begin_group', 'repeated'),                    # 0 outer
        ('begin_record', 'keep'),                       # 0 order 0
        ('body', raw[:1]), ('body', raw[1:]),
        ('end_record', True),
        ('begin_group', 'repeated'),                    # 1 inner, verdict early
        ('verdict', False),
        ('begin_record', 'omit'), ('body', b'not accepted'),
        ('end_record', True),
        ('end_group',),
        ('verdict', True),                              # 0 outer, verdict late
        ('end_group',),
        ('begin_record', 'twin'), ('body', raw), ('body', b''),
        ('end_record', True),                           # order 2
        ('begin_record', 'empty'), ('end_record', True),  # order 3
        ('end_batch',),
    ]


def big_body_tree(seed, total=16 * 1024 * 1024, piece=256 * 1024):
    """A tree whose records are larger than one staging chunk (blob path)."""
    rng = random.Random(seed)
    nodes = []
    written = 0
    number = 0
    while written < total:
        number += 1
        raw = rng.randbytes(piece)
        written += len(raw)
        nodes.append(dict(kind='record', id='big-' + str(number), body=raw,
                          approved=bool(rng.randrange(4))))
        if number % 3 == 0:
            nodes.append(dict(kind='group', id='shared', approved=True,
                              children=[dict(kind='record', id='inner-' + str(number),
                                             body=b'', approved=True)]))
    return [dict(kind='group', id='shared', approved=True, children=nodes)]


def chunks_of(raw, rng):
    yield ('body', b'')
    offset = 0
    while offset < len(raw):
        width = rng.choice([1, 7, 4093, 8191, 65536, 65537])
        yield ('body', raw[offset:offset + width])
        offset += width


def events_from_nodes(nodes, seed=0):
    rng = random.Random(seed)

    def walk(children):
        for node in children:
            if node['kind'] == 'group':
                yield ('begin_group', node['id'])
                early = rng.choice([True, False])
                if early:
                    yield ('verdict', node['approved'])
                yield from walk(node['children'])
                if not early:
                    yield ('verdict', node['approved'])
                yield ('end_group',)
            else:
                yield ('begin_record', node['id'])
                yield from chunks_of(node['body'], rng)
                yield ('end_record', node['approved'])
    yield from walk(nodes)
    yield ('end_batch',)


def read_all(archive, batch_id):
    with archive.read_records(batch_id) as records:
        return [(r.source_order, r.id, r.body, r.sha256) for r in records]


def scratch_dir():
    folder = tempfile.mkdtemp(prefix='catalogue-diff-scratch-')
    return Path(folder)


def with_scratch(path, function):
    previous = tempfile.tempdir
    tempfile.tempdir = str(path)
    try:
        return function()
    finally:
        tempfile.tempdir = previous


# -------------------------------------------------------------- differential --

def differential():
    archive_root = scratch_dir()
    scratch = archive_root / 'scratch'
    scratch.mkdir()
    checked = 0
    with Archive(archive_root / 'archive.sqlite') as archive:
        def run(label, nodes, seed):
            expected = reference.expected(nodes)
            receipt = archive.ingest_batch(label, events_from_nodes(nodes, seed))
            actual = read_all(archive, label)
            assert actual == expected, (label, 'differential mismatch')
            assert receipt.record_count == len(expected), label
            assert receipt.total_bytes == sum(len(row[2]) for row in expected), label
            assert [r[0] for r in actual] == sorted(r[0] for r in actual), label
            return receipt

        # Random trees: repeated 'group' tags occur naturally in the oracle.
        for seed in range(60):
            nodes = reference.tree(seed)
            run('tree-' + str(seed), nodes, seed)
            checked += 1

        # Explicit repeated-tag / early-and-late-verdict catalogue.
        raw = b'\x00\xffA'
        digest = hashlib.sha256(raw).hexdigest()
        receipt = archive.ingest_batch('explicit', iter(explicit_catalogue()))
        assert read_all(archive, 'explicit') == [
            (0, 'keep', raw, digest),
            (2, 'twin', raw, digest),
            (3, 'empty', b'', hashlib.sha256(b'').hexdigest())], 'explicit rows'
        assert receipt.record_count == 3 and receipt.total_bytes == 6
        checked += 1

        # Outer late veto accepts nothing but must still publish normally.
        vetoed = [
            ('begin_group', 'repeated'),
            ('begin_record', 'keep'), ('body', raw), ('end_record', True),
            ('verdict', False),                       # final outer review, late
            ('end_group',), ('end_batch',)]
        empty = archive.ingest_batch('vetoed', iter(vetoed))
        assert (empty.record_count, empty.total_bytes) == (0, 0)
        assert read_all(archive, 'vetoed') == []
        checked += 1

        # Re-chunking does not change input identity; rejected bodies do.
        original = explicit_catalogue()
        rechunked = []
        rng = random.Random(7)
        for event in original:
            if event[0] == 'body':
                data = event[1]
                if not data:
                    rechunked.append(event)
                    continue
                offset = 0
                while offset < len(data):
                    width = rng.randint(1, max(1, len(data) - offset))
                    rechunked.append(('body', data[offset:offset + width]))
                    offset += width
            else:
                rechunked.append(event)
        replay = archive.ingest_batch('explicit', iter(rechunked))
        assert replay == receipt, 're-chunked identity changed'
        rejected = [('begin_group', 'g'), ('begin_record', 'r'),
                    ('body', b'not published'), ('end_record', False),
                    ('verdict', True), ('end_group',), ('end_batch',)]
        first = archive.ingest_batch('rejected', iter(rejected))
        changed = list(rejected)
        changed[2] = ('body', b'also not published')
        try:
            archive.ingest_batch('rejected', iter(changed))
        except BatchConflict:
            pass
        else:
            raise AssertionError('rejected-body identity did not change')
        assert archive.receipt('rejected') == first
        checked += 1

        # Large records exercise the incremental blob path; the oracle compares
        # byte-for-byte despite bodies exceeding one staging chunk.
        big = big_body_tree(11)
        big_receipt = run('big', big, 11)
        assert big_receipt.record_count > 40
        assert all(len(row[2]) == 256 * 1024 for row in reference.expected(big)
                   if row[1].startswith('big-'))
        checked += 1

    assert list(scratch.iterdir()) == [], 'differential left staging entries'
    return checked


# --------------------------------------------------------- fault and cleanup --

def _fresh_staging_files(scratch):
    return sorted(p.name for p in scratch.iterdir())


def fault_probes():
    root = scratch_dir()
    scratch = root / 'scratch'
    scratch.mkdir()
    outcomes = {}
    sample = explicit_catalogue()
    old_raw = b'old-batch-body'

    def sample_receipt(archive):
        return archive.ingest_batch('stable', iter([
            ('begin_group', 'g'), ('begin_record', 'r'), ('body', old_raw),
            ('end_record', True), ('verdict', True), ('end_group',),
            ('end_batch',)]))

    def observe(archive, batch_id, stable, stage):
        """Fail once at `stage`; assert old results survive and scratch is clean."""
        before = archive.current()
        marker = OSError('injected ' + stage)
        original = getattr(qualification.Staging, stage)

        def broken(self, *arguments, **keywords):
            raise marker

        setattr(qualification.Staging, stage, broken)
        try:
            try:
                archive.ingest_batch(batch_id, iter(sample))
            except OSError as exc:
                assert exc is marker, (stage, 'original exception replaced', exc)
            else:
                raise AssertionError(stage + ': failure was published')
        finally:
            setattr(qualification.Staging, stage, original)
        # Observed while the Archive object is still alive.
        assert _fresh_staging_files(scratch) == [], (stage, 'staging left behind')
        assert archive.receipt(batch_id) is None, (stage, 'receipt leaked')
        assert archive.current() == before, (stage, 'current pointer moved')
        assert archive.receipt('stable') == stable, (stage, 'old receipt lost')
        assert read_all(archive, 'stable') == [(0, 'r', old_raw,
                                                hashlib.sha256(old_raw).hexdigest())]
        retry = archive.ingest_batch(batch_id, iter(sample))
        assert retry.record_count == 3, (stage, 'retry failed')
        assert _fresh_staging_files(scratch) == [], (stage, 'retry left staging')
        outcomes[stage] = True

    # Wrap the staging file creation in the isolated scratch directory too.
    def body(archive):
        stable = sample_receipt(archive)
        for stage in ('begin_group', 'begin_record', 'append_body', 'end_record',
                      'finalize'):
            # These run during validation, before publication.
            observe(archive, 'fault-' + stage, stable, stage)
        # Force the incremental blob path with a body larger than one chunk.
        large = [('begin_record', 'large')] + [
            ('body', b'z' * STAGE_CHUNK) for _ in range(3)] + [
            ('end_record', True), ('end_batch',)]
        before = archive.current()
        original = qualification.Staging.chunks

        def truncated(self, record, width=STAGE_CHUNK):
            produced = 0
            for piece in original(self, record, width):
                produced += len(piece)
                yield piece
                if produced >= STAGE_CHUNK:
                    raise OSError('injected chunks')

        qualification.Staging.chunks = truncated
        try:
            try:
                archive.ingest_batch('fault-chunks', iter(large))
            except OSError as exc:
                assert str(exc) == 'injected chunks'
            else:
                raise AssertionError('chunks failure was published')
        finally:
            qualification.Staging.chunks = original
        assert _fresh_staging_files(scratch) == [], 'chunks: staging left behind'
        assert archive.receipt('fault-chunks') is None
        assert archive.current() == before
        assert read_all(archive, 'stable') == [(0, 'r', old_raw,
                                                hashlib.sha256(old_raw).hexdigest())]
        assert archive.ingest_batch('fault-chunks', iter(large)).record_count == 1
        outcomes['chunks'] = True

        # Cleanup must not replace an in-flight failure with its own error.
        original_unlink = qualification.os.unlink

        def refusing_unlink(path, *arguments, **keywords):
            raise PermissionError('injected unlink')

        qualification.os.unlink = refusing_unlink
        try:
            # A source fault with a refusing cleanup must still surface the
            # original source exception, not the PermissionError.
            marker = StructureError('injected structural stop')

            def broken():
                yield from sample
                raise marker

            try:
                archive.ingest_batch('unlink-during-fault', broken())
            except StructureError as exc:
                assert exc is marker, 'cleanup replaced the original failure'
            else:
                raise AssertionError('unlink-during-fault was published')
            assert archive.receipt('unlink-during-fault') is None

            # A successful round: publication succeeds even if cleanup's unlink
            # is refused (only the scratch file is left for the outer process).
            receipt = archive.ingest_batch('unlink-tolerant', iter(sample))
            assert receipt.record_count == 3
        finally:
            qualification.os.unlink = original_unlink
        for path in scratch.iterdir():
            path.unlink()
        outcomes['cleanup_does_not_mask'] = True

        # Rejections (zero accepted) publish normally and release staging.
        veto = [('begin_group', 'g'), ('begin_record', 'r'), ('body', b'x'),
                ('end_record', True), ('verdict', False), ('end_group',),
                ('end_batch',)]
        receipt = archive.ingest_batch('all-veto', iter(veto))
        assert receipt.record_count == 0
        assert _fresh_staging_files(scratch) == []
        outcomes['veto_publishes'] = True
        return outcomes

    with Archive(root / 'archive.sqlite') as archive:
        def guarded():
            return body(archive)
        result = with_scratch(scratch, guarded)
    assert list(scratch.iterdir()) == [], 'fault probes left staging entries'
    return result


# ---------------------------------------------------------- bounded big body --

def bounded_big_body():
    """16MiB of 256KiB records must not scale Python peak with batch size."""
    root = scratch_dir()
    nodes = big_body_tree(23, total=16 * 1024 * 1024, piece=256 * 1024)
    events = events_from_nodes(nodes, 23)
    with Archive(root / 'archive.sqlite') as archive:
        gc.collect()
        tracemalloc.start()
        receipt = archive.ingest_batch('bounded', events)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert receipt.record_count == len(reference.expected(nodes))
        assert peak <= 4 * 1024 * 1024, ('big-body peak', peak)
        sample = [row for row in read_all(archive, 'bounded') if row[1].startswith('big-')][:2]
        assert len(sample) == 2
        assert all(len(body) == 256 * 1024 for _, _, body, _ in sample)
    return {'python_peak_bytes': peak, 'record_count': receipt.record_count}


def main():
    report = {
        'differential_catalogues': differential(),
        'staging_faults': fault_probes(),
        'bounded_big_body': bounded_big_body(),
    }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

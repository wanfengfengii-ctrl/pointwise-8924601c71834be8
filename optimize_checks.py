"""Original regressions, live rollback/resource probes, and frozen ingestion loads."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import statistics
import subprocess
import sys
import tempfile

from catalogue import Archive, StructureError

ROOT = Path(__file__).resolve().parent
MIB = 1024 * 1024

# Fixed workload receipts, independently derived from README framing and expected rows.
EXPECTED = {
    ('late-veto', 64): (
        '1e4c4ca709093a8d78a5aabd0258b100ee526907bafb8378edb4a7d7dda7e86c',
        '9a3cda527ebebcd3d9ba65ae88af6b594273e261cd1cc08d0cf7f1e7645fda24', 0, 0),
    ('late-veto', 256): (
        '6c51c4dd9cbaf95d9ec833ffd279bb261cdcf32ca57308d0a97b0449fea4c1ed',
        '9a3cda527ebebcd3d9ba65ae88af6b594273e261cd1cc08d0cf7f1e7645fda24', 0, 0),
    ('interleaved', 64): (
        '646cdef5a5c4849b7b9867984ce88c8d6a9a1be9de10588b154d1b24a226a3c0',
        '43274450dc731545f18250778aa58b6f7384806c47a4a05015ec91153e22c665', 726, 47579136),
    ('interleaved', 256): (
        '329d438864b7166ab1221783c8908d0ffced39e9d4e039a0c92064d0cd7e1ec8',
        '46e85920c6804a40f298370ca96973b318412d7ddfed8c3b0be6c6deb6a924bd', 2909, 190644224),
}


def sample():
    yield ('begin_group', 'repeated')
    yield ('begin_record', 'keep')
    yield ('body', b'\x00')
    yield ('body', b'\xffA')
    yield ('end_record', True)
    yield ('begin_group', 'repeated')
    yield ('begin_record', 'omit')
    yield ('body', b'not accepted')
    yield ('end_record', True)
    yield ('verdict', False)
    yield ('end_group',)
    yield ('verdict', True)
    yield ('end_group',)
    yield ('begin_record', 'twin')
    yield ('body', b'\x00\xffA')
    yield ('end_record', True)
    yield ('begin_record', 'empty')
    yield ('end_record', True)
    yield ('end_batch',)


def verify_sample(archive, batch_id):
    raw = b'\x00\xffA'
    digest = hashlib.sha256(raw).hexdigest()
    with archive.read_records(batch_id) as rows:
        actual = [(r.sequence, r.source_order, r.id, r.body, r.sha256) for r in rows]
    assert actual == [
        (0, 0, 'keep', raw, digest),
        (1, 2, 'twin', raw, digest),
        (2, 3, 'empty', b'', hashlib.sha256(b'').hexdigest())]


def live_probes():
    # The archive is outside the standard temporary workspace. Inspect it while
    # the writer is still open, rather than relying on process-exit cleanup.
    with tempfile.TemporaryDirectory(prefix='catalogue-live-') as folder:
        root = Path(folder)
        scratch = root / 'scratch'
        scratch.mkdir()
        previous = tempfile.tempdir
        tempfile.tempdir = str(scratch)
        try:
            with Archive(root / 'archive.sqlite') as archive:
                def clean():
                    assert list(scratch.iterdir()) == [], 'per-ingestion temporary entries remain'
                    allowed = {'archive.sqlite', 'scratch', 'archive.sqlite-wal',
                               'archive.sqlite-shm', 'archive.sqlite-journal'}
                    assert {p.name for p in root.iterdir()} <= allowed, 'unexpected temporary entry'

                old = archive.ingest_batch('old', sample())
                verify_sample(archive, 'old')
                clean()

                def failure(batch_id, source, error_type, marker=None):
                    before = archive.current()
                    try:
                        archive.ingest_batch(batch_id, source)
                    except error_type as exc:
                        if marker is not None:
                            assert exc is marker, 'original exception was replaced'
                    else:
                        raise AssertionError('failed ingestion was accepted')
                    assert archive.current() == before
                    assert archive.receipt(batch_id) is None
                    assert archive.receipt('old') == old
                    verify_sample(archive, 'old')
                    clean()

                def bad_tail():
                    yield from sample()
                    yield ('end_group',)

                failure('bad-tail', bad_tail(), StructureError)
                marker = OSError('source failed after end_batch')

                def broken():
                    yield from sample()
                    raise marker

                failure('source-error', broken(), OSError, marker)

                class CloseFailure:
                    def __init__(self):
                        self.values = iter(sample())
                        self.closed = False

                    def __iter__(self):
                        return self

                    def __next__(self):
                        return next(self.values)

                    def close(self):
                        self.closed = True
                        raise marker

                closing = CloseFailure()
                failure('close-error', closing, OSError, marker)
                assert closing.closed

                for wanted in ('body_staged', 'record_written', 'before_commit'):
                    marker = OSError(wanted)

                    def observe(phase, detail):
                        if phase == wanted:
                            raise marker

                    archive.observer = observe
                    try:
                        failure('observer-' + wanted, sample(), OSError, marker)
                    finally:
                        archive.observer = None
                    receipt = archive.ingest_batch('observer-' + wanted, sample())
                    assert receipt.record_count == 3 and receipt.total_bytes == 6
                    verify_sample(archive, receipt.batch_id)
                    clean()

                archive._connection.execute(
                    "CREATE TRIGGER resource_probe_reject BEFORE INSERT ON records "
                    "BEGIN SELECT RAISE(ABORT,'probe storage rejection'); END")
                try:
                    failure('storage', sample(), sqlite3.IntegrityError)
                finally:
                    archive._connection.execute('DROP TRIGGER resource_probe_reject')
                archive.ingest_batch('storage', sample())
                verify_sample(archive, 'storage')
                clean()
            with Archive(root / 'archive.sqlite') as reopened:
                assert reopened.receipt('old') == old
                assert reopened.ingest_batch('old', sample()) == old
                verify_sample(reopened, 'old')
                assert list(scratch.iterdir()) == []
        finally:
            tempfile.tempdir = previous
    return {'late_verdict_occurrences': True, 'binary_empty_and_order': True,
            'source_close_tail_failures': True, 'observer_and_storage_rollback': True,
            'live_temporary_entries_released': True, 'reopen_retry': True}


def check_results(report):
    assert len(report['reports']) == len(EXPECTED)
    assert {(g['case'], g['input_mib']) for g in report['reports']} == set(EXPECTED)
    for group in report['reports']:
        key = group['case'], group['input_mib']
        assert len(group['runs']) == 3
        for run in group['runs']:
            actual = (run['input_sha256'], run['manifest_sha256'],
                      run['record_count'], run['total_bytes'])
            assert actual == EXPECTED[key], (key, actual)


def check_limits(report):
    expected = {(case, size) for case in ('late-veto', 'interleaved') for size in (64, 256)}
    assert len(report['reports']) == 4
    assert {(r['case'], r['input_mib']) for r in report['reports']} == expected
    medians = {}
    for group in report['reports']:
        runs = group['runs']
        assert len(runs) == 3 and {r['trial'] for r in runs} == {1, 2, 3}
        peaks = []
        for run in runs:
            assert (run['case'], run['input_mib']) == (group['case'], group['input_mib'])
            peak = run['python_peak_bytes']
            assert type(peak) is int and 0 <= peak <= 32 * MIB, (
                group['case'], group['input_mib'], run['trial'], peak)
            peaks.append(peak)
        medians[group['case'], group['input_mib']] = statistics.median(peaks)
    for case in ('late-veto', 'interleaved'):
        growth = medians[case, 256] - medians[case, 64]
        assert growth <= 4 * MIB, (case, 'fourfold median peak growth', growth)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('baseline', 'target'))
    args = parser.parse_args()
    subprocess.run([sys.executable, '-B', str(ROOT / 'checks.py')], cwd=ROOT, check=True)
    probes = live_probes()
    with tempfile.TemporaryDirectory(prefix='catalogue-measure-') as folder:
        env = dict(os.environ, TMPDIR=folder, TEMP=folder, TMP=folder, SQLITE_TMPDIR=folder)
        raw = subprocess.check_output(
            [sys.executable, '-B', str(ROOT / 'bench.py'), 'full'],
            cwd=ROOT, env=env, text=True)
        report = json.loads(raw)
        assert list(Path(folder).iterdir()) == [], 'measurement left temporary entries'
    check_results(report)
    if args.mode == 'target':
        check_limits(report)
    print(json.dumps({'mode': args.mode, 'live_probes': probes,
                      'measurement': report}, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

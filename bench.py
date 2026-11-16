"""Run complete ingestion in fresh processes and print reproducible JSON metrics."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc

from catalogue import Archive
import workloads

ROOT = Path(__file__).resolve().parent


def worker(case, mib):
    with tempfile.TemporaryDirectory(prefix='catalogue-bench-') as directory:
        with Archive(Path(directory) / 'archive.sqlite') as archive:
            # No event iterator or body exists before the measured window.
            tracemalloc.start()
            started = time.perf_counter()
            receipt = archive.ingest_batch('measured', workloads.events(case, mib))
            elapsed = time.perf_counter() - started
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            # Full bytes, order, identities and hashes are checked after measurement.
            workloads.verify(archive, receipt, case, mib)
            result = dict(case=case, input_mib=mib, python_peak_bytes=peak,
                          elapsed_seconds=elapsed, record_count=receipt.record_count,
                          total_bytes=receipt.total_bytes,
                          input_sha256=receipt.input_sha256,
                          manifest_sha256=receipt.manifest_sha256)
        if sorted(p.name for p in Path(directory).iterdir()) != ['archive.sqlite']:
            raise AssertionError('unexpected archive resources after close')
        return result


def run(sizes, rounds):
    reports = []
    for case in workloads.CASES:
        for mib in sizes:
            runs = []
            for trial in range(rounds):
                raw = subprocess.check_output(
                    [sys.executable, '-B', str(ROOT / 'bench.py'), 'worker',
                     '--case', case, '--mib', str(mib)], cwd=ROOT, text=True)
                item = json.loads(raw)
                item['trial'] = trial + 1
                runs.append(item)
            assert len({(r['input_sha256'], r['manifest_sha256'],
                         r['record_count'], r['total_bytes']) for r in runs}) == 1
            reports.append(dict(case=case, input_mib=mib, runs=runs,
                                median_peak_bytes=statistics.median(
                                    r['python_peak_bytes'] for r in runs)))
    growth = []
    if len(sizes) == 2:
        for case in workloads.CASES:
            pair = [r for r in reports if r['case'] == case]
            growth.append(dict(case=case, scale=sizes[1] / sizes[0],
                               median_peak_growth_bytes=pair[1]['median_peak_bytes']
                               - pair[0]['median_peak_bytes']))
    return dict(reports=reports, growth=growth)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['full', 'smoke', 'worker'])
    parser.add_argument('--case', choices=workloads.CASES, default=workloads.CASES[0])
    parser.add_argument('--mib', type=int, default=1)
    args = parser.parse_args()
    if args.mode == 'worker':
        report = worker(args.case, args.mib)
    else:
        report = run([64, 256] if args.mode == 'full' else [1], 3 if args.mode == 'full' else 1)
    print(json.dumps(report, sort_keys=True))


if __name__ == '__main__':
    main()

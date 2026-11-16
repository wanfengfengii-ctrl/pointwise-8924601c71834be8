"""Command-line access to the same public archive and source APIs."""
import argparse
import base64
from dataclasses import asdict
import json
from pathlib import Path
import tempfile

from . import Archive, StructureError, open_source


def emit(value):
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def demo():
    sample = Path(__file__).resolve().parents[1] / 'samples' / 'directory.jsonl'
    with tempfile.TemporaryDirectory(prefix='catalogue-demo-') as directory:
        with Archive(Path(directory) / 'archive.sqlite') as archive:
            result = archive.ingest_batch('sample', open_source(sample, chunk_size=2))
            replay = archive.ingest_batch('sample', open_source(sample, chunk_size=3))
            assert result == replay
            with archive.read_records('sample') as rows:
                records = [dict(id=row.id, body_b64=base64.b64encode(row.body).decode('ascii'),
                                sha256=row.sha256, source_order=row.source_order) for row in rows]

            def invalid_tail():
                yield from open_source(sample)
                yield ('end_group',)
            try:
                archive.ingest_batch('invalid', invalid_tail())
            except StructureError:
                pass
            else:
                raise AssertionError('invalid tail was published')
            assert archive.current() == result
            assert archive.receipt('invalid') is None
            emit(dict(receipt=asdict(result), replay_equal=result == replay,
                      invalid_tail_rejected=True, records=records))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    ingest = commands.add_parser('ingest')
    ingest.add_argument('database')
    ingest.add_argument('batch_id')
    ingest.add_argument('source')
    ingest.add_argument('--chunk-size', type=int, default=65536)
    listing = commands.add_parser('list')
    listing.add_argument('database')
    listing.add_argument('batch_id')
    receipt = commands.add_parser('receipt')
    receipt.add_argument('database')
    receipt.add_argument('batch_id')
    commands.add_parser('demo')
    args = parser.parse_args()
    if args.command == 'demo':
        demo()
        return
    with Archive(args.database) as archive:
        if args.command == 'ingest':
            emit(asdict(archive.ingest_batch(
                args.batch_id, open_source(args.source, chunk_size=args.chunk_size))))
        elif args.command == 'receipt':
            result = archive.receipt(args.batch_id)
            emit(None if result is None else asdict(result))
        else:
            with archive.read_records(args.batch_id) as rows:
                for row in rows:
                    emit(dict(id=row.id, sequence=row.sequence, source_order=row.source_order,
                              body_b64=base64.b64encode(row.body).decode('ascii'), sha256=row.sha256))


if __name__ == '__main__':
    main()

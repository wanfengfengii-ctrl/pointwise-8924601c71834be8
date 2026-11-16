from pathlib import Path
import tempfile
import unittest

from catalogue import Archive, StructureError, open_source
from tests.common import read

SAMPLE = Path(__file__).resolve().parents[1] / 'samples' / 'directory.jsonl'


class SourceTests(unittest.TestCase):
    def test_sample_and_rechunk(self):
        with Archive(':memory:') as archive:
            first = archive.ingest_batch('sample', open_source(SAMPLE, chunk_size=1))
            second = archive.ingest_batch('sample', open_source(SAMPLE, chunk_size=7))
            self.assertEqual(first, second)
            self.assertEqual([(r[0], r[1]) for r in read(archive, 'sample')],
                             [('r1', b'same'), ('r3', b'same'), ('r4', b'')])

    def test_file_errors_and_validation(self):
        with tempfile.TemporaryDirectory(prefix='catalogue-source-') as directory:
            path = Path(directory) / 'input.jsonl'
            path.write_text('{"event":"body","data":"%%%"}\n', encoding='utf-8')
            with self.assertRaises(StructureError), Archive(':memory:') as archive:
                archive.ingest_batch('bad', open_source(path))
            with self.assertRaises(FileNotFoundError):
                next(open_source(Path(directory) / 'missing'))
            for size in (0, -1, True, 1.5):
                with self.assertRaises(ValueError):
                    open_source(path, chunk_size=size)
            # Early closing an opened iterator releases the actual file.
            path.write_text('{"event":"end_batch"}\n', encoding='utf-8')
            source = open_source(path)
            self.assertEqual(next(source), ('end_batch',))
            source.close()
            self.assertEqual(list(source), [])
            path.unlink()

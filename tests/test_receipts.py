from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from catalogue import Archive, BatchConflict
from tests.common import MemoryCase, read, simple


class ReceiptTests(MemoryCase):
    def test_rechunk_and_rejected_input_identity(self):
        initial = simple(b'abc')
        result = self.archive.ingest_batch('same', iter(initial))
        split = initial[:2] + [('body', b'a'), ('body', b''), ('body', b'bc')] + initial[3:]
        self.assertEqual(self.archive.ingest_batch('same', iter(split)), result)
        rejected = simple(b'not published')
        rejected[3] = ('end_record', False)
        self.archive.ingest_batch('rejected', iter(rejected))
        changed = list(rejected)
        changed[2] = ('body', b'also not published')
        with self.assertRaises(BatchConflict):
            self.archive.ingest_batch('rejected', iter(changed))
        changed_review = list(rejected)
        changed_review[4] = ('verdict', False)
        with self.assertRaises(BatchConflict):
            self.archive.ingest_batch('rejected', iter(changed_review))
        latest = self.archive.current()
        self.assertEqual(self.archive.ingest_batch('same', iter(split)), result)
        self.assertEqual(self.archive.current(), latest)

    def test_conflicting_retry(self):
        original = self.archive.ingest_batch('id', iter(simple()))
        with self.assertRaises(BatchConflict):
            self.archive.ingest_batch('id', iter(simple(b'new')))
        self.assertEqual(self.archive.receipt('id'), original)
        self.assertEqual(self.archive.current(), original)
        self.assertEqual(read(self.archive, 'id')[0][1], b'old')

    def test_invalid_batch_id(self):
        for value in ('', None, 1, 'a\x00b'):
            with self.assertRaises(ValueError):
                self.archive.receipt(value)
            with self.assertRaises(ValueError):
                self.archive.ingest_batch(value, iter(simple()))
        self.assertIsNone(self.archive.receipt('missing'))


class PersistenceTests(unittest.TestCase):
    def test_commit_failure_reopen_retry(self):
        with tempfile.TemporaryDirectory(prefix='catalogue-receipts-') as directory:
            path = Path(directory) / 'archive.sqlite'
            marker = OSError('before commit')
            with Archive(path) as archive:
                old = archive.ingest_batch('old', iter(simple()))

                def observe(phase, detail):
                    if phase == 'before_commit':
                        # A separate real connection cannot see this transaction.
                        with closing(sqlite3.connect(path)) as reader:
                            self.assertEqual(reader.execute(
                                'SELECT batch_id FROM current_batch').fetchone(), ('old',))
                            self.assertEqual(reader.execute(
                                "SELECT COUNT(*) FROM batches WHERE batch_id='new'").fetchone()[0], 0)
                        raise marker
                archive.observer = observe
                with self.assertRaises(OSError) as raised:
                    archive.ingest_batch('new', iter(simple(b'new')))
                self.assertIs(raised.exception, marker)
                self.assertEqual(archive.current(), old)
                self.assertIsNone(archive.receipt('new'))
            with Archive(path) as reopened:
                self.assertEqual(reopened.current(), old)
                result = reopened.ingest_batch('new', iter(simple(b'new')))
                self.assertEqual(result.generation, 2)
                self.assertEqual(reopened.ingest_batch('new', iter(simple(b'new'))), result)
                self.assertEqual(read(reopened, 'new')[0][1], b'new')
                self.assertEqual(read(reopened, 'old')[0][1], b'old')
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ['archive.sqlite'])

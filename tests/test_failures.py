import sqlite3
from catalogue import StructureError
from tests.common import MemoryCase, read, simple


class FailureTests(MemoryCase):
    def test_malformed_preserves_old(self):
        old = self.archive.ingest_batch('old', iter(simple()))
        invalid = simple(b'new')[:-3] + [('end_group',), ('end_batch',)]
        with self.assertRaises(StructureError):
            self.archive.ingest_batch('new', iter(invalid))
        with self.assertRaises(StructureError):
            self.archive.ingest_batch('tail', iter(simple(b'new') + [('body', b'tail')]))
        self.assertEqual(self.archive.current(), old)
        self.assertIsNone(self.archive.receipt('new'))
        self.assertEqual(read(self.archive, 'old')[0][1], b'old')

    def test_source_failure_and_retry(self):
        marker = OSError('finite source interrupted')
        closed = []

        def interrupted():
            try:
                yield from simple(b'partial')[:3]
                raise marker
            finally:
                closed.append(True)
        with self.assertRaises(OSError) as raised:
            self.archive.ingest_batch('retry', interrupted())
        self.assertIs(raised.exception, marker)
        self.assertEqual(closed, [True])
        self.assertIsNone(self.archive.current())
        good = self.archive.ingest_batch('retry', iter(simple(b'whole')))
        self.assertEqual(self.archive.ingest_batch('retry', iter(simple(b'whole'))), good)

    def test_error_after_end_batch_is_still_failure(self):
        marker = OSError('failure after final marker')

        def broken_tail():
            yield from simple()
            raise marker
        with self.assertRaises(OSError) as raised:
            self.archive.ingest_batch('tail', broken_tail())
        self.assertIs(raised.exception, marker)
        self.assertIsNone(self.archive.receipt('tail'))

    def test_close_failure_prevents_publish(self):
        marker = OSError('source close failed')

        class Source:
            def __init__(self):
                self.values = iter(simple())
                self.closed = False

            def __iter__(self):
                return self

            def __next__(self):
                return next(self.values)

            def close(self):
                self.closed = True
                raise marker
        source = Source()
        with self.assertRaises(OSError) as raised:
            self.archive.ingest_batch('close', source)
        self.assertIs(raised.exception, marker)
        self.assertTrue(source.closed)
        self.assertIsNone(self.archive.current())

    def test_staging_and_last_verdict_failures(self):
        old = self.archive.ingest_batch('old', iter(simple()))
        marker = OSError('stage failed')

        def observe(phase, detail):
            if phase == 'body_staged':
                raise marker
        self.archive.observer = observe
        with self.assertRaises(OSError) as raised:
            self.archive.ingest_batch('new', iter(simple(b'new')))
        self.assertIs(raised.exception, marker)
        self.assertEqual(self.archive.current(), old)
        self.archive.observer = None

        def final_review_failure():
            yield from simple(b'new')[:4]
            raise marker
        with self.assertRaises(OSError):
            self.archive.ingest_batch('new', final_review_failure())
        result = self.archive.ingest_batch('new', iter(simple(b'new')))
        self.assertEqual(result.generation, old.generation + 1)

    def test_real_sqlite_write_error_rolls_back(self):
        old = self.archive.ingest_batch('old', iter(simple()))
        connection = self.archive._connection
        connection.execute("CREATE TRIGGER reject_record BEFORE INSERT ON records "
                           "BEGIN SELECT RAISE(ABORT,'storage rejection'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.archive.ingest_batch('new', iter(simple(b'new')))
        self.assertFalse(connection.in_transaction)
        self.assertIsNone(self.archive.receipt('new'))
        self.assertEqual(self.archive.current(), old)
        connection.execute('DROP TRIGGER reject_record')
        self.archive.ingest_batch('new', iter(simple(b'new')))
        self.assertEqual(read(self.archive, 'new')[0][1], b'new')

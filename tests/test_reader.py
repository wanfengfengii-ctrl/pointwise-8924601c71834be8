import hashlib
from catalogue import IntegrityError
from tests.common import MemoryCase, read, simple


class ReaderTests(MemoryCase):
    def test_equal_content_distinct_occurrences(self):
        stream = [('begin_record', 'a'), ('body', b'\x00\xffabc'), ('end_record', True),
                  ('begin_record', 'b'), ('body', b'\x00'), ('body', b'\xffab'),
                  ('body', b'c'), ('end_record', True), ('end_batch',)]
        receipt = self.archive.ingest_batch('two', iter(stream))
        rows = read(self.archive, 'two')
        self.assertEqual([r[0] for r in rows], ['a', 'b'])
        self.assertEqual([r[1] for r in rows], [b'\x00\xffabc'] * 2)
        self.assertEqual([r[2] for r in rows], [hashlib.sha256(b'\x00\xffabc').hexdigest()] * 2)
        self.assertEqual(receipt.record_count, 2)
        self.assertEqual(receipt.total_bytes, 10)

    def test_close_exhaustion_and_missing(self):
        self.archive.ingest_batch('id', iter(simple()))
        records = self.archive.read_records('id')
        self.assertEqual(next(records).id, 'r')
        records.close()
        records.close()
        self.assertEqual(list(records), [])
        with self.archive.read_records('id') as exhausted:
            self.assertEqual(len(list(exhausted)), 1)
            self.assertTrue(exhausted.closed)
        with self.assertRaises(KeyError):
            self.archive.read_records('missing')

    def test_corruption_closes_reader(self):
        self.archive.ingest_batch('id', iter(simple()))
        self.archive._connection.execute("UPDATE records SET body=X'00' WHERE batch_id='id'")
        reader = self.archive.read_records('id')
        with self.assertRaises(IntegrityError):
            next(reader)
        self.assertTrue(reader.closed)

    def test_missing_row_is_manifest_failure(self):
        self.archive.ingest_batch('id', iter(simple()))
        self.archive._connection.execute("DELETE FROM records WHERE batch_id='id'")
        reader = self.archive.read_records('id')
        with self.assertRaises(IntegrityError):
            list(reader)
        self.assertTrue(reader.closed)

    def test_source_order_gap_is_preserved(self):
        stream = [('begin_record', 'no'), ('end_record', False),
                  ('begin_record', 'yes'), ('end_record', True), ('end_batch',)]
        self.archive.ingest_batch('gap', iter(stream))
        with self.archive.read_records('gap') as records:
            row = next(records)
            self.assertEqual((row.sequence, row.source_order), (0, 1))

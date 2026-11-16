import hashlib
from catalogue import StructureError
from tests.common import MemoryCase, read, simple
from tests.reference import events, expected, tree


class IngestTests(MemoryCase):
    def test_all_and_empty(self):
        stream = simple(b'')
        receipt = self.archive.ingest_batch('empty-body', iter(stream))
        self.assertEqual(receipt.record_count, 1)
        self.assertEqual(receipt.total_bytes, 0)
        self.assertEqual(read(self.archive, 'empty-body'),
                         [('r', b'', hashlib.sha256(b'').hexdigest())])
        empty = self.archive.ingest_batch('empty-batch', iter([('end_batch',)]))
        self.assertEqual(empty.record_count, 0)
        self.assertEqual(read(self.archive, 'empty-batch'), [])

    def test_late_outer_veto(self):
        old = self.archive.ingest_batch('old', iter(simple()))
        stream = [
            ('begin_group', 'outer'), ('begin_record', 'r1'), ('body', b'a'),
            ('end_record', True), ('begin_group', 'inner'),
            ('begin_record', 'r2'), ('body', b'b'), ('end_record', True),
            ('verdict', True), ('end_group',), ('verdict', False),
            ('end_group',), ('end_batch',)]
        veto = self.archive.ingest_batch('veto', iter(stream))
        self.assertEqual(veto.record_count, 0)
        self.assertEqual(read(self.archive, 'veto'), [])
        self.assertEqual(read(self.archive, 'old')[0][1], b'old')
        self.assertEqual(self.archive.receipt('old'), old)
        self.assertEqual(self.archive.current(), veto)

    def test_outer_pass_inner_veto(self):
        stream = [
            ('begin_group', 'outer'), ('begin_record', 'direct'), ('body', b'a'),
            ('end_record', True), ('begin_group', 'inner'), ('verdict', False),
            ('begin_record', 'nested'), ('body', b'b'), ('end_record', True),
            ('end_group',), ('begin_record', 'self-veto'), ('body', b'c'),
            ('end_record', False), ('verdict', True), ('end_group',), ('end_batch',)]
        self.archive.ingest_batch('mixed', iter(stream))
        self.assertEqual([row[:2] for row in read(self.archive, 'mixed')], [('direct', b'a')])

    def test_differential(self):
        for seed in range(80):
            nodes = tree(seed)
            self.archive.ingest_batch(str(seed), events(nodes, seed))
            with self.archive.read_records(str(seed)) as records:
                actual = [(r.source_order, r.id, r.body, r.sha256) for r in records]
            self.assertEqual(actual, expected(nodes), 'seed=' + str(seed))

    def test_deep_tree_is_not_python_recursion(self):
        depth = 1500
        stream = ([('begin_group', 'repeated')] * depth
                  + [('begin_record', 'x'), ('end_record', True)]
                  + [('verdict', True), ('end_group',)] * depth + [('end_batch',)])
        self.archive.ingest_batch('deep', iter(stream))
        self.assertEqual(read(self.archive, 'deep')[0][1], b'')

    def test_invalid_structure(self):
        cases = [
            [], [('body', b'x'), ('end_batch',)],
            [('begin_record', 'a'), ('end_record', 1), ('end_batch',)],
            [('begin_record', 'a'), ('body', bytearray(b'x')), ('end_record', True), ('end_batch',)],
            [('begin_group', 'g'), ('end_group',), ('end_batch',)],
            [('begin_group', 'g'), ('verdict', True), ('verdict', False), ('end_group',), ('end_batch',)],
            [('verdict', True), ('end_batch',)],
            [('end_group',), ('end_batch',)],
            [('end_batch',), ('end_batch',)],
            [('begin_record', 'a'), ('begin_group', 'g')],
            [('begin_record', 'a'), ('end_record', True),
             ('begin_record', 'a'), ('end_record', True), ('end_batch',)],
            [('end_batch', 'extra')], [('unknown',)],
            [('begin_record', ''), ('end_record', True), ('end_batch',)],
        ]
        for number, stream in enumerate(cases):
            with self.subTest(number=number), self.assertRaises(StructureError):
                self.archive.ingest_batch('invalid-' + str(number), iter(stream))
            self.assertIsNone(self.archive.receipt('invalid-' + str(number)))

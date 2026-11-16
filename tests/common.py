"""Shared finite event fixtures."""
import unittest
from catalogue import Archive


def simple(body=b'old', record_id='r'):
    return [('begin_group', 'g'), ('begin_record', record_id), ('body', body),
            ('end_record', True), ('verdict', True), ('end_group',), ('end_batch',)]


def read(archive, batch_id):
    with archive.read_records(batch_id) as records:
        return [(item.id, item.body, item.sha256) for item in records]


class MemoryCase(unittest.TestCase):
    def setUp(self):
        self.archive = Archive(':memory:')
        self.addCleanup(self.archive.close)

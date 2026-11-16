import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class CliTests(unittest.TestCase):
    def call(self, *arguments):
        output = subprocess.check_output([sys.executable, '-B', '-m', 'catalogue', *map(str, arguments)],
                                         cwd=ROOT, text=True)
        return [json.loads(line) for line in output.splitlines()]

    def test_real_file_ingest_reopen_and_demo(self):
        with tempfile.TemporaryDirectory(prefix='catalogue-cli-') as directory:
            path = Path(directory) / 'archive.sqlite'
            original = self.call('ingest', path, 'sample', ROOT / 'samples' / 'directory.jsonl')[0]
            self.assertEqual(self.call('receipt', path, 'sample')[0], original)
            self.assertEqual(self.call('ingest', path, 'sample', ROOT / 'samples' / 'directory.jsonl',
                                       '--chunk-size', 1)[0], original)
            self.assertEqual([r['id'] for r in self.call('list', path, 'sample')], ['r1', 'r3', 'r4'])
        result = self.call('demo')[0]
        self.assertTrue(result['replay_equal'])
        self.assertTrue(result['invalid_tail_rejected'])
        self.assertEqual(result['receipt']['record_count'], 3)

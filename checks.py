"""Complete original regression suite followed by fresh-process measurement smoke."""
from pathlib import Path
import subprocess
import sys
import unittest


def main():
    root = Path(__file__).resolve().parent
    suite = unittest.defaultTestLoader.discover(str(root / 'tests'), top_level_dir=str(root))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    subprocess.run([sys.executable, '-B', str(root / 'bench.py'), 'smoke'],
                   cwd=root, check=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

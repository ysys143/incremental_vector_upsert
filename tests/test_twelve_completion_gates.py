"""Small mutation checks of the actual twelve-hour reporting entry point."""
from pathlib import Path
import tempfile
import time
import unittest

from vectorbench.evidence.contracts import save, sha
from vectorbench.reporting.twelve_report import report


class CompletionGateTests(unittest.TestCase):
    def test_missing_history_prevents_report_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); auth = root / 'authorization.json'
            now = time.time()
            save(auth, {'t0': now, 'deadline': now + 43200})
            manifest = {'authorization': {'path': str(auth), 'sha256': sha(auth)},
                'execution_window_end': now + 43200, 'task_seconds': 43200,
                'nodes': {}, 'matrix': {'histories': [{'id': 'required-history'}]}}
            with self.assertRaisesRegex(AssertionError, 'missing declared histories/fresh'):
                report(root, manifest, {}, root)
            self.assertFalse((root / 'completion.json').exists())
            self.assertFalse((root / 'report.md').exists())


if __name__ == '__main__':
    unittest.main()

"""Regression checks for the moved package and frozen source coverage."""

import ast
from pathlib import Path
import tempfile
import unittest

from vectorbench.evidence.source_manifest import SYNTHETIC_PLAN, freeze_sources, source_files
from vectorbench.evidence.contracts import sha


class ArchitectureBoundaryTests(unittest.TestCase):
    def test_every_live_python_consumer_is_in_the_frozen_snapshot(self):
        live = {*Path("src/vectorbench").rglob("*.py"), *Path("scripts").glob("*.py")}
        self.assertTrue(live.issubset(set(source_files())))
        self.assertTrue({Path("pyproject.toml"), Path("requirements/base.txt"),
                         Path("requirements/orchestration.txt")}.issubset(source_files()))
        with tempfile.TemporaryDirectory(prefix="ivu-source-regression-") as directory:
            frozen = freeze_sources(Path(directory), SYNTHETIC_PLAN)
            self.assertEqual(set(frozen), {str(path) for path in source_files(SYNTHETIC_PLAN)})
            for name, digest in frozen.items():
                self.assertEqual(sha(Path(directory) / "source" / name), digest)

    def test_subprocess_module_targets_exist_after_move(self):
        count = 0
        for path in (*Path("src/vectorbench").rglob("*.py"), *Path("scripts").glob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, (ast.List, ast.Tuple)):
                    continue
                for left, right in zip(node.elts, node.elts[1:]):
                    if not (isinstance(left, ast.Constant) and left.value == "-m"
                            and isinstance(right, ast.Constant) and isinstance(right.value, str)):
                        continue
                    module = right.value
                    if not module.startswith(("vectorbench.", "scripts.")):
                        continue
                    count += 1
                    target = (Path("src") if module.startswith("vectorbench.") else Path()) / module.replace(".", "/")
                    self.assertTrue(target.with_suffix(".py").is_file() or (target / "__init__.py").is_file(), module)
        self.assertGreater(count, 0)


if __name__ == "__main__":
    unittest.main()

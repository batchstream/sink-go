import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("coverage_gate", Path(__file__).with_name("check-coverage.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class CoverageGateTests(unittest.TestCase):
    def test_union_weights_statements_and_excludes_generated_code(self):
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.out"
            second = Path(directory) / "second.out"
            first.write_text("mode: atomic\nmod/pkg/a.go:1.1,2.1 9 0\nmod/pkg/a.go:3.1,4.1 1 7\nmod/gen/a.pb.go:1.1,2.1 100 0\n")
            second.write_text("mode: atomic\nmod/pkg/a.go:1.1,2.1 9 1\nmod/pkg/a.go:3.1,4.1 1 0\n")
            self.assertEqual(gate.coverage([first]), {"mod/pkg": [10, 1]})
            self.assertEqual(gate.coverage([first, second]), {"mod/pkg": [10, 10]})
            second.write_text("mode: atomic\nmod/pkg/a.go:1.1,2.1 8 1\n")
            with self.assertRaises(ValueError):
                gate.coverage([first, second])

    def test_missing_empty_and_malformed_evidence_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            profile = Path(directory) / "coverage.out"
            for text in ["", "mode: atomic\n", "mode: atomic\ninvalid\n", "mode: unknown\n"]:
                profile.write_text(text)
                with self.assertRaises(ValueError):
                    gate.coverage([profile])
        self.assertTrue(gate.check({}, {"mod/pkg": 80}))
        self.assertTrue(gate.check({"mod/pkg": [10000, 7999]}, {"mod/pkg": 80}))
        self.assertFalse(gate.check({"mod/pkg": [10000, 8000]}, {"mod/pkg": 80}))
        with self.assertRaises(ValueError):
            gate.check({}, {"mod/pkg": 101})

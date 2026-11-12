import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("harness", Path(__file__).resolve().parents[1] / "harness.py")
h = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(h)


def metrics():
    return dict(requests=100, duration_us=1_000_000, rps=100, p50_us=10, p95_us=20, p99_us=30,
                ok=100, limited=0, other=0, connect_errors=0, read_errors=0, write_errors=0, timeouts=0)


class HarnessTests(unittest.TestCase):
    def parse(self, m):
        return h.parse_wrk("ordinary output\n" + h.MARKER + json.dumps(m) + "\n")

    def test_valid_output(self):
        self.assertEqual(self.parse(metrics())["rps"], 100)

    def test_missing_machine_record(self):
        with self.assertRaises(ValueError):
            h.parse_wrk("Requests/sec: 100")

    def test_multiple_records(self):
        line = h.MARKER + json.dumps(metrics()) + "\n"
        with self.assertRaises(ValueError):
            h.parse_wrk(line * 2)

    def test_missing_metric(self):
        for key in metrics():
            m = metrics()
            del m[key]
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.parse(m)

    def test_negative_nan_infinite_and_boolean_rejected(self):
        for value in [-1, float("nan"), float("inf"), True, "100"]:
            m = metrics()
            m["rps"] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.parse(m)

    def test_status_total_required(self):
        m = metrics()
        m["ok"] = 99
        with self.assertRaises(ValueError):
            self.parse(m)

    def test_no_requests_rejected(self):
        m = metrics()
        m.update(requests=0, ok=0)
        with self.assertRaises(ValueError):
            self.parse(m)

    def test_invalid_percentiles(self):
        m = metrics()
        m["p95_us"] = 100
        with self.assertRaises(ValueError):
            self.parse(m)

    def test_allow_requires_no_limit(self):
        m = metrics()
        m.update(ok=99, limited=1)
        self.assertTrue(h.assess(m, "head", "allow"))
        self.assertFalse(h.assess(m, "head", "mixed"))

    def test_deny_requires_no_admissions(self):
        self.assertTrue(h.assess(metrics(), "head", "deny"))
        self.assertFalse(h.assess(metrics(), "baseline", "deny"))

    def test_backend_failures_are_not_performance_success(self):
        m = metrics()
        m.update(ok=0, other=100)
        self.assertTrue(h.assess(m, "head", "mixed"))

    def test_transport_failure(self):
        for key in ("connect_errors", "read_errors", "write_errors", "timeouts"):
            m = metrics()
            m[key] = 1
            self.assertTrue(h.assess(m, "baseline", "allow"))

    def test_matrix(self):
        self.assertEqual(len(h.cases(False)), 5)
        self.assertEqual(len(h.cases(True)), 9)
        self.assertEqual(len(set(h.cases(True))), 9)

    def test_report_presents_failure_without_claiming_wins(self):
        row = dict(backend="redis", variant="head", clients=100, mode="mixed", repeat=0,
                   id="case", status="error", error="Redis OOM")
        text = h.summarize([row])
        self.assertIn("0/1", text)
        self.assertIn("Redis OOM", text)
        self.assertIn("different admission semantics", text)

    def test_repeated_run_median(self):
        rows = []
        for n, rps in enumerate([10, 20, 1000]):
            rows.append(dict(backend="memory", variant="head", clients=100, mode="allow", repeat=n,
                             id=str(n), status="ok", metrics={**metrics(), "rps": rps}))
        self.assertIn("median=20.0", h.summarize(rows))

    def test_reports_are_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            h.write_reports(Path(tmp), [])
            self.assertEqual(json.loads((Path(tmp) / "measurements.json").read_text()), [])
            self.assertTrue((Path(tmp) / "assessment.md").exists())


if __name__ == "__main__":
    unittest.main()

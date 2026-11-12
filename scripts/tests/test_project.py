"""Repository structural checks, explicitly NOT a substitute for Rust compilation."""
from __future__ import annotations
from pathlib import Path
import re
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[2]


class ProjectTests(unittest.TestCase):
    def test_all_toml_files_parse(self):
        paths = [p for p in ROOT.rglob('*.toml') if 'target' not in p.parts]
        for path in paths:
            with self.subTest(path=path.relative_to(ROOT)):
                tomllib.loads(path.read_text())

    def test_documentation_relative_links_exist(self):
        for path in ROOT.rglob('*.md'):
            if 'target' in path.parts:
                continue
            for target in re.findall(r'\]\(([^)]+)\)', path.read_text()):
                if ':' in target or target.startswith('#'):
                    continue
                target = target.split('#', 1)[0]
                self.assertTrue((path.parent / target).exists(), f'{path}: {target}')

    def test_performance_is_not_run_in_functional_ci(self):
        source = (ROOT / '.github/workflows/CI.yml').read_text()
        self.assertNotIn('cargo bench ', source)
        self.assertNotIn('cargo test --locked --all-targets', source)
        manifest = tomllib.loads((ROOT / 'Cargo.toml').read_text())
        for bench in manifest['bench']:
            self.assertFalse(bench['test'])

    def test_release_defaults_to_dry_run_and_requires_committed_locks(self):
        source = (ROOT / '.github/workflows/release.yml').read_text()
        self.assertRegex(source, r'dry_run:\n(?:[^\n]*\n){0,5}\s+default: true')
        self.assertIn('git ls-files --error-unmatch Cargo.lock benchmarks/Cargo.lock', source)
        self.assertIn('environment: crates-io', source)
        self.assertNotIn('pull_request_target:', source)

    def test_redis_tests_never_flush_database(self):
        for path in (ROOT / 'tests').glob('redis*.rs'):
            self.assertNotRegex(path.read_text(), r'(?i)redis::cmd\("flush(?:db|all)"\)')

    def test_benchmark_dependency_is_isolated(self):
        library = tomllib.loads((ROOT / 'Cargo.toml').read_text())
        bench = tomllib.loads((ROOT / 'benchmarks/Cargo.toml').read_text())
        self.assertNotIn('tower_governor', library['dependencies'])
        self.assertIn('workspace', bench)
        self.assertFalse(bench['package']['publish'])

    def test_workflows_parse_when_pyyaml_is_available(self):
        try:
            import yaml
        except ImportError:
            self.skipTest('PyYAML absent; CI actionlint validates workflow syntax separately')
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            doc = yaml.load(path.read_text(), Loader=yaml.BaseLoader)
            self.assertIn('on', doc, str(path))
            self.assertIn('jobs', doc, str(path))
            for name, job in doc['jobs'].items():
                if 'uses' in job:
                    self.assertTrue((ROOT / job['uses']).exists())
                    continue
                self.assertIn('runs-on', job, name)
                self.assertIn('timeout-minutes', job, name)
                self.assertTrue(job.get('steps'), name)

    def test_no_untrusted_expression_interpolation_in_shell(self):
        # User-dispatch inputs must be passed through environment variables.
        for path in (ROOT / '.github/workflows').glob('*.yml'):
            for line in path.read_text().splitlines():
                if 'python3 ' in line or 'cargo publish ' in line:
                    self.assertNotIn('${{ inputs.', line, str(path))

    def test_redis_example_is_feature_gated(self):
        manifest = tomllib.loads((ROOT / 'Cargo.toml').read_text())
        example = next(x for x in manifest['example'] if x['name'] == 'redis')
        self.assertEqual(example['required-features'], ['redis'])


if __name__ == '__main__':
    unittest.main()

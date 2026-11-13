"""Regression coverage for causal controls and readable product comparisons."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[1]))
import comparison
import diagnose
import report
import verdicts
import workflow
from test_campaign import row

class DiagnosisTest(unittest.TestCase):
    def test_short_diagnostics_cannot_become_formal_comparisons(self):
        rows=[row(v,repeat=i,measurement_seconds=10,campaign_stage='diagnostic',
                  diagnostic_profile='wait-warm',admission_policy='wait',instrumented=True)
              for i in range(3) for v in ('baseline','head')]
        self.assertFalse(comparison.scorecard(rows))
        self.assertFalse(verdicts.analyze(rows)['capacity'])
        cells=diagnose.summarize(rows)
        self.assertEqual(len(cells),2)
        self.assertTrue(all(c['rounds']==3 for c in cells))
        self.assertIn('not five-repeat capacity claims',diagnose.markdown(dict(backend='memory',rate=35000,cells=cells)))

    def test_scorecard_retains_failed_repeats_and_marks_policy_mismatch(self):
        rows=[row(v,repeat=i,server_cpu_pct=40 if v=='head' else 20,
                  scenario='memory/1/512/mixed/10000rps',workload_mode='mixed',
                  http_200=500,http_429=500) for i in range(5) for v in ('baseline','head','governor')]
        for r in rows:
            if r['variant']=='baseline':r.update(http_200=1000,http_429=0)
        cells=comparison.scorecard(rows)
        peer=next(c for c in cells if c['variant']=='governor')
        self.assertEqual(peer['cpu_vs_head'],.5)
        rows[-1]['dropped_capacity']=1
        peer=next(c for c in comparison.scorecard(rows) if c['variant']=='governor')
        self.assertEqual(peer['recorded'],5)
        self.assertEqual(peer['passing'],4)
        self.assertIsNone(peer['cpu_vs_head'])
        rows[-1]['dropped_capacity']=0
        for r in rows:
            if r['variant']=='governor':r.update(http_200=700,http_429=300)
        peer=next(c for c in comparison.scorecard(rows) if c['variant']=='governor')
        self.assertIn('different response mix',peer['comparison'])
        self.assertIsNone(peer['cpu_vs_head'])

    def test_profiles_and_instrumentation_are_never_pooled(self):
        rows=[row(v,repeat=i,admission_policy=policy,reuse_warmup_pool=True,instrumented=False)
              for i in range(5) for v in ('baseline','head','governor') for policy in ('drop','wait')]
        cells=comparison.scorecard(rows)
        self.assertEqual(len(cells),6)
        self.assertTrue(all(c['recorded']==5 for c in cells))
        summarized=report.summarize(dict(rounds=rows))
        self.assertEqual(len(summarized['summary']),6)
        self.assertEqual(len(verdicts.analyze(rows)['capacity']),2)
        socket_rows=[row(v,repeat=i,server_nodelay=enabled)
                     for i in range(5) for v in ('baseline','head','governor') for enabled in (False,True)]
        self.assertEqual(len(comparison.scorecard(socket_rows)),6)
        self.assertEqual(len(verdicts.analyze(socket_rows)['capacity']),2)

    def test_controlled_tcp_experiment_balances_order_and_keeps_three_repeats(self):
        calls=[]
        def launch(command):
            def value(name):return command[command.index(name)+1]
            variants=command[command.index('--variants')+1:command.index('--clients')]
            folder=Path(value('--output'));folder.mkdir()
            enabled='--server-nagle' not in command
            calls.append((variants,enabled))
            records=[row(v,server_nodelay=enabled,measurement_seconds=30) for v in variants]
            (folder/'raw.json').write_text(json.dumps(dict(rounds=records)))
            (folder/'environment.json').write_text(json.dumps(dict(git_sha='sha',images={'shared':'id'})))
            class Process:
                def wait(self):return 0
                def poll(self):return 0
            return Process()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'run'
            with patch.object(sys,'argv',['diagnose.py','--backend','memory','--output',str(root)]), \
                 patch.object(diagnose.subprocess,'Popen',side_effect=launch),patch.object(diagnose.signal,'signal'):
                diagnose.main()
            rows=json.loads((root/'raw.json').read_text())['rounds']
            cells=json.loads((root/'diagnosis.json').read_text())['cells']
            self.assertEqual(len(rows),24)
            self.assertEqual(len(cells),8)
            self.assertTrue(all(c['rounds']==3 for c in cells))
            self.assertEqual(calls[0],(['baseline','head'],False))
            self.assertEqual(calls[4],(['head','baseline'],True))

    def test_diagnostic_matrix_and_flat_artifact_summary(self):
        self.assertEqual([c['shard'] for c in workflow.matrix('diagnose')['include']],['memory','redis'])
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name,data in [('status.json',dict(outcome='success')),('environment.json',dict(git_sha='sha')),
                              ('raw.json',dict(rounds=[row('head',campaign_stage='diagnostic')]))]:
                (root/name).write_text(json.dumps(data))
            (root/'assessment.md').write_text('# Queue diagnosis\nshort controlled observations')
            index,content=workflow.aggregate(root,'diagnose','memory')
            self.assertTrue(index['complete'])
            self.assertIn('Queue diagnosis',content)
            self.assertNotIn('1/1 requested scenarios',content)

if __name__=='__main__':unittest.main()

"""Exercise complete campaign control flow with deterministic subprocess fixtures."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).parents[1]))
import campaign
import workflow
import workloads
from test_campaign import row


class FollowupTest(unittest.TestCase):
    def run_campaign(self, root, options, passing_ceiling=12000):
        calls=[]
        def launch(command):
            def value(name): return command[command.index(name)+1]
            start=command.index('--variants')+1
            peers=command[start:command.index('--rates')]
            rate=int(value('--rates'));repeats=int(value('--repeats'))
            clients=int(value('--clients'));mode=value('--modes');seconds=int(value('--duration'))
            backend=value('--backends');folder=Path(value('--output'));folder.mkdir()
            calls.append(dict(rate=rate,repeats=repeats,clients=clients,mode=mode))
            records=[]
            for repeat in range(repeats):
                for peer in peers:
                    rejected=0 if peer=='baseline' or mode=='allow' else 500 if mode=='mixed' else 990
                    records.append(row(peer,rate,repeat,scenario=f'{backend}/{clients}/512/{mode}/{rate}rps',
                        measurement_seconds=seconds,workload_clients=clients,clients_observed=clients,
                        workload_mode=mode,http_200=1000-rejected,http_429=rejected,
                        scheduled_latency={'p99_ms':1 if rate<=passing_ceiling else 20}))
            (folder/'raw.json').write_text(json.dumps(dict(rounds=records)))
            (folder/'environment.json').write_text(json.dumps(dict(git_sha='sha',source_hashes={'src':'hash'},images={'shared':'image'})))
            class Process:
                def wait(self): return 0
                def poll(self): return 0
            return Process()
        with patch.object(sys,'argv',['campaign.py','--output',str(root),*options]), \
             patch.object(campaign.subprocess,'Popen',side_effect=launch), \
             patch.object(campaign.signal,'signal'):
            campaign.main()
        return calls,json.loads((root/'qualification.json').read_text())

    def test_failed_initial_baseline_reaches_long_confirmations_and_all_nine_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'run'
            calls,coverage=self.run_campaign(root,['--backend','memory','--probe-rates','31000','41000','--full-matrix'])
            plan=json.loads((root/'campaign-plan.json').read_text())
            self.assertEqual(plan['refinement_trace'][0]['for_variant'],'baseline')
            self.assertLessEqual(len(plan['refinement_trace']),3)
            self.assertTrue(any(c['repeats']==5 for c in calls))
            self.assertEqual((coverage['executed_cases'],coverage['qualified_cases']),(9,9))

    def test_targeted_followups_have_fresh_controls_and_only_requested_cases(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);folder=root/'benchmark-diagnostics-redis'
            calls,coverage=self.run_campaign(folder,['--backend','redis','--targets',
                '1:allow:20000','10000:mixed:15000'],passing_ceiling=25000)
            self.assertEqual({(c['clients'],c['mode']) for c in calls},{(1,'allow'),(10000,'mixed')})
            self.assertEqual(sum(c['repeats']==5 for c in calls),2)
            self.assertEqual(sum(c['repeats']==1 and c['rate']==15000 for c in calls),2)
            self.assertEqual((coverage['requested_cases'],coverage['qualified_cases']),(2,2))
            (folder/'status.json').write_text(json.dumps(dict(outcome='success')))
            index,content=workflow.aggregate(root,'compare','redis')
            self.assertTrue(index['complete'])
            self.assertTrue(index['qualification_complete'])
            self.assertEqual(len(index['shards']),1)
            self.assertIn('targeted follow-up only',content)

    def test_target_selection_is_validated_and_single_backend_aggregation_is_explicit(self):
        for values in (['1:allow:0'],['2:allow:1000'],['1:deny:1000'],['oops'],
                       ['1:allow:1000','1:allow:2000']):
            with self.subTest(values=values),self.assertRaises(ValueError): workloads.parse_targets(values)
        self.assertEqual(workflow.matrix('compare','memory')['include'],[dict(shard='memory',backends='memory')])
        for suite,backend in [('compare','both'),('smoke','redis'),('micro','redis')]:
            with self.assertRaises(ValueError): workflow.matrix(suite,backend,['1:allow:20000'])


if __name__=='__main__': unittest.main()

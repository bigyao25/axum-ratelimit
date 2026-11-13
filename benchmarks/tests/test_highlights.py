"""Regression tests for benchmark status and executive summaries.

Synthetic fixtures validate decision rules; they are not measured outcomes.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))
import highlights


def row(variant, reasons=(), **changes):
    result=dict(variant=variant, campaign_stage='confirm',
                scenario='memory/1/512/allow/30000rps',
                target_rps=30000, repeat=0, platform_id='host1',
                resource_layout_id='layout', measurement_seconds=30,
                loadgen_cpu_limit=2, loadgen_cpu_pct=100,
                invalid_reasons=list(reasons), load_failure_reasons=[])
    result.update(changes)
    return result


class ClassificationTests(unittest.TestCase):
    def classify(self, rows, issues=()):
        return highlights.classify_campaign(
            rows, issues, lambda r:r.get('invalid_reasons', []))

    def test_peer_only_scheduler_drop_is_warning(self):
        status,blocking,peers=self.classify(
            [row('baseline'),row('head'),row('peer_b',['scheduler_drops'])])
        self.assertEqual((status,blocking),('completed_with_peer_exclusions',[]))
        self.assertEqual(len(peers),1)

    def test_core_invalid_is_blocking(self):
        rows=[row('baseline'),row('head',['scheduler_drops']),row('peer_a')]
        self.assertEqual(self.classify(rows)[0],'measurement_failure')

    def test_malformed_competitor_result_is_blocking(self):
        rows=[row('baseline'),row('head'),
              row('peer_b',['request accounting mismatch'])]
        self.assertEqual(self.classify(rows)[0],'measurement_failure')

    def test_generator_saturation_cannot_be_attributed_to_peer(self):
        rows=[row('baseline'),row('head'),
              row('peer_a',['scheduler_drops'],loadgen_cpu_pct=180)]
        self.assertEqual(self.classify(rows)[0],'measurement_failure')

    def test_missing_paired_controls_is_blocking(self):
        rows=[row('baseline'),row('head'),
              row('peer_b',['scheduler_drops'],repeat=1)]
        self.assertEqual(self.classify(rows)[0],'measurement_failure')

    def test_missing_head_is_blocking(self):
        self.assertEqual(self.classify([row('baseline')])[0],'measurement_failure')

    def test_peer_only_coverage_is_warning(self):
        issue={'case':'100-reject','reason':'missing coverage','variants':['peer_a']}
        rows=[row('baseline'),row('head')]
        self.assertEqual(self.classify(rows,[issue])[0],
                         'completed_with_peer_exclusions')
        issue['variants']=['head']
        self.assertEqual(self.classify(rows,[issue])[0],'measurement_failure')

    def test_clean_capacity_overload_does_not_fail_workflow(self):
        rows=[row('baseline'),row('head',
              load_failure_reasons=['in-flight limit reached'])]
        self.assertEqual(self.classify(rows)[0],'completed')


class ExecutiveSummaryTests(unittest.TestCase):
    def test_lower_rate_confirmation_does_not_qualify_the_requested_load(self):
        rows=[row(v,repeat=i,scenario='memory/1/512/allow/17500rps',target_rps=17500)
              for i in range(5) for v in ('baseline','head')]
        targets=[dict(clients=1,mode='allow',rate=35000)]
        coverage=highlights.confirmation_coverage(rows,targets,'memory')
        self.assertTrue(coverage['complete'])
        self.assertFalse(coverage['requested_rates_complete'])
        content=highlights.concise(rows,{},'completed',requested=targets,backend='memory',scope='targeted')
        self.assertIn('0/1 qualified at the requested rate',content)
        self.assertIn('confirmed rates **17,500 RPS**',content)
        targets[0]['rate']=17500
        self.assertTrue(highlights.confirmation_coverage(rows,targets,'memory')['requested_rates_complete'])

    def test_qualified_claim_requires_five_clean_30s_repeats(self):
        rows=[row(v,repeat=i,scenario='memory/100/512/allow/12000rps',
                  target_rps=12000) for i in range(5) for v in ('baseline','head')]
        content=highlights.concise(rows,{},'completed')
        self.assertIn('12,000 offered RPS',content)
        self.assertLess(len(content.splitlines()),35)
        rows[0]['load_failure_reasons']=['in-flight limit reached']
        self.assertNotIn('12,000 offered RPS qualified',highlights.concise(rows,{},'completed'))

    def test_four_of_five_is_explicit_and_execution_is_separate_from_qualification(self):
        rows=[row(v,repeat=i,scenario='redis/10000/512/mixed/18000rps',target_rps=18000)
              for i in range(5) for v in ('baseline','head')]
        rows[-1]['load_failure_reasons']=['in-flight limit reached']
        coverage=highlights.confirmation_coverage(rows,[dict(clients=10000,mode='mixed')],'redis')
        self.assertEqual((coverage['executed_cases'],coverage['qualified_cases']),(1,0))
        content=highlights.concise(rows,{},'completed')
        self.assertIn('4/5 | 5/5 | not qualified',content)
        self.assertIn('redis/10,000/mixed',content)

    def test_missing_cases_and_targeted_scope_are_visible(self):
        cases=[dict(clients=1,mode='allow'),dict(clients=10000,mode='mixed')]
        content=highlights.concise([],{},'failure',requested=cases,backend='redis',scope='targeted')
        self.assertIn('0/2 requested scenarios',content)
        self.assertIn('targeted follow-up only',content)

    def test_controls_hosts_and_repeat_identity_cannot_be_mixed_to_qualify(self):
        for changes in ({'platform_id':'different'}, {'repeat':0}, {'measurement_seconds':5}):
            rows=[row(v,repeat=i) for i in range(5) for v in ('baseline','head')]
            rows[-1].update(changes)
            self.assertFalse(highlights.confirmation_coverage(rows)['complete'])
        rows=[row('head',repeat=i) for i in range(5)]
        self.assertFalse(highlights.confirmation_coverage(rows)['complete'])

    def test_peer_warning_not_presented_as_peer_code_defect(self):
        rows=[row('baseline'),row('head'),
              row('peer_b',['scheduler_drops'],dropped_late=1055)]
        content=highlights.concise(rows,{},'completed_with_peer_exclusions')
        self.assertIn('1,055 missed scheduled arrivals',content)
        self.assertIn('not evidence of a competitor code bug',content)

    def test_winning_claim_uses_supplied_verified_verdict(self):
        rows=[row('baseline'),row('head')]
        verdict={'costs':[{'winner':'head','competitor':'peer_a',
                           'condition':'redis/100/512/allow','rate':9000,
                           'metric':'server_cpu_pct','ratio':1.49,
                           'low':1.39,'high':1.59,'response_mix_matched':True}]}
        content=highlights.concise(rows,verdict,'completed')
        self.assertIn('1.49x',content)
        verdict['costs'][0]['winner']='inconclusive'
        self.assertNotIn('1.49x',highlights.concise(rows,verdict,'completed'))


if __name__=='__main__':
    unittest.main()

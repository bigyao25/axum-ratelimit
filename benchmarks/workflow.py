#!/usr/bin/env python3
"""Actions matrix planning and a host-separated artifact index."""
import argparse
import json
import os
from pathlib import Path

import report
import verdicts
import highlights
import workloads
import comparison


def matrix(suite, backends='both', targets=(), report_run=''):
    selected=['memory','redis'] if backends=='both' else [backends]
    if any(b not in ('memory','redis') for b in selected): raise ValueError('Unknown backend')
    if report_run and (not report_run.isascii() or not report_run.isdecimal()
                       or int(report_run)<=0 or suite!='compare' or targets):
        raise ValueError('Report-only mode requires compare, a positive run ID, and no targets')
    if targets:
        workloads.parse_targets(targets)
        if suite!='compare' or len(selected)!=1:
            raise ValueError('Targets require compare and one selected backend')
    if suite in ('compare','diagnose'): return {'include':[{'shard':b,'backends':b} for b in selected]}
    if suite=='micro': return {'include':[{'shard':'micro','backends':'memory'}]}
    if suite=='smoke': return {'include':[{'shard':'smoke','backends':' '.join(selected)}]}
    raise ValueError('Unknown benchmark suite')


def aggregate(root, suite, backends='both'):
    """One short overview. Detailed round tables stay in shard artifacts."""
    shards=[]
    sections=[]
    expected=matrix(suite,backends)['include']
    for item in expected:
        shard=item['shard']
        artifact_name='benchmark-diagnostics-'+shard
        folder=root/artifact_name
        # download-artifact v8 flattens a sole matched artifact even when
        # merge-multiple is false. Never apply this fallback to multiple shards.
        if len(expected)==1 and not folder.exists() and (root/'status.json').exists():
            folder=root
        status=json.loads((folder/'status.json').read_text()) if (folder/'status.json').exists() else {}
        env=json.loads((folder/'environment.json').read_text()) if (folder/'environment.json').exists() else {}
        raw=json.loads((folder/'raw.json').read_text()) if (folder/'raw.json').exists() else {'rounds':[]}
        platforms=sorted({r['platform_id'] for r in raw['rounds'] if r.get('platform_id')})
        classification_path=folder/'result-classification.json'
        classification=json.loads(classification_path.read_text()) if classification_path.exists() else {}
        semantic=classification.get('status')
        environmental=semantic=='inconclusive_environment'
        complete=(status.get('outcome')=='success' and (bool(raw['rounds']) or environmental)
                  and bool(env) and (folder/'assessment.md').exists())
        if suite=='compare':
            complete=(complete and (folder/'verdicts.json').exists()
                      and (len(platforms)==1 or (environmental and not platforms)))
            observed={r.get('scenario','').split('/')[0] for r in raw['rounds'] if r.get('scenario')}
            if observed-set(item['backends'].split()): complete=False
        if semantic in ('measurement_failure','failure'):
            complete=False
        entry=dict(shard=shard,outcome=status.get('outcome','missing'),complete=complete,
                   verification=semantic or 'unknown',
                   platforms=platforms,git_sha=env.get('git_sha'),
                   rounds=len(raw['rounds']),artifact=artifact_name)
        shards.append(entry)
        rows=[{**r,'invalid_reasons':report.validate(r),
               'load_failure_reasons':report.load_failures(r)} for r in raw['rounds']]
        plan_path=folder/'campaign-plan.json'
        plan=json.loads(plan_path.read_text()) if plan_path.exists() else {}
        coverage=highlights.confirmation_coverage(rows,plan.get('requested_cases',()),plan.get('backend'))
        entry['confirmation_coverage']=coverage
        if suite=='diagnose':
            entry['confirmation_coverage']=highlights.confirmation_coverage([])
            sections+=['## '+shard.capitalize(),'',(folder/'assessment.md').read_text() if (folder/'assessment.md').exists() else 'Missing diagnostic assessment.']
            continue
        verdict_path=folder/'verdicts.json'
        verdict=json.loads(verdict_path.read_text()) if verdict_path.exists() else verdicts.analyze(rows)
        outcome=semantic or ('completed' if complete else 'measurement_failure')
        if not semantic and complete and any(r['invalid_reasons'] and r.get('variant') not in highlights.CORE for r in rows):
            outcome='completed_with_peer_exclusions'
        sections += ['## '+shard.capitalize(), '',
                     highlights.concise(rows,verdict,outcome,plan.get('issues',[]),
                                        recoveries=plan.get('environment_recoveries',[]),
                                        unresolved=plan.get('environment_unresolved',[]),
                                        requested=plan.get('requested_cases',()),backend=plan.get('backend'),
                                        scope=plan.get('scope','matrix')),
                     comparison.scorecard_markdown(rows),
                     'Artifact: `'+artifact_name+'`.']
    shas={s['git_sha'] for s in shards if s['git_sha']}
    success=all(s['complete'] for s in shards) and len(shas)==1
    incomplete_evidence=any(x['verification']=='inconclusive_environment' for x in shards)
    status=('Incomplete — inspect failing shard' if not success else
            'Inconclusive — one or more runner-limited shards' if incomplete_evidence else
            'Complete with warnings' if any(x['verification'] in ('completed_with_environment_limits','completed_with_peer_exclusions')
                                          or (x['confirmation_coverage']['requested_cases'] and not x['confirmation_coverage']['complete'])
                                          or x['confirmation_coverage']['requested_rates_complete'] is False for x in shards)
            else 'Complete')
    lines=['# Benchmark results','',f'**Run status:** {status}.',
           'Measurements from different hosted runners are **not combined or ranked**.',
           'Successful execution does not imply a performance winner or maximum throughput.',
           '']+sections
    return dict(suite=suite,complete=success,
                qualification_complete=all(x['confirmation_coverage']['complete'] for x in shards),
                requested_rates_complete=all(x['confirmation_coverage']['requested_rates_complete'] is not False for x in shards),
                shards=shards), '\n'.join(lines)+'\n'

def main():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    plan=sub.add_parser('plan');plan.add_argument('--suite',choices=['compare','smoke','micro','diagnose'],required=True)
    plan.add_argument('--github-output',action='store_true')
    plan.add_argument('--backends',choices=['both','memory','redis'],default='both')
    plan.add_argument('--targets',nargs='*',default=[])
    plan.add_argument('--report-run',default='')
    status=sub.add_parser('status');status.add_argument('folder',type=Path);status.add_argument('--outcome',required=True)
    merge=sub.add_parser('aggregate');merge.add_argument('root',type=Path);merge.add_argument('--suite',choices=['compare','smoke','diagnose'],required=True)
    merge.add_argument('--output',type=Path,required=True);merge.add_argument('--github-summary',action='store_true')
    merge.add_argument('--backends',choices=['both','memory','redis'],default='both')
    merge.add_argument('--measurement-run',default='')
    a=p.parse_args()
    if a.command=='plan':
        try: data=json.dumps(matrix(a.suite,a.backends,a.targets,a.report_run))
        except ValueError as error: p.error(str(error))
        print(data)
        if a.github_output:
            with open(os.environ['GITHUB_OUTPUT'],'a') as f: f.write('matrix='+data+'\n')
    elif a.command=='status':
        a.folder.mkdir(parents=True,exist_ok=True)
        (a.folder/'status.json').write_text(json.dumps(dict(outcome=a.outcome),indent=2))
    else:
        data,content=aggregate(a.root,a.suite,a.backends)
        if a.measurement_run:
            data['measurement_run']=a.measurement_run
            data['report_generator_sha']=os.environ.get('GITHUB_SHA')
            content += '\nReports regenerated from measurement run `'+a.measurement_run+'`; original measurement commits and raw files are preserved.\n'
        a.output.mkdir(parents=True,exist_ok=True)
        (a.output/'index.json').write_text(json.dumps(data,indent=2))
        (a.output/'overview.md').write_text(content)
        if a.github_summary:
            with open(os.environ['GITHUB_STEP_SUMMARY'],'a') as f: f.write(content[:200000])
        if not data['complete']: raise SystemExit('One or more benchmark shards are incomplete or failed')


if __name__=='__main__': main()

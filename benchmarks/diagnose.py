#!/usr/bin/env python3
"""Same-host controlled experiments for client admission and warmup pooling."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import signal

import report

HERE=Path(__file__).resolve().parent
PROFILES=[('drop-cold',['--admission-policy','drop','--cold-pool','--server-nagle']),
          ('drop-warm',['--admission-policy','drop','--server-nagle']),
          ('wait-warm',['--admission-policy','wait','--server-nagle']),
          ('wait-warm-nodelay',['--admission-policy','wait'])]


def median(items, key):
    values=[r.get(key) for r in items]
    return statistics.median(values) if values and all(isinstance(v,(int,float)) for v in values) else None


def summarize(rows):
    cells=[]
    for profile,_ in PROFILES:
        for variant in ('baseline','head'):
            items=[r for r in rows if r.get('diagnostic_profile')==profile and r['variant']==variant]
            if not items:continue
            def tail(key):
                return max((r.get(key,{}).get('max_ms') or 0 for r in items),default=0)
            cells.append(dict(profile=profile,variant=variant,rounds=len(items),
                passing=sum(not report.validate(r) and not report.load_failures(r) for r in items),
                capacity_drops=sum(r.get('dropped_capacity',0) for r in items),
                scheduler_drops=sum(r.get('dropped_late',0) for r in items),
                errors=sum(r.get('errors',0) for r in items),
                http_rps=median(items,'http_rps'),p99_ms=median(items,'p99_ms'),
                admission_max_ms=tail('admission_wait'),pool_max_ms=tail('pool_wait'),
                response_max_ms=tail('response_latency'),service_max_ms=tail('server_service'),
                server_runtime_max_ms=tail('server_runtime'),
                generator_runtime_max_ms=max(((r.get('runtime_wakeup') or {}).get('latency',{}).get('max_ms') or 0 for r in items),default=0),
                server_throttled_usec=sum(r.get('diagnostics',{}).get('cpu',{}).get('server',{}).get('throttled_usec',0) for r in items),
                redis_throttled_usec=sum(r.get('diagnostics',{}).get('cpu',{}).get('redis',{}).get('throttled_usec',0) for r in items),
                generator_throttled_usec=sum(r.get('loadgen_throttled_usec',0) for r in items),
                redis_slowlog_max_ms=max((entry[2]/1000 for r in items for entry in r.get('diagnostics',{}).get('redis_slowlog',[])),default=0),
                new_connections=sum(r.get('connection_pool',{}).get('created',0) for r in items)))
    return cells


def markdown(data, status='unknown', artifact_url=''):
    fmt=lambda v:'N/A' if v is None else f'{v:.2f}'
    lines=['# Queue diagnosis','',f"Execution: **{status}**; backend **{data['backend']}**; target **{data['rate']:,} RPS**.",
           'Four profiles run on the same host and images. Each has baseline and HEAD, 2-second warmup, 30-second measurement, three repeats; profile and product order reverse in alternating repeat blocks. These are diagnostic observations, not five-repeat capacity claims. Server timing and runtime probes are enabled equally for every profile.','',
           '| Profile | Product | Passing / rounds | HTTP RPS | Request p99 ms | Capacity / scheduler drops | New connections | Admission max ms | Pool max ms | Response max ms | Service max ms | Server runtime max ms | Generator runtime max ms |',
           '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for c in data.get('cells',[]):
        lines.append(f"| {c['profile']} | {c['variant']} | {c['passing']}/{c['rounds']} | {fmt(c['http_rps'])} | {fmt(c['p99_ms'])} | "
                     f"{c['capacity_drops']} / {c['scheduler_drops']} | {c['new_connections']} | {fmt(c['admission_max_ms'])} | {fmt(c['pool_max_ms'])} | "
                     f"{fmt(c['response_max_ms'])} | {fmt(c['service_max_ms'])} | {fmt(c['server_runtime_max_ms'])} | {fmt(c['generator_runtime_max_ms'])} |")
    lines+=['','`drop-cold` preserves the former client behavior; `drop-warm` changes only connection reuse; `wait-warm` additionally queues one pending admission for at most 100 ms from its original due time. Concurrency remains 512, requests are never retried, all intended arrivals remain in accounting, and schedule-inclusive latency includes admission waiting. The first three profiles keep server Nagle enabled; `wait-warm-nodelay` changes only server TCP_NODELAY to true.',
            'A service-time spike includes middleware/handler work and Redis waiting. Server runtime lag includes executor/OS scheduling. Response latency additionally includes transport and kernel/runtime queues. These signals narrow causes but do not alone prove a library defect or host interference. Raw slow examples, drop events, host CPU/pressure and Redis diagnostics retain event/context evidence.']
    for c in data.get('cells',[]):
        lines.append(f"- {c['profile']} / {c['variant']}: throttled CPU microseconds: server {c.get('server_throttled_usec',0)}, Redis {c.get('redis_throttled_usec',0)}, generator {c.get('generator_throttled_usec',0)}; slowest logged Redis command {c.get('redis_slowlog_max_ms',0):.2f} ms (diagnostic threshold 1 ms).")
    if data.get('failure'):lines+=['',str(data['failure'])]
    if artifact_url:lines+=['',f'[Full diagnostic artifact]({artifact_url})']
    return '\n'.join(lines)+'\n'


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--backend',choices=['memory','redis'],required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--allow-dirty',action='store_true')
    p.add_argument('--plan',action='store_true')
    a=p.parse_args();rate=35000 if a.backend=='memory' else 20000
    def interrupted(*_):raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM,interrupted)
    if a.plan:
        print(json.dumps(dict(backend=a.backend,rate=rate,profiles=[x[0] for x in PROFILES],rounds=24,load_seconds=768)));return
    if a.output.exists() and any(c.name!='run.log' for c in a.output.iterdir()):p.error('Use a fresh output directory')
    a.output.mkdir(parents=True,exist_ok=True)
    rows=[];environments=[];manifest=None;status='failure';data=dict(backend=a.backend,rate=rate)
    try:
        for block in range(3):
            profiles=PROFILES if block%2==0 else list(reversed(PROFILES))
            for profile,options in profiles:
                child=a.output/f'{profile}-{block}'
                products=['baseline','head'] if block%2==0 else ['head','baseline']
                command=[sys.executable,str(HERE/'bench.py'),'compare','--backends',a.backend,
                    '--variants',*products,'--clients','1','--modes','allow','--rates',str(rate),
                    '--duration','30','--warmup','2','--repeats','1','--diagnostics',*options,'--output',str(child)]
                if manifest:command+=['--reuse-build-manifest',str(manifest)]
                if a.allow_dirty:command+=['--allow-dirty']
                process=subprocess.Popen(command)
                try:code=process.wait()
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:process.wait(timeout=15)
                        except subprocess.TimeoutExpired:process.kill();process.wait()
                if (child/'environment.json').exists():
                    env=json.loads((child/'environment.json').read_text());environments.append(env)
                    if manifest is None and env.get('images'):manifest=child/'environment.json'
                if (child/'raw.json').exists():
                    rows += [dict(r,campaign_stage='diagnostic',diagnostic_profile=profile,repeat=block)
                             for r in json.loads((child/'raw.json').read_text())['rounds']]
                if code not in (0,75):raise RuntimeError(f'Diagnostic profile {profile} failed with exit {code}')
        if len({r['platform_id'] for r in rows})!=1:raise RuntimeError('Diagnostic host changed')
        if len({e['git_sha'] for e in environments})!=1:raise RuntimeError('Diagnostic source changed')
        status='completed'
    except Exception as error:
        data['failure']=str(error);raise
    finally:
        data['cells']=summarize(rows)
        (a.output/'raw.json').write_text(json.dumps(dict(rounds=rows),indent=2))
        if environments:(a.output/'environment.json').write_text(json.dumps(environments[0],indent=2))
        (a.output/'diagnosis.json').write_text(json.dumps(data,indent=2))
        (a.output/'result-classification.json').write_text(json.dumps(dict(status=status),indent=2))
        (a.output/'assessment.md').write_text(markdown(data,status))


if __name__=='__main__':main()

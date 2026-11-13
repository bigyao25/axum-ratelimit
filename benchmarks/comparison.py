"""Descriptive cost contrasts at matched workloads, never an algorithm winner."""
import collections
import statistics
import report
import verdicts
import workloads


def contrasts(rows):
    groups=collections.defaultdict(list)
    for row in rows:
        if row.get('workload')=='behavior' or row.get('invalid_reasons') or row.get('dropped',0)>0:
            continue
        key=tuple(row.get(k) for k in ('platform_id','scenario','target_rps','measurement_seconds',
                                      'warmup_seconds','http_version','resource_layout_id','resource_profile','server_cpu_limit','loadgen_cpu_limit','quota','window_seconds'))
        key+=tuple(row.get(k) for k in ('admission_policy','reuse_warmup_pool','instrumented'))
        groups[key].append(row)
    result=[]
    for group in groups.values():
        head=[r for r in group if r['variant']=='head']
        if not head:
            continue
        for name in sorted({r['variant'] for r in group}-{'head','baseline'}):
            peer=[r for r in group if r['variant']==name]
            med=lambda rs,k: statistics.median(r[k] for r in rs) if all(r.get(k) is not None for r in rs) else None
            ratio=lambda k: med(peer,k)/med(head,k) if med(head,k) and med(peer,k) is not None else None
            enough=min(len(head),len(peer))>=5
            separation=enough and (max(r['p99_ms'] for r in head)<min(r['p99_ms'] for r in peer) or max(r['p99_ms'] for r in peer)<min(r['p99_ms'] for r in head))
            result.append(dict(variant=name,algorithm=peer[0].get('algorithm','unknown'),scenario=peer[0]['scenario'],
                               repeats=min(len(head),len(peer)),p99_ratio=ratio('p99_ms'),cpu_ratio=ratio('server_cpu_pct'),
                               rss_ratio=ratio('server_rss_bytes'),redis_ratio=ratio('redis_memory_bytes'),
                               evidence='non-overlapping observed p99 ranges; descriptive only' if separation else ('overlapping p99 ranges' if enough else 'fewer than five matched repeats')))
    return result


def scorecard(rows):
    """Show all five-repeat outcomes and observed costs without cherry-picking passes."""
    groups=collections.defaultdict(lambda:collections.defaultdict(list))
    for row in rows:
        if row.get('campaign_stage','confirm')=='confirm' and row.get('target_rps'):
            groups[(verdicts.context(row),row['target_rps'])][row['variant']].append(row)
    result=[]
    for (context,rate),variants in sorted(groups.items(),key=lambda pair:str(pair[0])):
        head=variants.get('head',[]);baseline=variants.get('baseline',[])
        def med(items,key):
            values=[r.get(key) for r in items]
            return statistics.median(values) if values and all(isinstance(x,(int,float)) for x in values) else None
        for name,items in sorted(variants.items()):
            passed=sum(verdicts.passed(r) for r in items)
            qualified=verdicts.qualified(items) and verdicts.qualified(baseline)
            matched=name!='baseline' and workloads.matched_mix(head,items)
            comparable=qualified and verdicts.qualified(head) and matched
            hcpu=med(head,'server_cpu_pct');cpu=med(items,'server_cpu_pct')
            ratio=cpu/hcpu if comparable and hcpu and cpu is not None else None
            result.append(dict(condition=context[-1],platform_id=context[0],rate=rate,variant=name,
                admission_policy=items[0].get('admission_policy','legacy-drop'),
                recorded=len(items),passing=passed,qualified=qualified,response_mix_matched=matched,
                http_rps=med(items,'http_rps'),p99_ms=med(items,'p99_ms'),
                server_cpu_pct=cpu,cpu_vs_head=ratio,
                rejection_fraction=statistics.median(workloads.rejection_fraction(r) or 0 for r in items),
                drops=sum(r.get('dropped',0) for r in items),errors=sum(r.get('errors',0) for r in items),
                qualification='qualified' if qualified else 'not qualified',
                comparison='matched workload; observed medians, see paired intervals' if comparable else
                           'different response mix' if name!='baseline' and not matched else 'no qualified paired comparison'))
    return result


def scorecard_markdown(rows):
    cells=scorecard(rows)
    if not cells:return ''
    lines=['### Products at the same tested load','',
           'CPU is percent of one core. Values are medians of all recorded repetitions, including failed repetitions. CPU ratios are shown only for qualified, response-matched HEAD/peer pairs. They are observations, not statistical winners.','',
           '| Condition / RPS | Product | Passing / recorded | HTTP RPS | Request p99 ms | Server CPU % | CPU / HEAD | Rejection % | Drops | Assessment |',
           '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |']
    fmt=lambda x:'N/A' if x is None else f'{x:.2f}'
    for c in cells:
        assessment=c['qualification']+('; different response mix' if c['variant'] not in ('head','baseline') and not c['response_mix_matched'] else '')
        lines.append(f"| {c['condition']} / {c['rate']:,} | {c['variant']} | {c['passing']}/{c['recorded']} | "
                     f"{fmt(c['http_rps'])} | {fmt(c['p99_ms'])} | {fmt(c['server_cpu_pct'])} | {fmt(c['cpu_vs_head'])} | "
                     f"{100*c['rejection_fraction']:.1f} | {c['drops']} | {assessment} |")
    return '\n'.join(lines)+'\n'

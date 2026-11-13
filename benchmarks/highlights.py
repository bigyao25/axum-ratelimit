"""Concise, evidence-gated benchmark conclusions and run-result classification.

Benchmark success means valid HEAD/baseline evidence, not victory at every load.
A competitor-only invalid cell can be excluded only when paired control cells
are clean and its failure is not a data-integrity or shared-generator failure.
"""

from collections import defaultdict

CORE = frozenset(('head', 'baseline'))
ISOLATABLE = frozenset((
    'scheduler_drops', 'correctness_mismatch', 'quota_exceeded',
    'oom', 'redis_oom', 'http_503', 'http_other', 'errors', 'cpu_throttled',
))


def _cell(row):
    return (row.get('campaign_stage', 'confirm'), row.get('scenario'),
            row.get('target_rps'), row.get('repeat'), row.get('platform_id'),
            row.get('resource_layout_id'), row.get('measurement_seconds'))


def _integrity_failure(reasons):
    """Malformed/corrupt records are harness defects, even for competitors."""
    return any(reason not in ISOLATABLE for reason in reasons)


def classify_campaign(rows, issues, validate):
    """Return (status, blocking explanations, peer exclusions).

    Unknown failures fail closed. An isolated competitor failure is never
    attributed to the competitor's implementation without independent proof.
    """
    blocking = []
    peer_exclusions = []
    if not rows:
        blocking.append('no measurements')
    checked = [(row, validate(row)) for row in rows]
    seen_core = defaultdict(set)
    clean_core = defaultdict(set)
    for row, reasons in checked:
        if row.get('variant') in CORE:
            seen_core[_cell(row)].add(row['variant'])
            if not reasons:
                clean_core[_cell(row)].add(row['variant'])
    if not seen_core:
        blocking.append('no HEAD/baseline measurements')
    for cell, present in seen_core.items():
        if present != CORE:
            blocking.append(f'unpaired HEAD/baseline measurement: {cell}')
    for row, reasons in checked:
        if not reasons:
            continue
        variant = row.get('variant', 'unknown')
        detail = f"{variant} / {row.get('scenario', 'unknown')}: {', '.join(reasons)}"
        if variant in CORE:
            blocking.append(detail)
        elif (_integrity_failure(reasons)
              or clean_core[_cell(row)] != CORE
              or ('scheduler_drops' in reasons and
                  row.get('loadgen_cpu_pct', float('inf')) >=
                  85 * row.get('loadgen_cpu_limit', 1))):
            blocking.append('unverified peer measurement: ' + detail)
        else:
            peer_exclusions.append(detail)
    for issue in issues:
        affected = set(issue.get('variants') or ())
        detail = f"{issue.get('case', 'unknown')}: {issue.get('reason', 'unknown')}"
        if affected and not (affected & CORE):
            peer_exclusions.append('peer coverage excluded: ' + detail + ' (' + ', '.join(sorted(affected)) + ')')
        else:
            blocking.append(detail)
    status = ('measurement_failure' if blocking else
              'completed_with_peer_exclusions' if peer_exclusions else 'completed')
    return status, blocking, peer_exclusions


def _number(value):
    return f"{value:,.0f}"


def _condition(row):
    parts = row.get('scenario', '').split('/')
    if len(parts) < 4:
        return None
    try:
        return parts[0], int(parts[1]), parts[3]
    except ValueError:
        return None


def confirmation_coverage(rows, requested=(), backend=None):
    """Separate execution from paired five-repeat qualification, without pooling hosts."""
    groups=defaultdict(lambda:defaultdict(list))
    for row in rows:
        condition=_condition(row)
        if (condition and row.get('variant') in CORE
                and row.get('campaign_stage','confirm')=='confirm'):
            key=(condition,row.get('target_rps'))+tuple(row.get(k) for k in
                 ('platform_id','resource_layout_id','http_version','resource_profile',
                  'server_cpu_limit','loadgen_cpu_limit','measurement_seconds','warmup_seconds',
                  'quota','window_seconds'))
            key+=tuple(row.get(k) for k in ('admission_policy','reuse_warmup_pool','instrumented','server_nodelay'))
            groups[key][row['variant']].append(row)
    expected={(backend,case['clients'],case['mode']) for case in requested} if backend else set()
    if not expected:
        expected={_condition(row) for row in rows if _condition(row) and row.get('variant')=='head'}
    levels=[];executed=set();qualified=set()
    for key,variants in sorted(groups.items(),key=lambda item:str(item[0])):
        condition,rate=key[:2]
        counts={};complete=True
        for variant in ('baseline','head'):
            items=variants[variant]
            full=(len(items)==5 and {r.get('repeat') for r in items}==set(range(5))
                  and all(r.get('measurement_seconds',0)>=30 for r in items))
            complete &= full
            passed=sum(r.get('measurement_seconds',0)>=30 and not r.get('invalid_reasons')
                       and not r.get('load_failure_reasons') for r in items)
            counts[variant]=dict(rounds=len(items),passing=passed,complete=full)
        paired_pass=complete and all(counts[v]['passing']==5 for v in CORE)
        if complete: executed.add(condition)
        if paired_pass: qualified.add(condition)
        levels.append(dict(backend=condition[0],clients=condition[1],mode=condition[2],
                           rate=rate,platform_id=key[2],executed=complete,qualified=paired_pass,
                           counts=counts))
    requested_levels=[]
    if backend:
        for case in requested:
            if 'rate' not in case:continue
            condition=(backend,case['clients'],case['mode'])
            confirmed=[r['rate'] for r in levels if (r['backend'],r['clients'],r['mode'])==condition and r['qualified']]
            requested_levels.append(dict(backend=backend,clients=case['clients'],mode=case['mode'],
                requested_rate=case['rate'],qualified=case['rate'] in confirmed,confirmed_rates=sorted(set(confirmed))))
    return dict(requested_cases=len(expected),executed_cases=len(expected & executed),
                qualified_cases=len(expected & qualified),
                requested_levels=requested_levels,
                requested_rates_complete=all(r['qualified'] for r in requested_levels) if requested_levels else None,
                complete=bool(expected) and expected<=qualified,
                unqualified_cases=[dict(backend=b,clients=c,mode=m) for b,c,m in
                                   sorted(expected-qualified)],levels=levels)


def _hotspot(rows):
    groups = defaultdict(list)
    for row in rows:
        if row.get('variant') != 'head' or row.get('campaign_stage', 'confirm') != 'confirm':
            continue
        condition = _condition(row)
        if condition and condition[1:] == (1, 'allow'):
            groups[(condition[0], row.get('target_rps'))].append(row)
    results = []
    for (backend, rate), group in sorted(groups.items(), key=lambda item: item[0][1], reverse=True)[:2]:
        passing = sum(row.get('measurement_seconds', 0) >= 30
                      and not row.get('invalid_reasons')
                      and not row.get('load_failure_reasons') for row in group)
        results.append(f'{_number(rate)} RPS: {passing}/{len(group)} qualified')
    return results


def _cost_claim(row):
    variant = row['competitor']
    scenario = row['condition'].split('/')
    try:
        clients, mode = scenario[1], scenario[3]
    except IndexError:
        clients, mode = '?', '?'
    metric = ('schedule-inclusive p99' if row['metric'] == 'scheduled_p99_ms'
              else 'server CPU' if row['metric'] == 'server_cpu_pct' else row['metric'])
    ratio = row['ratio']
    if row['winner'] == 'head':
        description = f'{variant} had **{ratio:.2f}x** HEAD {metric}'
    else:
        description = f'{variant} used **{(1 - ratio) * 100:.1f}% less** {metric} than HEAD'
    return (f"{_number(row['rate'])} RPS, {clients} clients/{mode}: {description} "
            f"(paired 95% interval {row['low']:.2f}-{row['high']:.2f}); different algorithms")


def concise(rows, verdict, status='unknown', issues=(), artifact_url='',
            recoveries=(), unresolved=(), requested=(), backend=None, scope='matrix'):
    """Short, conservative Markdown; full round-level evidence stays in artifacts."""
    lines = ['### Key findings', '']
    if scope=='targeted':
        lines += ['**Scope: targeted follow-up only.** Other matrix cells were not rerun; prior runs are not pooled.','']
    coverage=confirmation_coverage(rows,requested,backend)
    if coverage['requested_cases']:
        n=coverage['requested_cases']
        lines += [f"**Confirmation coverage:** {coverage['executed_cases']}/{n} requested scenarios executed with five long paired repeats; "
                  f"**{coverage['qualified_cases']}/{n} qualified**. Execution success does not mean all scenarios qualified.",'']
    if coverage['requested_levels']:
        original=coverage['requested_levels']
        lines += [f"**Originally requested offered rates:** {sum(r['qualified'] for r in original)}/{len(original)} qualified at the requested rate. Lower-rate confirmations do not qualify the original target.",'']
        for target in original:
            if target['qualified']:continue
            confirmed=', '.join(_number(rate) for rate in target['confirmed_rates']) or 'none'
            lines.append(f"- {target['backend']}/{target['clients']:,}/{target['mode']}: requested **{_number(target['requested_rate'])} RPS**; confirmed rates **{confirmed} RPS**. Requested rate remains unconfirmed.")
        lines.append('')
    if not rows:
        lines += ['**No benchmark measurements available.** No performance claim can be made.']
        if unresolved:
            lines.append('**Inconclusive environment:** ' + str(unresolved[0].get('reason','unknown')) + '.')
        return '\n'.join(lines) + '\n'
    critical = [row for row in rows if row.get('variant') in CORE and row.get('invalid_reasons')]
    peers = [row for row in rows if row.get('variant') not in CORE and row.get('invalid_reasons')]
    quality = ('Failed / incomplete' if status in ('failure', 'measurement_failure', 'failed')
               else 'Inconclusive — generator/runner scheduling' if status=='inconclusive_environment'
               else 'Completed with environment-limited rates' if status=='completed_with_environment_limits'
               else 'Completed with excluded peer cells' if peers or issues
               else 'Completed' if status in ('completed', 'success')
               else str(status))
    lines.append(f'**Execution:** {quality}; {len(rows)} rounds; {len(critical)} invalid HEAD/baseline and {len(peers)} invalid peer rounds.')
    lines.append('')
    if critical:
        row = critical[0]
        lines.append(f"**Blocking issue:** {row.get('variant')} / {row.get('scenario')}: {', '.join(row['invalid_reasons'])}.")
        lines.append('')
    qualified = {}
    for level in coverage['levels']:
        if level['qualified']:
            key=(level['backend'],level['clients'],level['mode'])
            qualified[key]=max(qualified.get(key,0),level['rate'])
    if qualified:
        lines += ['**HEAD sustained-load evidence** (each: at least 5 clean repeats, >=30 s):', '']
        top = [(100, 'allow'), (10000, 'allow'), (100, 'mixed'), (100, 'reject')]
        selected = [(key, rate) for key, rate in qualified.items() if key[1:] in top]
        if not selected:
            selected = list(qualified.items())[:4]
        for (backend, clients, mode), rate in sorted(selected, key=lambda item: (item[0][0], top.index(item[0][1:]) if item[0][1:] in top else 99))[:4]:
            lines.append(f'- {backend}, {_number(clients)} clients/{mode}: **{_number(rate)} offered RPS** qualified.')
    else:
        lines.append('**No five-repeat HEAD sustained-load level is confirmed** in this report.')
    lines.append('')
    if coverage['levels']:
        lines += ['| Backend / clients / mode | Offered RPS | HEAD passing / recorded | Baseline passing / recorded | Confirmation |',
                  '| --- | ---: | ---: | ---: | --- |']
        for level in coverage['levels']:
            h,b=level['counts']['head'],level['counts']['baseline']
            state='qualified' if level['qualified'] else 'not qualified' if level['executed'] else 'incomplete / short'
            lines.append(f"| {level['backend']} / {level['clients']:,} / {level['mode']} | {level['rate']:,} | "
                         f"{h['passing']}/{h['rounds']} | {b['passing']}/{b['rounds']} | {state} |")
        lines.append('')
    if coverage['unqualified_cases']:
        names=[f"{c['backend']}/{c['clients']:,}/{c['mode']}" for c in coverage['unqualified_cases']]
        lines += ['**Scenarios without a qualified level:** '+', '.join(names)+'.','']
    strengths = [c for c in verdict.get('costs', []) if c.get('winner') == 'head'
                 and c.get('ratio', 0) > 1 and c.get('response_mix_matched')]
    weaknesses = [c for c in verdict.get('costs', []) if c.get('winner') == c.get('competitor')
                  and c.get('ratio', 1) < 1 and c.get('response_mix_matched')]
    strengths.sort(key=lambda x: x['ratio'], reverse=True)
    weaknesses.sort(key=lambda x: x['ratio'])
    if strengths:
        lines += ['**Evidence-backed comparative advantages** (matched, qualified workloads):', '']
        lines += ['- ' + _cost_claim(c) + '.' for c in strengths[:2]]
        lines.append('')
    if weaknesses:
        lines += ['**Measured trade-off:** ' + _cost_claim(weaknesses[0]) + '.', '']
    hot = _hotspot(rows)
    if hot:
        lines.append('**Single-client hotspot (not a confirmed capacity bound):** ' + '; '.join(hot) + '.')
        lines.append('')
    if peers:
        lines += ['**Excluded competitor-labeled measurements** (not evidence of a competitor code bug):', '']
        for row in peers[:3]:
            reason = ', '.join(row['invalid_reasons'])
            suffix = f"; {row.get('dropped_late', 0):,} missed scheduled arrivals" if 'scheduler_drops' in row['invalid_reasons'] else ''
            lines.append(f"- {row.get('variant')} / {row.get('scenario')}: {reason}{suffix}; excluded from ranking.")
        if len(peers) > 3:
            lines.append(f'- {len(peers) - 3} additional peer exclusions in diagnostics.')
        lines.append('')
    if recoveries:
        samples=[f"{e.get('rate', '?')} RPS ({e.get('phase','unknown')}: {e.get('issue','see diagnostics')})"
                 for e in recoveries[-2:]]
        lines += [f'**Load-generator recovery:** {len(recoveries)} excluded attempt(s); '+
                  '; '.join(samples)+'. Retried at lower offered rates; failed attempts are not ranked.','']
    if unresolved:
        lines += [f'**Unverified coverage:** {len(unresolved)} scenario(s) remained inconclusive after bounded recovery; no capacity claim for these scenarios.','']
    if issues:
        lines.append(f'**Coverage warnings:** {len(issues)} issue(s); see campaign-plan.json for affected variants.')
        lines.append('')
    lines += ['**Interpretation:** HEAD uses an exact sliding request log; GCRA and'
              ' token-bucket peers provide different boundary guarantees and may use fewer'
              ' resources. A lower CPU/memory reading does not establish equivalent correctness.',
              'Results are fixed-offered-load evidence, **not maximum throughput**. Backend'
              ' shards run on separate hosts; no cross-host ranking. Paired intervals are'
              ' exploratory, not simultaneous significance tests. This run did not'
              ' measure enabled-observer overhead.']
    if artifact_url:
        lines += ['', f'[Full diagnostic artifact]({artifact_url})']
    else:
        lines += ['', 'Full rounds, exclusions and qualification tables are in the diagnostic artifact.']
    return '\n'.join(lines) + '\n'

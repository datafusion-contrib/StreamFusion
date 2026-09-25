#!/usr/bin/env python3
"""Publish a searchable, standalone report from validated upstream SQL inventories."""
from __future__ import annotations

import argparse
from collections import Counter
import csv
import gzip
import subprocess
import json
from pathlib import Path

HTML = '''<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>StreamFusion upstream SQL inventory</title>
<style>
:root{color-scheme:light dark;font:15px system-ui,sans-serif;line-height:1.5;--muted:#68768a;--border:#8293a344}
body{max-width:1500px;margin:30px auto;padding:0 24px}h1{font-size:29px;margin-bottom:8px}p{max-width:1050px}
.controls{display:flex;gap:12px;flex-wrap:wrap;margin:25px 0}label{display:flex;flex-direction:column;gap:4px;font-size:13px}
input,select,button{font:inherit;padding:9px;border:1px solid var(--border);border-radius:6px;background:transparent;color:inherit}
input[type=search]{min-width:310px}.counts{display:flex;gap:15px;flex-wrap:wrap}.counts button{min-width:180px;text-align:left;cursor:pointer}.counts strong{display:block;font-size:24px}
.table{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:13px}th{text-align:left;position:sticky;top:0;background:Canvas}td,th{padding:12px 8px;border-bottom:1px solid var(--border);vertical-align:top}td:nth-child(2){max-width:420px;overflow-wrap:anywhere}td:last-child{min-width:320px;max-width:570px;overflow-wrap:anywhere}.tag{white-space:nowrap;font-weight:650}.accelerated{color:#169668}.gap{color:#d78915}.muted,small{color:var(--muted)}small{display:block;margin-top:4px}details{margin-top:7px}summary{cursor:pointer}.pages{display:flex;align-items:center;gap:15px;margin:18px 0}a{color:#428bdb}
@media(prefers-color-scheme:dark){.accelerated{color:#5edbb2}.gap{color:#ffc76b}}
</style>
<h1>Upstream SQL coverage</h1>
<p>Every upstream planner integration-test invocation has a label, category and reason. <b>Accelerated</b> means native admission was observed during execution translation and the test passed. <b>Should be accelerated</b> includes all in-scope streaming SQL gaps. Batch execution, validation-only fixtures and deliberate exclusions are listed separately. Labels do not measure speedup.</p>
<p class="muted">A test can execute multiple queries; one native query does not hide another query's fallback. Parameterized variants remain separate. The default view includes passing cases; select all outcomes to see skips and failures. The two releases have different test corpora.</p>
<div id="provenance"></div>
<div class="controls"><label>Flink line<select id="line"><option value="">Both</option><option>1.18</option><option>2.2</option></select></label><label>Label<select id="label"><option value="">All labels</option><option>accelerated</option><option>not accelerated</option><option>should be accelerated</option></select></label><label>Category<select id="category"><option value="">All categories</option></select></label><label>Outcome<select id="outcome"><option value="passed">Passed</option><option value="">All outcomes</option><option>skipped</option><option>failure</option><option>error</option></select></label><label>Search test, variant or reason<input id="search" type="search" placeholder="e.g. JSON, OVER, RocksDB, CalcITCase"></label></div>
<div class="counts" id="counts"></div><p id="total" class="muted"></p>
<div class="table"><table><thead><tr><th>Flink</th><th>Upstream test / variant</th><th>Label / category</th><th>Why</th></tr></thead><tbody id="rows"></tbody></table></div>
<div class="pages"><button id="previous">Previous</button><span id="page"></span><button id="next">Next</button></div>
<script id="inventory" type="application/json">DATA</script>
<script>
const data=JSON.parse(document.getElementById('inventory').textContent), $=id=>document.getElementById(id);
let page=0, selected=[];const size=100,labels=['accelerated','not accelerated','should be accelerated'];
const categories=[...new Set(data.tests.flatMap(t=>t.category.split(', ')))].sort();
for(const c of categories){const o=document.createElement('option');o.textContent=c;$('category').append(o)}
for(const p of data.provenance){const a=document.createElement('a');a.href=p.csv;a.textContent=`Flink ${p.line} CSV`;const span=document.createElement('p');const raw=document.createElement('a');raw.href=`flink-${p.line}.json.gz`;raw.textContent='full JSON';span.append(a,document.createTextNode(' · '),raw,document.createTextNode(` · StreamFusion ${p.revision.slice(0,12)} · ${p.cases.toLocaleString()} reported invocations`));$('provenance').append(span)}
function update(){page=0;const q=$('search').value.toLowerCase();selected=data.tests.filter(t=>(!$('line').value||t.line===$('line').value)&&(!$('label').value||t.label===$('label').value)&&(!$('category').value||t.category.split(', ').includes($('category').value))&&(!$('outcome').value||t.outcome===$('outcome').value)&&(!q||[t.test,t.variant,t.note,t.category,t.features].join(' ').toLowerCase().includes(q)));
$('counts').replaceChildren();for(const label of labels){const b=document.createElement('button'),n=document.createElement('strong');n.textContent=selected.filter(t=>t.label===label).length.toLocaleString();b.append(n,document.createTextNode(label));b.onclick=()=>{$('label').value=label;update()};$('counts').append(b)}render()}
function render(){$('rows').replaceChildren();for(const t of selected.slice(page*size,(page+1)*size)){const tr=document.createElement('tr');const line=document.createElement('td');line.textContent=t.line;const name=document.createElement('td');if(t.source){const link=document.createElement('a');link.href=t.source;link.textContent=t.test;name.append(link)}else{name.textContent=t.test}const variant=document.createElement('small');variant.textContent=t.variant;name.append(variant);const label=document.createElement('td');const tag=document.createElement('span');tag.textContent=t.label;tag.className='tag '+(t.label==='accelerated'?'accelerated':t.label==='should be accelerated'?'gap':'muted');const cat=document.createElement('small');cat.textContent=t.category;label.append(tag,cat);const note=document.createElement('td');note.textContent=t.note;const detail=document.createElement('details'),title=document.createElement('summary'),text=document.createElement('small');title.textContent='Evidence';text.textContent=`Outcome: ${t.outcome}; native plans: ${t.native}; host plans: ${t.host}; features: ${t.features}; invocation: ${t.id||'not executed'}`;detail.append(title,text);note.append(detail);tr.append(line,name,label,note);$('rows').append(tr)}$('total').textContent=`${selected.length.toLocaleString()} matching invocations`;$('page').textContent=`Page ${page+1} of ${Math.max(1,Math.ceil(selected.length/size))}`;$('previous').disabled=page===0;$('next').disabled=(page+1)*size>=selected.length}
for(const id of ['line','label','category','outcome','search'])$(id).addEventListener('input',update);
$('previous').onclick=()=>{page--;render()};$('next').onclick=()=>{page++;render()};update();
</script></html>'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--flink-repository', type=Path, help='Optional local Flink clone for verified source links')
    parser.add_argument('--run-url', help='CI run containing the original reports and observations')
    parser.add_argument('--run-note', help='Validation detail to retain with this snapshot')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    data = {'provenance': [], 'tests': []}
    for path in args.inventory:
        inventory = json.loads(path.read_text())
        tests = inventory['tests']
        if inventory.get('schema_version') != 1 or not tests or len(tests) != inventory['summary']['cases']:
            raise ValueError(f'Incomplete inventory: {path}')
        line = tests[0]['flink_line']
        if line not in ('1.18', '2.2') or any(t['flink_line'] != line for t in tests):
            raise ValueError(f'Mixed or unsupported Flink line: {path}')
        if any(p['line'] == line for p in data['provenance']):
            raise ValueError(f'Duplicate Flink line: {line}')
        filename = f'flink-{line}.csv'
        source_files = set()
        tag = 'release-' + ('1.18.1' if line == '1.18' else '2.2.1')
        if args.flink_repository:
            source_files = set(subprocess.check_output(
                ['git', '-C', str(args.flink_repository), 'ls-tree', '-r', '--name-only', tag,
                 'flink-table/flink-table-planner/src/test'], text=True).splitlines())
        for test in tests:
            relative = test['test_class'].split('$', 1)[0].replace('.', '/')
            choices = [f'flink-table/flink-table-planner/src/test/{language}/{relative}.{extension}'
                       for language in ('java', 'scala') for extension in ('java', 'scala')]
            source = next((p for p in choices if p in source_files), None)
            test['source_url'] = f'https://github.com/apache/flink/blob/{tag}/{source}' if source else ''
        raw = json.dumps(inventory, separators=(',', ':')).encode()
        (args.output / f'flink-{line}.json.gz').write_bytes(gzip.compress(raw, mtime=0))
        fields = [k for k in tests[0] if k not in ('sql', 'plans', 'operation_failures', 'planners', 'translations')]
        with (args.output / filename).open('w', newline='', encoding='utf-8', errors='backslashreplace') as stream:
            writer = csv.DictWriter(stream, fields, extrasaction='ignore')
            writer.writeheader(); writer.writerows(tests)
        data['provenance'].append(dict(line=line, csv=filename, **inventory['summary']))
        for t in tests:
            data['tests'].append(dict(line=line, test=t['test_class']+'#'+t['test_name'],
                                     variant=t['display_name'], label=t['label'], category=t['category'], features=t.get('features', ''), source=t['source_url'],
                                     note=t['note'], outcome=t['outcome'], id=t['invocation_id'],
                                     native=t['native_plans'], host=t['host_plans']))
    data['tests'].sort(key=lambda t:(t['line'],t['test'],t['variant']))
    serialized = json.dumps(data, separators=(',',':')).replace('<','\\u003c').replace('&','\\u0026')
    (args.output / 'report.html').write_text(HTML.replace('>DATA<','>'+serialized+'<'))
    summary = ['# Upstream SQL inventory', '', '[Open the searchable inventory](report.html). Download the per-line CSV files below.', '',
               'This is an observed snapshot, not a performance benchmark. Native labels require an admitted execution plan and passing upstream assertions. Tests with multiple query plans retain remaining fallback gaps. All in-scope streaming SQL gaps are targets, including functionality outside current documented support.', '',
               '| Flink | Passing cases | Accelerated | Should be accelerated | Not accelerated |', '|---|---:|---:|---:|---:|']
    for p in data['provenance']:
        counts=p['labels_passed']
        summary.append(f"| [{p['line']}]({p['csv']}) | {p['outcomes'].get('passed',0):,} | {counts.get('accelerated',0):,} | {counts.get('should be accelerated',0):,} | {counts.get('not accelerated',0):,} |")
    summary += ['', '## Streaming coverage targets', '', 'Counts are passing test invocations, including parameter variants; the same missing feature can affect many cases. Categories overlap when a case has several blockers.', '', '| Category | Flink 1.18 | Flink 2.2 |', '|---|---:|---:|']
    counts=Counter((c,t['line']) for t in data['tests'] if t['label']=='should be accelerated' and t['outcome']=='passed' for c in t['category'].split(', '))
    for c in sorted({key[0] for key in counts}, key=lambda c:-(counts[c,'1.18']+counts[c,'2.2'])):
        summary.append(f"| {c} | {counts[c,'1.18']:,} | {counts[c,'2.2']:,} |")
    summary += ['', '## Outside acceleration coverage', '', '| Reason | Flink 1.18 | Flink 2.2 |', '|---|---:|---:|']
    counts=Counter((t['category'],t['line']) for t in data['tests'] if t['label']=='not accelerated' and t['outcome']=='passed')
    for c in sorted({key[0] for key in counts}, key=lambda c:-(counts[c,'1.18']+counts[c,'2.2'])):
        summary.append(f"| {c} | {counts[c,'1.18']:,} | {counts[c,'2.2']:,} |")
    summary += ['', '## Provenance', '', 'Both corpora run unchanged release tests with StreamFusion installed. Every passing CSV row joins to one recorded JUnit invocation; skips and failures remain visible in the report but are excluded from the passing-case totals. These are the complete planner runtime corpora, including SQL, Table API, function, batch, catalog and compiled-plan fixtures; connector-module suites are separate.', '']
    if args.run_url:
        summary += [f'[Original CI run]({args.run_url}).', '']
    if args.run_note:
        summary += [args.run_note, '']
    for p in data['provenance']:
        summary.append(f"- Flink {p['line']}: StreamFusion `{p['revision']}`; {p['cases']:,} reported cases; outcomes `{p['outcomes']}`.")
    summary += ['', 'CSV exports escape unpaired Unicode surrogates as `\\uXXXX`; the compressed JSON preserves the exact strings. The JSON also contains observed SQL, original/final plan operators, fallback reasons, planner modes and translation errors. Native/host plan counts describe planning attempts; a passing negative fixture whose translations all fail receives no execution credit.', '', 'See [the upstream suite](../upstream-flink-suite.md#complete-sql-invocation-inventory) for reproduction, evidence semantics and the separate native-work contracts. Follow-up coverage accounting is tracked in [#168](https://github.com/datafusion-contrib/StreamFusion/issues/168).', '']
    (args.output / 'index.md').write_text('\n'.join(summary))


if __name__=='__main__':
    main()

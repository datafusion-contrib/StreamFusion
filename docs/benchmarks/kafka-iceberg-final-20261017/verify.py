import collections, gzip, json, math, pathlib, statistics, tarfile
root=pathlib.Path(__file__).resolve().parent
method=json.loads((root/'method.json').read_text())
rows=[json.loads(l) for l in (root/'observations.jsonl').read_text().splitlines()]
measurements=[r for r in rows if r['kind']=='measurement']
parity=[r for r in rows if r['kind']=='parity']
assert method['resumedCells']==[]
assert method['source']=='kafka-json' and method['runtimeMode']=='STREAMING'
assert method['parallelism']==method['partitions']==method['clusterSlots']==4
assert method['sessionTimeZone']=='UTC' and method['managedMemory']=='512 mb'
assert method['maxJavaHeapBytes']==4*1024**3
expected={tuple(k) for k in method['expectedCells']}
assert len(expected)==30 and {k[0] for k in expected}=={f'q{i}' for i in range(24) if i!=6}
assert {k[1] for k in expected}=={'disk-off'}
assert len(measurements)==240 and len(parity)==120
seen=collections.Counter((r['query'],r['mode'],r['variant'],r['trial'],r['engine']) for r in measurements)
assert seen==collections.Counter((*cell,trial,engine) for cell in expected for trial in (-1,0,1,2) for engine in ('flink','streamfusion'))
assert all(r['matched'] for r in parity)
assert len({r['table'] for r in measurements})==240
for cell in expected:
 for trial in (-1,0,1,2):
  matched=[r for r in measurements if (r['query'],r['mode'],r['variant'],r['trial'])==(*cell,trial)]
  assert matched[0]['fingerprint']==matched[1]['fingerprint']
for r in measurements:
 assert r['resources']['maxJavaHeapBytes']==method['maxJavaHeapBytes']
 assert r['source']=='kafka-json' and not r['fallbackReasons']
 if r['engine']=='streamfusion':
  assert r['sourceCount']==r['nativeSourceCount']>0 and r['nativeWriterCount']>0
 else:assert r['nativeSourceCount']==r['nativeWriterCount']==0
 if r['query']=='q4':assert r['rocksDBObserved']
 if r['query']=='q0':assert r['fingerprint']['count']==1840000
assert len(list((root/'cohorts').iterdir()))==1
native_graphs=stock_graphs=metadata_count=0
for cell in expected:
 path=root/'cohorts/cohort-001'/('-'.join(cell)+'.tar.gz')
 with tarfile.open(path) as archive:
  for member in archive.getmembers():
   if member.name.endswith('/graph.json'):
    payload=archive.extractfile(member).read().decode();graph=json.loads(payload)
    nodes=graph['nodes'];writers=[n for n in nodes if n['type']=='IcebergStreamWriter']
    assert writers and all(n['parallelism']==(1 if cell[0]=='q5' else 4) for n in writers)
    if '_sf_' in member.name:
     assert 'RowDataToArrow' not in payload and 'ArrowToRowData' not in payload
     sources=[n for n in nodes if n['type']=='Source: native-kafka-source']
     assert sources and all(n['parallelism']==4 for n in sources)
     native_graphs+=1
    else:
     assert 'NativeCalc' not in payload and 'native-kafka-source' not in payload
     sources=[n for n in nodes if n['type'].startswith('Source: src')]
     assert sources and all(n['parallelism']==4 for n in sources)
     stock_graphs+=1
   if member.name.endswith('.metadata.json'):
    metadata=json.load(archive.extractfile(member))
    assert metadata['partition-specs'][0]['fields'][0]['transform']=='bucket[4]'
    for k,v in method['fileSettings'].items():
     if k!='format-version':assert metadata['properties'][k]==v
    assert metadata['format-version']==2
    if cell[0] in {'q4','q9','q15','q16','q17','q18','q19'}:
     assert metadata['properties']['write.upsert.enabled']=='true'
     assert metadata['schemas'][0]['identifier-field-ids']
    metadata_count+=1
assert native_graphs==stock_graphs==120 and metadata_count==240
cohort=root/'cohorts/cohort-001'
env=json.loads((cohort/'environment.json').read_text());assert not env['dirty']
samples=collections.defaultdict(lambda:collections.defaultdict(list))
for r in measurements:
 if not r['warmup']:samples[(r['query'],r['variant'])][r['engine']].append(r['seconds'])
ratios={k:statistics.median(v['flink'])/statistics.median(v['streamfusion']) for k,v in samples.items()}
exact={k:v for k,v in ratios.items() if k[1]=='exact'}
preferred={q:next((v for (query,variant),v in ratios.items() if query==q and variant!='exact'),ratios[(q,'exact')]) for q,_ in exact}
gm=lambda xs:math.exp(statistics.mean(map(math.log,xs)))
audit=dict(commit=env['commit'],freshJvmCohorts=1,resumedCells=0,queries=23,cells=30,allTrials=240,measuredTrials=180,passingPairs=120,measuredPairs=90,nativeGraphs=native_graphs,stockGraphs=stock_graphs,verifiedTableMetadata=metadata_count,nativeGraphsWithoutRowTransposes=native_graphs,writerParallelism={'q5':1,'otherQueries':4},nativeMeasurementsWithDataFiles=sum(r['engine']=='streamfusion' and r['dataFiles']>0 for r in measurements),exactGeomean=gm(exact.values()),readmeSelectionGeomean=gm(preferred.values()),exactMedianWins=sum(v>1 for v in exact.values()),readmeSelectionMedianWins=sum(v>1 for v in preferred.values()),q12Validation='Per-bidder emitted-count bounds; unfinished processing-time windows remain open',libraries=env['libraries'])
(root/'audit.json').write_text(json.dumps(audit,indent=2)+'\n')
print(json.dumps(audit,indent=2))

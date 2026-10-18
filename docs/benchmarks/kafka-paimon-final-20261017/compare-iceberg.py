"""Recompute cross-format medians and physical write-volume witnesses from retained evidence."""
import collections,json,math,pathlib,statistics
paimon=pathlib.Path(__file__).resolve().parent
iceberg=paimon.parent/'kafka-iceberg-final-20261017'
def load(root):
 groups=collections.defaultdict(lambda:collections.defaultdict(list))
 for r in map(json.loads,(root/'observations.jsonl').read_text().splitlines()):
  if r['kind']=='measurement' and not r['warmup']:
   engine={'flink':'stock','streamfusion':'native'}.get(r['engine'],r['engine'])
   groups[r['query'],r['variant']][engine].append(r['seconds'])
 return {key:{e:statistics.median(s) for e,s in es.items()} for key,es in groups.items()}
p,i=load(paimon),load(iceberg)
assert set(p)==set(i)
ratio=lambda v:v['stock']/v['native']
gm=lambda vals:math.exp(statistics.mean(math.log(x) for x in vals))
keys={'q4','q9','q15','q16','q17','q18','q19'}
summary={}
for name,qs in [('all',None),('append',False),('primary-key',True)]:
 selected=[k for k in p if k[1]=='exact' and (qs is None or (k[0] in keys)==qs)]
 summary[name]={lake:gm(ratio(v[k]) for k in selected) for lake,v in [('paimon',p),('iceberg',i)]}
lines=['# Same-host Kafka sink comparisons','',
 '| Query | Paimon stock / native median, s | Paimon speedup | Iceberg stock / native median, s | Iceberg speedup |',
 '|---|---:|---:|---:|---:|']
for key in sorted((k for k in p if k[1]=='exact'),key=lambda k:int(k[0][1:])):
 pv,iv=p[key],i[key]
 lines.append(f'| {key[0]} | {pv["stock"]:.3f} / {pv["native"]:.3f} | {ratio(pv):.2f}× | {iv["stock"]:.3f} / {iv["native"]:.3f} | {ratio(iv):.2f}× |')
lines += ['', '| Exact-query selection | Paimon geomean | Iceberg geomean |','|---|---:|---:|']
for k,v in summary.items():lines.append(f'| {k} | {v["paimon"]:.2f}× | {v["iceberg"]:.2f}× |')
lines += ['', 'Both use 2M unchanged Kafka JSON events, four input partitions, job parallelism four, RocksDB, mini-batching off, a 4 GiB JVM heap and 512 MiB managed memory across four slots. Each cell has one warmup and three alternating measured pairs. Both keep all samples and validate committed output.', '',
 'The formats retain their own sink semantics. Paimon uses four fixed buckets, default Zstd level 1, 256 MiB append / 128 MiB primary-key file targets and in-job compaction. Iceberg uses four bucket-transform partitions, Zstd level 3, a 512 MiB target and Java delete files for upserts. Writer parallelism is audited separately. Optional native library sets differ; production engine/query source code is unchanged. These separate runs are a same-host diagnostic comparison, not a concurrent connector-only A/B.', '',
 'A speedup difference can come from either stock or native timings. Inspect both columns and the retained trial ranges; neither ratios nor elapsed time alone identify source, sink or state cost.', '']
(paimon/'iceberg-comparison.md').write_text('\n'.join(lines))
(paimon/'iceberg-comparison.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))

import tarfile
pobs=[r for r in map(json.loads,(paimon/'observations.jsonl').read_text().splitlines()) if r['kind']=='measurement' and not r['warmup'] and r['variant']=='exact']
iobs=[r for r in map(json.loads,(iceberg/'observations.jsonl').read_text().splitlines()) if r['kind']=='measurement' and not r['warmup'] and r['variant']=='exact']
volumes=[]
with tarfile.open(paimon/'evidence.tar.gz') as pa:
 for query in ['q15','q16','q17','q19','q23']:
  for engine in ['stock','native']:
   for row in [r for r in pobs if r['query']==query and r['engine']==engine]:
    files=json.load(pa.extractfile(row['evidence']+'/files.json'))
    volumes.append(dict(format='paimon',query=query,engine=engine,trial=row['trial'],seconds=row['seconds'],retainedDataFileRows=sum(f['physicalRows'] for f in files),retainedDataFiles=len(files),retainedRustFiles=row['rustParquetFiles'],retainedBytes=row['parquetBytes']))
  with tarfile.open(iceberg/'cohorts/cohort-001'/f'{query}-disk-off-exact.tar.gz') as archive:
   for member in archive.getmembers():
    if not member.name.endswith('.metadata.json'):continue
    token=member.name.split('/')[2]
    match=[r for r in iobs if r['query']==query and r['table'].rsplit('/',1)[1]==token]
    if not match:continue
    row=match[0];metadata=json.load(archive.extractfile(member));counters={}
    for snapshot in metadata['snapshots']:
     for key,value in snapshot['summary'].items():
      if key in ['added-records','added-equality-deletes','added-position-deletes','added-data-files','added-files-size']:
       counters[key]=counters.get(key,0)+int(value)
    volumes.append(dict(format='iceberg',query=query,engine={'flink':'stock','streamfusion':'native'}[row['engine']],trial=row['trial'],seconds=row['seconds'],**counters))
assert len(volumes)==60
(paimon/'write-volumes.json').write_text(json.dumps(volumes,indent=2)+'\n')

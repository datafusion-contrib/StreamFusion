import hashlib,json,os,platform,re,shutil,statistics,subprocess,sys,time
from pathlib import Path
root=Path.cwd();line=sys.argv[1];baseline=sys.argv[2];native=root/'native/target/release';out=Path(os.environ['RUNNER_TEMP'])/('collections-'+line);out.mkdir(exist_ok=True)
profile='bench' if line=='2.2' else 'bench,flink-1.18'
previous=Path(os.environ['RUNNER_TEMP'])/('collections-previous-'+line)
env=os.environ.copy();env.update(SF_BENCHMARK='true',MAVEN_OPTS='-Xmx768m')
subprocess.run(['git','diff','--exit-code',baseline,'--','native'],check=True)
subprocess.run(['git','worktree','add','--detach',str(previous),baseline],check=True)
for name in ['DynamicCollectionBenchmark.java','RecoveryExecutionMetrics.java']:
 shutil.copyfile(root/'src/test/java/tech/streamfusion'/name,previous/'src/test/java/tech/streamfusion'/name)
common=['mvn','-B','-ntp','-pl','streamfusion-runtime','-P'+profile,'-Dnative.build.skip=true','-Dnative.test.resource.skip=true','-Dmaven.javadoc.skip=true','-Dnative.resource.directory='+str(native)]
def invoke(label,cwd,args):
 with (out/(label+'.log')).open('w') as log:r=subprocess.run(common+args,cwd=cwd,env=env,stdout=log,stderr=subprocess.STDOUT)
 if r.returncode:
  print((out/(label+'.log')).read_text()[-14000:],flush=True);raise SystemExit(r.returncode)
 return (out/(label+'.log')).read_text()
for label,cwd in [('candidate',root),('previous',previous)]:invoke('compile-'+label,cwd,['test-compile'])
cases={
 'mapvalues':('MAP_VALUES(m)',True,'BIGINT',3,'MAP_VALUES'),
 'mapkeys':('MAP_KEYS(m)',True,'BIGINT',3,'MAP_KEYS'),
 'mapentries':('MAP_ENTRIES(m)',True,'BIGINT',3,'MAP_ENTRIES'),
 'reverse8':('ARRAY_REVERSE(arr)',False,'BIGINT',8,'ARRAY_REVERSE'),
 'reverse64':('ARRAY_REVERSE(arr)',False,'BIGINT',64,'ARRAY_REVERSE'),
 'prepend64':('ARRAY_PREPEND(arr,CAST(idx AS BIGINT))',False,'BIGINT',64,'ARRAY_PREPEND'),
 'row':('ROW(idx,idx+1)',False,'BIGINT',8,'ROW'),
 'array':('ARRAY[CAST(idx AS BIGINT),CAST(NULL AS BIGINT)]',False,'BIGINT',8,'ARRAY'),
 'map':('MAP[CAST(idx AS BIGINT),CAST(idx AS BIGINT)]',False,'BIGINT',8,None),
 'join':("ARRAY_JOIN(arr,'|','?')",False,'STRING',8,'ARRAY_JOIN'),
 'typeof':('TYPEOF(idx)',False,'BIGINT',8,None),
}
if line=='1.18':del cases['prepend64']
meta={'line':line,'baseline':baseline,'candidate':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'cpu':next(s.split(':',1)[1].strip() for s in Path('/proc/cpuinfo').read_text().splitlines() if s.startswith('model name')),'platform':platform.platform(),'java':subprocess.check_output(['java','-version'],stderr=subprocess.STDOUT,text=True),'native_sha256':hashlib.sha256((native/'libstreamfusion.so').read_bytes()).hexdigest(),'native_features':'release, no-default-features, mimalloc','heap':'2 GiB','parallelism':1,'warmup':2,'runs':5,'rows':2000000,'method':'Fresh JVM per expression/cohort; alternating stock/native pairs; MiniCluster construction and explain outside timing; production sources, transposes, JNI, deployment and row sink inside.'}
records=[]
def save(): (out/'results.json').write_text(json.dumps({'environment':meta,'records':records},indent=2)+'\n')
selected=os.environ.get('COLLECTION_CASES',','.join(cases)).split(',')
for cohort,cwd in [('previous',previous),('candidate',root)]:
 for name in selected:
  expression,ismap,kind,width,function=cases[name]
  args=['surefire:test@default-test','-Dtest=DynamicCollectionBenchmark','-Dsf.extraJvmArgs=-Xmx2g -Djava.library.path='+str(native),'-Dcollection.expression='+expression,'-Dcollection.map='+str(ismap).lower(),'-Dcollection.type='+kind,'-Dcollection.width='+str(width),'-Dcollection.bytes=64','-Dcollection.rows=2000000','-Dcollection.warmup=2','-Dcollection.runs=5']
  if cohort=='candidate' and function:args.append('-Dcollection.rustFunction='+function)
  if name=='map':args.append('-Dcollection.generatedFunction=MAP')
  body=invoke(cohort+'-'+name,cwd,args)
  m=re.search(r'\[collection\].*Flink=([0-9.]+)s Native=([0-9.]+)s flink_trials=(\[.*?\]) native_trials=(\[.*?\])',body)
  if not m:raise RuntimeError('No results for '+cohort+'/'+name)
  rec={'cohort':cohort,'case':name,'expression':expression,'map_source':ismap,'type':kind,'width':width,'stock_trials':json.loads(m[3]),'native_trials':json.loads(m[4]),'route_evidence':[s for s in body.splitlines() if s.startswith('[collection-route]') or s.startswith('[collection-engine]')],'all_trials':[s for s in body.splitlines() if s.startswith('[collection-trial]')]}
  records.append(rec);save();print(cohort,name,statistics.median(rec['stock_trials']),statistics.median(rec['native_trials']),flush=True)
agent=Path(os.environ['RUNNER_TEMP'])/'async-profiler-4.6-linux-x64/lib/libasyncProfiler.so'
for cohort,cwd in [('previous',previous),('candidate',root)]:
 for name in os.environ.get('PROFILE_CASES','mapvalues,mapentries,reverse64,typeof').split(','):
  expression,ismap,kind,width,function=cases[name]
  label='profile-'+cohort+'-'+name
  args=['surefire:test@default-test','-Dtest=DynamicCollectionBenchmark','-Dsf.extraJvmArgs=-Xmx2g -Djava.library.path='+str(native)+' -agentpath:'+str(agent)+'=start,event=cpu,alloc=1m,interval=1ms,file='+str(out/(label+'-%p.jfr')),'-Dcollection.expression='+expression,'-Dcollection.map='+str(ismap).lower(),'-Dcollection.type='+kind,'-Dcollection.width='+str(width),'-Dcollection.bytes=64','-Dcollection.rows=2000000','-Dcollection.warmup=1','-Dcollection.runs=1']
  invoke(label,cwd,args)
print('All collection release comparisons and profiles completed',flush=True)

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { Substrate } from '../sdk/dist/index.js';
const dir=await mkdtemp(join(tmpdir(),'substrate-cluster-e2e-'));const children=[],sockets=[];const token='substrate-local-development-token';
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function until(f,timeout=20000){let error;const end=Date.now()+timeout;while(Date.now()<end){try{const v=await f();if(v)return v;}catch(e){error=e;}await pause(50);}throw error??new Error('deadline exceeded');}
async function port(){const s=createServer();await new Promise(r=>s.listen(0,'127.0.0.1',r));const p=s.address().port;await new Promise(r=>s.close(r));return p;}
async function ws(url,cap){const incoming=[];const socket=new WebSocket(url.replace('http:','ws:')+'/v1/ws');sockets.push(socket);socket.onmessage=e=>incoming.push(JSON.parse(e.data));await new Promise((r,j)=>{socket.onopen=r;socket.onerror=j;});socket.send(JSON.stringify({type:'auth',token:cap}));await until(()=>incoming.find(v=>v.type==='authenticated'));return{socket,incoming,send:v=>socket.send(JSON.stringify(v))};}
try{
 const peers={};for(let i=1;i<=3;i++)peers[i]=`http://127.0.0.1:${await port()}`;
 async function startNode(i){const config=join(dir,`${i}.json`);const child=spawn(resolve('target/debug/substrate'),['--dev','--cluster-config',config,'--listen',peers[i].replace('http://','')],{stdio:['ignore','pipe','pipe'],env:{...process.env,RUST_LOG:'warn'}});let log='';child.stderr.on('data',v=>log+=v);child.stdout.on('data',v=>log+=v);child.logs=()=>log;children.push(child);await until(async()=>{if(child.exitCode!==null)throw Error(log);return(await fetch(`${peers[i]}/health`)).ok;});return child;}
 const active={};for(let i=1;i<=3;i++){await mkdir(join(dir,String(i)));await writeFile(join(dir,`${i}.json`),JSON.stringify({node_id:i,advertise:peers[i],peers,data_dir:join(dir,String(i)),shards:2,secret:'cluster-e2e-shared-internal-secret',bootstrap:i===1}));active[i]=await startNode(i);}

 const db=new Substrate({url:peers[1],token,namespace:'app',partition:'main'});
 await until(async()=>{await db.collection('docs').put('one',{owner:'alice',value:1});return true;});
 const service=await db.serviceToken({id:'service',namespace:'app',partition:'main',resources:['collection:docs'],permissions:['read'],expires_at_ms:Date.now()+60000});const reader=new Substrate({url:peers[2],token:service.token,namespace:'app',partition:'main'});assert.equal((await reader.collection('docs').get('one')).value.value,1);await assert.rejects(reader.collection('docs').put('one',{value:2}),e=>e.code==='forbidden');await assert.rejects(reader.scope('other','main').collection('docs').get('one'),e=>e.code==='forbidden');
 const cap=await db.capability({id:'browser',namespace:'app',partition:'main',resources:['room','docs'],permissions:['publish','subscribe','presence'],expires_at_ms:Date.now()+60000});
 const a=await ws(peers[1],cap.token),b=await ws(peers[2],cap.token);a.send({type:'channel',topic:'room'});b.send({type:'channel',topic:'room'});await until(()=>a.incoming.some(v=>v.type==='joined')&&b.incoming.some(v=>v.type==='joined'));
 a.send({type:'publish',topic:'room',payload:{crossNode:true}});await until(()=>b.incoming.some(v=>v.type==='publish'&&v.payload.crossNode));
 a.send({type:'presence',topic:'room',state:{name:'Alice'}});await until(()=>b.incoming.some(v=>v.type==='presence'&&v.members.some(m=>m.state.name==='Alice')));
 a.socket.close();await until(()=>b.incoming.some(v=>v.type==='presence_leave'));
 b.send({type:'subscribe',collection:'docs',id:'one'});await until(()=>b.incoming.some(v=>v.type==='snapshot'&&v.documents.length===1));await db.collection('docs').delete('one');await until(()=>b.incoming.some(v=>v.type==='changes'&&v.removed.includes('one')));
 await db.revoke('service');await assert.rejects(reader.collection('docs').get('one'),e=>e.code==='unauthorized');
 // Kill a real replica, accumulate a log larger than the RPC body bound, then recover it.
 active[3].kill('SIGKILL');await new Promise(r=>active[3].once('exit',r));
 for(let i=0;i<16;i++){await until(async()=>{await db.command('cache.set',{key:`large-${i}`,value:'x'.repeat(600000)},{requestId:`large-${i}`});return true;});}
 const applied=async(origin)=>{const text=await(await fetch(`${origin}/metrics`,{headers:{Authorization:`Bearer ${token}`}})).text();return [...text.matchAll(/substrate_cluster_shard_applied_index\{shard="(\d+)"\} (\d+)/g)].map(m=>[m[1],Number(m[2])]);};
 const boundary=await applied(peers[1]);assert.equal(boundary.length,2);
 active[3]=await startNode(3);
 let observed;try{await until(async()=>{observed=Object.fromEntries(await applied(peers[3]));return boundary.every(([shard,index])=>observed[shard]>=index);},30000);}catch(error){console.error({boundary,observed});throw error;}
 const recovered=new Substrate({url:peers[3],token,namespace:'app',partition:'main'});assert.equal((await recovered.cache.get('large-15')).length,600000);
 console.log('Cluster E2E passed: three binary processes, scoped service credentials, cross-node channels/presence, individual-document WebSocket snapshot/delete, revocation, killed-replica restart and large-log catch-up.');
}catch(error){for(const [i,child] of children.entries())console.error(`Node process ${i+1}: ${child.logs?.()}`);throw error;}finally{for(const socket of sockets)socket.close();for(const child of children)if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');await Promise.all(children.map(child=>(child.exitCode!==null||child.signalCode!==null)?Promise.resolve():new Promise(r=>child.once('exit',r))));await rm(dir,{recursive:true,force:true});}

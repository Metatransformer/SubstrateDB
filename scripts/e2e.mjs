import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createServer, request } from 'node:http';
import { mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createHmac } from 'node:crypto';
import { Substrate } from '../sdk/dist/index.js';

const temporary=await mkdtemp(join(tmpdir(),'substrate-e2e-'));
const token='substrate-local-development-token';
let child, socket, destination;
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function until(fn,ms=10000){const end=Date.now()+ms;let last;while(Date.now()<end){try{const result=await fn();if(result)return result;}catch(e){last=e;}await pause(50);}throw last??new Error('deadline exceeded');}
async function start(){
  await rm(join(temporary,'runtime.sock'),{force:true});
  let output='';child=spawn(resolve('target/debug/substrate'),['--dev','--data',join(temporary,'db.redb'),'--listen','127.0.0.1:0','--unix-socket',join(temporary,'runtime.sock')],{stdio:['ignore','pipe','pipe'],env:{...process.env,RUST_LOG:'info',NO_COLOR:'1'}});
  child.stdout.on('data',s=>output+=s);child.stderr.on('data',s=>output+=s);
  const port=await until(()=>{if(child.exitCode!==null)throw new Error(output);return output.match(/127\.0\.0\.1:(\d+)/)?.[1];});
  const url=`http://127.0.0.1:${port}`;await until(async()=>{const r=await fetch(`${url}/health`);return r.ok;});return url;
}
async function stop(crash=false){if(child&&child.exitCode===null){child.kill(crash?'SIGKILL':'SIGINT');await new Promise(r=>child.once('exit',r));}child=undefined;}
try{
  let url=await start();let db=new Substrate({url,token,namespace:'shop',partition:'customer-1'});
  const ipc=await new Promise((resolve,reject)=>{const req=request({socketPath:join(temporary,'runtime.sock'),path:'/health'},res=>{res.resume();resolve(res.statusCode);});req.on('error',reject);req.end();});assert.equal(ipc,200);assert.equal((await stat(join(temporary,'runtime.sock'))).mode&0o777,0o600);
  assert.equal((await fetch(`${url}/v1/command`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'})).status,401);
  await db.collection('orders').createIndex('customer',['customer']);
  const order=await db.collection('orders').saveAndEnqueue('o1',{customer:'c1',status:'new',body:'rust database'},{id:'process-1',queue:'orders',payload:{order:'o1'}},{requestId:'order-1'});
  assert.equal(order.document.revision,1);const claim=await db.jobs.claim('orders','e2e');assert.equal(claim.job.id,'process-1');await db.jobs.ack(claim.job.id,claim.receipt);
  await db.cache.set('visits',0);assert.equal(await db.cache.increment('visits'),1);
  await db.vectors.configure('orders','semantic',3);const indexed=await db.vectors.upsert('orders','o1','semantic',[1,0,0],1);await db.vectors.wait('orders',indexed.sequence);
  const results=await db.vectors.search({collection:'orders',name:'semantic',vector:[1,0,0],text:'database',filter:{customer:'c1'}});assert.equal(results.hits[0].id,'o1');
  const capability=await db.capability({id:'browser',namespace:'shop',partition:'customer-1',resources:['orders'],permissions:['subscribe'],expires_at_ms:Date.now()+60000,filter:{customer:'c1'}});
  const incoming=[];socket=new WebSocket(url.replace('http:','ws:')+'/v1/ws');socket.onmessage=e=>incoming.push(JSON.parse(e.data));
  await new Promise((r,j)=>{socket.onopen=r;socket.onerror=j;});socket.send(JSON.stringify({type:'auth',token:capability.token}));
  await until(()=>incoming.find(x=>x.type==='authenticated'));socket.send(JSON.stringify({type:'subscribe',collection:'orders',filter:{customer:'other'}}));
  const snapshot=await until(()=>incoming.find(x=>x.type==='snapshot'));assert.equal(snapshot.documents.length,1);assert.equal(snapshot.documents[0].value.customer,'c1');
  await db.collection('orders').put('o1',{customer:'c1',status:'processed'},1);
  const change=await until(()=>incoming.find(x=>x.type==='changes'));assert.equal(change.updated[0].value.status,'processed');
  await db.revoke('browser');await until(()=>incoming.find(x=>x.type==='error'&&x.error.code==='unauthorized'));socket.close();socket=undefined;
  let delivered=false;destination=createServer(async(req,res)=>{let body='';for await(const chunk of req)body+=chunk;const time=req.headers['substrate-timestamp'];const job=req.headers['substrate-job-id'];const expected=createHmac('sha256',`substrate-signing-v1:${token}`).update(`${time}.${job}.${body}`).digest('base64url');assert.equal(req.headers['substrate-signature'],expected);assert.deepEqual(JSON.parse(body),{hello:'worker'});delivered=true;res.writeHead(200);res.end('ok');});
  await new Promise(r=>destination.listen(0,'127.0.0.1',r));
  await db.jobs.enqueue({id:'http-job',queue:'http',payload:{hello:'worker'},delivery:{url:`http://127.0.0.1:${destination.address().port}/work`}});
  await until(async()=>delivered&&(await db.jobs.get('http-job')).status==='completed');
  await stop(true);url=await start();db=new Substrate({url,token,namespace:'shop',partition:'customer-1'});assert.equal((await db.collection('orders').get('o1')).value.status,'processed');assert.equal((await db.jobs.get('http-job')).status,'completed');
  console.log('E2E passed: HTTP auth, atomic order+job, worker ack, cache, async hybrid search, scoped WebSocket changes/revocation, signed HTTP delivery, SIGKILL recovery, Unix-socket IPC.');
}finally{socket?.close();await stop();if(destination)await new Promise(r=>destination.close(r));await rm(temporary,{recursive:true,force:true});}

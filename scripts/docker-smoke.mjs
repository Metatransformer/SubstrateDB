import { execFileSync } from 'node:child_process';
import assert from 'node:assert/strict';
import { Substrate } from '../sdk/dist/index.js';
const name=`substrate-smoke-${crypto.randomUUID()}`;const token=crypto.randomUUID();
const docker=(...args)=>execFileSync('docker',args,{encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function ready(url){let last;for(let i=0;i<100;i++){try{if((await fetch(`${url}/health`)).ok)return;}catch(e){last=e;}await pause(100);}throw last??Error('container not ready');}
try{
 docker('run','--rm','-d','--name',name,'-e',`SUBSTRATE_ADMIN_TOKEN=${token}`,'-p','127.0.0.1::7432','substratedb:dev');
 let url=`http://${docker('port',name,'7432/tcp')}`;await ready(url);assert.equal(docker('exec',name,'id','-u'),'10001');
 let db=new Substrate({url,token,namespace:'docker',partition:'main'});await db.collection('docs').put('persisted',{works:true});
 docker('restart',name);url=`http://${docker('port',name,'7432/tcp')}`;await ready(url);db=new Substrate({url,token,namespace:'docker',partition:'main'});assert.equal((await db.collection('docs').get('persisted')).value.works,true);
 console.log('Docker smoke passed: Linux runtime, non-root user, authenticated writes, persisted data after container restart.');
}finally{try{docker('rm','-f',name);}catch{}}

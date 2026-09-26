require('dotenv').config();
const express=require('express');
const {fork}=require('child_process');
const path=require('path');
const PORT=Number(process.env.PORT||10000);
let BOT_ID=Number(process.env.BOT_ID||0);
const MASTER_URL=String(process.env.MASTER_URL||'').replace(/\/$/,'');
const AGENT_SECRET=String(process.env.AGENT_SECRET||'');
if(!AGENT_SECRET)console.warn('⚠️ Configure AGENT_SECRET no Render deste Agent. BOT_ID é opcional.');
const app=express();app.use(express.json({limit:'512kb'}));
let child=null,currentConfig=null,logs=[];
let desiredRunning=false;
let stopPromise=null;
let lastChildHeartbeat=0, recoveryCount=0, watchdogRestarts=0, lastRecoveryAt=null;
const WATCHDOG_INTERVAL_MS=Math.max(30000,Number(process.env.WATCHDOG_INTERVAL_MS||60000));
const WATCHDOG_STALE_MS=Math.max(90000,Number(process.env.WATCHDOG_STALE_MS||150000));
let status={reachable:true,running:false,online:false,phase:'offline',lastReason:'',startedAt:null,pid:null,lastChangeAt:new Date().toISOString(),recoveryCount:0,watchdogRestarts:0,lastRecoveryAt:null,region:'',connectedAt:null};
function addLog(x){const line=`[${new Date().toISOString()}] ${String(x).trim()}`;logs.push(line);while(logs.length>180)logs.shift();console.log(line);heartbeat(String(x).trim()).catch(()=>{})}
function auth(req,res,next){const got=String(req.headers.authorization||'').replace(/^Bearer\s+/i,'');if(got!==AGENT_SECRET)return res.status(403).json({ok:false,error:'Agent Secret inválido'});next()}
function setStatus(p){status={...status,...p,reachable:true,lastChangeAt:new Date().toISOString()}}
function childMessage(m={}){if(m.type==='BOT_HEARTBEAT'){lastChildHeartbeat=Date.now();setStatus({running:true,pid:child&&child.pid,region:m.payload?.region||status.region||''});return;}if(m.type==='SL_ONLINE'){lastChildHeartbeat=Date.now();setStatus({running:true,online:true,phase:'online',lastReason:'',region:m.payload?.region||status.region||'',connectedAt:status.connectedAt||new Date().toISOString()});}if(m.type==='SL_CONNECTING')setStatus({running:true,online:false,phase:m.payload?.phase||'connecting',lastReason:m.payload?.reason||'Conectando...'});if(m.type==='SL_OFFLINE')setStatus({running:!!child,online:false,phase:m.payload?.phase||'offline',lastReason:m.payload?.reason||'Offline',connectedAt:null});}
function start(config){
 desiredRunning=true;
 if(child)return {ok:true,message:'Bot já está ligado.'};
 if(config)currentConfig=config;
 if(!currentConfig)throw new Error('Configuração do bot não carregada.');
 const safe={...currentConfig};delete safe.agentSecret;delete safe.renderApiKey;
 const env={...process.env,BOT_ID:String(BOT_ID),BOT_CONFIG_B64:Buffer.from(JSON.stringify(safe)).toString('base64')};
 child=fork(path.join(__dirname,'botsl.js'),[],{silent:true,env});
 lastChildHeartbeat=Date.now();
 const thisChild=child;
 setStatus({running:true,online:false,phase:'starting',lastReason:'Processo iniciado',startedAt:new Date().toISOString(),pid:child.pid});
 addLog('🚀 Processo do bot iniciado.');
 child.stdout.on('data',d=>addLog(d));child.stderr.on('data',d=>addLog(d));child.on('message',childMessage);
 child.on('exit',(c,signal)=>{
   addLog(`🛑 Processo finalizado código ${c}${signal?` sinal ${signal}`:''}`);
   if(child===thisChild)child=null;
   setStatus({running:false,online:false,phase:'offline',pid:null,lastReason:`Processo finalizado código ${c}${signal?` (${signal})`:''}`});
   // Só tenta religar se o painel ainda deseja o bot online.
   if(desiredRunning)setTimeout(()=>bootstrap(true).catch(e=>addLog('Bootstrap após queda: '+e.message)),8000);
 });
 return {ok:true,message:'Bot ligado.'};
}
async function stop(){
 desiredRunning=false;
 if(stopPromise)return stopPromise;
 if(!child){setStatus({running:false,online:false,phase:'offline',pid:null,lastReason:'Desligado pelo painel'});return {ok:true,message:'Bot já está desligado.'}}
 const c=child;
 setStatus({running:true,online:false,phase:'stopping',lastReason:'Encerrando processo...'});
 addLog('⏹️ Desligamento solicitado pelo painel.');
 stopPromise=new Promise(resolve=>{
   let done=false;
   const finish=(forced=false)=>{if(done)return;done=true;clearTimeout(forceTimer);clearTimeout(finalTimer);if(child===c)child=null;setStatus({running:false,online:false,phase:'offline',pid:null,lastReason:forced?'Processo encerrado à força':'Desligado pelo painel'});stopPromise=null;resolve({ok:true,message:forced?'Bot desligado (encerramento forçado).':'Bot desligado.'})};
   c.once('exit',()=>finish(false));
   try{c.kill('SIGTERM')}catch{finish(false);return}
   const forceTimer=setTimeout(()=>{if(done)return;addLog('⚠️ Processo não encerrou com SIGTERM; enviando SIGKILL.');try{c.kill('SIGKILL')}catch{}},8000);
   const finalTimer=setTimeout(()=>finish(true),12000);
 });
 return stopPromise;
}
async function restart(config){desiredRunning=false;await stop();if(config)currentConfig=config;return start(currentConfig)}
async function watchdog(){
 if(!desiredRunning||!child||stopPromise)return;
 const age=Date.now()-lastChildHeartbeat;
 if(age<WATCHDOG_STALE_MS)return;
 watchdogRestarts++; recoveryCount++; lastRecoveryAt=new Date().toISOString();
 setStatus({phase:'recovering',online:false,lastReason:`Watchdog: processo sem resposta por ${Math.round(age/1000)}s`,watchdogRestarts,recoveryCount,lastRecoveryAt});
 addLog(`⚠️ Watchdog detectou processo sem resposta por ${Math.round(age/1000)}s. Recuperando...`);
 const cfg=currentConfig; await stop(); desiredRunning=true; setTimeout(()=>{try{start(cfg)}catch(e){addLog('Auto Recovery: '+e.message)}},5000);
}
function send(type,payload={}){if(!child)return {ok:false,error:'Bot desligado.'};if(!status.online)return {ok:false,error:`Bot ainda não está online. Status: ${status.phase}`};child.send({type,payload});addLog('📨 '+type);return {ok:true,message:'Comando enviado.'}}
async function bootstrap(autoStart=true){if(!MASTER_URL||!AGENT_SECRET)return;const r=await fetch(MASTER_URL+'/api/agent/bootstrap',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({botId:BOT_ID,agentSecret:AGENT_SECRET,status})});const d=await r.json();if(!r.ok||!d.ok)throw new Error(d.error||`Master HTTP ${r.status}`);BOT_ID=Number(d.botId||d.config?.id||BOT_ID);currentConfig=d.config;desiredRunning=!!d.desiredOnline;if(autoStart&&desiredRunning&&!child)start(currentConfig);if(!desiredRunning&&child)await stop();return d}
async function heartbeat(log){if(!MASTER_URL||!AGENT_SECRET)return;try{const r=await fetch(MASTER_URL+'/api/agent/heartbeat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({botId:BOT_ID,agentSecret:AGENT_SECRET,status,log:log||''})});if(r.ok){const d=await r.json();desiredRunning=!!d.desiredOnline;if(!desiredRunning&&child)await stop();}}catch{}}
app.get('/',(_q,r)=>r.json({ok:true,agent:'SL Bot Agent',botId:BOT_ID,status}));app.get('/health',(_q,r)=>r.send('ok'));
app.get('/status',auth,(_q,r)=>r.json({ok:true,status}));app.get('/logs',auth,(_q,r)=>r.json({ok:true,logs}));
app.post('/start',auth,(q,r)=>{try{r.json(start(q.body?.config))}catch(e){r.status(400).json({ok:false,error:e.message})}});
app.post('/stop',auth,async(_q,r)=>{try{r.json(await stop())}catch(e){r.status(500).json({ok:false,error:e.message})}});
app.post('/restart',auth,async(q,r)=>{try{r.json(await restart(q.body?.config))}catch(e){r.status(400).json({ok:false,error:e.message})}});
app.post('/home',auth,(_q,r)=>r.json(send('GO_HOME')));app.post('/invite',auth,(q,r)=>r.json(send('GROUP_INVITE',q.body||{})));
app.listen(PORT,'0.0.0.0',()=>{console.log(`✅ Agent do bot ${BOT_ID||'?'} ativo na porta ${PORT}`);bootstrap(true).catch(e=>addLog('Bootstrap: '+e.message));setInterval(()=>heartbeat().catch(()=>{}),60000);setInterval(()=>watchdog().catch(e=>addLog('Watchdog: '+e.message)),WATCHDOG_INTERVAL_MS)});
process.on('SIGTERM',async()=>{desiredRunning=false;try{await stop()}catch{}process.exit(0)});

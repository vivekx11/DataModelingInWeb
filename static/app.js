/* ══ DATA MINING PRO — Core App Logic ══ */
let currentModule = 'ai';
const modules = ['ai','etl','bi','sql'];

function switchModule(mod) {
  currentModule = mod;
  modules.forEach(m => {
    const el = document.getElementById('mod-'+m);
    const nav = document.getElementById('nav-'+m);
    if(el) el.classList.toggle('active', m===mod);
    if(nav) nav.classList.toggle('active', m===mod);
  });
}

function fmt(v) {
  if(v===null||v===undefined) return '—';
  const n=parseFloat(v);
  if(isNaN(n)) return v;
  return Math.abs(n)>=1000?n.toLocaleString(undefined,{maximumFractionDigits:0}):n.toFixed(2);
}

function escHtml(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML;}

function chartOpts(title){
  return {responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{backgroundColor:'#0c1018',borderColor:'#1e2d45',borderWidth:1,titleFont:{family:'JetBrains Mono',size:10},bodyFont:{family:'JetBrains Mono',size:10}}},scales:{x:{ticks:{color:'#64748b',font:{family:'JetBrains Mono',size:9},maxRotation:30},grid:{color:'#1e2d45'}},y:{ticks:{color:'#64748b',font:{family:'JetBrains Mono',size:9}},grid:{color:'#1e2d45'}}}};
}

/* ══ LOADING RING HELPER ══ */
function showLoading(modId, stages) {
  const wrap = document.getElementById(modId+'-loading');
  const upload = document.getElementById(modId+'-upload');
  const dash = document.getElementById(modId+'-dash');
  if(upload) upload.style.display='none';
  if(dash) dash.style.display='none';
  if(wrap) wrap.style.display='flex';
  let idx=0;
  const timer = setInterval(()=>{
    idx=Math.min(idx+1,stages.length-1);
    const pct=stages[idx][0], msg=stages[idx][1];
    const pctEl=document.getElementById(modId+'-pct');
    const barEl=document.getElementById(modId+'-bar');
    const stgEl=document.getElementById(modId+'-stage');
    const ringEl=document.getElementById(modId+'-ring');
    if(pctEl) pctEl.textContent=pct+'%';
    if(barEl) barEl.style.width=pct+'%';
    if(stgEl) stgEl.textContent=msg;
    if(ringEl){const c=2*Math.PI*52;ringEl.setAttribute('stroke-dasharray',`${pct/100*c} ${c}`);}
  },800);
  return timer;
}

function showDash(modId, timer) {
  clearInterval(timer);
  const wrap=document.getElementById(modId+'-loading');
  const dash=document.getElementById(modId+'-dash');
  if(wrap) wrap.style.display='none';
  if(dash) dash.style.display='block';
}

function showUpload(modId) {
  const upload=document.getElementById(modId+'-upload');
  const dash=document.getElementById(modId+'-dash');
  const loading=document.getElementById(modId+'-loading');
  if(upload) upload.style.display='flex';
  if(dash) dash.style.display='none';
  if(loading) loading.style.display='none';
}

/* ══ GENERIC DROP ZONE SETUP ══ */
function setupDrop(zoneId, inputId, handler) {
  const dz=document.getElementById(zoneId);
  const fi=document.getElementById(inputId);
  if(!dz||!fi) return;
  dz.addEventListener('click',()=>fi.click());
  fi.addEventListener('change',e=>{if(e.target.files[0]) handler(e.target.files[0]);});
  dz.addEventListener('dragover',e=>{e.preventDefault();dz.classList.add('over');});
  dz.addEventListener('dragleave',()=>dz.classList.remove('over'));
  dz.addEventListener('drop',e=>{e.preventDefault();dz.classList.remove('over');if(e.dataTransfer.files[0]) handler(e.dataTransfer.files[0]);});
}

/* ══ BUILD KPI CARDS ══ */
function buildKPIs(containerId, kpis) {
  document.getElementById(containerId).innerHTML = kpis.map(k=>`
    <div class="kpi" style="--kc:${k.color}">
      <div class="kpi-lbl">${k.label}</div>
      <div class="kpi-val">${k.val}</div>
      <div class="kpi-sub">${k.sub}</div>
    </div>`).join('');
}

/* ══ BUILD LOG LIST ══ */
function buildLogList(containerId, logs, badgeId) {
  if(badgeId) document.getElementById(badgeId).textContent=logs.length+' steps';
  const el=document.getElementById(containerId);
  if(Array.isArray(logs) && logs.length>0 && typeof logs[0]==='object') {
    el.innerHTML=logs.map(l=>`<div class="log-item"><span class="log-phase ph-${(l.phase||'load').toLowerCase()}">${l.phase||''}</span><span class="log-msg">${l.msg}</span></div>`).join('');
  } else {
    el.innerHTML=logs.map(l=>`<div class="log-item"><div class="log-dot"></div><span class="log-msg">${l}</span></div>`).join('');
  }
}

/* ══ BUILD DATA PREVIEW TABLE ══ */
function buildPreviewTable(wrapId, infoId, badgeId, cols, rows, pageState) {
  const PER=12;
  const total=Math.ceil(rows.length/PER);
  const slice=rows.slice(pageState.page*PER,(pageState.page+1)*PER);
  if(badgeId) document.getElementById(badgeId).textContent=rows.length+' rows';
  if(infoId) document.getElementById(infoId).textContent=`Page ${pageState.page+1} of ${total||1}`;
  document.getElementById(wrapId).innerHTML=`<table><thead><tr>${cols.map(c=>`<th>${c}</th>`).join('')}</tr></thead><tbody>${slice.map(r=>`<tr>${cols.map(c=>`<td>${r[c]??''}</td>`).join('')}</tr>`).join('')}</tbody></table>`;
}

/* ══ DEMO CSV DATA ══ */
const DEMO_CSV = `Product,Category,Revenue,Cost,Units,Region,Month
Laptop Pro,Electronics,850000,550000,100,North,Jan
Wireless Mouse,Accessories,36000,18000,300,South,Jan
Monitor 4K,Electronics,640000,420000,200,East,Jan
Keyboard RGB,Accessories,70000,42000,200,West,Jan
SSD 1TB,Storage,150000,90000,200,North,Feb
Laptop Pro,Electronics,920000,590000,110,South,Feb
Graphics Card,Electronics,900000,580000,200,East,Feb
Headphones,Audio,160000,96000,200,West,Feb
Tablet,Electronics,560000,360000,200,North,Mar
Smartphone,Electronics,660000,420000,300,South,Mar
Router WiFi,Networking,64000,38000,200,East,Mar
Webcam HD,Electronics,90000,54000,200,West,Mar
Laptop Pro,Electronics,980000,630000,115,North,Apr
SSD 1TB,Storage,180000,108000,240,South,Apr
Monitor 4K,Electronics,700000,460000,220,East,Apr
Headphones,Audio,176000,104000,220,West,Apr`;

function demoFile(name){
  return new File([new Blob([DEMO_CSV],{type:'text/csv'})], name||'demo_sales.csv',{type:'text/csv'});
}

/* ══ AI DATA ANALYST MODULE ══ */
let aiCharts={};
const AI_STAGES=[[10,'Uploading file...'],[30,'Cleaning data...'],[55,'Running ML metrics...'],[80,'Generating visualizations...'],[95,'Finalizing...']];

function aiUpload(file){
  const timer=showLoading('ai',AI_STAGES);
  const fd=new FormData();fd.append('file',file);
  fetch('/ai/upload',{method:'POST',body:fd}).then(r=>r.json()).then(data=>{
    if(data.error){alert(data.error);showUpload('ai');clearInterval(timer);return;}
    window.aiData=data;
    showDash('ai',timer);
    aiRenderDash(data);
  }).catch(e=>{alert(e.message);showUpload('ai');clearInterval(timer);});
}

function aiRenderDash(d){
  buildKPIs('ai-kpis',[
    {label:'Rows',val:d.final_shape?.[0]||'-',sub:`original: ${d.original_shape?.[0]||'-'}`,color:'var(--accent)'},
    {label:'Columns',val:d.final_shape?.[1]||'-',sub:'after cleaning',color:'var(--accent2)'},
    {label:'Quality',val:d.quality_score||'-',sub:'out of 100',color:'var(--green)'},
    {label:'Accuracy',val:d.accuracy_score||'-',sub:'ML R² score',color:'var(--yellow)'}
  ]);
  // Change log
  let html='<table><thead><tr><th>Operation</th><th>Details</th><th>Impact</th></tr></thead><tbody>';
  html+=`<tr><td>📊 Original</td><td>${d.original_shape?.[0]||0} rows, ${d.original_shape?.[1]||0} cols</td><td>—</td></tr>`;
  html+=`<tr><td>🗑️ Duplicates</td><td>${d.duplicates_removed||0} rows</td><td>${d.duplicates_removed?'Removed':'None'}</td></tr>`;
  if(d.missing_values) for(const[col,val]of Object.entries(d.missing_values)) html+=`<tr><td>🔧 Missing</td><td>${col}: ${val}</td><td>Filled</td></tr>`;
  if(d.outliers) for(const[col,val]of Object.entries(d.outliers)) html+=`<tr><td>⚠️ Outliers</td><td>${col}: ${val}</td><td>Removed</td></tr>`;
  html+=`<tr><td>✅ Final</td><td>${d.final_shape?.[0]||0} rows, ${d.final_shape?.[1]||0} cols</td><td>Clean</td></tr>`;
  html+='</tbody></table>';
  document.getElementById('ai-changelog').innerHTML=html;
  // Accuracy metrics
  if(d.accuracy_metrics&&Object.keys(d.accuracy_metrics).length>0){
    document.getElementById('ai-metrics').innerHTML=Object.entries(d.accuracy_metrics).map(([k,v])=>`<div class="kpi" style="--kc:var(--green)"><div class="kpi-lbl">${k}</div><div class="kpi-val">${typeof v==='number'?v.toFixed(4):v}</div></div>`).join('');
  } else {
    document.getElementById('ai-metrics').innerHTML='<p style="color:var(--muted);font-family:var(--mono);font-size:11px">Upload data with numeric columns for ML metrics.</p>';
  }
  document.getElementById('ai-downloads').style.display='block';
}

function aiChart(type){
  fetch('/ai/chart',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({chart_type:type})}).then(r=>r.json()).then(d=>{
    if(d.chart){const cd=JSON.parse(d.chart);Plotly.newPlot('ai-chart-container',cd.data,{...cd.layout,paper_bgcolor:'#111827',plot_bgcolor:'#111827',font:{color:'#94a3b8'}});}
    else if(d.error) alert(d.error);
  });
}

function aiSendChat(){
  const inp=document.getElementById('ai-chat-input');
  const msg=inp.value.trim();if(!msg)return;
  const msgs=document.getElementById('ai-chat-msgs');
  msgs.innerHTML+=`<div class="msg user"><div class="msg-content">${escHtml(msg)}</div></div>`;
  inp.value='';msgs.scrollTop=msgs.scrollHeight;
  fetch('/ai/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:msg})}).then(r=>r.json()).then(d=>{
    msgs.innerHTML+=`<div class="msg ai"><div class="msg-content">${escHtml(d.response)}</div></div>`;
    msgs.scrollTop=msgs.scrollHeight;
  });
}

function aiShowTab(tab){
  ['ai-tab-log','ai-tab-viz','ai-tab-acc','ai-tab-chat'].forEach(t=>{
    document.getElementById(t).style.display=t==='ai-tab-'+tab?'block':'none';
  });
  document.querySelectorAll('#ai-tabs .tab').forEach(b=>b.classList.remove('active'));
  event.target.classList.add('active');
}

setupDrop('ai-dropzone','ai-file-input',aiUpload);

/* ══ ETL MODULE ══ */
let etlData=null, etlPage={page:0}, etlCharts={};
const ETL_STAGES=[[10,'Detecting schema...'],[30,'Imputing missing values...'],[50,'Removing outliers...'],[70,'Training ML models...'],[90,'Saving results...'],[97,'Finalizing...']];

function etlUpload(file){
  const timer=showLoading('etl',ETL_STAGES);
  const fd=new FormData();fd.append('file',file);
  fetch('/etl/upload',{method:'POST',body:fd}).then(r=>r.json()).then(j=>{
    if(j.error){alert(j.error);showUpload('etl');clearInterval(timer);return;}
    fetch(`/etl/results/${j.job_id}`).then(r=>r.json()).then(d=>{
      etlData=d;etlData._jobId=j.job_id;
      showDash('etl',timer);
      etlRenderDash(d);
    });
  }).catch(e=>{alert(e.message);showUpload('etl');clearInterval(timer);});
}

function etlRenderDash(d){
  document.getElementById('etl-subtitle').textContent=`${d.filename} · ${d.clean_shape.rows.toLocaleString()} rows × ${d.clean_shape.cols} cols`;
  const kpis=[
    {label:'Records',val:d.clean_shape.rows.toLocaleString(),sub:`raw: ${d.original_shape.rows.toLocaleString()}`,color:'var(--accent)'},
    {label:'Columns',val:d.clean_shape.cols,sub:`orig: ${d.original_shape.cols}`,color:'var(--accent2)'},
    {label:'Cleaned',val:(d.original_shape.rows-d.clean_shape.rows).toLocaleString(),sub:'removed',color:'var(--yellow)'},
  ];
  if(d.ml&&d.ml.best_model) kpis.push({label:'Best Model',val:d.ml.best_model.split(' ')[0],sub:d.ml.best_metrics.R2!==undefined?`R² ${d.ml.best_metrics.R2}`:`Acc ${d.ml.best_metrics.Accuracy}`,color:'var(--green)'});
  buildKPIs('etl-kpis',kpis);
  buildLogList('etl-log',d.logs,'etl-log-badge');
  // Distribution chart
  const distSel=document.getElementById('etl-dist-sel');
  distSel.innerHTML=Object.keys(d.distributions||{}).map(c=>`<option>${c}</option>`).join('');
  etlRenderDist();
  // Cat chart
  const catSel=document.getElementById('etl-cat-sel');
  catSel.innerHTML=Object.keys(d.cat_counts||{}).map(c=>`<option>${c}</option>`).join('');
  etlRenderCat();
  // Stats table
  document.getElementById('etl-stats-body').innerHTML=Object.entries(d.stats||{}).map(([col,s])=>`<tr><td style="color:var(--text)">${col}</td><td>${fmt(s.min)}</td><td>${fmt(s.max)}</td><td style="color:var(--accent)">${fmt(s.mean)}</td><td>${fmt(s.median)}</td><td style="color:var(--green)">${fmt(s.sum)}</td></tr>`).join('');
  // ML panel
  if(d.ml&&d.ml.best_model&&!d.ml.error){
    document.getElementById('etl-ml').style.display='block';
    document.getElementById('etl-ml-badge').textContent=d.ml.problem_type.toUpperCase();
    document.getElementById('etl-ml-best').textContent='✓ Best: '+d.ml.best_model;
    document.getElementById('etl-model-rows').innerHTML=Object.entries(d.ml.all_scores).map(([n,s])=>{
      const best=n===d.ml.best_model;const sc=Object.entries(s).filter(([k])=>k!=='error').map(([k,v])=>`${k}: ${v}`).join(' · ');
      return `<div class="ml-row" style="${best?'border-color:var(--green)40;background:var(--green)06':''}"><span class="ml-name">${best?'★ ':''}${n}</span><span class="ml-val" style="color:${best?'var(--green)':'var(--accent)'}">${sc}</span></div>`;
    }).join('');
    const fi=d.ml.feature_importance||{};
    if(Object.keys(fi).length>0){const mx=Math.max(...Object.values(fi));document.getElementById('etl-fi').innerHTML=Object.entries(fi).slice(0,6).map(([f,v])=>`<div class="fi-item"><div class="fi-lbl"><span>${f}</span><span style="color:var(--accent)">${(v*100).toFixed(1)}%</span></div><div class="fi-bg"><div class="fi-fill" style="width:${v/mx*100}%"></div></div></div>`).join('');}
  }
  // Preview
  etlPage.page=0;
  etlRenderPreview();
}

function etlRenderDist(){
  if(!etlData||!etlData.distributions)return;
  const col=document.getElementById('etl-dist-sel').value;
  const dist=etlData.distributions[col];if(!dist)return;
  if(etlCharts.dist)etlCharts.dist.destroy();
  etlCharts.dist=new Chart(document.getElementById('etl-dist-chart'),{type:'bar',data:{labels:dist.bins.map(b=>b.toLocaleString()),datasets:[{label:col,data:dist.counts,backgroundColor:'#38bdf820',borderColor:'#38bdf8',borderWidth:1.5,borderRadius:3}]},options:chartOpts()});
}
function etlRenderCat(){
  if(!etlData||!etlData.cat_counts)return;
  const col=document.getElementById('etl-cat-sel').value;
  const cc=etlData.cat_counts[col];if(!cc)return;
  if(etlCharts.cat)etlCharts.cat.destroy();
  const colors=['#00e5ff','#7c5cfc','#ff5c8a','#00e096','#ffd166','#f97316','#a78bfa','#60a5fa'];
  etlCharts.cat=new Chart(document.getElementById('etl-cat-chart'),{type:'bar',data:{labels:cc.labels,datasets:[{label:col,data:cc.values,backgroundColor:colors.map(c=>c+'30'),borderColor:colors,borderWidth:1.5,borderRadius:4}]},options:chartOpts()});
}
function etlRenderPreview(){
  if(!etlData)return;
  buildPreviewTable('etl-prev-wrap','etl-prev-info','etl-prev-badge',etlData.columns,etlData.preview||[],etlPage);
}
function etlPageChange(d){etlPage.page=Math.max(0,etlPage.page+d);etlRenderPreview();}

setupDrop('etl-dropzone','etl-file-input',etlUpload);

/* ══ BI REPORTER MODULE ══ */
let biData=null,biJobId=null,biPage={page:0},biCharts={};
const BI_STAGES=[[10,'Reading file...'],[30,'Cleaning data...'],[50,'Engineering features...'],[70,'Training ML model...'],[85,'Generating Excel report...'],[95,'Finalizing...']];

function biUpload(file){
  const timer=showLoading('bi',BI_STAGES);
  const fd=new FormData();fd.append('file',file);
  fetch('/bi/upload',{method:'POST',body:fd}).then(r=>r.json()).then(j=>{
    if(j.error){alert(j.error);showUpload('bi');clearInterval(timer);return;}
    biJobId=j.job_id;
    fetch(`/bi/results/${biJobId}`).then(r=>r.json()).then(d=>{
      biData=d;showDash('bi',timer);biRenderDash(d);
    });
  }).catch(e=>{alert(e.message);showUpload('bi');clearInterval(timer);});
}

function biRenderDash(d){
  document.getElementById('bi-subtitle').textContent=`${d.clean_shape[0].toLocaleString()} rows × ${d.clean_shape[1]} cols · Excel report ready`;
  const det=d.detected||{},st=d.stats||{};
  const kpis=[
    {label:'Records',val:d.clean_shape[0].toLocaleString(),sub:`raw: ${d.original_shape[0].toLocaleString()}`,color:'var(--accent)'},
    {label:'Columns',val:d.clean_shape[1],sub:'after engineering',color:'var(--accent2)'}
  ];
  if(det.revenue&&st[det.revenue]) kpis.push({label:'Total Revenue',val:fmt(st[det.revenue].sum),sub:`avg: ${fmt(st[det.revenue].mean)}`,color:'var(--green)'});
  if(st['Profit']) kpis.push({label:'Total Profit',val:fmt(st['Profit'].sum),sub:`avg: ${fmt(st['Profit'].mean)}`,color:'var(--green)'});
  if(d.ml&&d.ml.r2!==undefined) kpis.push({label:'ML R² Score',val:d.ml.r2,sub:'prediction accuracy',color:'var(--accent2)'});
  buildKPIs('bi-kpis',kpis);
  // Cat chart
  const cd=d.cat_data;
  if(cd&&cd.labels&&cd.labels.length){
    if(biCharts.cat)biCharts.cat.destroy();
    const colors=['#00e5ff','#7c5cfc','#ff5c8a','#00e096','#ffd166','#f97316','#a78bfa','#60a5fa'];
    biCharts.cat=new Chart(document.getElementById('bi-cat-chart'),{type:'bar',data:{labels:cd.labels,datasets:[{label:'Revenue',data:cd.values,backgroundColor:colors.map((c,i)=>colors[i%8]+'30'),borderColor:colors,borderWidth:1.5,borderRadius:4}]},options:chartOpts()});
  }
  buildLogList('bi-log',d.logs||[],'bi-log-badge');
  // ML
  const ml=d.ml;
  if(ml&&!ml.error&&ml.r2!==undefined){
    document.getElementById('bi-ml-badge').textContent='✓ '+(ml.model||'Random Forest');
    const grow=ml.growth_pct||0;
    const rows=[['R² Score',ml.r2,'var(--green)'],['MAE',fmt(ml.mae),'var(--yellow)'],['Current Avg',fmt(ml.current_avg),'var(--muted2)'],['Next Prediction',fmt(ml.next_prediction),'var(--accent)'],['Growth %',`${grow>0?'+':''}${grow}%`,grow>=0?'var(--green)':'var(--red)']];
    let html=rows.map(([l,v,c])=>`<div class="ml-row"><span class="ml-name">${l}</span><span class="ml-val" style="color:${c}">${v}</span></div>`).join('');
    const fi=ml.feature_importance||{};
    if(Object.keys(fi).length>0){const mx=Math.max(...Object.values(fi));html+=`<div style="margin-top:12px;font-family:var(--mono);font-size:9px;color:var(--muted);letter-spacing:.06em;margin-bottom:6px">FEATURE IMPORTANCE</div>`;html+=Object.entries(fi).map(([f,v])=>`<div class="fi-item"><div class="fi-lbl"><span>${f}</span><span style="color:var(--accent)">${v}%</span></div><div class="fi-bg"><div class="fi-fill" style="width:${v/mx*100}%"></div></div></div>`).join('');}
    document.getElementById('bi-ml-content').innerHTML=html;
  } else {
    document.getElementById('bi-ml-content').innerHTML='<p style="color:var(--muted);font-family:var(--mono);font-size:11px">Not enough data for ML</p>';
  }
  // Stats
  document.getElementById('bi-stats-body').innerHTML=Object.entries(st).map(([col,s])=>`<tr><td style="color:var(--text)">${col}</td><td>${fmt(s.min)}</td><td>${fmt(s.max)}</td><td style="color:var(--accent)">${fmt(s.mean)}</td><td style="color:var(--green)">${fmt(s.sum)}</td></tr>`).join('');
  // Preview
  biPage.page=0;biRenderPreview();
  // Download button
  document.getElementById('bi-dl-btn').onclick=()=>window.location=`/bi/download/${biJobId}`;
}
function biRenderPreview(){if(!biData)return;buildPreviewTable('bi-prev-wrap','bi-prev-info','bi-prev-badge',biData.columns,biData.preview||[],biPage);}
function biPageChange(d){biPage.page=Math.max(0,biPage.page+d);biRenderPreview();}

setupDrop('bi-dropzone','bi-file-input',biUpload);

/* ══ SQL ANALYTICS MODULE ══ */
let sqlData=null,sqlJobId=null,sqlPage={page:0},sqlCharts={},sqlView='overview';

const SQL_STAGES=[[8,'Reading file...'],[25,'Cleaning data...'],[45,'Loading into database...'],[65,'Running GROUP BY queries...'],[80,'Running CTE & Window queries...'],[95,'All queries executed...']];

function sqlUpload(file){
  const timer=showLoading('sql',SQL_STAGES);
  const fd=new FormData();fd.append('file',file);
  fetch('/sql/upload',{method:'POST',body:fd}).then(r=>r.json()).then(j=>{
    if(j.error){alert(j.error);showUpload('sql');clearInterval(timer);return;}
    sqlJobId=j.job_id;
    fetch(`/sql/results/${sqlJobId}`).then(r=>r.json()).then(d=>{
      sqlData=d;showDash('sql',timer);sqlRenderDash(d);
    });
  }).catch(e=>{alert(e.message);showUpload('sql');clearInterval(timer);});
}

function sqlRenderDash(d){
  document.getElementById('sql-subtitle').textContent=`${d.filename} · ${d.shape[0].toLocaleString()} rows · ${Object.keys(d.queries).length} queries · ${d.db_type}`;
  const kpis=[
    {label:'Rows',val:d.shape[0].toLocaleString(),sub:'loaded',color:'var(--accent)'},
    {label:'Columns',val:d.shape[1],sub:'in table',color:'var(--accent2)'},
    {label:'Queries',val:Object.keys(d.queries).length,sub:'auto-executed',color:'var(--green)'},
    {label:'Database',val:d.db_type.toUpperCase(),sub:d.table_name,color:'var(--yellow)'}
  ];
  buildKPIs('sql-kpis',kpis);
  buildLogList('sql-log',d.logs||[],'sql-log-badge');
  // Queries
  const qList=document.getElementById('sql-query-list');
  qList.innerHTML=Object.entries(d.queries).map(([key,q],i)=>{
    const ok=!q.error;
    return `<div class="qv-head" onclick="this.nextElementSibling.classList.toggle('open')">
      <div class="qv-status ${ok?'qv-ok':'qv-err'}"></div>
      <span class="qv-title">${q.title}</span>
      <span class="qv-rows">${ok?q.row_count+' rows':'error'}</span>
    </div>
    <div class="qv-body">
      <div class="sql-block">${q.sql}</div>
      ${ok&&q.rows.length?`<div class="tbl-wrap" style="max-height:180px"><table><thead><tr>${q.columns.map(c=>`<th>${c}</th>`).join('')}</tr></thead><tbody>${q.rows.slice(0,20).map(r=>`<tr>${q.columns.map(c=>`<td>${r[c]??''}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`:''}
    </div>`;
  }).join('');
  document.getElementById('sql-q-badge').textContent=Object.keys(d.queries).length+' queries';
  // Charts
  const ch=d.chart||{};
  if(ch.cat_revenue){
    if(sqlCharts.cat)sqlCharts.cat.destroy();
    const colors=['#00e5ff','#7c5cfc','#ff5c8a','#00e096','#ffd166','#f97316','#a78bfa','#60a5fa'];
    sqlCharts.cat=new Chart(document.getElementById('sql-cat-chart'),{type:'bar',data:{labels:ch.cat_revenue.labels,datasets:[{label:'Revenue',data:ch.cat_revenue.values,backgroundColor:colors.map(c=>c+'30'),borderColor:colors,borderWidth:1.5,borderRadius:4}]},options:chartOpts()});
  }
  if(ch.trend){
    if(sqlCharts.trend)sqlCharts.trend.destroy();
    sqlCharts.trend=new Chart(document.getElementById('sql-trend-chart'),{type:'line',data:{labels:ch.trend.labels,datasets:[{label:'Revenue',data:ch.trend.values,borderColor:'#00e5ff',backgroundColor:'#00e5ff15',fill:true,tension:.3,borderWidth:2,pointRadius:3}]},options:chartOpts()});
  }
  // Stats
  document.getElementById('sql-stats-body').innerHTML=Object.entries(d.stats||{}).map(([col,s])=>`<tr><td style="color:var(--text)">${col}</td><td>${fmt(s.min)}</td><td>${fmt(s.max)}</td><td style="color:var(--accent)">${fmt(s.mean)}</td><td style="color:var(--green)">${fmt(s.sum)}</td></tr>`).join('');
  // Editor
  document.getElementById('sql-editor-table').textContent='table: '+d.table_name;
  document.getElementById('sql-editor-area').value=`SELECT * FROM ${d.table_name} LIMIT 10;`;
  // Preview
  sqlPage.page=0;sqlRenderPreview();
}

function sqlShowView(v){
  sqlView=v;
  ['overview','queries','charts','editor','data'].forEach(n=>{
    const el=document.getElementById('sql-view-'+n);
    if(el)el.style.display=n===v?'block':'none';
  });
  document.querySelectorAll('#sql-nav .tab').forEach(t=>t.classList.remove('active'));
  event.target.classList.add('active');
}

function sqlRunCustom(){
  const sql=document.getElementById('sql-editor-area').value.trim();
  if(!sql)return;
  fetch(`/sql/query/${sqlJobId}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sql})}).then(r=>r.json()).then(d=>{
    if(d.error){document.getElementById('sql-custom-result').innerHTML=`<p style="color:var(--red);font-family:var(--mono);font-size:11px">${d.error}</p>`;return;}
    document.getElementById('sql-custom-result').innerHTML=`<p style="color:var(--green);font-family:var(--mono);font-size:10px;margin-bottom:8px">${d.row_count} rows returned</p><div class="tbl-wrap" style="max-height:200px"><table><thead><tr>${d.columns.map(c=>`<th>${c}</th>`).join('')}</tr></thead><tbody>${d.rows.slice(0,30).map(r=>`<tr>${d.columns.map(c=>`<td>${r[c]??''}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  });
}

function sqlExport(){if(sqlJobId)window.location=`/sql/export/${sqlJobId}`;}

function sqlRenderPreview(){if(!sqlData)return;buildPreviewTable('sql-prev-wrap','sql-prev-info','sql-prev-badge',sqlData.columns,sqlData.preview||[],sqlPage);}
function sqlPageChange(d){sqlPage.page=Math.max(0,sqlPage.page+d);sqlRenderPreview();}

setupDrop('sql-dropzone','sql-file-input',sqlUpload);

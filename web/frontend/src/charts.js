// ══════════════════════════════════════════════════════════════════════════════
//  charts.js  —  Heat Pulse
// ══════════════════════════════════════════════════════════════════════════════

const CHART_IDS = ['chart-boxplot','chart-qq','chart-timeseries','chart-decomp',
                   'chart-yoy','chart-heatmap','chart-cities','chart-scatter','chart-corrmatrix'];
const _instances = {};
function _destroy(id){ if(_instances[id]){_instances[id].destroy();delete _instances[id];} }
function _destroyAll(){ CHART_IDS.forEach(_destroy); }

// ── Season ────────────────────────────────────────────────────────────────
const SEASON_ORDER  = ['Spring','Summer','Autumn','Winter'];
const SEASON_LABELS = {Spring:'Yaz',Summer:'Yay',Autumn:'Payız',Winter:'Qış'};
const SEASON_COLORS = {Spring:'#66BB6A',Summer:'#FFA726',Autumn:'#EF5350',Winter:'#42A5F5'};
function getSeason(m){ return [3,4,5].includes(m)?'Spring':[6,7,8].includes(m)?'Summer':[9,10,11].includes(m)?'Autumn':'Winter'; }

function fiveNum(arr){
    if(!arr||!arr.length)return null;
    const s=[...arr].sort((a,b)=>a-b),n=s.length;
    const q=p=>{const pos=p*(n-1),lo=Math.floor(pos),hi=Math.ceil(pos);return lo===hi?s[lo]:s[lo]+(s[hi]-s[lo])*(pos-lo);};
    const q1=q(0.25),q3=q(0.75),iqr=q3-q1;
    return{min:s.find(v=>v>=q1-1.5*iqr)??s[0],q1,median:q(0.5),q3,max:[...s].reverse().find(v=>v<=q3+1.5*iqr)??s[n-1]};
}

function kruskalWallis(groups){
    const all=groups.flatMap(g=>g),n=all.length;
    const sorted=[...all].sort((a,b)=>a-b);
    const ranks=all.map(v=>{const t=sorted.reduce((a,sv,i)=>sv===v?[...a,i+1]:a,[]);return t.reduce((a,b)=>a+b,0)/t.length;});
    let off=0;
    const H=(12/(n*(n+1)))*groups.map(g=>{const rg=ranks.slice(off,off+g.length);off+=g.length;const rm=rg.reduce((a,b)=>a+b,0)/rg.length;return g.length*rm*rm;}).reduce((a,b)=>a+b,0)-3*(n+1);
    return{H,p:1-chi2CDF(H,groups.length-1)};
}
function chi2CDF(x,k){if(x<=0)return 0;const a=k/2,v=x/2;let s=1,t=1;for(let i=1;i<200;i++){t*=v/(a+i);s+=t;if(t<1e-10)break;}return Math.exp(-v+a*Math.log(v)-lnG(a+1))*s;}
function lnG(z){const g=7,c=[0.99999999999980993,676.5203681218851,-1259.1392167224028,771.32342877765313,-176.61502916214059,12.507343278686905,-0.13857109526572012,9.9843695780195716e-6,1.5056327351493116e-7];if(z<0.5)return Math.log(Math.PI/Math.sin(Math.PI*z))-lnG(1-z);z-=1;let x=c[0];for(let i=1;i<g+2;i++)x+=c[i]/(z+i);const t=z+g+0.5;return 0.5*Math.log(2*Math.PI)+(z+0.5)*Math.log(t)-t+Math.log(x);}

// ── Theme ─────────────────────────────────────────────────────────────────
const DARK={gridColor:'rgba(255,255,255,0.05)',tickColor:'#6b7280',titleColor:'#9ca3af',legendColor:'#9ca3af'};

function darkScales(xL='',yL=''){
    const ax=l=>({ticks:{color:DARK.tickColor,font:{size:9},maxRotation:45},grid:{color:DARK.gridColor},title:l?{display:true,text:l,color:DARK.titleColor,font:{size:9}}:{display:false}});
    return{x:ax(xL),y:ax(yL)};
}
function darkPlugins(title='',sub=''){
    return{
        legend:{labels:{color:DARK.legendColor,font:{size:9},boxWidth:10}},
        title:{display:!!title,text:sub?[title,sub]:title,color:DARK.titleColor,font:{size:10,weight:'bold'}},
        tooltip:{mode:'index',intersect:false}
    };
}
// Every Chart.js chart must have these two — fills the container div
function base(extra={}){return{responsive:true,maintainAspectRatio:false,...extra};}

// ══════════════════════════════════════════════════════════════════════════════
//  MASTER ENTRY
// ══════════════════════════════════════════════════════════════════════════════
function renderAllCharts(apiResult){
    _destroyAll();
    window.__lastApiResult=apiResult||{};
    const city=apiResult.city||'—';
    const fc=apiResult.forecast||[];
    const histFullRaw=apiResult.hist_full||[];
    const cutoff=new Date('2025-01-01');
    const histFull=histFullRaw.filter(r=>{
        const d=new Date(r.date);
        return !isNaN(d)&&d>=cutoff;
    });
    const histTemps=(histFull.map(r=>r.temp_max).filter(v=>v!=null).length>0)
        ? histFull.map(r=>r.temp_max).filter(v=>v!=null)
        : (apiResult.hist_temps||[]);

    _buildRightPanelShell(city);

    setTimeout(()=>{
        chart2_SeasonalBoxplot(city,histFull,histTemps);
        chart3_QQ(city,histTemps);
        chart4_TimeSeries(city,histFull);
        chart5_Decomposition(city,histFull);
        chart6_YearOverYear(city,histFull);
        chart7_CalendarHeatmap(city,histFull);
        chart8_AllCities(fc,city);
        chart9_ScatterMatrix(city,histFull);
        chart10_CorrMatrix(city,histFull);
    },50);
}

// ══════════════════════════════════════════════════════════════════════════════
//  SHELL — no tables, all charts full-width
// ══════════════════════════════════════════════════════════════════════════════
function _buildRightPanelShell(city){
    const panel=document.querySelector('#sidebar .flex-1');
    if(!panel)return;

    // Helper: standard chart card
    const box=(id,title,h=260)=>`
        <div class="bg-gray-900/40 p-6 rounded-3xl border border-gray-800 w-full">
            ${title?`<h3 class="text-xs font-bold uppercase tracking-widest text-gray-400 mb-4">${title}</h3>`:''}
            <div style="position:relative;width:100%;height:${h}px;">
                <canvas id="${id}"></canvas>
            </div>
        </div>`;

    panel.innerHTML=`
        <h2 class="text-2xl font-bold italic opacity-50 uppercase tracking-tighter mb-2">
            Predictive Analytics — ${city}
        </h2>

        <div class="bg-gray-900/40 p-6 rounded-3xl border border-gray-800 w-full">
            <h3 class="text-xs font-bold uppercase tracking-widest text-gray-400 mb-4">🌡️ 30-Day Weather Forecast</h3>
            <div class="table-scroll" id="forecast-table-container">
                <p class="text-gray-600 text-xs italic">Yüklənir…</p>
            </div>
        </div>

        <div class="bg-gray-900/40 p-6 rounded-3xl border border-gray-800 w-full">
            <h3 class="text-xs font-bold uppercase tracking-widest text-gray-400 mb-4">⚡ 30-Day Energy Forecast</h3>
            <div class="table-scroll" id="energy-table-container">
                <p class="text-gray-600 text-xs italic">Yüklənir…</p>
            </div>
        </div>

        ${box('tempChart','Distribution of Daily Max Temperature',220)}
        ${box('chart-boxplot','🌡️ Temperature Distribution — Last 4 Seasons',260)}
        ${box('chart-qq','📈 Q-Q Plot (Normality Test)',260)}
        ${box('chart-timeseries','📆 Full Time Series (2020–2026)',260)}
        ${box('chart-decomp','🔬 Seasonal Decomposition',340)}
        ${box('chart-yoy','📊 Year-over-Year Temperature',320)}

        <div class="bg-gray-900/40 p-6 rounded-3xl border border-gray-800 w-full">
            <h3 class="text-xs font-bold uppercase tracking-widest text-gray-400 mb-4">🗓 Calendar Heatmap</h3>
            <div style="overflow-x:auto;width:100%;"><canvas id="chart-heatmap"></canvas></div>
        </div>

        ${box('chart-cities','🏙 Daily Max Temperature by Year (7-day smooth)',320)}
        <div class="bg-gray-900/40 p-6 rounded-3xl border border-gray-800 w-full">
            <h3 class="text-xs font-bold uppercase tracking-widest text-gray-400 mb-4">🔵 Pairplot Analogue — Seasonal Scatter Matrix</h3>
            <div style="width:100%;display:flex;justify-content:center;">
                <canvas id="chart-scatter" style="display:block;margin:0 auto;"></canvas>
            </div>
        </div>

        <div class="bg-gray-900/40 p-6 rounded-3xl border border-gray-800 w-full">
            <h3 class="text-xs font-bold uppercase tracking-widest text-gray-400 mb-4">🔥 Correlation Matrix — Weather Features</h3>
            <div style="width:100%;display:flex;justify-content:center;">
                <canvas id="chart-corrmatrix" style="display:block;margin:0 auto;"></canvas>
            </div>
            <p id="corr-strong-pairs" class="text-[11px] text-gray-500 mt-3"></p>
        </div>
    `;
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 1 helper — called from app.js initCharts()
//  Fix: title color changed from '#4b5563' (black) to '#9ca3af' (light grey)
//  Fix: maintainAspectRatio:false added
// ══════════════════════════════════════════════════════════════════════════════
// (initCharts lives in app.js — we patch it by overriding chartTitleColor)
const CHART1_TITLE_COLOR = '#9ca3af';  // app.js reads this constant

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 2 — Seasonal Boxplot
// ══════════════════════════════════════════════════════════════════════════════
function chart2_SeasonalBoxplot(city,histFull,histTemps){
    const ctx=document.getElementById('chart-boxplot');
    if(!ctx)return;
    const bucket={};
    const seasonLabel=s=>({Spring:'Yaz',Summer:'Yay',Autumn:'Payız',Winter:'Qış'})[s]||s;
    const seasonColor=s=>({Spring:'#66BB6A',Summer:'#FFA726',Autumn:'#EF5350',Winter:'#42A5F5'})[s]||'#9ca3af';
    const seasonIdx=s=>SEASON_ORDER.indexOf(s);
    const getSeasonYear=(d,s)=>{
        const m=d.getMonth()+1;
        if(s==='Winter'&&(m===1||m===2))return d.getFullYear()-1;
        return d.getFullYear();
    };
    if(histFull&&histFull.length>0){
        const sorted=[...histFull].sort((a,b)=>new Date(a.date)-new Date(b.date));
        sorted.forEach(r=>{
            const d=new Date(r.date);
            if(isNaN(d)||r.temp_max==null)return;
            const s=getSeason(d.getMonth()+1);
            const sy=getSeasonYear(d,s);
            const key=`${sy}-${s}`;
            if(!bucket[key])bucket[key]={season:s,year:sy,vals:[]};
            bucket[key].vals.push(r.temp_max);
        });
    }
    const nonEmpty=Object.values(bucket)
        .filter(g=>g.vals.length>0)
        .sort((a,b)=>a.year===b.year?(seasonIdx(a.season)-seasonIdx(b.season)):(a.year-b.year));
    if(nonEmpty.length<1){_noData(ctx,'No seasonal data');return;}
    const display=nonEmpty.slice(-4);
    if(display.length<1){_noData(ctx,'No seasonal data');return;}

    const labels=display.map(g=>`${seasonLabel(g.season)} ${g.year}`);
    const stats=display.map(g=>fiveNum(g.vals));
    const validGroups=display.map(g=>g.vals).filter(g=>g.length>0);
    const kw=validGroups.length>=2?kruskalWallis(validGroups):{p:NaN};
    const allVals=validGroups.flat();
    const yTop=allVals.length?Math.max(...allVals)+1:35;

    _destroy('chart-boxplot');
    _instances['chart-boxplot']=new Chart(ctx,{
        type:'bar',
        data:{
            labels:[`${city} — KW p=${isNaN(kw.p)?'N/A':(kw.p<0.001?'<0.001':kw.p.toFixed(3))}${(!isNaN(kw.p)&&kw.p<0.05)?' ✓sig':''}`],
            datasets:display.map((g,i)=>({
                label:`${labels[i]} (n=${g.vals.length})`,
                data:[stats[i]?[stats[i].q1,stats[i].q3]:[0,0]],
                backgroundColor:seasonColor(g.season)+'aa',borderColor:seasonColor(g.season),
                borderWidth:1.5,borderRadius:3,barPercentage:0.55,
            }))
        },
        options:base({
            animation:{duration:500},
            plugins:{
                ...darkPlugins('Temperature Distribution — Last 4 Seasons','(latest seasons with available data)'),
                tooltip:{callbacks:{label:c=>{const st=stats[c.datasetIndex];return st?`${labels[c.datasetIndex]}: Q1=${st.q1.toFixed(1)}° Med=${st.median.toFixed(1)}° Q3=${st.q3.toFixed(1)}°`:'';}}},
            },
            scales:{...darkScales('','Max Temp (°C)'),x:{...darkScales().x,stacked:false},y:{...darkScales('','Max Temp (°C)').y,min:0,max:yTop}},
        }),
        plugins:[{id:'bw',afterDraw(chart){
            const{ctx:c,scales:{x,y}}=chart;
            display.forEach((g,si)=>{
                const st=stats[si];if(!st)return;
                const meta=chart.getDatasetMeta(si);if(!meta.data[0])return;
                const bar=meta.data[0],bx=bar.x,hw=bar.width/2,col=seasonColor(g.season);
                c.save();c.strokeStyle=col;c.lineWidth=1.5;
                [[st.q3,st.max],[st.q1,st.min]].forEach(([fr,to])=>{
                    c.beginPath();c.moveTo(bx,y.getPixelForValue(fr));c.lineTo(bx,y.getPixelForValue(to));c.stroke();
                    c.beginPath();c.moveTo(bx-hw*0.4,y.getPixelForValue(to));c.lineTo(bx+hw*0.4,y.getPixelForValue(to));c.stroke();
                });
                c.strokeStyle='#fff';c.lineWidth=2;
                c.beginPath();c.moveTo(bx-hw,y.getPixelForValue(st.median));c.lineTo(bx+hw,y.getPixelForValue(st.median));c.stroke();
                c.restore();
            });
        }}]
    });
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 3 — Q-Q Plot
// ══════════════════════════════════════════════════════════════════════════════
function chart3_QQ(city,histTemps){
    const ctx=document.getElementById('chart-qq');if(!ctx)return;
    const vals=(histTemps||[]).filter(v=>v!=null).sort((a,b)=>a-b);
    if(vals.length<5){_noData(ctx,'Not enough data');return;}
    const n=vals.length,mean=vals.reduce((a,b)=>a+b,0)/n;
    const std=Math.sqrt(vals.reduce((s,v)=>s+(v-mean)**2,0)/n);
    const probit=p=>{
        const a=[0,-3.969683028665376e+01,2.209460984245205e+02,-2.759285104469687e+02,1.383577518672690e+02,-3.066479806614716e+01,2.506628277459239e+00];
        const b=[-5.447609879822406e+01,1.615858368580409e+02,-1.556989798598866e+02,6.680131188771972e+01,-1.328068155288572e+01];
        const c=[-7.784894002430293e-03,-3.223964580411365e-01,-2.400758277161838e+00,-2.549732539343734e+00,4.374664141464968e+00,2.938163982698783e+00];
        const d=[7.784695709041462e-03,3.224671290700398e-01,2.445134137142996e+00,3.754408661907416e+00];
        const pL=0.02425,pH=1-pL;
        if(p<pL){const q=Math.sqrt(-2*Math.log(p));return(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/(((d[0]*q+d[1])*q+d[2])*q+d[3]);}
        if(p<=pH){const q=p-0.5,r=q*q;return(((((a[1]*r+a[2])*r+a[3])*r+a[4])*r+a[5])*r+a[6])*q/(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1);}
        const q=Math.sqrt(-2*Math.log(1-p));return-(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/(((d[0]*q+d[1])*q+d[2])*q+d[3]);
    };
    const theor=vals.map((_,i)=>probit((i+0.5)/n));
    const xM=theor.reduce((a,b)=>a+b,0)/n;
    const slope=theor.reduce((s,t,i)=>s+(t-xM)*(vals[i]-mean),0)/theor.reduce((s,t)=>s+(t-xM)**2,0);
    const int2=mean-slope*xM;
    const r2=1-vals.reduce((s,v,i)=>s+(v-(slope*theor[i]+int2))**2,0)/vals.reduce((s,v)=>s+(v-mean)**2,0);
    const step=Math.max(1,Math.floor(n/300));
    const pts=theor.filter((_,i)=>i%step===0).map((t,j)=>({x:t,y:vals[j*step]}));
    _destroy('chart-qq');
    _instances['chart-qq']=new Chart(ctx,{
        type:'scatter',
        data:{datasets:[
            {label:'Sample Quantiles',data:pts,backgroundColor:'rgba(21,101,192,0.35)',pointRadius:2,order:2},
            {label:`Reference (R²=${r2.toFixed(4)})`,data:[{x:theor[0],y:slope*theor[0]+int2},{x:theor[n-1],y:slope*theor[n-1]+int2}],type:'line',borderColor:'#ef4444',borderWidth:2,pointRadius:0,fill:false,order:1}
        ]},
        options:base({animation:{duration:400},plugins:darkPlugins(`Q-Q Plot — ${city}`,`R²=${r2.toFixed(4)}  |  ${r2>0.98?'✓ Normal':'✗ Non-normal'}`),scales:darkScales('Theoretical Quantiles','Sample Quantiles')})
    });
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 4 — Full Time Series
// ══════════════════════════════════════════════════════════════════════════════
function chart4_TimeSeries(city,histFull){
    const ctx=document.getElementById('chart-timeseries');if(!ctx)return;
    if(!histFull||histFull.length<10){_noData(ctx,'No historical data');return;}
    const s=[...histFull].sort((a,b)=>new Date(a.date)-new Date(b.date));
    const labels=s.map(r=>r.date),maxT=s.map(r=>r.temp_max??null),minT=s.map(r=>r.temp_min??null);
    const roll=maxT.map((_,i,a)=>{const ch=a.slice(Math.max(0,i-15),Math.min(a.length,i+16)).filter(v=>v!=null);return ch.length?ch.reduce((a,b)=>a+b,0)/ch.length:null;});
    _destroy('chart-timeseries');
    _instances['chart-timeseries']=new Chart(ctx,{
        type:'line',
        data:{labels,datasets:[
            {label:'Min Temp',data:minT,borderColor:'rgba(66,165,245,0)',backgroundColor:'rgba(66,165,245,0.15)',fill:'+1',pointRadius:0,tension:0.3,order:4},
            {label:'Max Temp',data:maxT,borderColor:'rgba(229,57,53,0.25)',borderWidth:0.5,backgroundColor:'transparent',pointRadius:0,fill:false,tension:0.3,order:3},
            {label:'30-day avg',data:roll,borderColor:'#B71C1C',borderWidth:2.2,backgroundColor:'transparent',pointRadius:0,fill:false,tension:0.4,order:2},
            {label:'35°C',data:labels.map(()=>35),borderColor:'rgba(255,0,0,0.45)',borderWidth:1.2,borderDash:[4,4],pointRadius:0,fill:false,order:1},
            {label:'0°C',data:labels.map(()=>0),borderColor:'rgba(66,165,245,0.35)',borderWidth:1,borderDash:[3,3],pointRadius:0,fill:false,order:1},
        ]},
        options:base({animation:{duration:0},elements:{point:{radius:0}},plugins:darkPlugins(`Full Time Series — ${city}`,'Light=daily · Dark=30-day avg'),scales:darkScales('','°C')})
    });
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 5 — Seasonal Decomposition
//  FIX: min data reduced to 14 (not 365) so 30-day fallback also works
// ══════════════════════════════════════════════════════════════════════════════
function chart5_Decomposition(city,histFull){
    const ctx=document.getElementById('chart-decomp');if(!ctx)return;
    if(!histFull||histFull.length<14){_noData(ctx,'Need ≥14 days of data');return;}

    const s=[...histFull].sort((a,b)=>new Date(a.date)-new Date(b.date));
    const labels=s.map(r=>r.date);
    const obs=s.map(r=>r.temp_mean??r.temp_max??null);

    // Period: use 365 if enough data, else half the series length (min 7)
    const P=Math.min(365,Math.max(7,Math.floor(obs.length/2)));
    const half=Math.floor(P/2);

    const trend=obs.map((_,i)=>{
        if(i<half||i>=obs.length-half)return null;
        const ch=obs.slice(i-half,i+half+1).filter(v=>v!=null);
        return ch.length?ch.reduce((a,b)=>a+b,0)/ch.length:null;
    });

    const detr=obs.map((v,i)=>v!=null&&trend[i]!=null?v-trend[i]:null);
    const byDoy={};
    detr.forEach((v,i)=>{if(v==null)return;const doy=_doy(labels[i]);if(!byDoy[doy])byDoy[doy]=[];byDoy[doy].push(v);});
    const seasonal=labels.map(l=>{const a=byDoy[_doy(l)];return a?a.reduce((x,y)=>x+y,0)/a.length:null;});
    const resid=obs.map((v,i)=>v!=null&&trend[i]!=null&&seasonal[i]!=null?v-trend[i]-seasonal[i]:null);

    _destroy('chart-decomp');
    _instances['chart-decomp']=new Chart(ctx,{
        type:'line',
        data:{labels,datasets:[
            {label:'Observed',data:obs,borderColor:'#42A5F5',borderWidth:1,pointRadius:0,fill:false,tension:0.3},
            {label:'Trend',data:trend,borderColor:'#ef4444',borderWidth:1.5,pointRadius:0,fill:false,tension:0.4},
            {label:'Seasonal',data:seasonal,borderColor:'#66BB6A',borderWidth:1,pointRadius:0,fill:false,tension:0.3},
            {label:'Residual',data:resid,borderColor:'#9ca3af',borderWidth:0.8,pointRadius:0.5,fill:false},
        ]},
        options:base({animation:{duration:0},plugins:darkPlugins(`Seasonal Decomposition — ${city}`,`Additive model · period=${P}`),scales:darkScales('','°C')})
    });
}

function _doy(ds){const d=new Date(ds);return Math.floor((d-new Date(d.getFullYear(),0,0))/86400000);}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 6 — Year-over-Year
// ══════════════════════════════════════════════════════════════════════════════
function chart6_YearOverYear(city,histFull){
    const ctx=document.getElementById('chart-yoy');if(!ctx)return;
    if(!histFull||histFull.length<10){_noData(ctx,'No historical data');return;}
    const byY={};
    histFull.forEach(r=>{const d=new Date(r.date);if(isNaN(d))return;const yr=d.getFullYear(),doy=_doy(r.date);if(!byY[yr])byY[yr]={};byY[yr][doy]=r.temp_mean??r.temp_max??null;});
    const years=Object.keys(byY).sort();
    const pal=['#EF5350','#FFA726','#FFEE58','#66BB6A','#42A5F5','#AB47BC','#26C6DA'];
    const doys=Array.from({length:365},(_,i)=>i+1);
    const mTicks=[1,32,60,91,121,152,182,213,244,274,305,335];
    const mNames=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const yVals=years.flatMap(yr=>doys.map(d=>byY[yr][d]).filter(v=>v!=null));
    const yMin=yVals.length?Math.min(...yVals):0;
    const yMax=yVals.length?Math.max(...yVals):40;
    _destroy('chart-yoy');
    _instances['chart-yoy']=new Chart(ctx,{
        type:'line',
        data:{labels:doys,datasets:years.map((yr,i)=>({label:yr,data:doys.map(d=>byY[yr][d]??null),borderColor:pal[i%pal.length],borderWidth:1.5,pointRadius:0,tension:0.3,fill:false}))},
        options:base({
            animation:{duration:0},
            plugins:darkPlugins(`Year-over-Year — ${city}`,'Mean temperature by day of year'),
            scales:{
                x:{ticks:{color:DARK.tickColor,font:{size:8},callback:val=>{const i=mTicks.indexOf(val+1);return i>=0?mNames[i]:'';},maxRotation:0},grid:{color:DARK.gridColor},title:{display:true,text:'Day of Year',color:DARK.titleColor,font:{size:9}}},
                y:{min:yMin-2,max:yMax+2,ticks:{color:DARK.tickColor,font:{size:9}},grid:{color:DARK.gridColor},title:{display:true,text:'Mean Temp (°C)',color:DARK.titleColor,font:{size:9}}}
            }
        })
    });
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 7 — Calendar Heatmap (raw canvas)
// ══════════════════════════════════════════════════════════════════════════════
function chart7_CalendarHeatmap(city,histFull){
    const cvs=document.getElementById('chart-heatmap');if(!cvs)return;
    if(!histFull||histFull.length<14){_noData(cvs,'No data');return;}
    const pivot={};
    histFull.forEach(r=>{
        const d=new Date(r.date);
        if(isNaN(d))return;
        const yr=d.getFullYear(),doy=_doy(r.date);
        if(!pivot[yr])pivot[yr]={};
        pivot[yr][doy]=r.temp_mean??null;
    });
    const years=Object.keys(pivot).sort();
    const CW=2,CH=18,PL=44,PT=20,PR=10,PB=34;
    const DAYS=366;
    const W=PL+DAYS*CW+PR,H=PT+years.length*CH+PB;
    cvs.width=W;cvs.height=H;cvs.style.width=W+'px';
    const c=cvs.getContext('2d');c.clearRect(0,0,W,H);
    const all=histFull.map(r=>r.temp_mean).filter(v=>v!=null&&isFinite(v));
    const vMin=Math.min(...all),vMax=Math.max(...all);
    const lerp=(t,a,b)=>a.map((v,i)=>v+(b[i]-v)*t);
    const rdYlBu=t=>{let r;if(t<0.25)r=lerp(t/0.25,[49,130,189],[116,173,209]);else if(t<0.5)r=lerp((t-0.25)/0.25,[116,173,209],[255,255,191]);else if(t<0.75)r=lerp((t-0.5)/0.25,[255,255,191],[253,174,97]);else r=lerp((t-0.75)/0.25,[253,174,97],[215,48,39]);return`rgb(${r.map(Math.round).join(',')})`};
    years.forEach((yr,yi)=>{
        c.fillStyle='#6b7280';c.font='9px monospace';c.textAlign='right';c.fillText(yr,PL-4,PT+yi*CH+CH*0.75);
        for(let doy=1;doy<=DAYS;doy++){const val=(pivot[yr]||{})[doy];c.fillStyle=val==null?'#1f2937':rdYlBu((val-vMin)/(vMax-vMin||1));c.fillRect(PL+(doy-1)*CW,PT+yi*CH,CW-0.5,CH-0.5);}
    });
    const mT=[1,32,60,91,121,152,182,213,244,274,305,335],mN=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    c.fillStyle='#6b7280';c.font='8px monospace';c.textAlign='left';
    mT.forEach((doy,i)=>c.fillText(mN[i],PL+(doy-1)*CW,H-6));
    const bY=H-18,bX=PL,bW=DAYS*CW;
    for(let px=0;px<bW;px++){c.fillStyle=rdYlBu(px/bW);c.fillRect(bX+px,bY,1,6);}
    c.fillStyle='#6b7280';c.font='7px monospace';
    c.textAlign='left';c.fillText(`${vMin.toFixed(0)}°`,bX,bY-2);
    c.textAlign='right';c.fillText(`${vMax.toFixed(0)}°`,bX+bW,bY-2);
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 8 — Daily Max by year (7-day smoothed)
// ══════════════════════════════════════════════════════════════════════════════
function chart8_AllCities(fc,city){
    const ctx=document.getElementById('chart-cities');if(!ctx)return;
    // Uses city historical context; if unavailable, falls back to forecast window.
    const src=(window.__lastApiResult?.hist_full||[]).filter(r=>r.temp_max!=null&&r.date);
    if(src.length<10){
        if(!fc||fc.length<5){_noData(ctx,'No forecast/historical data');return;}
        const labels=fc.map(d=>d.date?d.date.slice(5):'');
        const maxT=fc.map(d=>d.temp_max??null);
        const smooth=maxT.map((_,i,a)=>{const ch=a.slice(Math.max(0,i-3),Math.min(a.length,i+4)).filter(v=>v!=null);return ch.length?ch.reduce((a,b)=>a+b,0)/ch.length:null;});
        _destroy('chart-cities');
        _instances['chart-cities']=new Chart(ctx,{
            type:'line',
            data:{labels,datasets:[
                {label:`${city} (raw)`,data:maxT,borderColor:'rgba(239,83,80,0.25)',borderWidth:0.9,pointRadius:0,fill:false},
                {label:`${city} (7-day)`,data:smooth,borderColor:'#EF5350',borderWidth:2.1,pointRadius:0,fill:false,tension:0.35},
                {label:'35°C',data:labels.map(()=>35),borderColor:'rgba(255,0,0,0.45)',borderDash:[5,5],borderWidth:1.2,pointRadius:0,fill:false},
            ]},
            options:base({animation:{duration:350},plugins:darkPlugins(`Daily Max — ${city}`,'Fallback: 30-day forecast window'),scales:darkScales('','Max Temp (°C)')})
        });
        return;
    }
    const byYear={};
    src.forEach(r=>{
        const d=new Date(r.date);
        if(isNaN(d))return;
        const year=d.getFullYear();
        const doy=_doy(r.date);
        if(!byYear[year])byYear[year]={};
        byYear[year][doy]=r.temp_max;
    });
    const years=Object.keys(byYear).sort();
    const doys=Array.from({length:365},(_,i)=>i+1);
    const yVals=years.flatMap(yr=>doys.map(d=>byYear[yr][d]).filter(v=>v!=null));
    const yMin=yVals.length?Math.min(...yVals):0;
    const yMax=yVals.length?Math.max(...yVals):40;
    const pal=['#EF5350','#FFA726','#FDD835','#66BB6A','#42A5F5','#AB47BC','#26C6DA','#EC407A'];
    const smoothed=(arr,win=7)=>arr.map((_,i,a)=>{
        const h=Math.floor(win/2);
        const ch=a.slice(Math.max(0,i-h),Math.min(a.length,i+h+1)).filter(v=>v!=null);
        return ch.length?ch.reduce((x,y)=>x+y,0)/ch.length:null;
    });
    const mTicks=[1,32,60,91,121,152,182,213,244,274,305,335];
    const mNames=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    _destroy('chart-cities');
    _instances['chart-cities']=new Chart(ctx,{
        type:'line',
        data:{labels:doys,datasets:[
            ...years.map((yr,i)=>({
                label:String(yr),
                data:smoothed(doys.map(d=>byYear[yr][d]??null),7),
                borderColor:pal[i%pal.length],
                borderWidth:2,
                pointRadius:0,
                fill:false,
                tension:0.3,
                spanGaps:true,
            })),
            {label:'35°C threshold',data:doys.map(()=>35),borderColor:'rgba(255,0,0,0.45)',borderDash:[4,4],borderWidth:1.4,pointRadius:0,fill:false},
        ]},
        options:base({
            animation:{duration:0},
            plugins:darkPlugins(`All Years — ${city}`,'Daily max temperature · 7-day smoothed'),
            scales:{
                x:{ticks:{color:DARK.tickColor,font:{size:8},callback:val=>{const i=mTicks.indexOf(val+1);return i>=0?mNames[i]:'';},maxRotation:0},grid:{color:DARK.gridColor}},
                y:{min:yMin-2,max:yMax+2,ticks:{color:DARK.tickColor,font:{size:9}},grid:{color:DARK.gridColor},title:{display:true,text:'Max Temp (°C)',color:DARK.titleColor,font:{size:9}}}
            }
        })
    });
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 9 — Pairplot analogue (raw canvas)
// ══════════════════════════════════════════════════════════════════════════════
function chart9_ScatterMatrix(city,histFull){
    const cvs=document.getElementById('chart-scatter');if(!cvs)return;
    if(!histFull||histFull.length<20){_noData(cvs,'No historical data');return;}
    const vars=[
        {k:'temp_max',l:'T max'},
        {k:'temp_min',l:'T min'},
        {k:'humidity',l:'Humidity'},
        {k:'wind',l:'Wind'},
    ].filter(v=>histFull.some(r=>r[v.k]!=null));
    if(vars.length<2){_noData(cvs,'Need at least two variables');return;}
    const rows=histFull.filter(r=>vars.every(v=>r[v.k]!=null&&isFinite(r[v.k]))&&r.date);
    if(rows.length<20){_noData(cvs,'Not enough complete rows');return;}
    const maxSample=2000;
    const step=Math.max(1,Math.floor(rows.length/maxSample));
    const sample=rows.filter((_,i)=>i%step===0);

    const n=vars.length;
    const parentW=Math.min(700,Math.max(500,(cvs.parentElement?.clientWidth||700)-40));
    const PL=52,PT=16,GAP=10;
    const CELL=Math.max(74,Math.floor((parentW-PL-(n-1)*GAP-14)/n));
    const W=PL+n*(CELL+GAP)-GAP+14;
    const H=PT+n*(CELL+GAP)-GAP+34;
    cvs.width=W;cvs.height=H;cvs.style.width='min(100%,'+W+'px)';
    cvs.style.height='auto';
    const c=cvs.getContext('2d');
    c.fillStyle='#0b0f19';c.fillRect(0,0,W,H);

    const mm={};
    vars.forEach(v=>{
        const arr=sample.map(r=>r[v.k]).filter(x=>x!=null);
        mm[v.k]={min:Math.min(...arr),max:Math.max(...arr)};
    });
    const norm=(v,mn,mx)=>mx===mn?0.5:(v-mn)/(mx-mn);

    for(let ri=0;ri<n;ri++){
        for(let ci=0;ci<n;ci++){
            const x0=PL+ci*(CELL+GAP);
            const y0=PT+ri*(CELL+GAP);
            c.strokeStyle='rgba(156,163,175,0.2)';
            c.strokeRect(x0,y0,CELL,CELL);
            if(ri===ci){
                const arr=sample.map(r=>r[vars[ri].k]).filter(v=>v!=null);
                const bins=24,counts=Array(bins).fill(0);
                const mn=mm[vars[ri].k].min,mx=mm[vars[ri].k].max;
                arr.forEach(v=>{let bi=Math.floor(norm(v,mn,mx)*bins);if(bi>=bins)bi=bins-1;if(bi<0)bi=0;counts[bi]++;});
                const peak=Math.max(...counts,1);
                counts.forEach((ct,bi)=>{
                    const bw=CELL/bins,bh=(ct/peak)*(CELL-10);
                    c.fillStyle='rgba(66,165,245,0.7)';
                    c.fillRect(x0+bi*bw+0.5,y0+CELL-bh-1,bw-1,bh);
                });
                continue;
            }
            sample.forEach(r=>{
                const season=getSeason(new Date(r.date).getMonth()+1);
                c.fillStyle=SEASON_COLORS[season]+'66';
                const xv=norm(r[vars[ci].k],mm[vars[ci].k].min,mm[vars[ci].k].max);
                const yv=norm(r[vars[ri].k],mm[vars[ri].k].min,mm[vars[ri].k].max);
                c.beginPath();
                c.arc(x0+2+xv*(CELL-4),y0+CELL-2-yv*(CELL-4),1.7,0,Math.PI*2);
                c.fill();
            });
        }
        c.fillStyle='#9ca3af';c.font='10px monospace';c.textAlign='right';c.textBaseline='middle';
        c.fillText(vars[ri].l,PL-5,PT+ri*(CELL+GAP)+CELL/2);
        c.fillStyle='#9ca3af';c.textAlign='center';c.textBaseline='top';
        c.fillText(vars[ri].l,PL+ri*(CELL+GAP)+CELL/2,H-14);
    }
}

// ══════════════════════════════════════════════════════════════════════════════
//  CHART 10 — Correlation Matrix (raw canvas)
// ══════════════════════════════════════════════════════════════════════════════
function chart10_CorrMatrix(city,histFull){
    const cvs=document.getElementById('chart-corrmatrix');if(!cvs)return;
    const strongPairsEl=document.getElementById('corr-strong-pairs');
    if(strongPairsEl)strongPairsEl.textContent='';
    if(!histFull||histFull.length<10){_noData(cvs,'No data');return;}
    const VARS={temp_max:'T max',temp_min:'T min',temp_mean:'T mean',humidity:'Humidity',wind:'Wind',solar:'Solar'};
    const keys=Object.keys(VARS).filter(k=>histFull.some(r=>r[k]!=null));
    const lbls=keys.map(k=>VARS[k]),n=keys.length;
    if(n<2){_noData(cvs,'Need ≥2 numeric variables');return;}
    const pearson=(a,b)=>{const p=a.map((v,i)=>[v,b[i]]).filter(([x,y])=>x!=null&&y!=null);if(p.length<3)return NaN;const n2=p.length,mx=p.reduce((s,[x])=>s+x,0)/n2,my=p.reduce((s,[,y])=>s+y,0)/n2;const num=p.reduce((s,[x,y])=>s+(x-mx)*(y-my),0),dx=Math.sqrt(p.reduce((s,[x])=>s+(x-mx)**2,0)),dy=Math.sqrt(p.reduce((s,[,y])=>s+(y-my)**2,0));return dx*dy?num/(dx*dy):NaN;};
    const mat=keys.map(ki=>keys.map(kj=>pearson(histFull.map(r=>r[ki]),histFull.map(r=>r[kj]))));
    const parentW=Math.min(700,Math.max(500,(cvs.parentElement?.clientWidth||700)-40));
    const PL=70,PT=10;
    const CELL=Math.max(40,Math.floor((parentW-PL-10)/n));
    cvs.width=PL+n*CELL+10;cvs.height=PT+n*CELL+30;cvs.style.width='min(100%,'+cvs.width+'px)';
    cvs.style.height='auto';
    const c=cvs.getContext('2d');c.fillStyle='#0b0f19';c.fillRect(0,0,cvs.width,cvs.height);
    const cw=t=>t<0.5?`rgb(${Math.round(59+(t/0.5)*196)},${Math.round(76+(t/0.5)*179)},${Math.round(192+(t/0.5)*63)})`:((f=>`rgb(255,${Math.round(255-f*211)},${Math.round(255-f*255)})`)((t-0.5)/0.5));
    for(let ri=0;ri<n;ri++){
        for(let ci=0;ci<n;ci++){
            const r=mat[ri][ci],t=isNaN(r)?0.5:(r+1)/2;
            const x=PL+ci*CELL,y=PT+ri*CELL;
            c.fillStyle=cw(t);c.fillRect(x,y,CELL-1,CELL-1);
            c.fillStyle=Math.abs(r??0)>0.5?'#000':'#ccc';c.font='bold 11px monospace';c.textAlign='center';c.textBaseline='middle';
            c.fillText(isNaN(r)?'—':r.toFixed(2),x+CELL/2,y+CELL/2);
        }
        c.fillStyle='#9ca3af';c.font='10px monospace';c.textAlign='right';c.textBaseline='middle';c.fillText(lbls[ri],PL-4,PT+ri*CELL+CELL/2);
        c.save();c.translate(PL+ri*CELL+CELL/2,PT+n*CELL+14);c.rotate(-Math.PI/4);c.textAlign='right';c.font='10px monospace';c.fillStyle='#9ca3af';c.fillText(lbls[ri],0,0);c.restore();
    }
    if(strongPairsEl){
        const pairs=[];
        for(let i=0;i<n;i++){
            for(let j=i+1;j<n;j++){
                const r=mat[i][j];
                if(!isNaN(r)&&Math.abs(r)>0.7){
                    pairs.push({a:lbls[i],b:lbls[j],r});
                }
            }
        }
        pairs.sort((a,b)=>Math.abs(b.r)-Math.abs(a.r));
        strongPairsEl.textContent=pairs.length
            ? `Strong pairs (${city}, |r|>0.70): `+pairs.slice(0,4).map(p=>`${p.a}–${p.b}=${p.r.toFixed(2)}`).join(' | ')
            : `Strong pairs (${city}, |r|>0.70): none`;
    }
}

function _noData(canvas,msg){
    const c=canvas.getContext('2d');c.clearRect(0,0,canvas.width||400,canvas.height||100);
    c.fillStyle='#4b5563';c.font='11px monospace';c.textAlign='center';c.textBaseline='middle';
    c.fillText(msg,(canvas.width||400)/2,(canvas.height||100)/2);
}
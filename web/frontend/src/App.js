// ── 1. Qlobal Dəyişənlər ──────────────────────────────────────────────────
let globalData   = [];
let geoLayer     = null;
let colorfulMode = false;
let tempChartInst   = null;
let energyChartInst = null;

// ── Xəritəni başlat ───────────────────────────────────────────────────────
const map = L.map('map', { zoomControl: false }).setView([40.4093, 49.8671], 7);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap', maxZoom: 19
}).addTo(map);

const smallIcon = L.icon({
    iconUrl:    'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
    iconSize:   [15, 25], iconAnchor: [7, 25], popupAnchor: [1, -20],
    shadowUrl:  'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
    shadowSize: [25, 25]
});

// ── DOM Elementləri ───────────────────────────────────────────────────────
const sidebar     = document.getElementById('sidebar');
const overlay     = document.getElementById('overlay');
const closeBtn    = document.getElementById('close-btn');
const searchInput = document.getElementById('map-search');
const modeBtn     = document.getElementById('toggle-mode');

// ── 2. Backend-dən şəhər listini çək ─────────────────────────────────────
async function fetchWeatherData() {
    try {
        const res    = await fetch('http://127.0.0.1:8000/api/weather');
        const result = await res.json();
        if (result.status === "success") {
            globalData = result.data;
            displayCities(globalData);
        }
    } catch (e) { console.error("Backend xətası:", e); }
}

// ── 3. Marker-ləri xəritəyə əlavə et ─────────────────────────────────────
function displayCities(cities) {
    cities.forEach(city => {
        const marker = L.marker([city.lat, city.lon], { icon: smallIcon }).addTo(map);
        marker.on('click', () => {
            openDetails(city);
            map.flyTo([city.lat, city.lon], 10, { duration: 1.5 });
        });
    });
}

// ── 4. Sidebar-ı aç və datanı doldur ─────────────────────────────────────
async function openDetails(city) {
    sidebar.classList.remove('-translate-x-full');
    overlay.classList.remove('hidden');
    setLoadingState();

    try {
        const res    = await fetch(`http://127.0.0.1:8000/api/details/${city.city}`);
        const result = await res.json();
        if (result.status !== "success") { console.error("Backend Error:", result.message); return; }

        const w  = result.weather;
        const e  = result.energy;
        const m  = result.accuracy_metrics;
        const fc = result.forecast;
        const ef = result.energy_forecast;
        const ht = result.hist_temps || [];

        // Sol panel
        renderWeatherSection(result.city, w);
        renderImpactSection(w);
        renderMetricsSection(m, e);

        // Sağ panel — sidebar transition 500ms, ona görə 520ms gözləyirik
        setTimeout(() => {
            // 1. Shell + bütün chartlar (tempChart daxil)
            renderAllCharts(result);
            // 2. tempChart (distribusiya histoqramı) — shell yarandıqdan sonra
            initCharts(fc, ef, ht);
            // 3. Cədvəllər — shell-dəki ID-lərə yazılır
            renderForecastTable(fc);
            renderEnergyTable(ef);
        }, 520);

    } catch (err) { console.error("Connection Error:", err); }
}

// ── Sol panel render funksiyaları ─────────────────────────────────────────
function renderWeatherSection(cityName, w) {
    const icon = getWeatherIcon(w.condition);
    document.getElementById('weather-section').innerHTML = `
        <h1 class="text-5xl font-black italic uppercase tracking-tighter text-white">${cityName}</h1>
        <div class="flex items-center gap-4 mt-5">
            <div class="flex-shrink-0 drop-shadow-[0_0_12px_rgba(255,255,255,0.15)]">${icon}</div>
            <div>
                <div class="flex items-end gap-2">
                    <span class="text-6xl font-light text-white leading-none">${w.temp_max ?? '--'}°</span>
                </div>
                <span class="text-gray-400 uppercase text-xs tracking-widest mt-1 block">${w.condition ?? ''}</span>
            </div>
        </div>
        <p class="text-gray-500 text-sm italic mt-4">Humidity: ${w.humidity ?? '--'}% &nbsp;|&nbsp; Min: ${w.temp_min ?? '--'}°C</p>
        <p class="text-gray-600 text-xs mt-1">${w.date ?? ''}</p>
    `;
}

function renderImpactSection(w) {
    const alertText  = (w.alert > 0) ? '⚠ Persistence Alert' : 'System Stable';
    const alertColor = (w.alert > 0) ? 'text-red-500' : 'text-green-500';
    document.getElementById('impact-section').innerHTML = `
        <div class="flex flex-col items-center">
            <div class="text-5xl font-black italic text-white">${w.impact ?? '--'}<span class="text-sm">/100</span></div>
            <p class="mt-2 text-[10px] text-red-500 font-bold uppercase tracking-widest">Impact Factor</p>
            <p class="text-[10px] ${alertColor} mt-1">${alertText}</p>
        </div>
    `;
}

function renderMetricsSection(m, e) {
    if (!m || Object.keys(m).length === 0) {
        document.getElementById('metrics-section').innerHTML = `
            <h4 class="text-[10px] text-gray-500 uppercase tracking-widest font-bold mb-4">Model Reliability</h4>
            <p class="text-xs text-gray-600 italic">Metrik tapılmadı</p>`;
        return;
    }
    document.getElementById('metrics-section').innerHTML = `
        <h4 class="text-[10px] text-gray-500 uppercase tracking-widest font-bold mb-4">Model Reliability</h4>
        <div class="space-y-3">
            <div class="flex justify-between text-xs border-b border-gray-800 pb-2">
                <span class="text-gray-400">Temp. Precision (R²)</span>
                <span class="text-green-400 font-mono">${((m.temp_r2 ?? 0) * 100).toFixed(1)}%</span>
            </div>
            <div class="flex justify-between text-xs border-b border-gray-800 pb-2">
                <span class="text-gray-400">Wind Error (RMSE)</span>
                <span class="text-yellow-400 font-mono">±${m.wind_rmse ?? '--'} m/s</span>
            </div>
            <div class="flex justify-between text-xs border-b border-gray-800 pb-2">
                <span class="text-gray-400">Solar R²</span>
                <span class="text-blue-400 font-mono">${((m.solar_r2 ?? 0) * 100).toFixed(1)}%</span>
            </div>
        </div>
        <div class="mt-6 pt-4 border-t border-gray-800 space-y-1">
            <p class="text-[10px] text-gray-500 uppercase tracking-widest font-bold mb-2">30-Day Energy Total</p>
            <div class="flex justify-between text-xs">
                <span class="text-gray-400">💨 Wind</span>
                <span class="text-cyan-400 font-mono">${e?.wind ?? '--'} kWh</span>
            </div>
            <div class="flex justify-between text-xs">
                <span class="text-gray-400">☀️ Solar</span>
                <span class="text-orange-400 font-mono">${e?.solar ?? '--'} kWh</span>
            </div>
            <div class="flex justify-between text-xs font-bold border-t border-gray-800 pt-1 mt-1">
                <span class="text-white">Total</span>
                <span class="text-white font-mono">${e?.total ?? '--'} kWh</span>
            </div>
        </div>
    `;
}

// ── Sağ panel: 30 günlük Hava cədvəli ────────────────────────────────────
function renderForecastTable(fc) {
    const container = document.getElementById('forecast-table-container');
    if (!container || !fc || fc.length === 0) return;
    const conditionEmoji = c => ({ 'Sunny':'☀️','Clear':'🌙','Rain':'🌧','Cloudy':'☁️','Snow':'❄️','Storm':'⛈️','Fog':'🌫️' })[c] || '🌡️';
    const rows = fc.map(d => `
        <tr class="border-b border-gray-800/50 hover:bg-white/5 transition-colors">
            <td class="py-2 pr-4 text-gray-500 text-xs font-mono whitespace-nowrap">${d.date}</td>
            <td class="py-2 pr-4 text-xs">${conditionEmoji(d.condition)} <span class="text-gray-400">${d.condition}</span></td>
            <td class="py-2 pr-3 text-right">
                <span class="text-red-400 font-mono text-xs">${d.temp_max ?? '--'}°</span>
                <span class="text-gray-600 mx-1">/</span>
                <span class="text-blue-400 font-mono text-xs">${d.temp_min ?? '--'}°</span>
            </td>
            <td class="py-2 pr-3 text-right text-gray-400 font-mono text-xs">${d.humidity ?? '--'}%</td>
            <td class="py-2 text-right"><span class="font-mono text-xs ${impactColor(d.impact)}">${d.impact ?? '--'}</span></td>
            <td class="py-2 pl-3 text-center text-xs">
                ${d.alert > 0 ? '<span class="text-red-500 text-[10px]">⚠ Alert</span>' : '<span class="text-gray-700">—</span>'}
            </td>
        </tr>`).join('');
    container.innerHTML = `
        <table class="w-full text-left">
            <thead><tr class="border-b border-gray-700">
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-500 font-bold pr-4">Date</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-500 font-bold pr-4">Condition</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-500 font-bold pr-3 text-right">Temp</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-500 font-bold pr-3 text-right">Hum.</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-500 font-bold text-right">Impact</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-500 font-bold pl-3 text-center">Alert</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>`;
}

function impactColor(score) {
    if (score == null) return 'text-gray-500';
    if (score >= 70)   return 'text-red-500';
    if (score >= 40)   return 'text-yellow-400';
    return 'text-green-400';
}

// ── Sağ panel: 30 günlük Enerji cədvəli ──────────────────────────────────
function renderEnergyTable(ef) {
    const container = document.getElementById('energy-table-container');
    if (!container || !ef || ef.length === 0) return;
    const rows = ef.map(d => `
        <tr class="border-b border-gray-800/50 hover:bg-white/5 transition-colors">
            <td class="py-2 pr-4 text-gray-500 text-xs font-mono whitespace-nowrap">${d.date}</td>
            <td class="py-2 pr-4 text-cyan-400 font-mono text-xs text-right">${d.wind} kWh</td>
            <td class="py-2 pr-4 text-orange-400 font-mono text-xs text-right">${d.solar} kWh</td>
            <td class="py-2 text-white font-mono text-xs text-right font-bold">${d.total} kWh</td>
        </tr>`).join('');
    container.innerHTML = `
        <table class="w-full text-left">
            <thead><tr class="border-b border-gray-700">
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-500 font-bold pr-4">Date</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-cyan-600 font-bold pr-4 text-right">💨 Wind</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-orange-600 font-bold pr-4 text-right">☀️ Solar</th>
                <th class="pb-3 text-[10px] uppercase tracking-widest text-gray-400 font-bold text-right">Total</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>`;
}

// ── Chart.js — tempChart (distribusiya) + energyChart ────────────────────
function initCharts(fc, ef, histTemps) {
    if (tempChartInst)   { tempChartInst.destroy();   tempChartInst   = null; }
    if (energyChartInst) { energyChartInst.destroy(); energyChartInst = null; }

    ['tempChart', 'energyChart'].forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.style.width = ''; el.style.height = ''; }
    });

    const distValues = (histTemps && histTemps.length > 5)
        ? histTemps
        : (fc || []).map(d => d.temp_max).filter(v => v != null);

    // ── Distribusiya Histoqramı ───────────────────────────────────────
    const tempCtx = document.getElementById('tempChart');
    if (tempCtx && distValues.length > 0) {
        const values = distValues;
        const mean   = values.reduce((a, b) => a + b, 0) / values.length;
        const sorted = [...values].sort((a, b) => a - b);
        const median = sorted.length % 2 === 0
            ? (sorted[sorted.length/2 - 1] + sorted[sorted.length/2]) / 2
            : sorted[Math.floor(sorted.length/2)];
        const std  = Math.sqrt(values.reduce((s, v) => s + (v - mean)**2, 0) / values.length);
        const skew = values.reduce((s, v) => s + ((v - mean)/std)**3, 0) / values.length;
        const swLabel = Math.abs(skew) < 0.5 ? 'NORMAL' : 'NOT NORMAL';

        const BINS = 10;
        const minV = Math.min(...values), maxV = Math.max(...values);
        const binW = (maxV - minV) / BINS;
        const counts = Array(BINS).fill(0);
        values.forEach(v => { let i = Math.floor((v - minV) / binW); if (i >= BINS) i = BINS - 1; counts[i]++; });
        const density    = counts.map(c => c / (values.length * binW));
        const binCenters = Array.from({ length: BINS }, (_, i) => minV + (i + 0.5) * binW);
        const binLabels  = binCenters.map(v => v.toFixed(1) + '°');

        const CURVE_POINTS = 60;
        const step  = (maxV - minV) / CURVE_POINTS;
        const curveX = Array.from({ length: CURVE_POINTS + 1 }, (_, i) => minV + i * step);
        const curveY = curveX.map(x => (1/(std*Math.sqrt(2*Math.PI))) * Math.exp(-0.5*((x-mean)/std)**2));

        tempChartInst = new Chart(tempCtx, {
            data: {
                labels: binLabels,
                datasets: [
                    { type:'bar',  label:'Density',     data:density, backgroundColor:'rgba(66,165,245,0.55)', borderColor:'rgba(255,255,255,0.15)', borderWidth:0.5, borderRadius:2, order:2 },
                    { type:'line', label:'Normal fit',  data:curveY.filter((_,i)=>i%Math.ceil(CURVE_POINTS/BINS)===0).slice(0,BINS), borderColor:'#ef4444', borderWidth:2, pointRadius:0, tension:0.5, fill:false, order:1 }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 600 },
                plugins: {
                    legend: { labels: { color:'#9ca3af', font:{ size:10 }, boxWidth:12 } },
                    title: {
                        display: true,
                        text: 'Distribution of Daily Max Temperature',
                        color: '#9ca3af',
                        font: { size:11, weight:'bold' },
                    },
                    subtitle: {
                        display: true,
                        text: `Skew=${skew.toFixed(2)}  |  Mean=${mean.toFixed(1)}°  |  Median=${median.toFixed(1)}°  |  σ=${std.toFixed(1)}  →  [${swLabel}]`,
                        color: '#9ca3af',
                        font: { size:9 }
                    },
                    tooltip: { callbacks: { label: ctx => ctx.datasetIndex===0 ? `Density: ${ctx.parsed.y.toFixed(4)}` : `Normal: ${ctx.parsed.y.toFixed(4)}` } }
                },
                scales: {
                    x: { ticks:{ color:'#6b7280', font:{size:9} }, grid:{ color:'rgba(255,255,255,0.05)' }, title:{ display:true, text:'Max Temp (°C)', color:'#6b7280', font:{size:9} } },
                    y: { ticks:{ color:'#6b7280', font:{size:9} }, grid:{ color:'rgba(255,255,255,0.05)' }, title:{ display:true, text:'Density',      color:'#6b7280', font:{size:9} } }
                }
            },
            plugins: [{
                id: 'distLines',
                afterDraw(chart) {
                    const { ctx: c, scales: { x, y } } = chart;
                    const meanBin = (mean   - minV) / binW - 0.5;
                    const medBin  = (median - minV) / binW - 0.5;
                    const toPixel = bi => x.left + ((bi - x.min) / (x.max - x.min)) * (x.right - x.left);
                    c.save();
                    // Mean
                    c.beginPath(); c.strokeStyle='#1e3a8a'; c.lineWidth=1.8; c.setLineDash([5,4]);
                    c.moveTo(toPixel(meanBin), y.top); c.lineTo(toPixel(meanBin), y.bottom); c.stroke();
                    c.fillStyle='#93c5fd'; c.font='bold 9px sans-serif';
                    c.fillText(`Mean=${mean.toFixed(1)}°`, toPixel(meanBin)+4, y.top+14);
                    // Median
                    c.beginPath(); c.strokeStyle='#16a34a'; c.setLineDash([3,3]);
                    c.moveTo(toPixel(medBin), y.top); c.lineTo(toPixel(medBin), y.bottom); c.stroke();
                    c.fillStyle='#86efac';
                    c.fillText(`Med=${median.toFixed(1)}°`, toPixel(medBin)+4, y.top+26);
                    c.restore();
                }
            }]
        });
    }

    // ── Enerji Qrafiki ────────────────────────────────────────────────
    const energyCtx = document.getElementById('energyChart');
    if (energyCtx && ef && ef.length > 0) {
        energyChartInst = new Chart(energyCtx, {
            type: 'bar',
            data: {
                labels: ef.map(d => d.date ? d.date.slice(5) : ''),
                datasets: [
                    { label:'Wind kWh',  data:ef.map(d=>d.wind),  backgroundColor:'rgba(34,211,238,0.7)',  borderRadius:4, stack:'energy' },
                    { label:'Solar kWh', data:ef.map(d=>d.solar), backgroundColor:'rgba(251,146,60,0.7)', borderRadius:4, stack:'energy' }
                ]
            },
            options: chartOptions('30-Day Energy Forecast (kWh)', true)
        });
    }
}

function chartOptions(title, stacked=false) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { labels: { color:'#9ca3af', font:{size:10}, boxWidth:12 } },
            title:  { display:true, text:title, color:'#9ca3af', font:{size:11, weight:'bold'} }
        },
        scales: {
            x: { stacked, ticks:{ color:'#6b7280', font:{size:9}, maxRotation:45 }, grid:{ color:'rgba(255,255,255,0.04)' } },
            y: { stacked, ticks:{ color:'#6b7280', font:{size:9} },                 grid:{ color:'rgba(255,255,255,0.06)' } }
        }
    };
}

// ── Yükləmə göstəricisi ───────────────────────────────────────────────────
// FIX: cədvəl ID-ləri silindi — onlar artıq _buildRightPanelShell tərəfindən
//      idarə edilir, setLoadingState zamanı hələ DOM-da yoxdurlar
function setLoadingState() {
    ['weather-section', 'impact-section', 'metrics-section'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.innerHTML = `
            <div class="animate-pulse h-4 bg-gray-800 rounded w-3/4 mb-2"></div>
            <div class="animate-pulse h-4 bg-gray-800 rounded w-1/2"></div>`;
    });
}

// ── Colorful Mode ─────────────────────────────────────────────────────────
const azerbaijanGeoJSON = {
    type:"FeatureCollection",features:[{type:"Feature",properties:{name:"Azerbaijan"},
    geometry:{type:"MultiPolygon",coordinates:[[[[44.7,39.7],[45.1,39.6],[45.9,39.2],[46.5,38.8],[47.5,38.4],[48.6,38.4],
    [49.0,39.0],[49.2,40.2],[50.5,40.5],[49.3,41.5],[48.4,41.9],[47.3,41.9],[46.4,42.1],[44.8,41.9],[44.7,39.7]]]]}}]
};

async function toggleColorfulMode() {
    colorfulMode = !colorfulMode;
    const btn = document.getElementById('toggle-mode');
    const mapContainer = document.getElementById('map');
    if (colorfulMode) {
        btn.innerHTML = "Dark Mode";
        btn.classList.add('text-red-500','border-red-600');
        geoLayer = L.geoJSON(azerbaijanGeoJSON, { style:{ color:"#ef4444", weight:3, fillColor:"#ef4444", fillOpacity:0.2, dashArray:'5, 10' } }).addTo(map);
        mapContainer.style.filter = "brightness(0.7) saturate(1.5) contrast(1.2)";
        map.flyTo([40.4, 48.5], 7.5, { duration:2 });
    } else {
        btn.innerHTML = "Colorful Mode";
        btn.classList.remove('text-red-500','border-red-600');
        if (geoLayer) { map.removeLayer(geoLayer); geoLayer = null; }
        mapContainer.style.filter = "none";
    }
}

// ── Sidebar Bağla ─────────────────────────────────────────────────────────
function closeSidebar() {
    sidebar.classList.add('-translate-x-full');
    overlay.classList.add('hidden');
}

// ── Event Listeners ───────────────────────────────────────────────────────
if (closeBtn)    closeBtn.addEventListener('click', closeSidebar);
if (overlay)     overlay.addEventListener('click', closeSidebar);
if (modeBtn)     modeBtn.addEventListener('click', toggleColorfulMode);
if (searchInput) searchInput.addEventListener('input', e => {
    const v = e.target.value.toLowerCase();
    const found = globalData.find(c => c.city.toLowerCase().includes(v));
    if (found) map.flyTo([found.lat, found.lon], 9, { duration:1 });
});

// ── Admin Panel ───────────────────────────────────────────────────────────
const adminBtn   = document.getElementById('admin-btn');
const adminModal = document.getElementById('admin-modal');
const closeAdmin = document.getElementById('close-admin');
const adminForm  = document.getElementById('admin-form');

if (adminBtn)   adminBtn.onclick   = () => adminModal.classList.replace('hidden','flex');
if (closeAdmin) closeAdmin.onclick = () => adminModal.classList.replace('flex','hidden');
if (adminForm) {
    adminForm.onsubmit = async e => {
        e.preventDefault();
        const newCity = {
            city: document.getElementById('city-name').value,
            lat:  parseFloat(document.getElementById('city-lat').value),
            lon:  parseFloat(document.getElementById('city-lon').value),
            temp: parseFloat(document.getElementById('city-temp').value),
            risk: parseFloat(document.getElementById('city-temp').value) > 30 ? "High" : "Low"
        };
        try {
            const res = await fetch('http://127.0.0.1:8000/api/admin/add', {
                method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(newCity)
            });
            if (res.ok) {
                alert(`${newCity.city} uğurla əlavə edildi!`);
                adminForm.reset();
                adminModal.classList.replace('flex','hidden');
                fetchWeatherData();
            }
        } catch (err) { console.error("Əlavə etmə xətası:", err); alert("Serverə qoşulmaq mümkün olmadı!"); }
    };
}

// ── Başlat ────────────────────────────────────────────────────────────────
setTimeout(() => { map.invalidateSize(); }, 400);
fetchWeatherData();
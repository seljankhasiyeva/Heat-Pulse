// 1. Qlobal Dəyişənlər
let globalData = []; 
let geoLayer = null;
let colorfulMode = false;

// Xəritəni başlat
const map = L.map('map', { zoomControl: false }).setView([40.4093, 49.8671], 7);

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap',
    maxZoom: 19
}).addTo(map);

// 1. Küçük boyutlu marker ikonu tanımla
const smallIcon = L.icon({
    iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
    iconSize: [15, 25], // Pin boyutunu buradan küçültebilirsin (Genişlik, Yükseklik)
    iconAnchor: [7, 25],
    popupAnchor: [1, -20],
    shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
    shadowSize: [25, 25]
});


// DOM Elementləri
const sidebar = document.getElementById('sidebar');
const sidebarContent = document.getElementById('sidebar-content');
const overlay = document.getElementById('overlay');
const closeBtn = document.getElementById('close-btn');
const searchInput = document.getElementById('map-search');
const modeBtn = document.getElementById('toggle-mode');

// 3. Backend-dən datanı çəkən funksiya
async function fetchWeatherData() {
    try {
        const response = await fetch('http://127.0.0.1:8000/api/weather');
        const result = await response.json();

        if (result.status === "success") {
            globalData = result.data;
            displayCities(globalData);
        }
    } catch (error) {
        console.error("Backend xətası:", error);
        if(sidebarContent) {
            sidebarContent.innerHTML = "<p class='text-red-500 p-4 font-bold'>⚠️ Server bağlantısı yoxdur!</p>";
        }
    }
}

// 4. Şəhər detallarını Sidebar-da göstər
function openDetails(city) {
    const sidebar = document.getElementById('sidebar');
    const sidebarContent = document.getElementById('sidebar-content');
    const overlay = document.getElementById('overlay');

    // Sidebar-ı gizli haldan çıxarırıq
    sidebar.classList.remove('-translate-x-full');
    if(overlay) overlay.classList.remove('hidden');

    // İçini dinamik doldururuq
    sidebarContent.innerHTML = `
        <div class="animate-fadeIn p-4">
            <h2 class="text-red-600 font-bold tracking-widest text-[10px] uppercase mb-1 italic">Live Analytics</h2>
            <h1 class="text-4xl font-black italic text-white mb-6 tracking-tighter uppercase">${city.city}</h1>
            
            <div class="space-y-4">
                <div class="bg-gray-900 border border-gray-800 p-6 rounded-3xl">
                    <p class="text-gray-500 text-xs uppercase mb-1 font-bold">Temperatur</p>
                    <p class="text-4xl font-bold text-white">${city.temp}°C</p>
                </div>
                <div class="bg-gray-900 border border-gray-800 p-6 rounded-3xl">
                    <p class="text-gray-500 text-xs uppercase mb-1 font-bold">Risk Statusu</p>
                    <p class="text-2xl font-bold ${city.temp > 30 ? 'text-red-500' : 'text-green-500'}">
                        ${city.temp > 30 ? 'HIGH' : 'NORMAL'} RISK
                    </p>
                </div>
            </div>

            <div class="mt-8 bg-gray-800/30 p-6 rounded-3xl border border-gray-700/50 backdrop-blur-md text-white">
                <h3 class="text-xs font-bold text-gray-300 mb-2 uppercase tracking-widest">Termal Analiz</h3>
                <p class="text-sm italic">
                    ${city.temp > 30 
                        ? "Yüksək temperatur aşkarlanmışdır. Jeneratör soyutma sistemlərini yoxlayın." 
                        : "Sıcaklıq normaldır. Sistem stabil işləyir."}
                </p>
            </div>
        </div>
    `;
}

// 5. Markerləri Xəritəyə Düz
function displayCities(cities) {
    // Əvvəlki markerləri təmizləmək üçün (əgər lazımdırsa)
    cities.forEach(city => {
        const lat = city.lat;
        const lon = city.lon;
        
        // Kiçik ikonu burada tətbiq edirik
        const marker = L.marker([lat, lon], { icon: smallIcon }).addTo(map);
        
        marker.on('click', () => {
            // Sidebar-da datanı göstərən əsas funksiyanı çağırırıq
            openDetails(city);
            // Şəhərə fokuslanmaq
            map.flyTo([lat, lon], 10, { duration: 1.5 });
        });
    });
}

// 6. Colorful Mode Funksiyası
// Azərbaycanın sadələşdirilmiş GeoJSON datası (Birbaşa kodun daxilində)
const azerbaijanGeoJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": { "name": "Azerbaijan" },
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [[[[44.7, 39.7], [45.1, 39.6], [45.9, 39.2], [46.5, 38.8], [47.5, 38.4], [48.6, 38.4], [49.0, 39.0], [49.2, 40.2], [50.5, 40.5], [49.3, 41.5], [48.4, 41.9], [47.3, 41.9], [46.4, 42.1], [44.8, 41.9], [44.7, 39.7]]]]
            }
        }
    ]
};

async function toggleColorfulMode() {
    colorfulMode = !colorfulMode;
    const btn = document.getElementById('toggle-mode');
    const mapContainer = document.getElementById('map');

    if (colorfulMode) {
        btn.innerHTML = "Dark Mode";
        btn.classList.add('text-red-500', 'border-red-600');

        // İnternetə ehtiyac duymadan birbaşa obyektdən çəkirik
        geoLayer = L.geoJSON(azerbaijanGeoJSON, {
            style: {
                color: "#ef4444", // Neon qırmızı sərhəd
                weight: 3,
                fillColor: "#ef4444",
                fillOpacity: 0.2,
                dashArray: '5, 10' // Sərhədlər qırıq-qırıq (daha modern görünüş)
            }
        }).addTo(map);

        // Vizual effektlər
        mapContainer.style.filter = "brightness(0.7) saturate(1.5) contrast(1.2)";
        map.flyTo([40.4, 48.5], 7.5, { duration: 2 });

    } else {
        btn.innerHTML = "Colorful Mode";
        btn.classList.remove('text-red-500', 'border-red-600');
        
        if (geoLayer) {
            map.removeLayer(geoLayer);
            geoLayer = null;
        }
        mapContainer.style.filter = "none";
    }
}

// 7. Event Listeners
if(closeBtn) closeBtn.addEventListener('click', closeSidebar);
if(overlay) overlay.addEventListener('click', closeSidebar);
if(modeBtn) modeBtn.addEventListener('click', toggleColorfulMode);

function closeSidebar() {
    sidebar.classList.add('-translate-x-full');
    overlay.classList.add('hidden');
}

// Axtarış
if(searchInput) {
    searchInput.addEventListener('input', (e) => {
        const value = e.target.value.toLowerCase();
        const found = globalData.find(c => c.city.toLowerCase().includes(value));
        if (found) {
            map.flyTo([found.lat, found.lon], 9, { duration: 1 });
        }
    });
}

// Xəritənin ölçülərini yenilə (boz ekran olmasın deyə)
setTimeout(() => { map.invalidateSize(); }, 400);

// Sistemi başlat
fetchWeatherData();

// Admin Panel Elementləri
const adminBtn = document.getElementById('admin-btn');
const adminModal = document.getElementById('admin-modal');
const closeAdmin = document.getElementById('close-admin');
const adminForm = document.getElementById('admin-form');

// Modalı aç/bağla
adminBtn.onclick = () => adminModal.classList.replace('hidden', 'flex');
closeAdmin.onclick = () => adminModal.classList.replace('flex', 'hidden');

// Yeni Şəhər Əlavə Etmə Funksiyası
adminForm.onsubmit = async (e) => {
    e.preventDefault();

    const newCity = {
        city: document.getElementById('city-name').value,
        lat: parseFloat(document.getElementById('city-lat').value),
        lon: parseFloat(document.getElementById('city-lon').value),
        temp: parseFloat(document.getElementById('city-temp').value),
        risk: parseFloat(document.getElementById('city-temp').value) > 30 ? "High" : "Low"
    };

    try {
        // Backend-ə POST istəyi (main.py-da yaratdığımız endpoint)
        const response = await fetch('http://127.0.0.1:8000/api/admin/add', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(newCity)
        });

        if (response.ok) {
            alert(`${newCity.city} uğurla əlavə edildi!`);
            adminForm.reset();
            adminModal.classList.replace('flex', 'hidden');
            
            // Xəritəni yeniləyirik ki, yeni marker görünsün
            fetchWeatherData(); 
        }
    } catch (error) {
        console.error("Əlavə etmə xətası:", error);
        alert("Serverə qoşulmaq mümkün olmadı!");
    }
};
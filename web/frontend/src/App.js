// 1. Qlobal Dəyişənlər
let globalData = []; // Axtarış üçün datanı burada saxlayacağıq
const map = L.map('map', { zoomControl: false }).setView([40.4093, 49.8671], 7);

// 2. Xəritə Layeri (Dark Mode)
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap'
}).addTo(map);

// Elementləri bir dəfə tuturuq
const sidebar = document.getElementById('sidebar');
const sidebarContent = document.getElementById('sidebar-content');
const overlay = document.getElementById('overlay');
const closeBtn = document.getElementById('close-btn');
const searchInput = document.getElementById('map-search');

// 3. Backend-dən datanı çəkən funksiya
async function fetchWeatherData() {
    try {
        const response = await fetch('http://127.0.0.1:8000/api/weather');
        const result = await response.json();

        if (result.status === "success") {


            globalData = result.data; // Datanı yaddaşa yazırıq
            displayCities(globalData);
        }
    } catch (error) {
        console.error("Backend xətası:", error);
        if(sidebarContent) {
            sidebarContent.innerHTML = "<p class='text-red-500 p-4'>Server bağlantısı yoxdur!</p>";
        }
    }
}

// 4. Şəhər detallarını Sidebar-da göstər
function openDetails(city) {
    sidebar.classList.remove('-translate-x-full');
    overlay.classList.remove('hidden');

    sidebarContent.innerHTML = `
        <div class="animate-fadeIn">
            <h2 class="text-red-600 font-bold tracking-widest text-xs uppercase mb-1 italic">Live Feed</h2>
            <h1 class="text-5xl font-black italic text-white mb-6 tracking-tighter uppercase">${city.city}</h1>
            
            <div class="space-y-4">
                <div class="bg-gray-900 border border-gray-800 p-6 rounded-3xl">
                    <p class="text-gray-500 text-xs uppercase mb-1 font-bold">Temperatur</p>
                    <p class="text-4xl font-bold text-white">${city.temp}°C</p>
                </div>
                <div class="bg-gray-900 border border-gray-800 p-6 rounded-3xl">
                    <p class="text-gray-500 text-xs uppercase mb-1 font-bold">Risk Statusu</p>
                    <p class="text-2xl font-bold ${city.risk === 'High' ? 'text-red-500' : 'text-green-500'}">
                        ${city.risk.toUpperCase()} RISK
                    </p>
                </div>
            </div>

            <div class="mt-8 bg-gray-800/30 p-6 rounded-3xl border border-gray-700/50 backdrop-blur-md">
                <h3 class="text-xs font-bold text-gray-300 mb-4 uppercase tracking-widest">Həftəlik Trend</h3>
                <div class="h-32 flex items-end justify-between gap-1">
                    <div class="w-full bg-gray-700 h-1/2 rounded-t-sm"></div>
                    <div class="w-full bg-gray-700 h-3/4 rounded-t-sm"></div>
                    <div class="w-full bg-red-600 h-full rounded-t-sm shadow-[0_0_10px_rgba(220,38,38,0.5)]"></div>
                    <div class="w-full bg-gray-700 h-2/3 rounded-t-sm"></div>
                </div>
            </div>
        </div>
    `;
}

// 5. Markerləri Xəritəyə Düz
function displayCities(cities) {
    cities.forEach(city => {
        const lat = city.lat || 40.4;
        const lon = city.lon || 49.8;
        
        const marker = L.marker([lat, lon]).addTo(map);
        
        // Markerə klikləyəndə sidebar açılsın
        marker.on('click', () => {
            openDetails(city);
            map.flyTo([lat, lon], 10, { duration: 1.5 });
        });
    });
}

// 6. Sidebar Bağlamaq
function closeSidebar() {
    sidebar.classList.add('-translate-x-full');
    overlay.classList.add('hidden');
}

closeBtn.addEventListener('click', closeSidebar);
overlay.addEventListener('click', closeSidebar);

// 7. Axtarış (Search) Funksiyası
if(searchInput) {
    searchInput.addEventListener('input', (e) => {
        const value = e.target.value.toLowerCase();
        const found = globalData.find(c => c.city.toLowerCase().includes(value));
        if (found) {
            map.flyTo([found.lat, found.lon], 9);
        }
    });
}

// Xəritənin ölçülərini düzəltmək üçün (əgər gizli qalıbsa)
setTimeout(() => { map.invalidateSize(); }, 500);

// Sistemi başlat
fetchWeatherData();
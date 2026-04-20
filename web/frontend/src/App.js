// 1. Xəritəni başlat (Azərbaycan mərkəzli)
const map = L.map('map').setView([40.4093, 49.8671], 7);

// 2. Xəritənin dizaynını (Layer) əlavə et (Dark Mode görünüşü üçün)
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// 3. Backend-dən datanı çəkən funksiya
async function fetchWeatherData() {
    try {
        // FastAPI serverinin ünvanı
        const response = await fetch('http://127.0.0.1:8000/api/weather');
        const result = await response.json();

        if (result.status === "success") {
            displayCities(result.data);
        }
    } catch (error) {
        console.error("Backend-ə bağlanarkən xəta oldu:", error);
        document.getElementById('city-list').innerHTML = "<p class='text-red-500'>Server bağlantısı yoxdur!</p>";
    }
}

// 4. Şəhərləri həm xəritədə, həm də sidebar-da göstər
function displayCities(cities) {
    const cityList = document.getElementById('city-list');
    cityList.innerHTML = ""; // "Loading..." yazısını silirik

    cities.forEach(city => {
        // Xəritəyə Marker (nöqtə) əlavə et
        const marker = L.marker([city.lat || 40.4, city.lon || 49.8]).addTo(map);
        marker.bindPopup(`<b>${city.city}</b><br>Temp: ${city.temp}°C<br>Risk: ${city.risk}`);

        // Sidebar-a şəhər kartı əlavə et
        const cityCard = `
            <div class="p-4 bg-gray-700 rounded-lg shadow border-l-4 ${city.risk === 'High' ? 'border-red-500' : 'border-green-500'}">
                <h3 class="font-bold">${city.city}</h3>
                <p class="text-sm text-gray-300">Temperatur: ${city.temp}°C</p>
                <span class="text-xs font-semibold uppercase">${city.risk} Risk</span>
            </div>
        `;
        cityList.innerHTML += cityCard;
    });
}

// Səhifə yüklənəndə datanı çək
fetchWeatherData();
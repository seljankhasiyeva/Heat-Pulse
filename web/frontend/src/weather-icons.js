// ── weather-icons.js ─────────────────────────────────────────────────────
// Hər condition üçün animasiyalı SVG ikon qaytarır

const WEATHER_ICONS = {

    "Clear Sky": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .sun-core { animation: pulse-sun 2s ease-in-out infinite; transform-origin: 40px 40px; }
        .sun-ray  { animation: spin-ray 8s linear infinite; transform-origin: 40px 40px; }
        @keyframes pulse-sun { 0%,100%{r:14} 50%{r:16} }
        @keyframes spin-ray  { from{transform:rotate(0deg)} to{transform:rotate(360deg)} }
      </style>
      <g class="sun-ray">
        <line x1="40" y1="6"  x2="40" y2="14" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
        <line x1="40" y1="66" x2="40" y2="74" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
        <line x1="6"  y1="40" x2="14" y2="40" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
        <line x1="66" y1="40" x2="74" y2="40" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
        <line x1="16" y1="16" x2="22" y2="22" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
        <line x1="58" y1="58" x2="64" y2="64" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
        <line x1="64" y1="16" x2="58" y2="22" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
        <line x1="22" y1="58" x2="16" y2="64" stroke="#fbbf24" stroke-width="3" stroke-linecap="round"/>
      </g>
      <circle class="sun-core" cx="40" cy="40" r="14" fill="#fbbf24"/>
    </svg>`,

    "Cloudy": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .cloud-main { animation: float-cloud 3s ease-in-out infinite; }
        .cloud-back { animation: float-cloud 3s ease-in-out infinite 0.5s; opacity:0.5; }
        @keyframes float-cloud { 0%,100%{transform:translateY(0)} 50%{transform:translateY(-4px)} }
      </style>
      <g class="cloud-back">
        <ellipse cx="50" cy="38" rx="18" ry="12" fill="#64748b"/>
        <ellipse cx="38" cy="42" rx="14" ry="10" fill="#64748b"/>
        <rect x="24" y="42" width="44" height="10" rx="5" fill="#64748b"/>
      </g>
      <g class="cloud-main">
        <ellipse cx="44" cy="34" rx="20" ry="14" fill="#94a3b8"/>
        <ellipse cx="30" cy="40" rx="16" ry="12" fill="#94a3b8"/>
        <rect x="14" y="40" width="50" height="12" rx="6" fill="#94a3b8"/>
      </g>
    </svg>`,

    "Cloudy-Sunny": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .cs-ray   { animation: spin-ray 8s linear infinite; transform-origin: 24px 30px; }
        .cs-cloud { animation: float-cloud 3s ease-in-out infinite; }
        @keyframes spin-ray   { from{transform:rotate(0deg)} to{transform:rotate(360deg)} }
        @keyframes float-cloud{ 0%,100%{transform:translateY(0)} 50%{transform:translateY(-3px)} }
      </style>
      <g class="cs-ray">
        <line x1="24" y1="8"  x2="24" y2="14" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
        <line x1="24" y1="46" x2="24" y2="52" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
        <line x1="4"  y1="30" x2="10" y2="30" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
        <line x1="38" y1="30" x2="44" y2="30" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
        <line x1="11" y1="15" x2="16" y2="20" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
        <line x1="32" y1="40" x2="37" y2="45" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
        <line x1="37" y1="15" x2="32" y2="20" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
        <line x1="16" y1="40" x2="11" y2="45" stroke="#fbbf24" stroke-width="2.5" stroke-linecap="round"/>
      </g>
      <circle cx="24" cy="30" r="10" fill="#fbbf24"/>
      <g class="cs-cloud">
        <ellipse cx="50" cy="46" rx="20" ry="13" fill="#94a3b8"/>
        <ellipse cx="36" cy="52" rx="15" ry="11" fill="#94a3b8"/>
        <rect x="21" y="52" width="49" height="11" rx="5.5" fill="#94a3b8"/>
      </g>
    </svg>`,

    "Fog": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .fog-line { animation: fog-drift 2.5s ease-in-out infinite; }
        .fog-line:nth-child(2) { animation-delay: 0.4s; }
        .fog-line:nth-child(3) { animation-delay: 0.8s; }
        .fog-line:nth-child(4) { animation-delay: 1.2s; }
        @keyframes fog-drift { 0%,100%{opacity:0.4;transform:translateX(0)} 50%{opacity:1;transform:translateX(4px)} }
      </style>
      <rect class="fog-line" x="10" y="22" width="60" height="6" rx="3" fill="#94a3b8"/>
      <rect class="fog-line" x="14" y="35" width="52" height="6" rx="3" fill="#94a3b8"/>
      <rect class="fog-line" x="8"  y="48" width="56" height="6" rx="3" fill="#94a3b8"/>
      <rect class="fog-line" x="16" y="61" width="44" height="6" rx="3" fill="#94a3b8"/>
    </svg>`,

    "Drizzle": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .drz-drop { animation: drz-fall 1.4s linear infinite; }
        .drz-drop:nth-child(2){animation-delay:0.2s}
        .drz-drop:nth-child(3){animation-delay:0.5s}
        .drz-drop:nth-child(4){animation-delay:0.8s}
        .drz-drop:nth-child(5){animation-delay:1.1s}
        .drz-drop:nth-child(6){animation-delay:0.35s}
        @keyframes drz-fall {
          0%  { transform:translateY(-6px); opacity:0 }
          20% { opacity:1 }
          80% { opacity:1 }
          100%{ transform:translateY(14px); opacity:0 }
        }
      </style>
      <!-- Cloud -->
      <ellipse cx="44" cy="28" rx="20" ry="13" fill="#64748b"/>
      <ellipse cx="28" cy="34" rx="16" ry="11" fill="#64748b"/>
      <rect x="12" y="34" width="52" height="10" rx="5" fill="#64748b"/>
      <!-- Drops -->
      <line class="drz-drop" x1="24" y1="52" x2="22" y2="60" stroke="#7dd3fc" stroke-width="2" stroke-linecap="round"/>
      <line class="drz-drop" x1="34" y1="52" x2="32" y2="60" stroke="#7dd3fc" stroke-width="2" stroke-linecap="round"/>
      <line class="drz-drop" x1="44" y1="52" x2="42" y2="60" stroke="#7dd3fc" stroke-width="2" stroke-linecap="round"/>
      <line class="drz-drop" x1="54" y1="52" x2="52" y2="60" stroke="#7dd3fc" stroke-width="2" stroke-linecap="round"/>
      <line class="drz-drop" x1="29" y1="58" x2="27" y2="66" stroke="#7dd3fc" stroke-width="2" stroke-linecap="round"/>
      <line class="drz-drop" x1="49" y1="58" x2="47" y2="66" stroke="#7dd3fc" stroke-width="2" stroke-linecap="round"/>
    </svg>`,

    "Rain": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .rain-drop { animation: rain-fall 1s linear infinite; }
        .rain-drop:nth-child(2){animation-delay:0.15s}
        .rain-drop:nth-child(3){animation-delay:0.3s}
        .rain-drop:nth-child(4){animation-delay:0.45s}
        .rain-drop:nth-child(5){animation-delay:0.6s}
        .rain-drop:nth-child(6){animation-delay:0.1s}
        .rain-drop:nth-child(7){animation-delay:0.55s}
        @keyframes rain-fall {
          0%  { transform:translateY(-8px); opacity:0 }
          15% { opacity:1 }
          85% { opacity:1 }
          100%{ transform:translateY(18px); opacity:0 }
        }
      </style>
      <ellipse cx="44" cy="26" rx="20" ry="13" fill="#475569"/>
      <ellipse cx="28" cy="32" rx="16" ry="11" fill="#475569"/>
      <rect x="12" y="32" width="52" height="10" rx="5" fill="#475569"/>
      <line class="rain-drop" x1="22" y1="50" x2="18" y2="62" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rain-drop" x1="32" y1="50" x2="28" y2="62" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rain-drop" x1="42" y1="50" x2="38" y2="62" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rain-drop" x1="52" y1="50" x2="48" y2="62" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rain-drop" x1="27" y1="58" x2="23" y2="70" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rain-drop" x1="47" y1="56" x2="43" y2="68" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rain-drop" x1="37" y1="58" x2="33" y2="70" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
    </svg>`,

    "Rain Showers": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .rs-sun  { animation: spin-ray 8s linear infinite; transform-origin: 20px 22px; }
        .rs-drop { animation: rain-fall 0.9s linear infinite; }
        .rs-drop:nth-child(2){animation-delay:0.1s}
        .rs-drop:nth-child(3){animation-delay:0.3s}
        .rs-drop:nth-child(4){animation-delay:0.5s}
        .rs-drop:nth-child(5){animation-delay:0.7s}
        .rs-drop:nth-child(6){animation-delay:0.2s}
        @keyframes spin-ray  { from{transform:rotate(0deg)} to{transform:rotate(360deg)} }
        @keyframes rain-fall {
          0%  { transform:translateY(-8px); opacity:0 }
          20% { opacity:1 }
          80% { opacity:1 }
          100%{ transform:translateY(16px); opacity:0 }
        }
      </style>
      <g class="rs-sun">
        <line x1="20" y1="6"  x2="20" y2="11" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
        <line x1="20" y1="33" x2="20" y2="38" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
        <line x1="4"  y1="22" x2="9"  y2="22" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
        <line x1="31" y1="22" x2="36" y2="22" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
        <line x1="9"  y1="11" x2="13" y2="15" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
        <line x1="27" y1="29" x2="31" y2="33" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
        <line x1="31" y1="11" x2="27" y2="15" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
        <line x1="13" y1="29" x2="9"  y2="33" stroke="#fbbf24" stroke-width="2" stroke-linecap="round"/>
      </g>
      <circle cx="20" cy="22" r="8" fill="#fbbf24"/>
      <ellipse cx="52" cy="34" rx="20" ry="12" fill="#475569"/>
      <ellipse cx="36" cy="40" rx="16" ry="11" fill="#475569"/>
      <rect x="20" y="40" width="52" height="10" rx="5" fill="#475569"/>
      <line class="rs-drop" x1="28" y1="56" x2="24" y2="68" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rs-drop" x1="40" y1="56" x2="36" y2="68" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rs-drop" x1="52" y1="56" x2="48" y2="68" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rs-drop" x1="34" y1="63" x2="30" y2="75" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rs-drop" x1="58" y1="56" x2="54" y2="68" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
      <line class="rs-drop" x1="46" y1="63" x2="42" y2="75" stroke="#38bdf8" stroke-width="2.5" stroke-linecap="round"/>
    </svg>`,

    "Snowfall": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .snow-flake { animation: snow-fall 1.6s linear infinite; }
        .snow-flake:nth-child(2){animation-delay:0.2s}
        .snow-flake:nth-child(3){animation-delay:0.5s}
        .snow-flake:nth-child(4){animation-delay:0.9s}
        .snow-flake:nth-child(5){animation-delay:1.2s}
        .snow-flake:nth-child(6){animation-delay:0.7s}
        @keyframes snow-fall {
          0%  { transform:translateY(-6px) rotate(0deg); opacity:0 }
          20% { opacity:1 }
          80% { opacity:1 }
          100%{ transform:translateY(16px) rotate(180deg); opacity:0 }
        }
      </style>
      <ellipse cx="44" cy="26" rx="20" ry="13" fill="#64748b"/>
      <ellipse cx="28" cy="32" rx="16" ry="11" fill="#64748b"/>
      <rect x="12" y="32" width="52" height="10" rx="5" fill="#64748b"/>
      <!-- Snowflakes -->
      <g class="snow-flake">
        <line x1="22" y1="50" x2="22" y2="60" stroke="#e0f2fe" stroke-width="2" stroke-linecap="round"/>
        <line x1="17" y1="55" x2="27" y2="55" stroke="#e0f2fe" stroke-width="2" stroke-linecap="round"/>
        <line x1="18" y1="51" x2="26" y2="59" stroke="#e0f2fe" stroke-width="1.5" stroke-linecap="round"/>
        <line x1="26" y1="51" x2="18" y2="59" stroke="#e0f2fe" stroke-width="1.5" stroke-linecap="round"/>
      </g>
      <g class="snow-flake">
        <line x1="40" y1="52" x2="40" y2="62" stroke="#e0f2fe" stroke-width="2" stroke-linecap="round"/>
        <line x1="35" y1="57" x2="45" y2="57" stroke="#e0f2fe" stroke-width="2" stroke-linecap="round"/>
        <line x1="36" y1="53" x2="44" y2="61" stroke="#e0f2fe" stroke-width="1.5" stroke-linecap="round"/>
        <line x1="44" y1="53" x2="36" y2="61" stroke="#e0f2fe" stroke-width="1.5" stroke-linecap="round"/>
      </g>
      <g class="snow-flake">
        <line x1="58" y1="50" x2="58" y2="60" stroke="#e0f2fe" stroke-width="2" stroke-linecap="round"/>
        <line x1="53" y1="55" x2="63" y2="55" stroke="#e0f2fe" stroke-width="2" stroke-linecap="round"/>
        <line x1="54" y1="51" x2="62" y2="59" stroke="#e0f2fe" stroke-width="1.5" stroke-linecap="round"/>
        <line x1="62" y1="51" x2="54" y2="59" stroke="#e0f2fe" stroke-width="1.5" stroke-linecap="round"/>
      </g>
      <g class="snow-flake">
        <line x1="30" y1="62" x2="30" y2="70" stroke="#bae6fd" stroke-width="2" stroke-linecap="round"/>
        <line x1="26" y1="66" x2="34" y2="66" stroke="#bae6fd" stroke-width="2" stroke-linecap="round"/>
      </g>
      <g class="snow-flake">
        <line x1="50" y1="62" x2="50" y2="70" stroke="#bae6fd" stroke-width="2" stroke-linecap="round"/>
        <line x1="46" y1="66" x2="54" y2="66" stroke="#bae6fd" stroke-width="2" stroke-linecap="round"/>
      </g>
    </svg>`,

    "Thunderstorm": `
    <svg viewBox="0 0 80 80" width="72" height="72" xmlns="http://www.w3.org/2000/svg">
      <style>
        .thunder-cloud { animation: rumble 0.5s ease-in-out infinite alternate; }
        .lightning     { animation: flash 1.8s ease-in-out infinite; transform-origin: 42px 50px; }
        .rain-t { animation: rain-fall 0.9s linear infinite; }
        .rain-t:nth-child(2){animation-delay:0.15s}
        .rain-t:nth-child(3){animation-delay:0.4s}
        .rain-t:nth-child(4){animation-delay:0.7s}
        @keyframes rumble { 0%{transform:translateX(-1px)} 100%{transform:translateX(1px)} }
        @keyframes flash  { 0%,40%,60%,100%{opacity:1} 50%{opacity:0.1} }
        @keyframes rain-fall {
          0%  { transform:translateY(-6px); opacity:0 }
          20% { opacity:0.7 }
          100%{ transform:translateY(14px); opacity:0 }
        }
      </style>
      <g class="thunder-cloud">
        <ellipse cx="44" cy="24" rx="22" ry="14" fill="#334155"/>
        <ellipse cx="26" cy="30" rx="17" ry="12" fill="#334155"/>
        <rect x="9"  y="30" width="55" height="12" rx="6" fill="#334155"/>
      </g>
      <!-- Lightning bolt -->
      <polygon class="lightning" points="44,44 36,58 42,58 38,72 52,54 45,54" fill="#fde047"/>
      <!-- Rain lines -->
      <line class="rain-t" x1="18" y1="50" x2="14" y2="60" stroke="#38bdf8" stroke-width="2" stroke-linecap="round"/>
      <line class="rain-t" x1="28" y1="50" x2="24" y2="60" stroke="#38bdf8" stroke-width="2" stroke-linecap="round"/>
      <line class="rain-t" x1="62" y1="50" x2="58" y2="60" stroke="#38bdf8" stroke-width="2" stroke-linecap="round"/>
      <line class="rain-t" x1="68" y1="50" x2="64" y2="60" stroke="#38bdf8" stroke-width="2" stroke-linecap="round"/>
    </svg>`,
};

// Alias-lar (JSON-dən gələn condition adları match olsun)
WEATHER_ICONS["Sunny"]           = WEATHER_ICONS["Clear Sky"];
WEATHER_ICONS["Clear"]           = WEATHER_ICONS["Clear Sky"];
WEATHER_ICONS["Partly Cloudy"]   = WEATHER_ICONS["Cloudy-Sunny"];
WEATHER_ICONS["Cloudy-Sunny"]    = WEATHER_ICONS["Cloudy-Sunny"];

/**
 * Condition adına görə ikon SVG string qaytarır.
 * Tapılmasa default olaraq "Cloudy" qaytarır.
 */
function getWeatherIcon(condition) {
    if (!condition) return WEATHER_ICONS["Cloudy"];
    // Tam uyğunluq
    if (WEATHER_ICONS[condition]) return WEATHER_ICONS[condition];
    // Partial match (məsələn "Heavy Rain" → Rain)
    const lower = condition.toLowerCase();
    if (lower.includes("thunder")) return WEATHER_ICONS["Thunderstorm"];
    if (lower.includes("snow"))    return WEATHER_ICONS["Snowfall"];
    if (lower.includes("shower"))  return WEATHER_ICONS["Rain Showers"];
    if (lower.includes("drizzle")) return WEATHER_ICONS["Drizzle"];
    if (lower.includes("rain"))    return WEATHER_ICONS["Rain"];
    if (lower.includes("fog") || lower.includes("mist")) return WEATHER_ICONS["Fog"];
    if (lower.includes("cloud"))   return WEATHER_ICONS["Cloudy"];
    if (lower.includes("sun") || lower.includes("clear")) return WEATHER_ICONS["Clear Sky"];
    return WEATHER_ICONS["Cloudy"];
}

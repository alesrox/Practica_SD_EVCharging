const API_URL = 'http://localhost:7500/charging_points';
const REFRESH_INTERVAL = 2000; // Cada 2 segundos

const container = document.getElementById('cp-container');
const lastUpdatedSpan = document.getElementById('last-updated');
const connectionStatusSpan = document.getElementById('connection-status');

function getStatusClass(estado) {
    if (!estado) return 'bg-default';
    const s = estado.toUpperCase();
    if (s === 'ACTIVADO') return 'bg-activado';
    if (s === 'SUMINISTRANDO') return 'bg-suministrando';
    if (s === 'PARADO') return 'bg-parado';
    if (s === 'AVERIADO') return 'bg-averiado';
    if (s === 'DESCONECTADO') return 'bg-desconectado';
    return 'bg-default';
}

async function updateUI() {
    try {
        const response = await fetch(API_URL);
        if (!response.ok) throw new Error("Error API");
        const data = await response.json();

        const now = new Date();
        lastUpdatedSpan.textContent = now.toLocaleTimeString();
        connectionStatusSpan.textContent = "Conectado";
        connectionStatusSpan.className = "conn-ok";

        container.innerHTML = ''; 
        Object.values(data).forEach(punto => {
            const card = document.createElement('div');
            const bgClass = getStatusClass(punto.estado);
            card.className = `cp-card ${bgClass}`;

            card.onclick = () => {
                alert(`ID: ${punto.id}`);
            };

            // Datos SIEMPRE visibles
            let htmlContent = `
                <div class="cp-status">${punto.estado || 'DESCONOCIDO'}</div>
                <div class="cp-id">ID: ${punto.id}</div>
                <div class="cp-info">${punto.location}</div>
                <div class="cp-info">${punto.price} €/kWh</div>
            `;

            // Datos SOLO visibles si SUMINISTRANDO
            if (punto.estado && punto.estado.toUpperCase() === 'SUMINISTRANDO') {
                const kwh = parseFloat(punto.kwh || 0);
                const precio = parseFloat(punto.price || 0);
                const ticket = (kwh * precio).toFixed(2);

                htmlContent += `
                    <div class="cp-info" style="margin-top: 5px;">Driver: ${punto.driver || 'N/A'}</div>
                    <div class="cp-info">Consumo: ${kwh.toFixed(2)} kWh</div>
                    <div class="cp-info">Importe: ${ticket} €</div>
                `;
            }

            card.innerHTML = htmlContent;
            container.appendChild(card);
        });

    } catch (error) {
        console.error("Error fetching data:", error);
        connectionStatusSpan.textContent = "Error de Conexión";
        connectionStatusSpan.className = "conn-error"; // Color rojo
    }
}

document.addEventListener('DOMContentLoaded', () => {
    updateUI();
    setInterval(updateUI, REFRESH_INTERVAL);
});
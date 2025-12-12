const API_URL = 'http://localhost:7500/charging_points';
const REFRESH_INTERVAL = 2000; 

const container = document.getElementById('cp-container');
const logOngoingBody = document.getElementById('log-ongoing-body');
const ongoingPlaceholder = document.getElementById('ongoing-placeholder');
const logApp = document.getElementById('log-app');
const appPlaceholder = document.getElementById('app-placeholder');
const lastUpdatedSpan = document.getElementById('last-updated');
const connectionStatusSpan = document.getElementById('connection-status');

function getDateParts(timestamp) {
    if (!timestamp) return { date: '-', time: '-' };
    const dateObj = new Date(timestamp * 1000);
    const pad = (n) => String(n).padStart(2, '0');
    
    return {
        date: `${pad(dateObj.getDate())}/${pad(dateObj.getMonth() + 1)}/${dateObj.getFullYear()}`,
        time: `${pad(dateObj.getHours())}:${pad(dateObj.getMinutes())}:${pad(dateObj.getSeconds())}`
    };
}

function getStatusClass(estado) {
    if (!estado) return 'bg-default';
    const s = estado.toUpperCase();
    const map = {
        'ACTIVADO': 'bg-activado',
        'SUMINISTRANDO': 'bg-suministrando',
        'PARADO': 'bg-parado',
        'AVERIADO': 'bg-averiado',
        'DESCONECTADO': 'bg-desconectado'
    };
    return map[s] || 'bg-default';
}

function getCardHTML(punto) {
    let html = `
        <div class="cp-status">${punto.estado || 'DESCONOCIDO'}</div>
        <div class="cp-id">ID: ${punto.id}</div>
        <div class="cp-info">${punto.location}</div>
        <div class="cp-info">${punto.price} €/kWh</div>
    `;

    if (punto.estado && punto.estado.toUpperCase() === 'SUMINISTRANDO') {
        const kwh = parseFloat(punto.kwh || 0);
        const precio = parseFloat(punto.price || 0);
        const ticket = (kwh * precio).toFixed(2);
        
        html += `
            <div class="cp-info" style="margin-top: 10px; border-top: 1px solid rgba(255,255,255,0.3); padding-top: 5px; width: 100%; text-align: center;">
                Driver: ${punto.driver || 'N/A'}<br>
                Consumo: ${kwh.toFixed(2)} kWh<br>
                Importe: ${ticket} €
            </div>
        `;
    }
    return html;
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

        const currentIds = new Set();
        Object.values(data).forEach(punto => {
            currentIds.add(String(punto.id));
            const cardId = `cp-card-${punto.id}`;
            let cardElement = document.getElementById(cardId);
            const newClass = `cp-card ${getStatusClass(punto.estado)}`;
            const newHTML = getCardHTML(punto);

            if (cardElement) {
                if (cardElement.className !== newClass) cardElement.className = newClass;
                if (cardElement.innerHTML !== newHTML) cardElement.innerHTML = newHTML;
            } else {
                cardElement = document.createElement('div');
                cardElement.id = cardId;
                cardElement.className = newClass;
                cardElement.innerHTML = newHTML;
                cardElement.onclick = () => alert(`Punto ID: ${punto.id}`);
                container.appendChild(cardElement);
            }
        });
        
        Array.from(container.children).forEach(child => {
            const idNum = child.id.replace('cp-card-', '');
            if (!currentIds.has(idNum)) container.removeChild(child);
        });

        let ongoingRows = '';
        let appMessages = '';
        let hasOngoing = false;
        let hasAlerts = false;

        Object.values(data).forEach(punto => {
            const estadoUpper = punto.estado ? punto.estado.toUpperCase() : "";

            if (estadoUpper === 'SUMINISTRANDO') {
                hasOngoing = true;
                const { date, time } = getDateParts(punto.time);
                ongoingRows += `
                    <tr>
                        <td style="font-weight: bold;">${date}</td>
                        <td>${time}</td>
                        <td><span style="background: #e8f8f5; color: #27ae60; padding: 2px 6px; border-radius: 4px; font-weight: bold;">${punto.id}</span></td>
                        <td>${punto.driver || 'N/A'}</td>
                    </tr>
                `;
            }

            if (estadoUpper === 'PARADO') {
                hasAlerts = true;
                appMessages += `
                    <div class="alert-item">
                        <span class="alert-icon">⚠️</span>
                        <div>
                            <strong>Punto [${punto.id}]</strong> está fuera de servicio (Out of Order).
                        </div>
                    </div>
                `;
            }
        });

        if (logOngoingBody.innerHTML !== ongoingRows) logOngoingBody.innerHTML = ongoingRows;
        ongoingPlaceholder.style.display = hasOngoing ? 'none' : 'block';

        if (logApp.innerHTML !== appMessages) logApp.innerHTML = appMessages;
        appPlaceholder.style.display = hasAlerts ? 'none' : 'block';


    } catch (error) {
        console.error("Error fetching data:", error);
        connectionStatusSpan.textContent = "Sin Conexión";
        connectionStatusSpan.className = "conn-error";
    }
}

document.addEventListener('DOMContentLoaded', () => {
    updateUI();
    setInterval(updateUI, REFRESH_INTERVAL);
});
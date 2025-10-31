@echo off
setlocal enabledelayedexpansion

REM ===== Cargar variable HOST_IP desde .env =====
for /f "usebackq tokens=1,2 delims==" %%a in (".env") do (
    if "%%a"=="HOST_IP" set HOST_IP=%%b
)

REM Mostrar valor cargado para verificar
echo HOST_IP cargado: %HOST_IP%
if "%HOST_IP%"=="" (
    echo [ERROR] No se encontro HOST_IP en el archivo .env
    pause
    exit /b
)

REM ===== Instalar dependencias =====
echo Instalando dependencias...
pip install -r requirements.txt

REM ===== Pedir ID =====
set /p ID=Introduce el ID (por ejemplo MAD1): 
if "%ID%"=="" set ID=MAD1

REM ===== Ejecutar los scripts en ventanas separadas =====
echo Iniciando ev_cp_e.py en una nueva ventana...
start "EV_CP_E" cmd /k "python ev_cp/ev_cp_e.py %ID% --broker %HOST_IP%:9092"

echo Iniciando ev_cp_m.py en una nueva ventana...
start "EV_CP_M" cmd /k "python ev_cp/ev_cp_m.py %ID% --central %HOST_IP%:6000"

echo Todo iniciado correctamente.

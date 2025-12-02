@echo off
cd ..
setlocal enabledelayedexpansion

REM ===== Cargar variable CENTRAL_IP desde .env =====
for /f "usebackq tokens=1,2 delims==" %%a in (".env") do (
    if "%%a"=="CENTRAL_IP" set CENTRAL_IP=%%b
)

REM Mostrar valor cargado para verificar
echo CENTRAL_IP cargado: %CENTRAL_IP%
if "%CENTRAL_IP%"=="" (
    echo [ERROR] No se encontro CENTRAL_IP en el archivo .env
    pause
    exit /b
)

REM ===== Instalar dependencias =====
echo Instalando dependencias...
pip install -r requirements.txt

REM ===== Pedir ID =====
set /p ID=Introduce el ID (por ejemplo MAD1): 
if "%ID%"=="" set ID=MAD1

set /p PORT=Introduce el puerto de escucha (por defecto 6001): 
if "%PORT%"=="" set PORT=6001

REM ===== Ejecutar los scripts en ventanas separadas =====
echo Iniciando ev_cp_e.py en una nueva ventana...
start "EV_CP_E" cmd /k "python ev_cp/ev_cp_e.py %ID% --broker %CENTRAL_IP%:9092 --port %PORT%"

echo Iniciando ev_cp_m.py en una nueva ventana...
start "EV_CP_M" cmd /k "python ev_cp/ev_cp_m.py %ID% --central %CENTRAL_IP%:6000 --engine localhost:%PORT%"

echo Todo iniciado correctamente.

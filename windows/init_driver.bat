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
set /p ID=Introduce el ID (por ejemplo DRI1): 
if "%ID%"=="" set ID=DRI1

set /p FILE=Introduce el nombre del archivo (por ejemplo data.json): 
if "%FILE%"=="" set FILE=data.json

REM ===== Ejecutar los scripts en ventanas separadas =====
if not "%FILE%"=="" (
    echo Iniciando ev_driver.py con archivo en una nueva ventana...
    start "EV_DRIVER" cmd /k "python ev_driver/ev_driver.py %ID% --broker %CENTRAL_IP%:9092 --file %FILE%"
) else (
    echo Iniciando ev_driver.py sin archivo en una nueva ventana...
    start "EV_DRIVER" cmd /k "python ev_driver/ev_driver.py %ID% --broker %CENTRAL_IP%:9092"
)

echo Todo iniciado correctamente.
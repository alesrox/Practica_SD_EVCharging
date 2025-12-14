@echo off
cd ..
setlocal enabledelayedexpansion

REM =========================================================
REM                 CONFIGURACIÓN DE EJECUCIÓN
REM =========================================================

set PYTHON_CMD=python
set CP_M_TITLE="EV_CP_M_Engine"
set CP_E_TITLE="EV_CP_E_Broker Listener"

REM ===== 1. Cargar variable CENTRAL_IP desde .env =====
echo.
echo Cargando configuracion de .env...
if not exist .env (
    echo [ERROR] No se encontro el archivo .env.
    pause
    exit /b
)

set CENTRAL_IP=
for /f "usebackq tokens=1,2 delims==" %%a in (".env") do (
    if "%%a"=="CENTRAL_IP" set CENTRAL_IP=%%b
)

REM Validar si CENTRAL_IP fue cargado
if "%CENTRAL_IP%"=="" (
    echo [ERROR] No se encontro CENTRAL_IP en el archivo .env.
    pause
    exit /b
)
echo CENTRAL_IP cargado: %CENTRAL_IP%

REM ===== 2. Instalar dependencias =====
echo.
echo Instalando dependencias...
pip install -r requirements.txt
cls

REM ===== 3. Pedir ID y PORT =====
echo.
set /p ID=Introduce el ID (por ejemplo MAD1) [MAD1]: 
if "%ID%"=="" set ID=MAD1

set /p PORT=Introduce el puerto de escucha [6001]: 
if "%PORT%"=="" set PORT=6001

echo.
echo Usando:
echo   ID: !ID!
echo   PORT: !PORT!
echo   CENTRAL_IP: !CENTRAL_IP!
echo.

REM ===== 4. Ejecutar scripts en ventanas separadas/actual =====

REM Ejecutar ev_cp_m.py en una nueva ventana con todos los parametros
echo.
echo Iniciando ev_cp_m.py en una nueva ventana...
start "%CP_M_TITLE%" cmd /k "!PYTHON_CMD! ev_cp\ev_cp_m.py !ID! --central !CENTRAL_IP!:6000 --engine 127.0.0.1:!PORT! --registry !CENTRAL_IP!:8000"

REM Ejecutar ev_cp_e.py en la ventana actual (como hace el SH por defecto)
echo.
!PYTHON_CMD! ev_cp\ev_cp_e.py !ID! --broker !CENTRAL_IP!:9092 --port !PORT!

REM El script se detendra cuando ev_cp_e.py termine/sea cerrado
echo.
echo El proceso ev_cp_e.py ha finalizado.
pause
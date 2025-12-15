@echo off
cd ..
setlocal enabledelayedexpansion

REM =========================================================
REM                 CONFIGURACIÓN DE EJECUCIÓN
REM =========================================================

set PYTHON_CMD=python

REM ===== 1. Cargar variable REGISTRY_IP desde .env =====
echo.
echo Cargando configuracion de .env...

if not exist .env (
    echo [ERROR] No se encontro el archivo .env en el directorio raiz.
    echo Asegurate de ejecutar este script desde la carpeta windows.
    pause
    exit /b
)

set REGISTRY_IP=
REM CORRECCION: Se quito "usebackq" para usar comillas simples estandar para comandos
for /f "tokens=1,2 delims==" %%a in ('findstr /B "REGISTRY_IP=" .env') do (
    set REGISTRY_IP=%%b
)

REM Validar si REGISTRY_IP fue cargado
if "%REGISTRY_IP%"=="" (
    echo [ERROR] No se encontro REGISTRY_IP en el archivo .env.
    pause
    exit /b
)
echo REGISTRY_IP cargado: %REGISTRY_IP%

REM ===== 2. Instalar dependencias =====
echo.
echo Instalando dependencias...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo [ERROR] Fallo la instalacion de dependencias.
    pause
    exit /b
)
cls

REM ===== 3. Ejecutar el script principal =====
echo.
echo Iniciando EV Registry (ev_registry.py) en esta ventana...
echo Usando Python: %PYTHON_CMD%
echo.

REM Ejecutar ev_registry.py.
%PYTHON_CMD% ev_registry/ev_registry.py

REM Si el script de Python falla, esto mantendrá la ventana abierta para ver el error
echo.
echo El proceso ev_registry.py ha finalizado.
pause
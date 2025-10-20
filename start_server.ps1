# PowerShell скрипт для запуску сервера
Write-Host "Активація віртуального середовища та запуск сервера..." -ForegroundColor Green

# Активація venv
& "venv\Scripts\Activate.ps1"

# Запуск сервера
python run_server.py

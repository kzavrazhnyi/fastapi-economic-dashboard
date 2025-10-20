@echo off
echo Активація віртуального середовища та запуск сервера...
call venv\Scripts\activate.bat
python run_server.py
pause

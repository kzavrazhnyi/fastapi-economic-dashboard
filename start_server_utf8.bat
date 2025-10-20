@echo off
chcp 65001 >nul
echo 🚀 Запуск FastAPI сервера з економетричними даними...
echo 📊 Доступ до дашборду: http://localhost:8000
echo 📚 API документація: http://localhost:8000/docs
echo 🔄 Для зупинки сервера натисніть Ctrl+C
echo --------------------------------------------------
call venv\Scripts\activate.bat
python run_server.py
pause

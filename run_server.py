#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для запуску FastAPI сервера з економетричними даними
"""

import uvicorn
import sys
import os

# Налаштування кодування для Windows (безпечний спосіб)
if sys.platform == "win32":
    try:
        import locale
        locale.setlocale(locale.LC_ALL, 'uk_UA.UTF-8')
    except:
        pass

# Додаємо поточну директорію до Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    import os
    
    # Отримуємо порт з змінної середовища (для Render) або використовуємо 8000
    port = int(os.environ.get("PORT", 8000))
    
    print("🚀 Запуск FastAPI сервера з економетричними даними...")
    print(f"📊 Доступ до дашборду: http://localhost:{port}")
    print(f"📚 API документація: http://localhost:{port}/docs")
    print("🔄 Для зупинки сервера натисніть Ctrl+C")
    print("-" * 50)
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,  # Вимкнути reload для продакшену
        log_level="info"
    )

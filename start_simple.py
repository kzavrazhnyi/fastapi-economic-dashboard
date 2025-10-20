# -*- coding: utf-8 -*-
"""
Простий скрипт запуску сервера без складного кодування
"""

import os
import sys

# Встановлюємо змінну середовища для кодування
os.environ['PYTHONIOENCODING'] = 'utf-8'

# Запускаємо сервер через venv
if sys.platform == "win32":
    os.system('venv\\Scripts\\python.exe run_server.py')
else:
    os.system('python run_server.py')

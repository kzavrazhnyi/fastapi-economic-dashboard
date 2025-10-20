# -*- coding: utf-8 -*-
"""
Альтернативний скрипт запуску сервера з правильним кодуванням UTF-8
"""

import os
import sys
import subprocess

# Встановлюємо кодування UTF-8 для Windows
if sys.platform == "win32":
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    
    # Альтернативний спосіб для PowerShell
    try:
        subprocess.run([
            'powershell', '-Command', 
            '[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; '
            'venv\\Scripts\\python.exe run_server.py'
        ], check=True)
    except subprocess.CalledProcessError:
        # Якщо PowerShell не працює, використовуємо звичайний запуск
        os.system('venv\\Scripts\\python.exe run_server.py')
else:
    os.system('python run_server.py')

# FastAPI Demo Economic Data

Проєкт для генерації демо-даних з економетричної статистики для торгівельно-виробничого підприємства з інтерактивною веб-візуалізацією.

## Встановлення та запуск

### 1. Активація віртуального середовища
```powershell
venv\Scripts\Activate.ps1
```

### 2. Встановлення залежностей
```powershell
pip install -r requirements.txt
```

### 3. Запуск сервера
```powershell
# Варіант 1: Простий запуск (рекомендовано)
python start_simple.py

# Варіант 2: Прямий запуск через venv
venv\Scripts\python.exe run_server.py

# Варіант 3: Через batch файл з UTF-8
start_server_utf8.bat

# Варіант 4: Через PowerShell скрипт
powershell -ExecutionPolicy Bypass -File start_server.ps1

# Варіант 5: Прямий запуск uvicorn
venv\Scripts\python.exe -m uvicorn app.main:app --reload
```

### 4. Доступ до додатку
- Веб-інтерфейс: http://localhost:8000
- API документація: http://localhost:8000/docs
- Альтернативна документація: http://localhost:8000/redoc

## Структура проєкту

```
aicursoragent/
├── venv/                    # Віртуальне середовище
├── app/
│   ├── __init__.py         # Python пакет
│   ├── main.py             # FastAPI додаток
│   ├── data_generator.py   # Генератор демо-даних
│   ├── models.py           # Моделі даних
│   └── static/
│       └── index.html      # Веб-інтерфейс
├── data/                   # Згенеровані CSV файли
├── requirements.txt        # Залежності Python
├── run_server.py          # Скрипт запуску сервера
├── start_simple.py        # Простий скрипт запуску
├── start_server_utf8.bat  # Batch файл з UTF-8
├── start_server.ps1       # PowerShell скрипт
├── .gitignore             # Git ignore файл
└── README.md              # Документація
```

## Функціональність

- **Генерація демо-даних**: продажі, запаси, собівартість, прибуток
- **REST API**: ендпоінти для отримання статистичних даних
- **Інтерактивна візуалізація**: графіки та діаграми з Plotly.js
- **Дашборд**: KPI метрики та аналітика в реальному часі
- **Перегляд файлів**: інтерактивний перегляд CSV файлів з пагінацією
- **Статистика файлів**: детальний аналіз структури та змісту даних

## API Ендпоінти

- `GET /` - головна сторінка з візуалізацією
- `GET /api/sales` - дані про продажі (з фільтрами за датою, категорією, регіоном)
- `GET /api/inventory` - дані про запаси (з фільтрами за категорією, низькими запасами)
- `GET /api/profit` - дані про прибутковість (з фільтрами за категорією, мінімальною маржею)
- `GET /api/trends` - часові ряди (з агрегацією за періодом)
- `GET /api/stats` - загальна статистика (KPI метрики)
- `GET /api/categories` - список категорій продуктів
- `GET /api/regions` - список регіонів продажу
- `GET /api/files` - список файлів даних
- `GET /api/files/{filename}` - вміст CSV файлу з пагінацією
- `GET /api/files/{filename}/stats` - статистика CSV файлу
- `POST /api/regenerate` - регенерація всіх даних

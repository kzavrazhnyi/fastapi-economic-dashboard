# -*- coding: utf-8 -*-
"""
FastAPI додаток для економетричної статистики торгівельно-виробничого підприємства
"""

import sys
import os
import numpy as np

# Налаштування кодування для Windows (безпечний спосіб)
if sys.platform == "win32":
    try:
        import locale
        locale.setlocale(locale.LC_ALL, 'uk_UA.UTF-8')
    except:
        pass

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool
from typing import List, Optional
from datetime import datetime, date
import pandas as pd
try:
    # Спробуємо відносні імпорти (коли запускається як модуль)
    from .models import SalesData, InventoryData, ProfitData, TrendData, StatsData
    from .data_generator import DataGenerator
    from .worldbank_client import WorldBankDataProvider
    from .crypto_client import CryptoDataProvider
except ImportError:
    # Якщо не працює, використовуємо абсолютні імпорти (коли запускається як скрипт)
    from models import SalesData, InventoryData, ProfitData, TrendData, StatsData
    from data_generator import DataGenerator
    from worldbank_client import WorldBankDataProvider
    from crypto_client import CryptoDataProvider

# Ініціалізація FastAPI додатку
app = FastAPI(
    title="Economic Data Dashboard",
    description="API для економетричної статистики торгівельно-виробничого підприємства",
    version="1.0.0"
)

# Додаємо CORS middleware для підтримки фронтенду
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Дозволяє всі джерела
    allow_credentials=True,
    allow_methods=["*"],  # Дозволяє всі методи (GET, POST, etc.)
    allow_headers=["*"],  # Дозволяє всі заголовки
)

# Підключення статичних файлів
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Глобальна змінна для кешування даних
cached_data = None

# Глобальний екземпляр крипто провайдера для кешування
crypto_provider = None

# Глобальний екземпляр World Bank провайдера для кешування
worldbank_provider = None

# Змінна для зберігання серверних логів
server_logs = []


def get_crypto_provider():
    """Отримання глобального екземпляра крипто провайдера"""
    global crypto_provider
    if crypto_provider is None:
        crypto_provider = CryptoDataProvider()
    return crypto_provider

def get_worldbank_provider():
    """Отримання глобального екземпляра World Bank провайдера"""
    global worldbank_provider
    if worldbank_provider is None:
        worldbank_provider = WorldBankDataProvider()
    return worldbank_provider

def add_server_log(log_type: str, message: str, details: dict = None):
    """Додавання логу до серверних логів"""
    global server_logs
    import datetime
    
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "type": log_type,
        "message": message,
        "details": details or {}
    }
    
    server_logs.append(log_entry)
    
    # Обмежуємо кількість логів до 100 записів
    if len(server_logs) > 100:
        server_logs = server_logs[-100:]

def load_data():
    """Завантаження даних з CSV файлів або генерація нових"""
    global cached_data
    
    # Тимчасово очищуємо кеш для тестування
    cached_data = None
    
    if cached_data is not None:
        return cached_data
    
    data_dir = "data"
    
    # Перевіряємо чи існують CSV файли
    required_files = ["sales.csv", "inventory.csv", "profit.csv", "trends.csv", "stats.csv"]
    files_exist = all(os.path.exists(os.path.join(data_dir, file)) for file in required_files)
    
    if not files_exist:
        print("CSV файли не знайдено, генеруємо нові дані...")
        generator = DataGenerator()
        cached_data = generator.generate_all_data()
    else:
        print("Завантажуємо дані з CSV файлів...")
        try:
            cached_data = {
                "sales": pd.read_csv(os.path.join(data_dir, "sales.csv"), encoding='utf-8').to_dict('records'),
                "inventory": pd.read_csv(os.path.join(data_dir, "inventory.csv"), encoding='utf-8').to_dict('records'),
                "profit": pd.read_csv(os.path.join(data_dir, "profit.csv"), encoding='utf-8').to_dict('records'),
                "trends": pd.read_csv(os.path.join(data_dir, "trends.csv"), encoding='utf-8').to_dict('records'),
                "stats": pd.read_csv(os.path.join(data_dir, "stats.csv"), encoding='utf-8').to_dict('records')
            }
        except Exception as e:
            print(f"Помилка завантаження CSV файлів: {e}")
            print("Генеруємо нові дані...")
            generator = DataGenerator()
            cached_data = generator.generate_all_data()
    
    return cached_data


@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Головна сторінка з інтерактивним дашбордом"""
    try:
        with open("app/static/index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Файл index.html не знайдено</h1>")

@app.get("/docs-page", response_class=HTMLResponse)
async def read_docs():
    """Сторінка технічної документації"""
    try:
        with open("app/static/docs.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Файл docs.html не знайдено</h1>")


@app.get("/api/sales")
async def get_sales(
    start_date: Optional[str] = Query(None, description="Початкова дата (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Кінцева дата (YYYY-MM-DD)"),
    category: Optional[str] = Query(None, description="Категорія продукту"),
    region: Optional[str] = Query(None, description="Регіон продажу"),
    limit: int = Query(1000, description="Максимальна кількість записів")
):
    """Отримання даних про продажі з можливістю фільтрації"""
    try:
        print(f"🔍 Sales API запит: start_date={start_date}, end_date={end_date}, category={category}, region={region}, limit={limit}")
        
        data = load_data()
        sales_df = pd.DataFrame(data["sales"])
        
        print(f"📊 Sales DataFrame shape: {sales_df.shape}")
        print(f"📊 Sales DataFrame columns: {list(sales_df.columns)}")
        
        # Конвертуємо дату в datetime для правильної фільтрації
        if 'date' in sales_df.columns:
            sales_df['date'] = pd.to_datetime(sales_df['date'])
            print(f"📅 Date column converted to datetime")
        
        # Фільтрація за датою
        if start_date:
            start_dt = pd.to_datetime(start_date)
            sales_df = sales_df[sales_df['date'] >= start_dt]
            print(f"📅 Filtered by start_date: {start_date}, remaining rows: {len(sales_df)}")
        
        if end_date:
            end_dt = pd.to_datetime(end_date)
            sales_df = sales_df[sales_df['date'] <= end_dt]
            print(f"📅 Filtered by end_date: {end_date}, remaining rows: {len(sales_df)}")
        
        # Фільтрація за категорією
        if category:
            sales_df = sales_df[sales_df['category'] == category]
            print(f"📂 Filtered by category: {category}, remaining rows: {len(sales_df)}")
        
        # Фільтрація за регіоном
        if region:
            sales_df = sales_df[sales_df['region'] == region]
            print(f"🌍 Filtered by region: {region}, remaining rows: {len(sales_df)}")
        
        # Обмеження кількості записів
        sales_df = sales_df.head(limit)
        
        # Конвертуємо дату назад в рядок для JSON серіалізації
        if 'date' in sales_df.columns:
            sales_df['date'] = sales_df['date'].dt.strftime('%Y-%m-%d')
        
        result = sales_df.to_dict('records')
        print(f"✅ Sales API повертає {len(result)} записів")
        return result
    
    except Exception as e:
        print(f"❌ Помилка Sales API: {str(e)}")
        print(f"❌ Тип помилки: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні даних про продажі: {str(e)}")


@app.get("/api/inventory")
async def get_inventory(
    category: Optional[str] = Query(None, description="Категорія продукту"),
    low_stock: bool = Query(False, description="Показати тільки товари з низькими запасами")
):
    """Отримання даних про запаси"""
    try:
        data = load_data()
        inventory_df = pd.DataFrame(data["inventory"])
        
        # Фільтрація за категорією
        if category:
            inventory_df = inventory_df[inventory_df['category'] == category]
        
        # Фільтрація за низькими запасами
        if low_stock:
            inventory_df = inventory_df[inventory_df['current_stock'] <= inventory_df['min_stock']]
        
        return inventory_df.to_dict('records')
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні даних про запаси: {str(e)}")


@app.get("/api/profit")
async def get_profit(
    category: Optional[str] = Query(None, description="Категорія продукту"),
    min_margin: Optional[float] = Query(None, description="Мінімальна маржа прибутку (%)")
):
    """Отримання даних про прибутковість"""
    try:
        data = load_data()
        profit_df = pd.DataFrame(data["profit"])
        
        # Фільтрація за категорією
        if category:
            profit_df = profit_df[profit_df['category'] == category]
        
        # Фільтрація за мінімальною маржею
        if min_margin is not None:
            profit_df = profit_df[profit_df['profit_percentage'] >= min_margin]
        
        return profit_df.to_dict('records')
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні даних про прибутковість: {str(e)}")


@app.get("/api/trends")
async def get_trends(
    start_date: Optional[str] = Query(None, description="Початкова дата (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Кінцева дата (YYYY-MM-DD)"),
    period: str = Query("daily", description="Період агрегації: daily, weekly, monthly")
):
    """Отримання часових рядів для аналізу трендів"""
    try:
        print(f"🔍 Trends API запит: start_date={start_date}, end_date={end_date}, period={period}")
        
        data = load_data()
        trends_data = data["trends"]
        
        print(f"📊 Trends data type: {type(trends_data)}, length: {len(trends_data) if hasattr(trends_data, '__len__') else 'N/A'}")
        
        # Якщо це список словників
        if isinstance(trends_data, list):
            trends_df = pd.DataFrame(trends_data)
        else:
            trends_df = trends_data
        
        print(f"📈 DataFrame shape: {trends_df.shape}")
        print(f"📈 DataFrame columns: {list(trends_df.columns)}")
        
        # Конвертуємо дату
        trends_df['date'] = pd.to_datetime(trends_df['date'])
        
        # Фільтрація за датою
        if start_date:
            trends_df = trends_df[trends_df['date'] >= start_date]
        if end_date:
            trends_df = trends_df[trends_df['date'] <= end_date]
        
        # Агрегація за періодом
        if period == "weekly":
            trends_df = trends_df.groupby(trends_df['date'].dt.to_period('W')).agg({
                'total_revenue': 'sum',
                'total_profit': 'sum',
                'total_sales': 'sum',
                'avg_order_value': 'mean'
            }).reset_index()
            trends_df['date'] = trends_df['date'].dt.start_time
        
        elif period == "monthly":
            trends_df = trends_df.groupby(trends_df['date'].dt.to_period('M')).agg({
                'total_revenue': 'sum',
                'total_profit': 'sum',
                'total_sales': 'sum',
                'avg_order_value': 'mean'
            }).reset_index()
            trends_df['date'] = trends_df['date'].dt.start_time
        
        result = trends_df.to_dict('records')
        print(f"✅ Trends API повертає {len(result)} записів")
        return result
    
    except Exception as e:
        print(f"❌ Помилка Trends API: {str(e)}")
        print(f"❌ Тип помилки: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні трендів: {str(e)}")


@app.get("/api/stats")
async def get_stats():
    """Отримання загальної статистики (KPI метрики)"""
    try:
        print("🔍 Stats API запит")
        data = load_data()
        stats = data["stats"]
        
        print(f"📊 Stats data type: {type(stats)}")
        print(f"📊 Stats data length: {len(stats) if hasattr(stats, '__len__') else 'N/A'}")
        print(f"📊 Stats data content: {stats}")
        
        # Якщо це список словників, беремо перший
        if isinstance(stats, list) and len(stats) > 0:
            result = stats[0]
            print(f"✅ Stats API повертає перший елемент списку: {result}")
            return result
        # Якщо це словник, повертаємо як є
        elif isinstance(stats, dict):
            print(f"✅ Stats API повертає словник: {stats}")
            return stats
        else:
            print(f"⚠️ Stats API повертає порожній об'єкт, тип: {type(stats)}")
            return {}
    
    except Exception as e:
        print(f"❌ Помилка Stats API: {str(e)}")
        print(f"❌ Тип помилки: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні статистики: {str(e)}")


@app.get("/api/categories")
async def get_categories():
    """Отримання списку доступних категорій"""
    try:
        data = load_data()
        sales_data = data["sales"]
        
        # Якщо це список словників
        if isinstance(sales_data, list):
            categories = list(set(item['category'] for item in sales_data if 'category' in item))
        # Якщо це DataFrame
        else:
            sales_df = pd.DataFrame(sales_data)
            categories = sales_df['category'].unique().tolist()
        
        return {"categories": categories}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні категорій: {str(e)}")


@app.get("/api/regions")
async def get_regions():
    """Отримання списку доступних регіонів"""
    try:
        data = load_data()
        sales_data = data["sales"]
        
        # Якщо це список словників
        if isinstance(sales_data, list):
            regions = list(set(item['region'] for item in sales_data if 'region' in item))
        # Якщо це DataFrame
        else:
            sales_df = pd.DataFrame(sales_data)
            regions = sales_df['region'].unique().tolist()
        
        return {"regions": regions}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні регіонів: {str(e)}")


@app.get("/api/files")
async def get_data_files():
    """Отримання списку файлів даних"""
    try:
        data_dir = "data"
        files_info = []
        
        if os.path.exists(data_dir):
            for filename in os.listdir(data_dir):
                if filename.endswith('.csv'):
                    filepath = os.path.join(data_dir, filename)
                    file_size = os.path.getsize(filepath)
                    file_mtime = os.path.getmtime(filepath)
                    
                    files_info.append({
                        "name": filename,
                        "size": file_size,
                        "modified": datetime.fromtimestamp(file_mtime).isoformat(),
                        "path": f"/api/files/{filename}"
                    })
        
        return {"files": files_info}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при отриманні списку файлів: {str(e)}")


@app.get("/api/files/{filename}")
async def get_file_content(
    filename: str,
    limit: int = Query(100, description="Максимальна кількість рядків"),
    offset: int = Query(0, description="Зміщення для пагінації")
):
    """Отримання вмісту CSV файлу"""
    try:
        data_dir = "data"
        filepath = os.path.join(data_dir, filename)
        
        if not os.path.exists(filepath) or not filename.endswith('.csv'):
            raise HTTPException(status_code=404, detail="Файл не знайдено")
        
        # Читаємо CSV файл
        df = pd.read_csv(filepath, encoding='utf-8')
        
        # Застосовуємо пагінацію
        total_rows = len(df)
        df_paginated = df.iloc[offset:offset + limit]
        
        return {
            "filename": filename,
            "total_rows": total_rows,
            "offset": offset,
            "limit": limit,
            "data": df_paginated.to_dict('records'),
            "columns": df.columns.tolist()
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при читанні файлу: {str(e)}")


@app.get("/api/files/{filename}/stats")
async def get_file_stats(filename: str):
    """Отримання статистики CSV файлу"""
    try:
        data_dir = "data"
        filepath = os.path.join(data_dir, filename)
        
        if not os.path.exists(filepath) or not filename.endswith('.csv'):
            raise HTTPException(status_code=404, detail="Файл не знайдено")
        
        df = pd.read_csv(filepath, encoding='utf-8')
        
        stats = {
            "filename": filename,
            "total_rows": int(len(df)),
            "total_columns": int(len(df.columns)),
            "columns": df.columns.tolist(),
            "data_types": df.dtypes.astype(str).to_dict(),
            "memory_usage": int(df.memory_usage(deep=True).sum()),
            "null_counts": {k: int(v) for k, v in df.isnull().sum().to_dict().items()}
        }
        
        # Додаємо статистику для числових колонок
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        if len(numeric_columns) > 0:
            numeric_stats = df[numeric_columns].describe()
            # Конвертуємо NumPy типи в стандартні Python типи
            stats["numeric_stats"] = {}
            for col in numeric_stats.columns:
                stats["numeric_stats"][col] = {}
                for stat_name in numeric_stats.index:
                    value = numeric_stats.loc[stat_name, col]
                    # Конвертуємо NumPy типи в стандартні Python типи
                    if isinstance(value, (np.integer, np.int64, np.int32)):
                        stats["numeric_stats"][col][stat_name] = int(value)
                    elif isinstance(value, (np.floating, np.float64, np.float32)):
                        stats["numeric_stats"][col][stat_name] = float(value)
                    else:
                        stats["numeric_stats"][col][stat_name] = str(value)
        
        return stats
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при аналізі файлу: {str(e)}")


@app.post("/api/regenerate")
async def regenerate_data():
    """Примусова регенерація всіх даних"""
    try:
        global cached_data
        cached_data = None
        
        generator = DataGenerator()
        cached_data = generator.generate_all_data()
        
        return {"message": "Дані успішно регенеровано"}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при регенерації даних: {str(e)}")


# Ендпоінти для роботи з даними Світового банку
@app.get("/api/worldbank/indicators")
async def get_worldbank_indicators(
    countries: Optional[str] = Query(None, description="Коди країн через кому (UA,US,DE)"),
    indicators: Optional[str] = Query(None, description="Показники через кому (GDP,GDP_PER_CAPITA,INFLATION)"),
    start_year: int = Query(2020, description="Початковий рік"),
    end_year: int = Query(2023, description="Кінцевий рік")
):
    """Отримання економетричних показників зі Світового банку"""
    try:
        provider = get_worldbank_provider()
        
        country_list = countries.split(',') if countries else None
        indicator_list = indicators.split(',') if indicators else None
        
        # Додаємо лог запиту
        add_server_log("request", "World Bank API запит", {
            "countries": country_list,
            "indicators": indicator_list,
            "start_year": start_year,
            "end_year": end_year
        })
        
        data = await run_in_threadpool(
            provider.get_economic_indicators,
            country_codes=country_list,
            indicators=indicator_list,
            start_year=start_year,
            end_year=end_year
        )
        
        # --- ПОЧАТОК ВИПРАВЛЕННЯ ---

        # Якщо дані порожні, повертаємо порожню структуру
        if data is None or data.empty:
            return {
                "data": [], "columns": [], "total_records": 0,
                "countries": [], "years": []
            }

        # Замінюємо NaN/NaT на None для коректної JSON-серіалізації
        safe_data_df = data.where(pd.notnull(data), None)

        # 1. Конвертуємо основні дані (list of dicts)
        # safe_data_df.to_dict('records') - ЗАЛИШАЄ NUMPY ТИПИ!
        # Конвертуємо вручну:
        data_records = []
        for _, row in safe_data_df.iterrows():
            record = {}
            for col in safe_data_df.columns:
                value = row[col]
                # Явно конвертуємо типи NumPy в стандартні типи Python
                if isinstance(value, (np.integer, np.int64, np.int32)):
                    record[col] = int(value)
                elif isinstance(value, (np.floating, np.float64, np.float32)):
                    record[col] = float(value)
                elif pd.isna(value) or value is None:
                    record[col] = None
                else:
                    record[col] = str(value)
            data_records.append(record)
        
        # 2. Конвертуємо список країн (щоб уникнути NumPy рядків)
        countries_list = []
        if 'Country_Name' in safe_data_df.columns:
            countries_list = [str(c) for c in safe_data_df['Country_Name'].unique() if c is not None]

        # 3. Конвертуємо список років (щоб уникнути NumPy чисел)
        years_list = []
        if 'Year' in safe_data_df.columns:
            raw_years = [y for y in safe_data_df['Year'].unique() if y is not None]
            # Конвертуємо в int, щоб правильно сортувати
            int_years = sorted([int(y) for y in raw_years])
            years_list = int_years # FastAPI може серіалізувати звичайний int

        # Отримуємо час останнього оновлення
        params = {
            'country_codes': country_list,
            'indicators': indicator_list,
            'start_year': start_year,
            'end_year': end_year
        }
        last_update = provider.get_last_update_time('economic_indicators', params)

        return {
            "data": data_records,
            "columns": safe_data_df.columns.tolist(),
            "last_update": last_update,
            "total_records": len(data_records),
            "countries": countries_list,
            "years": years_list
        }
        
        # --- КІНЕЦЬ ВИПРАВЛЕННЯ ---
    
    except Exception as e:
        # Додамо логування в консоль сервера для легшого дебагу
        print(f"!!! КРИТИЧНА ПОМИЛКА в /api/worldbank/indicators: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Помилка отримання даних Світового банку: {str(e)}")

@app.get("/api/worldbank/comparison")
async def get_country_comparison(
    countries: str = Query(..., description="Коди країн через кому (UA,US,DE)"),
    indicator: str = Query("GDP_PER_CAPITA", description="Показник для порівняння"),
    years: int = Query(10, description="Кількість років для аналізу")
):
    """Порівняння країн за конкретним показником"""
    try:
        provider = WorldBankDataProvider()
        
        country_list = countries.split(',')
        
        # Валідація індикатора
        if indicator not in provider.indicators:
            raise HTTPException(
                status_code=400, 
                detail=f"Невідомий індикатор: {indicator}. Доступні: {list(provider.indicators.keys())}"
            )
        
        data = await run_in_threadpool(
            provider.get_country_comparison,
            countries=country_list,
            indicator=indicator,
            years=years
        )
        
        return {
            "data": data.to_dict('records'),
            "indicator": indicator,
            "countries": country_list,
            "analysis_years": years,
            "total_records": len(data)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка порівняння країн: {str(e)}")

@app.get("/api/worldbank/trends/{country}")
async def get_country_trends(
    country: str,
    indicators: Optional[str] = Query(None, description="Показники через кому (GDP,GDP_PER_CAPITA,INFLATION)"),
    years: int = Query(20, description="Кількість років для аналізу")
):
    """Аналіз трендів для конкретної країни"""
    try:
        provider = WorldBankDataProvider()
        
        indicator_list = indicators.split(',') if indicators else None
        
        analysis = await run_in_threadpool(
            provider.get_trend_analysis,
            country=country,
            indicators=indicator_list,
            years=years
        )
        
        return analysis
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка аналізу трендів: {str(e)}")

@app.get("/api/worldbank/countries")
async def get_available_countries():
    """Отримання списку доступних країн"""
    try:
        provider = get_worldbank_provider()
        return {
            "countries": provider.countries,
            "total_countries": len(provider.countries)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка отримання списку країн: {str(e)}")

@app.get("/api/server-logs")
async def get_server_logs():
    """Отримання серверних логів"""
    global server_logs
    return {"logs": server_logs}

@app.post("/api/clear-cache")
async def clear_cache():
    """Очищення кешу World Bank API"""
    try:
        provider = get_worldbank_provider()
        provider.clear_cache()
        return {"message": "Кеш очищено успішно"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка очищення кешу: {str(e)}")

@app.get("/api/worldbank/indicators-list")
async def get_available_indicators():
    """Отримання списку доступних показників"""
    try:
        provider = get_worldbank_provider()
        return {
            "indicators": provider.indicators,
            "total_indicators": len(provider.indicators)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка отримання списку показників: {str(e)}")

@app.get("/api/worldbank/normalized")
async def get_normalized_data(
    countries: Optional[str] = Query(None, description="Коди країн через кому (UA,US,DE)"),
    indicators: Optional[str] = Query(None, description="Показники через кому (GDP,GDP_PER_CAPITA,INFLATION)"),
    start_year: int = Query(2020, description="Початковий рік"),
    end_year: int = Query(2023, description="Кінцевий рік"),
    currency: str = Query("USD", description="Валюта для конвертації")
):
    """Отримання нормалізованих економічних показників"""
    try:
        provider = WorldBankDataProvider()
        
        country_list = countries.split(',') if countries else None
        indicator_list = indicators.split(',') if indicators else None
        
        # Отримуємо сирі дані
        raw_data = await run_in_threadpool(
            provider.get_economic_indicators,
            country_codes=country_list,
            indicators=indicator_list,
            start_year=start_year,
            end_year=end_year
        )
        
        # Нормалізуємо дані
        normalized_data = provider.normalize_data(raw_data)
        normalized_data = normalized_data.where(pd.notnull(normalized_data), None)
        
        # Конвертуємо валюту якщо потрібно
        if currency != "USD":
            normalized_data = provider.convert_to_usd(normalized_data, currency)
        
        # Конвертуємо DataFrame в список словників з правильним кодуванням
        data_records = []
        for _, row in normalized_data.iterrows():
            record = {}
            for col in normalized_data.columns:
                value = row[col]
                # Конвертуємо NumPy типи в стандартні Python типи
                if isinstance(value, (np.integer, np.int64, np.int32)):
                    record[col] = int(value)
                elif isinstance(value, (np.floating, np.float64, np.float32)):
                    record[col] = float(value) if value is not None else None
                elif isinstance(value, str):
                    # Переконуємося, що рядок правильно закодований
                    record[col] = value.encode('utf-8').decode('utf-8')
                else:
                    record[col] = None if value is None else str(value)
            data_records.append(record)
        
        return {
            "data": data_records,
            "columns": normalized_data.columns.tolist(),
            "total_records": len(normalized_data),
            "countries": normalized_data['Country_Name'].unique().tolist() if not normalized_data.empty else [],
            "years": sorted(normalized_data['Year'].unique().tolist()) if not normalized_data.empty else [],
            "currency": currency,
            "normalization_info": {
                "GDP": "в мільярдах доларів",
                "GDP_PER_CAPITA": "в доларах",
                "INFLATION": "відсотки",
                "UNEMPLOYMENT": "відсотки",
                "EXPORTS": "в мільярдах доларів",
                "IMPORTS": "в мільярдах доларів",
                "POPULATION": "в мільйонах осіб"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка нормалізації даних: {str(e)}")

@app.get("/api/worldbank/economic-health")
async def get_economic_health_analysis(
    countries: Optional[str] = Query(None, description="Коди країн через кому (UA,US,DE)"),
    start_year: int = Query(2020, description="Початковий рік"),
    end_year: int = Query(2023, description="Кінцевий рік")
):
    """Аналіз економічного здоров'я країн"""
    try:
        provider = WorldBankDataProvider()
        
        country_list = countries.split(',') if countries else None
        
        # Отримуємо дані для аналізу
        data = await run_in_threadpool(
            provider.get_economic_indicators,
            country_codes=country_list,
            indicators=['GDP_PER_CAPITA', 'INFLATION', 'UNEMPLOYMENT', 'LIFE_EXPECTANCY'],
            start_year=start_year,
            end_year=end_year
        )
        
        # Нормалізуємо дані
        normalized_data = provider.normalize_data(data)
        
        # Аналізуємо економічне здоров'я
        health_analysis = provider.analyze_economic_health(normalized_data)
        
        return {
            "analysis": health_analysis,
            "analysis_period": f"{start_year}-{end_year}",
            "total_countries": len(health_analysis),
            "methodology": {
                "GDP_PER_CAPITA": "30% ваги - показник економічного розвитку",
                "INFLATION": "25% ваги - стабільність цін",
                "UNEMPLOYMENT": "25% ваги - зайнятість населення",
                "LIFE_EXPECTANCY": "20% ваги - якість життя"
            }
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка аналізу економічного здоров'я: {str(e)}")

@app.get("/api/worldbank/currency-rates")
async def get_currency_rates():
    """Отримання курсів валют для конвертації"""
    try:
        provider = WorldBankDataProvider()
        rates = provider.get_currency_conversion_rates()
        
        return {
            "rates": rates,
            "base_currency": "USD",
            "note": "Курси є приблизними та можуть відрізнятися від реальних"
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка отримання курсів валют: {str(e)}")


@app.get("/api/crypto/markets")
async def get_crypto_markets(currency: str = 'usd', per_page: int = 100):
    """Отримання ринкових даних для топ криптовалют."""
    try:
        provider = get_crypto_provider()
        data = await run_in_threadpool(provider.get_market_data, currency=currency, per_page=per_page)
        
        # Отримуємо час останнього оновлення / Get last update time
        params = {
            "vs_currency": currency,
            "order": "market_cap_desc",
            "per_page": min(per_page, 100),
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h",
        }
        last_update = provider.get_last_update_time("coins/markets", params)
        
        return {
            "data": data,
            "last_update": last_update,
            "currency": currency,
            "total_coins": len(data)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/crypto/coins/{coin_id}/history")
async def get_crypto_coin_history(coin_id: str, currency: str = 'usd', days: int = 30):
    """Отримання історичних даних для графіка."""
    try:
        provider = get_crypto_provider()
        data = await run_in_threadpool(provider.get_coin_history, coin_id=coin_id, currency=currency, days=days)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/crypto/global")
async def get_crypto_global():
    """Глобальні метрики ринку криптовалют."""
    try:
        provider = get_crypto_provider()
        data = await run_in_threadpool(provider.get_global)
        
        # Отримуємо час останнього оновлення / Get last update time
        params = {}
        last_update = provider.get_last_update_time("global", params)
        
        return {
            "data": data.get('data', data),
            "last_update": last_update
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/yahoo-finance/{symbol}")
async def get_yahoo_finance_data(symbol: str):
    """Отримання даних з Yahoo Finance через проксі"""
    try:
        import requests
        
        # Yahoo Finance API URL
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d"
        
        # Заголовки для обходу блокування
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        print(f"📈 Запит до Yahoo Finance для {symbol}")
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=f"Yahoo Finance API помилка: {response.status_code}")
        
        data = response.json()
        
        if not data.get('chart') or not data['chart'].get('result') or len(data['chart']['result']) == 0:
            raise HTTPException(status_code=404, detail="Дані не знайдено")
        
        result = data['chart']['result'][0]
        meta = result['meta']
        
        # Обробляємо дані
        current_price = meta.get('regularMarketPrice', meta.get('previousClose', 0))
        previous_close = meta.get('previousClose', current_price)
        change = current_price - previous_close
        change_percent = (change / previous_close * 100) if previous_close != 0 else 0
        
        result_data = {
            "symbol": symbol,
            "currentPrice": current_price,
            "change": change,
            "changePercent": change_percent,
            "previousClose": previous_close,
            "open": meta.get('regularMarketOpen', previous_close),
            "high": meta.get('regularMarketDayHigh', current_price),
            "low": meta.get('regularMarketDayLow', current_price),
            "volume": meta.get('regularMarketVolume', 0),
            "marketCap": meta.get('marketCap', 0),
            "currency": meta.get('currency', 'USD'),
            "exchange": meta.get('exchangeName', ''),
            "timezone": meta.get('timezone', ''),
            "lastUpdate": meta.get('regularMarketTime', 0)
        }
        
        print(f"✅ Yahoo Finance дані отримано для {symbol}: ${current_price}")
        
        return result_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Помилка мережі Yahoo Finance: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Помилка мережі: {str(e)}")
    except Exception as e:
        print(f"❌ Помилка Yahoo Finance API: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Помилка сервера: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

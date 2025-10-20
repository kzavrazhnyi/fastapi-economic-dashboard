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
from .models import SalesData, InventoryData, ProfitData, TrendData, StatsData
from .data_generator import DataGenerator
from .worldbank_client import WorldBankDataProvider

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


def load_data():
    """Завантаження даних з CSV файлів або генерація нових"""
    global cached_data
    
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
                "sales": pd.read_csv(os.path.join(data_dir, "sales.csv")).to_dict('records'),
                "inventory": pd.read_csv(os.path.join(data_dir, "inventory.csv")).to_dict('records'),
                "profit": pd.read_csv(os.path.join(data_dir, "profit.csv")).to_dict('records'),
                "trends": pd.read_csv(os.path.join(data_dir, "trends.csv")).to_dict('records'),
                "stats": pd.read_csv(os.path.join(data_dir, "stats.csv")).to_dict('records')
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
        data = load_data()
        sales_df = pd.DataFrame(data["sales"])
        
        # Фільтрація за датою
        if start_date:
            sales_df = sales_df[sales_df['date'] >= start_date]
        if end_date:
            sales_df = sales_df[sales_df['date'] <= end_date]
        
        # Фільтрація за категорією
        if category:
            sales_df = sales_df[sales_df['category'] == category]
        
        # Фільтрація за регіоном
        if region:
            sales_df = sales_df[sales_df['region'] == region]
        
        # Обмеження кількості записів
        sales_df = sales_df.head(limit)
        
        return sales_df.to_dict('records')
    
    except Exception as e:
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
        data = load_data()
        trends_data = data["trends"]
        
        # Якщо це список словників
        if isinstance(trends_data, list):
            trends_df = pd.DataFrame(trends_data)
        else:
            trends_df = trends_data
        
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
        
        return trends_df.to_dict('records')
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка при завантаженні трендів: {str(e)}")


@app.get("/api/stats")
async def get_stats():
    """Отримання загальної статистики (KPI метрики)"""
    try:
        data = load_data()
        stats = data["stats"]
        
        # Якщо це список словників, беремо перший
        if isinstance(stats, list) and len(stats) > 0:
            return stats[0]
        # Якщо це словник, повертаємо як є
        elif isinstance(stats, dict):
            return stats
        else:
            return {}
    
    except Exception as e:
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
        df = pd.read_csv(filepath)
        
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
        
        df = pd.read_csv(filepath)
        
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
        provider = WorldBankDataProvider()
        
        country_list = countries.split(',') if countries else None
        indicator_list = indicators.split(',') if indicators else None
        
        data = await run_in_threadpool(
            provider.get_economic_indicators,
            country_codes=country_list,
            indicators=indicator_list,
            start_year=start_year,
            end_year=end_year
        )
        
        return {
            "data": data.to_dict('records'),
            "columns": data.columns.tolist(),
            "total_records": len(data),
            "countries": data['Country_Name'].unique().tolist() if not data.empty else [],
            "years": sorted(data['Year'].unique().tolist()) if not data.empty else []
        }
    
    except Exception as e:
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
        provider = WorldBankDataProvider()
        return {
            "countries": provider.countries,
            "total_countries": len(provider.countries)
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Помилка отримання списку країн: {str(e)}")

@app.get("/api/worldbank/indicators-list")
async def get_available_indicators():
    """Отримання списку доступних показників"""
    try:
        provider = WorldBankDataProvider()
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
                    record[col] = float(value)
                elif isinstance(value, str):
                    # Переконуємося, що рядок правильно закодований
                    record[col] = value.encode('utf-8').decode('utf-8')
                else:
                    record[col] = str(value)
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# -*- coding: utf-8 -*-
"""
Модуль для роботи з API Світового банку
"""

import wbgapi as wb
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import sys
import os

# Налаштування кодування для Windows
if sys.platform == "win32":
    try:
        import locale
        locale.setlocale(locale.LC_ALL, 'uk_UA.UTF-8')
    except:
        pass

class WorldBankDataProvider:
    """Клас для отримання економетричних даних зі Світового банку"""
    
    def __init__(self):
        self.indicators = {
            'GDP': 'NY.GDP.MKTP.CD',  # ВВП (поточні ціни, долари США)
            'GDP_PER_CAPITA': 'NY.GDP.PCAP.CD',  # ВВП на душу населення
            'INFLATION': 'FP.CPI.TOTL.ZG',  # Інфляція споживчих цін
            'UNEMPLOYMENT': 'SL.UEM.TOTL.ZS',  # Безробіття
            'EXPORTS': 'NE.EXP.GNFS.CD',  # Експорт товарів та послуг
            'IMPORTS': 'NE.IMP.GNFS.CD',  # Імпорт товарів та послуг
            'POPULATION': 'SP.POP.TOTL',  # Населення
            'LIFE_EXPECTANCY': 'SP.DYN.LE00.IN',  # Очікувана тривалість життя
            'INTERNET_USERS': 'IT.NET.USER.ZS',  # Користувачі інтернету (% населення)
            'EDUCATION_EXPENDITURE': 'SE.XPD.TOTL.GD.ZS',  # Витрати на освіту (% ВВП)
        }
        
        self.countries = {
            'UA': 'Україна',
            'US': 'США',
            'DE': 'Німеччина',
            'FR': 'Франція',
            'GB': 'Великобританія',
            'PL': 'Польща',
            'RU': 'Росія',
            'CN': 'Китай',
            'JP': 'Японія',
            'IN': 'Індія',
            'BR': 'Бразилія',
            'CA': 'Канада',
            'AU': 'Австралія',
            'IT': 'Італія',
            'ES': 'Іспанія',
        }
    
    def get_economic_indicators(self, country_codes: List[str] = None, 
                              indicators: List[str] = None, 
                              start_year: int = 2020, 
                              end_year: int = 2023) -> pd.DataFrame:
        """
        Отримати економетричні показники зі Світового банку
        
        Args:
            country_codes: Список кодів країн (за замовченням всі доступні)
            indicators: Список показників (за замовченням всі доступні)
            start_year: Початковий рік
            end_year: Кінцевий рік
        
        Returns:
            DataFrame з даними
        """
        try:
            if country_codes is None:
                country_codes = list(self.countries.keys())
            
            # Визначаємо індикатори: приймаємо як ключі (GDP, INFLATION) або вже коди (NY.GDP.MKTP.CD)
            if indicators is None:
                indicator_codes = list(self.indicators.values())
                indicator_names = list(self.indicators.keys())
            else:
                # Мапимо дружні імена на коди; якщо прийшов вже код, залишаємо як є
                indicator_codes = [self.indicators.get(ind, ind) for ind in indicators]
                # Зворотна мапа код-> ім'я для перейменування колонок
                code_to_name = {code: name for name, code in self.indicators.items()}
                indicator_names = [code_to_name.get(code, code) for code in indicator_codes]
            
            # Обмежуємо кількість країн та показників для стабільності API
            country_codes = country_codes[:8]  # Максимум 8 країн
            indicator_codes = indicator_codes[:4]  # Максимум 4 показники
            indicator_names = indicator_names[:4]
            
            # Отримуємо дані зі Світового банку з обробкою помилок
            try:
                data = wb.data.DataFrame(
                    indicator_codes,
                    country_codes,
                    time=range(start_year, end_year + 1),
                    labels=True,
                    skipBlanks=True
                )

                # Якщо порожньо – пробуємо HTTP fallback
                if data.empty:
                    print("WB wbgapi повернув порожні дані. Пробуємо HTTP fallback...")
                    data = self._fetch_via_http(country_codes, indicator_codes, start_year, end_year)
                    if data is None or data.empty:
                        print("HTTP fallback теж не повернув даних. Використовуємо зразкові дані...")
                        return self._get_sample_data()

            except Exception as api_error:
                print(f"API помилка wbgapi: {api_error}")
                print("Пробуємо HTTP fallback...")
                data = self._fetch_via_http(country_codes, indicator_codes, start_year, end_year)
                if data is None or data.empty:
                    print("HTTP fallback не вдався. Використовуємо зразкові дані...")
                    return self._get_sample_data()
            
            # Перетворюємо дані в зручний формат
            df = data.reset_index()
            
            # Перейменовуємо колонки за вибраними індикаторами
            # Після reset_index перші дві колонки це 'economy'/'Country' і 'time'/'Year' (з labels=True)
            # Інші — це коди індикаторів -> перейменовуємо у дружні імена
            code_to_name = {code: name for name, code in self.indicators.items()}
            rename_map = {code: code_to_name.get(code, code) for code in df.columns if code not in ['Country', 'Year', 'economy', 'time']}
            df.rename(columns=rename_map, inplace=True)
            # Узгоджуємо назви перших двох колонок
            if 'economy' in df.columns:
                df.rename(columns={'economy': 'Country'}, inplace=True)
            if 'time' in df.columns:
                df.rename(columns={'time': 'Year'}, inplace=True)
            
            # Додаємо назви країн
            df['Country_Name'] = df['Country'].map(self.countries)
            df['Country_Name'] = df['Country_Name'].fillna(df['Country'])
            
            # Переміщуємо колонки
            cols = ['Country', 'Country_Name', 'Year'] + indicator_names
            df = df[cols]
            
            return df
            
        except Exception as e:
            print(f"Помилка отримання даних зі Світового банку: {e}")
            print("Використовуємо зразкові дані...")
            return self._get_sample_data()

    def _fetch_via_http(self, country_codes: List[str], indicator_codes: List[str], start_year: int, end_year: int) -> Optional[pd.DataFrame]:
        """
        HTTP fallback: напряму викликає World Bank API
        Формат: https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?date={start}:{end}&format=json&per_page=20000
        Об'єднує результати по країнам та індикаторам у один DataFrame.
        """
        base_url = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
        rows = []

        # Невеликий ліміт ретраїв
        def fetch_json(url, params, retries=2):
            for attempt in range(retries + 1):
                try:
                    resp = requests.get(url, params=params, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                    # Очікується список [meta, data]
                    if isinstance(data, list) and len(data) == 2 and isinstance(data[1], list):
                        return data[1]
                except Exception:
                    if attempt == retries:
                        return None
            return None

        for country in country_codes:
            for code in indicator_codes:
                url = base_url.format(country=country.lower(), indicator=code)
                params = {
                    'date': f"{start_year}:{end_year}",
                    'format': 'json',
                    'per_page': 20000
                }
                data_items = fetch_json(url, params)
                if not data_items:
                    continue
                for item in data_items:
                    year = item.get('date')
                    val = item.get('value')
                    try:
                        year_int = int(year)
                    except Exception:
                        continue
                    rows.append({
                        'Country': country,
                        'Year': year_int,
                        code: float(val) if val is not None else None
                    })

        if not rows:
            return None

        df = pd.DataFrame(rows)
        # Приберемо NaN (з World Bank часто приходять null) — залишаємо None
        df = df.where(pd.notnull(df), None)
        # Зведемо по Country/Year, щоб об'єднати індикатори
        df = df.groupby(['Country', 'Year'], as_index=False).first()

        # Перейменуємо коди індикаторів у дружні назви, якщо відомі
        code_to_name = {v: k for k, v in self.indicators.items()}
        rename_map = {col: code_to_name.get(col, col) for col in df.columns if col not in ['Country', 'Year']}
        df.rename(columns=rename_map, inplace=True)

        # Додамо назви країн
        df['Country_Name'] = df['Country'].map(self.countries).fillna(df['Country'])
        # Впорядкуємо колонки
        ordered_cols = ['Country', 'Country_Name', 'Year'] + [c for c in df.columns if c not in ['Country', 'Country_Name', 'Year']]
        return df[ordered_cols]
    
    def get_country_comparison(self, countries: List[str], 
                             indicator: str = 'GDP_PER_CAPITA',
                             years: int = 10) -> pd.DataFrame:
        """
        Порівняння країн за конкретним показником
        
        Args:
            countries: Список кодів країн
            indicator: Показник для порівняння
            years: Кількість років для аналізу
        
        Returns:
            DataFrame з порівняльними даними
        """
        try:
            current_year = datetime.now().year
            start_year = current_year - years
            
            # Дозволяємо як дружню назву, так і вже код
            indicator_code = self.indicators.get(indicator, indicator)
            data = wb.data.DataFrame(
                [indicator_code],
                countries,
                time=range(start_year, current_year + 1),
                labels=True,
                skipBlanks=True
            )
            
            df = data.reset_index()
            df.columns = ['Country', 'Year', 'Value']
            df['Country_Name'] = df['Country'].map(self.countries)
            df['Country_Name'] = df['Country_Name'].fillna(df['Country'])
            
            return df
            
        except Exception as e:
            print(f"Помилка порівняння країн: {e}")
            return self._get_sample_comparison_data()
    
    def get_trend_analysis(self, country: str, 
                          indicators: List[str] = None,
                          years: int = 20) -> Dict:
        """
        Аналіз трендів для конкретної країни
        
        Args:
            country: Код країни
            indicators: Список показників для аналізу
            years: Кількість років для аналізу
        
        Returns:
            Словник з результатами аналізу
        """
        try:
            if indicators is None:
                indicators = ['GDP', 'GDP_PER_CAPITA', 'INFLATION', 'UNEMPLOYMENT']
            
            current_year = datetime.now().year
            start_year = current_year - years
            
            print(f"Спроба отримати тренди для {country}, показники: {indicators}, роки: {start_year}-{current_year}")
            
            indicator_codes = [self.indicators[ind] for ind in indicators]
            print(f"Коди показників: {indicator_codes}")
            
            # Обмежуємо кількість показників для стабільності
            limited_indicators = indicator_codes[:2]  # Максимум 2 показники
            limited_years = min(years, 10)  # Максимум 10 років
            
            print(f"Обмежені параметри: показники={limited_indicators}, років={limited_years}")
            
            data = wb.data.DataFrame(
                limited_indicators,
                [country],
                time=range(current_year - limited_years, current_year + 1),
                labels=True,
                skipBlanks=True
            )
            
            if data.empty:
                print("API повернув порожні дані, пробуємо HTTP fallback...")
                return self._fetch_trends_via_http(country, limited_indicators, current_year - limited_years, current_year)
            
            df = data.reset_index()
            df.columns = ['Country', 'Year'] + [ind for ind in indicators if self.indicators[ind] in limited_indicators]
            
            # Обчислюємо тренди
            trends = {}
            for indicator in indicators:
                if indicator in df.columns:
                    values = df[indicator].dropna()
                    if len(values) > 1:
                        # Простий лінійний тренд
                        x = np.arange(len(values))
                        y = values.values
                        trend_slope = np.polyfit(x, y, 1)[0]
                        trends[indicator] = {
                            'slope': trend_slope,
                            'direction': 'зростання' if trend_slope > 0 else 'спад',
                            'latest_value': values.iloc[-1],
                            'years_analyzed': len(values)
                        }
            
            print(f"Успішно отримано тренди: {list(trends.keys())}")
            return {
                'country': self.countries.get(country, country),
                'country_code': country,
                'analysis_period': f"{current_year - limited_years}-{current_year}",
                'trends': trends,
                'raw_data': df.to_dict('records')
            }
            
        except Exception as e:
            print(f"Помилка аналізу трендів через wbgapi: {e}")
            print(f"Тип помилки: {type(e).__name__}")
            print("Пробуємо HTTP fallback...")
            try:
                return self._fetch_trends_via_http(country, indicator_codes[:2], current_year - min(years, 10), current_year)
            except Exception as http_error:
                print(f"HTTP fallback також не вдався: {http_error}")
                print("Використовуємо зразкові дані...")
                return self._get_sample_trend_data(indicators)
    
    def _fetch_trends_via_http(self, country: str, indicator_codes: List[str], start_year: int, end_year: int) -> Dict:
        """HTTP fallback для отримання трендів"""
        import requests
        import time
        
        trends = {}
        raw_data = []
        
        for indicator_code in indicator_codes:
            url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}"
            params = {
                'date': f"{start_year}:{end_year}",
                'format': 'json',
                'per_page': 1000
            }
            
            try:
                print(f"HTTP запит для {indicator_code}: {url}?date={params['date']}")
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                json_data = response.json()
                
                if len(json_data) > 1 and isinstance(json_data[1], list):
                    values = []
                    years = []
                    for entry in json_data[1]:
                        if entry['value'] is not None:
                            values.append(float(entry['value']))
                            years.append(int(entry['date']))
                    
                    if len(values) > 1:
                        # Обчислюємо тренд
                        x = np.arange(len(values))
                        trend_slope = np.polyfit(x, values, 1)[0]
                        
                        # Знаходимо дружню назву показника
                        friendly_name = None
                        for name, code in self.indicators.items():
                            if code == indicator_code:
                                friendly_name = name
                                break
                        
                        if friendly_name:
                            trends[friendly_name] = {
                                'slope': trend_slope,
                                'direction': 'зростання' if trend_slope > 0 else 'спад',
                                'latest_value': values[-1],
                                'years_analyzed': len(values)
                            }
                            
                            # Додаємо до raw_data
                            for i, year in enumerate(years):
                                existing_row = next((row for row in raw_data if row['Year'] == year), None)
                                if existing_row:
                                    existing_row[friendly_name] = values[i]
                                else:
                                    raw_data.append({
                                        'Country': country,
                                        'Year': year,
                                        friendly_name: values[i]
                                    })
                
                time.sleep(1)  # Затримка між запитами
                
            except Exception as e:
                print(f"HTTP помилка для {indicator_code}: {e}")
                continue
        
        if not trends:
            raise Exception("HTTP fallback не вдався - немає даних")
        
        return {
            'country': self.countries.get(country, country),
            'country_code': country,
            'analysis_period': f"{start_year}-{end_year}",
            'trends': trends,
            'raw_data': raw_data
        }
    
    def _get_sample_data(self) -> pd.DataFrame:
        """Повертає зразкові дані у випадку помилки API"""
        countries = ['UA', 'US', 'DE', 'PL']
        years = list(range(2020, 2024))
        
        # Реалістичні дані для кожної країни (GDP в мільярдах доларів)
        country_data = {
            'UA': {'gdp_base': 200, 'gdp_per_capita': 4500, 'inflation': 8, 'unemployment': 8, 'life_exp': 72},
            'US': {'gdp_base': 25000, 'gdp_per_capita': 65000, 'inflation': 3, 'unemployment': 4, 'life_exp': 79},
            'DE': {'gdp_base': 4000, 'gdp_per_capita': 48000, 'inflation': 2, 'unemployment': 3, 'life_exp': 81},
            'PL': {'gdp_base': 700, 'gdp_per_capita': 18000, 'inflation': 4, 'unemployment': 5, 'life_exp': 78}
        }
        
        data = []
        for country in countries:
            base = country_data[country]
            for year in years:
                # Додаємо невеликі варіації для реалістичності
                gdp_variation = np.random.uniform(0.9, 1.1)
                inflation_variation = np.random.uniform(0.8, 1.2)
                
                row = {
                    'Country': country,
                    'Country_Name': 'Ukraine' if country == 'UA' else ('USA' if country == 'US' else ('Germany' if country == 'DE' else 'Poland')),
                    'Year': year,
                    'GDP': base['gdp_base'] * gdp_variation * 1_000_000_000,  # GDP в доларах (не мільярдах)
                    'GDP_PER_CAPITA': base['gdp_per_capita'] * gdp_variation,
                    'INFLATION': base['inflation'] * inflation_variation,
                    'UNEMPLOYMENT': base['unemployment'] + np.random.uniform(-1, 1),
                    'EXPORTS': base['gdp_base'] * 0.3 * gdp_variation * 1_000_000_000,  # Експорт в доларах
                    'IMPORTS': base['gdp_base'] * 0.35 * gdp_variation * 1_000_000_000,  # Імпорт в доларах
                    'POPULATION': (40 if country == 'UA' else (330 if country == 'US' else (83 if country == 'DE' else 38))) * 1_000_000,  # Населення в особах
                    'LIFE_EXPECTANCY': base['life_exp'] + np.random.uniform(-1, 1),
                    'INTERNET_USERS': np.random.uniform(70, 95),
                    'EDUCATION_EXPENDITURE': np.random.uniform(3, 6)
                }
                data.append(row)
        
        return pd.DataFrame(data)
    
    def _get_sample_comparison_data(self) -> pd.DataFrame:
        """Повертає зразкові дані для порівняння країн"""
        countries = ['UA', 'US', 'DE', 'PL']
        years = list(range(2020, 2024))
        
        data = []
        for country in countries:
            for year in years:
                row = {
                    'Country': country,
                    'Country_Name': self.countries[country],
                    'Year': year,
                    'Value': np.random.uniform(3000, 60000)
                }
                data.append(row)
        
        return pd.DataFrame(data)
    
    def normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Нормалізує дані для правильного відображення
        
        Args:
            df: DataFrame з даними Світового банку
        
        Returns:
            Нормалізований DataFrame
        """
        df_normalized = df.copy()
        
        # ВВП переводимо в мільярди доларів
        if 'GDP' in df_normalized.columns:
            df_normalized['GDP'] = df_normalized['GDP'] / 1_000_000_000
        
        # ВВП на душу залишаємо як є (вже в доларах)
        # Інфляцію та безробіття залишаємо як є (відсотки)
        
        # Експорт та імпорт переводимо в мільярди доларів
        for col in ['EXPORTS', 'IMPORTS']:
            if col in df_normalized.columns:
                df_normalized[col] = df_normalized[col] / 1_000_000_000
        
        # Населення переводимо в мільйони
        if 'POPULATION' in df_normalized.columns:
            df_normalized['POPULATION'] = df_normalized['POPULATION'] / 1_000_000
        
        return df_normalized
    
    def analyze_economic_health(self, df: pd.DataFrame) -> Dict:
        """
        Аналізує економічне здоров'я країн на основі показників
        
        Args:
            df: DataFrame з економічними показниками
        
        Returns:
            Словник з аналізом економічного здоров'я
        """
        analysis = {}
        
        # Перевіряємо чи DataFrame не порожній
        if df.empty:
            return analysis
            
        for country in df['Country'].unique():
            country_data = df[df['Country'] == country]
            
            # Останні доступні дані
            latest_data = country_data.iloc[-1] if not country_data.empty else None
            
            if latest_data is not None:
                health_score = 0
                indicators = {}
                
                # ВВП на душу (вага 30%)
                if 'GDP_PER_CAPITA' in latest_data and pd.notna(latest_data['GDP_PER_CAPITA']):
                    gdp_per_capita = float(latest_data['GDP_PER_CAPITA'])  # Конвертуємо в Python float
                    if gdp_per_capita > 50000:
                        gdp_score = 100
                    elif gdp_per_capita > 25000:
                        gdp_score = 80
                    elif gdp_per_capita > 10000:
                        gdp_score = 60
                    elif gdp_per_capita > 5000:
                        gdp_score = 40
                    else:
                        gdp_score = 20
                    
                    health_score += gdp_score * 0.3
                    indicators['GDP_PER_CAPITA'] = {
                        'value': float(gdp_per_capita),
                        'score': int(gdp_score),
                        'weight': 0.3
                    }
                
                # Інфляція (вага 25%)
                if 'INFLATION' in latest_data and pd.notna(latest_data['INFLATION']):
                    inflation = float(latest_data['INFLATION'])  # Конвертуємо в Python float
                    if inflation < 2:
                        inflation_score = 100
                    elif inflation < 5:
                        inflation_score = 80
                    elif inflation < 10:
                        inflation_score = 60
                    elif inflation < 20:
                        inflation_score = 40
                    else:
                        inflation_score = 20
                    
                    health_score += inflation_score * 0.25
                    indicators['INFLATION'] = {
                        'value': float(inflation),
                        'score': int(inflation_score),
                        'weight': 0.25
                    }
                
                # Безробіття (вага 25%)
                if 'UNEMPLOYMENT' in latest_data and pd.notna(latest_data['UNEMPLOYMENT']):
                    unemployment = float(latest_data['UNEMPLOYMENT'])  # Конвертуємо в Python float
                    if unemployment < 3:
                        unemployment_score = 100
                    elif unemployment < 5:
                        unemployment_score = 80
                    elif unemployment < 8:
                        unemployment_score = 60
                    elif unemployment < 15:
                        unemployment_score = 40
                    else:
                        unemployment_score = 20
                    
                    health_score += unemployment_score * 0.25
                    indicators['UNEMPLOYMENT'] = {
                        'value': float(unemployment),
                        'score': int(unemployment_score),
                        'weight': 0.25
                    }
                
                # Очікувана тривалість життя (вага 20%)
                if 'LIFE_EXPECTANCY' in latest_data and pd.notna(latest_data['LIFE_EXPECTANCY']):
                    life_expectancy = float(latest_data['LIFE_EXPECTANCY'])  # Конвертуємо в Python float
                    if life_expectancy > 80:
                        life_score = 100
                    elif life_expectancy > 75:
                        life_score = 80
                    elif life_expectancy > 70:
                        life_score = 60
                    elif life_expectancy > 65:
                        life_score = 40
                    else:
                        life_score = 20
                    
                    health_score += life_score * 0.2
                    indicators['LIFE_EXPECTANCY'] = {
                        'value': float(life_expectancy),
                        'score': int(life_score),
                        'weight': 0.2
                    }
                
                # Визначаємо рівень економічного здоров'я
                if health_score >= 80:
                    health_level = "Excellent"
                elif health_score >= 60:
                    health_level = "Good"
                elif health_score >= 40:
                    health_level = "Medium"
                elif health_score >= 20:
                    health_level = "Poor"
                else:
                    health_level = "Critical"
                
                analysis[country] = {
                    'country_name': str(latest_data.get('Country_Name', country)),  # Конвертуємо в Python string
                    'health_score': round(float(health_score), 1),  # Конвертуємо в Python float
                    'health_level': health_level,
                    'indicators': indicators,
                    'latest_year': int(latest_data.get('Year', 0))  # Конвертуємо в Python int
                }
            else:
                # Якщо немає даних для країни
                analysis[country] = {
                    'country_name': self.countries.get(country, country),
                    'health_score': 0,
                    'health_level': "No Data",
                    'indicators': {},
                    'latest_year': 'N/A'
                }
        
        return analysis
    
    def get_currency_conversion_rates(self) -> Dict[str, float]:
        """
        Повертає курси валют для конвертації (приблизні)
        """
        return {
            'USD': 1.0,      # Базова валюта
            'EUR': 0.85,     # Євро
            'UAH': 36.5,     # Гривня
            'PLN': 4.0,      # Злотий
            'GBP': 0.8,      # Фунт стерлінгів
            'JPY': 110.0,    # Єна
            'CNY': 6.5,      # Юань
            'RUB': 90.0,     # Рубль
        }
    
    def convert_to_usd(self, df: pd.DataFrame, currency: str = 'USD') -> pd.DataFrame:
        """
        Конвертує дані в долари США
        
        Args:
            df: DataFrame з даними
            currency: Валюта для конвертації
        
        Returns:
            DataFrame з конвертованими даними
        """
        df_converted = df.copy()
        rates = self.get_currency_conversion_rates()
        
        if currency in rates and currency != 'USD':
            rate = rates[currency]
            
            # Конвертуємо ВВП та інші грошові показники
            money_columns = ['GDP', 'GDP_PER_CAPITA', 'EXPORTS', 'IMPORTS']
            for col in money_columns:
                if col in df_converted.columns:
                    df_converted[col] = df_converted[col] / rate
        
        return df_converted
    
    def _get_sample_trend_data(self, indicators: List[str] = None) -> Dict:
        """Повертає зразкові дані для аналізу трендів"""
        if indicators is None:
            indicators = ['GDP', 'GDP_PER_CAPITA']
        
        # Базові дані для всіх показників
        all_data = {
            'GDP': {
                'slope': 15,
                'direction': 'зростання',
                'latest_value': 200,
                'years_analyzed': 4,
                'raw_values': [150, 170, 180, 200]
            },
            'GDP_PER_CAPITA': {
                'slope': 500,
                'direction': 'зростання',
                'latest_value': 4500,
                'years_analyzed': 4,
                'raw_values': [3500, 3800, 4000, 4500]
            },
            'INFLATION': {
                'slope': -0.5,
                'direction': 'спад',
                'latest_value': 8.5,
                'years_analyzed': 4,
                'raw_values': [10.0, 9.5, 9.0, 8.5]
            },
            'UNEMPLOYMENT': {
                'slope': -0.3,
                'direction': 'спад',
                'latest_value': 7.8,
                'years_analyzed': 4,
                'raw_values': [8.5, 8.2, 8.0, 7.8]
            }
        }
        
        # Фільтруємо тільки обрані показники
        filtered_trends = {}
        filtered_raw_data = []
        
        years = [2020, 2021, 2022, 2023]
        
        for indicator in indicators:
            if indicator in all_data:
                filtered_trends[indicator] = {
                    'slope': all_data[indicator]['slope'],
                    'direction': all_data[indicator]['direction'],
                    'latest_value': all_data[indicator]['latest_value'],
                    'years_analyzed': all_data[indicator]['years_analyzed']
                }
        
        # Створюємо raw_data тільки для обраних показників
        for i, year in enumerate(years):
            row = {'Country': 'UA', 'Year': year}
            for indicator in indicators:
                if indicator in all_data:
                    row[indicator] = all_data[indicator]['raw_values'][i]
            filtered_raw_data.append(row)
        
        return {
            'country': 'Україна',
            'country_code': 'UA',
            'analysis_period': '2020-2024',
            'trends': filtered_trends,
            'raw_data': filtered_raw_data
        }

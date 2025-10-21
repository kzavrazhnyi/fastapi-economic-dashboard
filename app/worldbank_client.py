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

                # Якщо порожньо – кидаємо виняток
                if data.empty:
                    raise Exception("World Bank API повернув порожні дані")

            except Exception as api_error:
                print(f"API помилка wbgapi: {api_error}")
                raise Exception(f"World Bank API недоступний: {api_error}")
            
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
            raise Exception(f"World Bank API недоступний: {e}")

    
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
                raise Exception("World Bank API повернув порожні дані для трендів")
            
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
            raise Exception(f"World Bank API недоступний для трендів: {e}")
    
    
    
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
    

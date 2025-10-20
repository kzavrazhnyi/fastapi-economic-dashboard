# -*- coding: utf-8 -*-
"""
Модуль для роботи з API Світового банку
"""

import wbgapi as wb
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
                
                # Перевіряємо чи дані не порожні
                if data.empty:
                    print("API повернув порожні дані, використовуємо зразкові дані...")
                    return self._get_sample_data()
                
            except Exception as api_error:
                print(f"API помилка: {api_error}")
                print("Використовуємо зразкові дані...")
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
            
            indicator_codes = [self.indicators[ind] for ind in indicators]
            
            data = wb.data.DataFrame(
                [self.indicators.get(code, code) for code in indicator_codes],
                [country],
                time=range(start_year, current_year + 1),
                labels=True,
                skipBlanks=True
            )
            
            df = data.reset_index()
            df.columns = ['Country', 'Year'] + indicators
            
            # Обчислюємо тренди
            trends = {}
            for indicator in indicators:
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
            
            return {
                'country': self.countries.get(country, country),
                'country_code': country,
                'analysis_period': f"{start_year}-{current_year}",
                'trends': trends,
                'raw_data': df.to_dict('records')
            }
            
        except Exception as e:
            print(f"Помилка аналізу трендів: {e}")
            return self._get_sample_trend_data()
    
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
    
    def _get_sample_trend_data(self) -> Dict:
        """Повертає зразкові дані для аналізу трендів"""
        return {
            'country': 'Україна',
            'country_code': 'UA',
            'analysis_period': '2020-2024',
            'trends': {
                'GDP': {
                    'slope': 15,  # мільярди доларів на рік
                    'direction': 'зростання',
                    'latest_value': 200,  # мільярди доларів
                    'years_analyzed': 4
                },
                'GDP_PER_CAPITA': {
                    'slope': 500,
                    'direction': 'зростання',
                    'latest_value': 4500,
                    'years_analyzed': 4
                }
            },
            'raw_data': []
        }

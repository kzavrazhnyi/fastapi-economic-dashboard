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
        # In-memory cache for API responses / Кеш в пам'яті для відповідей API
        self._cache = {}
        self._cache_ttl = 1800  # 30 minutes cache TTL / TTL кешу 30 хвилин
        self._last_request_time = 0
        self._min_request_interval = 10  # Minimum 10 seconds between requests / Мінімум 10 секунд між запитами
        
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
            'CN': 'Китай',
            'JP': 'Японія',
            'IN': 'Індія',
            'BR': 'Бразилія',
            'CA': 'Канада',
            'AU': 'Австралія',
            'IT': 'Італія',
            'ES': 'Іспанія',
        }
    
    def _get_cache_key(self, endpoint: str, params: Dict[str, any]) -> str:
        """Generate cache key for API request / Генерує ключ кешу для API запиту"""
        sorted_params = sorted(params.items())
        return f"{endpoint}:{':'.join(f'{k}={v}' for k, v in sorted_params)}"

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid / Перевіряє чи кешовані дані ще актуальні"""
        if cache_key not in self._cache:
            return False
        cached_time, _ = self._cache[cache_key]
        return datetime.now() - cached_time < timedelta(seconds=self._cache_ttl)

    def _get_from_cache(self, cache_key: str) -> any:
        """Get data from cache if valid / Отримує дані з кешу якщо актуальні"""
        if self._is_cache_valid(cache_key):
            _, data = self._cache[cache_key]
            print(f"Returning cached World Bank data for key: {cache_key}")
            return data
        return None

    def _save_to_cache(self, cache_key: str, data: any):
        """Save data to cache / Зберігає дані в кеш"""
        self._cache[cache_key] = (datetime.now(), data)
        print(f"Cached World Bank data for key: {cache_key}")

    def _rate_limit_delay(self):
        """Apply rate limiting / Застосовує обмеження частоти запитів"""
        import time
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        if time_since_last_request < self._min_request_interval:
            delay = self._min_request_interval - time_since_last_request
            print(f"World Bank API rate limiting: waiting {delay:.2f} seconds")
            time.sleep(delay)
        self._last_request_time = time.time()

    def get_last_update_time(self, endpoint: str, params: Dict[str, any]) -> str:
        """Get last update time for cached data / Отримує час останнього оновлення кешованих даних"""
        cache_key = self._get_cache_key(endpoint, params)
        if cache_key in self._cache:
            cached_time, _ = self._cache[cache_key]
            return cached_time.strftime("%d.%m.%Y, %H:%M:%S")
        return None
    
    def clear_cache(self):
        """Clear all cached data / Очистити весь кеш"""
        self._cache.clear()
        print("Кеш World Bank API очищено")
    
    def get_economic_indicators(self, country_codes: List[str] = None, 
                              indicators: List[str] = None, 
                              start_year: int = 2020, 
                              end_year: int = 2023) -> pd.DataFrame:
        """
        Отримати економетричні показники зі Світового банку
        Get econometric indicators from the World Bank
        """
        try:
            # Check cache / Перевіряємо кеш
            params = {
                'country_codes': country_codes,
                'indicators': indicators,
                'start_year': start_year,
                'end_year': end_year
            }
            cache_key = self._get_cache_key('economic_indicators', params)
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data
            
            # Apply rate limiting / Застосовуємо rate limiting
            self._rate_limit_delay()
            if country_codes is None:
                country_codes = list(self.countries.keys())
            
            # Define indicators / Визначаємо індикатори
            if indicators is None:
                indicator_codes = list(self.indicators.values())
                indicator_names = list(self.indicators.keys())
            else:
                indicator_codes = [self.indicators.get(ind, ind) for ind in indicators]
                code_to_name = {code: name for name, code in self.indicators.items()}
                indicator_names = [code_to_name.get(code, code) for code in indicator_codes]
            
            # Limit API parameters for stability / Обмежуємо кількість країн та показників
            country_codes = country_codes[:8]
            indicator_codes = indicator_codes[:4]
            indicator_names = indicator_names[:4]
            
            # Get data from World Bank with error handling
            # Отримуємо дані зі Світового банку з обробкою помилок
            try:
                # Логування параметрів для дебагу / Logging parameters for debugging
                print(f"🔍 World Bank API параметри:")
                print(f"   Країни: {country_codes}")
                print(f"   Показники: {indicator_codes}")
                print(f"   Роки: {start_year}-{end_year}")
                print(f"   Діапазон: {list(range(start_year, end_year + 1))}")
                
                # Додаємо лог до серверних логів
                try:
                    import sys
                    import os
                    # Додаємо батьківську директорію до Python path
                    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    if parent_dir not in sys.path:
                        sys.path.append(parent_dir)
                    from app.main import add_server_log
                    
                    add_server_log("worldbank_request", "Запит до World Bank API", {
                        "countries": country_codes,
                        "indicators": indicator_codes,
                        "years": list(range(start_year, end_year + 1)),
                        "start_year": start_year,
                        "end_year": end_year
                    })
                except Exception as log_error:
                    print(f"Помилка логування: {log_error}")
                
                # Use direct HTTP requests instead of wbgapi for better reliability
                # / Використовуємо прямі HTTP запити замість wbgapi для кращої надійності
                import requests
                
                # Build URL for World Bank API v2
                # / Створюємо URL для World Bank API v2
                countries_str = ';'.join(country_codes)
                date_range = f"{start_year}:{end_year}"
                
                print(f"🌐 Прямі запити до World Bank API:")
                print(f"   Країни: {countries_str}")
                print(f"   Показники: {indicator_codes}")
                print(f"   Роки: {date_range}")
                
                # Make separate requests for each indicator (API limitation)
                # / Робимо окремі запити для кожного показника (обмеження API)
                all_data = []
                
                for indicator_code in indicator_codes:
                    url = f"https://api.worldbank.org/v2/country/{countries_str}/indicator/{indicator_code}?date={date_range}&format=json&per_page=1000"
                    
                    print(f"   Запит для {indicator_code}: {url}")
                    
                    # Додаємо детальний лог для кожного показника
                    try:
                        import sys
                        import os
                        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                        from app.main import add_server_log
                        
                        add_server_log("worldbank_indicator_request", f"Запит показника {indicator_code}", {
                            "url": url,
                            "indicator": indicator_code,
                            "countries": countries_str,
                            "date_range": date_range
                        })
                    except Exception as log_error:
                        print(f"Помилка логування показника: {log_error}")
                    
                    try:
                        response = requests.get(url, timeout=30)
                        response.raise_for_status()
                        
                        data_json = response.json()
                        
                        if data_json and len(data_json) >= 2 and data_json[1]:
                            metadata = data_json[0]
                            data_records = data_json[1]
                            
                            print(f"   ✅ Отримано {len(data_records)} записів для {indicator_code}")
                            
                            # Додаємо лог успішного запиту
                            try:
                                import sys
                                import os
                                sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                                from app.main import add_server_log
                                
                                add_server_log("worldbank_indicator_success", f"Успішний запит показника {indicator_code}", {
                                    "indicator": indicator_code,
                                    "records_count": len(data_records),
                                    "total_records": metadata.get('total', 0),
                                    "url": url
                                })
                            except Exception as log_error:
                                print(f"Помилка логування успіху: {log_error}")
                            
                            # Convert to DataFrame format
                            # / Перетворюємо в формат DataFrame
                            for record in data_records:
                                if record.get('value') is not None:
                                    all_data.append({
                                        'Country': record['country']['id'],
                                        'Country_Name': record['country']['value'],
                                        'Year': int(record['date']),
                                        'Indicator': record['indicator']['id'],
                                        'Value': record['value']
                                    })
                        else:
                            print(f"   ⚠️ Немає даних для {indicator_code}")
                            
                            # Додаємо лог відсутності даних
                            try:
                                import sys
                                import os
                                sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                                from app.main import add_server_log
                                
                                add_server_log("worldbank_indicator_no_data", f"Немає даних для показника {indicator_code}", {
                                    "indicator": indicator_code,
                                    "url": url
                                })
                            except Exception as log_error:
                                print(f"Помилка логування відсутності даних: {log_error}")
                            
                    except Exception as indicator_error:
                        print(f"   ❌ Помилка для {indicator_code}: {indicator_error}")
                        
                        # Додаємо лог помилки
                        try:
                            import sys
                            import os
                            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                            from app.main import add_server_log
                            
                            add_server_log("worldbank_indicator_error", f"Помилка запиту показника {indicator_code}", {
                                "indicator": indicator_code,
                                "url": url,
                                "error": str(indicator_error),
                                "error_type": type(indicator_error).__name__
                            })
                        except Exception as log_error:
                            print(f"Помилка логування помилки: {log_error}")
                        
                        continue
                
                if not all_data:
                    raise Exception("Немає даних після обробки всіх показників")
                
                print(f"✅ Загалом отримано {len(all_data)} записів")
                
                # Create DataFrame and pivot to have indicators as columns
                # / Створюємо DataFrame та повертаємо показники як колонки
                df_temp = pd.DataFrame(all_data)
                df = df_temp.pivot_table(
                    index=['Country', 'Country_Name', 'Year'], 
                    columns='Indicator', 
                    values='Value', 
                    aggfunc='first'
                ).reset_index()
                
                # Rename columns to friendly names
                # / Перейменовуємо колонки на дружні назви
                code_to_name = {code: name for name, code in self.indicators.items()}
                rename_map = {code: code_to_name.get(code, code) for code in df.columns if code not in ['Country', 'Country_Name', 'Year']}
                df.rename(columns=rename_map, inplace=True)


            except Exception as api_error:
                print(f"API помилка World Bank: {api_error}")
                print("Повертаємо порожній DataFrame.")
                
                # Логування помилки
                print(f"🌐 Помилка запиту до World Bank API:")
                print(f"   URL: {url if 'url' in locals() else 'N/A'}")
                print(f"   Параметри: countries={country_codes}, indicators={indicator_codes}, years={list(range(start_year, end_year + 1))}")
                print(f"   Відповідь API: {str(api_error)}")
                
                # Create an empty DataFrame with the expected columns to prevent crashes
                # / Створюємо порожній DataFrame з очікуваними колонками, щоб уникнути збоїв
                cols = ['Country', 'Country_Name', 'Year'] + indicator_names
                df = pd.DataFrame(columns=cols)
            
            # Add country names if not already present / Додаємо назви країн якщо ще немає
            if 'Country_Name' not in df.columns:
                df['Country_Name'] = df['Country'].map(self.countries)
                df['Country_Name'] = df['Country_Name'].fillna(df['Country'])
            
            # Reorder columns / Переміщуємо колонки
            # Ensure all requested/generated indicators are in the list
            # / Переконуємося, що всі запитані/згенеровані індикатори є в списку
            final_indicator_names = [name for name in indicator_names if name in df.columns]
            cols = ['Country', 'Country_Name', 'Year'] + final_indicator_names
            
            # Select only columns that actually exist in df
            # / Вибираємо тільки ті колонки, які реально існують в df
            existing_cols = [col for col in cols if col in df.columns]
            df = df[existing_cols]
            
            # Save to cache / Зберігаємо в кеш
            self._save_to_cache(cache_key, df)
            
            return df
            
        except Exception as e:
            # This outer catch handles unexpected errors in *this* function's logic
            # / Цей зовнішній catch обробляє неочікувані помилки в логіці *цієї* функції
            print(f"Критична помилка в get_economic_indicators: {e}")
            # Return an empty DF as a last resort / Повертаємо порожній DF в крайньому випадку
            cols = ['Country', 'Country_Name', 'Year'] + (indicator_names if 'indicator_names' in locals() else [])
            return pd.DataFrame(columns=cols)

    
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
            
            # Логування запиту порівняння країн
            countries_str = ';'.join(countries)
            date_range = f"{start_year}:{current_year}"
            url = f"https://api.worldbank.org/v2/country/{countries_str}/indicator/{indicator_code}?date={date_range}&format=json&per_page=1000"
            
            print(f"🔍 Запит порівняння країн до World Bank API:")
            print(f"   URL: {url}")
            print(f"   Параметри: countries={countries}, indicator={indicator_code}, years={list(range(start_year, current_year + 1))}")
            
            # Додаємо лог до серверних логів
            try:
                import sys
                import os
                sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                from app.main import add_server_log
                
                add_server_log("worldbank_comparison_request", "Запит порівняння країн", {
                    "url": url,
                    "countries": countries,
                    "indicator": indicator_code,
                    "years": list(range(start_year, current_year + 1))
                })
            except Exception as log_error:
                print(f"Помилка логування порівняння: {log_error}")
            
            # Use direct HTTP request instead of wbgapi
            # / Використовуємо прямий HTTP запит замість wbgapi
            import requests
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data_json = response.json()
            
            if not data_json or len(data_json) < 2 or not data_json[1]:
                # Додаємо лог відсутності даних
                try:
                    import sys
                    import os
                    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    from app.main import add_server_log
                    
                    add_server_log("worldbank_comparison_no_data", "Немає даних для порівняння", {
                        "url": url,
                        "countries": countries,
                        "indicator": indicator_code
                    })
                except Exception as log_error:
                    print(f"Помилка логування відсутності даних: {log_error}")
                
                raise Exception("World Bank API повернув порожні дані для порівняння")
            
            data_records = data_json[1]
            
            # Додаємо лог успішного запиту
            try:
                import sys
                import os
                sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                from app.main import add_server_log
                
                add_server_log("worldbank_comparison_success", "Успішний запит порівняння", {
                    "url": url,
                    "countries": countries,
                    "indicator": indicator_code,
                    "records_count": len(data_records)
                })
            except Exception as log_error:
                print(f"Помилка логування успіху порівняння: {log_error}")
            
            # Convert to DataFrame format
            # / Перетворюємо в формат DataFrame
            df_data = []
            for record in data_records:
                if record.get('value') is not None:
                    df_data.append({
                        'Country': record['country']['id'],
                        'Year': int(record['date']),
                        'Value': record['value']
                    })
            
            if not df_data:
                raise Exception("Немає даних після обробки")
            
            df = pd.DataFrame(df_data)
            df['Country_Name'] = df['Country'].map(self.countries)
            df['Country_Name'] = df['Country_Name'].fillna(df['Country'])
            
            return df
            
        except Exception as e:
            print(f"Помилка порівняння країн: {e}")
            # Return empty DataFrame instead of calling non-existent method
            # / Повертаємо порожній DataFrame замість виклику неіснуючого методу
            return pd.DataFrame(columns=['Country', 'Year', 'Value', 'Country_Name'])
    
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
            
            # Use direct HTTP requests instead of wbgapi
            # / Використовуємо прямі HTTP запити замість wbgapi
            import requests
            
            all_data = []
            
            for indicator_code in limited_indicators:
                date_range = f"{current_year - limited_years}:{current_year}"
                url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}?date={date_range}&format=json&per_page=1000"
                
                print(f"   Запит трендів для {indicator_code}: {url}")
                
                # Додаємо лог для трендів
                try:
                    import sys
                    import os
                    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    from app.main import add_server_log
                    
                    add_server_log("worldbank_trends_request", f"Запит трендів для {indicator_code}", {
                        "url": url,
                        "country": country,
                        "indicator": indicator_code,
                        "date_range": date_range
                    })
                except Exception as log_error:
                    print(f"Помилка логування трендів: {log_error}")
                
                try:
                    response = requests.get(url, timeout=30)
                    response.raise_for_status()
                    
                    data_json = response.json()
                    
                    if data_json and len(data_json) >= 2 and data_json[1]:
                        data_records = data_json[1]
                        
                        print(f"   ✅ Отримано {len(data_records)} записів для трендів {indicator_code}")
                        
                        # Додаємо лог успішного запиту трендів
                        try:
                            import sys
                            import os
                            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                            from app.main import add_server_log
                            
                            add_server_log("worldbank_trends_success", f"Успішний запит трендів {indicator_code}", {
                                "url": url,
                                "country": country,
                                "indicator": indicator_code,
                                "records_count": len(data_records)
                            })
                        except Exception as log_error:
                            print(f"Помилка логування успіху трендів: {log_error}")
                        
                        # Convert to DataFrame format
                        # / Перетворюємо в формат DataFrame
                        for record in data_records:
                            if record.get('value') is not None:
                                all_data.append({
                                    'Country': record['country']['id'],
                                    'Year': int(record['date']),
                                    'Indicator': record['indicator']['id'],
                                    'Value': record['value']
                                })
                    else:
                        print(f"   ⚠️ Немає даних для трендів {indicator_code}")
                        
                        # Додаємо лог відсутності даних трендів
                        try:
                            import sys
                            import os
                            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                            from app.main import add_server_log
                            
                            add_server_log("worldbank_trends_no_data", f"Немає даних для трендів {indicator_code}", {
                                "url": url,
                                "country": country,
                                "indicator": indicator_code
                            })
                        except Exception as log_error:
                            print(f"Помилка логування відсутності даних трендів: {log_error}")
                        
                except Exception as indicator_error:
                    print(f"   ❌ Помилка для трендів {indicator_code}: {indicator_error}")
                    
                    # Додаємо лог помилки трендів
                    try:
                        import sys
                        import os
                        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                        from app.main import add_server_log
                        
                        add_server_log("worldbank_trends_error", f"Помилка запиту трендів {indicator_code}", {
                            "url": url,
                            "country": country,
                            "indicator": indicator_code,
                            "error": str(indicator_error),
                            "error_type": type(indicator_error).__name__
                        })
                    except Exception as log_error:
                        print(f"Помилка логування помилки трендів: {log_error}")
                    
                    continue
            
            if not all_data:
                raise Exception("Немає даних для аналізу трендів")
            
            print(f"✅ Загалом отримано {len(all_data)} записів для трендів")
            
            # Create DataFrame and pivot to have indicators as columns
            # / Створюємо DataFrame та повертаємо показники як колонки
            df_temp = pd.DataFrame(all_data)
            data = df_temp.pivot_table(
                index=['Country', 'Year'], 
                columns='Indicator', 
                values='Value', 
                aggfunc='first'
            ).reset_index()
            
            if data.empty:
                raise Exception("World Bank API повернув порожні дані для трендів")
            
            # Rename columns to friendly names
            # / Перейменовуємо колонки на дружні назви
            code_to_name = {code: name for name, code in self.indicators.items()}
            rename_map = {code: code_to_name.get(code, code) for code in data.columns if code not in ['Country', 'Year']}
            data.rename(columns=rename_map, inplace=True)
            
            df = data
            
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
    

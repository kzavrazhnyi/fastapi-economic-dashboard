# -*- coding: utf-8 -*-
"""
Client for CoinGecko public API to fetch crypto market data.
"""
from typing import Any, Dict, List
import requests
import logging
import time
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class CryptoDataProvider:
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self):
        # In-memory cache for API responses / Кеш в пам'яті для відповідей API
        self._cache = {}
        self._cache_ttl = 600  # 10 minutes cache TTL / TTL кешу 10 хвилин
        self._last_request_time = 0
        self._min_request_interval = 5  # Minimum 5 seconds between requests / Мінімум 5 секунд між запитами

    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate cache key for API request / Генерує ключ кешу для API запиту"""
        sorted_params = sorted(params.items())
        return f"{endpoint}:{':'.join(f'{k}={v}' for k, v in sorted_params)}"
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid / Перевіряє чи кешовані дані ще валідні"""
        if cache_key not in self._cache:
            return False
        
        cached_time, _ = self._cache[cache_key]
        return datetime.now() - cached_time < timedelta(seconds=self._cache_ttl)
    
    def _get_from_cache(self, cache_key: str) -> Any:
        """Get data from cache / Отримує дані з кешу"""
        if self._is_cache_valid(cache_key):
            _, data = self._cache[cache_key]
            logger.info(f"Returning cached data for key: {cache_key}")
            return data
        return None
    
    def _save_to_cache(self, cache_key: str, data: Any):
        """Save data to cache / Зберігає дані в кеш"""
        self._cache[cache_key] = (datetime.now(), data)
        logger.info(f"Cached data for key: {cache_key}")
    
    def _rate_limit_delay(self):
        """Ensure minimum delay between API requests / Забезпечує мінімальну затримку між API запитами"""
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time
        
        if time_since_last_request < self._min_request_interval:
            delay = self._min_request_interval - time_since_last_request
            logger.info(f"Rate limiting: waiting {delay:.2f} seconds")
            time.sleep(delay)
        
        self._last_request_time = time.time()

    def get_last_update_time(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Get last update time for cached data / Отримує час останнього оновлення кешованих даних"""
        cache_key = self._get_cache_key(endpoint, params)
        if cache_key in self._cache:
            cached_time, _ = self._cache[cache_key]
            return cached_time.strftime("%d.%m.%Y, %H:%M:%S")
        return None

    def get_market_data(self, currency: str = "usd", per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch market data for top cryptocurrencies ordered by market cap.
        Returns a JSON list of objects as provided by CoinGecko.
        Raises exception if API is unavailable.
        """
        try:
            # Check cache first / Спочатку перевіряємо кеш
            params = {
                "vs_currency": currency,
                "order": "market_cap_desc",
                "per_page": min(per_page, 100),  # Limit to avoid rate limits
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h",
            }
            
            cache_key = self._get_cache_key("coins/markets", params)
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data
            
            # Rate limiting / Обмеження частоти запитів
            self._rate_limit_delay()
            
            url = f"{self.BASE_URL}/coins/markets"
            
            logger.info(f"Making request to CoinGecko API: {url}")
            resp = requests.get(url, params=params, timeout=15)
            
            if resp.status_code == 429:
                logger.warning("CoinGecko API rate limit exceeded")
                raise requests.exceptions.HTTPError("Rate limit exceeded. Please try again later.")
            
            resp.raise_for_status()
            data = resp.json()
            
            if not isinstance(data, list):
                logger.error(f"Unexpected response format from CoinGecko API: {type(data)}")
                raise ValueError("Invalid response format from CoinGecko API")
            
            # Cache the response / Кешуємо відповідь
            self._save_to_cache(cache_key, data)
            
            logger.info(f"Successfully fetched {len(data)} coins from CoinGecko API")
            return data
            
        except requests.exceptions.Timeout:
            logger.error("CoinGecko API request timeout")
            raise requests.exceptions.Timeout("CoinGecko API is not responding. Please try again later.")
        except requests.exceptions.RequestException as e:
            logger.error(f"CoinGecko API request failed: {e}")
            raise requests.exceptions.RequestException(f"Failed to fetch crypto data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching market data: {e}")
            raise Exception(f"Unexpected error: {e}")

    def get_coin_history(self, coin_id: str, currency: str = "usd", days: int = 30) -> Dict[str, Any]:
        """Fetch historical market chart for a coin.
        Returns a JSON with arrays of prices/market_caps/total_volumes.
        Raises exception if API is unavailable.
        """
        try:
            # Check cache first / Спочатку перевіряємо кеш
            params = {
                "vs_currency": currency,
                "days": min(days, 30),  # Limit to avoid rate limits
                "interval": "daily",
            }
            
            cache_key = self._get_cache_key(f"coins/{coin_id}/market_chart", params)
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data
            
            # Rate limiting / Обмеження частоти запитів
            self._rate_limit_delay()
            
            url = f"{self.BASE_URL}/coins/{coin_id}/market_chart"
            
            logger.info(f"Making request to CoinGecko API for {coin_id} history")
            resp = requests.get(url, params=params, timeout=15)
            
            if resp.status_code == 429:
                logger.warning("CoinGecko API rate limit exceeded")
                raise requests.exceptions.HTTPError("Rate limit exceeded. Please try again later.")
            
            resp.raise_for_status()
            data = resp.json()
            
            # Cache the response / Кешуємо відповідь
            self._save_to_cache(cache_key, data)
            
            logger.info(f"Successfully fetched history for {coin_id}")
            return data
            
        except requests.exceptions.Timeout:
            logger.error("CoinGecko API request timeout")
            raise requests.exceptions.Timeout("CoinGecko API is not responding. Please try again later.")
        except requests.exceptions.RequestException as e:
            logger.error(f"CoinGecko API request failed: {e}")
            raise requests.exceptions.RequestException(f"Failed to fetch coin history: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching coin history: {e}")
            raise Exception(f"Unexpected error: {e}")

    def get_global(self) -> Dict[str, Any]:
        """Fetch global crypto market data (total market cap, dominance, etc.).
        Raises exception if API is unavailable.
        """
        try:
            # Check cache first / Спочатку перевіряємо кеш
            params = {}
            cache_key = self._get_cache_key("global", params)
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data
            
            # Rate limiting / Обмеження частоти запитів
            self._rate_limit_delay()
            
            url = f"{self.BASE_URL}/global"
            
            logger.info("Making request to CoinGecko global API")
            resp = requests.get(url, timeout=15)
            
            if resp.status_code == 429:
                logger.warning("CoinGecko API rate limit exceeded")
                raise requests.exceptions.HTTPError("Rate limit exceeded. Please try again later.")
            
            resp.raise_for_status()
            data = resp.json()
            
            # Cache the response / Кешуємо відповідь
            self._save_to_cache(cache_key, data)
            
            logger.info("Successfully fetched global data from CoinGecko API")
            return data
            
        except requests.exceptions.Timeout:
            logger.error("CoinGecko API request timeout")
            raise requests.exceptions.Timeout("CoinGecko API is not responding. Please try again later.")
        except requests.exceptions.RequestException as e:
            logger.error(f"CoinGecko API request failed: {e}")
            raise requests.exceptions.RequestException(f"Failed to fetch global crypto data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error fetching global data: {e}")
            raise Exception(f"Unexpected error: {e}")

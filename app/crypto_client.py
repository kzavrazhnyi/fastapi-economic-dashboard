# -*- coding: utf-8 -*-
"""
Client for CoinGecko public API to fetch crypto market data.
"""
from typing import Any, Dict, List
import requests
import logging

logger = logging.getLogger(__name__)


class CryptoDataProvider:
    BASE_URL = "https://api.coingecko.com/api/v3"

    def get_market_data(self, currency: str = "usd", per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch market data for top cryptocurrencies ordered by market cap.
        Returns a JSON list of objects as provided by CoinGecko.
        Raises exception if API is unavailable.
        """
        try:
            url = f"{self.BASE_URL}/coins/markets"
            params = {
                "vs_currency": currency,
                "order": "market_cap_desc",
                "per_page": min(per_page, 100),  # Limit to avoid rate limits
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h",
            }
            
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
            url = f"{self.BASE_URL}/coins/{coin_id}/market_chart"
            params = {
                "vs_currency": currency,
                "days": min(days, 30),  # Limit to avoid rate limits
                "interval": "daily",
            }
            
            logger.info(f"Making request to CoinGecko API for {coin_id} history")
            resp = requests.get(url, params=params, timeout=15)
            
            if resp.status_code == 429:
                logger.warning("CoinGecko API rate limit exceeded")
                raise requests.exceptions.HTTPError("Rate limit exceeded. Please try again later.")
            
            resp.raise_for_status()
            data = resp.json()
            
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
            url = f"{self.BASE_URL}/global"
            
            logger.info("Making request to CoinGecko global API")
            resp = requests.get(url, timeout=15)
            
            if resp.status_code == 429:
                logger.warning("CoinGecko API rate limit exceeded")
                raise requests.exceptions.HTTPError("Rate limit exceeded. Please try again later.")
            
            resp.raise_for_status()
            data = resp.json()
            
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

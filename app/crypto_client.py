# -*- coding: utf-8 -*-
"""
Client for CoinGecko public API to fetch crypto market data.
"""
from typing import Any, Dict, List
import requests


class CryptoDataProvider:
    BASE_URL = "https://api.coingecko.com/api/v3"

    def get_market_data(self, currency: str = "usd", per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch market data for top cryptocurrencies ordered by market cap.
        Returns a JSON list of objects as provided by CoinGecko.
        """
        url = f"{self.BASE_URL}/coins/markets"
        params = {
            "vs_currency": currency,
            "order": "market_cap_desc",
            "per_page": per_page,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h",
        }
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def get_coin_history(self, coin_id: str, currency: str = "usd", days: int = 30) -> Dict[str, Any]:
        """Fetch historical market chart for a coin.
        Returns a JSON with arrays of prices/market_caps/total_volumes.
        """
        url = f"{self.BASE_URL}/coins/{coin_id}/market_chart"
        params = {
            "vs_currency": currency,
            "days": days,
            "interval": "daily",
        }
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def get_global(self) -> Dict[str, Any]:
        """Fetch global crypto market data (total market cap, dominance, etc.)."""
        url = f"{self.BASE_URL}/global"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.json()

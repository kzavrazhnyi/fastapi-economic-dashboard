"""
Моделі даних для економетричної статистики торгівельно-виробничого підприємства
"""

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from enum import Enum


class ProductCategory(str, Enum):
    """Категорії продуктів"""
    ELECTRONICS = "electronics"
    CLOTHING = "clothing"
    FOOD = "food"
    BOOKS = "books"
    HOME = "home"
    SPORTS = "sports"


class Region(str, Enum):
    """Регіони продажу"""
    KYIV = "kyiv"
    KHARKIV = "kharkiv"
    LVIV = "lviv"
    ODESSA = "odessa"
    DNIPRO = "dnipro"


class SalesData(BaseModel):
    """Модель даних про продажі"""
    id: int
    date: datetime
    product_name: str
    category: ProductCategory
    quantity: int
    unit_price: float
    total_revenue: float
    region: Region
    customer_id: int


class InventoryData(BaseModel):
    """Модель даних про запаси"""
    id: int
    product_name: str
    category: ProductCategory
    current_stock: int
    min_stock: int
    max_stock: int
    unit_cost: float
    last_updated: datetime


class ProfitData(BaseModel):
    """Модель даних про прибутковість"""
    id: int
    product_name: str
    category: ProductCategory
    unit_cost: float
    unit_price: float
    profit_margin: float
    profit_percentage: float
    total_profit: float


class TrendData(BaseModel):
    """Модель даних для часових рядів"""
    date: datetime
    total_revenue: float
    total_profit: float
    total_sales: int
    avg_order_value: float


class StatsData(BaseModel):
    """Модель загальної статистики"""
    total_revenue: float
    total_profit: float
    total_sales: int
    avg_profit_margin: float
    top_product: str
    top_region: str
    inventory_turnover: float


# Crypto models
class CoinMarketData(BaseModel):
    id: str
    symbol: str
    name: str
    image: str
    current_price: float
    market_cap: Optional[int] = None
    market_cap_rank: Optional[int] = None
    total_volume: Optional[float] = None
    price_change_percentage_24h: Optional[float] = None

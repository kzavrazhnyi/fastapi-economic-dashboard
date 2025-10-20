# -*- coding: utf-8 -*-
"""
Генератор демо-даних для економетричної статистики торгівельно-виробничого підприємства
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os
import sys
from typing import List
from .models import (
    SalesData, InventoryData, ProfitData, TrendData, StatsData,
    ProductCategory, Region
)

# Налаштування кодування для Windows (безпечний спосіб)
if sys.platform == "win32":
    try:
        import locale
        locale.setlocale(locale.LC_ALL, 'uk_UA.UTF-8')
    except:
        pass

# Ініціалізація Faker для генерації випадкових даних
fake = Faker('uk_UA')  # Українська локаль для реалістичних даних

# Налаштування для генерації даних
PRODUCTS = {
    ProductCategory.ELECTRONICS: [
        "Смартфон Samsung Galaxy", "Ноутбук Dell Inspiron", "Планшет iPad",
        "Навушники AirPods", "Камера Canon EOS", "Телевізор LG OLED"
    ],
    ProductCategory.CLOTHING: [
        "Джинси Levis", "Футболка Nike", "Куртка The North Face",
        "Взуття Adidas", "Сукня Zara", "Светр H&M"
    ],
    ProductCategory.FOOD: [
        "Хліб домашній", "Молоко 3.2%", "Сир Гауда", "Йогурт грецький",
        "М'ясо курки", "Овочі сезонні"
    ],
    ProductCategory.BOOKS: [
        "Роман '1984'", "Підручник математики", "Детектив Агати Крісті",
        "Енциклопедія", "Книга рецептів", "Журнал мод"
    ],
    ProductCategory.HOME: [
        "Диван модульний", "Стол обідній", "Лампа настільна",
        "Килим перський", "Ваза декоративна", "Горщик для квітів"
    ],
    ProductCategory.SPORTS: [
        "Гантелі 10кг", "Велосипед горний", "Кросівки для бігу",
        "М'яч футбольний", "Ракетка тенісна", "Абонемент у спортзал"
    ]
}

# Ціни та собівартість для різних категорій
PRICE_RANGES = {
    ProductCategory.ELECTRONICS: {"min_price": 500, "max_price": 5000, "cost_ratio": 0.6},
    ProductCategory.CLOTHING: {"min_price": 50, "max_price": 500, "cost_ratio": 0.4},
    ProductCategory.FOOD: {"min_price": 10, "max_price": 100, "cost_ratio": 0.7},
    ProductCategory.BOOKS: {"min_price": 20, "max_price": 200, "cost_ratio": 0.5},
    ProductCategory.HOME: {"min_price": 100, "max_price": 2000, "cost_ratio": 0.5},
    ProductCategory.SPORTS: {"min_price": 30, "max_price": 800, "cost_ratio": 0.6}
}


class DataGenerator:
    """Клас для генерації демо-даних"""
    
    def __init__(self):
        self.data_dir = "data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def generate_sales_data(self, days: int = 365, records_per_day: int = 50) -> List[SalesData]:
        """Генерація даних про продажі"""
        sales_data = []
        record_id = 1
        
        # Додаємо сезонність та тренди
        base_date = datetime.now() - timedelta(days=days)
        
        for day in range(days):
            current_date = base_date + timedelta(days=day)
            
            # Сезонність: більше продажів у вихідні та перед святами
            weekend_multiplier = 1.5 if current_date.weekday() >= 5 else 1.0
            holiday_multiplier = 2.0 if current_date.month in [12, 1, 6] else 1.0
            daily_records = int(records_per_day * weekend_multiplier * holiday_multiplier)
            
            for _ in range(daily_records):
                category = random.choice(list(ProductCategory))
                product_name = random.choice(PRODUCTS[category])
                quantity = random.randint(1, 10)
                
                price_range = PRICE_RANGES[category]
                unit_price = round(random.uniform(price_range["min_price"], price_range["max_price"]), 2)
                total_revenue = quantity * unit_price
                
                sales_data.append(SalesData(
                    id=record_id,
                    date=current_date,
                    product_name=product_name,
                    category=category,
                    quantity=quantity,
                    unit_price=unit_price,
                    total_revenue=total_revenue,
                    region=random.choice(list(Region)),
                    customer_id=random.randint(1000, 9999)
                ))
                record_id += 1
        
        return sales_data
    
    def generate_inventory_data(self) -> List[InventoryData]:
        """Генерація даних про запаси"""
        inventory_data = []
        
        for category, products in PRODUCTS.items():
            for product_name in products:
                price_range = PRICE_RANGES[category]
                unit_cost = round(random.uniform(
                    price_range["min_price"] * price_range["cost_ratio"],
                    price_range["max_price"] * price_range["cost_ratio"]
                ), 2)
                
                current_stock = random.randint(10, 500)
                min_stock = random.randint(5, 50)
                max_stock = current_stock + random.randint(100, 1000)
                
                inventory_data.append(InventoryData(
                    id=len(inventory_data) + 1,
                    product_name=product_name,
                    category=category,
                    current_stock=current_stock,
                    min_stock=min_stock,
                    max_stock=max_stock,
                    unit_cost=unit_cost,
                    last_updated=datetime.now() - timedelta(days=random.randint(1, 30))
                ))
        
        return inventory_data
    
    def generate_profit_data(self, sales_data: List[SalesData]) -> List[ProfitData]:
        """Генерація даних про прибутковість на основі продажів"""
        profit_data = []
        
        # Групуємо продажі за продуктами
        product_stats = {}
        for sale in sales_data:
            if sale.product_name not in product_stats:
                product_stats[sale.product_name] = {
                    'category': sale.category,
                    'total_revenue': 0,
                    'total_quantity': 0,
                    'avg_price': 0
                }
            
            product_stats[sale.product_name]['total_revenue'] += sale.total_revenue
            product_stats[sale.product_name]['total_quantity'] += sale.quantity
        
        # Розраховуємо середню ціну
        for product_name, stats in product_stats.items():
            stats['avg_price'] = stats['total_revenue'] / stats['total_quantity']
        
        # Генеруємо дані про прибутковість
        for product_name, stats in product_stats.items():
            category = stats['category']
            price_range = PRICE_RANGES[category]
            
            unit_price = stats['avg_price']
            unit_cost = round(unit_price * random.uniform(
                price_range["cost_ratio"] * 0.8,
                price_range["cost_ratio"] * 1.2
            ), 2)
            
            profit_margin = unit_price - unit_cost
            profit_percentage = round((profit_margin / unit_price) * 100, 2)
            total_profit = profit_margin * stats['total_quantity']
            
            profit_data.append(ProfitData(
                id=len(profit_data) + 1,
                product_name=product_name,
                category=category,
                unit_cost=unit_cost,
                unit_price=unit_price,
                profit_margin=profit_margin,
                profit_percentage=profit_percentage,
                total_profit=total_profit
            ))
        
        return profit_data
    
    def generate_trend_data(self, sales_data: List[SalesData]) -> List[TrendData]:
        """Генерація часових рядів для трендів"""
        trend_data = []
        
        # Групуємо продажі за днями
        daily_stats = {}
        for sale in sales_data:
            date_key = sale.date.date()
            if date_key not in daily_stats:
                daily_stats[date_key] = {
                    'total_revenue': 0,
                    'total_sales': 0,
                    'orders': 0
                }
            
            daily_stats[date_key]['total_revenue'] += sale.total_revenue
            daily_stats[date_key]['total_sales'] += sale.quantity
            daily_stats[date_key]['orders'] += 1
        
        # Розраховуємо тренди
        for date, stats in daily_stats.items():
            avg_order_value = stats['total_revenue'] / stats['orders'] if stats['orders'] > 0 else 0
            
            # Приблизний розрахунок прибутку (30% маржа в середньому)
            total_profit = stats['total_revenue'] * 0.3
            
            trend_data.append(TrendData(
                date=datetime.combine(date, datetime.min.time()),
                total_revenue=round(stats['total_revenue'], 2),
                total_profit=round(total_profit, 2),
                total_sales=stats['total_sales'],
                avg_order_value=round(avg_order_value, 2)
            ))
        
        return sorted(trend_data, key=lambda x: x.date)
    
    def generate_stats_data(self, sales_data: List[SalesData], profit_data: List[ProfitData]) -> StatsData:
        """Генерація загальної статистики"""
        total_revenue = sum(sale.total_revenue for sale in sales_data)
        total_profit = sum(profit.total_profit for profit in profit_data)
        total_sales = sum(sale.quantity for sale in sales_data)
        
        # Топ продукт за виручкою
        product_revenue = {}
        for sale in sales_data:
            product_revenue[sale.product_name] = product_revenue.get(sale.product_name, 0) + sale.total_revenue
        top_product = max(product_revenue, key=product_revenue.get) if product_revenue else "Немає даних"
        
        # Топ регіон за продажами
        region_sales = {}
        for sale in sales_data:
            region_sales[sale.region] = region_sales.get(sale.region, 0) + sale.quantity
        top_region = max(region_sales, key=region_sales.get) if region_sales else "Немає даних"
        
        avg_profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        inventory_turnover = random.uniform(4.0, 12.0)  # Типові значення для різних галузей
        
        return StatsData(
            total_revenue=round(total_revenue, 2),
            total_profit=round(total_profit, 2),
            total_sales=total_sales,
            avg_profit_margin=round(avg_profit_margin, 2),
            top_product=top_product,
            top_region=top_region,
            inventory_turnover=round(inventory_turnover, 2)
        )
    
    def save_to_csv(self, data: List, filename: str):
        """Збереження даних у CSV файл"""
        if not data:
            return
        
        # Конвертуємо Pydantic моделі в словники
        data_dicts = [item.model_dump() for item in data]
        df = pd.DataFrame(data_dicts)
        
        filepath = os.path.join(self.data_dir, filename)
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"Збережено {len(data)} записів у файл {filepath}")
    
    def generate_all_data(self):
        """Генерація всіх типів даних"""
        print("Початок генерації демо-даних...")
        
        # Генеруємо продажі
        print("Генерація даних про продажі...")
        sales_data = self.generate_sales_data()
        self.save_to_csv(sales_data, "sales.csv")
        
        # Генеруємо запаси
        print("Генерація даних про запаси...")
        inventory_data = self.generate_inventory_data()
        self.save_to_csv(inventory_data, "inventory.csv")
        
        # Генеруємо прибутковість
        print("Генерація даних про прибутковість...")
        profit_data = self.generate_profit_data(sales_data)
        self.save_to_csv(profit_data, "profit.csv")
        
        # Генеруємо тренди
        print("Генерація часових рядів...")
        trend_data = self.generate_trend_data(sales_data)
        self.save_to_csv(trend_data, "trends.csv")
        
        # Генеруємо статистику
        print("Генерація загальної статистики...")
        stats_data = self.generate_stats_data(sales_data, profit_data)
        self.save_to_csv([stats_data], "stats.csv")
        
        print("Генерація даних завершена!")
        return {
            "sales": sales_data,
            "inventory": inventory_data,
            "profit": profit_data,
            "trends": trend_data,
            "stats": stats_data
        }


if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_all_data()

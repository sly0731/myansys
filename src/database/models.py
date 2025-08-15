#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : models.py
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

from dataclasses import dataclass,field
from typing import Dict, List

@dataclass
class StockBasicSchema:
    """股票基础信息表结构"""
    table_name: str = 'stock_basic'
    columns: Dict[str, str] = field(default_factory=lambda:{
        'symbol': 'VARCHAR(10) PRIMARY KEY',
        'name': 'VARCHAR(50) NOT NULL',
        'market': 'VARCHAR(10) NOT NULL',
        'industry': 'VARCHAR(50)',
        'list_date': 'DATE',
        'status': 'SMALLINT DEFAULT 1'
    })
    indexes: List[str] = field(default_factory=lambda: [
        'CREATE INDEX idx_stock_basic_market ON stock_basic (market)',
        'CREATE INDEX idx_stock_basic_status ON stock_basic (status)'
    ])

@dataclass
class StockHotDataSchema:
    """股票热数据表结构模板"""
    table_name_prefix: str = 'stock_hot_'
    columns: Dict[str, str] = field(default_factory=lambda: {
        'trade_date': 'DATE PRIMARY KEY',
        'symbol': 'VARCHAR(10) NOT NULL',
        'open': 'NUMERIC(12,4) NOT NULL',
        'high': 'NUMERIC(12,4) NOT NULL',
        'low': 'NUMERIC(12,4) NOT NULL',
        'close': 'NUMERIC(12,4) NOT NULL',
        'volume': 'BIGINT NOT NULL',
        'amount': 'NUMERIC(20,4) NOT NULL',
        'pct_change': 'NUMERIC(10,6)',
        'turnover_rate': 'NUMERIC(10,6)',
        'amplitude': 'NUMERIC(10,6)',
        'adjust_flag': 'SMALLINT DEFAULT 0',
        'factor_qfq': 'NUMERIC(10,6)',
        'factor_hfq': 'NUMERIC(10,6)',
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    })
    indexes: List[str] = field(default_factory=lambda: [
        'CREATE INDEX ON {table_name} (trade_date)'
    ])

@dataclass
class TradingCalendarSchema:
    """交易日历表结构"""
    table_name: str = 'trading_calendar'
    columns: Dict[str, str] = field(default_factory=lambda: {
        'trade_date': 'DATE PRIMARY KEY',
        'is_trading_day': 'BOOLEAN NOT NULL',
        'exchange': 'VARCHAR(20) DEFAULT \'SSE\'',
        'holiday_name': 'VARCHAR(50)',
        'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    })
    indexes: List[str] = field(default_factory=lambda: [
        'CREATE INDEX idx_trading_calendar_date ON trading_calendar (trade_date)',
        'CREATE INDEX idx_trading_calendar_exchange ON trading_calendar (exchange)'
    ])

@dataclass
class TaskProgressSchema:
    """任务进度表结构"""
    table_name: str = 'task_progress'
    columns: Dict[str, str] = field(default_factory=lambda: {
        'task_id': 'VARCHAR(100) PRIMARY KEY',
        'progress_data': 'JSONB',
        'update_time': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    })
    indexes: List[str] = field(default_factory=lambda: [
        'CREATE INDEX idx_task_progress_update_time ON task_progress (update_time)'
    ])


#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : Untitled-1
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

import re
from typing import Optional, Dict, Any
from datetime import datetime

def format_date(date_str: Any) -> Optional[str]:
    """格式化日期字符串"""
    if not date_str or str(date_str) == 'nan':
        return None
    try:
        if isinstance(date_str, datetime):
            return date_str.strftime('%Y-%m-%d')
        if isinstance(date_str, str):
            if re.match(r'^\d{8}$', date_str):
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                return date_str
        return str(date_str)
    except:
        return None

def validate_symbol(symbol: str) -> bool:
    """验证股票代码是否有效"""
    return symbol.isalnum() and len(symbol) == 6

def build_stock_data_dict(
    symbol: str,
    trade_date: str,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    amount: float,
    pct_change: Optional[float] = None,
    turnover_rate: Optional[float] = None,
    amplitude: Optional[float] = None,
    qfq_factor: Optional[float] = None,
    hfq_factor: Optional[float] = None
) -> Dict[str, Any]:
    """构建股票数据字典"""
    return {
        'symbol': symbol,
        'trade_date': trade_date,
        'open': open_price,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'amount': amount,
        'pct_change': pct_change,
        'turnover_rate': turnover_rate,
        'amplitude': amplitude,
        'adjust_flag': 0,
        'factor_qfq': qfq_factor,
        'factor_hfq': hfq_factor
    }
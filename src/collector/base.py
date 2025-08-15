#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : Untitled-1
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional

class DataCollector(ABC):
    """数据采集器抽象基类"""
    
    @abstractmethod
    def fetch_basic_info(self) -> List[Dict]:
        """获取股票基础信息"""
        pass
    
    @abstractmethod
    def fetch_history_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = ""
    ) -> List[Dict]:
        """获取历史交易数据"""
        pass
    
    @abstractmethod
    def save_to_database(self, data: List[Dict]) -> int:
        """保存数据到数据库"""
        pass
    
    @abstractmethod
    def is_trading_day(self, date: str) -> bool:
        """判断是否为交易日"""
        pass
    
    @abstractmethod
    def get_previous_trading_day(self, date: str) -> Optional[str]:
        """获取上一个交易日"""
        pass
    
    @abstractmethod
    def get_next_trading_day(self, date: str) -> Optional[str]:
        """获取下一个交易日"""
        pass
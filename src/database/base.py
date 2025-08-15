#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : base.py
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union, Any

class DatabaseInterface(ABC):
    """数据库操作抽象接口"""
    
    @abstractmethod
    def execute(
        self, 
        query: str, 
        params: Optional[Union[tuple, dict, list]] = None,
        fetch: bool = False,
        many: bool = False
    ) -> Union[bool, List[Dict]]:
        """执行SQL语句"""
        pass
    
    @abstractmethod
    def ensure_table_exists(self, table_name: str) -> bool:
        """确保表存在"""
        pass
    
    @abstractmethod
    def insert(self, table_name: str, data: Dict) -> bool:
        """插入单条数据"""
        pass
    
    @abstractmethod
    def bulk_insert(self, table_name: str, data: List[Dict]) -> int:
        """批量插入数据"""
        pass
    
    @abstractmethod
    def bulk_upsert(self, table_name: str, data: List[Dict], update_fields: List[str]) -> int:
        """批量插入或更新数据"""
        pass
    
    @abstractmethod
    def query(
        self,
        table_name: str,
        conditions: Optional[Dict] = None,
        fields: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        ascending: bool = True,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """查询数据"""
        pass
    
    @abstractmethod
    def delete(self, table_name: str, conditions: Dict) -> int:
        """删除数据"""
        pass
    
    @abstractmethod
    def get_stock_table_name(self, symbol: str) -> str:
        """获取股票对应的表名"""
        pass
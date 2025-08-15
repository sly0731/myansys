#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : Untitled-1
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

import pandas as pd
import pandas_market_calendars as mcal
import akshare as ak
from typing import Optional, Union, List, Dict, Any
from datetime import datetime, date
from ..database.manager import StockDatabaseManager
from .date_utils import parse_date

class TradingDayChecker:
    """交易日检查器"""
    
    def __init__(self, db_manager: StockDatabaseManager):
        self.db = db_manager
    
    def is_trading_day(
        self, 
        date: Union[str, date, datetime], 
        method: str = 'auto',
        exchange: str = 'SSE',
        update_local: bool = False
    ) -> bool:
        """判断是否为交易日"""
        date_obj = parse_date(date)
        date_str = date_obj.strftime('%Y-%m-%d')
        
        if method == 'auto':
            # 优先从数据库查询
            result = self.db.query(
                'trading_calendar',
                conditions={'trade_date': date_str, 'exchange': exchange},
                fields=['is_trading_day']
            )
            
            if result:
                return bool(result[0]['is_trading_day'])
            # 从交易所日历查询
            exchange_result = self._check_with_exchange(date_obj, exchange)

            if update_local:
                self._update_local_calendar([date_obj], exchange_result, exchange)
            return exchange_result
        
        elif method == 'database':
            result = self.db.query(
                'trading_calendar',
                conditions={'trade_date': date_str, 'exchange': exchange},
                fields=['is_trading_day']
            )
            if not result:
                raise ValueError("数据库中无此日期记录")
            return result[0]['is_trading_day']
            
        elif method == 'exchange':
            result = self._check_with_exchange(date_obj, exchange)
            if update_local:
                self._update_local_calendar([date_obj], result, exchange)
            return result
            
        elif method == 'akshare':
            result = self._check_with_ak(date_obj)
            if update_local:
                self._update_local_calendar([date_obj], result, exchange)
            return result
            
        else:
            raise ValueError("无效的方法，请选择 'auto', 'database', 'exchange' 或 'akshare'")
    
    def _check_with_exchange(self, date: date, exchange: str) -> bool:
        """使用交易所日历检查"""
        try:
            exchange_cal = mcal.get_calendar(exchange)
            schedule = exchange_cal.schedule(
                start_date=date,
                end_date=date
            )
            return not schedule.empty
        except Exception as e:
            raise ValueError(f"无法获取 {exchange} 交易所日历: {e}")
    
    def _check_with_ak(self, date: date) -> bool:
        """使用akshare检查"""
        try:
            trade_dates = ak.tool_trade_date_hist_sina()
            trade_dates['trade_date'] = pd.to_datetime(trade_dates['trade_date']).dt.date
            return date in trade_dates['trade_date'].values
        except Exception as e:
            raise ValueError(f"无法从akshare获取交易日历: {e}")
    
    def _update_local_calendar(
        self, 
        dates: List[date], 
        is_trading_day: Union[bool, List[Dict[str, Any]]],  # 修改参数类型
        exchange: str
    ) -> None:
        """更新本地交易日历"""
        data = [{
        'trade_date': date.strftime('%Y-%m-%d'),
        'is_trading_day': is_trading_day,
        'exchange': exchange,
        'holiday_name': None if is_trading_day else '节假日'
    } for date in dates]
        
        self.db.bulk_upsert(
            'trading_calendar',
            data,
            update_fields=['is_trading_day', 'holiday_name']
        )
    
    def update_calendar(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchange: str = 'SSE',
        source: str = 'exchange'
    ):
        """更新交易日历"""
        start = parse_date(start_date)
        end = parse_date(end_date)
        
        if source == 'exchange':
            self._update_from_exchange(start, end, exchange)
        elif source == 'akshare':
            self._update_from_ak(start, end, exchange)
        else:
            raise ValueError("无效的数据源，请选择 'exchange' 或 'akshare'")
    
    def _update_from_exchange(self, start: date, end: date, exchange: str)-> None:
        """从交易所日历更新"""
        try:
            exchange_cal = mcal.get_calendar(exchange)
            schedule = exchange_cal.schedule(start_date=start, end_date=end)
            
            all_dates = pd.date_range(start, end)
            trading_days = schedule.index.date if not schedule.empty else []
            
            data = []
            for date in all_dates:
                data.append({
                    'trade_date': date.strftime('%Y-%m-%d'),
                    'is_trading_day': date in trading_days,
                    'exchange': exchange,
                    'holiday_name': None if date in trading_days else '节假日'
                })
            
            self.db.bulk_upsert(
                'trading_calendar',
                data,
                update_fields=['is_trading_day', 'holiday_name']
            )
            
            logger.info(f"成功更新 {len(data)} 条交易日历记录")
            
        except Exception as e:
            raise ValueError(f"从交易所 {exchange} 更新日历失败: {e}")
    
    def _update_from_ak(self, start: date, end: date, exchange: str)-> None:
        """从akshare更新"""
        try:
            trade_dates = ak.tool_trade_date_hist_sina()
            trade_dates['trade_date'] = pd.to_datetime(trade_dates['trade_date']).dt.date
            
            all_dates = pd.date_range(start, end)
            
            data = []
            for date in all_dates:
                date_obj = date.date()
                data.append({
                    'trade_date': date_obj.strftime('%Y-%m-%d'),
                    'is_trading_day': date_obj in trade_dates['trade_date'].values,
                    'exchange': exchange,
                    'holiday_name': None if date_obj in trade_dates['trade_date'].values else '节假日'
                })
            
            self.db.bulk_upsert(
                'trading_calendar',
                data,
                update_fields=['is_trading_day', 'holiday_name']
            )
            
            logger.info(f"成功更新 {len(data)} 条交易日历记录")
            
        except Exception as e:
            raise ValueError(f"从akshare更新日历失败: {e}")
    
    def get_trading_days(
        self,
        start_date: Union[str, date, datetime],
        end_date: Union[str, date, datetime],
        exchange: str = 'SSE',
        only_trading_days: bool = True
    ) -> List[Dict[str, Any]]:
        """获取交易日历"""
        start = parse_date(start_date)
        end = parse_date(end_date)
        
        conditions = {
            'trade_date': {
                'operator': 'BETWEEN',
                'value': [start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')]
            },
            'exchange': exchange
        }
        
        if only_trading_days:
            conditions['is_trading_day'] = True
        
        return self.db.query(
            'trading_calendar',
            conditions=conditions,
            order_by='trade_date',
            ascending=True
        )
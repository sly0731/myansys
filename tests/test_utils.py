#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : test_utils.py
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

import pytest
from typing import List, Union, Optional, Dict
from datetime import date, datetime
from unittest.mock import MagicMock
from src.utils.date_utils import parse_date, format_date_to_str
from src.utils.checker import TradingDayChecker
from src.utils.decorators import retry

class TestDateUtils:
    """日期工具测试"""
    
    def test_parse_date(self):
        """测试日期解析"""
        # 测试字符串日期
        assert parse_date("20230101") == date(2023, 1, 1)
        assert parse_date("2023-01-01") == date(2023, 1, 1)
        
        # 测试date对象
        d = date(2023, 1, 1)
        assert parse_date(d) == d
        
        # 测试datetime对象
        dt = datetime(2023, 1, 1)
        assert parse_date(dt) == date(2023, 1, 1)
        
        # 测试无效输入
        with pytest.raises(ValueError):
            parse_date("invalid_date")
    
    def test_format_date_to_str(self):
        """测试日期格式化"""
        d = date(2023, 1, 1)
        assert format_date_to_str(d) == "2023-01-01"
        
        dt = datetime(2023, 1, 1)
        assert format_date_to_str(dt) == "2023-01-01"

class TestTradingDayChecker:
    """交易日检查器测试"""
    
    @pytest.fixture
    def checker(self):
        mock_db = MagicMock()
        return TradingDayChecker(mock_db)
    
    def test_is_trading_day(self, checker):
        """测试交易日判断"""
        checker.db.query.return_value = [{'is_trading_day': True}]
        assert checker.is_trading_day("2023-01-03") is True
        
        checker.db.query.return_value = []
        with patch('pandas_market_calendars.get_calendar') as mock_cal:
            mock_cal.return_value.schedule.return_value = MagicMock()
            assert checker.is_trading_day("2023-01-03", method='exchange') is True
    
    def test_update_calendar(self, checker):
        """测试更新交易日历"""
        with patch.object(checker, '_update_from_exchange') as mock_update:
            checker.update_calendar("20230101", "20230110")
            mock_update.assert_called_once()

class TestDecorators:
    """装饰器测试"""
    
    def test_retry_success(self):
        """测试重试装饰器-成功情况"""
        mock_func = MagicMock(return_value=True)
        decorated = retry(max_retries=3)(mock_func)
        assert decorated() is True
        mock_func.assert_called_once()
    
    def test_retry_failure(self):
        """测试重试装饰器-失败情况"""
        mock_func = MagicMock(side_effect=Exception("test error"))
        decorated = retry(max_retries=2)(mock_func)
        
        with pytest.raises(Exception):
            decorated()
        
        assert mock_func.call_count == 2
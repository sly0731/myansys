#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : Untitled-1
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from src.collector.a_stock import AStockCollector
from src.database.manager import StockDatabaseManager

class TestAStockCollector:
    """A股采集器测试类"""
    
    @pytest.fixture
    def mock_db(self):
        return MagicMock(spec=StockDatabaseManager)
    
    @pytest.fixture
    def collector(self, mock_db):
        return AStockCollector(mock_db)
    
    @patch('akshare.stock_info_a_code_name')
    def test_fetch_basic_info(self, mock_ak, collector):
        """测试获取基础信息"""
        # 准备模拟数据
        mock_df = pd.DataFrame({
            'code': ['600000', '000001'],
            'name': ['浦发银行', '平安银行']
        })
        mock_ak.return_value = mock_df
        
        # 模拟深交所数据
        with patch('akshare.stock_info_sz_name_code') as mock_sz:
            mock_sz.return_value = pd.DataFrame({
                'A股代码': ['000001'],
                '所属行业': ['银行']
            })
            
            # 模拟上交所数据
            with patch('akshare.stock_info_sh_name_code') as mock_sh:
                mock_sh.return_value = pd.DataFrame({
                    '证券代码': ['600000'],
                    '上市日期': ['1999-11-10']
                })
                
                result = collector.fetch_basic_info()
                assert len(result) == 2
                assert result[0]['symbol'] == '600000'
    
    @patch('akshare.stock_zh_a_hist')
    def test_fetch_history_data(self, mock_hist, collector):
        """测试获取历史数据"""
        # 准备模拟K线数据
        mock_kline = pd.DataFrame({
            '日期': ['2023-01-03', '2023-01-04'],
            '开盘': [10.0, 10.2],
            '收盘': [10.5, 10.6]
        })
        mock_hist.return_value = mock_kline
        
        # 模拟复权因子数据
        with patch('akshare.stock_zh_a_daily') as mock_factor:
            mock_factor.return_value = pd.DataFrame({
                'date': ['2023-01-01'],
                'qfq_factor': [1.0]
            })
            
            result = collector.fetch_history_data(
                symbol='600000',
                start_date='20230101',
                end_date='20230110'
            )
            assert len(result) == 2
            assert result[0]['open'] == 10.0
    
    def test_save_to_database(self, collector, mock_db):
        """测试保存数据到数据库"""
        test_data = [{
            'symbol': '600000',
            'trade_date': '2023-01-01',
            'open': 10.0
        }]
        
        mock_db.bulk_upsert.return_value = 1
        result = collector.save_to_database(test_data)
        assert result == 1
        mock_db.bulk_upsert.assert_called_once()
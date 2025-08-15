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
from datetime import date
from src.database.manager import StockDatabaseManager
from src.database.exceptions import DatabaseError

class TestStockDatabaseManager:
    """数据库管理器测试类"""
    
    @pytest.fixture
    def mock_db_config(self):
        return {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_pass'
        }
    
    @pytest.fixture
    def db_manager(self, mock_db_config):
        with patch('psycopg2.pool.SimpleConnectionPool') as mock_pool:
            mock_pool.return_value.getconn.return_value = MagicMock()
            return StockDatabaseManager(config=mock_db_config)
    
    def test_ensure_table_exists(self, db_manager):
        """测试确保表存在"""
        with patch.object(db_manager, 'execute') as mock_execute:
            mock_execute.return_value = [{'exists': False}]
            result = db_manager.ensure_table_exists('stock_hot_600000')
            assert result is True
            mock_execute.assert_called()
    
    def test_insert_data(self, db_manager):
        """测试插入数据"""
        test_data = {
            'trade_date': '2023-01-01',
            'symbol': '600000',
            'open': 10.0,
            'close': 10.5
        }
        
        with patch.object(db_manager, 'execute') as mock_execute:
            mock_execute.return_value = True
            result = db_manager.insert('stock_hot_600000', test_data)
            assert result is True
    
    def test_bulk_insert(self, db_manager):
        """测试批量插入"""
        test_data = [{
            'trade_date': '2023-01-01',
            'symbol': '600000',
            'open': 10.0
        }]
        
        with patch.object(db_manager, 'execute') as mock_execute:
            mock_execute.return_value = 1
            result = db_manager.bulk_insert('stock_hot_600000', test_data)
            assert result == 1
    
    def test_query_data(self, db_manager):
        """测试查询数据"""
        with patch.object(db_manager, 'execute') as mock_execute:
            mock_execute.return_value = [{
                'trade_date': '2023-01-01',
                'close': 10.5
            }]
            result = db_manager.query(
                'stock_hot_600000',
                conditions={'symbol': '600000'},
                fields=['trade_date', 'close']
            )
            assert len(result) == 1
            assert 'close' in result[0]
    
    def test_error_handling(self, db_manager):
        """测试错误处理"""
        with patch.object(db_manager, 'execute', side_effect=Exception('DB error')):
            with pytest.raises(DatabaseError):
                db_manager.query('nonexistent_table')
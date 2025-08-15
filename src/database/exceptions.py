#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : exceptions.py
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

class DatabaseError(Exception):
    """数据库操作基础异常"""
    pass

class ConnectionError(DatabaseError):
    """数据库连接异常"""
    pass

class QueryError(DatabaseError):
    """查询异常"""
    pass

class TableExistsError(DatabaseError):
    """表已存在异常"""
    pass

class InvalidSymbolError(DatabaseError):
    """无效股票代码异常"""
    pass

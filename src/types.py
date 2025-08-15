#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : types.py
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

# src/types.py
from typing import Any, Dict, List, Tuple, Union
from psycopg2.sql import Composed, SQL

# 常用类型别名
QueryParam = Union[Tuple[Any, ...], Dict[str, Any], List[Any]]
QueryType = Union[str, Composed, SQL]
ExecuteResult = Union[bool, List[Dict[str, Any]]]
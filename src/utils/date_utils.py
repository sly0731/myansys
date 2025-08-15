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
from datetime import datetime, date, timedelta
from typing import List,Union, Optional

def parse_date(date_str: Union[str, date, datetime]) -> date:
    """解析日期为date对象"""
    if isinstance(date_str, str):
        try:
            if re.match(r'^\d{8}$', date_str):
                return datetime.strptime(date_str, '%Y%m%d').date()
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            raise ValueError("不支持的日期格式")
        except ValueError as e:
            raise ValueError(f"日期字符串格式应为 'YYYY-MM-DD' 或 'YYYYMMDD': {e}")
    elif isinstance(date_str, datetime):
        return date_str.date()
    elif isinstance(date_str, date):
        return date_str
    else:
        raise TypeError("日期应为字符串、date或datetime对象")

def format_date_to_str(date_obj: Union[date, datetime]) -> str:
    """将日期对象格式化为字符串"""
    if isinstance(date_obj, datetime):
        return date_obj.strftime('%Y-%m-%d')
    if isinstance(date_obj, date):
        return date_obj.strftime('%Y-%m-%d')
    raise TypeError("输入必须是date或datetime对象")

def get_date_range(start_date: Union[str, date, datetime], 
                  end_date: Union[str, date, datetime]) -> List[date]:
    """获取日期范围内的所有日期"""
    start = parse_date(start_date)
    end = parse_date(end_date)
    return [start + timedelta(days=x) for x in range((end - start).days + 1)]
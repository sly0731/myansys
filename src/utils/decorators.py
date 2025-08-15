#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : Untitled-1
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

import time
import logging
from typing import Callable, Any
from functools import wraps

logger = logging.getLogger(__name__)

def retry(max_retries: int = 3, delay: float = 1):
    """重试装饰器"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(
                        f"尝试 {attempt}/{max_retries} 失败: {str(e)}"
                    )
                    if attempt < max_retries:
                        time.sleep(delay * attempt)
            raise last_exception
        return wrapper
    return decorator

def log_execution_time(func: Callable):
    """记录执行时间装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        logger.info(
            f"函数 {func.__name__} 执行时间: {end_time - start_time:.4f} 秒"
        )
        return result
    return wrapper
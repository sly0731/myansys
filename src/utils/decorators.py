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
import random
from typing import Callable, TypeVar, Any, Optional, Type, Union, Tuple, cast
from functools import wraps

logger = logging.getLogger(__name__)

# 定义类型变量
F = TypeVar('F', bound=Callable[..., Any])  # 函数类型

def retry(
    max_retries: int = 3,
    delay: float = 1,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    jitter: float = 0.1
) -> Callable[[F], F]:
    """重试装饰器
    
    Args:
        max_retries: 最大重试次数，默认为3
        delay: 基础重试间隔时间(秒)，默认为1
        exceptions: 要捕获的异常类型，默认为所有异常
        jitter: 随机抖动系数，避免同时重试
    
    Returns:
        装饰后的函数
    """
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Optional[Exception] = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    logger.warning(
                        f"尝试 {attempt}/{max_retries} 失败: {str(e)}"
                    )
                    if attempt < max_retries:
                        sleep_time = delay * attempt * (1 + jitter * (random.random() - 0.5))
                        time.sleep(sleep_time)
            raise cast(Exception, last_exception)
        return cast(F, wrapper)
    return decorator

def log_execution_time(func: F) -> F:
    """记录函数执行时间的装饰器
    
    Args:
        func: 要装饰的函数
    
    Returns:
        装饰后的函数
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start_time: float = time.perf_counter()
        result: Any = func(*args, **kwargs)
        end_time: float = time.perf_counter()
        logger.info(
            f"函数 {func.__name__} 执行时间: {end_time - start_time:.4f} 秒"
        )
        return result
    return cast(F, wrapper)
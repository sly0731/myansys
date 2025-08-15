#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : Untitled-1
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

import psycopg2
from psycopg2 import sql, pool, extras
from psycopg2.sql import Composed
import logging
from typing import Dict, List, Optional, Union, Any, Tuple
from contextlib import contextmanager
import configparser
import os
from datetime import datetime

from .base import DatabaseInterface
from .exceptions import *
from .models import (
    StockBasicSchema, 
    StockHotDataSchema,
    TradingCalendarSchema,
    TaskProgressSchema
)

logger = logging.getLogger(__name__)

class StockDatabaseManager(DatabaseInterface):
    """PostgreSQL/TimescaleDB数据库管理器实现"""
    
    def __init__(self, config_file: str = 'config.ini', section: str = 'postgresql'):
        """
        初始化数据库管理器
        
        Args:
            config_file: 配置文件路径
            section: 配置文件中数据库配置的节名
        """
        self.db_config = self._load_db_config(config_file, section)
        self._init_connection_pool()
        self._init_schemas()
        self._ensure_base_structure()
        self.connection = None

    def _load_db_config(self, config_file: str, section: str) -> Dict:
        """加载数据库配置"""
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"配置文件 {config_file} 不存在")
        
        config = configparser.ConfigParser()
        config.read(config_file)
        
        if section not in config:
            raise KeyError(f"配置文件中找不到节 [{section}]")
            
        required_keys = ['host', 'database', 'user', 'password']
        for key in required_keys:
            if not config[section].get(key):
                raise ValueError(f"配置文件中缺少必需的键: {key}")
            
        return {
            'host': config[section].get('host'),
            'database': config[section].get('database'),
            'user': config[section].get('user'),
            'password': config[section].get('password'),
            'port': config[section].get('port', '5432')
        }
    
    def _init_connection_pool(self)-> None:
        """初始化连接池"""
        try:
            self.pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                **self.db_config
            )
            logger.info("数据库连接池初始化成功")
        except Exception as e:
            logger.error("数据库连接池初始化失败: %s", e)
            raise ConnectionError("Failed to initialize connection pool") from e
    
    def _init_schemas(self):
        """初始化表结构定义"""
        self.schemas = {
            'stock_basic': StockBasicSchema(),
            'trading_calendar': TradingCalendarSchema(),
            'task_progress': TaskProgressSchema(),
            'stock_hot': StockHotDataSchema()
        }
    
    def _ensure_base_structure(self)->None:
        """确保基础数据库结构存在"""
        try:
            # 创建TimescaleDB扩展
            self.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            
            # 创建基础表
            self._create_table_if_not_exists('stock_basic')
            self._create_table_if_not_exists('trading_calendar')
            self._create_table_if_not_exists('task_progress')
            
            # 创建动态生成股票表的函数
            self._create_stock_hot_table_function()
            
            logger.info("基础数据库结构已初始化")
        except Exception as e:
            logger.error("初始化基础数据库结构失败: %s", e)
            raise
    
    def _create_table_if_not_exists(self, table_type: str)-> None:
        """创建表如果不存在"""
        schema = self.schemas[table_type]
        columns_sql = ', '.join([f"{col} {dtype}" for col, dtype in schema.columns.items()])
        
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema.table_name} (
                {columns_sql}
            )
        """
        self.execute(create_table_sql)
        
        # 创建索引
        for index_sql in schema.indexes:
            self.execute(index_sql.format(table_name=schema.table_name))
    
    def _create_stock_hot_table_function(self)-> None:

        """创建动态生成股票表的函数"""
        create_function_sql = """
        CREATE OR REPLACE FUNCTION create_stock_hot_table(symbol VARCHAR) 
        RETURNS VOID AS $func$
        BEGIN
            EXECUTE format('
                CREATE TABLE IF NOT EXISTS %I (
                    trade_date DATE PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL DEFAULT %L,
                    open NUMERIC(12,4) NOT NULL,
                    high NUMERIC(12,4) NOT NULL,
                    low NUMERIC(12,4) NOT NULL,
                    close NUMERIC(12,4) NOT NULL,
                    volume BIGINT NOT NULL,
                    amount NUMERIC(20,4) NOT NULL,
                    pct_change NUMERIC(10,6),
                    turnover_rate NUMERIC(10,6),
                    amplitude NUMERIC(10,6),
                    adjust_flag SMALLINT DEFAULT 0,
                    factor_qfq NUMERIC(10,6),
                    factor_hfq NUMERIC(10,6),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )', 'stock_hot_' || symbol, symbol);
            
            EXECUTE format('
                CREATE INDEX IF NOT EXISTS %I 
                ON %I (trade_date)', 
                'idx_stock_hot_' || symbol || '_date', 
                'stock_hot_' || symbol);
            
            EXECUTE format('
                COMMENT ON TABLE %I IS %L', 
                'stock_hot_' || symbol, 
                '股票' || symbol || '的热度数据表');
            
            EXECUTE format('
                COMMENT ON COLUMN %I.adjust_flag IS %L', 
                'stock_hot_' || symbol, 
                '0:不复权,1:前复权,2:后复权');
            
            EXECUTE format('
                ALTER TABLE %I SET (
                    autovacuum_enabled = true,
                    toast.autovacuum_enabled = true
                )', 'stock_hot_' || symbol);
            
            EXECUTE $trigger$
            CREATE OR REPLACE FUNCTION update_stock_hot_timestamp()
            RETURNS TRIGGER AS $trigger_func$
            BEGIN
                NEW.updated_at := CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $trigger_func$ LANGUAGE plpgsql
            $trigger$;
            
            EXECUTE format('
                CREATE TRIGGER %I
                BEFORE UPDATE ON %I
                FOR EACH ROW EXECUTE FUNCTION update_stock_hot_timestamp()', 
                'update_stock_hot_' || symbol || '_timestamp',
                'stock_hot_' || symbol);
        END;
        $func$ LANGUAGE plpgsql;
        """
        self.execute(create_function_sql)
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接"""
        conn = self.pool.getconn()
        conn.autocommit = False
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error("数据库操作失败: %s", e)
            raise DatabaseError(str(e)) from e
        finally:
            self.pool.putconn(conn)
    
    def execute(
        self, 
        query: sql.Composed, 
        params: tuple = (), 
        fetch: bool = True
    ) -> Union[List[Dict[str, Any]], int, None]:
        """执行SQL语句（基础方法）
        
        Args:
            query: 构建好的SQL对象
            params: 查询参数
            fetch: 是否获取结果
            
        Returns:
            根据操作类型返回：
            - SELECT: 结果列表
            - INSERT/UPDATE/DELETE: 影响的行数
            - 不获取结果时返回None
            
        Raises:
            DatabaseError: 当执行失败时
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                
                if not fetch:
                    self.connection.commit()
                    if cursor.rowcount != -1:  # 返回影响行数
                        return cursor.rowcount
                    return None
                
                # 获取结果并转换为字典列表
                if cursor.description:
                    columns = [col[0] for col in cursor.description]
                    return [dict(zip(columns, row)) for row in cursor.fetchall()]
                return []
                
        except Exception as e:
            self.connection.rollback()
            logger.error(f"SQL执行失败: {str(e)}\nQuery: {query.as_string(self.connection)}")
            raise DatabaseError(f"SQL执行失败: {str(e)}") from e
    
    def ensure_table_exists(self, table_name: str) -> bool:
        """确保表存在"""
        if table_name.startswith('stock_hot_'):
            symbol = table_name.replace('stock_hot_', '')
            if not symbol.isalnum():
                raise InvalidSymbolError(f"股票代码 {symbol} 包含非法字符")
            
            check_sql = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """
            result = self.execute(check_sql, (table_name,), fetch=True)
            exists = result[0]['exists']
            
            if not exists:
                self.execute("SELECT create_stock_hot_table(%s)", (symbol,))
                logger.info("创建股票 %s 的数据表", symbol)
                return True
            return False
        else:
            raise ValueError(f"不支持自动创建表 {table_name}")
    
    def get_stock_table_name(self, symbol: str) -> str:
        """获取股票对应的表名"""
        if not symbol.isalnum():
            raise InvalidSymbolError(f"股票代码 {symbol} 包含非法字符")
        return f'stock_hot_{symbol}'
    
    def insert(self, table_name: str, data: Dict) -> bool:
        """插入单条数据"""
        columns = list(data.keys())
        values = [data[col] for col in columns]
        
        query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES ({values})
            ON CONFLICT DO NOTHING
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values=sql.SQL(', ').join([sql.Placeholder()] * len(values))
        )
        
        result = self.execute(query, tuple(values))
        if result and isinstance(result, list):  # 先检查类型
            return result[0]
    
    def bulk_insert(self, table_name: str, data: List[Dict]) -> int:
        """批量插入数据"""
        if not data:
            return 0
            
        columns = list(data[0].keys())
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # 创建临时表
                    cursor.execute(
                        sql.SQL("""
                            CREATE TEMP TABLE temp_bulk_data (
                                LIKE {} INCLUDING DEFAULTS
                            ) ON COMMIT DROP
                        """).format(sql.Identifier(table_name)))
                    
                    # 使用copy_from批量插入
                    with cursor.copy(
                        sql.SQL("COPY temp_bulk_data ({}) FROM STDIN").format(columns_sql)
                    ) as copy:
                        for item in data:
                            copy.write_row([item[col] for col in columns])
                    
                    # 从临时表合并到主表
                    cursor.execute(
                        sql.SQL("""
                            INSERT INTO {} 
                            SELECT * FROM temp_bulk_data
                            ON CONFLICT DO NOTHING
                        """).format(sql.Identifier(table_name)))
                    
                    conn.commit()
                    return len(data)
                except Exception as e:
                    conn.rollback()
                    logger.error("批量插入失败: %s", e)
                    raise
    
    def bulk_upsert(self, table_name: str, data: List[Dict], update_fields: List[str]) -> int:
        """批量插入或更新数据"""
        if not update_fields:  # 必须字段检查
            raise ValueError("更新字段列表不能为空")
    
        if not data:  # 空数据视为合法情况
            logger.debug("接收到空数据列表")
            return 0
            
        columns = list(data[0].keys())
        columns_sql = sql.SQL(', ').join(map(sql.Identifier, columns))
        
        with self._get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    # 创建临时表
                    cursor.execute(
                        sql.SQL("""
                            CREATE TEMP TABLE temp_upsert_data (
                                LIKE {} INCLUDING DEFAULTS
                            ) ON COMMIT DROP
                        """).format(sql.Identifier(table_name)))
                    
                    # 使用copy_from批量插入
                    with cursor.copy(
                        sql.SQL("COPY temp_upsert_data ({}) FROM STDIN").format(columns_sql)
                    ) as copy:
                        for item in data:
                            copy.write_row([item[col] for col in columns])
                    
                    # 构建UPDATE部分
                    update_set = sql.SQL(', ').join(
                        sql.SQL("{} = EXCLUDED.{}").format(
                            sql.Identifier(f), sql.Identifier(f))
                        for f in update_fields
                    )
                    
                    # 执行UPSERT
                    cursor.execute(
                        sql.SQL("""
                            INSERT INTO {} 
                            SELECT * FROM temp_upsert_data
                            ON CONFLICT (trade_date) DO UPDATE SET
                                {}
                        """).format(
                            sql.Identifier(table_name),
                            update_set
                        ))
                    
                    conn.commit()
                    return len(data)
                except Exception as e:
                    conn.rollback()
                    logger.error("批量插入或更新失败: %s", e)
                    raise
    
    def query(
    self,
    table_name: str,
    conditions: Optional[Dict[str, Any]] = None,
    fields: Optional[List[str]] = None,
    order_by: Optional[str] = None,
    ascending: bool = True,
    limit: Optional[int] = None
) -> List[Dict[str, Any]]:
        """执行查询并返回结果列表
        
        Args:
            table_name: 表名
            conditions: 查询条件字典
            fields: 要返回的字段列表
            order_by: 排序字段
            ascending: 是否升序
            limit: 结果限制数量
            
        Returns:
            总返回包含字典的列表（无结果时返回空列表）
            
        Raises:
            DatabaseError: 当查询执行失败时
            TypeError: 当返回结果格式不符合预期时
        """
        try:
            # 构造SELECT字段部分
            select_fields = (
                sql.SQL(', ').join(map(sql.Identifier, fields)) 
                if fields else sql.SQL('*')
            )
            
            # 基础查询语句（修正了括号匹配）
            query = sql.SQL("SELECT {} FROM {}").format(
                select_fields, 
                sql.Identifier(table_name)
            )
            
            params = []
            where_clauses = []
            
            # 添加WHERE条件
            if conditions:
                for col, val in conditions.items():
                    if isinstance(val, dict) and 'operator' in val:  # 处理特殊操作符
                        op = val['operator']
                        if op.upper() == 'BETWEEN' and isinstance(val['value'], (list, tuple)):
                            where_clauses.append(
                                sql.SQL("{} BETWEEN %s AND %s").format(sql.Identifier(col))
                            )
                            params.extend(val['value'])
                        else:
                            where_clauses.append(
                                sql.SQL("{} {} %s").format(sql.Identifier(col), sql.SQL(op))
                            )
                            params.append(val['value'])
                    else:  # 普通等于条件
                        where_clauses.append(
                            sql.SQL("{} = %s").format(sql.Identifier(col))
                        )
                        params.append(val)
                
                query = sql.SQL("{} WHERE {}").format(
                    query, 
                    sql.SQL(" AND ").join(where_clauses)
                )
            
            # 添加ORDER BY
            if order_by:
                order_dir = sql.SQL("ASC") if ascending else sql.SQL("DESC")
                query = sql.SQL("{} ORDER BY {} {}").format(
                    query, 
                    sql.Identifier(order_by), 
                    order_dir
                )
            
            # 添加LIMIT
            if limit:
                query = sql.SQL("{} LIMIT %s").format(query)
                params.append(limit)
            
            # 执行查询
            result = self.execute(query, tuple(params), fetch=True)
            
            # 类型验证
            if not isinstance(result, list):
                raise TypeError(
                    f"预期返回列表类型，实际得到 {type(result).__name__}")
            
            if result and not all(isinstance(row, dict) for row in result):
                raise TypeError("结果中的行必须是字典类型")
                
            return result
            
        except Exception as e:
            logger.error(f"查询{table_name}失败: {str(e)}")
            raise DatabaseError(f"数据库查询失败: {str(e)}") from e

    
    def delete(
    self,
    table_name: str,
    conditions: Dict[str, Any],
    return_affected_rows: bool = True
) -> int:
        """删除符合条件的数据
        
        Args:
            table_name: 目标表名
            conditions: 删除条件字典 {列名: 值}
            return_affected_rows: 是否返回影响行数
            
        Returns:
            影响的行数（默认返回）
            或 0（当return_affected_rows=False且操作成功时）
            
        Raises:
            DatabaseError: 当删除操作失败时
            TypeError: 当返回结果类型不符合预期时
        """
        try:
            # 参数校验（新增）
            if not conditions:
                raise ValueError("删除条件不能为空字典")

            # 构建WHERE条件
            where_clauses = [
                sql.SQL("{} = %s").format(sql.Identifier(col))
                for col in conditions.keys()
            ]
            
            query = sql.SQL("DELETE FROM {} WHERE {}").format(
                sql.Identifier(table_name),
                sql.SQL(" AND ").join(where_clauses)
            )
            
            # 执行删除（fetch=False表示不需要返回结果集）
            result = self.execute(
                query, 
                tuple(conditions.values()),
                fetch=False
            )
            
            # 严格的类型检查
            if not isinstance(result, int) or result < 0:
                raise TypeError(
                    f"预期返回非负整数影响行数，实际得到 {type(result).__name__}: {result}"
                )
                
            return result if return_affected_rows else 0
            
        except Exception as e:
            logger.error(
                "删除表[%s]记录失败，条件: %s。错误: %s",
                table_name,
                conditions,
                str(e),
                exc_info=True
            )
            raise DatabaseError(f"删除操作失败: {str(e)}") from e
    
    def save_task_progress(self, task_id: str, progress_data: Dict) -> bool:
        """保存任务进度"""
        result = self.execute(
            """
            INSERT INTO task_progress (task_id, progress_data)
            VALUES (%s, %s)
            ON CONFLICT (task_id) DO UPDATE SET
                progress_data = EXCLUDED.progress_data,
                update_time = CURRENT_TIMESTAMP
            """,
            (task_id, extras.Json(progress_data))
        )
        if result and isinstance(result, list):  # 先检查类型
            return result[0]
    
    def load_task_progress(self, task_id: str) -> Optional[Dict]:
        """加载任务进度"""
        result = self.execute(
            "SELECT progress_data FROM task_progress WHERE task_id = %s",
            (task_id,),
            fetch=True
        )
        if result and isinstance(result, list):  # 先检查类型
            return result[0]['progress_data'] if result else None

        # return result[0]['progress_data'] if result else None
    
    def delete_task_progress(self, task_id: str) -> bool:
        """删除任务进度"""
        result = self.execute(
            "DELETE FROM task_progress WHERE task_id = %s",
            (task_id,)  
        )
        if result and isinstance(result, list):  # 先检查类型
            return result[0]
    
    def close(self):
        """关闭连接池"""
        if hasattr(self, 'pool'):
            self.pool.closeall()
            logger.info("数据库连接池已关闭")
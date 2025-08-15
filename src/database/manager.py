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
from psycopg2 import sql, pool, extras,Composed
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
    
    def _init_connection_pool(self):
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
    
    def _ensure_base_structure(self):
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
    
    def _create_table_if_not_exists(self, table_type: str):
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
    
    def _create_stock_hot_table_function(self):
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
    query: Union[str, Composed],  # 允许 psycopg2.sql.Composed 类型
    params: Optional[Union[Tuple[Any, ...], Dict[str, Any], List[Any]]] = None,
    fetch: bool = False,
    many: bool = False
) -> Union[bool, List[Dict[str, Any]]]:
        """执行SQL语句
        
        Args:
            query: SQL查询语句
            params: 查询参数，可以是元组、字典或列表
            fetch: 是否返回查询结果
            many: 是否执行批量操作
            
        Returns:
            如果 fetch=True 返回查询结果(List[Dict])
            否则返回 bool 表示操作是否成功
            
        Raises:
            QueryError: 当SQL执行失败时
            ValueError: 当批量操作缺少参数时
        """
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=extras.DictCursor) as cursor:
                try:
                    if many:
                        if not params:
                            raise ValueError("批量操作需要参数列表")
                        cursor.executemany(query, params)  # type: ignore
                    else:
                        if params:
                            cursor.execute(query, params)
                        else:
                            cursor.execute(query)
                    
                    conn.commit()
                    
                    if fetch:
                        columns = [desc[0] for desc in cursor.description]
                        return [
                            dict(zip(columns, row)) 
                            for row in cursor.fetchall()
                        ]
                    return True
                    
                except Exception as e:
                    conn.rollback()
                    logger.error("SQL执行失败: %s\nSQL: %s", e, query)
                    raise QueryError(f"SQL execution failed: {e}") from e
    
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
        
        return self.execute(query, tuple(values))
    
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
        if not data or not update_fields:
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
        conditions: Optional[Dict] = None,
        fields: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        ascending: bool = True,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """查询数据"""
        select_fields = (
            sql.SQL(', ').join(map(sql.Identifier, fields)) 
            if fields else sql.SQL('*')
        )
        
        query = sql.SQL("SELECT {} FROM {}").format(
            select_fields, sql.Identifier(table_name))
        
        params = []
        where_clauses = []
        
        if conditions:
            for col, val in conditions.items():
                where_clauses.append(sql.SQL("{} = %s").format(sql.Identifier(col)))
                params.append(val)
            
            query = sql.SQL("{} WHERE {}").format(
                query, sql.SQL(" AND ").join(where_clauses))
        
        if order_by:
            order_dir = sql.SQL("ASC") if ascending else sql.SQL("DESC")
            query = sql.SQL("{} ORDER BY {} {}").format(
                query, sql.Identifier(order_by), order_dir)
        
        if limit:
            query = sql.SQL("{} LIMIT %s").format(query)
            params.append(limit)
        
        result = self.execute(query, tuple(params), fetch=True)
        if not isinstance(result, list):
            raise TypeError("Expected list result from query")
        return result
    
    def delete(self, table_name: str, conditions: Dict) -> int:
        """删除数据"""
        where_clauses = [
            sql.SQL("{} = %s").format(sql.Identifier(col))
            for col in conditions.keys()
        ]
        
        query = sql.SQL("DELETE FROM {} WHERE {}").format(
            sql.Identifier(table_name),
            sql.SQL(" AND ").join(where_clauses)
        )
        success = self.execute(query, tuple(conditions.values()))
        if not isinstance(success, bool):
            raise TypeError("Expected bool result from delete operation")
        return cursor.rowcount if success else 0
    
    def save_task_progress(self, task_id: str, progress_data: Dict) -> bool:
        """保存任务进度"""
        return self.execute(
            """
            INSERT INTO task_progress (task_id, progress_data)
            VALUES (%s, %s)
            ON CONFLICT (task_id) DO UPDATE SET
                progress_data = EXCLUDED.progress_data,
                update_time = CURRENT_TIMESTAMP
            """,
            (task_id, extras.Json(progress_data))
        )
    
    def load_task_progress(self, task_id: str) -> Optional[Dict]:
        """加载任务进度"""
        result = self.execute(
            "SELECT progress_data FROM task_progress WHERE task_id = %s",
            (task_id,),
            fetch=True
        )
        return result[0]['progress_data'] if result else None
    
    def delete_task_progress(self, task_id: str) -> bool:
        """删除任务进度"""
        return self.execute(
            "DELETE FROM task_progress WHERE task_id = %s",
            (task_id,)
        )
    
    def close(self):
        """关闭连接池"""
        if hasattr(self, 'pool'):
            self.pool.closeall()
            logger.info("数据库连接池已关闭")
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@File    : Untitled-1
@Author  : Sun
@Email   : 
@Date    : 2025-08-15
@Desc    : 
"""

import akshare as ak
import logging
import time
from typing import Dict, List, Optional
import pandas as pd

from ..database.manager import StockDatabaseManager
from .base import DataCollector
from .helpers import format_date, validate_symbol, build_stock_data_dict
from ..utils.decorators import retry

logger = logging.getLogger(__name__)

class AStockCollector(DataCollector):
    """A股数据采集器实现"""
    
    def __init__(self, db_manager: StockDatabaseManager):
        """
        初始化采集器
        
        Args:
            db_manager: 数据库管理器实例
        """
        self.db = db_manager
        self.market_map = {
            '6': 'SH',
            '0': 'SZ',
            '3': 'SZ',
            '8': 'BJ'
        }
    
    @retry(max_retries=3, delay=1)
    def fetch_basic_info(self) -> List[Dict]:
        """获取A股基础信息"""
        try:
            logger.info("开始获取A股基础数据...")
            
            # 获取各市场数据
            stock_info = ak.stock_info_a_code_name()
            sz_stocks = ak.stock_info_sz_name_code(symbol="A股列表")
            sh_stocks = ak.stock_info_sh_name_code()
            
            # 处理数据
            symbol_info = self._process_market_data(sz_stocks, sh_stocks)
            
            # 合并数据
            data = []
            for _, row in stock_info.iterrows():
                symbol = row['code']
                info = symbol_info.get(symbol, {})
                
                data.append({
                    'symbol': symbol,
                    'name': row['name'],
                    'market': self._get_market(symbol),
                    'industry': info.get('industry'),
                    'list_date': format_date(info.get('list_date')),
                    'status': 1
                })
            
            logger.info(f"成功获取 {len(data)} 条A股数据")
            return data
            
        except Exception as e:
            logger.error(f"获取基础数据失败: {e}", exc_info=True)
            raise
    
    def _get_market(self, symbol: str) -> str:
        """根据股票代码获取市场"""
        return self.market_map.get(symbol[0], 'SH')
    
    def _process_market_data(self, sz_stocks, sh_stocks) -> Dict:
        """处理市场数据"""
        symbol_info = {}
        
        # 处理深交所数据
        for _, row in sz_stocks.iterrows():
            symbol = row['A股代码']
            symbol_info[symbol] = {
                'industry': row.get('所属行业'),
                'list_date': row.get('A股上市日期')
            }
        
        # 处理上交所数据
        for _, row in sh_stocks.iterrows():
            symbol = row['证券代码']
            symbol_info[symbol] = {
                'industry': None,
                'list_date': row.get('上市日期')
            }
        
        return symbol_info
    
    @retry(max_retries=3, delay=1)
    def fetch_history_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        adjust: str = ""
    ) -> List[Dict]:
        """获取股票历史交易数据"""
        try:
            if not validate_symbol(symbol):
                raise ValueError(f"无效的股票代码: {symbol}")
            
            market = "sh" if symbol.startswith("6") else "sz"
            stock_code = f"{market}{symbol}"
            
            # 获取未复权日线数据
            raw_df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust
            )
            
            if raw_df.empty:
                logger.warning(f"未找到股票 {symbol} 在 {start_date} 到 {end_date} 期间的交易数据")
                return []
            
            # 获取复权因子
            qfq_factor_df = ak.stock_zh_a_daily(symbol=stock_code, adjust="qfq-factor")
            hfq_factor_df = ak.stock_zh_a_daily(symbol=stock_code, adjust="hfq-factor")
            
            # 处理日期格式
            raw_df["日期"] = pd.to_datetime(raw_df["日期"])
            qfq_factor_df["date"] = pd.to_datetime(qfq_factor_df["date"])
            hfq_factor_df["date"] = pd.to_datetime(hfq_factor_df["date"])
            
            # 准备结果列表
            result = []
            
            for _, row in raw_df.iterrows():
                trade_date = row["日期"]
                date_str = trade_date.strftime("%Y-%m-%d")
                
                # 获取前复权因子
                matched_qfq = qfq_factor_df[qfq_factor_df["date"] <= trade_date]
                qfq_factor = matched_qfq.iloc[-1]["qfq_factor"] if not matched_qfq.empty else qfq_factor_df.iloc[-1]["qfq_factor"]
                
                # 获取后复权因子
                matched_hfq = hfq_factor_df[hfq_factor_df["date"] <= trade_date]
                hfq_factor = matched_hfq.iloc[-1]["hfq_factor"] if not matched_hfq.empty else hfq_factor_df.iloc[-1]["hfq_factor"]
                
                # 构建数据字典
                data = build_stock_data_dict(
                    symbol=symbol,
                    trade_date=date_str,
                    open_price=float(row["开盘"]),
                    high=float(row["最高"]),
                    low=float(row["最低"]),
                    close=float(row["收盘"]),
                    volume=int(row["成交量"]),
                    amount=float(row["成交额"]),
                    pct_change=float(row["涨跌幅"]) if "涨跌幅" in row else None,
                    turnover_rate=float(row["换手率"]) if "换手率" in row else None,
                    amplitude=float(row["振幅"]) if "振幅" in row else None,
                    qfq_factor=float(qfq_factor),
                    hfq_factor=float(hfq_factor)
                )
                result.append(data)
            
            logger.info(f"成功获取股票 {symbol} 在 {start_date} 到 {end_date} 期间的 {len(result)} 条交易数据")
            return result
            
        except Exception as e:
            logger.error(f"获取股票 {symbol} 历史数据失败: {e}", exc_info=True)
            raise
    
    def save_to_database(self, data: List[Dict]) -> int:
        """保存数据到数据库"""
        if not data:
            return 0
            
        symbol = data[0]['symbol']
        table_name = self.db.get_stock_table_name(symbol)
        
        try:
            # 确保表存在
            self.db.ensure_table_exists(table_name)
            
            # 批量插入数据
            update_fields = [
                'open', 'high', 'low', 'close', 'volume', 'amount',
                'pct_change', 'turnover_rate', 'amplitude',
                'factor_qfq', 'factor_hfq'
            ]
            
            count = self.db.bulk_upsert(
                table_name=table_name,
                data=data,
                update_fields=update_fields
            )
            
            logger.info(f"成功保存股票 {symbol} 的 {count} 条数据")
            return count
            
        except Exception as e:
            logger.error(f"保存股票 {symbol} 数据失败: {e}")
            raise
    
    def is_trading_day(self, date: str) -> bool:
        """判断是否为交易日"""
        try:
            # 首先尝试从数据库查询
            result = self.db.query(
                'trading_calendar',
                conditions={'trade_date': date},
                fields=['is_trading_day']
            )
            
            if result:
                return result[0]['is_trading_day']
            
            # 数据库无记录则从AKShare查询
            trade_dates = ak.tool_trade_date_hist_sina()
            trade_dates['trade_date'] = trade_dates['trade_date'].astype(str)
            return date in trade_dates['trade_date'].values
            
        except Exception as e:
            logger.error(f"判断交易日 {date} 失败: {e}", exc_info=True)
            raise
    
    def get_previous_trading_day(self, date: str) -> Optional[str]:
        """获取上一个交易日"""
        try:
            # 从数据库查询
            result = self.db.query(
                'trading_calendar',
                conditions={'trade_date': {'operator': '<', 'value': date}},
                fields=['trade_date'],
                order_by='trade_date',
                ascending=False,
                limit=1
            )
            
            if result:
                return result[0]['trade_date']
            
            # 从AKShare获取
            trade_cal = ak.tool_trade_date_hist_sina()
            trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
            input_date = pd.to_datetime(date)
            
            previous_days = trade_cal[trade_cal['trade_date'] < input_date]
            if not previous_days.empty:
                return previous_days.iloc[-1]['trade_date'].strftime('%Y-%m-%d')
            
            return None
            
        except Exception as e:
            logger.error(f"获取上一个交易日失败: {e}", exc_info=True)
            raise
    
    def get_next_trading_day(self, date: str) -> Optional[str]:
        """获取下一个交易日"""
        try:
            # 从数据库查询
            result = self.db.query(
                'trading_calendar',
                conditions={'trade_date': {'operator': '>', 'value': date}},
                fields=['trade_date'],
                order_by='trade_date',
                ascending=True,
                limit=1
            )
            
            if result:
                return result[0]['trade_date']
            
            # 从AKShare获取
            trade_cal = ak.tool_trade_date_hist_sina()
            trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'])
            input_date = pd.to_datetime(date)
            
            next_days = trade_cal[trade_cal['trade_date'] > input_date]
            if not next_days.empty:
                return next_days.iloc[0]['trade_date'].strftime('%Y-%m-%d')
            
            return None
            
        except Exception as e:
            logger.error(f"获取下一个交易日失败: {e}", exc_info=True)
            raise
    
    def fetch_and_save_all(
        self,
        start_date: str,
        end_date: str,
        batch_size: int = 50,
        task_id: Optional[str] = None,
        force_redo: bool = False
    ) -> Dict[str, int]:
        """获取并保存所有股票在指定日期范围内的数据"""
        stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'processed': 0,
            'records': 0
        }
        
        try:
            # 获取所有股票代码
            stocks = self.db.query(
                'stock_basic',
                conditions={'status': 1},
                fields=['symbol']
            )
            
            stats['total'] = len(stocks)
            logger.info(f"开始处理 {stats['total']} 只股票的数据")
            
            # 加载任务进度
            progress = None
            if task_id and not force_redo:
                progress = self.db.load_task_progress(task_id)
            
            processed_symbols = set()
            if progress:
                processed_symbols = set(progress.get('processed_symbols', []))
                stats.update(progress.get('stats', stats))
                logger.info(f"从任务 {task_id} 断点恢复，已处理 {len(processed_symbols)} 只股票")
            
            # 分批处理
            for i in range(0, len(stocks), batch_size):
                batch = stocks[i:i + batch_size]
                logger.info(f"正在处理第 {i//batch_size + 1} 批，共 {len(batch)} 只股票")
                
                for stock in batch:
                    symbol = stock['symbol']
                    
                    if symbol in processed_symbols and not force_redo:
                        stats['processed'] += 1
                        continue
                    
                    try:
                        data = self.fetch_history_data(symbol, start_date, end_date)
                        if data:
                            count = self.save_to_database(data)
                            stats['records'] += count
                            stats['success'] += 1
                        else:
                            stats['failed'] += 1
                        
                        processed_symbols.add(symbol)
                        stats['processed'] += 1
                        
                        # 保存进度
                        if task_id:
                            self.db.save_task_progress(task_id, {
                                'stats': stats,
                                'processed_symbols': list(processed_symbols),
                                'current_batch': i
                            })
                        
                        # 适当延迟
                        time.sleep(0.5)
                        
                    except Exception as e:
                        stats['failed'] += 1
                        logger.error(f"处理股票 {symbol} 失败: {e}", exc_info=True)
                        continue
            
            # 任务完成，清除进度
            if task_id:
                self.db.delete_task_progress(task_id)
            
            logger.info(
                f"处理完成！成功: {stats['success']}, 失败: {stats['failed']}, "
                f"总记录数: {stats['records']}"
            )
            return stats
            
        except Exception as e:
            logger.error(f"批量处理失败: {e}", exc_info=True)
            raise
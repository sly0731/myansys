import logging
from datetime import datetime, timedelta
from src.database.manager import StockDatabaseManager
from src.collector.a_stock import AStockCollector
from src.utils.checker import TradingDayChecker

def setup_logging():
    """配置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/stock_analysis.log'),
            logging.StreamHandler()
        ]
    )

def main():
    """主程序"""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # 初始化数据库管理器
        db_manager = StockDatabaseManager()
        
        # 初始化采集器
        collector = AStockCollector(db_manager)
        
        # 初始化交易日检查器
        day_checker = TradingDayChecker(db_manager)
        
        # 更新最近一年的交易日历
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=365)
        day_checker.update_calendar(start_date, end_date)
        
        # 获取并保存所有股票数据
        result = collector.fetch_and_save_all(
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            task_id=f"collect_{start_date}_{end_date}"
        )
        
        logger.info("程序执行完成，结果: %s", result)
        
    except Exception as e:
        logger.error("程序运行出错: %s", e, exc_info=True)
    finally:
        if 'db_manager' in locals():
            db_manager.close()

if __name__ == "__main__":
    main()
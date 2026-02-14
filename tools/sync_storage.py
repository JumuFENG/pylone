#!/usr/bin/env python3
"""
存储数据同步工具 - 修复版本

基于深入分析和测试验证的SQLite线程错误修复版本

使用示例:
    # 同步单只股票的H5数据到SQLite
    python tools/sync_storage.py --direction h5_to_sqlite --fcode 000001
    
    # 同步多只股票的所有数据类型
    python tools/sync_storage.py --direction sqlite_to_h5 --fcodes 000001,000002,000003
    
    # 同步特定数据类型
    python tools/sync_storage.py --direction h5_to_sqlite --fcodes 000001 --data-types klines,fflow
    
    # 自定义同步数量限制
    python tools/sync_storage.py --direction h5_to_sqlite --fcode 000001 --klines-limit 50 --fflow-limit 200
"""

import asyncio
import argparse
import sys
import os
from typing import List, Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from app.stock.storage.storage_manager import DataSyncManager
from app.lofig import logger


class FixedSyncStorageTool:
    """修复版存储同步工具类"""
    
    def __init__(self):
        self.sync_manager = DataSyncManager()
        self.total_synced = 0
        self.total_errors = 0
    
    async def sync_single_stock(self, fcode: str, direction: str, data_types: List[str],
                            kline_types: List[int], limits: Dict[str, int]) -> Dict[str, int]:
        """同步单只股票 - 修复版"""
        print(f"\n正在同步股票 {fcode} ({direction})...")
        
        try:
            # 使用修复后的管理器，添加重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = await self.sync_manager.sync_single_stock(fcode, direction, data_types, kline_types, limits)
                    
                    # 显示同步结果
                    for data_type, count in result.items():
                        if count > 0:
                            print(f"  {data_type}: {count} 条记录")
                            self.total_synced += count
                    
                    return result
                    
                except Exception as e:
                    if "threads can only be started once" in str(e):
                        logger.warning(f"线程错误，尝试 {attempt + 1}/{max_retries}: {e}")
                        if attempt < max_retries - 1:
                            print(f"  线程错误，等待2秒后重试...")
                            await asyncio.sleep(2)
                            continue
                        else:
                            logger.error(f"线程错误重试失败: {e}")
                            raise
                    else:
                        raise
            
        except Exception as e:
            print(f"  错误: {str(e)}")
            self.total_errors += 1
            return {}
    
    async def sync_multiple_stocks(self, fcodes: List[str], direction: str, data_types: List[str],
                               kline_types: List[int], limits: Dict[str, int]) -> Dict[str, Dict[str, int]]:
        """同步多只股票 - 修复版"""
        print(f"\n开始同步 {len(fcodes)} 只股票 ({direction})")
        print("-" * 60)
        
        results = {}
        
        for fcode in fcodes:
            result = await self.sync_single_stock(fcode, direction, data_types, kline_types, limits)
            results[fcode] = result
            
            # 股票间短暂休息，避免资源竞争
            await asyncio.sleep(0.001)
        
        return results
    
    def print_summary(self, fcodes: List[str], direction: str):
        """打印同步摘要"""
        print("\n" + "=" * 60)
        print("同步完成摘要")
        print("=" * 60)
        print(f"同步方向: {direction}")
        print(f"股票数量: {len(fcodes)}")
        print(f"总同步记录数: {self.total_synced}")
        print(f"错误数量: {self.total_errors}")
        
        if self.total_errors > 0:
            print(f"\n警告: 有 {self.total_errors} 个错误发生")
        else:
            print("\n所有同步操作成功完成")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="存储数据同步工具 - 修复版本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  %(prog)s --direction h5_to_sqlite --fcode 000001
  %(prog)s --direction sqlite_to_h5 --fcodes 000001,000002
  %(prog)s --direction h5_to_sqlite --fcodes 000001 --data-types klines,fflow
  %(prog)s --direction h5_to_sqlite --fcode 000001 --klines-limit 50 --fflow-limit 200
        """)
    
    parser.add_argument(
        '--direction',
        choices=['h5_to_sqlite', 'sqlite_to_h5'],
        required=True,
        help='同步方向')
    
    parser.add_argument(
        '--fcode',
        help='单个股票代码')
    
    parser.add_argument(
        '--fcodes',
        help='多个股票代码，用逗号分隔')
    
    parser.add_argument(
        '--data-types',
        default='klines,fflow,transactions',
        help='数据类型，用逗号分隔 (默认: klines,fflow,transactions)')
    
    parser.add_argument(
        '--kline-types',
        default='101',
        help='K线类型，用逗号分隔 (默认: 101)')
    
    parser.add_argument(
        '--klines-limit',
        type=int,
        default=100,
        help='K线数据同步数量限制 (默认: 100)')
    
    parser.add_argument(
        '--fflow-limit',
        type=int,
        default=100,
        help='资金流数据同步数量限制 (默认: 100)')
    
    parser.add_argument(
        '--transactions-limit',
        type=int,
        default=100,
        help='交易数据同步数量限制 (默认: 100)')
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='详细输出')
    
    return parser.parse_args()


def validate_arguments(args):
    """验证参数"""
    if not args.fcode and not args.fcodes:
        print("错误: 必须指定 --fcode 或 --fcodes")
        return False
    
    if args.fcode and args.fcodes:
        print("错误: --fcode 和 --fcodes 不能同时使用")
        return False
    
    return True


def parse_list_argument(arg_str: str) -> List[str]:
    """解析列表参数"""
    if not arg_str:
        return []
    return [item.strip() for item in arg_str.split(',') if item.strip()]


def parse_int_list_argument(arg_str: str) -> List[int]:
    """解析整数列表参数"""
    if not arg_str:
        return []
    try:
        return [int(item.strip()) for item in arg_str.split(',') if item.strip()]
    except ValueError:
        print(f"错误: 无法解析整数列表: {arg_str}")
        return []


async def main():
    """主函数 - 修复版"""
    args = parse_arguments()
    
    # 验证参数
    if not validate_arguments(args):
        sys.exit(1)
    
    # 解析参数
    fcodes = [args.fcode] if args.fcode else parse_list_argument(args.fcodes)
    data_types = parse_list_argument(args.data_types)
    kline_types = parse_int_list_argument(args.kline_types)
    
    # 设置同步限制
    limits = {
        'klines': args.klines_limit,
        'fflow': args.fflow_limit,
        'transactions': args.transactions_limit
    }
    
    # 设置日志级别
    if args.verbose:
        logger.setLevel('DEBUG')
    
    # 验证数据类型
    valid_data_types = ['klines', 'fflow', 'transactions']
    for dt in data_types:
        if dt not in valid_data_types:
            print(f"错误: 无效的数据类型 {dt}，支持的类型: {valid_data_types}")
            sys.exit(1)
    
    # 创建修复版同步工具
    sync_tool = FixedSyncStorageTool()
    
    try:
        # 执行同步
        if len(fcodes) == 1:
            await sync_tool.sync_single_stock(
                fcodes[0], args.direction, data_types, kline_types, limits)
        else:
            await sync_tool.sync_multiple_stocks(
                fcodes, args.direction, data_types, kline_types, limits)
        
        # 打印摘要
        sync_tool.print_summary(fcodes, args.direction)
        
    except KeyboardInterrupt:
        print("\n同步被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n同步过程中发生错误: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


async def test_sync():
    """测试同步功能"""
    sync_tool = FixedSyncStorageTool()
    # from app.stock.manager import AllStocks
    # fcodes = await AllStocks.read_all()

    fcodes = [fcode.code for fcode in fcodes if fcode.typekind == 'ABStock']
    # fcodes = ['sz161129', 'sz162411', 'sh510050', 'sh510210', 'sh510300', 'sh510500', 'sh512100', 'sh513050', 'sh513660', 'sh518880', 'sh588300', 'sz159915']

    try:
        await sync_tool.sync_multiple_stocks(fcodes, 'h5_to_sqlite', ['klines', 'fflow', 'transactions'], [101], {'klines': 10, 'fflow': 10, 'transactions': 10})
        return True
    except Exception as e:
        print(f"测试失败: {e}")
        return False


if __name__ == "__main__":
    # 检查是否是测试模式
    asyncio.run(test_sync())

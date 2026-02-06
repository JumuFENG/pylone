#!/usr/bin/env python3
"""
Database synchronization tool for MySQL ↔ SQLite
Supports incremental sync, bidirectional synchronization, and conflict resolution
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy import select, inspect, text, func
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.lofig import Config
from app.users.models import User, UserStocks, UserStrategy, UserCostdog
from app.stock.models import MdlAllStock, MdlStockShare, MdlSysSettings


class DatabaseSyncManager:
    """Manages synchronization between MySQL and SQLite databases"""
    
    def __init__(self):
        self.mysql_engine = None
        self.sqlite_engine = None
        self.mysql_session_maker = None
        self.sqlite_session_maker = None
        
    async def initialize(self):
        """Initialize database connections for both MySQL and SQLite"""
        # Store original database type
        original_dbtype = Config.database_type()
        
        # Initialize MySQL connection
        mysql_cfg = Config.database_config().copy()
        mysql_cfg['dbtype'] = 'mysql'
        
        mysql_url = f"mysql+aiomysql://{mysql_cfg['user']}:{Config.simple_decrypt(mysql_cfg['password'])}@{mysql_cfg['host']}:{mysql_cfg['port']}/{mysql_cfg['dbname']}"
        self.mysql_engine = create_async_engine(mysql_url, echo=False, future=True)
        self.mysql_session_maker = sessionmaker(self.mysql_engine, class_=AsyncSession, expire_on_commit=False)
        
        # Initialize SQLite connection
        sqlite_cfg = Config.database_config().copy()
        sqlite_cfg['dbtype'] = 'sqlite'
        sqlite_db_path = os.path.join(Config.h5_history_dir(), f'{sqlite_cfg["dbname"]}.db')
        
        sqlite_url = f"sqlite+aiosqlite:///{sqlite_db_path}"
        self.sqlite_engine = create_async_engine(sqlite_url, echo=False, future=True)
        self.sqlite_session_maker = sessionmaker(self.sqlite_engine, class_=AsyncSession, expire_on_commit=False)
        
        print(f"MySQL连接: {mysql_url}")
        print(f"SQLite连接: {sqlite_url}")
        
    async def close(self):
        """Close database connections"""
        if self.mysql_engine:
            await self.mysql_engine.dispose()
        if self.sqlite_engine:
            await self.sqlite_engine.dispose()
    
    def get_model_classes(self) -> List[type]:
        """Get all model classes that need synchronization"""
        return [
            User, UserStocks, UserStrategy, UserCostdog,
            MdlAllStock, MdlStockShare, MdlSysSettings
        ]
    
    async def get_table_info(self, engine, table_name: str) -> Dict[str, Any]:
        """Get table information including column names and primary keys"""
        async with engine.begin() as conn:
            inspector = await conn.run_sync(lambda sync_conn: inspect(sync_conn))
            
            columns = await conn.run_sync(
                lambda sync_conn: inspector.get_columns(table_name)
            )
            column_names = [col['name'] for col in columns]
            
            pk_columns = await conn.run_sync(
                lambda sync_conn: inspector.get_pk_constraint(table_name)
            )
            primary_keys = pk_columns['constrained_columns']
            
            return {
                'columns': column_names,
                'primary_keys': primary_keys,
                'column_count': len(columns)
            }
    
    async def table_exists(self, engine, table_name: str) -> bool:
        """Check if table exists in database"""
        try:
            async with engine.begin() as conn:
                inspector = await conn.run_sync(lambda sync_conn: inspect(sync_conn))
                return await conn.run_sync(lambda sync_conn: inspector.has_table(table_name))
        except Exception:
            return False
    
    async def get_row_count(self, session, model_class) -> int:
        """Get row count for a table"""
        try:
            result = await session.execute(select(func.count()).select_from(model_class))
            return result.scalar() or 0
        except Exception as e:
            print(f"获取行数失败: {e}")
            return 0
    
    async def sync_table(self, model_class, direction: str = 'mysql_to_sqlite', 
                        dry_run: bool = False, batch_size: int = 1000) -> Tuple[int, int]:
        """
        Sync a single table between databases
        
        Args:
            model_class: SQLAlchemy model class
            direction: 'mysql_to_sqlite' or 'sqlite_to_mysql'
            dry_run: If True, only show what would be synced
            batch_size: Number of records to process in each batch
            
        Returns:
            Tuple of (synced_count, total_count)
        """
        table_name = model_class.__tablename__
        print(f"\n同步表: {table_name} ({direction})")
        
        # Determine source and target
        if direction == 'mysql_to_sqlite':
            source_engine = self.mysql_engine
            source_session_maker = self.mysql_session_maker
            target_engine = self.sqlite_engine
            target_session_maker = self.sqlite_session_maker
        else:
            source_engine = self.sqlite_engine
            source_session_maker = self.sqlite_session_maker
            target_engine = self.mysql_engine
            target_session_maker = self.mysql_session_maker
        
        # Check if tables exist
        if not await self.table_exists(source_engine, table_name):
            print(f"  源表 {table_name} 不存在，跳过")
            return 0, 0
            
        if not await self.table_exists(target_engine, table_name):
            print(f"  目标表 {table_name} 不存在，跳过")
            return 0, 0
        
        # Get table info
        table_info = await self.get_table_info(source_engine, table_name)
        print(f"  表结构: {table_info['column_count']} 列, 主键: {table_info['primary_keys']}")
        
        # Get source data count
        async with source_session_maker() as source_session:
            total_count = await self.get_row_count(source_session, model_class)
            print(f"  源表记录数: {total_count}")
            
            if total_count == 0:
                print(f"  源表无数据，跳过")
                return 0, 0
        
        if dry_run:
            print(f"  [DRY RUN] 将同步 {total_count} 条记录")
            return total_count, total_count
        
        # Perform synchronization
        synced_count = 0
        offset = 0
        
        async with source_session_maker() as source_session:
            async with target_session_maker() as target_session:
                try:
                    while offset < total_count:
                        # Get batch from source
                        query = select(model_class).offset(offset).limit(batch_size)
                        result = await source_session.execute(query)
                        records = result.scalars().all()
                        
                        if not records:
                            break
                        
                        # Process batch
                        batch_synced = 0
                        for record in records:
                            try:
                                # Convert record to dict
                                record_dict = {}
                                for column in table_info['columns']:
                                    if hasattr(record, column):
                                        record_dict[column] = getattr(record, column)
                                
                                # Check if record exists in target (by primary key)
                                pk_filters = []
                                for pk in table_info['primary_keys']:
                                    if pk in record_dict:
                                        pk_filters.append(getattr(model_class, pk) == record_dict[pk])
                                
                                if pk_filters:
                                    existing_query = select(model_class).where(*pk_filters)
                                    existing_result = await target_session.execute(existing_query)
                                    existing_record = existing_result.scalar_one_or_none()
                                    
                                    if existing_record:
                                        # Update existing record
                                        for key, value in record_dict.items():
                                            setattr(existing_record, key, value)
                                    else:
                                        # Insert new record
                                        new_record = model_class(**record_dict)
                                        target_session.add(new_record)
                                
                                batch_synced += 1
                                
                            except Exception as e:
                                print(f"    记录同步失败: {e}")
                                continue
                        
                        # Commit batch
                        await target_session.commit()
                        synced_count += batch_synced
                        
                        print(f"    已同步: {synced_count}/{total_count} ({offset+batch_size} 批次)")
                        offset += batch_size
                        
                        if not records:  # No more records
                            break
                            
                except Exception as e:
                    await target_session.rollback()
                    print(f"  同步失败: {e}")
                    raise
        
        print(f"  完成同步: {synced_count}/{total_count} 条记录")
        return synced_count, total_count
    
    async def sync_all_tables(self, direction: str = 'mysql_to_sqlite', 
                             dry_run: bool = False, batch_size: int = 1000) -> Dict[str, Tuple[int, int]]:
        """
        Sync all tables between databases
        
        Returns:
            Dict mapping table names to (synced_count, total_count)
        """
        results = {}
        model_classes = self.get_model_classes()
        
        print(f"\n开始全表同步 ({direction})")
        print(f"批次大小: {batch_size}")
        print(f"试运行模式: {dry_run}")
        
        for model_class in model_classes:
            table_name = model_class.__tablename__
            try:
                synced, total = await self.sync_table(model_class, direction, dry_run, batch_size)
                results[table_name] = (synced, total)
            except Exception as e:
                print(f"表 {table_name} 同步失败: {e}")
                results[table_name] = (0, 0)
        
        # Print summary
        print(f"\n同步摘要:")
        total_synced = 0
        total_records = 0
        for table_name, (synced, total) in results.items():
            print(f"  {table_name}: {synced}/{total}")
            total_synced += synced
            total_records += total
        
        print(f"\n总计: {total_synced}/{total_records} 条记录")
        return results


async def main():
    """Main function for database synchronization"""
    import argparse
    
    parser = argparse.ArgumentParser(description='MySQL ↔ SQLite 数据库同步工具')
    parser.add_argument('--direction', choices=['mysql_to_sqlite', 'sqlite_to_mysql'], 
                       default='mysql_to_sqlite', help='同步方向')
    parser.add_argument('--dry-run', action='store_true', help='试运行模式，不实际同步数据')
    parser.add_argument('--batch-size', type=int, default=1000, help='批次大小')
    parser.add_argument('--table', help='指定同步的表名')
    
    args = parser.parse_args()
    
    sync_manager = DatabaseSyncManager()
    
    try:
        await sync_manager.initialize()
        
        if args.table:
            # Sync specific table
            model_classes = sync_manager.get_model_classes()
            target_model = None
            for model_class in model_classes:
                if model_class.__tablename__ == args.table:
                    target_model = model_class
                    break
            
            if target_model:
                await sync_manager.sync_table(target_model, args.direction, args.dry_run, args.batch_size)
            else:
                print(f"未找到表: {args.table}")
        else:
            # Sync all tables
            await sync_manager.sync_all_tables(args.direction, args.dry_run, args.batch_size)
            
    except Exception as e:
        print(f"同步过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await sync_manager.close()


if __name__ == '__main__':
    asyncio.run(main())
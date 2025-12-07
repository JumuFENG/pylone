# -*- coding: utf-8 -*-
"""系统设置管理模块"""
from datetime import datetime
from typing import Optional, Dict, Any
from app.db import query_one_value, query_values, upsert_one
from app.stock.models import MdlSysSettings
from app.lofig import Config
import platform


class SystemSettings:
    """系统设置管理类"""

    # 预定义的设置项
    SETTINGS_KEYS = {
        'lastdaily_run_at': '最后日更新时间',
        'lastweekly_run_at': '最后周更新时间',
        'lastmonthly_run_at': '最后月更新时间',
        'system_version': '系统版本',
        'data_update_enabled': '数据自动更新',
        'notification_enabled': '通知开关',
    }

    version = '1.0.0'

    @classmethod
    async def get(cls, key: str, default: str = '') -> str:
        """获取设置项的值"""
        value = await query_one_value(
            MdlSysSettings,
            MdlSysSettings.value,
            MdlSysSettings.key == key
        )
        return value if value is not None else default

    @classmethod
    async def set(cls, key: str, value: str) -> None:
        """设置项的值"""
        await upsert_one(
            MdlSysSettings,
            {'key': key, 'value': value},
            ['key']
        )

    @classmethod
    async def get_all(cls) -> Dict[str, str]:
        """获取所有设置项"""
        rows = await query_values(MdlSysSettings)
        return {key: value for key, value in rows}

    @classmethod
    async def get_all_with_description(cls) -> list:
        """获取所有设置项及其描述"""
        settings = await cls.get_all()
        result = []
        for key, value in settings.items():
            result.append({
                'key': key,
                'value': value,
                'description': cls.SETTINGS_KEYS.get(key, '未知设置')
            })
        return result

    @classmethod
    async def delete(cls, key: str) -> None:
        """删除设置项"""
        from app.db import delete_records
        await delete_records(MdlSysSettings, MdlSysSettings.key == key)

    @classmethod
    def get_system_info(cls) -> Dict[str, Any]:
        """获取系统信息"""
        try:
            return {
                'platform': platform.system(),
                'platform_version': platform.version(),
                'python_version': platform.python_version(),
                'hostname': platform.node(),
                'processor': platform.processor(),
                'app_name': Config.app_name,
                'system_version': cls.version,
            }
        except Exception as e:
            return {
                'error': str(e),
                'platform': platform.system(),
                'python_version': platform.python_version(),
                'app_name': Config.app_name,
            }

    @classmethod
    def format_bytes(cls, bytes_value: int) -> str:
        """格式化字节数为可读格式"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.2f} PB"

    @classmethod
    async def initialize_defaults(cls) -> None:
        """初始化默认设置"""
        defaults = {
            'data_update_enabled': '1',
            'notification_enabled': '1',
        }

        for key, value in defaults.items():
            existing = await cls.get(key)
            if not existing:
                await cls.set(key, value)

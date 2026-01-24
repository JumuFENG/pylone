# -*- coding: utf-8 -*-
"""系统设置管理模块"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import IntEnum
from app.db import query_one_value, query_values, upsert_one, query_aggregate
from app.stock.models import MdlSysSettings
from app.lofig import Config
import platform


class SettingValueType(IntEnum):
    """设置值类型枚举"""
    READONLY = 0    # 只读项，无法通过网页修改
    BOOLEAN = 1     # 开关量（0/1）
    NUMBER = 2      # 数值量
    STRING = 3      # 字符串量


class SystemSettings:
    """系统设置管理类"""
    version = '1.0.0'
    settings = {}

    @classmethod
    async def get(cls, key: str, default: str = '') -> str:
        """获取设置项的值"""
        if key in cls.settings:
            return cls.settings[key]
        value = await query_one_value(
            MdlSysSettings,
            MdlSysSettings.value,
            MdlSysSettings.key == key
        )
        return value or default

    @classmethod
    async def set(cls, key: str, value: str) -> None:
        """设置项的值"""
        if key in cls.settings and cls.settings[key] == value:
            return

        await upsert_one(
            MdlSysSettings,
            {'key': key, 'value': value},
            ['key']
        )

        cls.settings[key] = value

    @classmethod
    async def query_value_type(cls, key: str) -> Optional[int]:
        """查询设置项的值类型"""
        return await query_one_value(MdlSysSettings, MdlSysSettings.valtype, MdlSysSettings.key == key)

    @classmethod
    def validate_value(cls, value: str, valtype: int) -> None:
        """验证值是否符合类型要求"""
        if valtype == SettingValueType.BOOLEAN:
            if value not in ('0', '1', 'true', 'false', 'True', 'False'):
                raise ValueError(f"开关量的值必须是 0/1 或 true/false")
        elif valtype == SettingValueType.NUMBER:
            try:
                float(value)
            except ValueError:
                raise ValueError(f"数值量的值必须是数字")
        # STRING 类型不需要特殊验证
        return True

    @classmethod
    async def get_all(cls) -> Dict[str, str]:
        """获取所有设置项（仅返回key-value字典）"""
        rows = await query_values(MdlSysSettings, [MdlSysSettings.key, MdlSysSettings.value])
        cls.settings = {key: value for key, value in rows}
        return cls.settings

    @classmethod
    async def get_all_with_metadata(cls) -> List[Dict[str, Any]]:
        """获取所有设置项及其元数据，按id排序"""
        rows = await query_values(MdlSysSettings)

        settings = []
        for row in rows:
            settings.append({
                'id': row.id,
                'key': row.key,
                'value': row.value,
                'name': row.name,
                'valtype': row.valtype,
                'valtype_name': cls._get_valtype_name(row.valtype),
                'editable': row.valtype != SettingValueType.READONLY
            })
        return settings

    @classmethod
    def _get_valtype_name(cls, valtype: int) -> str:
        """获取值类型的名称"""
        type_names = {
            SettingValueType.READONLY: '只读',
            SettingValueType.BOOLEAN: '开关',
            SettingValueType.NUMBER: '数值',
            SettingValueType.STRING: '字符串'
        }
        return type_names.get(valtype, '未知')

    @classmethod
    async def create(cls, key: str, value: str, name: str, valtype: int) -> None:
        """创建新的设置项"""
        # 检查key是否已存在
        existing = await cls.get(key)
        if existing:
            raise ValueError(f"设置项 '{key}' 已存在")

        # 验证valtype
        if valtype not in [t.value for t in SettingValueType]:
            raise ValueError(f"无效的值类型: {valtype}")

        # 验证值
        cls.validate_value(value, valtype)

        # 创建设置项
        await upsert_one(
            MdlSysSettings,
            {'key': key, 'value': value, 'name': name, 'valtype': valtype},
            ['key']
        )

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
                'app_name': Config.client_config().get('app_name', 'pyswee'),
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
        # 预定义的设置项（key, value, name, valtype）
        defaults = [
            ('lastdaily_run_at', '', '每日更新于', SettingValueType.READONLY),
            ('lastweekly_run_at', '', '每周更新于', SettingValueType.READONLY),
            ('lastmonthly_run_at', '', '每月更新于', SettingValueType.READONLY),
            ('realtime_kline_enabled', '1', '实盘数据', SettingValueType.BOOLEAN),
            ('bkchanges_update_realtime', '1', '板块异动', SettingValueType.BOOLEAN),
            ('daily_15min', '1', '15分钟', SettingValueType.BOOLEAN),
            ('daily_5min', '0', '5分钟', SettingValueType.BOOLEAN),
            ('daily_1min', '0', '1分钟', SettingValueType.BOOLEAN),
            ('daily_trans', '1', '逐笔', SettingValueType.BOOLEAN),
        ]

        for key, value, name, valtype in defaults:
            existing = await cls.get(key)
            if not existing:
                await upsert_one(
                    MdlSysSettings,
                    {'key': key, 'value': value, 'name': name, 'valtype': valtype},
                    ['key']
                )

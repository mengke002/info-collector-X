"""
日志配置模块
"""
import logging
import logging.handlers
import os
from typing import Dict, Any


def setup_logging(log_file: str, log_level: str = 'INFO', max_bytes: int = 10485760, backup_count: int = 5):
    """设置日志配置

    Args:
        log_file: 日志文件路径
        log_level: 日志级别
        max_bytes: 单个日志文件最大字节数
        backup_count: 保留的备份日志文件数量
    """
    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 创建logger
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # 清除现有handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 创建formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 文件handler (滚动日志)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info(f"日志系统已初始化，日志文件: {log_file}")


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的logger

    Args:
        name: logger名称

    Returns:
        Logger实例
    """
    return logging.getLogger(name)
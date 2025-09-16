#!/usr/bin/env python3
"""
X/Twitter 信息收集与分析系统 - 数据采集部分
主执行脚本
"""
import sys
import argparse
import json
import logging
from datetime import datetime, timezone, timedelta

from src.logger import setup_logging
from src.database import DatabaseManager
from src.config import config
from src.tasks import run_crawl_task, run_full_crawl_task, run_user_profiling_task, run_scavenger_task


def get_utc_time():
    """获取UTC时间"""
    return datetime.now(timezone.utc)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='X/Twitter 信息收集与分析系统')
    parser.add_argument('--task',
                       choices=['high_freq', 'medium_freq', 'low_freq', 'full_crawl', 'user_profiling', 'scavenger'],
                       default='high_freq',
                       help='要执行的任务类型')
    parser.add_argument('--output', choices=['json', 'text'], default='text',
                       help='输出格式')
    parser.add_argument('--recreate-db', action='store_true',
                       help='删除并重新创建所有表')
    # 默认并发从配置获取（环境变量 > config.ini > 默认值）
    try:
        default_max_workers = config.get_crawler_config().get('default_concurrent_workers', 1)
    except Exception:
        default_max_workers = 1

    parser.add_argument('--max-workers', type=int, default=default_max_workers,
                       help='最大并发线程数（CLI参数优先生效，其次为环境变量/配置文件）')

    args = parser.parse_args()

    print(f"X/Twitter 信息收集系统")
    print(f"执行时间: {get_utc_time().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"执行任务: {args.task}")
    print(f"并发线程: {args.max_workers}")
    print("-" * 50)

    # 初始化日志
    logging_config = config.get_logging_config()
    setup_logging(logging_config['log_file'], logging_config['log_level'])
    logger = logging.getLogger(__name__)

    # 初始化数据库管理器
    db_manager = DatabaseManager(config)

    if args.recreate_db:
        print("正在删除并重新创建数据库表...")
        db_manager.recreate_tables()
        print("数据库表已重新创建。")

    # 执行对应任务
    if args.task == 'high_freq':
        result = run_crawl_task('high', max_workers=args.max_workers, limit=10)
    elif args.task == 'medium_freq':
        result = run_crawl_task('medium', max_workers=args.max_workers, limit=40)
    elif args.task == 'low_freq':
        result = run_crawl_task('low', max_workers=args.max_workers, limit=60)
    elif args.task == 'full_crawl':
        result = run_full_crawl_task(max_workers=args.max_workers)
    elif args.task == 'user_profiling':
        result = run_user_profiling_task()
    elif args.task == 'scavenger':
        result = run_scavenger_task(max_workers=args.max_workers, limit=100)
    else:
        print(f"未知任务类型: {args.task}")
        sys.exit(1)

    # 输出结果
    if args.output == 'json':
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        print_result(result, args.task)

    # 根据结果设置退出码
    if result.get('success', False):
        print(f"\n✅ 任务执行成功")
        sys.exit(0)
    else:
        print(f"\n❌ 任务执行失败: {result.get('error', '未知错误')}")
        sys.exit(1)


def print_result(result: dict, task_type: str):
    """打印结果"""
    if not result.get('success', False):
        print(f"❌ 任务失败: {result.get('error', '未知错误')}")
        return

    print(f"✅ 任务完成")
    print(f"   处理用户: {result.get('users_processed', 0)} 个")
    print(f"   成功爬取: {result.get('users_success', 0)} 个")
    print(f"   失败次数: {result.get('users_failed', 0)} 个")
    print(f"   新增帖子: {result.get('posts_inserted', 0)} 条")
    print(f"   耗时: {result.get('elapsed_seconds', 0):.1f} 秒")

    if task_type == 'user_profiling':
        print(f"   更新用户分组: {result.get('users_updated', 0)} 个")


if __name__ == "__main__":
    main()

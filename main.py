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
from src.tasks import (run_crawl_task, run_full_crawl_task, run_user_profiling_task, run_scavenger_task,
                      run_user_profiling_analysis_task,
                      run_intelligence_report_task, run_kol_report_task, run_full_analysis_pipeline,
                      run_post_insights_task)


def get_utc_time():
    """获取UTC时间"""
    return datetime.now(timezone.utc)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='X/Twitter 信息收集与分析系统')
    parser.add_argument('--task',
                       choices=['high_freq', 'medium_freq', 'low_freq', 'full_crawl', 'user_profiling', 'scavenger',
                               'user_analysis', 'intelligence_report', 'kol_report', 'full_analysis', 'post_insights'],
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

    # 分析任务相关参数
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='批处理大小（用于帖子洞察分析）')
    parser.add_argument('--user-limit', type=int, default=50,
                       help='用户分析数量限制')
    parser.add_argument('--days', type=int, default=30,
                       help='分析天数范围')
    parser.add_argument('--hours', type=int, default=24,
                       help='报告时间范围（小时）')
    parser.add_argument('--report-limit', type=int, default=300,
                       help='报告分析的最大帖子数量')
    parser.add_argument('--flow', choices=['dual', 'light', 'deep', 'intelligence'], default='dual',
                       help='报告生成流程 (dual=双轨制, light=仅日报资讯, deep=仅深度报告, intelligence=原有多模型并行)')
    parser.add_argument('--hours-back', type=int, default=36,
                       help='洞察分析回溯小时数')
    parser.add_argument('--user-id', type=int,
                       help='用户ID（用于KOL报告生成）')

    args = parser.parse_args()

    task_limits = config.get_task_limits_config()

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
        result = run_crawl_task('high', max_workers=args.max_workers, limit=task_limits['high_limit'])
    elif args.task == 'medium_freq':
        result = run_crawl_task('medium', max_workers=args.max_workers, limit=task_limits['medium_limit'])
    elif args.task == 'low_freq':
        result = run_crawl_task('low', max_workers=args.max_workers, limit=task_limits['low_limit'])
    elif args.task == 'full_crawl':
        result = run_full_crawl_task(max_workers=args.max_workers)
    elif args.task == 'user_profiling':
        result = run_user_profiling_task()
    elif args.task == 'scavenger':
        result = run_scavenger_task(max_workers=args.max_workers, limit=100)

    # 分析任务
    elif args.task == 'user_analysis':
        result = run_user_profiling_analysis_task(limit=args.user_limit, days=args.days)
    elif args.task == 'intelligence_report':
        result = run_intelligence_report_task(hours=args.hours, limit=args.report_limit, flow=args.flow)
    elif args.task == 'kol_report':
        if args.user_id is None:
            print("❌ KOL报告需要指定 --user-id 参数")
            sys.exit(1)
        result = run_kol_report_task(user_id=args.user_id, days=args.days)
    elif args.task == 'full_analysis':
        result = run_full_analysis_pipeline(
            user_limit=args.user_limit,
            user_days=args.days,
            report_hours=args.hours,
            report_limit=args.report_limit
        )
    elif args.task == 'post_insights':
        result = run_post_insights_task(hours_back=args.hours_back, batch_size=args.batch_size)
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

    # 采集任务的结果显示
    if task_type in ['high_freq', 'medium_freq', 'low_freq', 'full_crawl', 'scavenger']:
        print(f"   处理用户: {result.get('users_processed', 0)} 个")
        print(f"   成功爬取: {result.get('users_success', 0)} 个")
        print(f"   失败次数: {result.get('users_failed', 0)} 个")
        print(f"   新增帖子: {result.get('posts_inserted', 0)} 条")
        print(f"   耗时: {result.get('elapsed_seconds', 0):.1f} 秒")

    # 用户画像任务
    elif task_type == 'user_profiling':
        print(f"   更新用户分组: {result.get('users_updated', 0)} 个")

    # 用户档案分析任务
    elif task_type == 'user_analysis':
        print(f"   处理用户: {result.get('users_processed', 0)} 个")
        print(f"   成功分析: {result.get('users_success', 0)} 个")
        print(f"   失败次数: {result.get('users_failed', 0)} 个")

    # 情报报告生成任务
    elif task_type == 'intelligence_report':
        print(f"   报告标题: {result.get('report_title', '未知')}")
        print(f"   分析帖子: {result.get('posts_analyzed', 0)} 条")
        print(f"   时间范围: {result.get('time_range', '未知')}")

    # KOL报告生成任务
    elif task_type == 'kol_report':
        print(f"   报告标题: {result.get('report_title', '未知')}")
        print(f"   用户: @{result.get('user_handle', '未知')}")

    # 完整分析流水线
    elif task_type == 'full_analysis':
        print(f"   流水线状态: {result.get('message', '未知')}")
        pipeline_results = result.get('pipeline_results', {})

        if 'user_profiling' in pipeline_results:
            up_result = pipeline_results['user_profiling']
            print(f"   用户档案: 处理 {up_result.get('users_processed', 0)} 个，成功 {up_result.get('users_success', 0)} 个")

        if 'intelligence_report' in pipeline_results:
            ir_result = pipeline_results['intelligence_report']
            print(f"   情报报告: {ir_result.get('report_title', '未知')}")

    # 显示自定义消息
    if 'message' in result and task_type != 'full_analysis':
        print(f"   备注: {result['message']}")


if __name__ == "__main__":
    main()

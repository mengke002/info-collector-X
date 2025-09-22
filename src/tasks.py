"""
任务调度模块
"""
import logging
from typing import Dict, Any, Optional

from .processor import UserDataProcessor
from .database import DatabaseManager
from .config import config as app_config

# 导入分析模块
from .post_enrichment import run_post_enrichment
from .user_profiling import run_user_profiling
from .intelligence_reports import run_daily_intelligence_report, run_kol_report
from .post_processor import TwitterPostProcessor

logger = logging.getLogger(__name__)


def run_crawl_task(crawl_group: str, max_workers: int = 1, limit: Optional[int] = None) -> Dict[str, Any]:
    """执行爬取任务

    Args:
        crawl_group: 爬取分组 (high/medium/low)
        max_workers: 最大并发线程数
        limit: 最大处理用户数

    Returns:
        任务执行结果
    """
    try:
        processor = UserDataProcessor()

        # 检查是否在睡眠窗口
        if processor.check_sleep_window():
            return {
                'success': True,
                'users_processed': 0,
                'users_success': 0,
                'users_failed': 0,
                'posts_inserted': 0,
                'elapsed_seconds': 0,
                'message': '当前在睡眠窗口内，跳过爬取任务'
            }

        # 获取待爬取的用户
        db_manager = DatabaseManager()

        if limit is None:
            limits_config = app_config.get_task_limits_config()
            group_limit_map = {
                'high': limits_config['high_limit'],
                'medium': limits_config['medium_limit'],
                'low': limits_config['low_limit'],
            }
            limit = group_limit_map.get(crawl_group, limits_config['medium_limit'])

        users = db_manager.get_users_for_crawl(crawl_group, limit)

        if not users:
            logger.info(f"没有找到 {crawl_group} 分组的待爬取用户")
            return {
                'success': True,
                'users_processed': 0,
                'users_success': 0,
                'users_failed': 0,
                'posts_inserted': 0,
                'elapsed_seconds': 0,
                'message': f'没有找到 {crawl_group} 分组的待爬取用户'
            }

        # 批量处理用户
        result = processor.process_users_batch(users, max_workers)
        result['crawl_group'] = crawl_group

        return result

    except Exception as e:
        logger.error(f"爬取任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'users_processed': 0,
            'users_success': 0,
            'users_failed': 0,
            'posts_inserted': 0,
            'elapsed_seconds': 0,
        }


def run_full_crawl_task(max_workers: int = 1, batch_size: int = 50) -> Dict[str, Any]:
    """执行全量爬取任务

    Args:
        max_workers: 最大并发线程数
        batch_size: 每批处理的用户数

    Returns:
        任务执行结果
    """
    try:
        processor = UserDataProcessor()
        db_manager = DatabaseManager()

        # 检查是否在睡眠窗口
        if processor.check_sleep_window():
            return {
                'success': True,
                'total_users': 0,
                'users_processed': 0,
                'users_success': 0,
                'users_failed': 0,
                'posts_inserted': 0,
                'elapsed_seconds': 0,
                'message': '当前在睡眠窗口内，跳过全量爬取任务'
            }

        # 获取所有活跃用户
        all_users = db_manager.get_all_active_users()

        if not all_users:
            logger.info("没有找到活跃用户")
            return {
                'success': True,
                'total_users': 0,
                'users_processed': 0,
                'users_success': 0,
                'users_failed': 0,
                'posts_inserted': 0,
                'elapsed_seconds': 0,
                'message': '没有找到活跃用户'
            }

        logger.info(f"开始全量爬取，共 {len(all_users)} 个用户，批次大小 {batch_size}")

        # 分批处理
        total_processed = 0
        total_success = 0
        total_failed = 0
        total_posts = 0
        total_elapsed = 0

        for i in range(0, len(all_users), batch_size):
            batch_users = all_users[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(all_users) + batch_size - 1) // batch_size

            logger.info(f"处理第 {batch_num}/{total_batches} 批，{len(batch_users)} 个用户")

            batch_result = processor.process_users_batch(batch_users, max_workers,
                                                        delay_after_batch=(batch_num < total_batches))

            total_processed += batch_result['users_processed']
            total_success += batch_result['users_success']
            total_failed += batch_result['users_failed']
            total_posts += batch_result['posts_inserted']
            total_elapsed += batch_result['elapsed_seconds']

            if not batch_result['success']:
                logger.warning(f"第 {batch_num} 批处理有问题，但继续执行")

        return {
            'success': True,
            'total_users': len(all_users),
            'users_processed': total_processed,
            'users_success': total_success,
            'users_failed': total_failed,
            'posts_inserted': total_posts,
            'elapsed_seconds': total_elapsed,
        }

    except Exception as e:
        logger.error(f"全量爬取任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'total_users': 0,
            'users_processed': 0,
            'users_success': 0,
            'users_failed': 0,
            'posts_inserted': 0,
            'elapsed_seconds': 0,
        }


def run_user_profiling_task() -> Dict[str, Any]:
    """执行用户画像更新任务

    Returns:
        任务执行结果
    """
    try:
        processor = UserDataProcessor()
        result = processor.update_user_profiling()
        return result

    except Exception as e:
        logger.error(f"用户画像任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'users_updated': 0,
            'elapsed_seconds': 0,
        }


def run_scavenger_task(max_workers: int = 1, limit: int = 100, hours_back: int = 12) -> Dict[str, Any]:
    """执行清道夫任务（处理长时间未调度的用户）

    Args:
        max_workers: 最大并发线程数
        limit: 最大处理用户数
        hours_back: 回溯小时数

    Returns:
        任务执行结果
    """
    try:
        processor = UserDataProcessor()
        db_manager = DatabaseManager()

        # 获取长时间未调度的用户
        stale_users = db_manager.get_stale_users(hours_back, limit)

        if not stale_users:
            logger.info(f"没有找到长时间未调度的用户（回溯 {hours_back} 小时）")
            return {
                'success': True,
                'users_processed': 0,
                'users_success': 0,
                'users_failed': 0,
                'posts_inserted': 0,
                'elapsed_seconds': 0,
                'message': f'没有找到长时间未调度的用户（回溯 {hours_back} 小时）'
            }

        # 批量处理用户
        result = processor.process_users_batch(stale_users, max_workers)
        result['task_type'] = 'scavenger'
        result['hours_back'] = hours_back

        return result

    except Exception as e:
        logger.error(f"清道夫任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'users_processed': 0,
            'users_success': 0,
            'users_failed': 0,
            'posts_inserted': 0,
            'elapsed_seconds': 0,
        }


def run_init_users_task(csv_file_path: str) -> Dict[str, Any]:
    """执行用户初始化任务

    Args:
        csv_file_path: CSV文件路径

    Returns:
        任务执行结果
    """
    try:
        processor = UserDataProcessor()
        success = processor.initialize_users_from_csv(csv_file_path)

        if success:
            return {
                'success': True,
                'message': '用户数据初始化完成'
            }
        else:
            return {
                'success': False,
                'error': '用户数据初始化失败'
            }

    except Exception as e:
        logger.error(f"用户初始化任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e)
        }


# 分析任务函数

def run_post_enrichment_task(batch_size: int = 100, max_workers: int = 3) -> Dict[str, Any]:
    """执行帖子富化分析任务

    Args:
        batch_size: 批处理大小
        max_workers: 最大并发数

    Returns:
        任务执行结果
    """
    try:
        logger.info(f"开始执行帖子富化分析任务，批次大小: {batch_size}, 并发数: {max_workers}")
        result = run_post_enrichment(batch_size, max_workers)

        return {
            'success': True,
            'task_type': 'post_enrichment',
            'posts_processed': result.get('total', 0),
            'posts_success': result.get('success', 0),
            'posts_failed': result.get('failed', 0),
            'message': f"帖子富化分析完成: 处理{result.get('total', 0)}条，成功{result.get('success', 0)}条"
        }

    except Exception as e:
        logger.error(f"帖子富化分析任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'task_type': 'post_enrichment',
            'posts_processed': 0,
            'posts_success': 0,
            'posts_failed': 0,
        }


def run_user_profiling_analysis_task(limit: int = 50, days: int = 30) -> Dict[str, Any]:
    """执行用户档案分析任务

    Args:
        limit: 最大处理用户数
        days: 分析天数范围

    Returns:
        任务执行结果
    """
    try:
        logger.info(f"开始执行用户档案分析任务，用户数: {limit}, 分析天数: {days}")
        result = run_user_profiling(limit, days)

        return {
            'success': True,
            'task_type': 'user_profiling_analysis',
            'users_processed': result.get('total', 0),
            'users_success': result.get('success', 0),
            'users_failed': result.get('failed', 0),
            'message': f"用户档案分析完成: 处理{result.get('total', 0)}个用户，成功{result.get('success', 0)}个"
        }

    except Exception as e:
        logger.error(f"用户档案分析任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'task_type': 'user_profiling_analysis',
            'users_processed': 0,
            'users_success': 0,
            'users_failed': 0,
        }


def run_intelligence_report_task(hours: int = 24, limit: int = 300) -> Dict[str, Any]:
    """执行情报报告生成任务

    Args:
        hours: 时间范围（小时）
        limit: 最大帖子数量

    Returns:
        任务执行结果
    """
    try:
        logger.info(f"开始执行情报报告生成任务，时间范围: {hours}小时, 最大帖子数: {limit}")
        result = run_daily_intelligence_report(hours, limit)

        if result['success']:
            return {
                'success': True,
                'task_type': 'intelligence_report',
                'report_title': result.get('report_title'),
                'posts_analyzed': result.get('posts_count', 0),
                'time_range': result.get('time_range'),
                'message': f"情报报告生成成功: 分析了{result.get('posts_count', 0)}条帖子"
            }
        else:
            return {
                'success': False,
                'error': result.get('error'),
                'task_type': 'intelligence_report',
                'posts_analyzed': result.get('posts_count', 0),
            }

    except Exception as e:
        logger.error(f"情报报告生成任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'task_type': 'intelligence_report',
            'posts_analyzed': 0,
        }


def run_kol_report_task(user_id: int, days: int = 30) -> Dict[str, Any]:
    """执行KOL报告生成任务

    Args:
        user_id: 用户ID
        days: 分析天数

    Returns:
        任务执行结果
    """
    try:
        logger.info(f"开始执行KOL报告生成任务，用户ID: {user_id}, 分析天数: {days}")
        result = run_kol_report(user_id, days)

        if result['success']:
            return {
                'success': True,
                'task_type': 'kol_report',
                'report_title': result.get('report_title'),
                'user_handle': result.get('user_handle'),
                'message': f"KOL报告生成成功: @{result.get('user_handle')}"
            }
        else:
            return {
                'success': False,
                'error': result.get('error'),
                'task_type': 'kol_report',
                'user_id': user_id,
            }

    except Exception as e:
        logger.error(f"KOL报告生成任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'task_type': 'kol_report',
            'user_id': user_id,
        }


def run_full_analysis_pipeline(post_batch_size: int = 100,
                              post_max_workers: int = 3,
                              user_limit: int = 50,
                              user_days: int = 30,
                              report_hours: int = 24,
                              report_limit: int = 300) -> Dict[str, Any]:
    """执行完整的分析挖掘流水线

    Args:
        post_batch_size: 帖子富化批次大小
        post_max_workers: 帖子富化并发数
        user_limit: 用户档案分析数量
        user_days: 用户档案分析天数
        report_hours: 报告时间范围（小时）
        report_limit: 报告分析帖子数量上限

    Returns:
        任务执行结果
    """
    logger.info("开始执行完整的分析挖掘流水线")

    pipeline_results = {}
    overall_success = True

    # 第一步：帖子富化分析
    logger.info("=== 第一步：执行帖子富化分析 ===")
    enrichment_result = run_post_enrichment_task(post_batch_size, post_max_workers)
    pipeline_results['post_enrichment'] = enrichment_result
    if not enrichment_result['success']:
        overall_success = False
        logger.error("帖子富化分析失败，终止流水线执行")
        return {
            'success': False,
            'pipeline_results': pipeline_results,
            'error': '帖子富化分析失败'
        }

    # 第二步：用户档案分析
    logger.info("=== 第二步：执行用户档案分析 ===")
    profiling_result = run_user_profiling_analysis_task(user_limit, user_days)
    pipeline_results['user_profiling'] = profiling_result
    if not profiling_result['success']:
        overall_success = False
        logger.warning("用户档案分析失败，但继续执行报告生成")

    # 第三步：情报报告生成
    logger.info("=== 第三步：执行情报报告生成 ===")
    report_result = run_intelligence_report_task(report_hours, report_limit)
    pipeline_results['intelligence_report'] = report_result
    if not report_result['success']:
        overall_success = False
        logger.error("情报报告生成失败")

    return {
        'success': overall_success,
        'pipeline_results': pipeline_results,
        'message': f"分析挖掘流水线完成，整体状态: {'成功' if overall_success else '部分失败'}"
    }


def test_crawler_connection() -> Dict[str, Any]:
    """测试爬虫连接

    Returns:
        测试结果
    """
    try:
        processor = UserDataProcessor()
        success = processor.crawler.test_connection()

        if success:
            return {
                'success': True,
                'message': 'RSS Hub连接测试成功'
            }
        else:
            return {
                'success': False,
                'error': 'RSS Hub连接测试失败'
            }

    except Exception as e:
        logger.error(f"连接测试失败: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def run_postprocess_task(hours_back: int = 36) -> Dict[str, Any]:
    """执行帖子后处理任务

    Args:
        hours_back: 回溯小时数

    Returns:
        任务执行结果
    """
    try:
        logger.info(f"开始执行帖子后处理任务，回溯 {hours_back} 小时")

        processor = TwitterPostProcessor()
        result = processor.process_posts(hours_back)

        # 获取处理统计信息
        stats = processor.get_processing_stats(hours_back)

        return {
            'success': True,
            'task_type': 'postprocess',
            'hours_back': hours_back,
            'posts_total': result['total'],
            'posts_success': result['success'],
            'posts_failed': result['failed'],
            'success_rate': round(result['success'] / max(result['total'], 1) * 100, 2),
            'processing_stats': stats,
            'message': f"帖子后处理完成: 处理{result['total']}条，成功{result['success']}条，失败{result['failed']}条"
        }

    except Exception as e:
        logger.error(f"帖子后处理任务执行失败: {e}")
        return {
            'success': False,
            'error': str(e),
            'task_type': 'postprocess',
            'hours_back': hours_back,
            'posts_total': 0,
            'posts_success': 0,
            'posts_failed': 0,
            'success_rate': 0.0,
        }

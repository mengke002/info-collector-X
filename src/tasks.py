"""
任务调度模块
"""
import logging
from typing import Dict, Any, Optional

from .processor import UserDataProcessor
from .database import DatabaseManager
from .config import config as app_config

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

"""
用户数据处理器模块
"""
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from .crawler import RSSCrawler
from .database import DatabaseManager

logger = logging.getLogger(__name__)


class UserDataProcessor:
    """用户数据处理器"""

    def __init__(self, config=None):
        """初始化用户数据处理器

        Args:
            config: 配置对象
        """
        if config is None:
            from .config import config as default_config
            config = default_config

        self.config = config
        self.crawler_config = config.get_crawler_config()
        self.groups_config = config.get_crawl_groups_config()
        self.failure_config = config.get_failure_handling_config()

        self.crawler = RSSCrawler(config)
        self.db_manager = DatabaseManager(config, auto_init=False)

    def process_single_user(self, user_info: Dict[str, Any]) -> Dict[str, Any]:
        """处理单个用户的数据采集

        Args:
            user_info: 用户信息字典

        Returns:
            处理结果
        """
        user_id = user_info['id']
        user_handle = user_info['user_id']

        result = {
            'user_id': user_id,
            'user_handle': user_handle,
            'success': False,
            'posts_count': 0,
            'error': None
        }

        try:
            logger.info(f"开始处理用户 @{user_handle} (ID: {user_id})")

            # 爬取用户帖子数据
            posts_data = self.crawler.crawl_user_posts(user_handle)

            if posts_data is None:
                # 爬取失败，更新失败状态
                retry_time = self._calculate_retry_time()
                self.db_manager.update_user_crawl_failure(user_id, retry_time)
                result['error'] = '爬取RSS数据失败'
                logger.error(f"用户 @{user_handle} 爬取失败")
                return result

            # 处理帖子数据
            processed_posts = []
            for post_data in posts_data:
                post_data['user_table_id'] = user_id
                processed_posts.append(post_data)

            # 存储帖子数据
            if processed_posts:
                inserted_count = self.db_manager.insert_posts(processed_posts)
                result['posts_count'] = inserted_count
                logger.info(f"用户 @{user_handle} 新增 {inserted_count} 条帖子")

            # 更新用户成功状态
            next_crawl_time = self._calculate_next_crawl_time(user_info['crawl_group'])
            self.db_manager.update_user_crawl_success(user_id, next_crawl_time)

            result['success'] = True
            logger.info(f"用户 @{user_handle} 处理完成")

        except Exception as e:
            # 处理过程中发生异常
            logger.error(f"处理用户 @{user_handle} 时发生异常: {e}")
            retry_time = self._calculate_retry_time()
            self.db_manager.update_user_crawl_failure(user_id, retry_time)
            result['error'] = str(e)

        return result

    def process_users_batch(self, users: List[Dict[str, Any]], max_workers: int = 1) -> Dict[str, Any]:
        """批量处理用户数据

        Args:
            users: 用户信息列表
            max_workers: 最大并发线程数

        Returns:
            批量处理结果
        """
        if not users:
            return {
                'success': True,
                'users_processed': 0,
                'users_success': 0,
                'users_failed': 0,
                'posts_inserted': 0,
                'elapsed_seconds': 0,
            }

        start_time = time.time()
        logger.info(f"开始批量处理 {len(users)} 个用户，并发数: {max_workers}")

        results = []
        posts_inserted = 0

        if max_workers == 1:
            # 串行处理
            for user in users:
                result = self.process_single_user(user)
                results.append(result)
                posts_inserted += result['posts_count']

                # 单个用户处理完成后的延迟
                if len(results) < len(users):  # 不是最后一个用户
                    delay = random.uniform(6, 12)
                    logger.debug(f"用户间延迟 {delay:.1f} 秒")
                    time.sleep(delay)
        else:
            # 并发处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                future_to_user = {
                    executor.submit(self.process_single_user, user): user
                    for user in users
                }

                # 收集结果
                for future in as_completed(future_to_user):
                    try:
                        result = future.result()
                        results.append(result)
                        posts_inserted += result['posts_count']
                    except Exception as e:
                        user = future_to_user[future]
                        logger.error(f"并发处理用户 {user['user_id']} 时发生异常: {e}")
                        results.append({
                            'user_id': user['id'],
                            'user_handle': user['user_id'],
                            'success': False,
                            'posts_count': 0,
                            'error': str(e)
                        })

        # 批次处理完成后的延迟
        if max_workers > 1:
            batch_delay = random.uniform(
                self.crawler_config['batch_interval_min'],
                self.crawler_config['batch_interval_max']
            )
            logger.info(f"批次处理完成，延迟 {batch_delay:.1f} 秒")
            time.sleep(batch_delay)

        # 统计结果
        users_success = sum(1 for r in results if r['success'])
        users_failed = len(results) - users_success
        elapsed_seconds = time.time() - start_time

        logger.info(f"批量处理完成: 成功 {users_success}，失败 {users_failed}，新增帖子 {posts_inserted}，耗时 {elapsed_seconds:.1f}秒")

        return {
            'success': True,
            'users_processed': len(results),
            'users_success': users_success,
            'users_failed': users_failed,
            'posts_inserted': posts_inserted,
            'elapsed_seconds': elapsed_seconds,
            'details': results
        }

    def _calculate_next_crawl_time(self, crawl_group: str) -> datetime:
        """计算下次爬取时间

        Args:
            crawl_group: 爬取分组

        Returns:
            下次爬取时间
        """
        now = datetime.now(timezone.utc)

        if crawl_group == 'high':
            # 高频：每20分钟
            interval_minutes = self.groups_config['high_interval_minutes']
            next_time = now + timedelta(minutes=interval_minutes)
        elif crawl_group == 'medium':
            # 中频：每2小时
            interval_hours = self.groups_config['medium_interval_hours']
            next_time = now + timedelta(hours=interval_hours)
        elif crawl_group == 'low':
            # 低频：每6小时
            interval_hours = self.groups_config['low_interval_hours']
            next_time = now + timedelta(hours=interval_hours)
        else:
            # 默认中频
            next_time = now + timedelta(hours=2)

        return next_time

    def _calculate_retry_time(self) -> datetime:
        """计算重试时间（带随机抖动）

        Returns:
            重试时间
        """
        now = datetime.now(timezone.utc)

        # 随机延迟15-25分钟
        retry_delay = random.uniform(
            self.failure_config['retry_delay_min'],
            self.failure_config['retry_delay_max']
        )

        return now + timedelta(minutes=retry_delay)

    def check_sleep_window(self) -> bool:
        """检查是否在睡眠窗口内

        Returns:
            是否在睡眠窗口内
        """
        sleep_config = self.config.get_sleep_window_config()
        now_utc = datetime.now(timezone.utc)
        current_hour = now_utc.hour

        start_hour = sleep_config['start_hour']
        end_hour = sleep_config['end_hour']

        # 检查是否在睡眠窗口内 (UTC 17:00 - 22:59)
        if start_hour <= current_hour <= end_hour:
            logger.info(f"当前时间 {now_utc.strftime('%H:%M UTC')} 在睡眠窗口内，跳过爬取任务")
            return True

        return False

    def initialize_users_from_csv(self, csv_file_path: str) -> bool:
        """从CSV文件初始化用户数据

        Args:
            csv_file_path: CSV文件路径

        Returns:
            是否初始化成功
        """
        try:
            self.db_manager.init_users_from_csv(csv_file_path)
            logger.info("用户数据初始化完成")
            return True
        except Exception as e:
            logger.error(f"用户数据初始化失败: {e}")
            return False

    def update_user_profiling(self) -> Dict[str, Any]:
        """更新用户画像

        Returns:
            更新结果
        """
        try:
            start_time = time.time()
            updated_count = self.db_manager.update_user_profiling()
            elapsed_seconds = time.time() - start_time

            logger.info(f"用户画像更新完成，更新 {updated_count} 个用户")

            return {
                'success': True,
                'users_updated': updated_count,
                'elapsed_seconds': elapsed_seconds,
            }

        except Exception as e:
            logger.error(f"用户画像更新失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'users_updated': 0,
                'elapsed_seconds': 0,
            }
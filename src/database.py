"""
MySQL 数据库管理器
"""
import logging
import pymysql
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, config=None, auto_init=True):
        """初始化数据库管理器

        Args:
            config: 配置对象
            auto_init: 是否自动初始化数据库表
        """
        if config is None:
            from .config import config as default_config
            config = default_config

        self.config = config
        self.db_config = config.get_database_config()

        if auto_init:
            self.init_database()

    @contextmanager
    def get_connection(self):
        """获取数据库连接上下文管理器"""
        conn = None
        try:
            conn = pymysql.connect(**self.db_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def init_database(self):
        """初始化数据库表结构"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 创建 twitter_users 表
                cursor.execute(self._get_twitter_users_table_sql())
                logger.info("已创建或确认 twitter_users 表")

                # 创建 twitter_posts 表
                cursor.execute(self._get_twitter_posts_table_sql())
                logger.info("已创建或确认 twitter_posts 表")

                conn.commit()
                logger.info("数据库表初始化完成")

        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise

    def _get_twitter_users_table_sql(self) -> str:
        """获取创建 twitter_users 表的SQL"""
        return """
        CREATE TABLE IF NOT EXISTS `twitter_users` (
          `id` INT AUTO_INCREMENT PRIMARY KEY,
          `user_id` VARCHAR(255) NOT NULL COMMENT 'Twitter用户的Handle (如 @elonmusk)',
          `crawl_group` ENUM('high', 'medium', 'low') NOT NULL DEFAULT 'medium' COMMENT '爬取频率分组',
          `last_crawled_at` DATETIME DEFAULT NULL COMMENT '上次成功爬取的时间',
          `next_crawl_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '计划的下次爬取时间',
          `crawl_status` ENUM('pending', 'success', 'failed', 'quarantined') NOT NULL DEFAULT 'pending' COMMENT '上次爬取任务状态',
          `failed_attempts` INT NOT NULL DEFAULT 0 COMMENT '连续失败尝试次数',
          `avg_posts_per_day` FLOAT DEFAULT 0.0 COMMENT '日均发帖量，用于动态调整分组',
          `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY `uniq_user_id` (`user_id`),
          KEY `idx_crawl_group` (`crawl_group`),
          KEY `idx_next_crawl_time` (`next_crawl_time`),
          KEY `idx_crawl_status` (`crawl_status`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='待监控的Twitter用户列表';
        """

    def _get_twitter_posts_table_sql(self) -> str:
        """获取创建 twitter_posts 表的SQL"""
        return """
        CREATE TABLE IF NOT EXISTS `twitter_posts` (
          `id` INT AUTO_INCREMENT PRIMARY KEY,
          `user_table_id` INT NOT NULL COMMENT '关联到twitter_users表的ID',
          `post_url` VARCHAR(512) NOT NULL COMMENT '帖子唯一链接',
          `post_content` TEXT COMMENT '帖子内容 (由HTML转换的Markdown格式)',
          `post_type` ENUM('Original', 'Reply', 'Quote', 'LinkShare') DEFAULT 'Original' COMMENT '帖子类型，通过规则预判断',
          `media_urls` JSON DEFAULT NULL COMMENT '帖子中包含的媒体链接 (图片/视频)',
          `published_at` DATETIME DEFAULT NULL COMMENT '帖子发布时间',
          `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          UNIQUE KEY `uniq_post_url` (`post_url`),
          KEY `idx_user_table_id` (`user_table_id`),
          KEY `idx_published_at` (`published_at`),
          KEY `idx_post_type` (`post_type`),
          CONSTRAINT `fk_post_user` FOREIGN KEY (`user_table_id`) REFERENCES `twitter_users` (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='收集到的Twitter帖子数据';
        """

    def recreate_tables(self):
        """删除并重新创建所有表"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 删除表（注意外键约束的顺序）
                cursor.execute("DROP TABLE IF EXISTS twitter_posts")
                cursor.execute("DROP TABLE IF EXISTS twitter_users")
                logger.info("已删除现有表")

                conn.commit()

                # 重新创建表
                self.init_database()
                logger.info("表重新创建完成")

        except Exception as e:
            logger.error(f"重新创建表失败: {e}")
            raise

    def init_users_from_csv(self, csv_file_path: str):
        """从CSV文件初始化用户数据

        Args:
            csv_file_path: CSV文件路径
        """
        import pandas as pd

        try:
            # 读取CSV文件
            df = pd.read_csv(csv_file_path)
            logger.info(f"从CSV文件读取到 {len(df)} 个用户")

            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 准备插入语句
                insert_sql = """
                INSERT IGNORE INTO twitter_users (user_id, crawl_group)
                VALUES (%s, %s)
                """

                inserted_count = 0
                for _, row in df.iterrows():
                    user_id = row['id'].strip('@') if row['id'].startswith('@') else row['id']

                    cursor.execute(insert_sql, (user_id, 'medium'))
                    if cursor.rowcount > 0:
                        inserted_count += 1

                conn.commit()
                logger.info(f"成功插入 {inserted_count} 个新用户到数据库")

        except Exception as e:
            logger.error(f"从CSV初始化用户失败: {e}")
            raise

    def get_users_for_crawl(self, crawl_group: str, limit: int = 10) -> List[Dict[str, Any]]:
        """获取待爬取的用户列表

        Args:
            crawl_group: 爬取分组 (high/medium/low)
            limit: 最大返回数量

        Returns:
            用户信息列表
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                sql = """
                SELECT id, user_id, crawl_group, last_crawled_at, failed_attempts
                FROM twitter_users
                WHERE crawl_group = %s
                  AND next_crawl_time <= NOW()
                  AND crawl_status != 'quarantined'
                ORDER BY RAND()
                LIMIT %s
                """

                cursor.execute(sql, (crawl_group, limit))
                users = cursor.fetchall()

                logger.info(f"获取到 {len(users)} 个 {crawl_group} 分组的待爬取用户")
                return users

        except Exception as e:
            logger.error(f"获取待爬取用户失败: {e}")
            raise

    def get_all_active_users(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """获取所有活跃用户（用于全量爬取）

        Args:
            limit: 限制返回数量，None表示不限制

        Returns:
            用户信息列表
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                sql = """
                SELECT id, user_id, crawl_group, last_crawled_at, failed_attempts
                FROM twitter_users
                WHERE crawl_status != 'quarantined'
                ORDER BY last_crawled_at ASC
                """

                if limit:
                    sql += f" LIMIT {limit}"

                cursor.execute(sql)
                users = cursor.fetchall()

                logger.info(f"获取到 {len(users)} 个活跃用户用于全量爬取")
                return users

        except Exception as e:
            logger.error(f"获取活跃用户失败: {e}")
            raise

    def get_stale_users(self, hours_back: int = 12, limit: int = 100) -> List[Dict[str, Any]]:
        """获取长时间未被调度的用户（清道夫任务）

        Args:
            hours_back: 回溯小时数
            limit: 最大返回数量

        Returns:
            用户信息列表
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                sql = """
                SELECT id, user_id, crawl_group, last_crawled_at, failed_attempts
                FROM twitter_users
                WHERE next_crawl_time < NOW() - INTERVAL %s HOUR
                  AND crawl_status = 'pending'
                  AND crawl_status != 'quarantined'
                ORDER BY next_crawl_time ASC
                LIMIT %s
                """

                cursor.execute(sql, (hours_back, limit))
                users = cursor.fetchall()

                logger.info(f"获取到 {len(users)} 个长时间未调度的用户")
                return users

        except Exception as e:
            logger.error(f"获取长时间未调度用户失败: {e}")
            raise

    def update_user_crawl_success(self, user_id: int, next_crawl_time: datetime) -> bool:
        """更新用户爬取成功状态

        Args:
            user_id: 用户ID
            next_crawl_time: 下次爬取时间

        Returns:
            是否更新成功
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                sql = """
                UPDATE twitter_users
                SET last_crawled_at = NOW(),
                    next_crawl_time = %s,
                    crawl_status = 'success',
                    failed_attempts = 0,
                    updated_at = NOW()
                WHERE id = %s
                """

                cursor.execute(sql, (next_crawl_time, user_id))
                conn.commit()

                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"更新用户爬取成功状态失败: {e}")
            return False

    def update_user_crawl_failure(self, user_id: int, retry_time: datetime) -> bool:
        """更新用户爬取失败状态

        Args:
            user_id: 用户ID
            retry_time: 重试时间

        Returns:
            是否更新成功
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 先获取当前失败次数
                cursor.execute("SELECT failed_attempts FROM twitter_users WHERE id = %s", (user_id,))
                result = cursor.fetchone()
                if not result:
                    return False

                current_failures = result[0]
                new_failures = current_failures + 1

                # 检查是否需要隔离
                failure_config = self.config.get_failure_handling_config()
                max_failures = failure_config['max_failed_attempts']

                if new_failures >= max_failures:
                    # 进入隔离状态
                    sql = """
                    UPDATE twitter_users
                    SET failed_attempts = %s,
                        crawl_status = 'quarantined',
                        updated_at = NOW()
                    WHERE id = %s
                    """
                    cursor.execute(sql, (new_failures, user_id))
                    logger.warning(f"用户 {user_id} 因连续失败 {new_failures} 次被隔离")
                else:
                    # 更新失败状态但不隔离
                    sql = """
                    UPDATE twitter_users
                    SET failed_attempts = %s,
                        next_crawl_time = %s,
                        crawl_status = 'failed',
                        updated_at = NOW()
                    WHERE id = %s
                    """
                    cursor.execute(sql, (new_failures, retry_time, user_id))

                conn.commit()
                return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"更新用户爬取失败状态失败: {e}")
            return False

    def insert_posts(self, posts: List[Dict[str, Any]]) -> int:
        """批量插入帖子数据

        Args:
            posts: 帖子数据列表

        Returns:
            插入的记录数
        """
        if not posts:
            return 0

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                sql = """
                INSERT IGNORE INTO twitter_posts
                (user_table_id, post_url, post_content, post_type, media_urls, published_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """

                values = []
                for post in posts:
                    values.append((
                        post['user_table_id'],
                        post['post_url'],
                        post['post_content'],
                        post.get('post_type', 'Original'),
                        post.get('media_urls'),
                        post.get('published_at')
                    ))

                cursor.executemany(sql, values)
                conn.commit()

                inserted_count = cursor.rowcount
                logger.info(f"成功插入 {inserted_count} 条帖子数据")
                return inserted_count

        except Exception as e:
            logger.error(f"插入帖子数据失败: {e}")
            return 0

    def update_user_profiling(self) -> int:
        """更新用户画像（日均发帖量和分组），带冷启动托底策略

        - 日均= 近7天发帖数 / 实际观测天数（1..7）
        - 分组阈值：>10 高频，>1 中频，其余低频
        - 冷启动保护：近7天0帖且新建账户（created_at<3天）→ 中频

        Returns:
            更新的用户数量
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 用一次聚合 + JOIN 计算7日统计并更新，避免重复子查询
                sql = """
                UPDATE twitter_users tu
                LEFT JOIN (
                  SELECT
                    tp.user_table_id,
                    COUNT(*) AS cnt,
                    LEAST(
                      7,
                      GREATEST(1, DATEDIFF(CURDATE(), DATE(MIN(tp.published_at))) + 1)
                    ) AS days_observed
                  FROM twitter_posts tp
                  WHERE tp.published_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                  GROUP BY tp.user_table_id
                ) s ON s.user_table_id = tu.id
                SET
                  tu.avg_posts_per_day = COALESCE(s.cnt / NULLIF(s.days_observed, 0), 0),
                  tu.crawl_group = CASE
                    WHEN COALESCE(s.cnt / NULLIF(s.days_observed, 0), 0) > 10 THEN 'high'
                    WHEN COALESCE(s.cnt / NULLIF(s.days_observed, 0), 0) > 1 THEN 'medium'
                    WHEN COALESCE(s.cnt, 0) = 0 AND DATEDIFF(CURDATE(), DATE(tu.created_at)) < 3 THEN 'medium'
                    ELSE 'low'
                  END,
                  tu.updated_at = NOW()
                WHERE tu.crawl_status != 'quarantined'
                """

                cursor.execute(sql)
                conn.commit()

                updated_count = cursor.rowcount
                logger.info(f"成功更新 {updated_count} 个用户的画像信息")
                return updated_count

        except Exception as e:
            logger.error(f"更新用户画像失败: {e}")
            return 0

"""
用户数字档案分析器 (User Profiling)
基于用户的富化帖子数据，生成动态更新的用户数字档案
"""
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from .database import DatabaseManager
from .llm_client import get_llm_client
from .config import config

logger = logging.getLogger(__name__)


class UserProfileAnalyzer:
    """用户数字档案分析器"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """初始化分析器"""
        if db_manager is not None:
            self.db_manager = db_manager
        else:
            self.db_manager = DatabaseManager(config)

        self.llm_client = get_llm_client()
        if not self.llm_client:
            raise RuntimeError("LLM客户端初始化失败，无法进行用户档案分析")

        logger.info("用户数字档案分析器初始化完成")

    def format_user_posts_for_analysis(self, posts: List[Dict[str, Any]]) -> str:
        """
        将用户的帖子数据格式化为分析用的文本

        Args:
            posts: 用户的富化帖子数据

        Returns:
            格式化后的文本
        """
        formatted_posts = []

        for i, post in enumerate(posts, 1):
            # 格式化发布时间
            published_at = post.get('published_at')
            if published_at:
                if isinstance(published_at, str):
                    time_str = published_at[:10]  # 取日期部分
                else:
                    time_str = published_at.strftime('%Y-%m-%d')
            else:
                time_str = '未知日期'

            # 格式化帖子信息
            post_info = f"[T_{i}] [{time_str}] [{post.get('content_type', '未知类型')}] [{post.get('post_tag', '无标签')}] {post.get('post_content', '')[:200]}..."

            formatted_posts.append(post_info)

        return '\n'.join(formatted_posts)

    def get_user_profile_prompt(self, user_posts_collection: str, user_id: str) -> str:
        """
        构建用户数字档案生成的提示词

        Args:
            user_posts_collection: 格式化的用户帖子合集
            user_id: 用户ID

        Returns:
            完整的提示词
        """
        return f"""# Role: 资深FBI心理侧写师与数据分析师

# Context:
你正在分析一位目标人物在过去一个月内发布的所有公开言论。你的任务是基于这些原始数据，构建一份关于他/她的、可量化的深度数字档案。

# Input Data:
你将收到一份言论合集，包含了该用户过去一个月的所有帖子。每条帖子都附带了预处理的元数据。格式如下：
`[T_id] [YYYY-MM-DD] [内容类型] [内容标签] 帖子内容...`

'''
{user_posts_collection}
'''

# Your Task:
请基于以上全部信息，深入分析并以严格的JSON格式返回该用户的数字档案。

{{
  "top_keywords": [
    "分析所有帖子，提取出最能代表其本月思考核心的5个关键词"
  ],
  "sentiment_trend": "分析其整体情绪趋势，例如：'整体乐观，但在月底对市场表现出焦虑' 或 '持续保持批判和反思的态度'。",
  "mentioned_assets": {{
    "tools": ["列出本月提及的所有工具名称"],
    "stocks": ["列出本月提及的所有股票/投资标的"],
    "projects": ["列出本月提及的所有项目名称"]
  }},
  "content_format_ratio": {{
    "original_thought_percentage": "估算原创观点帖子的百分比",
    "link_sharing_percentage": "估算链接分享帖子的百分比",
    "reply_interaction_percentage": "估算回复互动帖子的百分比"
  }},
  "interaction_graph": {{
    "top_5_interacted_users": [
      "分析其回复和@提及，列出互动最频繁的5个用户名"
    ]
  }},
  "network_role": "基于其发帖和互动模式，为其分配一个网络角色。候选角色：'意见领袖 (Influencer)', '社交枢纽 (Hub)', '信息源 (Source)', '学习者 (Learner)', '广播者 (Broadcaster)'。",
  "intellectual_trajectory_summary": "用一段话总结他/她本月的思想动态，回答问题：'与上个月相比，他/她本月的思考在朝着什么方向发展？'"
}}"""

    def analyze_user_profile(self, user_id: int, days: int = 30) -> Dict[str, Any]:
        """
        分析单个用户的数字档案

        Args:
            user_id: 用户ID
            days: 分析的天数范围

        Returns:
            分析结果
        """
        logger.info(f"开始分析用户 {user_id} 的数字档案")

        try:
            # 获取用户的富化帖子数据
            posts = self.db_manager.get_user_enriched_posts(user_id, days)

            if not posts:
                logger.warning(f"用户 {user_id} 在过去 {days} 天内没有富化帖子数据")
                return {
                    'user_id': user_id,
                    'success': False,
                    'error': '没有可分析的帖子数据'
                }

            # 获取用户ID字符串（用于提示词）
            user_info = self.db_manager.get_connection()
            with user_info as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM twitter_users WHERE id = %s", (user_id,))
                result = cursor.fetchone()
                user_handle = result[0] if result else f"user_{user_id}"

            # 格式化帖子数据
            user_posts_collection = self.format_user_posts_for_analysis(posts)

            # 构建提示词
            prompt = self.get_user_profile_prompt(user_posts_collection, user_handle)

            # 调用Smart LLM进行分析
            response = self.llm_client.call_smart_model(prompt, temperature=0.3)

            if not response['success']:
                logger.error(f"LLM调用失败，用户 {user_id}: {response.get('error')}")
                return {
                    'user_id': user_id,
                    'success': False,
                    'error': response.get('error')
                }

            # 提取JSON结果
            json_result = self.llm_client.extract_json_from_response(response['content'])

            if not json_result:
                logger.error(f"无法从LLM响应中提取JSON，用户 {user_id}")
                return {
                    'user_id': user_id,
                    'success': False,
                    'error': 'JSON提取失败'
                }

            # 添加统计信息
            json_result['analysis_period'] = {
                'days': days,
                'total_posts': len(posts),
                'analysis_date': datetime.now().isoformat()
            }

            # 保存用户画像到数据库
            if self.db_manager.save_user_profile(user_id, json_result):
                logger.info(f"用户 {user_id} 数字档案分析完成并已保存")
                return {
                    'user_id': user_id,
                    'success': True,
                    'profile_data': json_result
                }
            else:
                logger.error(f"用户 {user_id} 数字档案保存失败")
                return {
                    'user_id': user_id,
                    'success': False,
                    'error': '档案保存失败',
                    'profile_data': json_result
                }

        except Exception as e:
            logger.error(f"分析用户 {user_id} 数字档案时发生异常: {e}", exc_info=True)
            return {
                'user_id': user_id,
                'success': False,
                'error': str(e)
            }

    def get_users_for_profiling(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        获取需要进行档案分析的用户

        Args:
            limit: 最大返回数量

        Returns:
            用户列表
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                # 选择活跃用户进行档案分析
                # 优先选择最近有帖子且没有或档案较旧的用户
                sql = """
                SELECT DISTINCT u.id, u.user_id
                FROM twitter_users u
                JOIN twitter_posts p ON u.id = p.user_table_id
                JOIN post_insights pi ON p.id = pi.post_id
                LEFT JOIN twitter_user_profiles up ON u.id = up.user_table_id
                WHERE pi.status = 'completed'
                  AND p.published_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                  AND (up.id IS NULL OR up.generated_at < DATE_SUB(NOW(), INTERVAL 7 DAY))
                  AND u.crawl_status != 'quarantined'
                GROUP BY u.id, u.user_id
                HAVING COUNT(p.id) >= 3
                ORDER BY COUNT(p.id) DESC, u.last_crawled_at DESC
                LIMIT %s
                """

                cursor.execute(sql, (limit,))
                users = [{'id': row[0], 'user_id': row[1]} for row in cursor.fetchall()]

                logger.info(f"获取到 {len(users)} 个需要档案分析的用户")
                return users

        except Exception as e:
            logger.error(f"获取档案分析用户失败: {e}")
            return []

    def run_user_profiling(self, limit: int = 50, days: int = 30) -> Dict[str, Any]:
        """
        运行用户档案分析任务

        Args:
            limit: 最大处理用户数
            days: 分析天数范围

        Returns:
            处理结果统计
        """
        logger.info(f"开始运行用户档案分析任务，处理 {limit} 个用户")

        try:
            # 获取需要分析的用户
            users = self.get_users_for_profiling(limit)

            if not users:
                logger.info("没有需要档案分析的用户")
                return {'total': 0, 'success': 0, 'failed': 0}

            success_count = 0
            failed_count = 0

            # 逐个处理用户（用户档案分析通常较慢，不适合高并发）
            for user in users:
                user_id = user['id']
                user_handle = user['user_id']

                logger.info(f"正在分析用户 @{user_handle} (ID: {user_id})")

                result = self.analyze_user_profile(user_id, days)

                if result['success']:
                    success_count += 1
                    logger.info(f"用户 @{user_handle} 档案分析成功")
                else:
                    failed_count += 1
                    logger.error(f"用户 @{user_handle} 档案分析失败: {result.get('error')}")

            result_summary = {
                'total': len(users),
                'success': success_count,
                'failed': failed_count
            }

            logger.info(f"用户档案分析任务完成: {result_summary}")
            return result_summary

        except Exception as e:
            logger.error(f"用户档案分析任务执行失败: {e}", exc_info=True)
            return {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}


def run_user_profiling(limit: int = 50, days: int = 30) -> Dict[str, Any]:
    """
    便捷函数：运行用户档案分析

    Args:
        limit: 最大处理用户数
        days: 分析天数范围

    Returns:
        处理结果
    """
    analyzer = UserProfileAnalyzer()
    return analyzer.run_user_profiling(limit, days)

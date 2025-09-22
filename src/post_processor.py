"""
Twitter 帖子后处理模块
基于即刻项目的后处理逻辑，针对X/Twitter平台进行适配
"""
import json
import logging
import re
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse
import time

from .database import DatabaseManager
from .llm_client import LLMClient
from .config import config

logger = logging.getLogger(__name__)


class TwitterPostProcessor:
    """Twitter 帖子后处理器"""

    def __init__(self, db_manager: DatabaseManager = None, llm_client: LLMClient = None):
        self.db_manager = db_manager or DatabaseManager()
        self.llm_client = llm_client or LLMClient()

        # 获取并发配置
        postprocessing_config = config.get_postprocessing_config()
        self.fast_llm_workers = postprocessing_config['fast_llm_workers']
        self.fast_vlm_workers = postprocessing_config['fast_vlm_workers']
        self.image_processing_workers = postprocessing_config['image_processing_workers']

        logger.info(f"后处理器初始化完成 - FastLLM:{self.fast_llm_workers}, FastVLM:{self.fast_vlm_workers}, 图片处理:{self.image_processing_workers}")

    def process_posts(self, hours_back: int = 36) -> Dict[str, int]:
        """处理指定时间范围内的帖子

        Args:
            hours_back: 回溯小时数

        Returns:
            处理结果统计
        """
        logger.info(f"开始后处理任务，回溯 {hours_back} 小时")

        # 获取需要处理的帖子
        posts_to_process = self._get_posts_to_process(hours_back)

        if not posts_to_process:
            logger.info("没有需要处理的帖子")
            return {'total': 0, 'success': 0, 'failed': 0}

        logger.info(f"找到 {len(posts_to_process)} 条帖子需要处理")

        # 按帖子类型分组处理
        text_posts, image_posts = self._categorize_posts(posts_to_process)

        # 并发处理
        results = {'total': len(posts_to_process), 'success': 0, 'failed': 0}

        if text_posts:
            logger.info(f"处理 {len(text_posts)} 条纯文本帖子")
            text_results = self._process_text_posts_concurrent(text_posts)
            results['success'] += text_results['success']
            results['failed'] += text_results['failed']

        if image_posts:
            logger.info(f"处理 {len(image_posts)} 条图文帖子")
            image_results = self._process_image_posts_concurrent(image_posts)
            results['success'] += image_results['success']
            results['failed'] += image_results['failed']

        logger.info(f"后处理完成: 总计={results['total']}, 成功={results['success']}, 失败={results['failed']}")
        return results

    def _get_posts_to_process(self, hours_back: int) -> List[Dict]:
        """获取需要处理的帖子"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        query = """
        SELECT tp.id, tp.post_content, tp.media_urls, tp.published_at, tu.user_id
        FROM twitter_posts tp
        LEFT JOIN twitter_users tu ON tp.user_table_id = tu.id
        LEFT JOIN postprocessing pp ON tp.id = pp.post_id
        WHERE tp.published_at >= %s
          AND pp.post_id IS NULL
          AND tp.post_content IS NOT NULL
        ORDER BY tp.published_at DESC
        """

        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (cutoff_time,))

                posts = []
                for row in cursor.fetchall():
                    post_id, content, media_urls_str, published_at, user_id = row

                    # 解析媒体URLs
                    media_urls = []
                    if media_urls_str:
                        try:
                            media_urls = json.loads(media_urls_str) if isinstance(media_urls_str, str) else media_urls_str
                        except (json.JSONDecodeError, TypeError):
                            logger.warning(f"无法解析帖子 {post_id} 的媒体URL: {media_urls_str}")

                    posts.append({
                        'id': post_id,
                        'content': content,
                        'media_urls': media_urls or [],
                        'published_at': published_at,
                        'user_id': user_id
                    })

                return posts

        except Exception as e:
            logger.error(f"获取待处理帖子失败: {e}")
            return []

    def _categorize_posts(self, posts: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """将帖子按类型分类：纯文本和包含图片"""
        text_posts = []
        image_posts = []

        for post in posts:
            # 检查是否有图片（只处理 png/jpg/jpeg 格式）
            has_images = False
            if post['media_urls']:
                for url in post['media_urls']:
                    if self._is_supported_image_format(url):
                        has_images = True
                        break

            if has_images:
                image_posts.append(post)
            else:
                text_posts.append(post)

        return text_posts, image_posts

    def _is_supported_image_format(self, url: str) -> bool:
        """检查URL是否为支持的图片格式"""
        if not url:
            return False

        url_lower = url.lower()
        supported_formats = ['.png', '.jpg', '.jpeg']

        return any(url_lower.endswith(fmt) for fmt in supported_formats)

    def _process_text_posts_concurrent(self, posts: List[Dict]) -> Dict[str, int]:
        """并发处理纯文本帖子"""
        results = {'success': 0, 'failed': 0}

        with ThreadPoolExecutor(max_workers=self.fast_llm_workers) as executor:
            future_to_post = {
                executor.submit(self._process_text_post, post): post
                for post in posts
            }

            for future in as_completed(future_to_post):
                post = future_to_post[future]
                try:
                    success = future.result()
                    if success:
                        results['success'] += 1
                        logger.debug(f"文本帖子 {post['id']} 处理成功")
                    else:
                        results['failed'] += 1
                        logger.warning(f"文本帖子 {post['id']} 处理失败")
                except Exception as e:
                    results['failed'] += 1
                    logger.error(f"文本帖子 {post['id']} 处理异常: {e}")

        return results

    def _process_image_posts_concurrent(self, posts: List[Dict]) -> Dict[str, int]:
        """并发处理图文帖子"""
        results = {'success': 0, 'failed': 0}

        with ThreadPoolExecutor(max_workers=self.fast_vlm_workers) as executor:
            future_to_post = {
                executor.submit(self._process_image_post, post): post
                for post in posts
            }

            for future in as_completed(future_to_post):
                post = future_to_post[future]
                try:
                    success = future.result()
                    if success:
                        results['success'] += 1
                        logger.debug(f"图文帖子 {post['id']} 处理成功")
                    else:
                        results['failed'] += 1
                        logger.warning(f"图文帖子 {post['id']} 处理失败")
                except Exception as e:
                    results['failed'] += 1
                    logger.error(f"图文帖子 {post['id']} 处理异常: {e}")

        return results

    def _process_text_post(self, post: Dict) -> bool:
        """处理单个纯文本帖子"""
        try:
            content = post['content']
            if not content or len(content.strip()) < 10:
                logger.debug(f"帖子 {post['id']} 内容太短，跳过处理")
                return False

            # 调用LLM分析
            prompt = self._get_llm_prompt(content)
            result = self.llm_client.call_fast_model(prompt)

            if not result.get('success', False):
                logger.warning(f"帖子 {post['id']} LLM调用失败: {result.get('error', '未知错误')}")
                return False

            interpretation = result.get('content', '')
            if not interpretation:
                logger.warning(f"帖子 {post['id']} LLM生成解读为空")
                return False

            # 获取使用的模型名称
            llm_config = config.get_llm_config()
            model_name = llm_config['fast_model_name']

            # 保存到数据库
            return self._save_postprocessing_result(
                post_id=post['id'],
                interpretation=interpretation,
                model_name=model_name,
                status='success'
            )

        except Exception as e:
            logger.error(f"处理文本帖子 {post['id']} 失败: {e}")
            return False

    def _process_image_post(self, post: Dict) -> bool:
        """处理单个图文帖子"""
        try:
            content = post['content'] or ""
            media_urls = post['media_urls']

            # 筛选支持的图片URLs
            image_urls = [url for url in media_urls if self._is_supported_image_format(url)]

            if not image_urls:
                logger.debug(f"帖子 {post['id']} 没有支持的图片格式，降级为文本处理")
                return self._process_text_post(post)

            # 限制图片数量（避免上下文过长）
            if len(image_urls) > 4:
                image_urls = image_urls[:4]
                logger.debug(f"帖子 {post['id']} 图片数量过多，只处理前4张")

            # 调用VLM分析
            prompt = self._get_vlm_prompt(content)
            llm_config = config.get_llm_config()

            # 首先尝试主要VLM模型
            interpretation = None
            model_name = llm_config['fast_vlm_model_name']

            try:
                # 准备图片数据
                image_data_list = []
                for url in image_urls:
                    image_data_list.append({
                        'type': 'url',
                        'data': url,
                        'url': url,
                        'success': True
                    })

                # 调用VLM - 检查是否有call_vlm方法
                if hasattr(self.llm_client, 'call_vlm'):
                    result = self.llm_client.call_vlm(prompt, image_data_list, model_name=model_name)
                    if result.get('success', False):
                        interpretation = result.get('content', '')
                else:
                    # 如果没有VLM支持，降级为文本处理
                    logger.warning(f"LLM客户端不支持VLM，帖子 {post['id']} 降级为文本处理")
                    return self._process_text_post(post)

            except Exception as e:
                logger.warning(f"主要VLM模型 {model_name} 处理失败: {e}，尝试备用模型")

                # 尝试备用模型
                fallback_model = llm_config['fast_vlm_fallback_model_name']
                try:
                    if hasattr(self.llm_client, 'call_vlm'):
                        result = self.llm_client.call_vlm(prompt, image_data_list, model_name=fallback_model)
                        if result.get('success', False):
                            interpretation = result.get('content', '')
                            model_name = fallback_model
                            logger.info(f"使用备用VLM模型 {fallback_model} 处理成功")
                except Exception as e2:
                    logger.error(f"备用VLM模型 {fallback_model} 也失败: {e2}，降级为文本处理")
                    return self._process_text_post(post)

            if not interpretation:
                logger.warning(f"帖子 {post['id']} VLM生成解读失败，降级为文本处理")
                return self._process_text_post(post)

            # 保存到数据库
            return self._save_postprocessing_result(
                post_id=post['id'],
                interpretation=interpretation,
                model_name=model_name,
                status='success'
            )

        except Exception as e:
            logger.error(f"处理图文帖子 {post['id']} 失败: {e}")
            return False

    def _get_llm_prompt(self, post_text: str) -> str:
        """生成LLM分析提示词（纯文本）"""
        return f"""# Role: 社交媒体内容分析师

# Context:
你正在分析一条来自X/Twitter的纯文本推文。你的任务是深度挖掘文本背后的信息、情绪和潜在意图。

# Input:
- 推文内容: "{post_text}"

# Your Task:
请分析给定的文本，完成以下任务，并按顺序输出：
1. **原始推文**: 原始推文内容及简单概括
2. **核心观点与主题**: 以列表形式，提炼出推文的核心观点、讨论的主题或关键信息
3. **情绪与语气**: 分析作者在字里行间流露出的情绪（如喜悦、深思、批判等）和整体语气
4. **深入解读**: 结合以上分析，做一个深度解读。推断作者发表这条推文的可能动机，以及他/她希望引发读者怎样的思考或共鸣。

严格遵循上述输出要求，用中文输出你的完整分析结果。"""

    def _get_vlm_prompt(self, post_text: str) -> str:
        """生成VLM分析提示词（图文）"""
        return f"""# Role: 社交媒体内容分析师

# Context:
你正在分析一条来自X/Twitter的推文。这条推文包含文本和一张或多张图片。你的任务是深度融合文本和图片信息，提取信息与价值。

# Input:
- 推文文本: "{post_text}"
- 图片: 参考附件

# Your Task:
请结合给定的文本和所有图片，完成以下分析，并按顺序输出：
1. **原始推文**: 原始推文内容及简单说明
2. **图片信息**: 每张图片分别展示了什么内容？有什么意义？它们如何与文本内容关联？
3. **深入解读**: 结合推文内容和图片信息，做1个深度解读，分析作者的情绪、观点以及他/她真正想传达的核心思想。

严格遵循上述输出要求，用中文输出你的完整分析结果。"""

    def _save_postprocessing_result(self, post_id: int, interpretation: str,
                                   model_name: str, status: str = 'success') -> bool:
        """保存后处理结果到数据库"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                insert_query = """
                INSERT INTO postprocessing
                (post_id, interpretation_text, model_name, status, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                interpretation_text = VALUES(interpretation_text),
                model_name = VALUES(model_name),
                status = VALUES(status),
                created_at = VALUES(created_at)
                """

                cursor.execute(insert_query, (
                    post_id,
                    interpretation,
                    model_name,
                    status,
                    datetime.now()
                ))

                conn.commit()
                logger.debug(f"帖子 {post_id} 后处理结果已保存")
                return True

        except Exception as e:
            logger.error(f"保存帖子 {post_id} 后处理结果失败: {e}")
            return False

    def get_processing_stats(self, hours_back: int = 24) -> Dict[str, Any]:
        """获取处理统计信息"""
        cutoff_time = datetime.now() - timedelta(hours=hours_back)

        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()

                # 总体统计
                stats_query = """
                SELECT
                    COUNT(*) as total_posts,
                    COUNT(pp.post_id) as processed_posts,
                    SUM(CASE WHEN pp.status = 'success' THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN pp.status = 'failed' THEN 1 ELSE 0 END) as failed_count
                FROM twitter_posts tp
                LEFT JOIN postprocessing pp ON tp.id = pp.post_id
                WHERE tp.published_at >= %s
                """

                cursor.execute(stats_query, (cutoff_time,))
                total_posts, processed_posts, success_count, failed_count = cursor.fetchone()

                # 按模型统计
                model_stats_query = """
                SELECT model_name, COUNT(*) as count
                FROM postprocessing pp
                JOIN twitter_posts tp ON pp.post_id = tp.id
                WHERE tp.published_at >= %s AND pp.status = 'success'
                GROUP BY model_name
                ORDER BY count DESC
                """

                cursor.execute(model_stats_query, (cutoff_time,))
                model_stats = {row[0]: row[1] for row in cursor.fetchall()}

                return {
                    'time_range_hours': hours_back,
                    'total_posts': total_posts or 0,
                    'processed_posts': processed_posts or 0,
                    'unprocessed_posts': (total_posts or 0) - (processed_posts or 0),
                    'success_count': success_count or 0,
                    'failed_count': failed_count or 0,
                    'success_rate': round((success_count or 0) / max(processed_posts or 1, 1) * 100, 2),
                    'model_stats': model_stats
                }

        except Exception as e:
            logger.error(f"获取处理统计信息失败: {e}")
            return {
                'time_range_hours': hours_back,
                'total_posts': 0,
                'processed_posts': 0,
                'unprocessed_posts': 0,
                'success_count': 0,
                'failed_count': 0,
                'success_rate': 0.0,
                'model_stats': {}
            }
"""
帖子富化分析器 (Post Enrichment)
使用Fast LLM对每条帖子进行结构化信息提取和分析
"""
import logging
import json
import concurrent.futures
from typing import List, Dict, Any, Optional
from datetime import datetime

from .database import DatabaseManager
from .llm_client import get_llm_client
from .config import config

logger = logging.getLogger(__name__)


class PostEnrichmentAnalyzer:
    """帖子富化分析器"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """初始化分析器"""
        if db_manager is not None:
            self.db_manager = db_manager
        else:
            self.db_manager = DatabaseManager(config)

        self.llm_client = get_llm_client()
        if not self.llm_client:
            raise RuntimeError("LLM客户端初始化失败，无法进行帖子富化分析")

        logger.info("帖子富化分析器初始化完成")

    def get_enrichment_prompt(self, post_content: str) -> str:
        """
        构建帖子富化分析的提示词

        Args:
            post_content: 帖子内容

        Returns:
            完整的提示词
        """
        return f"""# Role: 高效信息提取与结构化助理

# Context:
你正在处理一条来自X/Twitter的帖子。你的任务是深入理解其内容，并以结构化的JSON格式返回你的分析。请优先保证JSON格式的严格正确性。

# Input Post:
'''
{post_content}
'''

# Your Task:
请严格按照以下JSON格式返回你的分析结果。不要添加任何解释性文字。

{{
  "llm_summary": "用一句话（不超过40个字）精准概括这篇帖子的核心内容。",
  "post_tag": "为帖子内容打上一个最合适的标签。候选标签：'技术讨论', '产品发布', '行业观察', '投资分析', '创业心路', '工具推荐', '资源分享', '生活感悟', '时事评论'。",
  "value_assets": [
    {{
      "asset_url": "提取帖子中分享的第一个有效URL",
      "asset_type": "判断该URL的类型。候选类型：'GitHub Repo', 'Research Paper', 'Blog Post', 'News Article', 'Product Website', 'Documentation'"
    }}
  ],
  "mentioned_entities": [
    {{
      "entity_name": "提取帖子中提及的专有名词，如项目名、人名、公司名",
      "entity_type": "判断该实体的类型。候选类型：'Project', 'Person', 'Company', 'Technology', 'Event'"
    }}
  ],
  "content_type": "从更深层次判断这篇帖子的内容形式。候选形式：'教程/指南', '观点/评论', '读书/学习笔记', '项目更新', '提问/求助', '新闻/快讯', '推广/广告'。",
  "is_incomplete": "判断此帖子是否像一个未完待续的系列（例如，结尾有'未完待续'、'1/N'、'明天继续'等字样，或内容在逻辑上明显未结束）。返回布尔值 true 或 false。"
}}"""

    def analyze_single_post(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析单个帖子

        Args:
            post: 帖子数据

        Returns:
            分析结果
        """
        post_id = post['id']
        post_content = post['post_content']

        logger.info(f"开始分析帖子 {post_id}")

        try:
            # 构建提示词
            prompt = self.get_enrichment_prompt(post_content)

            # 调用Fast LLM
            response = self.llm_client.call_fast_model(prompt, temperature=0.1)

            if not response['success']:
                logger.error(f"LLM调用失败，帖子 {post_id}: {response.get('error')}")
                return {
                    'post_id': post_id,
                    'success': False,
                    'error': response.get('error'),
                    'status': 'failed'
                }

            # 提取JSON结果
            json_result = self.llm_client.extract_json_from_response(response['content'])

            if not json_result:
                logger.error(f"无法从LLM响应中提取JSON，帖子 {post_id}")
                return {
                    'post_id': post_id,
                    'success': False,
                    'error': 'JSON提取失败',
                    'status': 'failed'
                }

            # 验证JSON结构
            required_fields = ['llm_summary', 'post_tag', 'value_assets', 'mentioned_entities', 'content_type', 'is_incomplete']
            if not all(field in json_result for field in required_fields):
                logger.warning(f"LLM返回的JSON缺少必需字段，帖子 {post_id}")
                # 使用默认值填充缺失字段
                for field in required_fields:
                    if field not in json_result:
                        if field == 'value_assets' or field == 'mentioned_entities':
                            json_result[field] = []
                        elif field == 'is_incomplete':
                            json_result[field] = False
                        else:
                            json_result[field] = '未知'

            logger.info(f"帖子 {post_id} 分析完成")
            return {
                'post_id': post_id,
                'success': True,
                'analysis_result': json_result,
                'status': 'completed'
            }

        except Exception as e:
            logger.error(f"分析帖子 {post_id} 时发生异常: {e}", exc_info=True)
            return {
                'post_id': post_id,
                'success': False,
                'error': str(e),
                'status': 'failed'
            }

    def process_posts_batch(self, posts: List[Dict[str, Any]], max_workers: int = 3) -> Dict[str, Any]:
        """
        批量处理帖子的富化分析

        Args:
            posts: 帖子列表
            max_workers: 最大并发数

        Returns:
            处理结果统计
        """
        if not posts:
            logger.info("没有帖子需要处理")
            return {'total': 0, 'success': 0, 'failed': 0}

        logger.info(f"开始批量分析 {len(posts)} 个帖子，并发数: {max_workers}")

        # 为所有帖子预创建pending状态的分析记录
        post_ids = [post['id'] for post in posts]
        self.db_manager.create_pending_post_analysis(post_ids)

        success_count = 0
        failed_count = 0

        # 使用线程池并发处理
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_post = {executor.submit(self.analyze_single_post, post): post for post in posts}

            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_post):
                result = future.result()
                post_id = result['post_id']

                if result['success']:
                    # 更新数据库
                    if self.db_manager.update_post_analysis(
                        post_id,
                        result['analysis_result'],
                        result['status']
                    ):
                        success_count += 1
                        logger.info(f"帖子 {post_id} 分析结果已保存")
                    else:
                        logger.error(f"帖子 {post_id} 分析结果保存失败")
                        failed_count += 1
                else:
                    # 标记为失败状态
                    self.db_manager.update_post_analysis(
                        post_id,
                        {},
                        'failed'
                    )
                    failed_count += 1
                    logger.error(f"帖子 {post_id} 分析失败: {result.get('error')}")

        logger.info(f"批量分析完成: 总计 {len(posts)}, 成功 {success_count}, 失败 {failed_count}")

        return {
            'total': len(posts),
            'success': success_count,
            'failed': failed_count
        }

    def run_enrichment_analysis(self, batch_size: int = 100, max_workers: int = 3) -> Dict[str, Any]:
        """
        运行帖子富化分析任务

        Args:
            batch_size: 批处理大小
            max_workers: 最大并发数

        Returns:
            处理结果统计
        """
        logger.info("开始运行帖子富化分析任务")

        try:
            # 获取待分析的帖子
            posts = self.db_manager.get_posts_for_enrichment(batch_size)

            if not posts:
                logger.info("没有需要富化分析的帖子")
                return {'total': 0, 'success': 0, 'failed': 0}

            # 批量处理
            result = self.process_posts_batch(posts, max_workers)

            logger.info(f"帖子富化分析任务完成: {result}")
            return result

        except Exception as e:
            logger.error(f"帖子富化分析任务执行失败: {e}", exc_info=True)
            return {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}


def run_post_enrichment(batch_size: int = 100, max_workers: int = 3) -> Dict[str, Any]:
    """
    便捷函数：运行帖子富化分析

    Args:
        batch_size: 批处理大小
        max_workers: 最大并发数

    Returns:
        处理结果
    """
    analyzer = PostEnrichmentAnalyzer()
    return analyzer.run_enrichment_analysis(batch_size, max_workers)
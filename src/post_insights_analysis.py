
"""
帖子洞察分析器 (Post Insights Analyzer)
实现统一的增强解读流程，合并原有的 post_enrichment 和 post_processing 功能。
"""
import logging
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

from .database import DatabaseManager
from .llm_client import LLMClient
from .config import config

logger = logging.getLogger(__name__)


class PostInsightsAnalyzer:
    """帖子洞察分析器，执行增强解读"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None, llm_client: Optional[LLMClient] = None):
        self.db_manager = db_manager or DatabaseManager()
        self.llm_client = llm_client or LLMClient()
        if not self.llm_client:
            raise RuntimeError("LLM客户端初始化失败")

        # 获取并发配置
        postprocessing_config = config.get_postprocessing_config()
        self.fast_llm_workers = postprocessing_config['fast_llm_workers']
        self.fast_vlm_workers = postprocessing_config['fast_vlm_workers']

        logger.info("帖子洞察分析器初始化完成")

    def _robust_json_parser(self, raw_content: str) -> Optional[Dict[str, Any]]:
        """健壮的JSON解析器，用于处理LLM可能返回的不规范格式"""
        try:
            # 1. 尝试直接解析
            return json.loads(raw_content)
        except json.JSONDecodeError:
            # 2. 如果失败，使用正则表达式提取被包裹的JSON
            logger.warning("直接解析JSON失败，尝试使用正则提取...")
            json_match = re.search(r'\{.*\}', raw_content, re.DOTALL)
            if json_match:
                json_string = json_match.group(0)
                try:
                    return json.loads(json_string)
                except json.JSONDecodeError as e:
                    logger.error(f"正则提取后解析JSON仍然失败: {e}")
                    return None
            else:
                logger.error("无法从LLM响应中找到任何JSON对象")
                return None

    def get_unified_text_prompt(self, post_text: str) -> str:
        """获取纯文本帖子的统一增强解读Prompt"""
        return f"""# Role: 推特帖子信息提取与深度分析助理

# Context:
你正在分析一条来自X/Twitter的帖子。你的任务是深入理解其内容，并以一个包含结构化信息和深度解读的JSON格式返回你的分析。

# Input Post:
'''
{post_text}
'''

# Your Task:
请严格按照以下JSON格式返回你的分析结果。不要添加任何解释性文字。

{{
  "llm_summary": "用50字左右精准概括这篇帖子的核心内容。",
  "post_tag": "为帖子内容打上一个最合适的标签。候选标签：'技术讨论', '产品发布', '行业观察', '投资分析', '创业心路', '工具推荐', '资源分享', '生活感悟', '时事评论'。",
  "mentioned_entities": [
    {{
      "entity_name": "提取帖子中提及的专有名词，如项目名、人名、公司名",
      "entity_type": "判断该实体的类型。候选类型：'Project', 'Person', 'Company', 'Technology', 'Event'"
    }}
  ],
  "content_type": "从更深层次判断这篇帖子的内容形式。候选形式：'教程/指南', '观点/评论', '读书/学习笔记', '项目更新', '提问/求助', '新闻/快讯'。",
  "deep_interpretation": "（**此项为重点**）结合以上分析，生成一个“文本深度洞察摘要”，该摘要将作为后续宏观分析的输入。摘要需自然融合3个层面（若能识别到）：1.**核心信息**: 推文讨论的主要观点和关键信息是什么？2.**情绪意图**: 作者展现了怎样的情绪与语气？他/她发文的潜在动机是什么？3.**思想价值**: 这条推文试图引发读者怎样的思考或共鸣？350字左右"
}}
"""

    def get_unified_vlm_prompt(self, post_text: str) -> str:
        """获取图文帖子的统一增强解读Prompt"""
        return f"""# Role: 推特帖子信息提取与深度分析助理

# Context:
你正在分析一条来自X/Twitter的图文帖子。你的任务是深度融合文本和图片信息，并以一个包含结构化信息和深度解读的JSON格式返回你的分析。

# Input:
- Post 文本: """{post_text}"""
- 图片: 参考附件

# Your Task:
请严格按照以下JSON格式返回你的分析结果。不要添加任何解释性文字。

{{
  "llm_summary": "用50字左右精准概括这篇帖子的核心内容。",
  "post_tag": "为帖子内容打上一个最合适的标签。候选标签：'技术讨论', '产品发布', '行业观察', '投资分析', '创业心路', '工具推荐', '资源分享', '生活感悟', '时事评论'。",
  "image_description": "详细描述图片内容，以及图片与文本是如何关联的。250字左右",
  "mentioned_entities": [
    {{
      "entity_name": "提取帖子中提及的专有名词，如项目名、人名、公司名",
      "entity_type": "判断该实体的类型。候选类型：'Project', 'Person', 'Company', 'Technology', 'Event'"
    }}
  ],
  "content_type": "从更深层次判断这篇帖子的内容形式。候选形式：'教程/指南', '观点/评论', '读书/学习笔记', '项目更新', '提问/求助', '新闻/快讯'。",
  "deep_interpretation": "（**此项为重点**）结合文本和图片信息，生成1个**图文综合摘要**，作为对这条帖子的完整解读，也作为后续宏观分析任务的输入支撑。350字左右"
}}
"""

    def _extract_image_urls(self, post: Dict) -> List[str]:
        """从帖子数据中提取有效的图片URL"""
        image_urls = []
        media_urls_str = post.get('media_urls')
        if media_urls_str:
            try:
                media_urls = json.loads(media_urls_str)
                if isinstance(media_urls, list):
                    for url in media_urls:
                        if isinstance(url, str) and "twimg" in url and "video" not in url:
                            image_urls.append(url)
            except (json.JSONDecodeError, TypeError):
                pass
        return list(set(image_urls)) # 去重

    def _analyze_single_post(self, post: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """分析单个帖子，返回帖子ID和分析结果字典"""
        post_id = post['id']
        post_content = post.get('post_content', '')
        image_urls = self._extract_image_urls(post)

        try:
            if image_urls:
                # --- VLM (图文) 处理 ---
                prompt = self.get_unified_vlm_prompt(post_content)
                image_data_list = [{'type': 'url', 'data': url, 'url': url, 'success': True} for url in image_urls]
                response = self.llm_client.call_vlm(prompt, image_data_list)
                model_name = self.llm_client.vlm_model
            else:
                # --- LLM (纯文本) 处理 ---
                prompt = self.get_unified_text_prompt(post_content)
                response = self.llm_client.call_fast_model(prompt)
                model_name = self.llm_client.fast_model

            if not response or not response.get('success'):
                raise ValueError(f"LLM API调用失败: {response.get('error')}")

            analysis_result = self._robust_json_parser(response['content'])
            if not analysis_result:
                raise ValueError("无法从LLM响应中提取有效的JSON")
            
            analysis_result['model_name'] = model_name
            return post_id, analysis_result

        except Exception as e:
            logger.error(f"分析帖子 {post_id} 时发生异常: {e}")
            return post_id, {'error': str(e)}

    def run_analysis(self, batch_size: int = 100) -> Dict[str, Any]:
        """运行帖子洞察分析任务"""
        logger.info(f"开始运行帖子洞察分析任务，批次大小: {batch_size}")
        
        try:
            posts = self.db_manager.get_posts_for_insight_analysis(limit=batch_size)
            if not posts:
                logger.info("没有需要进行洞察分析的帖子")
                return {'total': 0, 'success': 0, 'failed': 0}

            success_count = 0
            failed_count = 0

            with ThreadPoolExecutor(max_workers=self.fast_llm_workers) as executor:
                future_to_post_id = {executor.submit(self._analyze_single_post, post): post['id'] for post in posts}

                for future in as_completed(future_to_post_id):
                    post_id = future_to_post_id[future]
                    try:
                        _, result_data = future.result()
                        if 'error' in result_data:
                            self.db_manager.save_post_insight(post_id, {'deep_interpretation': result_data['error']}, status='failed')
                            failed_count += 1
                        else:
                            self.db_manager.save_post_insight(post_id, result_data, status='completed')
                            success_count += 1
                    except Exception as e:
                        logger.error(f"保存帖子 {post_id} 的分析结果时失败: {e}")
                        self.db_manager.save_post_insight(post_id, {'deep_interpretation': str(e)}, status='failed')
                        failed_count += 1

            logger.info(f"洞察分析任务完成: 总计 {len(posts)}, 成功 {success_count}, 失败 {failed_count}")
            return {'total': len(posts), 'success': success_count, 'failed': failed_count}

        except Exception as e:
            logger.error(f"帖子洞察分析任务执行失败: {e}", exc_info=True)
            return {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}

def run_post_insights_analysis_task(batch_size: int = 100) -> Dict[str, Any]:
    """便捷函数：运行帖子洞察分析"""
    try:
        analyzer = PostInsightsAnalyzer()
        return analyzer.run_analysis(batch_size=batch_size)
    except Exception as e:
        logger.error(f"创建PostInsightsAnalyzer失败: {e}")
        return {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}

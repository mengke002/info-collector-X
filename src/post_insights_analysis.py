
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

    def _calculate_content_complexity(self, post_text: str, image_count: int) -> str:
        """
        根据内容复杂度计算合适的洞察长度指引

        Args:
            post_text: 帖子文本内容
            image_count: 图片数量

        Returns:
            洞察长度指引字符串
        """
        text_length = len(post_text)

        # 纯文本帖子的长度判断
        if image_count == 0:
            if text_length < 100:
                return "100字左右"  # 短文本，简短洞察
            elif text_length < 300:
                return "150字左右"  # 中等文本
            else:
                return "250字左右"  # 长文本，更详细的洞察

        # 图文帖子的长度判断（需要更详细）
        else:
            if image_count == 1 and text_length < 150:
                return "150字左右"  # 单图简短文本
            elif image_count == 1 and text_length < 400:
                return "200字左右"  # 单图中等文本
            elif image_count > 2 or text_length >= 400:
                return "300字左右"  # 多图或长文本，需要详细解析
            else:
                return "250字左右"  # 默认情况

    def _robust_json_parser(self, raw_content: str) -> Optional[Dict[str, Any]]:
        """健壮的JSON解析器，用于处理LLM可能返回的不规范格式"""
        try:
            # 第一步：尝试直接解析
            return json.loads(raw_content)
        except json.JSONDecodeError:
            # 第二步：如果失败，使用正则提取并清理
            logger.warning("直接解析JSON失败，尝试使用正则提取并清理...")
            json_match = re.search(r'\{.*\}', raw_content, re.DOTALL)
            if not json_match:
                logger.error("无法从LLM响应中找到任何JSON对象")
                return None

            json_string = json_match.group(0)
            cleaned_string = re.sub(r",\s*([\}\]])", r"\1", json_string)

            try:
                # 第三步：尝试解析清理后的字符串
                return json.loads(cleaned_string)
            except json.JSONDecodeError as e:
                logger.error(f"最终解析JSON失败: {e}")
                return None

    def get_unified_text_prompt(self, post_text: str, interpretation_length: str = "150字左右") -> str:
        """
        获取纯文本帖子的统一增强解读Prompt

        Args:
            post_text: 帖子文本内容
            interpretation_length: 深度洞察的目标长度指引
        """
        return f"""# Role: 推特帖子信息提取与深度分析助理

# Context:
你正在分析一条来自X/Twitter的帖子。你的任务是深入理解其内容，并以一个包含结构化信息和深度解读的JSON格式返回你的分析。

# Input Post:
```
{post_text}
```

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
  "deep_interpretation": "（**此项为重点**）深入理解原帖，生成一个"文本深度洞察摘要"，该摘要将作为后续宏观分析的输入。摘要需以一段自然的文字，讲解帖子内容与你的理解，{interpretation_length}"
}}
"""

    def get_unified_vlm_prompt(self, post_text: str, image_count: int = 1, interpretation_length: str = "300字左右") -> str:
        """
        获取图文帖子的统一增强解读Prompt

        Args:
            post_text: 帖子文本内容
            image_count: 图片数量
            interpretation_length: 深度洞察的目标长度指引
        """
        # 根据图片数量调整图片描述的长度要求
        if image_count == 1:
            image_desc_length = "150字左右"
        elif image_count == 2:
            image_desc_length = "250字左右"
        else:
            image_desc_length = "300字左右"

        return f"""# Role: 推特帖子信息提取与深度分析助理

# Context:
你正在分析一条来自X/Twitter的图文帖子。你的任务是深度融合文本和图片信息，并以一个包含结构化信息和深度解读的JSON格式返回你的分析。

# Input:
- Post 文本: ```{post_text}```
- 图片: 参考附件（共{image_count}张）

# Your Task:
请严格按照以下JSON格式返回你的分析结果。不要添加任何解释性文字。

{{
  "llm_summary": "用50字左右精准概括这篇帖子的核心内容。",
  "post_tag": "为帖子内容打上一个最合适的标签。候选标签：'技术讨论', '产品发布', '行业观察', '投资分析', '创业心路', '工具推荐', '资源分享', '生活感悟', '时事评论'。",
  "image_description": "详细描述{'所有' if image_count > 1 else ''}图片内容，以及图片与文本是如何关联的。{image_desc_length}",
  "mentioned_entities": [
    {{
      "entity_name": "提取帖子中提及的专有名词，如项目名、人名、公司名",
      "entity_type": "判断该实体的类型。候选类型：'Project', 'Person', 'Company', 'Technology', 'Event'"
    }}
  ],
  "content_type": "从更深层次判断这篇帖子的内容形式。候选形式：'教程/指南', '观点/评论', '读书/学习笔记', '项目更新', '提问/求助', '新闻/快讯'。",
  "deep_interpretation": "（**此项为重点**）结合文本和{'所有' if image_count > 1 else ''}图片信息，生成1个**图文综合摘要**，作为对这条帖子的完整解读，也作为后续宏观分析任务的输入支撑。{interpretation_length}"
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

        # 计算合适的洞察长度
        interpretation_length = self._calculate_content_complexity(post_content, len(image_urls))

        try:
            if image_urls:
                # --- VLM (图文) 处理 ---
                prompt = self.get_unified_vlm_prompt(
                    post_content,
                    image_count=len(image_urls),
                    interpretation_length=interpretation_length
                )
                image_data_list = [{'type': 'url', 'data': url, 'url': url, 'success': True} for url in image_urls]
                response = self.llm_client.call_vlm(prompt, image_data_list)
                model_name = self.llm_client.vlm_model
            else:
                # --- LLM (纯文本) 处理 ---
                prompt = self.get_unified_text_prompt(post_content, interpretation_length=interpretation_length)
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

    def run_analysis(self, hours_back: int, batch_size: int = 1000) -> Dict[str, Any]:
        """运行帖子洞察分析任务"""
        logger.info(f"开始运行帖子洞察分析任务，回溯 {hours_back} 小时，批次大小: {batch_size}")
        
        try:
            posts = self.db_manager.get_posts_for_insight_analysis(hours_back=hours_back, limit=batch_size)
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

def run_post_insights_analysis_task(hours_back: int, batch_size: int = 1000) -> Dict[str, Any]:
    """便捷函数：运行帖子洞察分析"""
    try:
        analyzer = PostInsightsAnalyzer()
        return analyzer.run_analysis(hours_back=hours_back, batch_size=batch_size)
    except Exception as e:
        logger.error(f"创建PostInsightsAnalyzer失败: {e}")
        return {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}


"""
帖子洞察分析器 (Post Insights Analyzer)
实现统一的增强解读流程，合并原有的 post_enrichment 和 post_processing 功能。
"""
import logging
import json
import re
import base64
import os
import tempfile
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional, Tuple

from .database import DatabaseManager
from .llm_client import LLMClient
from .config import config

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("PIL/Pillow未安装，无法进行图片处理。请安装: pip install pillow")

logger = logging.getLogger(__name__)


def download_and_resize_image(url: str, max_dimension: int = 1024, timeout: int = 10) -> Optional[str]:
    """
    下载图片并调整尺寸，返回base64编码

    Args:
        url: 图片URL
        max_dimension: 最大边长，默认1024像素
        timeout: 下载超时时间

    Returns:
        base64编码的图片数据，失败时返回None
    """
    if not PIL_AVAILABLE:
        logger.error("PIL/Pillow未安装，无法处理图片")
        return None

    try:
        # 下载图片
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        # 检查内容大小，避免下载过大的文件
        content_length = response.headers.get('content-length')
        if content_length and int(content_length) > 50 * 1024 * 1024:  # 50MB限制
            logger.warning(f"图片文件过大 ({int(content_length)/(1024*1024):.1f}MB): {url}")
            return None

        # 创建临时文件
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name

            # 分块下载
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)

        try:
            # 使用PIL打开图片
            with Image.open(temp_path) as img:
                logger.debug(f"成功打开图片: {url}, 格式: {img.format}, 模式: {img.mode}, 尺寸: {img.size}")

                # 转换RGBA模式以支持透明度
                if img.mode in ('RGBA', 'LA'):
                    # 创建白色背景
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'RGBA':
                        background.paste(img, mask=img.split()[-1])  # 使用alpha通道作为mask
                    else:
                        background.paste(img, mask=img.split()[-1])
                    img = background
                elif img.mode not in ('RGB', 'L'):
                    img = img.convert('RGB')

                # 图片尺寸压缩：限制在指定尺寸以内
                width, height = img.size
                if width > max_dimension or height > max_dimension:
                    # 计算缩放比例，保持长宽比
                    scale_ratio = min(max_dimension / width, max_dimension / height)
                    new_width = int(width * scale_ratio)
                    new_height = int(height * scale_ratio)

                    # 使用高质量的重采样算法
                    img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                    logger.debug(f"图片尺寸压缩: {width}x{height} -> {new_width}x{new_height} (压缩比: {scale_ratio:.2f})")

                # 判断原始格式，决定输出格式
                # 如果原格式是JPG/JPEG/PNG，保持原格式；否则使用PNG
                url_lower = url.lower()
                if url_lower.endswith('.jpg') or url_lower.endswith('.jpeg'):
                    output_format = 'JPEG'
                    suffix = '.jpg'
                elif url_lower.endswith('.png'):
                    output_format = 'PNG'
                    suffix = '.png'
                else:
                    # 其他格式（webp, heic等）统一转为PNG
                    output_format = 'PNG'
                    suffix = '.png'

                # 保存为目标格式的临时文件
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as converted_file:
                    converted_path = converted_file.name
                    if output_format == 'JPEG':
                        img.save(converted_path, format=output_format, quality=85, optimize=True)
                    else:
                        img.save(converted_path, format=output_format, optimize=True)

            # 读取处理后的图片并编码为base64
            with open(converted_path, 'rb') as f:
                image_data = f.read()
                base64_image = base64.b64encode(image_data).decode('utf-8')

            # 清理临时文件
            os.unlink(temp_path)
            os.unlink(converted_path)

            logger.debug(f"图片处理成功: {url} -> {output_format} ({len(image_data)} bytes)")
            return base64_image

        except Exception as e:
            logger.warning(f"图片处理失败: {url}, 错误: {e}")
            return None

    except requests.exceptions.Timeout:
        logger.warning(f"图片下载超时: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"图片下载失败: {url}, 错误: {e}")
        return None
    except Exception as e:
        logger.error(f"图片处理异常: {url}, 错误: {e}")
        return None
    finally:
        # 确保清理临时文件
        try:
            if 'temp_path' in locals() and os.path.exists(temp_path):
                os.unlink(temp_path)
            if 'converted_path' in locals() and os.path.exists(converted_path):
                os.unlink(converted_path)
        except:
            pass


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

        # 获取VLM配置
        llm_config = config.get_llm_config()
        self.use_image_url = llm_config.get('use_image_url', False)

        # 图片预处理缓存：URL -> base64
        self.image_cache: Dict[str, Optional[str]] = {}

        # 图片处理线程池（用于预处理）
        self.image_processing_workers = postprocessing_config.get('image_processing_workers', 12)

        logger.info("帖子洞察分析器初始化完成")
        logger.info(f"使用图片URL模式: {self.use_image_url}")
        logger.info(f"图片处理并发数: {self.image_processing_workers}")

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

    def _preprocess_images(self, all_image_urls: List[str]) -> None:
        """
        预处理所有图片，使用多线程下载和resize

        Args:
            all_image_urls: 所有需要处理的图片URL列表
        """
        if not all_image_urls or self.use_image_url:
            # 如果使用URL模式，不需要预处理
            return

        # 去重
        unique_urls = list(set(all_image_urls))
        logger.info(f"开始预处理 {len(unique_urls)} 张唯一图片...")

        # 使用线程池并发下载和处理图片
        with ThreadPoolExecutor(max_workers=self.image_processing_workers, thread_name_prefix="ImagePreprocess") as executor:
            future_to_url = {
                executor.submit(download_and_resize_image, url): url
                for url in unique_urls
            }

            success_count = 0
            failed_count = 0

            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    base64_data = future.result()
                    self.image_cache[url] = base64_data
                    if base64_data:
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.error(f"预处理图片 {url} 时发生异常: {e}")
                    self.image_cache[url] = None
                    failed_count += 1

        logger.info(f"图片预处理完成: 成功 {success_count}, 失败 {failed_count}")

    def _prepare_image_data(self, image_urls: List[str]) -> List[Dict[str, Any]]:
        """
        准备图片数据，根据配置返回URL或base64格式

        Args:
            image_urls: 图片URL列表

        Returns:
            图片数据列表
        """
        image_data_list = []

        if self.use_image_url:
            # URL模式：直接使用URL
            for url in image_urls:
                image_data_list.append({
                    'type': 'url',
                    'data': url,
                    'url': url,
                    'success': True
                })
            logger.debug(f"准备了 {len(image_data_list)} 张图片 (URL模式)")
        else:
            # base64模式：从缓存获取
            for url in image_urls:
                base64_data = self.image_cache.get(url)
                if base64_data:
                    image_data_list.append({
                        'type': 'base64',
                        'data': base64_data,
                        'url': url,
                        'success': True
                    })
                else:
                    logger.warning(f"图片缓存中未找到或处理失败: {url}")

            logger.debug(f"准备了 {len(image_data_list)}/{len(image_urls)} 张图片 (base64模式)")

        return image_data_list

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

                # 准备图片数据（根据use_image_url配置决定URL或base64）
                image_data_list = self._prepare_image_data(image_urls)

                if not image_data_list:
                    # 没有有效图片数据，记录失败
                    error_msg = "没有有效图片数据（图片下载或处理失败）"
                    logger.error(f"帖子 {post_id} {error_msg}")
                    return post_id, {'error': error_msg}

                # 第一次尝试：使用主VLM模型（3次重试）
                response = self.llm_client.call_vlm(prompt, image_data_list)
                model_name = self.llm_client.vlm_model

                # 如果主VLM失败，尝试托底VLM模型
                if not response or not response.get('success'):
                    logger.warning(f"主VLM模型失败，尝试托底VLM模型处理帖子 {post_id}")
                    response = self.llm_client.call_vlm(
                        prompt,
                        image_data_list,
                        model_name=self.llm_client.vlm_fallback_model
                    )
                    model_name = self.llm_client.vlm_fallback_model

                # 如果托底VLM也失败，记录为失败
                if not response or not response.get('success'):
                    error_msg = f"主VLM和托底VLM都失败: {response.get('error') if response else 'No response'}"
                    logger.error(f"帖子 {post_id} {error_msg}")
                    return post_id, {'error': error_msg}

            else:
                # --- LLM (纯文本) 处理 ---
                prompt = self.get_unified_text_prompt(post_content, interpretation_length=interpretation_length)
                response = self.llm_client.call_fast_model(prompt)
                model_name = self.llm_client.fast_model

                if not response or not response.get('success'):
                    error_msg = f"LLM处理失败: {response.get('error') if response else 'No response'}"
                    logger.error(f"帖子 {post_id} {error_msg}")
                    return post_id, {'error': error_msg}

            # 解析结果
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

            logger.info(f"获取到 {len(posts)} 个需要分析的帖子")

            # 如果使用base64模式，先预处理所有图片
            if not self.use_image_url:
                # 收集所有图片URL
                all_image_urls = []
                for post in posts:
                    image_urls = self._extract_image_urls(post)
                    all_image_urls.extend(image_urls)

                # 预处理图片（多线程下载和resize）
                self._preprocess_images(all_image_urls)

            # 并发分析帖子
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

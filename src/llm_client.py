"""
LLM客户端模块
支持OpenAI compatible接口的streaming实现，包含VLM支持
参考即刻项目的实现方式
"""
import logging
import time
from typing import Dict, Any, List, Optional
import httpx
from openai import OpenAI

from .config import config

logger = logging.getLogger(__name__)


class LLMClient:
    """统一的LLM客户端，支持文本和视觉多模态模型"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        llm_config = config.get_llm_config()
        self.api_key = llm_config.get('openai_api_key')
        self.base_url = llm_config.get('openai_base_url', 'https://api.openai.com/v1')

        self.fast_model = llm_config.get('fast_model_name', 'gpt-3.5-turbo-16k')
        self.vlm_model = llm_config.get('fast_vlm_model_name', 'gpt-4-vision-preview')
        self.vlm_fallback_model = llm_config.get('fast_vlm_fallback_model_name', 'gpt-4-vision-preview')
        self.smart_model = llm_config.get('smart_model_name', 'gpt-4.1')
        self.report_models = llm_config.get('report_models', [])
        self.max_tokens = llm_config.get('max_tokens', 20000)
        self.timeout = float(llm_config.get('request_timeout', 300))
        self.retry_delay = int(llm_config.get('retry_delay', 5))

        if not self.api_key:
            raise ValueError("未找到OPENAI_API_KEY配置，请在环境变量或config.ini中设置")

        # 配置具备充足握手与读取时间的 httpx 客户端，防止反向代理在处理大 Context 首字延迟长时提前断开
        http_client = httpx.Client(
            timeout=httpx.Timeout(
                timeout=self.timeout,
                connect=30.0,
                read=self.timeout,
                write=60.0
            )
        )
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            http_client=http_client
        )

        self.logger.info(f"LLM客户端初始化成功")
        self.logger.info(f"Fast Model: {self.fast_model}")
        self.logger.info(f"VLM Model: {self.vlm_model}")
        self.logger.info(f"VLM Fallback Model: {self.vlm_fallback_model}")
        self.logger.info(f"Smart Model: {self.smart_model}")
        self.logger.info(f"Report Models: {self.report_models}")
        self.logger.info(f"Max Tokens: {self.max_tokens}")
        self.logger.info(f"Request Timeout: {self.timeout}s")

    def call_fast_model(self, prompt: str, temperature: float = 0.1, max_retries: int = 3) -> Dict[str, Any]:
        """
        调用快速模型进行信息提取
        适用于：结构化信息提取、分类等快速任务
        """
        return self._make_request(prompt, self.fast_model, temperature, max_retries)

    def call_smart_model(self, prompt: str, temperature: float = 0.5, max_retries: int = 3, model_override: Optional[str] = None, min_content_length: int = 1) -> Dict[str, Any]:
        if model_override:
            return self._make_request(prompt, model_override, temperature, max_retries, min_content_length=min_content_length)
        
        if not self.report_models:
            # Fallback to old smart_model_name if report_models is empty
            llm_config = config.get_llm_config()
            smart_model = llm_config.get('smart_model_name')
            if smart_model:
                return self._make_request(prompt, smart_model, temperature, max_retries, min_content_length=min_content_length)
            raise ValueError("未配置任何可用于生成报告的report_models或smart_model_name")

        last_response: Dict[str, Any] = {
            'success': False,
            'error': '所有报告模型均调用失败'
        }

        for index, model_name in enumerate(self.report_models):
            result = self._make_request(prompt, model_name, temperature, max_retries, min_content_length=min_content_length)
            if result.get('success'):
                return result

            last_response = result
            if index < len(self.report_models) - 1:
                fallback_target = self.report_models[index + 1]
                self.logger.warning(
                    f"模型 {model_name} 在 {max_retries} 次尝试后失败，将回退至 {fallback_target}"
                )
        return last_response

    def call_vlm(self, prompt: str, image_data_list: List[Dict[str, Any]],
                 model_name: Optional[str] = None, temperature: float = 0.3,
                 max_retries: int = 3) -> Dict[str, Any]:
        """
        调用视觉多模态模型进行图文分析，支持URL和base64混合模式

        Args:
            prompt: 文本提示词
            image_data_list: 图片数据列表，每个元素包含：
                - type: 'url' 或 'base64'
                - data: URL字符串 或 base64字符串
                - url: 原始URL（可选）
                - success: 是否成功（可选）
            model_name: 指定的模型名称，如果不提供则使用默认VLM模型
            temperature: 生成温度
            max_retries: 最大重试次数

        Returns:
            响应结果字典
        """
        if not image_data_list:
            return {
                'success': False,
                'error': '没有提供图片数据',
                'model': model_name or self.vlm_model
            }

        # 过滤出成功的图片数据
        valid_images = [img for img in image_data_list if img.get('success', False) and img.get('data')]

        if not valid_images:
            return {
                'success': False,
                'error': '没有有效的图片数据',
                'model': model_name or self.vlm_model
            }

        # 验证图片数量限制
        if len(valid_images) > 10:
            self.logger.warning(f"图片数量过多({len(valid_images)})，截取前10张")
            valid_images = valid_images[:10]

        used_model = model_name or self.vlm_model

        for attempt in range(max_retries):
            try:
                self.logger.info(f"调用VLM模型: {used_model} (尝试 {attempt + 1}/{max_retries})")
                self.logger.info(f"图片数量: {len(valid_images)}")
                self.logger.info(f"提示词长度: {len(prompt)} 字符")

                # 构建消息内容
                content = [{"type": "text", "text": prompt}]

                # 添加图片（支持URL和base64混合模式）
                for i, img_data in enumerate(valid_images):
                    img_type = img_data.get('type', 'url')
                    img_data_value = img_data.get('data')

                    if img_type == 'url':
                        # URL模式
                        content.append({
                            "type": "image_url",
                            "image_url": {"url": img_data_value}
                        })
                        self.logger.debug(f"添加图片 {i+1} (URL): {img_data_value[:50]}...")
                    elif img_type == 'base64':
                        # base64模式
                        # 检测是否已经包含data URI前缀
                        if img_data_value.startswith('data:'):
                            base64_url = img_data_value
                        else:
                            # 默认使用PNG格式
                            base64_url = f"data:image/png;base64,{img_data_value}"

                        content.append({
                            "type": "image_url",
                            "image_url": {"url": base64_url}
                        })
                        original_url = img_data.get('url', 'unknown')
                        self.logger.debug(f"添加图片 {i+1} (base64): {original_url[:50]}... (base64长度: {len(img_data_value)})")

                # 创建请求
                response = self.client.chat.completions.create(
                    model=used_model,
                    messages=[{
                        "role": "user",
                        "content": content
                    }],
                    temperature=temperature,
                    stream=True
                )

                # 收集streaming响应
                full_content = ""
                chunk_count = 0

                self.logger.info("开始处理VLM streaming响应...")

                for chunk in response:
                    chunk_count += 1
                    try:
                        # 安全检查chunk结构
                        if not hasattr(chunk, 'choices') or not chunk.choices:
                            self.logger.debug(f"跳过空VLM chunk {chunk_count}")
                            continue

                        # 安全检查choices列表长度
                        if len(chunk.choices) == 0:
                            self.logger.debug(f"跳过空choices的VLM chunk {chunk_count}")
                            continue

                        delta = chunk.choices[0].delta
                        content_chunk = getattr(delta, 'content', None)

                        if content_chunk:
                            full_content += content_chunk
                            self.logger.debug(f"VLM Chunk {chunk_count}: {content_chunk[:50]}...")
                    except IndexError as e:
                        self.logger.warning(f"VLM Chunk {chunk_count} 处理异常 (IndexError)，已跳过: {e}")
                        continue
                    except Exception as chunk_error:
                        self.logger.warning(f"VLM Chunk {chunk_count} 处理异常，已跳过: {chunk_error}")
                        self.logger.debug("异常VLM chunk详情: %r", chunk, exc_info=True)
                        continue

                self.logger.info(f"VLM调用完成 - 处理了 {chunk_count} 个chunks")
                self.logger.info(f"响应内容长度: {len(full_content)} 字符")

                # 检查响应内容是否为空
                if not full_content.strip():
                    raise ValueError("VLM返回空响应")

                return {
                    'success': True,
                    'content': full_content.strip(),
                    'model': used_model,
                    'provider': 'openai_compatible',
                    'attempt': attempt + 1
                }

            except Exception as e:
                error_msg = f"VLM调用失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.error(error_msg)

                # 如果是图片格式错误或400错误，不进行重试
                if "400" in str(e) or "图片输入格式" in str(e) or "解析错误" in str(e):
                    self.logger.error("检测到图片格式错误，不进行重试")
                    return {
                        'success': False,
                        'error': f"图片格式错误: {str(e)}",
                        'model': used_model,
                        'final_attempt': True
                    }

                # 如果不是最后一次尝试，等待后重试
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2  # 递增等待时间: 2, 4, 6秒
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    # 最后一次尝试失败
                    self.logger.error(error_msg, exc_info=True)
                    return {
                        'success': False,
                        'error': error_msg,
                        'model': used_model,
                        'total_attempts': max_retries
                    }


    def _make_request(self, prompt: str, model_name: str, temperature: float, max_retries: int = 3, min_content_length: int = 1) -> Dict[str, Any]:
        """
        执行具体的LLM请求，支持streaming和重试机制，以及截断检测与降级容错

        Args:
            prompt: 提示词
            model_name: 模型名称
            temperature: 生成温度
            max_retries: 最大重试次数
            min_content_length: 预期最小返回字符数（防流式异常提前中断截断）

        Returns:
            响应结果字典
        """
        for attempt in range(max_retries):
            try:
                self.logger.info(f"调用LLM: {model_name} (尝试 {attempt + 1}/{max_retries}, stream=True)")
                self.logger.info(f"提示词长度: {len(prompt)} 字符")

                last_finish_reason = None
                full_content = ""
                reasoning_content_full = ""

                # 创建streaming请求（纯流式传输，保持TCP长连接活跃避免网关空闲超时）
                response = self.client.chat.completions.create(
                    model=model_name,
                    messages=[
                        {'role': 'system', 'content': '你是一个专业的内容分析师,擅长总结和提取关键信息。'},
                        {'role': 'user', 'content': prompt}
                    ],
                    temperature=temperature,
                    max_tokens=self.max_tokens,
                    stream=True
                )

                chunk_count = 0
                self.logger.info("开始streaming响应处理...")

                for chunk in response:
                    chunk_count += 1
                    try:
                        # 安全检查chunk结构
                        if not hasattr(chunk, 'choices') or not chunk.choices:
                            self.logger.debug(f"跳过空chunk {chunk_count}")
                            continue

                        # 安全检查choices列表长度
                        if len(chunk.choices) == 0:
                            self.logger.debug(f"跳过空choices的chunk {chunk_count}")
                            continue

                        choice = chunk.choices[0]
                        finish_reason = getattr(choice, 'finish_reason', None)
                        if finish_reason:
                            last_finish_reason = finish_reason

                        delta = choice.delta

                        # 安全地获取reasoning_content和content（兼容reasoning_content、thought、reasoning等不同网关命名）
                        reasoning_content = (
                            getattr(delta, 'reasoning_content', None)
                            or getattr(delta, 'thought', None)
                            or getattr(delta, 'reasoning', None)
                        )
                        content_chunk = getattr(delta, 'content', None)

                        if reasoning_content:
                            # 推理思考内容单独收集，绝不混入最终回答正文
                            reasoning_content_full += reasoning_content
                            self.logger.debug(f"Chunk {chunk_count} - Reasoning: {reasoning_content[:50]}...")

                        if content_chunk:
                            # 只收集最终实际回答的content正文
                            full_content += content_chunk
                            self.logger.debug(f"Chunk {chunk_count} - Content: {content_chunk[:50]}...")
                    except IndexError as e:
                        self.logger.warning(f"Chunk {chunk_count} 处理异常 (IndexError)，已跳过: {e}")
                        continue
                    except Exception as chunk_error:
                        self.logger.warning(f"Chunk {chunk_count} 处理异常，已跳过: {chunk_error}")
                        self.logger.debug("异常chunk详情: %r", chunk, exc_info=True)
                        continue

                self.logger.info(
                    f"LLM流式调用完成 - 处理了 {chunk_count} 个chunks, finish_reason: {last_finish_reason}, "
                    f"实际回答内容: {len(full_content)} 字符, 思考推理内容: {len(reasoning_content_full)} 字符"
                )

                # 处理返回内容（严格区分：正文与思考过程分离）
                clean_content = full_content.strip()

                # 如果正文为空但有推理内容（极少数网关将回答放在思考字段中）
                if not clean_content and reasoning_content_full.strip():
                    self.logger.warning(f"模型 {model_name} 的正文为空，回退使用 reasoning_content ({len(reasoning_content_full)} 字符)")
                    clean_content = reasoning_content_full.strip()

                # 检查响应内容是否为空
                if not clean_content:
                    raise ValueError(f"LLM返回空响应 (finish_reason: {last_finish_reason})")

                # 严格的截断与完整性校验：
                # 1) finish_reason 校验：若有最小长度要求的任务，必须正常结束 (finish_reason == 'stop')
                # 若 finish_reason 为 None（中途断流）或 'length'（达到最大 token 截断）等，坚决不予放行！
                if min_content_length > 1 and last_finish_reason != 'stop':
                    raise ValueError(
                        f"模型 {model_name} 流式未正常结束 (finish_reason={last_finish_reason})，"
                        f"当前收到 {len(clean_content)} 字符，疑似连接中断或被服务商提前截断"
                    )

                # 2) 如果设置了预期最小长度，且返回长度明显低于预期（仅按实际正文字符数判定）
                if min_content_length > 1 and len(clean_content) < min_content_length:
                    raise ValueError(
                        f"模型 {model_name} 返回内容过短 ({len(clean_content)} 字符 < 最低预期 {min_content_length} 字符)，"
                        f"finish_reason: {last_finish_reason}，内容不完整"
                    )

                return {
                    'success': True,
                    'content': clean_content,
                    'reasoning_content': reasoning_content_full.strip(),
                    'model': model_name,
                    'provider': 'openai_compatible',
                    'attempt': attempt + 1,
                    'finish_reason': last_finish_reason
                }

            except Exception as e:
                error_msg = f"LLM调用失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}"
                self.logger.error(error_msg)

                # 如果是最后一次尝试，记录详细错误信息并返回失败
                if attempt == max_retries - 1:
                    self.logger.error(error_msg, exc_info=True)
                    return {
                        'success': False,
                        'error': error_msg,
                        'model': model_name,
                        'total_attempts': max_retries
                    }
                else:
                    # 等待后重试（递增等待：第1次失败等5秒，第2次失败等10秒...）
                    wait_time = (attempt + 1) * self.retry_delay
                    self.logger.info(f"等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)

    # 保留旧版本兼容性接口
    def call_llm(self, prompt: str, model_type: str = 'fast', temperature: float = 0.3, max_retries: int = 3) -> Dict[str, Any]:
        """兼容旧版本的调用接口"""
        if model_type == 'fast':
            return self.call_fast_model(prompt, temperature, max_retries)
        elif model_type == 'smart':
            return self.call_smart_model(prompt, temperature, max_retries)
        else:
            return self.call_fast_model(prompt, temperature, max_retries)

    def analyze_content(self, content: str, prompt_template: str, max_retries: int = 3) -> Dict[str, Any]:
        """使用快速模型分析内容（保持向后兼容性）"""
        try:
            # 格式化提示词
            prompt = prompt_template.format(content=content)
            return self.call_fast_model(prompt, max_retries=max_retries)

        except Exception as e:
            error_msg = f"内容分析失败: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'provider': 'openai_compatible'
            }


# 全局LLM客户端实例和兼容性函数
def get_llm_client() -> Optional[LLMClient]:
    """获取LLM客户端实例"""
    try:
        return LLMClient()
    except Exception as e:
        logger.error(f"创建LLM客户端失败: {e}")
        return None


def call_llm(prompt: str, model_type: str = 'fast', temperature: float = 0.3, max_retries: int = 3) -> Dict[str, Any]:
    """全局LLM调用函数"""
    client = get_llm_client()
    if client:
        return client.call_llm(prompt, model_type, temperature, max_retries)
    else:
        return {
            'success': False,
            'error': 'LLM客户端初始化失败'
        }
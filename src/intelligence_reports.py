"""
情报报告生成器 (Intelligence Report Generator)
基于富化后的帖子数据，生成高质量的情报分析报告
支持多模型并行生成和 Notion 推送
参考 info-collector-jk 项目的高级架构
"""
import logging
import json
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta
import re

from .database import DatabaseManager
from .llm_client import get_llm_client
from .config import config
from .notion_client import x_intelligence_notion_client
from .scoring import calculate_value_score

logger = logging.getLogger(__name__)


class IntelligenceReportGenerator:
    """情报报告生成器，支持多模型并行生成和 Notion 推送"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """初始化生成器"""
        if db_manager is not None:
            self.db_manager = db_manager
        else:
            self.db_manager = DatabaseManager(config)

        self.llm_client = get_llm_client()
        if not self.llm_client:
            raise RuntimeError("LLM客户端初始化失败，无法生成情报报告")

        # 获取LLM配置
        llm_config = config.get_llm_config()
        self.max_content_length = int(llm_config.get('max_content_length', 380000))
        self.max_llm_concurrency = max(1, int(llm_config.get('report_concurrency', 4)))  # 并发模型数量限制（默认4个全并发）

        # 获取评分配置
        self.scoring_config = config.get_scoring_config()

        analysis_config = config.get_analysis_config()
        context_mode = (analysis_config.get('interpretation_mode') if analysis_config else 'light') or 'light'
        if not isinstance(context_mode, str):
            context_mode = 'light'
        context_mode = context_mode.lower()
        if context_mode not in {'light', 'full'}:
            logger.warning(f"未知interpretation_mode配置: {context_mode}, 回退到light模式")
            context_mode = 'light'
        self.context_mode = context_mode

        # 获取排除标签配置
        self.exclude_tags = analysis_config.get('exclude_tags', []) if analysis_config else []

        logger.info(f"情报报告生成器初始化完成，context_mode={self.context_mode}, exclude_tags={self.exclude_tags}")

    def _log_task_start(self, task_type: str, **kwargs) -> None:
        """统一的任务开始日志记录"""
        details = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
        logger.info(f"开始执行 {task_type} 任务: {details}")

    def _log_task_complete(self, task_type: str, success_count: int, failure_count: int, **kwargs) -> None:
        """统一的任务完成日志记录"""
        status = "成功" if failure_count == 0 else f"部分成功"
        details = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
        logger.info(f"{task_type} 任务完成 ({status}): 成功 {success_count} 个，失败 {failure_count} 个。{details}")

    def _handle_task_exception(self, task_type: str, model_name: str, display_name: str, exception: Exception) -> Dict[str, Any]:
        """统一的任务异常处理"""
        error_msg = str(exception)
        logger.warning(f"{task_type} 任务异常 - 模型 {model_name} ({display_name}): {error_msg}")
        return {
            'model': model_name,
            'model_display': display_name,
            'success': False,
            'error': error_msg,
            'error_type': type(exception).__name__
        }

    def _create_error_response(self, error_msg: str, **additional_fields) -> Dict[str, Any]:
        """创建标准化的错误响应"""
        response = {
            'success': False,
            'error': error_msg,
            'items_analyzed': 0
        }
        response.update(additional_fields)
        return response

    def _bj_time(self) -> datetime:
        """获取北京时间"""
        return datetime.now(timezone.utc) + timedelta(hours=8)

    def _to_bj_time(self, dt: Optional[datetime]) -> Optional[datetime]:
        """将 UTC datetime 转换为北京时间"""
        if dt is None:
            return None
        if dt.tzinfo is not None:
            return dt.astimezone(timezone(timedelta(hours=8)))
        return dt + timedelta(hours=8)

    def _get_report_models(self) -> List[str]:
        """获取用于生成报告的模型列表"""
        if not self.llm_client:
            return []

        models: List[str] = []

        # 先尝试从 llm_client 的 report_models 属性获取
        raw_models = getattr(self.llm_client, 'report_models', None) or []
        for model_name in raw_models:
            if model_name and model_name not in models:
                models.append(model_name)

        # 如果没有找到模型列表，使用基础和优先模型
        if not models:
            base_model = getattr(self.llm_client, 'smart_model', None)
            priority_model = getattr(self.llm_client, 'priority_model', None)

            if base_model:
                models.append(base_model)
            if priority_model and priority_model not in models:
                models.insert(0, priority_model)

        return models

    def _clean_image_urls_from_content(self, content: str, media_count: int = 0) -> str:
        """
        清理帖子内容中的图片 URL，替换为简短说明

        Args:
            content: 原始帖子内容
            media_count: 图片数量（从 media_urls 字段获取）

        Returns:
            清理后的内容
        """
        if not content:
            return ""

        # 匹配 markdown 图片语法：![...](...) 或 ![](...)
        # 以及单独的 https://pbs.twimg.com/... URL
        import re

        # 统计并移除 markdown 图片
        markdown_img_pattern = r'!\[.*?\]\(https://pbs\.twimg\.com/[^\)]+\)'
        found_markdown = re.findall(markdown_img_pattern, content)

        # 移除所有 markdown 图片
        cleaned = re.sub(markdown_img_pattern, '', content)

        # 移除独立的图片 URL 行
        img_url_pattern = r'https://pbs\.twimg\.com/media/[^\s\n]+'
        cleaned = re.sub(img_url_pattern, '', cleaned)

        # 清理多余的空行（保留最多一个空行）
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        cleaned = cleaned.strip()

        # 在内容开头添加简短的图片说明
        if media_count > 0:
            img_note = f"[附{media_count}张图]"
            cleaned = f"{img_note}\n{cleaned}"

        return cleaned

    def _get_model_display_name(self, model_name: str) -> str:
        """根据模型名称生成用于展示的友好名称"""
        if not model_name:
            return 'LLM'

        lower_name = model_name.lower()
        if 'gemini' in lower_name:
            return 'Gemini'
        if 'deepseek' in lower_name:
            return 'DeepSeek'
        if 'grok' in lower_name:
            return 'Grok'
        # GLM模型识别：通用提取版本号（如GLM-4.5、GLM-4.6、GLM-4v等）
        if 'glm' in lower_name:
            # 匹配 GLM-数字.数字 或 GLM-数字v 等格式
            match = re.search(r'glm[- ]?(\d+\.?\d*v?)', lower_name)
            if match:
                version = match.group(1)
                return f'GLM{version}'
            else:
                return 'GLM'
        if 'gpt' in lower_name:
            return 'GPT'
        if 'claude' in lower_name:
            return 'Claude'

        return model_name

    def format_enriched_posts_for_smart_llm(self, enriched_posts: List[Dict[str, Any]]) -> Tuple[str, List[Dict[str, Any]]]:
        """
        为Smart LLM格式化富化后的帖子数据
        只包含核心内容以压缩上下文

        Args:
            enriched_posts: 富化后的帖子数据列表

        Returns:
            (格式化后的上下文字符串, 源映射列表)
        """
        context_parts = []
        sources = []
        total_chars = 0

        for i, post_data in enumerate(enriched_posts, 1):
            sid = f"T{i}"

            # 基础信息
            raw_user_handle = post_data.get('user_id') or 'unknown'
            user_handle = str(raw_user_handle).lstrip('@') or 'unknown'
            post_url = post_data.get('post_url') or '未知'

            # 核心内容
            original_content = post_data.get('post_content') or ''
            has_media = self._post_has_media(post_data)

            # 计算图片数量
            media_count = 0
            if has_media:
                media_urls = post_data.get('media_urls')
                if media_urls:
                    try:
                        if isinstance(media_urls, str):
                            parsed = json.loads(media_urls)
                        else:
                            parsed = media_urls
                        if isinstance(parsed, list):
                            media_count = len(parsed)
                    except (json.JSONDecodeError, TypeError):
                        pass

            # 清理图片 URL，压缩上下文
            original_content = self._clean_image_urls_from_content(original_content, media_count)

            include_deep = not (self.context_mode == 'light' and not has_media)

            deep_interpretation = ''
            if include_deep:
                deep_interpretation = (post_data.get('deep_interpretation') or '').strip()
                if not deep_interpretation:
                    deep_interpretation = "无深度洞察"

            # 拼接原帖和解析
            if include_deep:
                block = f"[{sid} @{user_handle}]\n{original_content}\n→ 洞察: {deep_interpretation}"
            else:
                block = f"[{sid} @{user_handle}]\n{original_content}"

            # 检查长度限制
            if total_chars + len(block) > self.max_content_length:
                logger.info(f"达到最大内容限制({self.max_content_length}),截断帖子列表于第 {i-1} 条")
                break

            context_parts.append(block)
            total_chars += len(block)

            # 添加到源映射，用于后续生成来源清单
            # 即使上下文简化了，来源清单依然需要这些信息
            llm_summary = post_data.get('llm_summary', '无摘要')
            sources.append({
                'sid': sid,
                'title': self._truncate(llm_summary or original_content, 100),
                'link': post_url,
                'nickname': user_handle,
                'excerpt': self._truncate(original_content, 120)
            })

        return "\n\n---\n\n".join(context_parts), sources

    def _truncate(self, text: str, max_len: int) -> str:
        """截断文本，保持可读性"""
        if not text:
            return ""
        if len(text) <= max_len:
            return text
        t = text[:max_len]
        # 尝试在句尾截断
        for d in ['。', '!', '?', '.', '!', '?', '\n']:
            pos = t.rfind(d)
            if pos > max_len * 0.7:
                return t[:pos + 1] + "\n..."
        return t + "\n..."

    def _post_has_media(self, post_data: Dict[str, Any]) -> bool:
        """判断帖子是否包含媒体内容"""
        has_media_flag = post_data.get('has_media')
        if has_media_flag is not None:
            try:
                return bool(int(has_media_flag))
            except (ValueError, TypeError):
                return bool(has_media_flag)

        media_urls = post_data.get('media_urls')
        if not media_urls:
            return False

        if isinstance(media_urls, str):
            try:
                parsed = json.loads(media_urls)
            except (json.JSONDecodeError, TypeError):
                return False
        elif isinstance(media_urls, list):
            parsed = media_urls
        else:
            return False

        if isinstance(parsed, list):
            return len(parsed) > 0

        return False

    def get_light_report_prompt(self, formatted_context: str, time_range: str) -> str:
        """
        构建"日报资讯"提示词（全面信息流式的简报）
        强调全面性和分类聚合
        """
        prompt_template = f"""# Role: 世界一流的科技资讯编辑，擅长快速提炼关键信息并分类呈现

# Context:
你正在为忙碌的科技从业者编写一份每日快讯。读者希望在最短时间内获取{time_range}内全球科技领域的重要动态。你收到的是经过筛选的X/Twitter技术领袖发布的帖子。

# Core Principles:
1. **全面覆盖 (Comprehensive Coverage)**: 尽可能涵盖所有有价值的信息点，不遗漏重要动态。
2. **分类清晰 (Clear Categorization)**: 按主题分类组织信息，便于快速查找。
3. **详略得当 (Appropriate Detail)**: 每条信息都应提供足够上下文，确保读者能理解其核心价值。避免过度压缩，但保持精炼。
4. **可追溯性 (Traceability)**: 每条信息必须标注来源 `[Source: T_n]`。

# Input Data Format:
你将收到一系列帖子，格式为：
- 纯文本帖：`[T_id @user_handle]` + 帖子内容
- 图文帖：`[T_id @user_handle]` + 帖子内容 + `→ 洞察: {{AI解读}}`

# Your Task:
生成一份结构化的日报资讯，严格按照以下Markdown格式。请为每条资讯提供详实、清晰的描述。

## 📰 今日要闻
*本节汇总最重要的5-15条核心新闻*
- **[新闻标题]**: 简要描述，说清楚新闻核心内容 (50-200字) [Source: T_n]

---

## 🔥 热门话题
*按话题分类组织讨论热点*

### 话题A: [话题名称]
- **[核心观点]**: 对观点的清晰阐述，说明其背景和重要性 (50-200字) [Source: T_n]
- **[核心观点]**: ... [Source: T_m]

### 话题B: [话题名称]
- **[核心观点]**: ... [Source: T_x]

---

## 💡 技术动态
*新技术、新功能、技术讨论*
- **[技术点]**: 详细说明其背景、核心内容和潜在影响 (50-200字) [Source: T_n]

---

## 🚀 产品发布
*新产品、新功能发布*
- **[产品名]**: 介绍其核心特性、目标用户和市场反应 (50-200字) [Source: T_n]

---

## 🛠️ 工具资源
*有价值的工具、库、教程*
- **[资源名]**: 详细说明其用途、特点和推荐理由 (50-200字) [Source: T_n]

---

## 📊 行业观察
*行业趋势、商业分析、市场动态*
- **[观察点]**: 阐述关键信息、数据和你的解读 (50-200字) [Source: T_n]

---

## 🎯 精选观点
*值得关注的独特见解*
- **[@用户]**: 清晰阐述其核心观点、论据和启发意义 (50-200字) [Source: T_n]

# Input Data:
```
{formatted_context}
```

# Important Notes:
1. **必须完整覆盖所有分类板块**：必须按顺序输出 ## 📰 今日要闻、## 🔥 热门话题、## 💡 技术动态、## 🚀 产品发布、## 🛠️ 工具资源、## 📊 行业观察、## 🎯 精选观点 全部7个板块，严禁省略任何板块！每个分类下至少整理3-5条信息。
2. **信息要全面**，不要遗漏有价值的内容。
3. 每条信息必须有 `[Source: T_n]` 标注。
4. **内容为王**: 确保每条信息的描述足够清晰、完整，能够独立成文。对于复杂或重要的动态，宁可篇幅稍长，也要说清楚来龙去脉和核心价值。
"""
        return prompt_template

    def get_intelligence_report_prompt(
        self,
        formatted_context: str,
        time_range: str,
        context_mode: Optional[str] = None
    ) -> str:
        """
        构建情报分析报告的提示词。
        提示词内容直接内联在此函数中，以减少外部文件依赖。
        """
        normalized_mode = (context_mode or getattr(self, 'context_mode', 'light') or 'light').lower()
        if normalized_mode not in {'light', 'full'}:
            normalized_mode = 'light'

        # 定义精确的数据格式描述，以匹配 format_enriched_posts_for_smart_llm 的输出
        # 这部分内容旨在告知LLM其接收到的`formatted_context`中每个帖子的详细结构
        if normalized_mode == 'light':
            accurate_data_format_description = """# Input Data Format:
你将收到一系列经过预处理的帖子，采用紧凑格式以优化上下文。每条帖子包含原始文本内容；只有当帖子包含图片或多媒体时，才会额外附带深度洞察。

**格式说明**：
- 纯文本帖：`[T_id @user_handle]` + 换行 + 帖子原文
- 图文帖：`[T_id @user_handle]` + 换行 + 帖子原文 + 换行 + `→ 洞察: {AI生成的综合解读}`

**重要**：
1. T_id 是来源标识符，你在分析中引用时使用 `[Source: T_id]` 格式
2. 对于纯文本帖，请直接基于原文进行分析
3. 对于图文帖，请综合原文和洞察内容进行分析"""
        else:
            accurate_data_format_description = """# Input Data Format:
你将收到一系列经过预处理的帖子，采用紧凑格式以优化上下文。每条帖子都包含原始内容和AI生成的深度洞察。

**格式说明**：
`[T_id @user_handle]` + 换行 + 帖子原文 + 换行 + `→ 洞察: {LLM生成的深度解读}`

**重要**：
1. T_id 是来源标识符，你在分析中引用时使用 `[Source: T_id]` 格式
2. 请综合利用原文和洞察两部分信息进行分析
3. 洞察部分是AI对帖子的深度解读，是你分析的核心依据"""

        # 核心提示词模板
        prompt_template = f"""# Role: 世界顶级的技术与风险投资分析师，拥有《经济学人》的编辑严谨度和《Stratechery》的前瞻性洞察力。

# Context:
你正在为一份全球顶级技术专家、创始人与VC合伙人阅读的内参撰写报告。他们时间宝贵，极度关注"信号"，厌恶"噪音"。你收到的原始材料是{time_range}内，由我们精心筛选的全球技术思想领袖在X/Twitter上发布的帖子，并经过了初步的AI洞察处理。

# Core Principles:
1.  **深度与价值优先 (Depth & Value First)**: 你的核心目标是挖掘出对从业者有直接价值的信息。在撰写每个部分时，都应追求内容的**深度和完整性**，**避免过于简短的概括**。
2.  **深度合成 (Deep Synthesis)**: 不要简单罗列。你需要将不同来源的信息点连接起来，构建成有意义的叙事（Narrative）。
3.  **注入洞见 (Inject Insight)**: 你不是一个总结者，而是一个分析师。在陈述事实和观点的基础上，**必须**加入你自己的、基于上下文的、有深度的分析和评论。
4.  **绝对可追溯 (Absolute Traceability)**: 你的每一条洞察、判断和建议，都必须在句末使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注信息来源。这是硬性要求,绝对不能遗漏。

{accurate_data_format_description}

# Your Task:
请严格按照以下五个层次的分析框架，生成一份**内容丰富详实、信息密度极高、洞察深刻**的完整Markdown情报报告。

**第一层次：动态与热点概览 (Dynamics & Hotspot Overview)**
*   **1.1 动态摘要**: 写一个300字左右的"执行摘要"，总结周期内最重要的动态和最关键的信号。
*   **1.2 核心话题**: 识别出本周期内所有值得关注的核心话题（不少于5个）。对每个话题，**详细阐述**其核心议题，并**尽可能全面地**列出最具代表性的观点和讨论方向。

**第二层次：观点对撞圆桌 (Perspectives Collision Round-table)**
*   任务：围绕本周期内最具争议性或多面性的话题，组织1场虚拟圆桌讨论。
*   要求：
    1.  **设定议题**: 明确本场圆桌的核心议题。
    2.  **邀请嘉宾**: 从数据中挑选持有不同（甚至对立）观点的用户作为"虚拟嘉宾"。
    3.  **呈现观点**: 清晰地展示每位嘉宾的核心论点，并直接引用其原文精华。
    4.  **分析师点评 (关键！)**: 在所有观点陈述完毕后，**加入你自己的、篇幅充足的分析师点评**。点评内容应包括但不限于：指出各方观点的盲区、点明争议的本质、预测该议题的未来走向、或者提出一个更高维度的综合性看法。
    5.  **备选方案**: 如果本周期内没有明显对立的观点，请选择一个核心话题，**深入剖析**其不同角度（如开发者、产品经理、用户）的论述，或将其改为对一个关键人物核心观点的深度剖析。

**第三层次：趋势与叙事深度分析 (Trend & Narrative Analysis)**
*   **3.1 趋势/信号**: 识别所有热度高或者讨论度快速上升的"趋势"或"微弱信号"。**详细描述**它是什么，为什么它现在出现，以及它可能对行业产生什么影响。**不要局限于少数几点**。
*   **3.2 宏大叙事**: 寻找不同话题之间的内在联系，构建一个或多个宏大叙事。**详细展开**这个叙事，例如，将"新AI模型的发布"、"开源社区的讨论"和"下游应用的探索"联系起来，形成一个关于"XXX技术从理论到实践的演进路径"的完整叙事。

**第四层次：精选资源库 (Curated Resource Library)**
*   任务：从本周期所有分享的链接中，精选出**所有具备高价值**的资源，尽可能多，不要只局限于几个。
*   要求：
    *   **4.1 教程与指南**: 挑选出所有有价值的教程、指南或深度学习笔记。
    *   **4.2 工具与项目**: 挑选出所有值得关注的新工具或开源项目。
    *   对每个入选的资源，**用一段话详细说明**其核心价值和推荐理由，而不仅仅是一句话概括。

**第五层次：角色化行动建议 (Role-Based Actionable Recommendations)**
*   任务：将所有分析转化为对特定角色的、**丰富且具体**的、可立即执行的建议。
*   要求：建议必须具体、新颖且具有前瞻性，并阐述其背后的逻辑。
    *   **给开发者的建议**: [例如：建议立即研究 `XXX` 框架，因为它在解决 `YYY` 问题上表现出巨大潜力。社区讨论表明...] [Source: T_n]
    *   **给产品经理/创业者的建议**: [例如：社区对 `ZZZ` 场景的需求反复出现，但现有解决方案均有缺陷，这可能是一个被忽视的蓝海市场。具体表现为...] [Source: T_m]
    *   **给投资者的建议**: [例如：`AAA` 领域的讨论热度与技术成熟度出现"共振"，可能预示着商业化拐点即将到来。关键信号包括...] [Source: T_k]
    *   ...(请为每个角色提供**尽可能多**的有价值建议)

# Output Format (Strictly follow this Markdown structure):

## 一、动态与热点概览
### 1.1 动态摘要
[执行摘要内容]
### 1.2 核心话题
*   **话题A**: [详细阐述]
    *   观点1: [内容] [Source: T_n]
    *   观点2: [内容] [Source: T_m]
    *   ... (更多观点)
*   **话题B**: ...
*   ... (更多话题)

---

## 二、观点对撞圆桌：[议题名称]
### 嘉宾观点
*   **正方代表 (`@user_handle_1`)**: [观点陈述] [Source: T_a]
*   **反方代表 (`@user_handle_2`)**: [观点陈述] [Source: T_b]
*   **中立/技术派 (`@user_handle_3`)**: [观点陈述] [Source: T_c]
### 分析师点评
[你对这场辩论的总结、洞察和更高维度的、篇幅充足的分析...]

---

## 三、趋势与叙事分析
### 3.1 发展趋势：[趋势名称]
[详细描述该趋势...] [Sources: T_d, T_e]
...(详尽的更多趋势)
### 3.2 宏大叙事：[叙事名称]
[详细描述该叙事...] [Sources: T_f, T_g]
...(详尽的更多叙事)

---

## 四、精选资源库
### 4.1 教程与指南
*   **[资源名称]**: [详细推荐理由] [Source: T_h]
*   ... (详尽的更多资源)
### 4.2 工具与项目
*   **[资源名称]**: [详细推荐理由] [Source: T_i]
*   ... (详尽的更多资源)

---

## 五、角色化行动建议
*   **To 开发者**:
    * [建议内容] [Source: T_j]
    * ... (详尽的更多建议)
*   **To 产品经理/创业者**:
    * [建议内容] [Source: T_k]
    * ... (详尽的更多建议)
*   **To 投资者/研究者**:
    * [建议内容] [Source: T_l]
    * ... (详尽的更多建议)

# Input Data:
```
{formatted_context}
```
"""

        return prompt_template

    async def _generate_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        enriched_posts: List[Dict[str, Any]],
        context_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """在独立线程中生成指定模型的报告"""
        return await asyncio.to_thread(
            self._generate_report_for_model_sync,
            model_name,
            display_name,
            enriched_posts,
            context_md,
            sources,
            prompt,
            start_time,
            end_time
        )

    def _generate_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        enriched_posts: List[Dict[str, Any]],
        context_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行指定模型的报告生成和Notion推送"""

        logger.info(f"[{display_name}] 模型线程启动，开始生成情报报告")

        # 针对本次分析的帖子数自适应计算最低正文长度（180条推文时约3500字，防止半截截断）
        min_report_length = min(3500, max(800, len(enriched_posts) * 20))

        # 调用LLM生成报告
        try:
            response = self.llm_client.call_smart_model(
                prompt,
                model_override=model_name,
                temperature=0.4,
                min_content_length=min_report_length
            )

            if not response or not response.get('success'):
                error_msg = f"LLM调用失败: {response.get('error') if response else 'Unknown error'}"
                logger.warning(f"[{display_name}] {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'model': model_name,
                    'model_display': display_name
                }

            llm_output = response.get('content', '')
        except Exception as e:
            error_msg = f"LLM调用异常: {str(e)}"
            logger.error(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name
            }

        # 为LLM生成的报告添加标准头部信息
        beijing_time = self._bj_time()
        start_bj = self._to_bj_time(start_time)
        end_bj = self._to_bj_time(end_time)
        header_info = [
            f"# 📊 X/Twitter 技术情报日报 - {display_name}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_bj.strftime('%Y-%m-%d %H:%M:%S')} - {end_bj.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(enriched_posts)} 条*",
            "",
            "---",
            ""
        ]

        # 清理LLM输出中可能的格式问题
        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)

        sources_section = self._render_sources_section(sources)

        # 构建报告尾部
        footer_lines = ["", "---", ""]
        provider = response.get('provider')
        model = response.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")

        footer_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(enriched_posts)} 条动态",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section

        # 应用来源链接增强后处理
        report_content = self._enhance_source_links(report_content, sources)

        title = f"X/Twitter 技术情报日报 - {display_name} - {end_bj.strftime('%Y-%m-%d %H:%M')}"

        # 保存报告到数据库
        try:
            if self.db_manager.save_intelligence_report(
                'daily',
                title,
                report_content,
                start_time,
                end_time
            ):
                logger.info(f"[{display_name}] 情报报告已成功保存到数据库")
            else:
                logger.warning(f"[{display_name}] 报告保存到数据库失败")
        except Exception as e:
            logger.error(f"[{display_name}] 保存报告到数据库时发生异常: {e}")

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_title': title,
            'report_content': report_content,
            'provider': response.get('provider') if response else None,
            'items_analyzed': len(enriched_posts)
        }

        # 尝试推送到Notion
        notion_push_info = None
        try:
            # 格式化Notion标题
            beijing_time = self._bj_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] [{display_name}] X技术情报日报 ({len(enriched_posts)}条动态)"

            logger.info(f"开始推送情报报告到Notion ({display_name}): {notion_title}")

            notion_result = x_intelligence_notion_client.create_report_page(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time
            )

            if notion_result.get('success'):
                logger.info(f"情报报告成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                logger.warning(f"推送情报报告到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            logger.warning(f"推送情报报告到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    def _clean_llm_output_for_notion(self, llm_output: str) -> str:
        """清理LLM输出内容，确保Notion兼容性"""
        if not llm_output:
            return ""

        # 保护Source引用格式，不要替换其中的方括号
        import re

        # 先提取所有Source引用
        source_pattern = r'\[Sources?:\s*[T\d\s,]+\]'
        sources = re.findall(source_pattern, llm_output)

        # 临时替换Source引用为占位符
        temp_llm_output = llm_output
        source_placeholders = {}
        for i, source in enumerate(sources):
            placeholder = f"__SOURCE_PLACEHOLDER_{i}__"
            source_placeholders[placeholder] = source
            temp_llm_output = temp_llm_output.replace(source, placeholder)

        # 替换其他可能导致Markdown链接冲突的方括号
        cleaned = temp_llm_output.replace('[', '【').replace(']', '】')

        # 恢复Source引用
        for placeholder, original_source in source_placeholders.items():
            cleaned = cleaned.replace(placeholder, original_source)

        # 确保行尾有适当的空格用于换行
        lines = cleaned.split('\n')
        processed_lines = []

        for line in lines:
            # 对于以*开头的斜体行，在行尾添加空格以确保换行
            if line.strip().startswith('*') and line.strip().endswith('*'):
                processed_lines.append(line.rstrip() + '  ')
            else:
                processed_lines.append(line)

        return '\n'.join(processed_lines)

    def _render_sources_section(self, sources: List[Dict[str, Any]]) -> str:
        """渲染来源清单部分"""
        if not sources:
            return ""

        lines = ["## 📚 来源清单 (Source List)", ""]
        for s in sources:
            # 清理标题中的方括号，避免与Markdown链接冲突
            clean_title = (s.get('title') or s.get('excerpt') or '').replace('[', '【').replace(']', '】')
            nickname = s.get('nickname') or ''
            if nickname:
                nickname_display = f"@{nickname}"
            else:
                nickname_display = ""

            link = s.get('link')
            if link:
                actor_part = f"[{nickname_display}]({link})" if nickname_display else f"[来源]({link})"
            else:
                actor_part = nickname_display or "来源"

            lines.append(f"- **【{s.get('sid')}】**: {actor_part}: {clean_title}")
        return "\n".join(lines)

    def _enhance_source_links(self, report_content: str, sources: List[Dict[str, Any]]) -> str:
        """
        增强报告中的来源链接，将 [Source: T1, T2] 中的每个 Txx 转换为可点击的链接
        """
        import re

        # 构建来源ID到链接的映射
        source_link_map = {s['sid']: s['link'] for s in sources}

        def replace_source_refs(match):
            # 提取完整的 Source 引用内容
            full_source_text = match.group(0)  # 如 "[Source: T2, T9, T18]"
            source_content = match.group(1)    # 如 "T2, T9, T18"

            # 分割并处理每个来源ID
            source_ids = [sid.strip() for sid in source_content.split(',')]
            linked_sources = []

            for sid in source_ids:
                if sid in source_link_map:
                    # 将 Txx 转换为链接
                    linked_sources.append(f"[{sid}]({source_link_map[sid]})")
                else:
                    # 如果找不到对应链接，保持原样
                    linked_sources.append(sid)

            # 重新组合
            return f"📎 [Source: {', '.join(linked_sources)}]"

        # 查找所有 [Source: ...] 或 [Sources: ...] 模式并替换
        pattern = r'\[Sources?:\s*([T\d\s,]+)\]'
        enhanced_content = re.sub(pattern, replace_source_refs, report_content)

        return enhanced_content

    def _fetch_and_score_posts(self, start_time: datetime, end_time: datetime, limit: int, candidate_multiplier: Optional[float] = None) -> List[Dict[str, Any]]:
        """获取并根据价值分筛选帖子"""
        # 确定候选池大小
        if candidate_multiplier is None:
            candidate_multiplier = self.scoring_config.get('candidate_multiplier', 3)

        candidate_limit = int(limit * candidate_multiplier)

        logger.info(f"正在获取候选帖子池 (limit={limit}, multiplier={candidate_multiplier}, candidate_limit={candidate_limit})")

        # 获取更大的候选池
        candidate_posts = self.db_manager.get_enriched_posts_for_report(
            start_time,
            end_time,
            candidate_limit,
            context_mode=self.context_mode,
            exclude_tags=self.exclude_tags
        )

        if not candidate_posts:
            logger.warning("候选池为空")
            return []

        logger.info(f"获取到 {len(candidate_posts)} 条候选帖子，开始计算价值分...")

        # 计算价值分
        for post in candidate_posts:
            post['value_score'] = calculate_value_score(post, self.scoring_config)

        # 排序: 优先按价值分降序，次优先按发布时间降序 (Tie-breaker)
        candidate_posts.sort(key=lambda p: (p.get('value_score', 0), p.get('published_at')), reverse=True)

        # 截取 Top N
        final_posts = candidate_posts[:limit]

        # 记录一下最高分和最低分以便调试
        if final_posts:
            highest = final_posts[0].get('value_score')
            lowest = final_posts[-1].get('value_score')
            logger.info(f"筛选完成: {len(final_posts)} 条。最高分: {highest}, 最低入选分: {lowest}")

        return final_posts

    async def generate_intelligence_report(self, hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
        """
        生成情报分析报告，支持多模型并行生成

        Args:
            hours: 时间范围（小时）
            limit: 最大帖子数量
            candidate_multiplier: 候选池倍数

        Returns:
            生成结果
        """
        self._log_task_start("情报报告生成", hours=hours, limit=limit, multiplier=candidate_multiplier)

        try:
            # 计算时间范围（统一以 UTC 基准查询数据库）
            end_time = datetime.now(timezone.utc).replace(tzinfo=None)
            start_time = end_time - timedelta(hours=hours)

            # 获取并筛选帖子
            enriched_posts = self._fetch_and_score_posts(start_time, end_time, limit, candidate_multiplier)

            if not enriched_posts:
                logger.warning(f"在指定时间范围内没有找到符合条件的帖子数据")
                return self._create_error_response('没有可用的帖子数据')

            logger.info(f"筛选出 {len(enriched_posts)} 条高价值帖子数据用于报告生成")

            # 格式化上下文
            formatted_context, sources = self.format_enriched_posts_for_smart_llm(enriched_posts)

            # 构建提示词
            time_range_str = f"过去{hours}小时"
            prompt = self.get_intelligence_report_prompt(
                formatted_context,
                time_range_str,
                context_mode=self.context_mode
            )

            logger.info(f"提示词长度: {len(prompt)} 字符")

            # 获取要使用的模型列表
            models_to_generate = self._get_report_models()
            if not models_to_generate:
                logger.warning("未配置任何可用于生成报告的模型")
                return self._create_error_response('未配置可用的LLM模型')

            model_reports: List[Dict[str, Any]] = []
            failures: List[Dict[str, Any]] = []
            tasks = []
            task_meta: List[Dict[str, str]] = []

            # 为每个模型创建并行任务
            for model_name in models_to_generate:
                display_name = self._get_model_display_name(model_name)
                task_meta.append({'model': model_name, 'display': display_name})
                tasks.append(
                    self._generate_report_for_model(
                        model_name=model_name,
                        display_name=display_name,
                        enriched_posts=enriched_posts,
                        context_md=formatted_context,
                        sources=sources,
                        prompt=prompt,
                        start_time=start_time,
                        end_time=end_time
                    )
                )

            logger.info(
                f"开始并行生成 {len(tasks)} 份情报报告 (并发度限制: {self.max_llm_concurrency}): {[meta['display'] for meta in task_meta]}"
            )

            # 通过信号量控制模型并发数，避免多模型超长请求同时涌入造成网关断流
            sem = asyncio.Semaphore(self.max_llm_concurrency)

            async def _run_bounded_task(coro):
                async with sem:
                    return await coro

            # 并行执行所有任务
            task_results = await asyncio.gather(*[_run_bounded_task(t) for t in tasks], return_exceptions=True)

            # 处理任务结果
            for meta, task_result in zip(task_meta, task_results):
                model_name = meta['model']
                display_name = meta['display']

                if isinstance(task_result, Exception):
                    error_msg = str(task_result)
                    logger.warning(
                        f"模型 {model_name} ({display_name}) 报告生成过程中出现未处理异常: {error_msg}"
                    )
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': error_msg
                    })
                    continue

                if task_result.get('success'):
                    model_reports.append(task_result)
                else:
                    failure_entry = {
                        'model': model_name,
                        'model_display': display_name,
                        'error': task_result.get('error', '报告生成失败')
                    }
                    failures.append(failure_entry)

            # 构建最终结果
            overall_success = len(model_reports) > 0
            result = {
                'success': overall_success,
                'items_analyzed': len(enriched_posts) if overall_success else 0,
                'model_reports': model_reports,
                'failures': failures
            }

            if overall_success:
                # 使用第一个成功的报告作为主要结果
                primary_report = model_reports[0]
                result['report_title'] = primary_report['report_title']
                result['report_content'] = primary_report['report_content']
                result['notion_push'] = primary_report.get('notion_push')
                start_bj = self._to_bj_time(start_time)
                end_bj = self._to_bj_time(end_time)
                result['time_range'] = f"{start_bj.strftime('%Y-%m-%d %H:%M')} - {end_bj.strftime('%Y-%m-%d %H:%M')}"

            self._log_task_complete(
                "情报报告生成",
                len(model_reports),
                len(failures),
                models=len(models_to_generate)
            )

            return result

        except Exception as e:
            logger.error(f"生成情报报告时发生异常: {e}", exc_info=True)
            return self._create_error_response(f'生成异常: {str(e)}')

    async def generate_light_reports(self, hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
        """
        生成日报资讯（Light Reports）- 多模型并行生成

        Args:
            hours: 时间范围（小时）
            limit: 最大帖子数量
            candidate_multiplier: 候选池倍数

        Returns:
            生成结果
        """
        self._log_task_start("日报资讯生成", hours=hours, limit=limit, multiplier=candidate_multiplier)

        try:
            # 计算时间范围（统一以 UTC 基准查询数据库）
            end_time = datetime.now(timezone.utc).replace(tzinfo=None)
            start_time = end_time - timedelta(hours=hours)

            # 获取并筛选帖子
            enriched_posts = self._fetch_and_score_posts(start_time, end_time, limit, candidate_multiplier)

            if not enriched_posts:
                logger.warning(f"在指定时间范围内没有找到符合条件的帖子数据")
                return self._create_error_response('没有可用的帖子数据')

            logger.info(f"筛选出 {len(enriched_posts)} 条高价值帖子数据，开始生成日报资讯")

            # 格式化上下文
            formatted_context, sources = self.format_enriched_posts_for_smart_llm(enriched_posts)

            # 构建日报资讯提示词
            time_range_str = f"过去{hours}小时"
            prompt = self.get_light_report_prompt(formatted_context, time_range_str)

            logger.info(f"日报资讯提示词长度: {len(prompt)} 字符")

            # 获取要使用的模型列表
            models_to_generate = self._get_report_models()
            if not models_to_generate:
                logger.warning("未配置任何可用于生成报告的模型")
                return self._create_error_response('未配置可用的LLM模型')

            model_reports: List[Dict[str, Any]] = []
            failures: List[Dict[str, Any]] = []
            tasks = []
            task_meta: List[Dict[str, str]] = []

            # 为每个模型创建并行任务
            for model_name in models_to_generate:
                display_name = self._get_model_display_name(model_name)
                task_meta.append({'model': model_name, 'display': display_name})
                tasks.append(
                    self._generate_light_report_for_model(
                        model_name=model_name,
                        display_name=display_name,
                        enriched_posts=enriched_posts,
                        context_md=formatted_context,
                        sources=sources,
                        prompt=prompt,
                        start_time=start_time,
                        end_time=end_time
                    )
                )

            logger.info(f"开始并行生成 {len(tasks)} 份日报资讯 (并发度限制: {self.max_llm_concurrency}): {[meta['display'] for meta in task_meta]}")

            # 通过信号量控制模型并发数，避免多模型超长请求同时涌入造成网关断流
            sem = asyncio.Semaphore(self.max_llm_concurrency)

            async def _run_bounded_task(coro):
                async with sem:
                    return await coro

            # 并行执行所有任务
            task_results = await asyncio.gather(*[_run_bounded_task(t) for t in tasks], return_exceptions=True)

            # 处理任务结果
            for meta, task_result in zip(task_meta, task_results):
                model_name = meta['model']
                display_name = meta['display']

                if isinstance(task_result, Exception):
                    error_msg = str(task_result)
                    logger.warning(f"模型 {model_name} ({display_name}) 日报资讯生成异常: {error_msg}")
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': error_msg
                    })
                    continue

                if task_result.get('success'):
                    model_reports.append(task_result)
                else:
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': task_result.get('error', '报告生成失败')
                    })

            # 构建最终结果
            overall_success = len(model_reports) > 0
            result = {
                'success': overall_success,
                'items_analyzed': len(enriched_posts) if overall_success else 0,
                'model_reports': model_reports,
                'failures': failures,
                'report_type': 'light'
            }

            self._log_task_complete(
                "日报资讯生成",
                len(model_reports),
                len(failures),
                models=len(models_to_generate)
            )

            return result

        except Exception as e:
            logger.error(f"生成日报资讯时发生异常: {e}", exc_info=True)
            return self._create_error_response(f'生成异常: {str(e)}')

    async def _generate_light_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        enriched_posts: List[Dict[str, Any]],
        context_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """生成单个模型的日报资讯"""
        return await asyncio.to_thread(
            self._generate_light_report_for_model_sync,
            model_name,
            display_name,
            enriched_posts,
            context_md,
            sources,
            prompt,
            start_time,
            end_time
        )

    def _generate_light_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        enriched_posts: List[Dict[str, Any]],
        context_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行单个模型的日报资讯生成和Notion推送"""

        logger.info(f"[{display_name}] 开始生成日报资讯")

        # 针对本次分析的帖子数自适应计算最低正文长度（180条推文时约3500字，防止半截截断）
        min_report_length = min(3500, max(800, len(enriched_posts) * 20))

        # 调用LLM生成报告
        try:
            response = self.llm_client.call_smart_model(
                prompt,
                model_override=model_name,
                temperature=0.3,
                min_content_length=min_report_length
            )

            if not response or not response.get('success'):
                error_msg = f"LLM调用失败: {response.get('error') if response else 'Unknown error'}"
                logger.warning(f"[{display_name}] {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'model': model_name,
                    'model_display': display_name
                }

            llm_output = response.get('content', '')

            # 校验核心板块是否存在（防止模型偷懒漏掉关键板块）
            if '今日要闻' not in llm_output or '热门话题' not in llm_output:
                error_msg = f"LLM输出不完整：缺少核心板块（未包含'今日要闻'或'热门话题'）"
                logger.warning(f"[{display_name}] {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'model': model_name,
                    'model_display': display_name
                }
        except Exception as e:
            error_msg = f"LLM调用异常: {str(e)}"
            logger.error(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name
            }

        # 为LLM生成的报告添加标准头部信息
        beijing_time = self._bj_time()
        start_bj = self._to_bj_time(start_time)
        end_bj = self._to_bj_time(end_time)
        header_info = [
            f"# 📰 X/Twitter 技术日报资讯 - {display_name}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_bj.strftime('%Y-%m-%d %H:%M:%S')} - {end_bj.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(enriched_posts)} 条*",
            "",
            "---",
            ""
        ]

        # 清理LLM输出
        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)

        sources_section = self._render_sources_section(sources)

        # 构建报告尾部
        footer_lines = ["", "---", ""]
        provider = response.get('provider')
        model = response.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")

        footer_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(enriched_posts)} 条动态",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section

        # 应用来源链接增强
        report_content = self._enhance_source_links(report_content, sources)

        title = f"X技术日报资讯 - {display_name} - {end_bj.strftime('%Y-%m-%d %H:%M')}"

        # 保存报告到数据库
        try:
            if self.db_manager.save_intelligence_report(
                'daily_light',
                title,
                report_content,
                start_time,
                end_time
            ):
                logger.info(f"[{display_name}] 日报资讯已成功保存到数据库")
            else:
                logger.warning(f"[{display_name}] 日报资讯保存到数据库失败")
        except Exception as e:
            logger.error(f"[{display_name}] 保存日报资讯到数据库时发生异常: {e}")

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_title': title,
            'report_content': report_content,
            'provider': response.get('provider') if response else None,
            'items_analyzed': len(enriched_posts)
        }

        # 尝试推送到Notion（使用层级结构）
        notion_push_info = None
        try:
            beijing_time = self._bj_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] [{display_name}] X技术日报 ({len(enriched_posts)}条)"

            logger.info(f"开始推送日报资讯到Notion ({display_name}): {notion_title}")

            notion_result = x_intelligence_notion_client.create_report_page_in_hierarchy(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time,
                report_type='light'
            )

            if notion_result.get('success'):
                logger.info(f"日报资讯成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                logger.warning(f"推送日报资讯到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            logger.warning(f"推送日报资讯到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    async def generate_deep_report(self, hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
        """
        生成深度报告（Deep Report）- 多模型并行生成

        Args:
            hours: 时间范围（小时）
            limit: 最大帖子数量
            candidate_multiplier: 候选池倍数

        Returns:
            生成结果
        """
        self._log_task_start("深度报告生成", hours=hours, limit=limit, multiplier=candidate_multiplier)

        try:
            # 计算时间范围（统一以 UTC 基准查询数据库）
            end_time = datetime.now(timezone.utc).replace(tzinfo=None)
            start_time = end_time - timedelta(hours=hours)

            # 获取并筛选帖子
            enriched_posts = self._fetch_and_score_posts(start_time, end_time, limit, candidate_multiplier)

            if not enriched_posts:
                logger.warning(f"在指定时间范围内没有找到符合条件的帖子数据")
                return self._create_error_response('没有可用的帖子数据')

            logger.info(f"筛选出 {len(enriched_posts)} 条高价值帖子数据，开始生成深度报告")

            # 格式化上下文
            formatted_context, sources = self.format_enriched_posts_for_smart_llm(enriched_posts)

            # 构建深度报告提示词
            time_range_str = f"过去{hours}小时"
            prompt = self.get_intelligence_report_prompt(
                formatted_context,
                time_range_str,
                context_mode=self.context_mode
            )

            logger.info(f"深度报告提示词长度: {len(prompt)} 字符")

            # 获取要使用的模型列表
            models_to_generate = self._get_report_models()
            if not models_to_generate:
                logger.warning("未配置任何可用于生成报告的模型")
                return self._create_error_response('未配置可用的LLM模型')

            model_reports: List[Dict[str, Any]] = []
            failures: List[Dict[str, Any]] = []
            tasks = []
            task_meta: List[Dict[str, str]] = []

            # 为每个模型创建并行任务
            for model_name in models_to_generate:
                display_name = self._get_model_display_name(model_name)
                task_meta.append({'model': model_name, 'display': display_name})
                tasks.append(
                    self._generate_deep_report_for_model(
                        model_name=model_name,
                        display_name=display_name,
                        enriched_posts=enriched_posts,
                        context_md=formatted_context,
                        sources=sources,
                        prompt=prompt,
                        start_time=start_time,
                        end_time=end_time
                    )
                )

            logger.info(f"开始并行生成 {len(tasks)} 份深度报告 (并发度限制: {self.max_llm_concurrency}): {[meta['display'] for meta in task_meta]}")

            # 通过信号量控制模型并发数，避免多模型超长请求同时涌入造成网关断流
            sem = asyncio.Semaphore(self.max_llm_concurrency)

            async def _run_bounded_task(coro):
                async with sem:
                    return await coro

            # 并行执行所有任务
            task_results = await asyncio.gather(*[_run_bounded_task(t) for t in tasks], return_exceptions=True)

            # 处理任务结果
            for meta, task_result in zip(task_meta, task_results):
                model_name = meta['model']
                display_name = meta['display']

                if isinstance(task_result, Exception):
                    error_msg = str(task_result)
                    logger.warning(f"模型 {model_name} ({display_name}) 深度报告生成异常: {error_msg}")
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': error_msg
                    })
                    continue

                if task_result.get('success'):
                    model_reports.append(task_result)
                else:
                    failures.append({
                        'model': model_name,
                        'model_display': display_name,
                        'error': task_result.get('error', '报告生成失败')
                    })

            # 构建最终结果
            overall_success = len(model_reports) > 0
            result = {
                'success': overall_success,
                'items_analyzed': len(enriched_posts) if overall_success else 0,
                'model_reports': model_reports,
                'failures': failures,
                'report_type': 'deep'
            }

            self._log_task_complete(
                "深度报告生成",
                len(model_reports),
                len(failures),
                models=len(models_to_generate)
            )

            return result

        except Exception as e:
            logger.error(f"生成深度报告时发生异常: {e}", exc_info=True)
            return self._create_error_response(f'生成异常: {str(e)}')

    async def _generate_deep_report_for_model(
        self,
        *,
        model_name: str,
        display_name: str,
        enriched_posts: List[Dict[str, Any]],
        context_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """生成深度报告"""
        return await asyncio.to_thread(
            self._generate_deep_report_for_model_sync,
            model_name,
            display_name,
            enriched_posts,
            context_md,
            sources,
            prompt,
            start_time,
            end_time
        )

    def _generate_deep_report_for_model_sync(
        self,
        model_name: str,
        display_name: str,
        enriched_posts: List[Dict[str, Any]],
        context_md: str,
        sources: List[Dict[str, Any]],
        prompt: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """同步执行深度报告生成和Notion推送"""

        logger.info(f"[{display_name}] 开始生成深度报告")

        # 针对本次分析的帖子数自适应计算最低正文长度（180条推文时约3500字，防止半截截断）
        min_report_length = min(3500, max(800, len(enriched_posts) * 20))

        # 调用LLM生成报告
        try:
            response = self.llm_client.call_smart_model(
                prompt,
                model_override=model_name,
                temperature=0.4,
                min_content_length=min_report_length
            )

            if not response or not response.get('success'):
                error_msg = f"LLM调用失败: {response.get('error') if response else 'Unknown error'}"
                logger.warning(f"[{display_name}] {error_msg}")
                return {
                    'success': False,
                    'error': error_msg,
                    'model': model_name,
                    'model_display': display_name
                }

            llm_output = response.get('content', '')
        except Exception as e:
            error_msg = f"LLM调用异常: {str(e)}"
            logger.error(f"[{display_name}] {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'model': model_name,
                'model_display': display_name
            }

        # 为LLM生成的报告添加标准头部信息
        beijing_time = self._bj_time()
        start_bj = self._to_bj_time(start_time)
        end_bj = self._to_bj_time(end_time)
        header_info = [
            f"# 📊 X/Twitter 技术情报深度报告 - {display_name}",
            "",
            f"*报告生成时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*数据范围: {start_bj.strftime('%Y-%m-%d %H:%M:%S')} - {end_bj.strftime('%Y-%m-%d %H:%M:%S')}*  ",
            "",
            f"*分析动态数: {len(enriched_posts)} 条*",
            "",
            "---",
            ""
        ]

        # 清理LLM输出
        cleaned_llm_output = self._clean_llm_output_for_notion(llm_output)

        sources_section = self._render_sources_section(sources)

        # 构建报告尾部
        footer_lines = ["", "---", ""]
        provider = response.get('provider')
        model = response.get('model')
        if provider:
            footer_lines.append(f"*分析引擎: {provider} ({model or 'unknown'})*")

        footer_lines.extend([
            "",
            f"📊 **统计摘要**: 本报告分析了 {len(enriched_posts)} 条动态",
            "",
            "*本报告由AI自动生成，仅供参考*"
        ])
        footer_section = "\n".join(footer_lines)

        report_content = "\n".join(header_info) + cleaned_llm_output + "\n\n" + sources_section + footer_section

        # 应用来源链接增强
        report_content = self._enhance_source_links(report_content, sources)

        title = f"X技术情报深度报告 - {display_name} - {end_bj.strftime('%Y-%m-%d %H:%M')}"

        # 保存报告到数据库
        try:
            if self.db_manager.save_intelligence_report(
                'daily_deep',
                title,
                report_content,
                start_time,
                end_time
            ):
                logger.info(f"[{display_name}] 深度报告已成功保存到数据库")
            else:
                logger.warning(f"[{display_name}] 深度报告保存到数据库失败")
        except Exception as e:
            logger.error(f"[{display_name}] 保存深度报告到数据库时发生异常: {e}")

        model_report = {
            'model': model_name,
            'model_display': display_name,
            'success': True,
            'report_title': title,
            'report_content': report_content,
            'provider': response.get('provider') if response else None,
            'items_analyzed': len(enriched_posts)
        }

        # 尝试推送到Notion（使用层级结构）
        notion_push_info = None
        try:
            beijing_time = self._bj_time()
            time_str = beijing_time.strftime('%H:%M')
            notion_title = f"[{time_str}] [{display_name}] X技术深度报告 ({len(enriched_posts)}条)"

            logger.info(f"开始推送深度报告到Notion ({display_name}): {notion_title}")

            notion_result = x_intelligence_notion_client.create_report_page_in_hierarchy(
                report_title=notion_title,
                report_content=report_content,
                report_date=beijing_time,
                report_type='deep'
            )

            if notion_result.get('success'):
                logger.info(f"深度报告成功推送到Notion ({display_name}): {notion_result.get('page_url')}")
                notion_push_info = {
                    'success': True,
                    'page_url': notion_result.get('page_url'),
                    'path': notion_result.get('path')
                }
            else:
                error_msg = notion_result.get('error', '未知错误')
                logger.warning(f"推送深度报告到Notion失败 ({display_name}): {error_msg}")
                notion_push_info = {
                    'success': False,
                    'error': error_msg
                }

        except Exception as e:
            logger.warning(f"推送深度报告到Notion时出错 ({display_name}): {e}")
            notion_push_info = {
                'success': False,
                'error': str(e)
            }

        if notion_push_info:
            model_report['notion_push'] = notion_push_info

        return model_report

    async def run_dual_report_generation(self, hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
        """
        运行双轨制报告生成流程
        先生成所有日报资讯，再生成深度报告

        Args:
            hours: 时间范围（小时）
            limit: 最大帖子数量
            candidate_multiplier: 候选池倍数

        Returns:
            综合结果
        """
        logger.info(f"开始执行双轨制报告生成流程: hours={hours}, limit={limit}, multiplier={candidate_multiplier}")

        dual_results = {
            'success': True,
            'light_reports': None,
            'deep_report': None
        }

        # 第一步: 生成日报资讯
        logger.info("=== 第一步: 生成日报资讯 ===")
        light_result = await self.generate_light_reports(hours, limit, candidate_multiplier)
        dual_results['light_reports'] = light_result

        if not light_result.get('success'):
            logger.warning("日报资讯生成失败，但继续执行深度报告")
            dual_results['success'] = False
        else:
            logger.info(f"日报资讯生成完成: {len(light_result.get('model_reports', []))} 个模型成功")

        # 第二步: 生成深度报告
        logger.info("=== 第二步: 生成深度报告 ===")
        deep_result = await self.generate_deep_report(hours, limit, candidate_multiplier)
        dual_results['deep_report'] = deep_result

        if not deep_result.get('success'):
            logger.error("深度报告生成失败")
            dual_results['success'] = False
        else:
            logger.info("深度报告生成完成")

        # 汇总统计
        total_items = 0
        if light_result.get('success'):
            total_items = light_result.get('items_analyzed', 0)
        elif deep_result.get('success'):
            total_items = deep_result.get('items_analyzed', 0)

        dual_results['items_analyzed'] = total_items
        dual_results['message'] = f"双轨制报告生成{'成功' if dual_results['success'] else '部分失败'}"

        logger.info(f"双轨制报告生成流程完成: {dual_results['message']}")

        return dual_results

    def generate_kol_report(self, user_id: int, days: int = 30) -> Dict[str, Any]:
        """
        生成KOL思想轨迹报告

        Args:
            user_id: 用户ID
            days: 分析天数

        Returns:
            生成结果
        """
        logger.info(f"开始生成KOL报告，用户ID: {user_id}，天数: {days}")

        try:
            # 获取用户信息
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT user_id FROM twitter_users WHERE id = %s", (user_id,))
                result = cursor.fetchone()
                if not result:
                    return {'success': False, 'error': '用户不存在'}
                user_handle = result[0]

            # 获取用户档案
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT profile_data FROM twitter_user_profiles WHERE user_table_id = %s",
                    (user_id,)
                )
                profile_result = cursor.fetchone()
                if not profile_result:
                    return {'success': False, 'error': '用户档案不存在'}

                user_profile_json = profile_result[0]

            # 获取用户的富化帖子数据
            enriched_posts = self.db_manager.get_user_enriched_posts(user_id, days)

            if not enriched_posts:
                return {'success': False, 'error': '没有可用的帖子数据'}

            # 格式化用户帖子合集
            user_posts_collection = self._format_user_posts_for_kol_report(enriched_posts)

            # 构建KOL报告提示词
            kol_prompt = self.get_kol_report_prompt(user_profile_json, user_posts_collection, user_handle)

            # 调用Smart LLM生成报告
            response = self.llm_client.call_smart_model(kol_prompt, temperature=0.3)

            if not response['success']:
                return {'success': False, 'error': response.get('error')}

            report_content = response['content']
            report_title = f"@{user_handle} 思想轨迹月度报告 - {self._bj_time().strftime('%Y-%m-%d')}"

            # 保存报告
            if self.db_manager.save_intelligence_report(
                'monthly_kol',
                report_title,
                report_content,
                related_user_id=user_id
            ):
                return {
                    'success': True,
                    'report_title': report_title,
                    'report_content': report_content,
                    'user_handle': user_handle
                }
            else:
                return {'success': False, 'error': '报告保存失败', 'report_content': report_content}

        except Exception as e:
            logger.error(f"生成KOL报告时发生异常: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _format_user_posts_for_kol_report(self, posts: List[Dict[str, Any]]) -> str:
        """为KOL报告格式化用户帖子数据"""
        formatted_posts = []
        for i, post in enumerate(posts, 1):
            published_at = post.get('published_at')
            time_str = published_at.strftime('%Y-%m-%d') if published_at else '未知日期'

            post_info = f"[T_{i}] [{time_str}] [{post.get('content_type', '未知类型')}] [{post.get('post_tag', '无标签')}] {post.get('post_content', '')}"
            formatted_posts.append(post_info)

        return '\n'.join(formatted_posts)

    def get_kol_report_prompt(self, user_profile_json: str, user_posts_collection: str, user_handle: str) -> str:
        """构建KOL报告提示词"""
        return f"""# Role: 资深人物分析师与传记作家

# Context:
你正在为一位重要的技术领袖撰写一份私密的月度思想纪要。你的任务是通读他/她本月发布的所有帖子及其数字档案，梳理出其思想脉络、关注点变化和核心洞察。

# Core Principles:
1.  **洞察其变 (Perceive the Change)**: 你的核心是发现"变化"。他/她的关注点从哪里转移到了哪里？对某个问题的看法是否发生了改变？
2.  **抓住精髓 (Capture the Essence)**: 不要流水账。你需要提炼出他/她本月最闪光的、最具代表性的观点和分享。
3.  **客观中立 (Stay Objective)**: 你的分析应基于原文，避免过度解读和主观臆断。

# Input Data:
1.  **用户数字档案**:
    '''
    {user_profile_json}
    '''
2.  **本月言论合集**:
    '''
    {user_posts_collection}
    '''

# Your Task:
请严格按照以下结构，生成一份关于 @{user_handle} 的月度思想轨迹报告。

## 1. 本月核心关注点
*   **领域A**: [描述...]
*   **领域B**: [描述...]

---

## 2. 关键观点与立场演变
### 2.1 本月金句
> [引用的"金句"]
*   **解读**: [你对此句话的解读...]
### 2.2 立场分析 (可选)
*   关于"[话题]"的观点，从[旧观点]演变为[新观点]，主要体现在...

---

## 3. 高价值分享与网络互动
### 3.1 高价值分享
*   **[项目/文章A]**: [价值说明] [Source: T_n]
*   **[项目/文章B]**: [价值说明] [Source: T_m]
### 3.2 核心互动
*   本月与 `@user_handle` 的关于 [话题] 的讨论值得关注，揭示了...

---

## 4. 思想轨迹总结
[总结内容...]"""


def run_daily_intelligence_report(hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
    """
    便捷函数：运行日度情报报告生成（保留兼容性，使用原有的多模型并行方式）

    Args:
        hours: 时间范围（小时）
        limit: 最大帖子数量
        candidate_multiplier: 候选池倍数

    Returns:
        生成结果
    """
    generator = IntelligenceReportGenerator()
    return asyncio.run(generator.generate_intelligence_report(hours, limit, candidate_multiplier))


def run_light_reports(hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
    """
    便捷函数：运行日报资讯生成

    Args:
        hours: 时间范围（小时）
        limit: 最大帖子数量
        candidate_multiplier: 候选池倍数

    Returns:
        生成结果
    """
    generator = IntelligenceReportGenerator()
    return asyncio.run(generator.generate_light_reports(hours, limit, candidate_multiplier))


def run_deep_report(hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
    """
    便捷函数：运行深度报告生成

    Args:
        hours: 时间范围（小时）
        limit: 最大帖子数量
        candidate_multiplier: 候选池倍数

    Returns:
        生成结果
    """
    generator = IntelligenceReportGenerator()
    return asyncio.run(generator.generate_deep_report(hours, limit, candidate_multiplier))


def run_dual_reports(hours: int = 24, limit: int = 300, candidate_multiplier: Optional[float] = None) -> Dict[str, Any]:
    """
    便捷函数：运行双轨制报告生成（先日报资讯，后深度报告）

    Args:
        hours: 时间范围（小时）
        limit: 最大帖子数量
        candidate_multiplier: 候选池倍数

    Returns:
        生成结果
    """
    generator = IntelligenceReportGenerator()
    return asyncio.run(generator.run_dual_report_generation(hours, limit, candidate_multiplier))


def run_kol_report(user_id: int, days: int = 30) -> Dict[str, Any]:
    """
    便捷函数：运行KOL报告生成

    Args:
        user_id: 用户ID
        days: 分析天数

    Returns:
        生成结果
    """
    generator = IntelligenceReportGenerator()
    return generator.generate_kol_report(user_id, days)

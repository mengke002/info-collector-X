"""
情报报告生成器 (Report Synthesis)
基于富化后的帖子数据，生成高质量的情报分析报告
"""
import logging
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import re

from .database import DatabaseManager
from .llm_client import get_llm_client
from .config import config

logger = logging.getLogger(__name__)


class IntelligenceReportGenerator:
    """情报报告生成器"""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """初始化生成器"""
        if db_manager is not None:
            self.db_manager = db_manager
        else:
            self.db_manager = DatabaseManager(config)

        self.llm_client = get_llm_client()
        if not self.llm_client:
            raise RuntimeError("LLM客户端初始化失败，无法生成情报报告")

        logger.info("情报报告生成器初始化完成")

    def format_context_for_smart_llm(self, enriched_posts: List[Dict[str, Any]]) -> str:
        """
        为Smart LLM格式化输入上下文

        Args:
            enriched_posts: 富化后的帖子数据列表

        Returns:
            格式化后的上下文字符串
        """
        context_parts = []

        for i, post_data in enumerate(enriched_posts, 1):
            # 处理媒体链接
            media_urls = post_data.get('media_urls')
            if media_urls and media_urls != 'null':
                try:
                    if isinstance(media_urls, str):
                        media_list = json.loads(media_urls)
                    else:
                        media_list = media_urls
                    media_str = ", ".join(media_list) if media_list else "无"
                except (json.JSONDecodeError, TypeError):
                    media_str = "无"
            else:
                media_str = "无"

            # 处理价值资产
            value_assets = post_data.get('value_assets')
            if value_assets and value_assets != 'null':
                try:
                    if isinstance(value_assets, str):
                        asset_list = json.loads(value_assets)
                    else:
                        asset_list = value_assets
                    assets_str = ", ".join([f"{asset.get('asset_type', '链接')}: {asset.get('asset_url', '')}"
                                          for asset in asset_list if asset and asset.get('asset_url')])
                    if not assets_str:
                        assets_str = "无"
                except (json.JSONDecodeError, TypeError):
                    assets_str = "无"
            else:
                assets_str = "无"

            # 处理提及实体
            mentioned_entities = post_data.get('mentioned_entities')
            if mentioned_entities and mentioned_entities != 'null':
                try:
                    if isinstance(mentioned_entities, str):
                        entities_list = json.loads(mentioned_entities)
                    else:
                        entities_list = mentioned_entities
                    entities_str = ", ".join([f"{entity.get('entity_name')} ({entity.get('entity_type')})"
                                            for entity in entities_list if entity and entity.get('entity_name')])
                    if not entities_str:
                        entities_str = "无"
                except (json.JSONDecodeError, TypeError):
                    entities_str = "无"
            else:
                entities_str = "无"

            # 构建单个帖子的信息块
            block = f"""
[Source: T{i} | User: @{post_data.get('user_id', 'unknown')}]
- 结构类型: {post_data.get('post_type', '未知')}
- 内容类型: {post_data.get('content_type', '未知')}
- 内容标签: {post_data.get('post_tag', '无标签')}
- 提及实体: {entities_str}
- LLM摘要: {post_data.get('llm_summary', '无摘要')}
- 价值链接: {assets_str}
- 媒体内容: {media_str}
- 原始内容:
'''
{post_data.get('post_content', '')[:500]}...
'''
"""
            context_parts.append(block)

        return "\n---\n".join(context_parts)

    def get_intelligence_report_prompt(self, formatted_context: str, time_range: str) -> str:
        """
        构建情报分析报告的提示词

        Args:
            formatted_context: 格式化的上下文
            time_range: 时间范围描述

        Returns:
            完整的提示词
        """
        return f"""# Role: 世界级技术与投资情报分析师兼《经济学人》资深编辑

# Context:
你正在为一份顶级内参撰写报告，读者是全球头部的技术专家、创业者和风险投资人。他们时间宝贵，极度关注"信号"，厌恶"噪音"。你收到的原始材料是{time_range}内，由我们精心筛选的约300位全球技术思想领袖在X/Twitter上发布的帖子。这些材料已经过初步的LLM富化处理。

# Core Principles:
1.  **信号优先 (Signal First)**: 你的首要任务是识别出预示未来的微弱但关键的信号。
2.  **深度合成 (Deep Synthesis)**: 不要简单罗列。你需要将不同来源的信息点连接起来，构建成有意义的叙事（Narrative）。
3.  **注入洞见 (Inject Insight)**: 你不是一个总结者，而是一个分析师。在陈述事实和观点的基础上，**必须**加入你自己的、基于上下文的、有深度的分析和评论。
4.  **绝对可追溯 (Absolute Traceability)**: 你的每一条洞察、判断和建议，都必须在句末使用 `[Source: T_n]` 或 `[Sources: T_n, T_m]` 的格式明确标注信息来源。

# Input Data Format:
你将收到一系列格式化的帖子信息块，结构如下：
`[Source: T_id | User: user_handle]`
`- 结构类型: [Original/Reply/...]`
`- 内容类型: [教程/指南/...]`
`- 内容标签: [技术讨论/...]`
`- 提及实体: [实体名 (类型), ...]`
`- LLM摘要: [AI生成的单句摘要]`
`- 价值链接: [GitHub Repo: http://...]`
`- 原始内容:`
`'''[帖子的完整Markdown内容]'''`

# Your Task:
请严格按照以下五个层次的分析框架，生成一份信息密度极高、洞察深刻的Markdown情报报告。

**第一层次：动态与热点概览 (Dynamics & Hotspot Overview)**
*   **1.1 动态摘要**: 写一个不超过150字的"执行摘要"，总结周期内最重要的动态和最关键的信号。
*   **1.2 核心话题**: 识别出2-3个最重要的核心话题。对每个话题，用一句话总结其核心议题，并列出2-3个最具代表性的观点。

**第二层次：观点对撞圆桌 (Perspectives Collision Round-table)**
*   任务：围绕本周期内一个最具争议性或多面性的话题，组织一场虚拟圆桌讨论。
*   要求：
    1.  **设定议题**: 明确本场圆桌的核心议题。
    2.  **邀请嘉宾**: 从数据中挑选持有不同（甚至对立）观点的用户作为"虚拟嘉宾"。
    3.  **呈现观点**: 清晰地展示每位嘉宾的核心论点，并直接引用其原文精华。
    4.  **分析师点评 (关键！)**: 在所有观点陈述完毕后，**加入你自己的分析师点评**。点评内容可以包括：指出各方观点的盲区、点明争议的本质、预测该议题的未来走向、或者提出一个更高维度的综合性看法。

**第三层次：趋势与叙事深度分析 (Trend & Narrative Analysis)**
*   **3.1 新兴趋势/信号**: 识别1-2个讨论度快速上升的"新兴趋势"或"微弱信号"。详细描述它是什么，为什么它现在出现，以及它可能对行业产生什么影响。
*   **3.2 宏大叙事**: 寻找不同话题之间的内在联系，构建一个或多个宏大叙事。例如，将"新AI模型的发布"、"开源社区的讨论"和"下游应用的探索"联系起来，形成一个关于"XXX技术从理论到实践的演进路径"的完整叙事。

**第四层次：精选资源库 (Curated Resource Library)**
*   任务：从本周期所有分享的链接中，精选出最有价值的2-3个资源。
*   要求：
    *   **4.1 教程与指南**: 挑选出最有价值的教程、指南或深度学习笔记。
    *   **4.2 工具与项目**: 挑选出最值得关注的新工具或开源项目。
    *   对每个入选的资源，用一句话说明其核心价值和推荐理由。

**第五层次：角色化行动建议 (Role-Based Actionable Recommendations)**
*   任务：将所有分析转化为对特定角色的、可立即执行的建议。
*   要求：建议必须具体、新颖且具有前瞻性。
    *   **给开发者的建议**: [例如：建议立即研究 `XXX` 框架，因为它在解决 `YYY` 问题上表现出巨大潜力。] [Source: T_n]
    *   **给产品经理/创业者的建议**: [例如：社区对 `ZZZ` 场景的需求反复出现，但现有解决方案均有缺陷，这可能是一个被忽视的蓝海市场。] [Source: T_m]
    *   **给投资者的建议**: [例如：`AAA` 领域的讨论热度与技术成熟度出现"共振"，可能预示着商业化拐点即将到来。] [Source: T_k]

# Output Format (Strictly follow this Markdown structure):

## 一、动态与热点概览
### 1.1 动态摘要
[执行摘要内容]
### 1.2 核心话题
*   **话题A**: [总结]
    *   观点1: [内容] [Source: T_n]
    *   观点2: [内容] [Source: T_m]
*   **话题B**: ...

---

## 二、观点对撞圆桌：[议题名称]
### 嘉宾观点
*   **正方代表 (`@user_handle_1`)**: [观点陈述] [Source: T_a]
*   **反方代表 (`@user_handle_2`)**: [观点陈述] [Source: T_b]
*   **中立/技术派 (`@user_handle_3`)**: [观点陈述] [Source: T_c]
### 分析师点评
[你对这场辩论的总结、洞察和更高维度的分析...]

---

## 三、趋势与叙事分析
### 3.1 新兴趋势：[趋势名称]
[详细描述该趋势...] [Sources: T_d, T_e]
### 3.2 宏大叙事：[叙事名称]
[详细描述该叙事...] [Sources: T_f, T_g]

---

## 四、精选资源库
### 4.1 教程与指南
*   **[资源名称]**: [推荐理由] [Source: T_h]
### 4.2 工具与项目
*   **[资源名称]**: [推荐理由] [Source: T_i]

---

## 五、角色化行动建议
*   **To 开发者**: [建议内容] [Source: T_j]
*   **To 产品经理/创业者**: [建议内容] [Source: T_k]
*   **To 投资者/研究者**: [建议内容] [Source: T_l]

# Input Data:
{formatted_context}"""

    def generate_intelligence_report(self, hours: int = 24, limit: int = 300) -> Dict[str, Any]:
        """
        生成情报分析报告

        Args:
            hours: 时间范围（小时）
            limit: 最大帖子数量

        Returns:
            生成结果
        """
        logger.info(f"开始生成情报报告，时间范围: {hours}小时，最大帖子数: {limit}")

        try:
            # 计算时间范围
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)

            # 获取富化后的帖子数据
            enriched_posts = self.db_manager.get_enriched_posts_for_report(
                start_time, end_time, limit
            )

            if not enriched_posts:
                logger.warning(f"在指定时间范围内没有找到富化的帖子数据")
                return {
                    'success': False,
                    'error': '没有可用的帖子数据',
                    'posts_count': 0
                }

            logger.info(f"获取到 {len(enriched_posts)} 条富化帖子数据")

            # 格式化上下文
            formatted_context = self.format_context_for_smart_llm(enriched_posts)

            # 构建提示词
            time_range_str = f"过去{hours}小时"
            prompt = self.get_intelligence_report_prompt(formatted_context, time_range_str)

            logger.info(f"提示词长度: {len(prompt)} 字符")

            # 调用Smart LLM生成报告
            response = self.llm_client.call_smart_model(prompt, temperature=0.4)

            if not response['success']:
                logger.error(f"LLM调用失败: {response.get('error')}")
                return {
                    'success': False,
                    'error': response.get('error'),
                    'posts_count': len(enriched_posts)
                }

            report_content = response['content']
            logger.info(f"生成的报告长度: {len(report_content)} 字符")

            # 生成报告标题
            report_title = f"X/Twitter 技术情报日报 - {end_time.strftime('%Y-%m-%d %H:%M')}"

            # 保存报告到数据库
            if self.db_manager.save_intelligence_report(
                'daily',
                report_title,
                report_content,
                start_time,
                end_time
            ):
                logger.info("情报报告已成功保存到数据库")
                return {
                    'success': True,
                    'report_title': report_title,
                    'report_content': report_content,
                    'posts_count': len(enriched_posts),
                    'time_range': f"{start_time.strftime('%Y-%m-%d %H:%M')} - {end_time.strftime('%Y-%m-%d %H:%M')}"
                }
            else:
                logger.error("报告保存到数据库失败")
                return {
                    'success': False,
                    'error': '报告保存失败',
                    'report_content': report_content,
                    'posts_count': len(enriched_posts)
                }

        except Exception as e:
            logger.error(f"生成情报报告时发生异常: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'posts_count': 0
            }

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
            report_title = f"@{user_handle} 思想轨迹月度报告 - {datetime.now().strftime('%Y-%m-%d')}"

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


def run_daily_intelligence_report(hours: int = 24, limit: int = 300) -> Dict[str, Any]:
    """
    便捷函数：运行日度情报报告生成

    Args:
        hours: 时间范围（小时）
        limit: 最大帖子数量

    Returns:
        生成结果
    """
    generator = IntelligenceReportGenerator()
    return generator.generate_intelligence_report(hours, limit)


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
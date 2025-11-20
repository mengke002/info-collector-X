"""
评分模块 (Scoring Module)
用于计算推文的动态价值评分 (Dynamic Value Score)
"""
import logging
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def calculate_value_score(post: Dict[str, Any], config: Dict[str, Any]) -> float:
    """
    计算单条推文的价值评分

    评分公式:
    Score = Base
          + (ContentTypeScore * Weight)
          + (TagScore * Weight)
          + (PostLength * LengthWeight)
          + (InterpretationLength * InterpWeight)
          + MediaBonus
          + LinkBonus

    Args:
        post: 推文数据字典
        config: 评分配置字典

    Returns:
        float: 计算出的价值分
    """
    score = float(config.get('base_score', 1.0))

    # 1. 内容类型评分 (Content Type)
    content_type = post.get('content_type', '未分类')
    type_scores = config.get('content_type_scores', {})
    # 如果是JSON字符串，尝试解析（虽然config.py应该已经处理好了，这里做个防御）
    if isinstance(type_scores, str):
        try:
            type_scores = json.loads(type_scores)
        except:
            type_scores = {}

    if content_type in type_scores:
        score += float(type_scores[content_type])

    # 2. 内容标签评分 (Post Tag)
    # post_tag 可能是单个字符串，也可能是逗号分隔的字符串，这里简化处理为单个主标签
    tag = post.get('post_tag')
    tag_scores = config.get('tag_scores', {})
    if isinstance(tag_scores, str):
        try:
            tag_scores = json.loads(tag_scores)
        except:
            tag_scores = {}

    if tag and tag in tag_scores:
        score += float(tag_scores[tag])

    # 3. 内容长度 (Post Content Length)
    content = post.get('post_content', '') or ''
    length_weight = float(config.get('post_length_weight', 0.0))
    score += len(content) * length_weight

    # 4. 深度解读长度 (Interpretation Length)
    # 如果没有深度解读，长度视为0
    interpretation = post.get('deep_interpretation', '') or ''
    interp_weight = float(config.get('interpretation_length_weight', 0.0))
    score += len(interpretation) * interp_weight

    # 5. 媒体加分 (Media Bonus)
    # 检查是否有媒体 (has_media 字段 或 media_urls 字段)
    has_media = False
    if post.get('has_media'):
        try:
            has_media = bool(int(post.get('has_media')))
        except:
            pass
    elif post.get('media_urls'):
        # 简单的非空检查
        urls = post.get('media_urls')
        if isinstance(urls, str) and urls != '[]' and urls != 'null':
            has_media = True
        elif isinstance(urls, list) and len(urls) > 0:
            has_media = True

    if has_media:
        score += float(config.get('media_bonus', 0.0))

    # 6. 链接加分 (Link Bonus)
    # 简单的 heuristic: 检查 post_type 是否为 LinkShare 或内容中是否包含 http
    is_link = False
    if post.get('post_type') == 'LinkShare':
        is_link = True
    elif 'http' in content:
        is_link = True

    if is_link:
        score += float(config.get('link_bonus', 0.0))

    return round(score, 4)

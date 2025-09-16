"""
RSS 数据采集模块
"""
import logging
import requests
import feedparser
import json
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin
from markdownify import markdownify
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class RSSCrawler:
    """RSS数据采集器"""

    def __init__(self, config=None):
        """初始化RSS采集器

        Args:
            config: 配置对象
        """
        if config is None:
            from .config import config as default_config
            config = default_config

        self.config = config
        self.crawler_config = config.get_crawler_config()

        # 设置请求会话
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        })

        # 添加Bearer Token认证头（如果配置了）
        bearer_token = self.crawler_config.get('bearer_token')
        if bearer_token:
            self.session.headers.update({
                'Authorization': f'Bearer {bearer_token}'
            })

    def crawl_user_posts(self, user_id: str) -> Optional[List[Dict[str, Any]]]:
        """爬取指定用户的帖子数据

        Args:
            user_id: Twitter用户ID（不含@符号）

        Returns:
            帖子数据列表，失败时返回None
        """
        try:
            # 构建RSS URL
            base_url = self.crawler_config['rss_hub_url']
            rss_url = urljoin(base_url, f'/twitter/user/{user_id}')

            logger.info(f"开始爬取用户 {user_id} 的RSS数据: {rss_url}")

            # 发送HTTP请求
            response = self.session.get(
                rss_url,
                timeout=self.crawler_config['request_timeout']
            )
            response.raise_for_status()

            # 解析RSS内容
            feed = feedparser.parse(response.content)

            if feed.bozo:
                logger.warning(f"RSS解析发现警告，用户 {user_id}: {feed.bozo_exception}")

            posts = []
            for entry in feed.entries:
                post_data = self._parse_rss_entry(entry)
                if post_data:
                    posts.append(post_data)

            logger.info(f"成功解析用户 {user_id} 的 {len(posts)} 条帖子")
            return posts

        except requests.exceptions.RequestException as e:
            logger.error(f"爬取用户 {user_id} 时HTTP请求失败: {e}")
            return None
        except Exception as e:
            logger.error(f"爬取用户 {user_id} 时发生未知错误: {e}")
            return None

    def _parse_rss_entry(self, entry) -> Optional[Dict[str, Any]]:
        """解析单个RSS条目

        Args:
            entry: feedparser解析的RSS条目

        Returns:
            解析后的帖子数据字典
        """
        try:
            # 提取基本信息
            post_url = entry.get('link', '')
            title = entry.get('title', '')
            description = entry.get('description', '')
            published = entry.get('published_parsed')

            if not post_url:
                logger.warning("RSS条目缺少link字段，跳过")
                return None

            # 解析发布时间
            published_at = None
            if published:
                published_at = datetime(*published[:6], tzinfo=timezone.utc)

            # 解析HTML描述内容
            post_content, media_urls = self._parse_description_html(description)

            # 判断帖子类型
            post_type = self._determine_post_type(post_content, title)

            return {
                'post_url': post_url,
                'post_content': post_content,
                'post_type': post_type,
                'media_urls': json.dumps(media_urls) if media_urls else None,
                'published_at': published_at,
            }

        except Exception as e:
            logger.error(f"解析RSS条目失败: {e}")
            return None

    def _parse_description_html(self, html_content: str) -> tuple[str, List[str]]:
        """解析RSS描述中的HTML内容

        Args:
            html_content: HTML内容字符串

        Returns:
            (markdown格式的内容, 媒体URL列表)
        """
        if not html_content:
            return '', []

        try:
            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # 提取媒体链接
            media_urls = []

            # 提取图片链接
            for img in soup.find_all('img'):
                src = img.get('src')
                if src and self._is_valid_media_url(src):
                    media_urls.append(src)

            # 提取视频链接（如果有）
            for video in soup.find_all('video'):
                src = video.get('src')
                if src and self._is_valid_media_url(src):
                    media_urls.append(src)

            # 转换为Markdown格式
            markdown_content = markdownify(html_content, heading_style="ATX")

            # 清理Markdown内容
            markdown_content = self._clean_markdown(markdown_content)

            return markdown_content.strip(), media_urls

        except Exception as e:
            logger.error(f"解析HTML描述失败: {e}")
            # 如果解析失败，返回原始内容（去除HTML标签）
            text_content = BeautifulSoup(html_content, 'html.parser').get_text()
            return text_content.strip(), []

    def _is_valid_media_url(self, url: str) -> bool:
        """检查是否为有效的媒体URL

        Args:
            url: 要检查的URL

        Returns:
            是否为有效媒体URL
        """
        if not url:
            return False

        # 检查是否为图片或视频URL
        media_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.mp4', '.mov', '.avi'}
        url_lower = url.lower()

        # 检查文件扩展名
        for ext in media_extensions:
            if ext in url_lower:
                return True

        # 检查是否为常见媒体服务的URL
        media_domains = ['pbs.twimg.com', 'video.twimg.com', 'abs.twimg.com']
        for domain in media_domains:
            if domain in url_lower:
                return True

        return False

    def _clean_markdown(self, markdown: str) -> str:
        """清理Markdown内容

        Args:
            markdown: 原始Markdown内容

        Returns:
            清理后的Markdown内容
        """
        if not markdown:
            return ''

        # 移除多余的空行
        markdown = re.sub(r'\n\s*\n\s*\n', '\n\n', markdown)

        # 移除行首尾空白
        lines = [line.strip() for line in markdown.split('\n')]
        markdown = '\n'.join(lines)

        # 移除引用块中的多余标记
        markdown = re.sub(r'<div class="rsshub-quote">\s*<br>\s*<br>', '\n\n> ', markdown)
        markdown = re.sub(r'<br>\s*</div>', '', markdown)
        markdown = re.sub(r'<br>', '\n', markdown)

        # 清理残留的HTML标签
        markdown = re.sub(r'<[^>]+>', '', markdown)

        return markdown.strip()

    def _determine_post_type(self, content: str, title: str) -> str:
        """判断帖子类型

        Args:
            content: 帖子内容
            title: 帖子标题

        Returns:
            帖子类型 (Original/Reply/Quote/LinkShare)
        """
        if not content and not title:
            return 'Original'

        full_text = f"{title} {content}".lower()

        # 检查是否为回复
        if full_text.startswith('@') or '回复' in full_text:
            return 'Reply'

        # 检查是否为引用转发
        if '> ' in content or 'quote' in full_text or '引用' in full_text:
            return 'Quote'

        # 检查是否为链接分享
        url_pattern = r'https?://[^\s]+'
        if re.search(url_pattern, full_text):
            # 计算链接字符数占总字符数的比例
            urls = re.findall(url_pattern, full_text)
            url_chars = sum(len(url) for url in urls)
            total_chars = len(full_text)

            if url_chars / total_chars > 0.3:  # 如果链接字符占比超过30%
                return 'LinkShare'

        return 'Original'

    def test_connection(self, test_user: str = 'yaohui12138') -> bool:
        """测试RSS Hub连接

        Args:
            test_user: 用于测试的用户名

        Returns:
            连接是否成功
        """
        try:
            posts = self.crawl_user_posts(test_user)
            if posts is not None:
                logger.info(f"RSS Hub连接测试成功，获取到 {len(posts)} 条测试数据")
                return True
            else:
                logger.error("RSS Hub连接测试失败")
                return False

        except Exception as e:
            logger.error(f"RSS Hub连接测试异常: {e}")
            return False
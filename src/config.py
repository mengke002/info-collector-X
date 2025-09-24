"""
配置管理模块
支持环境变量 > config.ini > 默认值的优先级机制
"""
import os
import configparser
import logging
from typing import Dict, Any
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class Config:
    """配置管理类，支持环境变量优先级的配置加载"""

    def __init__(self, config_path: str = 'config.ini'):
        # 本地开发时可加载 .env
        try:
            load_dotenv()
        except Exception:
            pass

        self.config_parser = configparser.ConfigParser()

        # 兼容多种位置查找 config.ini
        possible_paths = [
            config_path,
            os.path.join(os.getcwd(), config_path),
            os.path.join(os.path.dirname(os.path.dirname(__file__)), config_path),
        ]

        self.config_file = None
        for p in possible_paths:
            if os.path.exists(p):
                self.config_file = p
                break

        if self.config_file:
            try:
                self.config_parser.read(self.config_file, encoding='utf-8')
                logger.info(f"已加载配置文件: {self.config_file}")
            except (configparser.Error, UnicodeDecodeError):
                logger.warning("读取配置文件失败，跳过。")
        else:
            logger.info("未发现配置文件，将仅使用环境变量与默认值。")

    def _get_config_value(self, section: str, key: str, env_var: str, default_value: Any, value_type=str) -> Any:
        """按优先级获取配置值：环境变量 > config.ini > 默认值"""
        env_val = os.getenv(env_var)
        if env_val is not None:
            try:
                return value_type(env_val)
            except (ValueError, TypeError):
                return default_value

        try:
            if self.config_parser.has_section(section) and self.config_parser.has_option(section, key):
                cfg_val = self.config_parser.get(section, key)
                try:
                    return value_type(cfg_val)
                except (ValueError, TypeError):
                    return default_value
        except (configparser.Error, UnicodeDecodeError):
            pass

        return default_value

    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置（环境变量 > config.ini > 默认值）。
        环境变量命名采用 DB_*；为兼容，密码也支持 MYSQL_PASSWORD。
        """
        # 密码兼容：优先 DB_PASSWORD，然后 MYSQL_PASSWORD
        password = os.getenv('DB_PASSWORD')
        if password is None:
            password = os.getenv('MYSQL_PASSWORD')
        if password is None:
            # 从文件读取（如果有）
            try:
                if self.config_parser.has_section('database') and self.config_parser.has_option('database', 'password'):
                    password = self.config_parser.get('database', 'password')
            except (configparser.Error, UnicodeDecodeError):
                pass

        config = {
            'host': self._get_config_value('database', 'host', 'DB_HOST', None),
            'user': self._get_config_value('database', 'user', 'DB_USER', None),
            'database': self._get_config_value('database', 'database', 'DB_NAME', None),
            'port': self._get_config_value('database', 'port', 'DB_PORT', 3306, int),
            'password': password,
            'charset': 'utf8mb4',
            'autocommit': True,
        }

        # SSL 配置
        ssl_mode = self._get_config_value('database', 'ssl_mode', 'DB_SSL_MODE', 'disabled')
        if isinstance(ssl_mode, str) and ssl_mode.upper() == 'REQUIRED':
            config['ssl'] = {'mode': 'REQUIRED'}

        # 校验必填
        required = ['host', 'user', 'database', 'password']
        missing = [k for k in required if not config.get(k)]
        if missing:
            raise ValueError(f"数据库核心配置缺失: {', '.join(missing)}。请在 GitHub Secrets 或 config.ini 中设置。")

        return config

    def get_crawler_config(self) -> Dict[str, Any]:
        """获取爬虫配置（环境变量 > config.ini > 默认值）。"""
        return {
            'rss_hub_url': self._get_config_value('crawler', 'rss_hub_url', 'RSS_HUB_URL', 'https://xman1024-info.hf.space'),
            'bearer_token': self._get_config_value('crawler', 'bearer_token', 'RSS_HUB_TOKEN', ''),
            'default_concurrent_workers': self._get_config_value('crawler', 'default_concurrent_workers', 'CRAWLER_DEFAULT_CONCURRENT_WORKERS', 2, int),
            'batch_interval_min': self._get_config_value('crawler', 'batch_interval_min', 'CRAWLER_BATCH_INTERVAL_MIN', 60, int),
            'batch_interval_max': self._get_config_value('crawler', 'batch_interval_max', 'CRAWLER_BATCH_INTERVAL_MAX', 120, int),
            'request_timeout': self._get_config_value('crawler', 'request_timeout', 'CRAWLER_REQUEST_TIMEOUT', 30, int),
            'retry_attempts': self._get_config_value('crawler', 'retry_attempts', 'CRAWLER_RETRY_ATTEMPTS', 3, int),
            'retry_delay': self._get_config_value('crawler', 'retry_delay', 'CRAWLER_RETRY_DELAY', 5, int),
        }

    def get_sleep_window_config(self) -> Dict[str, int]:
        return {
            'start_hour': self._get_config_value('sleep_window', 'start_hour', 'SLEEP_START_HOUR', 17, int),
            'end_hour': self._get_config_value('sleep_window', 'end_hour', 'SLEEP_END_HOUR', 22, int),
        }

    def get_crawl_groups_config(self) -> Dict[str, int]:
        return {
            'high_interval_minutes': self._get_config_value('crawl_groups', 'high_interval_minutes', 'CRAWL_HIGH_INTERVAL_MINUTES', 20, int),
            'medium_interval_minutes': self._get_config_value('crawl_groups', 'medium_interval_minutes', 'CRAWL_MEDIUM_INTERVAL_MINUTES', 90, int),
            'low_interval_hours': self._get_config_value('crawl_groups', 'low_interval_hours', 'CRAWL_LOW_INTERVAL_HOURS', 5, int),
        }

    def get_task_limits_config(self) -> Dict[str, int]:
        """获取每次任务处理的用户数量上限配置。"""
        return {
            'high_limit': self._get_config_value('task_limits', 'high_limit', 'CRAWL_HIGH_LIMIT', 50, int),
            'medium_limit': self._get_config_value('task_limits', 'medium_limit', 'CRAWL_MEDIUM_LIMIT', 200, int),
            'low_limit': self._get_config_value('task_limits', 'low_limit', 'CRAWL_LOW_LIMIT', 300, int),
        }

    def get_failure_handling_config(self) -> Dict[str, int]:
        return {
            'max_failed_attempts': self._get_config_value('failure_handling', 'max_failed_attempts', 'FAIL_MAX_FAILED_ATTEMPTS', 5, int),
            'retry_delay_min': self._get_config_value('failure_handling', 'retry_delay_min', 'FAIL_RETRY_DELAY_MIN', 15, int),
            'retry_delay_max': self._get_config_value('failure_handling', 'retry_delay_max', 'FAIL_RETRY_DELAY_MAX', 25, int),
        }

    def get_logging_config(self) -> Dict[str, Any]:
        return {
            'log_file': self._get_config_value('logging', 'log_file', 'LOG_FILE', 'logs/crawler.log'),
            'log_level': self._get_config_value('logging', 'log_level', 'LOG_LEVEL', 'INFO'),
            'max_bytes': self._get_config_value('logging', 'max_bytes', 'LOG_MAX_BYTES', 10485760, int),
            'backup_count': self._get_config_value('logging', 'backup_count', 'LOG_BACKUP_COUNT', 5, int),
        }

    def get_analysis_config(self) -> Dict[str, Any]:
        """获取内容解读配置（目前支持选择提示词模式）"""
        mode = self._get_config_value('analysis', 'interpretation_mode', 'INTERPRETATION_MODE', 'light', str)

        if isinstance(mode, str):
            normalized_mode = mode.strip().lower()
        else:
            normalized_mode = 'light'

        if normalized_mode not in {'light', 'heavy'}:
            logger.warning(
                "interpretation_mode 配置值 %s 无效，将回退到 light 模式",
                mode
            )
            normalized_mode = 'light'

        return {
            'interpretation_mode': normalized_mode
        }

    def get_llm_config(self) -> Dict[str, Any]:
        """获取LLM配置，优先级：环境变量 > config.ini > 默认值"""
        # API密钥优先从环境变量获取，其次从 config.ini 获取
        openai_api_key = self._get_config_value('llm', 'openai_api_key', 'OPENAI_API_KEY', None)
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY 未设置。请在环境变量或config.ini中设置LLM功能需要API密钥。")

        return {
            # 快速模型配置
            'fast_model_name': self._get_config_value('llm', 'fast_model_name', 'LLM_FAST_MODEL_NAME', 'gpt-3.5-turbo-16k'),

            # 视觉多模态模型配置
            'fast_vlm_model_name': self._get_config_value('llm', 'fast_vlm_model_name', 'LLM_FAST_VLM_NAME', 'gpt-4-vision-preview'),
            'fast_vlm_fallback_model_name': self._get_config_value('llm', 'fast_vlm_fallback_model_name', 'LLM_FAST_VLM_FALLBACK_NAME', 'gpt-4-vision-preview'),

            # 智能模型配置
            'smart_model_name': self._get_config_value('llm', 'smart_model_name', 'LLM_SMART_MODEL_NAME', 'gpt-4-turbo'),

            # API配置
            'openai_api_key': openai_api_key,
            'openai_base_url': self._get_config_value('llm', 'openai_base_url', 'OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            'max_content_length': self._get_config_value('llm', 'max_content_length', 'LLM_MAX_CONTENT_LENGTH', 100000, int),
        }

    def get_fast_model_config(self) -> Dict[str, str]:
        """获取快速模型配置"""
        llm_config = self.get_llm_config()
        return {
            'provider': 'openai',
            'model_name': llm_config['fast_model_name'],
            'api_key': llm_config['openai_api_key'],
            'base_url': llm_config['openai_base_url']
        }

    def get_smart_model_config(self) -> Dict[str, str]:
        """获取智能模型配置"""
        llm_config = self.get_llm_config()
        return {
            'provider': 'openai',
            'model_name': llm_config['smart_model_name'],
            'api_key': llm_config['openai_api_key'],
            'base_url': llm_config['openai_base_url']
        }

    def get_postprocessing_config(self) -> Dict[str, int]:
        """获取后处理并发配置"""
        return {
            'fast_llm_workers': self._get_config_value('postprocessing', 'fast_llm_workers', 'EXECUTOR_FAST_LLM_WORKERS', 8, int),
            'fast_vlm_workers': self._get_config_value('postprocessing', 'fast_vlm_workers', 'EXECUTOR_FAST_VLM_WORKERS', 8, int),
            'image_processing_workers': self._get_config_value('postprocessing', 'image_processing_workers', 'EXECUTOR_IMAGE_PROCESSING_WORKERS', 12, int),
        }


# 全局配置实例
config = Config()

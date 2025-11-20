"""
工具函数模块
"""
import os
import logging
import time
from huggingface_hub import HfApi

logger = logging.getLogger(__name__)

def restart_hf_space(space_id: str = "Xman1024/info", token: str = None):
    """重启 Hugging Face Space

    Args:
        space_id: Space ID (username/space_name)
        token: API Token (默认从环境变量 RSS_HUB_TOKEN 读取)
    """
    if token is None:
        token = os.getenv('RSS_HUB_TOKEN')

    if not token:
        logger.warning("未设置 RSS_HUB_TOKEN，无法重启 Hugging Face Space")
        return

    try:
        logger.info(f"正在尝试重启 Hugging Face Space: {space_id}")
        hf_api = HfApi(token=token)

        # 重启 Space
        hf_api.restart_space(space_id, factory_reboot=False)

        logger.info(f"Space {space_id} 重启指令已发送，等待 10 秒...")
        time.sleep(10)
        logger.info("等待结束，继续执行任务")

    except Exception as e:
        logger.error(f"重启 Hugging Face Space 失败: {e}")

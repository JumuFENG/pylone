import os
import json
import requests
import base64
from functools import lru_cache


class Ollama:
    def __init__(self, api_key=None, model="qwen3-vl:235b"):
        self.api_key = api_key
        self.model = model
        self.url = "https://ollama.com/api/chat"

    def chat(self, content, images=None):
        """
        向Ollama API发送聊天请求

        参数:
            content (str): 用户输入的内容（必填）
            images (list, optional): base64图片列表，默认为None
        返回:
            str: API响应内容
        """

        # 构建请求头
        headers = {
            "Content-Type": "application/json"
        }

        assert self.api_key is not None, "API key is required"
        headers["Authorization"] = f"Bearer {self.api_key}"

        # 构建消息内容
        message = {
            "role": "user",
            "content": content
        }

        # 如果提供了图片路径，读取并编码图片
        if images:
            message["images"] = images
        # 构建请求数据
        data = {
            "model": self.model,
            "messages": [message],
            "stream": False
        }

        try:
            # 发送POST请求
            response = requests.post(self.url, headers=headers, json=data)
            response.raise_for_status()  # 检查HTTP错误

            # 解析响应
            result = response.json()
            return result.get("message", {}).get("content", "")

        except requests.exceptions.RequestException as e:
            print(f"请求错误: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}")
            return None

    def img_to_text(self, img):
        if isinstance(img, str) and os.path.isfile(img):
            with open(img, 'rb') as f:
                img = base64.b64encode(f.read()).decode('utf-8')

        return self.chat('识别图片中的数字,直接告诉我结果', images=[img])


@lru_cache(maxsize=5)
def ollama(api_key=None, model="qwen3-vl:235b") -> Ollama:
    return Ollama(api_key, model)

from typing import Any, List, Optional
from fastapi import Request, HTTPException
from fastapi.params import Depends


class PostParams:
    """自定义参数类，同时支持 Form Data 和 JSON Body, 用在POST请求的参数解析。

    可以通过以下方式使用：
    1. 作为依赖: img: str = Depends(PostParams("img"))
    2. 作为工厂函数: img: str = PostParams.create("img")
    """

    def __init__(self, field_name: str, default: Any = ..., alias: Optional[str] = None):
        """
        Args:
            field_name: 字段名称
            default: 默认值，使用 ... 表示必填
            alias: 字段别名
        """
        self.field_name = field_name
        self.default = default
        self.alias = alias or field_name

    async def __call__(self, request: Request):
        content_type = request.headers.get("content-type", "")
        value = None

        # 处理 JSON 格式
        if "application/json" in content_type:
            try:
                data = await request.json()
                value = data.get(self.alias)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid JSON format.")
        # 处理 Form Data 格式
        elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
            try:
                form = await request.form()
                value = form.get(self.alias)
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid form data.")
        else:
            raise HTTPException(
                status_code=400,
                detail="Unsupported content type. Use application/json or form data."
            )

        # 处理默认值和必填验证
        if value is None:
            if self.default is ...:
                raise HTTPException(status_code=400, detail=f"Field '{self.field_name}' is required.")
            return self.default

        return value

    @classmethod
    def create(cls, field_name: str, default: Any = ..., alias: Optional[str] = None) -> Depends:
        """工厂方法，返回 Depends 对象，使用更简洁"""
        return Depends(cls(field_name, default, alias))

def pparam_doc(nter:List[tuple]):
    """生成 JSON/FormData 的 OpenAPI 文档"""
    properties = {}
    required = []
    for n,t,e,r in nter:
        properties[n] = {
            "type": t,
            "example": e
        }
        if r:
            required.append(n)

    return {
        "requestBody": {
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": properties,
                        "required": required
                    }
                },
                "application/x-www-form-urlencoded": {
                    "schema": {
                        "type": "object",
                        "properties": properties,
                        "required": required
                    }
                }
            }
        }
    }


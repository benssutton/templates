from pydantic import BaseModel


class RootInfo(BaseModel):
    title: str
    version: str
    description: str
    docs: str
    MCP: str

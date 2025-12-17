from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from app.lofig import Config, logger
from app.users.router import router as users_router
from app.admin.router import router as admin_router
from app.stock.router import router as stock_router
from app.api import router as api_router
from app.tasks.timer_task import Timers
from app.admin.system_settings import SystemSettings


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行
    await SystemSettings.initialize_defaults()
    await SystemSettings.get_all()
    Timers.setup()
    yield
    # 关闭时执行（如果需要清理资源）


cfg = Config.client_config()
app = FastAPI(title=cfg.get('app_name', 'pyswee'), lifespan=lifespan)

app.include_router(users_router)
app.include_router(admin_router)
app.include_router(api_router)
app.include_router(stock_router)

# 挂载静态文件
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/html", StaticFiles(directory="html", html=True), name="html")

# 保留 templates 用于服务端渲染（如果需要）
templates = Jinja2Templates(directory="templates")


@app.get("/")
async def root():
    return RedirectResponse(url="/html/index.html")

@app.get("/login.html")
async def login_redirect():
    return RedirectResponse(url="/html/login.html")

@app.get("/register.html")
async def register_redirect():
    return RedirectResponse(url="/html/register.html")

@app.get("/profile.html")
async def profile_redirect():
    return RedirectResponse(url="/html/profile.html")

@app.get("/admin.html")
async def admin_redirect():
    return RedirectResponse(url="/html/admin.html")

@app.get("/settings.html")
async def settings_redirect():
    return RedirectResponse(url="/html/settings.html")

# deprecated
@app.get("/userbind")
async def userbind_redirect():
    return RedirectResponse(url="/users/bind")


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=cfg.get('port', 8000), log_config=None, access_log=True)

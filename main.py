from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from app.lofig import Config, logger
from app.users.router import router as users_router
from app.admin.router import router as admin_router
from app.users.manager import fastapi_users


cfg = Config.client_config()
app = FastAPI(title=cfg.get('app_name', 'pyswee'))

app.include_router(users_router)
app.include_router(admin_router)

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



if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=cfg.get('port', 8000))

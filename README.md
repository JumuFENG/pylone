# phoniun交易后台

基于 FastAPI 实现的phoniun交易后台，使用 MySQL 数据库。
完整的交易系统需要配合[交易端](https://github.com/JumuFENG/pyphon)和[策略端](https://github.com/JumuFENG/pyiun)一起使用

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置文件，配置数据库和程序相关的设置

编辑 `config/config.json` 文件，进行配置：

```json
{
    "database": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "your_password",
        "database": "user_management"
    },
    "client": {
        "app_name": "pylone",
        "port": "yourport"
    }
}
```

手动创建数据库，或者执行初始化脚本：

```python
python tools/init_db.py
```

## 运行应用

```bash
python main.py
```

或者使用 uvicorn：

```bash
uvicorn main:app --reload
```

应用将在 http://localhost:{yourport}/ 启动

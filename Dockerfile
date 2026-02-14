# image: https://docker.aityp.com/image/docker.io/python:3.12-slim?platform=linux/arm64
FROM python:3.12-slim

RUN useradd -m pyuser

WORKDIR /home/pyuser/pylone

RUN chown -R pyuser:pyuser /usr/local/lib/python3.12/site-packages && \
    chown -R pyuser:pyuser /usr/local/bin && \
    chown -R pyuser:pyuser /home/pyuser/pylone

USER pyuser

# 安装Python依赖
COPY --chown=pyuser:pyuser requirements.txt requirements-extra.txt main.py ./
COPY --chown=pyuser:pyuser ./packages ./packages/
RUN pip install --upgrade pip && \
    pip install --user -r requirements.txt && \
    pip install ./packages/*.whl && \
    pip install --user -r requirements-extra.txt && \
    rm -rf ./packages/

# 复制应用代码
COPY --chown=pyuser:pyuser ./app ./app/
COPY --chown=pyuser:pyuser ./tools ./tools/
COPY --chown=pyuser:pyuser ./html ./html/
COPY --chown=pyuser:pyuser ./static ./static/

# 创建必要的目录
RUN mkdir -p data config logs

CMD ["python", "main.py"]

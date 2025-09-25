FROM python:3.12-slim


WORKDIR /app


COPY . .


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt


CMD ["python", "bot_vip_nalog_v22.py"]

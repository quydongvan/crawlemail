FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    LANG=C.UTF-8

# System deps cho Chromium/ChromeDriver
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    chromium chromium-driver \
    fonts-liberation libnss3 libgdk-pixbuf-2.0-0 libgtk-3-0 \
    libx11-xcb1 libxcb-dri3-0 libxcomposite1 libxdamage1 libxrandr2 \
    libasound2 curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER=/usr/bin/chromedriver

WORKDIR /app

# Nếu có requirements.txt
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# App code
COPY . /app

# Cloud Run cấp PORT động
ENV PORT=8080
EXPOSE 8080

# (Khuyến nghị) chạy non-root
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Lắng nghe $PORT, tăng timeout cho Selenium
# Đổi "app:app" nếu tên module/biến Flask khác
CMD exec gunicorn app:app -b 0.0.0.0:$PORT --worker-class gthread --threads 8 --timeout 3600

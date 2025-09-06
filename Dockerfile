# ---- Base: Python + Chromium + ChromeDriver ----
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Cài package hệ thống cần cho Chromium/ChromeDriver và build deps cơ bản
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    fonts-liberation \
    libnss3 \
    libgdk-pixbuf-2.0-0 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libasound2 \
    curl ca-certificates \
    build-essential \
 && rm -rf /var/lib/apt/lists/*

# Biến môi trường cho Selenium/Chromium
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER=/usr/bin/chromedriver

# Tạo thư mục app
WORKDIR /app

# ---- Dependencies ----
# Nếu bạn có requirements.txt thì COPY vào và cài đặt
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# ---- App code ----
COPY . /app

# Mặc định Flask sẽ listen 0.0.0.0 với Gunicorn
EXPOSE 8000

# LƯU Ý: app.py phải có biến Flask "app"
# Ví dụ: app = Flask(__name__)
# Nếu tên khác, đổi "app:app" bên dưới cho đúng (format <file>:<obj>)
CMD ["gunicorn", "-b", "0.0.0.0:8000", "app:app", "--workers", "1", "--threads", "4", "--timeout", "180"]

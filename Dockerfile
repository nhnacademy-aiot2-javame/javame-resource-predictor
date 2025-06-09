FROM python:3.9-slim

WORKDIR /app

# 필요한 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# 디렉토리 생성
RUN mkdir -p cache logs models/trained

# 실행 권한 부여 (올바른 경로)
RUN chmod +x scripts/streaming_startup.sh

# 기본 실행 명령
ENTRYPOINT ["/bin/bash", "scripts/streaming_startup.sh"]
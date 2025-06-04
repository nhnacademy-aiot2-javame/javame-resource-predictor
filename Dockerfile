FROM python:3.9-slim

WORKDIR /app

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# 실행 권한 부여 (scripts 폴더 내)
RUN chmod +x scripts/streaming_startup.sh

# 디렉토리 생성
RUN mkdir -p cache logs models/trained

# 실행 (scripts 폴더 내의 스크립트)
CMD ["./scripts/streaming_startup.sh"]

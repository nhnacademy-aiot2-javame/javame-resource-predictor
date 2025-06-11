<h1 align="center">⚙️ API 서비스</h1>
<div align="center">
이 프로젝트는 NHN 아카데미 3팀(Javame)의 자원 사용량 예측 AI 레포지토리입니다.</br>
서버 예측 자원사용량을 제공합니다. 
</br>
</br>
  
구조도는 아래와 같습니다.
</div>


![image](https://github.com/user-attachments/assets/357f4f2b-1ae2-4bef-a02e-518aeaf40579)





<div align="center">
<h3 tabindex="-1" class="heading-element" dir="auto">사용 기술 스택</h3>
<div>
  <img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white">
  <img src="https://img.shields.io/badge/scikit--learn-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white">
  <img src="https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=white">
  <img src="https://img.shields.io/badge/numpy-013243?style=for-the-badge&logo=numpy&logoColor=white">
</div>
<div>
  <img src="https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=MySQL&logoColor=white">
  <img src="https://img.shields.io/badge/InfluxDB-22ADF6?style=for-the-badge&logo=InfluxDB&logoColor=white">
  <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=Redis&logoColor=white"> 
</div>
<div>
  <img src="https://img.shields.io/badge/github-%23121011.svg?style=for-the-badge&logo=github&logoColor=white">
  <img src="https://img.shields.io/badge/git-F05032?style=for-the-badge&logo=git&logoColor=white">
  <img src="https://img.shields.io/badge/GitHub Actions-2088FF?style=for-the-badge&logo=githubActions&logoColor=white">
  <img src="https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white">
  <img src="https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black">
</div>
</div>

주요 기능
1. 멀티 테넌트 지원

- 하나의 컨테이너로 모든 회사/디바이스 자동 처리
- 동적 회사/디바이스 감지 및 관리

2. 실시간 예측

- JVM 메트릭 기반 시스템 자원 사용량 예측
- CPU, 메모리, 디스크 사용률 예측

3. 스트리밍 아키텍처

- InfluxDB 직접 처리로 실시간 데이터 수집
- 파일 캐싱 기반 효율적 데이터 처리

4. 자동화된 학습 파이프라인

- 정기적인 모델 재학습
- 예측 성능 모니터링
```
프로젝트 구조
resource-predictor/
├── core/                       # 핵심 모듈
│   ├── config_manager.py      # 통합 설정 관리
│   ├── db.py                  # MySQL 연결 관리
│   └── logger.py              # 로깅 시스템
│
├── data/                      # 데이터 처리
│   ├── collector.py           # InfluxDB → MySQL 수집
│   └── preprocessor.py        # 데이터 전처리
│
├── models/                    # AI 모델
│   ├── app_models.py          # 애플리케이션 영향도 모델
│   ├── prediction.py          # 시스템 예측 모델
│   └── trained/               # 학습된 모델 저장
│
├── pipelines/                 # 파이프라인
│   ├── data_pipeline.py       # 데이터 수집/전처리
│   ├── model_pipeline.py      # 모델 학습
│   └── prediction_pipeline.py # 예측 실행
│
├── scheduler/                 # 스케줄러
│   ├── global_scheduler.py    # 멀티 테넌트 스케줄러
│   └── health_monitor.py      # 시스템 모니터링
│
├── scripts/                   # 유틸리티
│   ├── setup_database.py      # DB 초기화
│   └── startup.sh             # 시작 스크립트
│
├── main.py                    # 메인 엔트리포인트
└── requirements.txt           # 패키지 목록
```
설치 및 실행
사전 요구사항

- Python 3.9+
- MySQL 8.0+
- InfluxDB 2.0+
- Docker (선택사항)

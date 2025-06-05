"""
JVM 메트릭 기반 시스템 리소스 예측 - 로깅 설정
"""
import logging
import os
from logging.handlers import RotatingFileHandler
import uuid
from config.settings import LOGGING

# 컨텍스트 정보를 저장하는 전역 변수
log_context = {
    'company_domain': None,
    'server_id': None,
    'device_id': None,
    'request_id': None
}

# 로그 컨텍스트 설정 함수
def set_log_context(company_domain=None, server_id=None, device_id=None):
    """로그 컨텍스트 설정"""
    if company_domain:
        log_context['company_domain'] = company_domain
    if server_id:
        log_context['server_id'] = server_id
    if device_id:
        log_context['device_id'] = device_id
    
    # 요청별 고유 ID 생성 (로그 추적용)
    log_context['request_id'] = str(uuid.uuid4())[:8]

class ContextFilter(logging.Filter):
    """로그 레코드에 컨텍스트 정보 추가하는 필터"""
    def filter(self, record):
        # 외부 라이브러리 로거는 필터링하지 않음
        if record.name.startswith(('matplotlib', 'urllib3', 'requests', 'influxdb', 'mysql')):
            # 외부 라이브러리는 기본값만 설정
            record.company = '-'
            record.server_id = '-'
            record.device_id = '-'
            record.request_id = '-'
        else:
            # 내부 로거는 컨텍스트 정보 사용
            record.company = log_context['company_domain'] or '-'
            record.server_id = log_context['server_id'] or '-'
            record.device_id = log_context['device_id'] or '-'
            record.request_id = log_context['request_id'] or '-'
        return True

def setup_logger():
    """로거 설정"""
    # 로그 디렉토리 생성
    log_file = LOGGING.get("file", "logs/jvm_metrics.log")
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 로깅 레벨 설정
    log_level = getattr(logging, LOGGING.get("level", "INFO"))
    
    # 로거 설정
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # 기존 핸들러 제거
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 컨텍스트 필터 생성
    context_filter = ContextFilter()
    
    # 확장된 로그 포맷 (컨텍스트 정보 포함)
    log_format = LOGGING.get("format", "%(asctime)s - [%(company)s:%(server_id)s:%(device_id)s] - %(request_id)s - %(levelname)s - %(message)s")
    
    # 파일 핸들러 추가 (로테이션 적용)
    max_bytes = LOGGING.get("max_bytes", 10 * 1024 * 1024)  # 기본 10MB
    backup_count = LOGGING.get("backup_count", 5)  # 기본 5개 백업
    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    file_handler.addFilter(context_filter)  # 핸들러에 필터 추가
    logger.addHandler(file_handler)
    
    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(log_format)
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(context_filter)  # 핸들러에 필터 추가
    logger.addHandler(console_handler)
    
    # 외부 라이브러리 로거 레벨 조정
    external_loggers = [
        'matplotlib', 'matplotlib.font_manager', 'matplotlib.pyplot',
        'matplotlib.colorbar', 'matplotlib.backend_bases', 'matplotlib.text',
        'urllib3', 'urllib3.connectionpool', 'urllib3.util.retry',
        'requests', 'requests.packages.urllib3',
        'influxdb_client', 'influxdb_client.client',
        'mysql', 'mysql.connector'
    ]
    
    for logger_name in external_loggers:
        ext_logger = logging.getLogger(logger_name)
        ext_logger.setLevel(logging.WARNING)
        # 상위로 전파 방지
        ext_logger.propagate = False
    
    return logger

# 로거 초기화
logger = setup_logger()

# 기본 컨텍스트 정보 설정
set_log_context()

# 컨텍스트 설정 함수 노출
def set_context(company_domain=None, server_id=None, device_id=None):
    """로그 컨텍스트 설정 외부 함수"""
    set_log_context(company_domain, server_id, device_id)
    logger.debug(f"로그 컨텍스트 설정: 회사={company_domain}, 서버ID={server_id}, 디바이스ID={device_id}")
"""
시간대 관리 유틸리티
서버 시간(UTC)을 한국 시간(KST)으로 자동 변환
"""
import os
from datetime import datetime, timedelta

# 환경변수로 시간대 오프셋 설정 가능 (기본값: 9시간)
TIMEZONE_OFFSET_HOURS = int(os.getenv('TIMEZONE_OFFSET_HOURS', '9'))

def get_current_time():
    """현재 시간을 반환 (시간대 보정 적용)"""
    return datetime.now() + timedelta(hours=TIMEZONE_OFFSET_HOURS)

def get_utc_time():
    """UTC 시간 반환 (보정 없음)"""
    return datetime.now()

def convert_to_local(utc_time):
    """UTC 시간을 로컬 시간으로 변환"""
    if utc_time is None:
        return None
    return utc_time + timedelta(hours=TIMEZONE_OFFSET_HOURS)

def convert_to_utc(local_time):
    """로컬 시간을 UTC로 변환"""
    if local_time is None:
        return None
    return local_time - timedelta(hours=TIMEZONE_OFFSET_HOURS)

def align_time(time_val, interval_minutes=1):
    """시간을 지정된 간격으로 정렬"""
    if time_val is None:
        time_val = get_current_time()
    
    # 초와 마이크로초를 0으로 설정
    time_val = time_val.replace(second=0, microsecond=0)
    
    # 분 단위 정렬
    if interval_minutes > 1:
        aligned_minute = (time_val.minute // interval_minutes) * interval_minutes
        time_val = time_val.replace(minute=aligned_minute)
    
    return time_val
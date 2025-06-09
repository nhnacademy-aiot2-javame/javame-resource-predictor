"""
시간대 관리 유틸리티
서버 시간(UTC)을 한국 시간(KST)으로 자동 변환
"""
import os
from datetime import datetime, timedelta


# 서버가 KST로 설정되어 있는지 확인
IS_SERVER_KST = os.getenv('IS_SERVER_KST', 'true').lower() == 'true'

def get_current_time():
    """현재 시간을 반환 (항상 KST)"""
    if IS_SERVER_KST:
        # 서버가 이미 KST면 그대로 사용
        return datetime.now()
    else:
        # 서버가 UTC면 9시간 더함
        return datetime.now() + timedelta(hours=9)

def get_utc_time():
    """UTC 시간 반환"""
    if IS_SERVER_KST:
        # 서버가 KST면 9시간 뺌
        return datetime.now() - timedelta(hours=9)
    else:
        # 서버가 UTC면 그대로
        return datetime.now()

def convert_to_local(utc_time):
    """UTC 시간을 KST로 변환"""
    if utc_time is None:
        return None
    if IS_SERVER_KST:
        # 입력이 실제 UTC라면 9시간 더함
        return utc_time + timedelta(hours=9)
    else:
        # 서버가 UTC인 경우에만 9시간 더함
        return utc_time + timedelta(hours=9)

def convert_to_utc(local_time):
    """KST를 UTC로 변환"""
    if local_time is None:
        return None
    if IS_SERVER_KST:
        # 입력이 KST라면 9시간 뺌
        return local_time - timedelta(hours=9)
    else:
        # 서버가 UTC면 변환 없음
        return local_time
    
def align_time(time_val, interval_minutes=1):
    """시간을 지정된 간격으로 정렬"""
    if time_val is None:
        return None
    
    # 문자열인 경우 datetime으로 변환
    if isinstance(time_val, str):
        time_val = datetime.fromisoformat(time_val.replace('Z', '+00:00'))
    
    # timezone 정보가 있으면 제거
    if hasattr(time_val, 'replace') and time_val.tzinfo is not None:
        time_val = time_val.replace(tzinfo=None)
    
    # 초와 마이크로초를 0으로 설정
    time_val = time_val.replace(second=0, microsecond=0)
    
    # 지정된 간격으로 분 정렬
    if interval_minutes and interval_minutes > 1:
        aligned_minute = (time_val.minute // interval_minutes) * interval_minutes
        time_val = time_val.replace(minute=aligned_minute)
    
    return time_val
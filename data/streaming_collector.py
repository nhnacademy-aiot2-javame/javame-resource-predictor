"""
개선된 데이터 수집기 - InfluxDB에서 직접 데이터 처리
MySQL 저장 없이 실시간 전처리 및 파일 캐싱
"""
import os
import json
import pickle
import pandas as pd
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient
from typing import Dict, List, Optional, Tuple

from core.config_manager import ConfigManager
from core.logger import logger
from core.time_utils import get_current_time

class StreamingDataCollector:
    """실시간 데이터 처리 수집기 - MySQL 저장 없음"""
    
    def __init__(self, config_manager=None, company_domain=None, device_id=None):
        """초기화"""
        if config_manager:
            self.config = config_manager
        else:
            self.config = ConfigManager()
            if company_domain:
                self.config.set('company_domain', company_domain)
            if device_id:
                self.config.set('device_id', device_id)
        
        # InfluxDB 설정
        influxdb_config = self.config.get_influxdb_config()
        self.influx_client = InfluxDBClient(
            url=influxdb_config['url'],
            token=influxdb_config['token'],
            org=influxdb_config['org'],
            timeout=60000
        )
        self.query_api = self.influx_client.query_api()
        
        # 기본 설정
        self.company_domain = self.config.get('company_domain')
        self.device_id = self.config.get('device_id')
        self.influxdb_bucket = influxdb_config['bucket']
        
        # 캐시 디렉토리 설정
        self.cache_dir = os.path.join("cache", self.company_domain, self.device_id or "global")
        os.makedirs(self.cache_dir, exist_ok=True)
        
        logger.info(f"스트리밍 데이터 수집기 초기화: {self.company_domain}/{self.device_id}")
    
    def __del__(self):
        """소멸자"""
        if hasattr(self, 'influx_client') and self.influx_client:
            self.influx_client.close()
    
    def get_training_data(self, start_time=None, end_time=None, cache_hours=6, force_refresh=False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """학습용 데이터 조회 - 캐시 우선 사용"""
        if end_time is None:
            end_time = get_current_time()  
        
        if start_time is None:
            start_time = end_time - timedelta(days=3)
        
        # 시간대 정보 일관성 보장
        if hasattr(start_time, 'replace') and start_time.tzinfo is not None:
            start_time = start_time.replace(tzinfo=None)
        if hasattr(end_time, 'replace') and end_time.tzinfo is not None:
            end_time = end_time.replace(tzinfo=None)
        
        # 고정된 캐시 키 사용
        jvm_cache_file = os.path.join(self.cache_dir, "jvm_training_latest.pkl")
        sys_cache_file = os.path.join(self.cache_dir, "sys_training_latest.pkl")
        
        # 강제 새로고침이 아니고 캐시가 유효한 경우
        if not force_refresh and self._is_cache_valid(jvm_cache_file, cache_hours) and self._is_cache_valid(sys_cache_file, cache_hours):
            logger.info("캐시된 학습 데이터 사용")
            try:
                with open(jvm_cache_file, 'rb') as f:
                    jvm_df = pickle.load(f)
                with open(sys_cache_file, 'rb') as f:
                    sys_df = pickle.load(f)
                
                # 캐시된 데이터의 시간대 정보 제거
                if not jvm_df.empty:
                    jvm_df['time'] = pd.to_datetime(jvm_df['time']).dt.tz_localize(None)
                if not sys_df.empty:
                    sys_df['time'] = pd.to_datetime(sys_df['time']).dt.tz_localize(None)
                
                # 요청된 시간 범위에 맞게 필터링
                if not jvm_df.empty:
                    jvm_df = jvm_df[(jvm_df['time'] >= start_time) & (jvm_df['time'] <= end_time)]
                if not sys_df.empty:
                    sys_df = sys_df[(sys_df['time'] >= start_time) & (sys_df['time'] <= end_time)]
                
                # 캐시된 데이터 검증
                if self._validate_training_data(jvm_df, sys_df):
                    return jvm_df, sys_df
                else:
                    logger.warning("캐시된 데이터가 유효하지 않음, 새로 조회")
            except Exception as e:
                logger.warning(f"캐시 로드 실패, 새로 조회: {e}")
        
        # InfluxDB에서 직접 조회
        logger.info(f"InfluxDB에서 데이터 조회: {start_time} ~ {end_time}")
        jvm_df = self._query_jvm_metrics(start_time, end_time)
        sys_df = self._query_system_resources(start_time, end_time)
        
        # 조회된 데이터 검증
        if not self._validate_training_data(jvm_df, sys_df):
            logger.error("InfluxDB에서 조회한 데이터가 유효하지 않음")
            # 유효성 검사 실패 시에도 데이터 반환 (빈 데이터프레임 대신)
            logger.warning("데이터 유효성 검사 실패했지만 데이터는 반환합니다")
        
        # 캐시에 저장 (덮어쓰기)
        try:
            with open(jvm_cache_file, 'wb') as f:
                pickle.dump(jvm_df, f)
            with open(sys_cache_file, 'wb') as f:
                pickle.dump(sys_df, f)
            logger.info(f"학습 데이터 캐시 업데이트 완료 (JVM: {len(jvm_df)}, SYS: {len(sys_df)})")
        except Exception as e:
            logger.warning(f"캐시 저장 실패: {e}")
        
        return jvm_df, sys_df

    def _validate_training_data(self, jvm_df: pd.DataFrame, sys_df: pd.DataFrame) -> bool:
        """학습 데이터 유효성 검증"""
        if jvm_df.empty or sys_df.empty:
            logger.warning("데이터프레임이 비어있음")
            return False
        
        # JVM 데이터 검증
        if len(jvm_df) < 50:  # 최소 50개 레코드
            logger.warning(f"JVM 데이터가 너무 적음: {len(jvm_df)}개")
            return False
        
        # 애플리케이션 수 확인
        app_count = jvm_df['application'].nunique()
        if app_count < 2:
            logger.warning(f"애플리케이션 수가 너무 적음: {app_count}개")
            return False
        
        # 메트릭 타입 수 확인
        metric_count = jvm_df['metric_type'].nunique()
        if metric_count < 3:
            logger.warning(f"메트릭 타입이 너무 적음: {metric_count}개")
            return False
        
        # 시스템 데이터 검증
        if len(sys_df) < 30:  # 최소 30개 레코드
            logger.warning(f"시스템 데이터가 너무 적음: {len(sys_df)}개")
            return False
        
        # 리소스 타입 확인
        resource_types = sys_df['resource_type'].unique()
        expected_resources = {'cpu', 'mem', 'disk'}
        if not expected_resources.issubset(set(resource_types)):
            logger.warning(f"필수 리소스 타입 누락: 기대={expected_resources}, 실제={set(resource_types)}")
            return False
        
        # 값 범위 검증
        if jvm_df['value'].isna().all():
            logger.warning("JVM 메트릭 값이 모두 NaN")
            return False
        
        if sys_df['value'].isna().all():
            logger.warning("시스템 리소스 값이 모두 NaN")
            return False
        
        logger.info(f"데이터 검증 통과: JVM {len(jvm_df)}개({app_count}개 앱, {metric_count}개 메트릭), SYS {len(sys_df)}개({len(resource_types)}개 리소스)")
        return True

    def clear_cache(self):
        """캐시 완전 삭제"""
        try:
            if os.path.exists(self.cache_dir):
                import shutil
                shutil.rmtree(self.cache_dir)
                logger.info(f"캐시 디렉토리 완전 삭제: {self.cache_dir}")
            
            # 디렉토리 재생성
            os.makedirs(self.cache_dir, exist_ok=True)
            logger.info(f"캐시 디렉토리 재생성: {self.cache_dir}")
            
        except Exception as e:
            logger.error(f"캐시 삭제 오류: {e}")
    
    def _is_cache_valid(self, cache_file: str, valid_hours: int) -> bool:
        """캐시 파일이 유효한지 확인"""
        if not os.path.exists(cache_file):
            return False
        
        file_time = datetime.fromtimestamp(os.path.getmtime(cache_file))
        return (get_current_time() - file_time).total_seconds() < valid_hours * 3600
            
    def _query_influxdb_data(self, start_time: datetime, end_time: datetime, query_type: str) -> pd.DataFrame:
        """InfluxDB에서 데이터 조회 (통합 함수)"""
        device_filter = f' and r["deviceId"] == "{self.device_id}"' if self.device_id else ""
        
        # timezone aware datetime을 UTC로 변환
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=None)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=None)
        
        start_str = start_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # 시간 정렬 간격 가져오기
        time_alignment = self.config.get('time_alignment_minutes', 1)
        
        base_query = f'''
        from(bucket: "{self.influxdb_bucket}")
        |> range(start: {start_str}, stop: {end_str})
        |> filter(fn: (r) => r["origin"] == "server_data")
        |> filter(fn: (r) => r["companyDomain"] == "{self.company_domain}"{device_filter})
        '''
        
        if query_type == 'jvm':
            query = base_query + f'''
            |> filter(fn: (r) => r["location"] == "service_resource_data")
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: {time_alignment}m, fn: mean, createEmpty: false)
            '''
        elif query_type == 'cpu':
            query = base_query + f'''
            |> filter(fn: (r) => r["location"] == "server_resource_data")
            |> filter(fn: (r) => r["gatewayId"] == "cpu")
            |> filter(fn: (r) => r["_measurement"] == "usage_idle")
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: {time_alignment}m, fn: mean, createEmpty: false)
            |> map(fn: (r) => ({{ r with _value: 100.0 - r._value }}))
            '''
        elif query_type == 'mem':
            query = base_query + f'''
            |> filter(fn: (r) => r["location"] == "server_resource_data")
            |> filter(fn: (r) => r["gatewayId"] == "mem")
            |> filter(fn: (r) => r["_measurement"] == "used_percent")
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: {time_alignment}m, fn: mean, createEmpty: false)
            '''
        elif query_type == 'disk':
            query = base_query + f'''
            |> filter(fn: (r) => r["location"] == "server_resource_data")
            |> filter(fn: (r) => r["gatewayId"] == "disk")
            |> filter(fn: (r) => r["_measurement"] == "used_percent")
            |> filter(fn: (r) => r["_field"] == "value")
            |> aggregateWindow(every: {time_alignment}m, fn: mean, createEmpty: false)
            '''
        
        try:
            logger.debug(f"{query_type} 데이터 쿼리 실행: {start_str} ~ {end_str} (간격: {time_alignment}분)")
            results = self.query_api.query(query)
            data = []
            
            for table in results:
                for record in table.records:
                    # 시간 정보를 timezone-naive로 변환
                    record_time = record.get_time()
                    if hasattr(record_time, 'replace'):
                        # timezone 정보 제거 및 초 단위를 0으로 설정
                        record_time = pd.to_datetime(record_time).tz_localize(None)
                        # 정렬된 시간으로 변환 (초와 마이크로초를 0으로)
                        record_time = record_time.replace(second=0, microsecond=0)
                    
                    if query_type == 'jvm':
                        data.append({
                            'time': record_time,
                            'application': record.values.get('gatewayId', ''),
                            'metric_type': record.get_measurement(),
                            'value': record.get_value(),
                            'device_id': record.values.get('deviceId', '')
                        })
                    else:
                        measurement = 'usage_user' if query_type == 'cpu' else 'used_percent'
                        data.append({
                            'time': record_time,
                            'resource_type': query_type,
                            'measurement': measurement,
                            'value': record.get_value(),
                            'device_id': record.values.get('deviceId', '')
                        })
            
            df = pd.DataFrame(data)
            if not df.empty:
                # 최종적으로 시간 정보 확인 및 정리
                df['time'] = pd.to_datetime(df['time']).dt.tz_localize(None)
                # 중복 제거 (같은 시간에 여러 값이 있을 경우 평균)
                if query_type == 'jvm':
                    df = df.groupby(['time', 'application', 'metric_type', 'device_id']).agg({'value': 'mean'}).reset_index()
                else:
                    df = df.groupby(['time', 'resource_type', 'measurement', 'device_id']).agg({'value': 'mean'}).reset_index()
                
                logger.info(f"{query_type} 데이터 조회 완료: {len(df)}개 (정렬됨)")
            else:
                logger.warning(f"{query_type} 데이터 조회 결과 없음")
            
            return df
            
        except Exception as e:
            logger.error(f"{query_type} 데이터 조회 오류: {e}")
            return pd.DataFrame()
    
    def _query_jvm_metrics(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """InfluxDB에서 JVM 메트릭 조회"""
        return self._query_influxdb_data(start_time, end_time, 'jvm')
    
    def _query_system_resources(self, start_time: datetime, end_time: datetime) -> pd.DataFrame:
        """InfluxDB에서 시스템 리소스 조회"""
        all_data = []
        
        for resource_type in ['cpu', 'mem', 'disk']:
            df = self._query_influxdb_data(start_time, end_time, resource_type)
            if not df.empty:
                all_data.append(df)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def get_latest_data(self, minutes=30) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """최근 데이터 조회 - 예측용"""
        end_time = datetime.utcnow()  # UTC 시간 사용
        start_time = end_time - timedelta(minutes=minutes)
        
        logger.info(f"최근 데이터 조회: {start_time} ~ {end_time} ({minutes}분)")
        
        # 최근 데이터는 캐시하지 않고 직접 조회
        jvm_df = self._query_jvm_metrics(start_time, end_time)
        sys_df = self._query_system_resources(start_time, end_time)
        
        # 데이터가 부족한 경우 기간 확장
        if jvm_df.empty or len(jvm_df) < 10:
            logger.warning(f"최근 {minutes}분 JVM 데이터 부족 ({len(jvm_df)}개), 기간 확장")
            
            # 최대 6시간까지 확장
            for extended_minutes in [60, 120, 360]:
                extended_start = end_time - timedelta(minutes=extended_minutes)
                jvm_df = self._query_jvm_metrics(extended_start, end_time)
                
                if not jvm_df.empty and len(jvm_df) >= 10:
                    logger.info(f"확장된 기간({extended_minutes}분)에서 JVM 데이터 발견: {len(jvm_df)}개")
                    # 시스템 데이터도 같은 기간으로 조회
                    sys_df = self._query_system_resources(extended_start, end_time)
                    break
            
            if jvm_df.empty:
                logger.error("확장된 기간에서도 JVM 데이터를 찾을 수 없습니다")
        
        logger.info(f"최근 데이터 조회 결과: JVM {len(jvm_df)}개, 시스템 {len(sys_df)}개")
        
        return jvm_df, sys_df
    
    def cleanup_cache(self, max_age_hours=48):
        """오래된 캐시 정리"""
        logger.info(f"캐시 정리 시작: {max_age_hours}시간 이상 파일 삭제")
        
        cutoff_time = get_current_time() - timedelta(hours=max_age_hours)
        deleted_count = 0
        
        try:
            for filename in os.listdir(self.cache_dir):
                filepath = os.path.join(self.cache_dir, filename)
                
                if os.path.isfile(filepath):
                    file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                    
                    if file_time < cutoff_time:
                        os.remove(filepath)
                        deleted_count += 1
                        logger.debug(f"캐시 파일 삭제: {filename}")
            
            logger.info(f"캐시 정리 완료: {deleted_count}개 파일 삭제")
            
        except Exception as e:
            logger.error(f"캐시 정리 오류: {e}")
    
    def get_cache_status(self) -> Dict:
        """캐시 상태 조회"""
        try:
            cache_files = [f for f in os.listdir(self.cache_dir) if f.endswith('.pkl')]
            total_size = sum(os.path.getsize(os.path.join(self.cache_dir, f)) for f in cache_files)
            
            return {
                'cache_dir': self.cache_dir,
                'file_count': len(cache_files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'files': cache_files
            }
            
        except Exception as e:
            logger.error(f"캐시 상태 조회 오류: {e}")
            return {'error': str(e)}
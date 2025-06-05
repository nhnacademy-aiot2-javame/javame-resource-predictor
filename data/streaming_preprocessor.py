"""
스트리밍 전처리기 - 파일 기반 영향도/특성 캐싱
MySQL 저장 없이 계산 결과를 파일로 관리
"""
import os
import sys
import json
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from core.time_utils import get_current_time

# 프로젝트 루트 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config_manager import ConfigManager
from core.logger import logger

class StreamingPreprocessor:
    """스트리밍 전처리기"""
    
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
        
        # 기본 설정
        self.company_domain = self.config.get('company_domain')
        self.device_id = self.config.get('device_id')
        
        # 캐시 디렉토리 설정
        self.cache_dir = os.path.join("cache", self.company_domain, self.device_id or "global")
        self.impact_dir = os.path.join(self.cache_dir, "impacts")
        self.features_dir = os.path.join(self.cache_dir, "features")
        
        os.makedirs(self.impact_dir, exist_ok=True)
        os.makedirs(self.features_dir, exist_ok=True)
        
        # 전처리 설정
        self.window_sizes = self.config.get('window_sizes', [5, 15, 30, 60])
        self.statistics = self.config.get('statistics', ['mean', 'std', 'max', 'min'])
        self.resample_interval = self.config.get('resample_interval', '5min')
        
        logger.info(f"스트리밍 전처리기 초기화: {self.company_domain}/{self.device_id}")
    
    def _cache_data(self, data: Any, cache_dir: str, cache_key: str, data_type: str) -> bool:
        """데이터 캐싱 (통합 함수)"""
        cache_file = os.path.join(cache_dir, f"{data_type}_{cache_key}.pkl")
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
            
            meta_file = os.path.join(cache_dir, f"{data_type}_{cache_key}_meta.json")
            metadata = {
                'created_at': get_current_time().isoformat(),  
                'company_domain': self.company_domain,
                'device_id': self.device_id,
                f'{data_type}_records': len(data) if hasattr(data, '__len__') else 0
            }
            
            with open(meta_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"{data_type} 캐시 저장 완료: {cache_key}")
            return True
            
        except Exception as e:
            logger.warning(f"{data_type} 캐시 저장 실패: {e}")
            return False
    
    def _load_cache_data(self, cache_dir: str, cache_key: str, data_type: str) -> Optional[Any]:
        """캐시 데이터 로드 (통합 함수)"""
        cache_file = os.path.join(cache_dir, f"{data_type}_{cache_key}.pkl")
        
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"{data_type} 캐시 로드 오류: {e}")
        
        return None
    
    def calculate_and_cache_impacts(self, jvm_df: pd.DataFrame, sys_df: pd.DataFrame, 
                                cache_key: str = None, force_recalculate=False) -> Optional[pd.DataFrame]:
        """영향도 계산 및 캐싱 - 고정 캐시 사용"""
        # 고정 캐시 파일명 사용
        impact_file = os.path.join(self.impact_dir, "impact_latest.pkl")
        
        # 입력 데이터의 시간대 정보 제거
        jvm_df = jvm_df.copy()
        sys_df = sys_df.copy()
        
        if not jvm_df.empty:
            jvm_df['time'] = pd.to_datetime(jvm_df['time']).dt.tz_localize(None)
        if not sys_df.empty:
            sys_df['time'] = pd.to_datetime(sys_df['time']).dt.tz_localize(None)
        
        # 입력 데이터 검증
        if not self._validate_input_data(jvm_df, sys_df, "영향도 계산"):
            return None
        
        # 캐시된 영향도가 있고 강제 재계산이 아니면 로드
        if not force_recalculate and os.path.exists(impact_file):
            try:
                # 캐시 파일 시간 확인 (6시간 이내)
                file_time = datetime.fromtimestamp(os.path.getmtime(impact_file))
                if (get_current_time() - file_time).total_seconds() < 6 * 3600:  #
                    with open(impact_file, 'rb') as f:
                        impact_df = pickle.load(f)
                    
                    # 캐시된 데이터의 시간대 정보도 제거
                    if not impact_df.empty:
                        impact_df['time'] = pd.to_datetime(impact_df['time']).dt.tz_localize(None)
                    
                    # 캐시된 데이터 검증
                    if self._validate_impact_data(impact_df):
                        logger.info("유효한 캐시된 영향도 사용")
                        return impact_df
                    else:
                        logger.warning("캐시된 영향도 데이터가 유효하지 않음, 재계산")
            except Exception as e:
                logger.warning(f"영향도 캐시 로드 실패: {e}")
        
        # 영향도 계산
        logger.info("영향도 계산 시작")
        impact_df = self._calculate_impact_scores(jvm_df, sys_df)
        
        if impact_df is not None and not impact_df.empty and self._validate_impact_data(impact_df):
            # 캐시에 저장 (덮어쓰기)
            try:
                with open(impact_file, 'wb') as f:
                    pickle.dump(impact_df, f)
                
                # 메타데이터도 업데이트
                meta_file = os.path.join(self.impact_dir, "impact_latest_meta.json")
                metadata = {
                    'created_at': get_current_time().isoformat(),
                    'company_domain': self.company_domain,
                    'device_id': self.device_id,
                    'jvm_records': len(jvm_df),
                    'sys_records': len(sys_df),
                    'impact_records': len(impact_df),
                    'unique_applications': impact_df['application'].nunique(),
                    'unique_resources': impact_df['resource_type'].nunique(),
                    'impact_score_range': {
                        'min': float(impact_df['impact_score'].min()),
                        'max': float(impact_df['impact_score'].max()),
                        'mean': float(impact_df['impact_score'].mean())
                    }
                }
                
                with open(meta_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                logger.info(f"영향도 캐시 업데이트 완료 ({len(impact_df)}개, {impact_df['application'].nunique()}개 앱)")
                
            except Exception as e:
                logger.warning(f"영향도 캐시 저장 실패: {e}")
        else:
            logger.error("영향도 계산 실패 또는 유효하지 않은 결과")
            return None
        
        return impact_df

    def generate_and_cache_features(self, jvm_df: pd.DataFrame, cache_key: str = None, 
                                force_recalculate=False) -> Optional[pd.DataFrame]:
        """특성 생성 및 캐싱 - 고정 캐시 사용"""
        # 고정 캐시 파일명 사용
        features_file = os.path.join(self.features_dir, "features_latest.pkl")
        
        # 입력 데이터 검증
        if not self._validate_input_data(jvm_df, None, "특성 생성"):
            return None
        
        # 캐시된 특성이 있고 강제 재계산이 아니면 로드
        if not force_recalculate and os.path.exists(features_file):
            try:
                # 캐시 파일 시간 확인 (6시간 이내)
                file_time = datetime.fromtimestamp(os.path.getmtime(features_file))
                if (get_current_time() - file_time).total_seconds() < 6 * 3600:
                    with open(features_file, 'rb') as f:
                        features_df = pickle.load(f)
                    
                    # 캐시된 데이터 검증
                    if self._validate_features_data(features_df):
                        logger.info("유효한 캐시된 특성 사용")
                        return features_df
                    else:
                        logger.warning("캐시된 특성 데이터가 유효하지 않음, 재생성")
            except Exception as e:
                logger.warning(f"특성 캐시 로드 실패: {e}")
        
        # 특성 생성
        logger.info("특성 생성 시작")
        features_df = self._generate_features(jvm_df)
        
        if features_df is not None and not features_df.empty and self._validate_features_data(features_df):
            # 캐시에 저장 (덮어쓰기)
            try:
                with open(features_file, 'wb') as f:
                    pickle.dump(features_df, f)
                
                # 메타데이터도 업데이트
                meta_file = os.path.join(self.features_dir, "features_latest_meta.json")
                metadata = {
                    'created_at': get_current_time().isoformat(),
                    'company_domain': self.company_domain,
                    'device_id': self.device_id,
                    'jvm_records': len(jvm_df),
                    'feature_records': len(features_df),
                    'unique_features': features_df['feature_name'].nunique(),
                    'unique_applications': features_df['application'].nunique(),
                    'feature_value_range': {
                        'min': float(features_df['value'].min()),
                        'max': float(features_df['value'].max()),
                        'mean': float(features_df['value'].mean())
                    }
                }
                
                with open(meta_file, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                logger.info(f"특성 캐시 업데이트 완료 ({len(features_df)}개, {features_df['feature_name'].nunique()}개 특성)")
                
            except Exception as e:
                logger.warning(f"특성 캐시 저장 실패: {e}")
        else:
            logger.error("특성 생성 실패 또는 유효하지 않은 결과")
            return None
        
        return features_df

    def _validate_input_data(self, jvm_df: pd.DataFrame, sys_df: Optional[pd.DataFrame], context: str) -> bool:
        """입력 데이터 유효성 검증"""
        if jvm_df is None or jvm_df.empty:
            logger.error(f"{context}: JVM 데이터가 비어있음")
            return False
        
        if len(jvm_df) < 20:
            logger.error(f"{context}: JVM 데이터가 너무 적음 ({len(jvm_df)}개)")
            return False
        
        if jvm_df['application'].nunique() < 1:
            logger.error(f"{context}: 애플리케이션이 없음")
            return False
        
        if jvm_df['metric_type'].nunique() < 2:
            logger.error(f"{context}: 메트릭 타입이 너무 적음 ({jvm_df['metric_type'].nunique()}개)")
            return False
        
        if sys_df is not None:
            if sys_df.empty or len(sys_df) < 10:
                logger.error(f"{context}: 시스템 데이터가 부족함")
                return False
        
        return True

    def _validate_impact_data(self, impact_df: pd.DataFrame) -> bool:
        """영향도 데이터 유효성 검증"""
        if impact_df is None or impact_df.empty:
            return False
        
        required_columns = ['time', 'application', 'resource_type', 'impact_score']
        if not all(col in impact_df.columns for col in required_columns):
            logger.error(f"영향도 데이터 필수 컬럼 누락: {required_columns}")
            return False
        
        if impact_df['application'].nunique() < 1:
            logger.error("영향도 데이터에 애플리케이션이 없음")
            return False
        
        if impact_df['resource_type'].nunique() < 2:
            logger.error("영향도 데이터에 리소스 타입이 부족함")
            return False
        
        if impact_df['impact_score'].isna().all():
            logger.error("모든 영향도 점수가 NaN")
            return False
        
        # 영향도 점수 범위 확인 (0~1 범위)
        valid_scores = impact_df['impact_score'].dropna()
        if len(valid_scores) == 0:
            logger.error("유효한 영향도 점수가 없음")
            return False
        
        if valid_scores.min() < 0 or valid_scores.max() > 2:
            logger.warning(f"영향도 점수 범위 이상: {valid_scores.min():.3f} ~ {valid_scores.max():.3f}")
        
        return True

    def _validate_features_data(self, features_df: pd.DataFrame) -> bool:
        """특성 데이터 유효성 검증"""
        if features_df is None or features_df.empty:
            return False
        
        required_columns = ['time', 'application', 'feature_name', 'value']
        if not all(col in features_df.columns for col in required_columns):
            logger.error(f"특성 데이터 필수 컬럼 누락: {required_columns}")
            return False
        
        if features_df['application'].nunique() < 1:
            logger.error("특성 데이터에 애플리케이션이 없음")
            return False
        
        if features_df['feature_name'].nunique() < 5:
            logger.error(f"특성이 너무 적음: {features_df['feature_name'].nunique()}개")
            return False
        
        if features_df['value'].isna().all():
            logger.error("모든 특성 값이 NaN")
            return False
        
        # 특성 값의 변동성 확인
        feature_stats = features_df.groupby('feature_name')['value'].agg(['mean', 'std', 'min', 'max'])
        zero_variance_features = feature_stats[feature_stats['std'] == 0]
        
        if len(zero_variance_features) > len(feature_stats) * 0.8:
            logger.warning(f"변동성 없는 특성이 너무 많음: {len(zero_variance_features)}/{len(feature_stats)}")
        
        return True

    def clear_cache(self):
        """캐시 완전 삭제"""
        try:
            if os.path.exists(self.cache_dir):
                import shutil
                shutil.rmtree(self.cache_dir)
                logger.info(f"전처리 캐시 디렉토리 완전 삭제: {self.cache_dir}")
            
            # 디렉토리 재생성
            os.makedirs(self.impact_dir, exist_ok=True)
            os.makedirs(self.features_dir, exist_ok=True)
            logger.info(f"전처리 캐시 디렉토리 재생성: {self.cache_dir}")
            
        except Exception as e:
            logger.error(f"전처리 캐시 삭제 오류: {e}")
    
    def _calculate_impact_scores(self, jvm_df: pd.DataFrame, sys_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """영향도 점수 계산 """
        if jvm_df.empty or sys_df.empty:
            logger.warning("영향도 계산을 위한 데이터가 충분하지 않음")
            return None
        
        try:
            # 시간 형식 검증 및 변환 (timezone 제거)
            if not pd.api.types.is_datetime64_any_dtype(jvm_df['time']):
                jvm_df['time'] = pd.to_datetime(jvm_df['time'])
            jvm_df['time'] = pd.to_datetime(jvm_df['time']).dt.tz_localize(None)
            
            if not pd.api.types.is_datetime64_any_dtype(sys_df['time']):
                sys_df['time'] = pd.to_datetime(sys_df['time'])
            sys_df['time'] = pd.to_datetime(sys_df['time']).dt.tz_localize(None)
            
            # 시스템 리소스 데이터 필터링
            sys_filtered = sys_df[
                ((sys_df['resource_type'] == 'cpu') & (sys_df['measurement'] == 'usage_user')) |
                ((sys_df['resource_type'] == 'mem') & (sys_df['measurement'] == 'used_percent')) |
                ((sys_df['resource_type'] == 'disk') & (sys_df['measurement'] == 'used_percent'))
            ]
            
            if sys_filtered.empty:
                logger.error("필터링된 시스템 리소스 데이터가 없음")
                return None
            
            # 피봇 테이블 생성
            jvm_pivot = jvm_df.pivot_table(
                index='time', 
                columns=['application', 'metric_type'], 
                values='value',
                aggfunc='mean'
            )
            
            sys_pivot = sys_filtered.pivot_table(
                index='time', 
                columns=['resource_type', 'measurement'], 
                values='value',
                aggfunc='mean'
            )
            
            if jvm_pivot.empty or sys_pivot.empty:
                logger.error("피봇 테이블 생성 실패")
                return None
            
            # 시계열 리샘플링 - 1분 단위로 변경
            jvm_resampled = jvm_pivot.resample('1min').mean().interpolate(method='linear', limit=5)
            sys_resampled = sys_pivot.resample('1min').mean().interpolate(method='linear', limit=5)
            
            # 공통 시간대 확인
            common_times = jvm_resampled.index.intersection(sys_resampled.index)
            
            logger.info(f"공통 시간대 수: {len(common_times)}")
            
            if len(common_times) < 5:
                logger.warning(f"공통 시간대가 너무 적음: {len(common_times)}개")
                # 원본 데이터로 다시 시도
                common_times = jvm_pivot.index.intersection(sys_pivot.index)
                if len(common_times) >= 5:
                    logger.info(f"원본 데이터 사용: {len(common_times)}개")
                    jvm_resampled = jvm_pivot
                    sys_resampled = sys_pivot
                else:
                    return None
            
            # 상관관계 계산
            impact_scores = self._compute_correlations(jvm_resampled, sys_resampled, common_times)
            
            if impact_scores:
                return pd.DataFrame(impact_scores)
            else:
                return None
                
        except Exception as e:
            logger.error(f"영향도 계산 중 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _compute_correlations(self, jvm_resampled, sys_resampled, common_times):
        """상관관계 기반 영향도 계산 """
        jvm_aligned = jvm_resampled.loc[common_times]
        sys_aligned = sys_resampled.loc[common_times]
        
        impact_scores = []
        
        for resource_col in sys_aligned.columns:
            resource_type, measurement = resource_col
            sys_values = sys_aligned[resource_col]
            
            for app in jvm_aligned.columns.get_level_values(0).unique():
                app_metrics = [col for col in jvm_aligned.columns if col[0] == app]
                
                if not app_metrics:
                    continue
                
                corr_sum = 0
                corr_count = 0
                
                for app_metric in app_metrics:
                    jvm_values = jvm_aligned[app_metric]
                    
                    if jvm_values.isnull().all() or sys_values.isnull().all():
                        continue
                    
                    try:
                        corr = sys_values.corr(jvm_values)
                        if not np.isnan(corr):
                            corr_sum += abs(corr)
                            corr_count += 1
                    except Exception:
                        continue
                
                if corr_count > 0:
                    impact_score = corr_sum / corr_count
                    
                    for time_point in common_times:
                        impact_scores.append({
                            'time': time_point,
                            'application': app,
                            'resource_type': resource_type,
                            'impact_score': impact_score,
                            'calculation_method': 'correlation'
                        })
        
        return impact_scores
    
    def _generate_features(self, jvm_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """시계열 특성 생성 """
        if jvm_df.empty:
            logger.warning("특성 생성을 위한 데이터가 없음")
            return None
        
        try:
            # 피봇 테이블로 변환
            pivot_df = jvm_df.pivot_table(
                index='time', 
                columns=['application', 'metric_type'], 
                values='value',
                aggfunc='mean'
            )
            
            all_features = []
            applications = pivot_df.columns.get_level_values(0).unique()
            
            for app in applications:
                app_cols = [col for col in pivot_df.columns if col[0] == app]
                app_df = pivot_df[app_cols].copy()
                app_df.columns = [col[1] for col in app_df.columns]
                app_df = app_df.dropna()
                
                if app_df.empty:
                    continue
                
                # 윈도우별 특성 생성
                app_features = self._generate_app_features(app, app_df)
                all_features.extend(app_features)
            
            # 시간 특성 추가
            if self.config.get('time_features', True) and all_features:
                time_features = self._generate_time_features(applications, all_features)
                all_features.extend(time_features)
            
            if all_features:
                feature_df = pd.DataFrame(all_features)
                feature_df = feature_df.drop_duplicates(
                    subset=['time', 'application', 'feature_name'], 
                    keep='last'
                )
                return feature_df
            else:
                return None
                
        except Exception as e:
            logger.error(f"특성 생성 중 오류: {e}")
            return None
    
    def _generate_app_features(self, app, app_df):
        """애플리케이션별 특성 생성"""
        features = []
        
        for window_size in self.window_sizes:
            rolling_window = app_df.rolling(window=f"{window_size}min", min_periods=1)
            
            stats_dict = {}
            for stat in self.statistics:
                if stat == 'mean':
                    stats_dict[stat] = rolling_window.mean()
                elif stat == 'std':
                    stats_dict[stat] = rolling_window.std()
                elif stat == 'max':
                    stats_dict[stat] = rolling_window.max()
                elif stat == 'min':
                    stats_dict[stat] = rolling_window.min()
            
            for stat_name, stat_df in stats_dict.items():
                resampled = stat_df.resample(self.resample_interval).last().dropna()
                
                for metric in resampled.columns:
                    for timestamp, value in resampled[metric].items():
                        if not pd.isna(value):
                            features.append({
                                'time': timestamp,
                                'application': app,
                                'feature_name': f"{metric}_{stat_name}_{window_size}min",
                                'value': float(value),
                                'window_size': window_size
                            })
        
        return features
    
    def _generate_time_features(self, applications, all_features):
        """시간 특성 생성"""
        time_features = []
        unique_times = set(feature['time'] for feature in all_features)
        
        for app in applications:
            for time_point in unique_times:
                time_feature_list = [
                    ('hour', time_point.hour),
                    ('day_of_week', time_point.dayofweek),
                    ('is_weekend', 1 if time_point.dayofweek >= 5 else 0)
                ]
                
                for feature_name, value in time_feature_list:
                    time_features.append({
                        'time': time_point,
                        'application': app,
                        'feature_name': feature_name,
                        'value': float(value),
                        'window_size': 0
                    })
        
        return time_features
    
    def load_cached_impacts(self, cache_key: str = None) -> Optional[pd.DataFrame]:
        """캐시된 영향도 로드"""
        impact_file = os.path.join(self.impact_dir, "impact_latest.pkl")
        
        if os.path.exists(impact_file):
            try:
                with open(impact_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"영향도 캐시 로드 오류: {e}")
        
        return None
   
    def load_cached_features(self, cache_key: str = None) -> Optional[pd.DataFrame]:
        """캐시된 특성 로드"""
        features_file = os.path.join(self.features_dir, "features_latest.pkl")
        
        if os.path.exists(features_file):
            try:
                with open(features_file, 'rb') as f:
                    return pickle.load(f)
            except Exception as e:
                logger.error(f"특성 캐시 로드 오류: {e}")
        
        return None
   
    def cleanup_cache(self, max_age_hours=48):
        """오래된 캐시 정리"""
        logger.info("캐시 정리 - 고정 캐시 방식으로 자동 관리됨")
        # 필요시 오래된 메타데이터 파일만 정리
        for cache_subdir in [self.impact_dir, self.features_dir]:
            try:
                for filename in os.listdir(cache_subdir):
                    # _meta.json 파일 중 오래된 것만 삭제
                    if filename.endswith('_meta.json') and not filename.startswith(('impact_latest', 'features_latest')):
                        filepath = os.path.join(cache_subdir, filename)
                        os.remove(filepath)
                        logger.debug(f"오래된 메타데이터 삭제: {filename}")
            except Exception as e:
                logger.error(f"캐시 정리 오류: {e}")
   
    def get_cache_status(self) -> Dict:
       """캐시 상태 조회"""
       try:
           impact_files = [f for f in os.listdir(self.impact_dir) if f.endswith('.pkl')]
           features_files = [f for f in os.listdir(self.features_dir) if f.endswith('.pkl')]
           
           impact_size = sum(os.path.getsize(os.path.join(self.impact_dir, f)) for f in impact_files)
           features_size = sum(os.path.getsize(os.path.join(self.features_dir, f)) for f in features_files)
           
           return {
               'cache_dir': self.cache_dir,
               'impacts': {
                   'file_count': len(impact_files),
                   'size_mb': round(impact_size / (1024 * 1024), 2),
                   'files': impact_files
               },
               'features': {
                   'file_count': len(features_files),
                   'size_mb': round(features_size / (1024 * 1024), 2),
                   'files': features_files
               },
               'total_size_mb': round((impact_size + features_size) / (1024 * 1024), 2)
           }
           
       except Exception as e:
           logger.error(f"캐시 상태 조회 오류: {e}")
           return {'error': str(e)}
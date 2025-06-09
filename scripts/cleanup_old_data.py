#!/usr/bin/env python
"""
오래된 데이터 정리 스크립트
- predictions: 7일 이상 된 데이터 삭제, 1일 이전 데이터는 최신 예측만 유지
- model_performance: 3일 이상 된 데이터 삭제
"""
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db import DatabaseManager
from core.logger import logger
from core.time_utils import get_current_time

def cleanup_predictions(db: DatabaseManager, retention_days: int = 7):
    """예측 데이터 정리"""
    try:
        logger.info("예측 데이터 정리 시작")
        
        current_time = get_current_time()
        
        # 1. 7일 이상 된 데이터 삭제
        old_date = current_time - timedelta(days=retention_days)
        
        delete_old_query = """
        DELETE FROM predictions 
        WHERE prediction_time < %s
        """
        
        result = db.execute_query(delete_old_query, (old_date,))
        if result:
            logger.info(f"{retention_days}일 이상 된 예측 데이터 삭제 완료")
        
        # 2. 1일 이전 데이터 중 중복 제거 (같은 target_time에 대해 최신 prediction_time만 유지)
        yesterday = current_time - timedelta(days=1)
        
        # 삭제할 중복 데이터 찾기
        find_duplicates_query = """
        SELECT p1.id
        FROM predictions p1
        INNER JOIN (
            SELECT company_domain, server_no, target_time, resource_type, device_id,
                   MAX(prediction_time) as latest_prediction
            FROM predictions
            WHERE target_time < %s
            GROUP BY company_domain, server_no, target_time, resource_type, device_id
        ) p2 ON p1.company_domain = p2.company_domain 
            AND p1.server_no = p2.server_no
            AND p1.target_time = p2.target_time
            AND p1.resource_type = p2.resource_type
            AND COALESCE(p1.device_id, '') = COALESCE(p2.device_id, '')
            AND p1.prediction_time < p2.latest_prediction
        WHERE p1.target_time < %s
        """
        
        duplicates = db.fetch_all(find_duplicates_query, (yesterday, yesterday))
        
        if duplicates:
            duplicate_ids = [row[0] for row in duplicates]
            
            # 배치로 삭제 (한 번에 1000개씩)
            batch_size = 1000
            for i in range(0, len(duplicate_ids), batch_size):
                batch = duplicate_ids[i:i + batch_size]
                placeholders = ','.join(['%s'] * len(batch))
                delete_query = f"DELETE FROM predictions WHERE id IN ({placeholders})"
                
                if db.execute_query(delete_query, tuple(batch)):
                    logger.info(f"중복 예측 데이터 {len(batch)}개 삭제")
        
        # 3. 삭제 후 통계
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            MIN(prediction_time) as oldest_prediction,
            MAX(prediction_time) as latest_prediction,
            COUNT(DISTINCT DATE(target_time)) as unique_days
        FROM predictions
        """
        
        stats = db.fetch_one(stats_query)
        if stats:
            total, oldest, latest, days = stats
            logger.info(f"예측 데이터 정리 완료: 총 {total}개, 기간: {oldest} ~ {latest}, {days}일")
        
        return True
        
    except Exception as e:
        logger.error(f"예측 데이터 정리 오류: {e}")
        return False

def cleanup_model_performance(db: DatabaseManager, retention_days: int = 3):
    """모델 성능 데이터 정리"""
    try:
        logger.info("모델 성능 데이터 정리 시작")
        
        current_time = get_current_time()
        old_date = current_time - timedelta(days=retention_days)
        
        # 3일 이상 된 데이터 삭제
        delete_query = """
        DELETE FROM model_performance 
        WHERE trained_at < %s
        """
        
        result = db.execute_query(delete_query, (old_date,))
        if result:
            logger.info(f"{retention_days}일 이상 된 모델 성능 데이터 삭제 완료")
        
        # 삭제 후 통계
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            MIN(trained_at) as oldest_training,
            MAX(trained_at) as latest_training,
            COUNT(DISTINCT application) as unique_apps
        FROM model_performance
        """
        
        stats = db.fetch_one(stats_query)
        if stats:
            total, oldest, latest, apps = stats
            logger.info(f"모델 성능 데이터 정리 완료: 총 {total}개, 기간: {oldest} ~ {latest}, {apps}개 앱")
        
        return True
        
    except Exception as e:
        logger.error(f"모델 성능 데이터 정리 오류: {e}")
        return False

def main():
    """메인 함수"""
    logger.info("데이터 정리 작업 시작")
    
    db = DatabaseManager()
    
    try:
        # 예측 데이터 정리
        cleanup_predictions(db, retention_days=7)
        
        # 모델 성능 데이터 정리
        cleanup_model_performance(db, retention_days=3)
        
        logger.info("데이터 정리 작업 완료")
        return True
        
    except Exception as e:
        logger.error(f"데이터 정리 작업 오류: {e}")
        return False
    finally:
        db.close()

if __name__ == "__main__":
    main()
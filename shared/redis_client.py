import redis
import os

# Redis connection
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

def get_redis_client():
    """Get Redis client instance"""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=False
    )

def publish_progress(job_id, percent, message, data=None, **kwargs):
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    
    redis_client = get_redis_client()
    progress_data = {
        'percent': percent,
        'message': message,
    }
    
    # Tạo data object để frontend có thể truy cập
    data_obj = {}
    
    if kwargs:
        for key, value in kwargs.items():
            if value is not None:
                progress_data[key] = value
                # Thêm vào data object nếu là field frontend cần
                if key in ['accumulated_total', 'accumulated_downloaded', 'accumulated_percent', 
                          'thuyet_minh_downloaded', 'thuyet_minh_total',
                          'total_cccd', 'processed_cccd', 'total_images', 'processed_images', 
                          'total_rows', 'estimated_cccd', 'processed']:
                    data_obj[key] = value
    
    if data:
        if isinstance(data, dict):
            # Copy tất cả fields từ data vào data_obj (bao gồm cả giá trị 0)
            for key, value in data.items():
                # Copy tất cả field, kể cả khi value = 0 (vì 0 là giá trị hợp lệ)
                if value is not None or (isinstance(value, (int, float)) and value == 0):
                    data_obj[key] = value
                    # Cũng copy lên top level cho backward compatibility
                    if key in ['total_cccd', 'processed_cccd', 'total_images', 'processed_images', 
                              'total_rows', 'estimated_cccd', 'processed']:
                        progress_data[key] = value
            # Đảm bảo các field accumulated_* và thuyet_minh_* được copy vào data_obj (kể cả khi = 0)
            for key in ['accumulated_total', 'accumulated_downloaded', 'accumulated_percent',
                       'thuyet_minh_downloaded', 'thuyet_minh_total']:
                if key in data:
                    # Copy ngay cả khi giá trị là 0
                    data_obj[key] = data[key]
            progress_data['data'] = data_obj
            # LOG: Kiểm tra data_obj sau khi copy (dùng INFO để luôn hiển thị)
            logger.info(f"[REDIS] publish_progress data_obj: accumulated_percent={data_obj.get('accumulated_percent')}, accumulated_total={data_obj.get('accumulated_total')}, accumulated_downloaded={data_obj.get('accumulated_downloaded')}, thuyet_minh_downloaded={data_obj.get('thuyet_minh_downloaded')}, thuyet_minh_total={data_obj.get('thuyet_minh_total')}")
        else:
            progress_data['data'] = data
    else:
        # Nếu không có data, vẫn tạo data object với các field từ kwargs
        if data_obj:
            progress_data['data'] = data_obj
    
    # LOG: Kiểm tra progress_data['data'] trước khi publish (dùng INFO để luôn hiển thị)
    if 'data' in progress_data and isinstance(progress_data['data'], dict):
        logger.info(f"[REDIS] publish_progress progress_data['data'] keys: {list(progress_data['data'].keys())}")
        logger.info(f"[REDIS] publish_progress progress_data['data'] values: accumulated_percent={progress_data['data'].get('accumulated_percent')}, accumulated_total={progress_data['data'].get('accumulated_total')}, accumulated_downloaded={progress_data['data'].get('accumulated_downloaded')}, thuyet_minh_downloaded={progress_data['data'].get('thuyet_minh_downloaded')}, thuyet_minh_total={progress_data['data'].get('thuyet_minh_total')}")
    
    progress_json = json.dumps(progress_data, ensure_ascii=False)
    progress_bytes = progress_json.encode('utf-8')
    
    processed_cccd = kwargs.get('processed_cccd') or (data.get('processed_cccd') if isinstance(data, dict) else None)
    total_cccd = kwargs.get('total_cccd') or (data.get('total_cccd') if isinstance(data, dict) else None)
    logger.info(f"📤 Publishing progress for job {job_id}: {percent}% - {message[:50]}... (processed_cccd={processed_cccd}, total_cccd={total_cccd})")
    
    # Publish to pub/sub (for real-time)
    try:
        redis_client.publish(f"job:{job_id}:progress", progress_bytes)
        logger.debug(f"✅ Published to pub/sub: job:{job_id}:progress")
    except Exception as e:
        logger.error(f"❌ Error publishing progress to pub/sub: {e}")
    
    # Also push to list (for polling fallback)
    progress_list_key = f"job:{job_id}:progress:list"
    try:
        result = redis_client.rpush(progress_list_key, progress_bytes)
        list_length = redis_client.llen(progress_list_key)
        logger.info(f"✅ Pushed to Redis list {progress_list_key}, new length: {result}, total list length: {list_length}")
        logger.debug(f"   Progress data: percent={percent}, processed_cccd={processed_cccd}, total_cccd={total_cccd}")
    except Exception as e:
        logger.error(f"❌ Error pushing progress to list: {e}")
    
    # Also set job status if needed
    try:
        # Limit list size to prevent memory issues (keep last 100 messages)
        redis_client.ltrim(progress_list_key, -100, -1)
    except Exception as e:
        logger.error(f"Error trimming progress list: {e}")

def is_job_cancelled(job_id):
    redis_client = get_redis_client()
    try:
        cancelled = redis_client.get(f"job:{job_id}:cancelled")
        if cancelled:
            # Handle both bytes and string
            if isinstance(cancelled, bytes):
                cancelled = cancelled.decode('utf-8')
            return cancelled.strip() == "1"
        return False
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error checking cancellation for job {job_id}: {e}")
        return False

def cancel_job(job_id):
    redis_client = get_redis_client()
    try:
        redis_client.set(f"job:{job_id}:cancelled", "1")
        redis_client.set(f"job:{job_id}:status", "cancelled")
        logger = logging.getLogger(__name__)
        logger.info(f"Job {job_id} marked as cancelled")
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"Error cancelling job {job_id}: {e}")
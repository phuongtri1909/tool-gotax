import redis
import os
import logging

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
    import time
    
    logger = logging.getLogger(__name__)
    
    # ✅ Retry mechanism với timeout handling
    max_retries = 3
    retry_delay = 0.5  # 0.5 giây
    
    for attempt in range(max_retries):
        try:
            redis_client = get_redis_client()
            
            # ✅ Test connection trước khi sử dụng
            try:
                redis_client.ping()
            except Exception as ping_e:
                logger.warning(f"⚠️ [REDIS] Connection test failed (attempt {attempt + 1}/{max_retries}): {ping_e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"❌ [REDIS] Cannot connect to Redis after {max_retries} attempts")
                    return
            
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
                else:
                    progress_data['data'] = data
            else:
                # Nếu không có data, vẫn tạo data object với các field từ kwargs
                if data_obj:
                    progress_data['data'] = data_obj
            
            progress_json = json.dumps(progress_data, ensure_ascii=False)
            progress_bytes = progress_json.encode('utf-8')
            
            processed_cccd = kwargs.get('processed_cccd') or (data.get('processed_cccd') if isinstance(data, dict) else None)
            total_cccd = kwargs.get('total_cccd') or (data.get('total_cccd') if isinstance(data, dict) else None)
            logger.info(f"📤 Publishing progress for job {job_id}: {percent}% - {message[:50]}...")
            
            # Publish to pub/sub (for real-time) với timeout
            try:
                redis_client.publish(f"job:{job_id}:progress", progress_bytes)
                logger.debug(f"✅ Published to pub/sub: job:{job_id}:progress")
            except Exception as e:
                logger.error(f"❌ Error publishing progress to pub/sub: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"❌ Failed to publish to pub/sub after {max_retries} attempts")
            
            # Also push to list (for polling fallback) với timeout
            progress_list_key = f"job:{job_id}:progress:list"
            try:
                result = redis_client.rpush(progress_list_key, progress_bytes)
                logger.debug(f"✅ Pushed to Redis list {progress_list_key}, new length: {result}")
            except Exception as e:
                logger.error(f"❌ Error pushing progress to list: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"❌ Failed to push to list after {max_retries} attempts")
            
            # Also set job status if needed
            try:
                # Limit list size to prevent memory issues (keep last 100 messages)
                redis_client.ltrim(progress_list_key, -100, -1)
            except Exception as e:
                logger.error(f"Error trimming progress list: {e}")
            
            # ✅ Nếu đến đây thì thành công, break khỏi retry loop
            break
            
        except Exception as e:
            logger.error(f"❌ [REDIS] Error in publish_progress (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                logger.error(f"❌ [REDIS] Failed to publish progress after {max_retries} attempts: {e}")
                return

def is_job_cancelled(job_id):
    """Check if a job has been cancelled"""
    logger = logging.getLogger(__name__)
    
    try:
        redis_client = get_redis_client()
        cancelled = redis_client.get(f"job:{job_id}:cancelled")
        if cancelled:
            # Handle both bytes and string
            if isinstance(cancelled, bytes):
                cancelled = cancelled.decode('utf-8')
            return cancelled.strip() == "1"
        return False
    except Exception as e:
        logger.error(f"Error checking cancellation for job {job_id}: {e}")
        return False

def cancel_job(job_id):
    logger = logging.getLogger(__name__)
    redis_client = get_redis_client()
    try:
        redis_client.set(f"job:{job_id}:cancelled", "1")
        redis_client.set(f"job:{job_id}:status", "cancelled")
        logger.info(f"Job {job_id} marked as cancelled")
    except Exception as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
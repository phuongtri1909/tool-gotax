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
    
    if kwargs:
        for key, value in kwargs.items():
            if value is not None:
                progress_data[key] = value
    
    if data:
        if isinstance(data, dict):
            for key in ['total_cccd', 'processed_cccd', 'total_images', 'processed_images', 'total_rows', 'estimated_cccd', 'processed']:
                if key in data and data[key] is not None:
                    progress_data[key] = data[key]
            progress_data['data'] = data
        else:
            progress_data['data'] = data
    
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
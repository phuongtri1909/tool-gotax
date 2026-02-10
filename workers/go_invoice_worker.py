#!/usr/bin/env python3
"""
Go-Invoice Worker
Consume jobs from Redis queue and call API server via HTTP
API server sẽ publish events vào Redis, worker lắng nghe từ Redis
"""
import sys
import os
import json
import asyncio
import logging
import httpx

# Get the project root directory (tool-gotax)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import shared modules
from shared.redis_client import get_redis_client, publish_progress

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis queues
QUEUE_GO_INVOICE = 'go-invoice:jobs'

# API Server URL
API_SERVER_URL = os.getenv('GO_INVOICE_API_URL', 'http://127.0.0.1:5000/api/go-invoice')


async def process_go_invoice_job(job_data):
    """
    Process go-invoice job by calling API server via HTTP
    API server sẽ publish events vào Redis
    """
    job_id = job_data.get('job_id')
    action = job_data.get('action', 'tongquat')
    params = job_data.get('params', {})
    
    redis_client = get_redis_client()
    
    try:
        # Update status: processing
        redis_client.set(f"job:{job_id}:status", "processing".encode('utf-8'))
        
        # Extract params
        token = params.get('token')
        type_invoice = params.get('type_invoice', 2)  # 2: mua vào (default)
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        proxy_url = params.get('proxy')
        
        if not token:
            error_msg = "Missing token"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
            publish_progress(job_id, 0, error_msg, {'type': 'error', 'error': error_msg, 'error_code': 'MISSING_TOKEN'})
            return
        
        if not all([start_date, end_date]):
            error_msg = "Missing start_date or end_date"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return
        
        if action == 'tongquat':
            endpoint = '/tongquat/queue'
            request_data = {
                'job_id': job_id,
                'Authorization': f'Bearer {token}',
                'type_invoice': type_invoice,
                'start_date': start_date,
                'end_date': end_date,
            }
            if proxy_url:
                request_data['proxy'] = proxy_url
            logger.info(f"[Job {job_id}] Bắt đầu tongquat từ {start_date} đến {end_date}")
            publish_progress(job_id, 0, "Bắt đầu đồng bộ hóa đơn...")
        elif action == 'chitiet':
            endpoint = '/chitiet/queue'
            request_data = {
                'job_id': job_id,
                'Authorization': f'Bearer {token}',
                'type_invoice': type_invoice,
                'start_date': start_date,
                'end_date': end_date,
            }
            if proxy_url:
                request_data['proxy'] = proxy_url
            logger.info(f"[Job {job_id}] Bắt đầu chi tiết từ {start_date} đến {end_date}")
            publish_progress(job_id, 0, "Bắt đầu đồng bộ hóa đơn (chi tiết)...")
        elif action == 'xmlhtml':
            endpoint = '/xmlhtml/queue'
            datas = params.get('datas', [])
            options = params.get('options', {'xml': True, 'html': True})
            request_data = {
                'job_id': job_id,
                'Authorization': f'Bearer {token}',
                'type_invoice': type_invoice,
                'start_date': start_date,
                'end_date': end_date,
                'datas': datas,
                'options': options,
            }
            if proxy_url:
                request_data['proxy'] = proxy_url
            
            logger.info(f"[Job {job_id}] Bắt đầu xuất XML/HTML")
            publish_progress(job_id, 0, "Bắt đầu xuất XML/HTML...")
            
        elif action == 'pdf':
            endpoint = '/pdf/queue'
            datas = params.get('datas', [])
            request_data = {
                'job_id': job_id,
                'Authorization': f'Bearer {token}',
                'type_invoice': type_invoice,
                'start_date': start_date,
                'end_date': end_date,
                'datas': datas,
            }
            if proxy_url:
                request_data['proxy'] = proxy_url
            
            logger.info(f"[Job {job_id}] Bắt đầu xuất PDF")
            publish_progress(job_id, 0, "Bắt đầu xuất PDF...")
            
        else:
            error_msg = f"Action không hợp lệ: {action}"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return
        
        # ✅ Gọi API server với timeout ngắn (10s) - API sẽ trả về "accepted" ngay
        # API xử lý trong background và ghi progress vào Redis
        # Worker sẽ poll Redis để đợi kết quả (giống Go Soft)
        async with httpx.AsyncClient(timeout=10.0) as client:
            logger.info(f"[Job {job_id}] Gọi API server: {API_SERVER_URL}{endpoint}")
            
            try:
                response = await client.post(
                    f"{API_SERVER_URL}{endpoint}",
                    json=request_data,
                    timeout=10.0
                )
                
                if response.status_code != 200:
                    error_msg = f"API server trả về lỗi: {response.status_code}"
                    error_code = None
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', error_msg)
                        error_code = error_data.get('error_code')
                        
                        # Check session/token errors
                        if error_code in ['TOKEN_INVALID', 'TOKEN_EXPIRED', 'MISSING_AUTHORIZATION', 'NOT_LOGGED_IN']:
                            error_msg = error_data.get('message', 'Token đã hết hạn. Vui lòng đăng nhập lại.')
                            logger.warning(f"[Job {job_id}] Token error ({error_code}): {error_msg}")
                    except:
                        pass
                    
                    publish_progress(job_id, 0, error_msg, {
                        'type': 'error',
                        'error': error_msg,
                        'error_code': error_code
                    })
                    redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                    redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                    return
                
                response_data = response.json()
                if response_data.get('status') != 'accepted':
                    error_msg = response_data.get('message', 'API server từ chối request')
                    logger.error(f"[Job {job_id}] {error_msg}")
                    redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                    redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                    publish_progress(job_id, 0, error_msg)
                    return
                
                logger.info(f"[Job {job_id}] API server đã chấp nhận request, đang đợi API hoàn thành...")
                    
            except httpx.TimeoutException:
                error_msg = "Timeout khi gọi API server (không thể kết nối)"
                logger.error(f"[Job {job_id}] {error_msg}")
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                publish_progress(job_id, 0, error_msg)
                return
        
        # ✅ Poll status trong Redis để đợi job hoàn thành (tối đa 2 giờ - giống Go Soft)
        # API server sẽ tự publish events vào Redis, Laravel frontend sẽ tự lắng nghe qua SSE/polling
        max_wait_time = 7200  # 2 hours
        poll_interval = 2
        waited_time = 0
        
        # Flag để check cancellation từ bên ngoài (Ctrl+C)
        should_stop = False
        
        while waited_time < max_wait_time and not should_stop:
            try:
                await asyncio.sleep(poll_interval)
                waited_time += poll_interval
                
                # Check cancelled flag
                cancelled = redis_client.get(f"job:{job_id}:cancelled")
                if cancelled:
                    cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                    if cancelled == '1':
                        logger.info(f"[Job {job_id}] Job đã bị cancel")
                        redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                        # ✅ Client message: thân thiện với người dùng
                        publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                        return
                
                # Check job status
                status = redis_client.get(f"job:{job_id}:status")
                if status:
                    status = status.decode('utf-8') if isinstance(status, bytes) else str(status).strip()
                    
                    if status == 'completed':
                        logger.info(f"[Job {job_id}] Job hoàn thành")
                        return
                    elif status == 'failed':
                        error = redis_client.get(f"job:{job_id}:error")
                        if error:
                            error = error.decode('utf-8') if isinstance(error, bytes) else str(error)
                        logger.error(f"[Job {job_id}] Job failed: {error}")
                        return
                    elif status == 'cancelled':
                        logger.info(f"[Job {job_id}] Job đã bị cancel")
                        return
            except asyncio.CancelledError:
                logger.info(f"[Job {job_id}] Task bị cancel (Ctrl+C)")
                should_stop = True
                # Set cancelled flag in Redis
                try:
                    redis_client.set(f"job:{job_id}:cancelled", "1".encode('utf-8'))
                    redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                    # ✅ Client message: thân thiện với người dùng
                    publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                except:
                    pass
                return
        
        logger.warning(f"[Job {job_id}] Timeout: Job chưa hoàn thành sau {max_wait_time} giây")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"[Job {job_id}] Exception: {error_msg}", exc_info=True)
        redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
        redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
        publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")


async def worker_loop(redis_client, semaphore, max_concurrent=5):
    """Async worker loop that processes jobs concurrently"""
    active_tasks = set()
    
    while True:
        try:
            # Check for completed tasks and remove them
            completed_tasks = [task for task in active_tasks if task.done()]
            for task in completed_tasks:
                active_tasks.remove(task)
                try:
                    task.result()
                except Exception as e:
                    logger.error(f"Task completed with error: {e}", exc_info=True)
            
            # Only pop new job if we have capacity
            if len(active_tasks) < max_concurrent:
                try:
                    result = await asyncio.to_thread(redis_client.blpop, [QUEUE_GO_INVOICE], timeout=1)
                    
                    if result:
                        queue_name, job_data_json = result
                        
                        if isinstance(job_data_json, bytes):
                            job_data_json = job_data_json.decode('utf-8')
                        
                        try:
                            job_data = json.loads(job_data_json)
                            job_id = job_data.get('job_id')
                            logger.info(f"Received job: {job_id}")
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing job data: {e}")
                            continue
                        
                        # Create async task
                        async def process_with_semaphore():
                            async with semaphore:
                                try:
                                    await process_go_invoice_job(job_data)
                                except asyncio.CancelledError:
                                    logger.info(f"[Job {job_id}] Task cancelled")
                                    # ✅ Set cancelled flag ngay lập tức để API server biết dừng
                                    try:
                                        redis_client.set(f"job:{job_id}:cancelled", "1")
                                        redis_client.set(f"job:{job_id}:status", "cancelled")
                                        # ✅ Client message: thân thiện với người dùng
                                        publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                                    except:
                                        pass
                                    raise  # Re-raise để worker loop biết
                                except Exception as e:
                                    logger.error(f"Error processing job {job_id}: {e}", exc_info=True)
                        
                        task = asyncio.create_task(process_with_semaphore())
                        active_tasks.add(task)
                    else:
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error checking queue: {e}", exc_info=True)
                    await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.5)
                
        except KeyboardInterrupt:
            logger.info("⏹️ Worker dừng bởi người dùng (Ctrl+C)")
            
            # ✅ Set cancelled flag cho TẤT CẢ jobs đang processing NGAY LẬP TỨC
            # Điều này quan trọng để API server biết dừng ngay
            try:
                # Lấy tất cả job IDs đang processing
                keys = redis_client.keys("job:*:status")
                for key in keys:
                    if isinstance(key, bytes):
                        key = key.decode('utf-8')
                    status = redis_client.get(key)
                    if status:
                        status = status.decode('utf-8') if isinstance(status, bytes) else str(status).strip()
                        if status == 'processing':
                            job_id = key.split(':')[1]
                            logger.info(f"⏹️ Setting cancelled flag for job {job_id}")
                            redis_client.set(f"job:{job_id}:cancelled", "1")
                            redis_client.set(f"job:{job_id}:status", "cancelled")
            except Exception as e:
                logger.warning(f"Error setting cancelled flags: {e}")
            
            if active_tasks:
                logger.info(f"⏳ Đang hủy {len(active_tasks)} task đang chạy...")
                # Cancel tất cả tasks ngay lập tức
                for task in active_tasks:
                    if not task.done():
                        task.cancel()
                
                # Đợi một chút để tasks cancel
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*active_tasks, return_exceptions=True),
                        timeout=2.0
                    )
                except asyncio.TimeoutError:
                    pass
            break
        except asyncio.CancelledError:
            logger.info("⏹️ Worker đã được cancel")
            break
        except Exception as e:
            logger.error(f"❌ Lỗi trong worker loop: {e}")
            await asyncio.sleep(5)


def main():
    """Main worker entry point"""
    redis_client = get_redis_client()
    
    # Test Redis connection
    try:
        redis_client.ping()
    except Exception as e:
        logger.error("Lỗi Redis: %s" % e)
        return
    max_concurrent = int(os.getenv('WORKER_MAX_CONCURRENT', '5'))
    logger.info("Go-Invoice Worker ready | queue: %s, max: %s" % (QUEUE_GO_INVOICE, max_concurrent))
    
    semaphore = asyncio.Semaphore(max_concurrent)
    
    try:
        asyncio.run(worker_loop(redis_client, semaphore, max_concurrent=max_concurrent))
    except KeyboardInterrupt:
        logger.info("⏹️ Worker đã dừng")
    except Exception as e:
        if not isinstance(e, (asyncio.CancelledError, KeyboardInterrupt)):
            logger.error(f"❌ Fatal error: {e}", exc_info=True)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Go-Soft Worker
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
QUEUE_GO_SOFT = 'go-soft:jobs'

async def process_go_soft_job(job_data):
    """
    Process go-soft job by calling API server via HTTP
    API server sẽ publish events vào Redis, worker lắng nghe từ Redis
    """
    job_id = job_data.get('job_id')
    action = job_data.get('action', 'crawl_tokhai')  # Default to crawl_tokhai
    params = job_data.get('params', {})
    
    redis_client = get_redis_client()
    
    # Khởi tạo các biến tracking ngay từ đầu (trước try block)
    results = []
    total_count = 0
    zip_filename = None
    download_id = None
    accumulated_total = 0
    accumulated_downloaded = 0
    job_completed = False
    event_count = 0
    
    try:
        # Update status: processing
        redis_client.set(f"job:{job_id}:status", "processing".encode('utf-8'))
        
        # Extract params
        session_id = params.get('session_id')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        
        if not all([session_id, start_date, end_date]):
            error_msg = "Thiếu thông tin: session_id, start_date, end_date"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return
        
        # API Server URL
        API_SERVER_URL = os.getenv('GO_SOFT_API_URL', 'http://127.0.0.1:5000/api/go-soft')
        
        # Xác định endpoint và message dựa trên action
        if action == 'crawl_tokhai':
            endpoint = '/crawl/tokhai'
            tokhai_type = params.get('tokhai_type', '00')
            if not tokhai_type or tokhai_type.strip() == "":
                tokhai_type = "00"
            request_data = {
                'job_id': job_id,
                'session_id': session_id,
                'tokhai_type': tokhai_type,
                'start_date': start_date,
                'end_date': end_date
            }
            logger.info(f"[Job {job_id}] Bắt đầu crawl tờ khai: {tokhai_type} từ {start_date} đến {end_date}")
            publish_progress(job_id, 0, "Bắt đầu crawl tờ khai...")
        elif action == 'crawl_thongbao':
            endpoint = '/crawl/thongbao'
            request_data = {
                'job_id': job_id,
                'session_id': session_id,
                'start_date': start_date,
                'end_date': end_date
            }
            logger.info(f"[Job {job_id}] Bắt đầu crawl thông báo từ {start_date} đến {end_date}")
            publish_progress(job_id, 0, "Bắt đầu crawl thông báo...")
        elif action == 'crawl_giaynoptien':
            endpoint = '/crawl/giaynoptien'
            request_data = {
                'job_id': job_id,
                'session_id': session_id,
                'start_date': start_date,
                'end_date': end_date
            }
            logger.info(f"[Job {job_id}] Bắt đầu crawl giấy nộp tiền từ {start_date} đến {end_date}")
            publish_progress(job_id, 0, "Bắt đầu crawl giấy nộp tiền...")
        elif action == 'crawl_batch':
            # ✅ Batch crawl - gọi API /crawl/batch/queue
            endpoint = '/crawl/batch/queue'
            crawl_types = params.get('crawl_types', [])
            tokhai_type = params.get('tokhai_type', '00')
            if not tokhai_type or tokhai_type.strip() == "":
                tokhai_type = "00"
            request_data = {
                'job_id': job_id,
                'session_id': session_id,
                'start_date': start_date,
                'end_date': end_date,
                'crawl_types': crawl_types,
                'tokhai_type': tokhai_type
            }
            crawl_types_str = ', '.join(crawl_types)
            logger.info(f"[Job {job_id}] Bắt đầu batch crawl ({crawl_types_str}) từ {start_date} đến {end_date}")
            publish_progress(job_id, 0, f"Bắt đầu crawl {len(crawl_types)} loại...")
        else:
            error_msg = f"Action không hợp lệ: {action}"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return
        
        # Gọi API server (POST request, không cần SSE)
        async with httpx.AsyncClient(timeout=10.0) as client:
            logger.info(f"[Job {job_id}] Gọi API server: {API_SERVER_URL}{endpoint}")
            response = await client.post(
                f"{API_SERVER_URL}{endpoint}",
                json=request_data,
                timeout=10.0
            )
            
            if response.status_code != 200:
                # ✅ Parse error response từ API để lấy error_code và message
                error_msg = f"API server trả về lỗi: {response.status_code}"
                error_code = None
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', error_msg)
                    error_code = error_data.get('error_code', None)
                    
                    # Nếu là session error, thêm thông tin rõ ràng
                    if error_code in ['SESSION_NOT_FOUND', 'SESSION_EXPIRED', 'NOT_LOGGED_IN', 'MISSING_SESSION_ID']:
                        error_msg = error_data.get('message', 'Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.')
                        logger.warning(f"[Job {job_id}] Session error ({error_code}): {error_msg}")
                    else:
                        logger.error(f"[Job {job_id}] {error_msg} (error_code: {error_code})")
                except:
                    logger.error(f"[Job {job_id}] {error_msg} (không parse được error response)")
                
                # Publish error event với error_code để frontend xử lý
                error_event = {
                    'type': 'error',
                    'error': error_msg,
                    'error_code': error_code
                }
                publish_progress(job_id, 0, error_msg, error_event)
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                return
            
            response_data = response.json()
            if response_data.get('status') != 'accepted':
                error_msg = response_data.get('message', 'API server từ chối request')
                logger.error(f"[Job {job_id}] {error_msg}")
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                publish_progress(job_id, 0, error_msg)
                return
            
            logger.info(f"[Job {job_id}] API server đã chấp nhận request, đang đợi API hoàn thành...")
        
        # Worker không cần lắng nghe events từ Redis
        # API server sẽ tự publish events vào Redis, Laravel frontend sẽ tự lắng nghe qua SSE
        # Worker chỉ cần poll status để biết khi nào job hoàn thành (hoặc đợi một chút rồi check result)
        
        # Poll status trong Redis (tối đa 2 giờ = 7200 giây)
        max_wait_time = 7200  # 2 hours
        poll_interval = 2  # Check mỗi 2 giây
        waited_time = 0
        
        while waited_time < max_wait_time:
            await asyncio.sleep(poll_interval)
            waited_time += poll_interval
            
            # ✅ Check cancelled flag trước
            cancelled = redis_client.get(f"job:{job_id}:cancelled")
            if cancelled:
                cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                if cancelled == '1':
                    logger.info(f"[Job {job_id}] Job đã bị cancel, dừng worker")
                    redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                    publish_progress(job_id, 0, "Job đã bị hủy")
                    return
            
            # Check job status
            status = redis_client.get(f"job:{job_id}:status")
            if status:
                status = status.decode('utf-8') if isinstance(status, bytes) else str(status).strip()
                
                if status == 'cancelled':
                    logger.info(f"[Job {job_id}] Job đã bị cancel")
                    publish_progress(job_id, 0, "Job đã bị hủy")
                    return
                
                if status == 'completed':
                    # Lấy result từ Redis (API đã lưu)
                    result_json = redis_client.get(f"job:{job_id}:result")
                    if result_json:
                        try:
                            result_data = json.loads(result_json.decode('utf-8') if isinstance(result_json, bytes) else result_json)
                            
                            # ✅ Kiểm tra nếu là batch crawl (có batch_results)
                            if 'batch_results' in result_data:
                                # Batch crawl result
                                batch_results = result_data.get('batch_results', {})
                                total_files = result_data.get('total_files', 0)
                                
                                # Log thông tin từng loại crawl trong batch
                                for crawl_type, batch_result in batch_results.items():
                                    type_total = batch_result.get('total', 0)
                                    type_download_id = batch_result.get('download_id')
                                    type_zip_filename = batch_result.get('zip_filename')
                                    logger.info(f"[Job {job_id}] Batch crawl - {crawl_type}: {type_total} file, download_id: {type_download_id}")
                                
                                job_completed = True
                                logger.info(f"[Job {job_id}] Batch crawl hoàn thành: {total_files} file tổng cộng")
                            else:
                                # Single crawl result
                                total_count = result_data.get('total', 0)
                                download_id = result_data.get('download_id')
                                zip_filename = result_data.get('zip_filename')
                                job_completed = True
                                logger.info(f"[Job {job_id}] Job hoàn thành: {total_count} file, download_id: {download_id}")
                            break
                        except Exception as e:
                            logger.warning(f"[Job {job_id}] Lỗi khi parse result từ Redis: {e}")
                    else:
                        logger.warning(f"[Job {job_id}] Status completed nhưng chưa có result trong Redis")
                        break
                
                elif status == 'failed':
                    error_json = redis_client.get(f"job:{job_id}:error")
                    error_msg = "Lỗi không xác định"
                    if error_json:
                        try:
                            error_msg = error_json.decode('utf-8') if isinstance(error_json, bytes) else str(error_json)
                        except:
                            pass
                    logger.error(f"[Job {job_id}] Job failed: {error_msg}")
                    raise Exception(error_msg)
        
        if not job_completed:
            logger.warning(f"[Job {job_id}] Timeout: Job chưa hoàn thành sau {max_wait_time} giây")
                
    except Exception as e:
        error_msg = str(e)
        error_type = type(e).__name__
        
        # Kiểm tra Redis status trước
        current_status = redis_client.get(f"job:{job_id}:status")
        if current_status:
            current_status = current_status.decode('utf-8') if isinstance(current_status, bytes) else str(current_status).strip()
        
        # Nếu đã completed, bỏ qua lỗi
        if current_status == 'completed':
            logger.info(f"[Job {job_id}] Job đã hoàn thành trong Redis, bỏ qua lỗi")
            return
        
        # Kiểm tra result trong Redis
        result_json = redis_client.get(f"job:{job_id}:result")
        if result_json:
            try:
                result_data = json.loads(result_json.decode('utf-8') if isinstance(result_json, bytes) else result_json)
                if result_data.get('total', 0) > 0:
                    redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                    logger.info(f"[Job {job_id}] Đã đánh dấu completed dựa trên result trong Redis")
                    return
            except:
                pass
        
        # Nếu có dữ liệu, cố gắng lưu
        if job_completed:
            logger.info(f"[Job {job_id}] Job đã completed, bỏ qua lỗi")
        elif download_id:
            # CHỈ lưu nếu có download_id (core service đã tạo ZIP)
            try:
                if accumulated_total > 0:
                    total_count = accumulated_total
                await save_job_result(redis_client, job_id, total_count, results, None, zip_filename, download_id)
                logger.info(f"[Job {job_id}] Đã lưu kết quả sau lỗi")
            except Exception as save_err:
                logger.error(f"[Job {job_id}] Lỗi khi lưu kết quả: {save_err}")
                redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                redis_client.set(f"job:{job_id}:error", f"{error_msg} (save failed: {save_err})".encode('utf-8'))
                publish_progress(job_id, 0, f"Lỗi: {error_msg}")
        else:
            logger.error(f"[Job {job_id}] Lỗi: {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
            publish_progress(job_id, 0, f"Lỗi: {error_msg}")


async def save_job_result(redis_client, job_id, total_count, results, zip_base64, zip_filename, download_id=None):
    """
    Lưu kết quả job vào Redis - chỉ lưu download_id, client sẽ download trực tiếp từ API server
    """
    try:
        result_data = {
            'total': total_count,
            'results': results,
            'zip_filename': zip_filename
        }
        
        if download_id:
            result_data['download_id'] = download_id
        
        redis_client.set(f"job:{job_id}:result", json.dumps(result_data, ensure_ascii=False).encode('utf-8'))
        redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
        publish_progress(job_id, 100, "Hoàn thành!")
        logger.info(f"[Job {job_id}] Đã lưu kết quả: {total_count} file")
    except Exception as e:
        logger.error(f"[Job {job_id}] Lỗi khi lưu kết quả: {e}")
        try:
            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
        except:
            pass

async def worker_loop(redis_client, semaphore, max_concurrent=3):
    """Async worker loop that processes jobs concurrently"""
    active_tasks = set()
    
    while True:
        try:
            # Check for completed tasks and remove them
            completed_tasks = [task for task in active_tasks if task.done()]
            for task in completed_tasks:
                active_tasks.remove(task)
                try:
                    task.result()  # This will raise exception if task failed
                except Exception as e:
                    logger.error(f"Task completed with error: {e}", exc_info=True)
            
            # Only pop new job if we have capacity
            if len(active_tasks) < max_concurrent:
                # Non-blocking check for jobs (use lpop instead of blpop in async context)
                # We'll use a small timeout to avoid blocking the event loop
                try:
                    # Use asyncio.to_thread to run blocking Redis operation
                    result = await asyncio.to_thread(redis_client.blpop, [QUEUE_GO_SOFT], timeout=1)
                    
                    if result:
                        queue_name, job_data_json = result
                        logger.info(f"Received job from queue: {queue_name}")
                        
                        # Decode bytes to string if needed
                        if isinstance(job_data_json, bytes):
                            job_data_json = job_data_json.decode('utf-8')
                        
                        logger.info(f"Job data (raw): {job_data_json[:200]}...")  # Log first 200 chars
                        
                        try:
                            job_data = json.loads(job_data_json)
                            job_id = job_data.get('job_id')
                            logger.info(f"Job data parsed successfully. Job ID: {job_id}")
                        except json.JSONDecodeError as e:
                            logger.error(f"Error parsing job data as JSON: {e}")
                            logger.error(f"Job data: {job_data_json}")
                            continue
                        
                        logger.info(f"Processing job: {job_id}")
                        
                        # Create async task for this job (runs concurrently)
                        async def process_with_semaphore():
                            async with semaphore:  # Limit concurrent jobs
                                try:
                                    logger.info(f"Starting async processing for job: {job_id}")
                                    await process_go_soft_job(job_data)
                                    logger.info(f"Completed async processing for job: {job_id}")
                                except Exception as e:
                                    logger.error(f"Error in async processing for job {job_id}: {e}", exc_info=True)
                                    # Update job status to failed
                                    await asyncio.to_thread(redis_client.set, f"job:{job_id}:status", "failed".encode('utf-8'))
                                    await asyncio.to_thread(redis_client.set, f"job:{job_id}:error", str(e).encode('utf-8'))
                                    publish_progress(job_id, 0, f"Lỗi: {str(e)}")
                        
                        task = asyncio.create_task(process_with_semaphore())
                        active_tasks.add(task)
                    else:
                        # No job received, wait a bit before checking again
                        await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Error checking queue: {e}", exc_info=True)
                    await asyncio.sleep(1)
            else:
                # At capacity, wait a bit before checking again
                await asyncio.sleep(0.5)
                
        except KeyboardInterrupt:
            logger.info("⏹️ Worker dừng bởi người dùng")
            if active_tasks:
                logger.info(f"⏳ Đang chờ {len(active_tasks)} task hoàn thành...")
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*active_tasks, return_exceptions=True),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    logger.info("⏹️ Timeout khi đợi tasks, đang cancel...")
                    for task in active_tasks:
                        if not task.done():
                            task.cancel()
            break
        except asyncio.CancelledError:
            # Suppress CancelledError khi shutdown
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
        logger.info("✅ Kết nối Redis thành công")
    except Exception as e:
        logger.error(f"❌ Lỗi kết nối Redis: {e}")
        return
    
    max_concurrent = int(os.getenv('WORKER_MAX_CONCURRENT', '10'))
    queue_length = redis_client.llen(QUEUE_GO_SOFT)
    
    logger.info(f"🚀 Go-Soft Worker đã khởi động")
    logger.info(f"📋 Queue: {QUEUE_GO_SOFT}, Số job trong queue: {queue_length}, Số job đồng thời tối đa: {max_concurrent}")
    
    # Create semaphore to limit concurrent jobs
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Run async worker loop
    try:
        asyncio.run(worker_loop(redis_client, semaphore, max_concurrent=max_concurrent))
    except KeyboardInterrupt:
        # Suppress KeyboardInterrupt ở level này
        logger.info("⏹️ Worker đã dừng")
    except Exception as e:
        # Chỉ log các exception thực sự, không phải CancelledError
        if not isinstance(e, (asyncio.CancelledError, KeyboardInterrupt)):
            logger.error(f"❌ Fatal error: {e}", exc_info=True)

if __name__ == '__main__':
    main()
import sys
import os
import json
import asyncio
import logging
import base64
import threading

# Get the project root directory (tool-gotax)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import shared modules
from shared.redis_client import get_redis_client, publish_progress, is_job_cancelled

# Import tool-go-quick modules
tool_go_quick_path = os.path.join(project_root, 'tool-go-quick')
sys.path.insert(0, tool_go_quick_path)

# Import model cache và extractor
from api.routes import get_model_cache, get_cccd_extractor_streaming

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis queues
QUEUE_GO_QUICK = 'go-quick:jobs'

async def process_go_quick_job(job_data):
    job_id = job_data.get('job_id')
    action = job_data.get('action', 'process-pdf')
    params = job_data.get('params', {})
    
    logger.info(f"[Job {job_id}] ⚡ Bắt đầu xử lý job, action={action}")
    
    redis_client = get_redis_client()
    
    try:
        if is_job_cancelled(job_id):
            logger.info(f"[Job {job_id}] Job đã bị cancel trước khi xử lý")
            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
            publish_progress(job_id, 0, "Job đã bị hủy")
            return
        
        # Update status: processing
        redis_client.set(f"job:{job_id}:status", "processing".encode('utf-8'))
        
        # Extract params
        file_path = params.get('file_path')
        file_type = params.get('file_type', 'pdf')  # pdf, excel, zip, images
        
        if not file_path:
            error_msg = "Thiếu thông tin: file_path"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return
        
        # Xác định func_type dựa trên action
        total_cccd = 0  # Sẽ được set sau khi có kết quả
        if action == 'process-pdf':
            func_type = 2  # PDF
            logger.info(f"[Job {job_id}] Bắt đầu xử lý PDF")
            publish_progress(job_id, 0, "Bắt đầu xử lý PDF...")
        elif action == 'process-excel':
            func_type = 3  # Excel
            logger.info(f"[Job {job_id}] Bắt đầu xử lý Excel")
            publish_progress(job_id, 0, "Bắt đầu xử lý Excel...")
        elif action == 'process-cccd':
            func_type = 1  # CCCD/ZIP
            logger.info(f"[Job {job_id}] Bắt đầu xử lý CCCD")
            publish_progress(job_id, 0, "Bắt đầu xử lý CCCD...")
        else:
            error_msg = f"Action không hợp lệ: {action}"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return
        
        # Read file content
        if not os.path.exists(file_path):
            error_msg = f"File không tồn tại: {file_path}"
            logger.error(f"[Job {job_id}] {error_msg}")
            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
            publish_progress(job_id, 0, error_msg)
            return
        
        # Get file name from path
        file_name = os.path.basename(file_path)
        
        # Read file as bytes
        with open(file_path, 'rb') as f:
            file_content = f.read()
        
        logger.info(f"[Job {job_id}] Đọc file: {file_name} ({len(file_content)} bytes)")
        
        # Load model cache (nếu chưa load)
        logger.info(f"[Job {job_id}] Đang load model cache...")
        model_cache = get_model_cache()
        logger.info(f"[Job {job_id}] Models đã sẵn sàng")
        
        # Tạo CCCDExtractor instance
        logger.info(f"[Job {job_id}] Đang tạo CCCDExtractor instance...")
        CCCDExtractorClass = get_cccd_extractor_streaming()
        extractor = CCCDExtractorClass(cached_models=model_cache)
        logger.info(f"[Job {job_id}] CCCDExtractor instance đã được tạo")
        
        # Tạo task
        task = {
            "func_type": func_type,
            "inp_path": file_content,  # Pass bytes directly
            "job_id": job_id  # Pass job_id để có thể publish progress trong DetectWorker
        }
        
        logger.info(f"[Job {job_id}] Bắt đầu xử lý với func_type={func_type}")
        
        # Check cancellation trước khi xử lý
        if is_job_cancelled(job_id):
            logger.info(f"[Job {job_id}] Job đã bị cancel trước khi xử lý")
            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
            publish_progress(job_id, 0, "Job đã bị hủy")
            return
        
        # Gọi trực tiếp handle_task (không qua HTTP)
        # Chạy trong thread pool để không block event loop
        logger.info(f"[Job {job_id}] Đang gọi extractor.handle_task() trong thread pool...")
        try:
            # Chạy handle_task trong thread pool để không block event loop
            # Điều này cho phép nhiều jobs chạy song song thực sự
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, extractor.handle_task, task)
            logger.info(f"[Job {job_id}] extractor.handle_task() đã hoàn thành")
        except Exception as e:
            # Nếu exception là do cancellation, handle riêng
            if "đã bị hủy" in str(e) or "Job đã bị hủy" in str(e):
                logger.info(f"[Job {job_id}] Job đã bị hủy trong quá trình xử lý: {e}")
                redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                publish_progress(job_id, 0, "Job đã bị hủy")
                return
            # Nếu là exception khác, re-raise để được handle ở ngoài
            raise
        
        # Check cancellation sau khi xử lý
        if is_job_cancelled(job_id):
            logger.info(f"[Job {job_id}] Job đã bị cancel sau khi xử lý")
            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
            publish_progress(job_id, 0, "Job đã bị hủy")
            return
        
        logger.info(f"[Job {job_id}] Đã xử lý xong")
        logger.info(f"[Job {job_id}] Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
        
        # Lấy total_cccd từ kết quả (func_type 1, 2 hoặc 3)
        if isinstance(result, dict) and result.get("status") == "success":
            # Lấy total_cccd từ result (có thể từ total_rows hoặc total_cccd)
            if "total_cccd" in result:
                total_cccd = result.get("total_cccd", 0)
            elif "total_rows" in result:
                # Excel: total_rows = số CCCD
                total_cccd = result.get("total_rows", 0)
            elif "total_images" in result:
                # PDF: total_images // 2 = số CCCD
                total_cccd = result.get("total_images", 0) // 2
            else:
                total_cccd = 0
            
            if total_cccd > 0:
                # Lưu total_cccd vào Redis để frontend có thể hiển thị
                redis_client.set(f"job:{job_id}:total_cccd", str(total_cccd))
                # Publish progress với format 0/total_cccd và 0% - GỬI total_cccd trong message
                publish_progress(job_id, 0, f"Bắt đầu xử lý... (0/{total_cccd} CCCD - 0%)", total_cccd=total_cccd, processed_cccd=0)
                logger.info(f"[Job {job_id}] ✅ Tổng số CCCD: {total_cccd}")
            else:
                logger.warning(f"[Job {job_id}] ⚠️ Không lấy được total_cccd từ result")
        
        # Nếu có zip_base64 (PDF/Excel), decode và gọi lại với func_type=1
        if isinstance(result, dict) and result.get("status") == "success" and result.get("zip_base64"):
            zip_base64 = result.get("zip_base64")
            zip_bytes = base64.b64decode(zip_base64)
            
            logger.info(f"[Job {job_id}] Đã convert xong, bắt đầu OCR với {len(zip_bytes)} bytes")
            # Publish progress với format 0/total_cccd nếu đã có total_cccd
            if total_cccd > 0:
                publish_progress(job_id, 20, f"Đang xử lý OCR... (0/{total_cccd} CCCD - 20%)", total_cccd=total_cccd, processed_cccd=0)
            else:
                publish_progress(job_id, 20, "Đang xử lý OCR...")
            
            # Tạo task2 để xử lý OCR
            task2 = {
                "func_type": 1,  # Chuyển sang xử lý CCCD
                "inp_path": zip_bytes,
                "job_id": job_id,
                "total_cccd": total_cccd  # Pass total_cccd để DetectWorker có thể publish progress
            }
            
            # Gọi lại handle_task với func_type=1
            logger.info(f"[Job {job_id}] Đang gọi extractor.handle_task() (OCR) trong thread pool...")
            try:
                # Chạy trong thread pool để không block event loop
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, extractor.handle_task, task2)
                logger.info(f"[Job {job_id}] extractor.handle_task() (OCR) đã hoàn thành")
            except Exception as e:
                # Nếu exception là do cancellation, handle riêng
                if "đã bị hủy" in str(e) or "Job đã bị hủy" in str(e):
                    logger.info(f"[Job {job_id}] Job đã bị hủy trong quá trình OCR: {e}")
                    redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                    publish_progress(job_id, 0, "Job đã bị hủy")
                    return
                # Nếu là exception khác, re-raise để được handle ở ngoài
                raise
            
            # Lấy total_cccd từ kết quả func_type=1 (nếu chưa có)
            if isinstance(result, dict) and result.get("status") == "success" and total_cccd == 0:
                total_cccd = result.get("total_cccd", 0)
                if total_cccd > 0:
                    redis_client.set(f"job:{job_id}:total_cccd", str(total_cccd))
                    # Publish lại progress với total_cccd
                    publish_progress(job_id, 20, f"Đang xử lý OCR... (0/{total_cccd} CCCD - 20%)", total_cccd=total_cccd, processed_cccd=0)
                    logger.info(f"[Job {job_id}] ✅ Tổng số CCCD (từ OCR): {total_cccd}")
                else:
                    logger.warning(f"[Job {job_id}] ⚠️ Không lấy được total_cccd từ OCR result")
        
        # Save result to Redis
        result_data = {
            'status': 'success',
            'data': result
        }
        
        redis_client.set(f"job:{job_id}:result", json.dumps(result_data, ensure_ascii=False).encode('utf-8'))
        redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
        
        # Publish final progress
        customer_count = 0
        if isinstance(result, dict) and 'customer' in result:
            customer_count = len(result.get('customer', []))
        
        # Lấy total_cccd từ result nếu chưa có (func_type=1 trực tiếp)
        if total_cccd == 0 and isinstance(result, dict):
            total_cccd = result.get("total_cccd", 0)
            if total_cccd == 0:
                # Fallback: dùng customer_count nếu không có total_cccd
                total_cccd = customer_count
            if total_cccd > 0:
                redis_client.set(f"job:{job_id}:total_cccd", str(total_cccd))
                logger.info(f"[Job {job_id}] ✅ Lấy total_cccd từ result cuối: {total_cccd}")
        
        # Publish progress với format cuối cùng
        if total_cccd > 0:
            publish_progress(job_id, 100, f"Hoàn thành! Đã xử lý {customer_count}/{total_cccd} CCCD (100%)", total_cccd=total_cccd, processed_cccd=customer_count)
            logger.info(f"[Job {job_id}] ✅ Job hoàn thành: {customer_count}/{total_cccd} CCCD")
        else:
            publish_progress(job_id, 100, f"Hoàn thành! Đã xử lý {customer_count} CCCD", processed_cccd=customer_count)
            logger.warning(f"[Job {job_id}] ⚠️ Job hoàn thành: {customer_count} CCCD (không có total_cccd)")
        
    except Exception as e:
        error_msg = f"Lỗi xử lý job: {str(e)}"
        logger.error(f"[Job {job_id}] {error_msg}", exc_info=True)
        redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
        redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
        publish_progress(job_id, 0, error_msg)
    finally:
        # Cleanup: Delete temp file
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"[Job {job_id}] Đã xóa temp file: {file_path}")
        except Exception as e:
            logger.warning(f"[Job {job_id}] Không thể xóa temp file: {e}")

async def process_job_wrapper(job_data):
    """Wrapper để xử lý job trong background task"""
    job_id = job_data.get('job_id', 'unknown')
    logger.info(f"[Job {job_id}] 🔄 process_job_wrapper được gọi")
    try:
        await process_go_quick_job(job_data)
        logger.info(f"[Job {job_id}] ✅ process_job_wrapper hoàn thành")
    except Exception as e:
        logger.error(f"❌ Error processing job {job_id}: {e}", exc_info=True)

async def main():
    """Main worker loop - xử lý nhiều jobs parallel"""
    redis_client = get_redis_client()
    logger.info(f"🚀 Go-Quick Worker started, listening on queue: {QUEUE_GO_QUICK}")
    logger.info(f"📊 Worker sẽ xử lý nhiều jobs parallel (không block)")
    
    # Set để track các tasks đang chạy
    running_tasks = set()
    max_concurrent_jobs = 10  # Số lượng jobs tối đa chạy cùng lúc
    
    while True:
        try:
            # Chỉ lấy job mới nếu chưa đạt max concurrent
            if len(running_tasks) < max_concurrent_jobs:
                # Blocking pop from queue (wait up to 1 second)
                # Chạy blpop trong thread pool để không block event loop
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, lambda: redis_client.blpop([QUEUE_GO_QUICK], timeout=1))
                
                if result:
                    queue_name, job_data_json = result
                    job_data = json.loads(job_data_json.decode('utf-8'))
                    
                    job_id = job_data.get('job_id')
                    logger.info(f"📥 Received job: {job_id} (Running: {len(running_tasks)}/{max_concurrent_jobs})")
                    
                    # Tạo task để xử lý job trong background
                    logger.info(f"[Job {job_id}] 🔄 Tạo asyncio task để xử lý...")
                    task = asyncio.create_task(process_job_wrapper(job_data))
                    running_tasks.add(task)
                    logger.info(f"[Job {job_id}] ✅ Task đã được tạo và thêm vào running_tasks (Total running: {len(running_tasks)})")
                    
                    # Xóa task khỏi set khi hoàn thành
                    def remove_task(task):
                        running_tasks.discard(task)
                        logger.debug(f"[Job {job_id}] 🗑️ Task đã hoàn thành, đã xóa khỏi running_tasks")
                    
                    task.add_done_callback(remove_task)
            else:
                # Đã đạt max concurrent, đợi một chút
                await asyncio.sleep(0.1)
            
            # Cleanup completed tasks
            running_tasks = {t for t in running_tasks if not t.done()}
                
        except KeyboardInterrupt:
            logger.info("⏹️ Worker dừng bởi người dùng, đợi các jobs đang chạy hoàn thành...")
            # Đợi tất cả tasks hoàn thành
            if running_tasks:
                await asyncio.gather(*running_tasks, return_exceptions=True)
            break
        except Exception as e:
            logger.error(f"❌ Error in worker loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # Wait before retry

if __name__ == '__main__':
    asyncio.run(main())

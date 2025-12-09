"""
Job Manager cho async processing
Lưu trữ job status và results trong memory (có thể mở rộng sang Redis/DB sau)
"""
import uuid
import threading
import time
import json
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
from enum import Enum

class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class JobData:
    def __init__(self, job_id: str, func_type: int, inp_data: bytes):
        self.job_id = job_id
        self.func_type = func_type
        self.inp_data = inp_data
        self.status = JobStatus.PENDING
        self.progress = 0
        self.message = "Đang khởi tạo..."
        self.result = None
        self.error = None
        self.created_at = datetime.now()
        self.updated_at = datetime.now()
        self.thread = None
        # Tracking cho CCCD
        self.total_cccd = 0  # Tổng số CCCD cần xử lý
        self.processed_cccd = 0  # Số CCCD đã xử lý xong
        self.total_images = 0  # Tổng số ảnh
        self.processed_images = 0  # Số ảnh đã xử lý
    
    def to_dict(self):
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "progress": min(100, max(0, self.progress)),  # Đảm bảo 0-100%
            "message": self.message,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "total_cccd": self.total_cccd,
            "processed_cccd": self.processed_cccd,
            "total_images": self.total_images,
            "processed_images": self.processed_images
        }

class JobManager:
    """Quản lý jobs - singleton pattern với giới hạn concurrent jobs"""
    _instance = None
    _lock = threading.Lock()
    
    # Config
    MAX_CONCURRENT_JOBS = 3  # Giới hạn số jobs chạy đồng thời
    MAX_QUEUE_SIZE = 50  # Giới hạn số jobs trong queue
    MAX_RESULT_SIZE_MB = 10  # Giới hạn kích thước result (MB)
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.jobs: Dict[str, JobData] = {}
        self.jobs_lock = threading.Lock()
        self.cleanup_thread = None
        self.running = True
        self.running_jobs_count = 0  # Đếm số jobs đang chạy
        self.job_queue = []  # Queue cho jobs chờ
        
        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_old_jobs, daemon=True)
        self.cleanup_thread.start()
        
        self._initialized = True
    
    def create_job(self, func_type: int, inp_data: bytes) -> str:
        """Tạo job mới và trả về job_id"""
        # Kiểm tra queue size
        with self.jobs_lock:
            if len(self.job_queue) >= self.MAX_QUEUE_SIZE:
                raise Exception(f"Queue đầy. Vui lòng thử lại sau. (Max: {self.MAX_QUEUE_SIZE} jobs)")
        
        job_id = str(uuid.uuid4())
        job = JobData(job_id, func_type, inp_data)
        
        with self.jobs_lock:
            self.jobs[job_id] = job
        
        return job_id
    
    def get_job(self, job_id: str) -> Optional[JobData]:
        """Lấy job theo job_id"""
        with self.jobs_lock:
            return self.jobs.get(job_id)
    
    def get_queue_info(self) -> dict:
        """Lấy thông tin về queue và running jobs"""
        with self.jobs_lock:
            return {
                "running": self.running_jobs_count,
                "max_concurrent": self.MAX_CONCURRENT_JOBS,
                "queue_size": len(self.job_queue),
                "max_queue": self.MAX_QUEUE_SIZE,
                "total_jobs": len(self.jobs)
            }
    
    def update_job(self, job_id: str, status: Optional[JobStatus] = None, 
                   progress: Optional[int] = None, message: Optional[str] = None,
                   result: Optional[Any] = None, error: Optional[str] = None,
                   total_cccd: Optional[int] = None, processed_cccd: Optional[int] = None,
                   total_images: Optional[int] = None, processed_images: Optional[int] = None):
        """Cập nhật job status"""
        with self.jobs_lock:
            job = self.jobs.get(job_id)
            if not job:
                return False
            
            if status:
                job.status = status
            if progress is not None:
                # Đảm bảo progress không vượt quá 100%
                job.progress = min(100, max(0, progress))
            if message:
                job.message = message
            if result is not None:
                job.result = result
            if error:
                job.error = error
            if total_cccd is not None:
                job.total_cccd = total_cccd
            if processed_cccd is not None:
                job.processed_cccd = processed_cccd
            if total_images is not None:
                job.total_images = total_images
            if processed_images is not None:
                job.processed_images = processed_images
            
            job.updated_at = datetime.now()
            return True
    
    def start_job(self, job_id: str, worker_func):
        """Start job trong background thread (hoặc thêm vào queue nếu đã đủ concurrent jobs)"""
        with self.jobs_lock:
            job = self.jobs.get(job_id)
            if not job or job.status != JobStatus.PENDING:
                return False
            
            # Kiểm tra số jobs đang chạy
            if self.running_jobs_count >= self.MAX_CONCURRENT_JOBS:
                # Thêm vào queue
                self.job_queue.append((job_id, worker_func))
                job.message = f"Đang chờ trong queue... (Vị trí: {len(self.job_queue)})"
                return True
            
            # Có thể chạy ngay
            self._start_job_immediately(job_id, worker_func)
            return True
    
    def _start_job_immediately(self, job_id: str, worker_func):
        """Start job ngay lập tức"""
        job = self.jobs.get(job_id)
        if not job:
            return
        
        job.status = JobStatus.RUNNING
        self.running_jobs_count += 1
        
        # Xóa input data để tiết kiệm RAM (đã được pass vào worker_func)
        # job.inp_data = None  # Comment lại vì worker_func cần dùng
        
        job.thread = threading.Thread(
            target=self._run_job,
            args=(job_id, worker_func),
            daemon=True
        )
        job.thread.start()
    
    def _process_queue(self):
        """Xử lý queue khi có slot trống"""
        with self.jobs_lock:
            if self.running_jobs_count < self.MAX_CONCURRENT_JOBS and self.job_queue:
                job_id, worker_func = self.job_queue.pop(0)
                self._start_job_immediately(job_id, worker_func)
    
    def _run_job(self, job_id: str, worker_func):
        """Chạy job trong background"""
        try:
            job = self.get_job(job_id)
            if not job:
                return
            
            # Call worker function với callback để update progress
            def progress_callback(progress: int, message: str, 
                                 total_cccd: int = None, processed_cccd: int = None,
                                 total_images: int = None, processed_images: int = None):
                self.update_job(
                    job_id, 
                    progress=progress, 
                    message=message,
                    total_cccd=total_cccd,
                    processed_cccd=processed_cccd,
                    total_images=total_images,
                    processed_images=processed_images
                )
            
            result = worker_func(job.func_type, job.inp_data, progress_callback)
            
            # Kiểm tra kích thước result
            import sys
            result_size_mb = sys.getsizeof(str(result)) / (1024 * 1024)
            if result_size_mb > self.MAX_RESULT_SIZE_MB:
                # Nếu result quá lớn, chỉ lưu metadata
                result = {
                    "status": "success",
                    "message": f"Kết quả quá lớn ({result_size_mb:.1f}MB). Vui lòng sử dụng streaming API.",
                    "size_mb": result_size_mb,
                    "note": "Result too large, use streaming API instead"
                }
            
            # Job completed
            self.update_job(
                job_id,
                status=JobStatus.COMPLETED,
                progress=100,
                message="Hoàn thành",
                result=result
            )
            
            # Xóa input data để tiết kiệm RAM
            with self.jobs_lock:
                job = self.jobs.get(job_id)
                if job:
                    job.inp_data = None
            
        except Exception as e:
            import traceback
            error_msg = str(e)
            print(f"Job {job_id} failed: {error_msg}")
            traceback.print_exc()
            self.update_job(
                job_id,
                status=JobStatus.FAILED,
                progress=100,
                message="Lỗi xử lý",
                error=error_msg
            )
        finally:
            # Giảm số jobs đang chạy và xử lý queue
            with self.jobs_lock:
                self.running_jobs_count = max(0, self.running_jobs_count - 1)
            
            # Xử lý queue nếu có
            self._process_queue()
    
    def _cleanup_old_jobs(self):
        """Xóa jobs cũ hơn 1 giờ"""
        while self.running:
            try:
                time.sleep(300)  # Check every 5 minutes
                
                cutoff_time = datetime.now() - timedelta(hours=1)
                
                with self.jobs_lock:
                    to_remove = [
                        job_id for job_id, job in self.jobs.items()
                        if job.updated_at < cutoff_time and job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]
                    ]
                    
                    for job_id in to_remove:
                        del self.jobs[job_id]
                        print(f"Cleaned up old job: {job_id}")
            
            except Exception as e:
                print(f"Error in cleanup thread: {e}")
    
    def cancel_job(self, job_id: str) -> bool:
        """Hủy job (nếu đang chạy)"""
        with self.jobs_lock:
            job = self.jobs.get(job_id)
            if not job:
                return False
            
            if job.status == JobStatus.RUNNING:
                # Note: Python threads không thể kill được, chỉ đánh dấu
                job.status = JobStatus.CANCELLED
                job.message = "Đã hủy"
                return True
            
            return False

# Global instance
job_manager = JobManager()


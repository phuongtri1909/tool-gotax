
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional

class ProgressTracker:
    """Lớp theo dõi tiến trình xử lý cho mỗi request (dựa trên token người dùng)"""
    
    _instances: Dict[str, 'ProgressTracker'] = {}
    _lock = threading.Lock()
    
    def __init__(self, token: str):
        self.token = token  # Thay vì request_id, sử dụng token
        self.start_time = datetime.now()
        self.status = "processing"  # processing, completed, failed
        self.total_invoices = 0
        self.processed_invoices = 0
        self.current_step = "Initializing..."
        self.error_message = None
        self.result = None
        self.progress_percentage = 0
        
    @classmethod
    def get_or_create(cls, token: str) -> 'ProgressTracker':
        """Lấy hoặc tạo mới progress tracker cho token"""
        with cls._lock:
            if token not in cls._instances:
                cls._instances[token] = ProgressTracker(token)
            return cls._instances[token]
    
    @classmethod
    def get(cls, token: str) -> Optional['ProgressTracker']:
        """Lấy progress tracker nếu tồn tại"""
        with cls._lock:
            return cls._instances.get(token)
    
    def update(self, 
               current_step: str = None,
               processed: int = None,
               total: int = None,
               percentage: int = None):
        """Cập nhật tiến trình"""
        with ProgressTracker._lock:
            if current_step:
                self.current_step = current_step
            if processed is not None:
                self.processed_invoices = processed
            if total is not None:
                self.total_invoices = total
            if percentage is not None:
                self.progress_percentage = percentage
            elif self.total_invoices > 0:
                # Auto-calculate percentage nếu không được cung cấp
                self.progress_percentage = min(99, int(
                    (self.processed_invoices / self.total_invoices) * 100
                ))
    
    def complete(self, result: Any = None):
        """Đánh dấu là hoàn thành"""
        with ProgressTracker._lock:
            self.status = "completed"
            self.progress_percentage = 100
            self.current_step = "Completed"
            self.result = result
    
    def fail(self, error_message: str):
        """Đánh dấu là thất bại"""
        with ProgressTracker._lock:
            self.status = "failed"
            self.error_message = error_message
            self.current_step = f"Error: {error_message}"
    
    def get_status(self) -> Dict[str, Any]:
        """Lấy tình trạng hiện tại"""
        elapsed_seconds = (datetime.now() - self.start_time).total_seconds()
        
        status_dict = {
            "token": self.token,  # Thay vì request_id
            "status": self.status,
            "progress_percentage": self.progress_percentage,
            "current_step": self.current_step,
            "processed_invoices": self.processed_invoices,
            "total_invoices": self.total_invoices,
            "elapsed_seconds": int(elapsed_seconds),
            "start_time": self.start_time.isoformat(),
        }
        
        if self.error_message:
            status_dict["error"] = self.error_message
        
        # Ước tính thời gian còn lại nếu có tốc độ xử lý
        if self.processed_invoices > 0 and elapsed_seconds > 0:
            invoices_per_second = self.processed_invoices / elapsed_seconds
            remaining_invoices = self.total_invoices - self.processed_invoices
            if invoices_per_second > 0:
                estimated_remaining = int(remaining_invoices / invoices_per_second)
                status_dict["estimated_remaining_seconds"] = estimated_remaining
        
        return status_dict
    
    @classmethod
    def cleanup(cls, token: str, keep_completed: bool = False):
        """Xóa progress tracker (tùy chọn giữ lại nếu hoàn thành)"""
        with cls._lock:
            tracker = cls._instances.get(token)
            if tracker:
                if not keep_completed or tracker.status != "completed":
                    del cls._instances[token]
    
    @classmethod
    def cleanup_old(cls, max_age_seconds: int = 3600):
        """Xóa các tracker cũ (mặc định 1 giờ)"""
        with cls._lock:
            now = datetime.now()
            to_delete = []
            
            for request_id, tracker in cls._instances.items():
                age = (now - tracker.start_time).total_seconds()
                if age > max_age_seconds:
                    to_delete.append(request_id)
            
            for request_id in to_delete:
                del cls._instances[request_id]


# Global cleanup thread (optional - runs every 30 minutes)
def _start_cleanup_thread():
    """Khởi động thread dọn dẹp tự động"""
    def cleanup_worker():
        while True:
            time.sleep(30 * 60)  # Chạy mỗi 30 phút
            ProgressTracker.cleanup_old(max_age_seconds=3600)  # Xóa tracker cũ hơn 1 giờ
    
    thread = threading.Thread(target=cleanup_worker, daemon=True)
    thread.start()

# Khởi động cleanup thread khi module được import
_start_cleanup_thread()

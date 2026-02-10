"""
Download Service - Lưu file vào filesystem và quản lý download_id
Tương tự như Go-Soft pattern
"""
import os
import uuid
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Storage directory cho các file download (giống Go-Soft pattern: tool-go-invoice/temp)
STORAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'temp')

# Đảm bảo thư mục tồn tại
os.makedirs(STORAGE_DIR, exist_ok=True)


def save_file_to_disk(file_bytes: bytes, file_extension: str = 'zip') -> Tuple[str, str]:
    """
    Lưu file vào disk và trả về download_id và file path
    
    Args:
        file_bytes: Bytes của file cần lưu
        file_extension: Extension của file (zip, xlsx, pdf, ...)
    
    Returns:
        Tuple (download_id, file_path)
    """
    # Tạo download_id (UUID)
    download_id = str(uuid.uuid4())
    
    # Tạo file path
    file_path = os.path.join(STORAGE_DIR, f"{download_id}.{file_extension}")
    
    # Lưu file vào disk
    try:
        with open(file_path, 'wb') as f:
            f.write(file_bytes)
        
        file_size = os.path.getsize(file_path)
        logger.info(f"✅ Đã lưu file: {file_path} (download_id: {download_id}, size: {file_size} bytes)")
        
        return download_id, file_path
    except Exception as e:
        logger.error(f"❌ Lỗi khi lưu file: {e}")
        raise


def get_file_path(download_id: str, file_extension: str = 'zip') -> Optional[str]:
    """
    Lấy file path từ download_id
    
    Args:
        download_id: Download ID
        file_extension: Extension của file
    
    Returns:
        File path nếu tồn tại, None nếu không
    """
    file_path = os.path.join(STORAGE_DIR, f"{download_id}.{file_extension}")
    
    if os.path.exists(file_path):
        return file_path
    
    return None


def delete_file(download_id: str, file_extension: str = 'zip') -> bool:
    """
    Xóa file từ disk
    
    Args:
        download_id: Download ID
        file_extension: Extension của file
    
    Returns:
        True nếu xóa thành công, False nếu không
    """
    file_path = get_file_path(download_id, file_extension)
    
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"✅ Đã xóa file: {file_path}")
            return True
        except Exception as e:
            logger.error(f"❌ Lỗi khi xóa file: {e}")
            return False
    
    return False

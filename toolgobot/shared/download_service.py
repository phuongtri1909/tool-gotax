"""
Download Service - Go Bot: lưu file theo chunk (ghi + đọc stream)
- Ghi file: ghi từ buffer xuống disk theo chunk 8KB (tránh giữ cả file trong RAM khi ghi).
- Download: API endpoint stream file theo chunk 8KB (dùng ở routes).
"""
import os
import uuid
import logging
from io import BytesIO
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

CHUNK_SIZE = 8192  # 8KB (giống Go Invoice / Go Soft)

# Thư mục temp cho file download: tool-gobot/temp
STORAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'temp')
os.makedirs(STORAGE_DIR, exist_ok=True)


def save_file_to_disk(file_bytes: bytes, file_extension: str = 'xlsx') -> Tuple[str, str]:
    """
    Lưu file xuống disk theo chunk 8KB (giai đoạn ghi – chunk khi ghi).
    Trả về download_id và file path.

    Args:
        file_bytes: Bytes của file cần lưu
        file_extension: Extension (xlsx, zip, ...)

    Returns:
        Tuple (download_id, file_path)
    """
    download_id = str(uuid.uuid4())
    file_path = os.path.join(STORAGE_DIR, f"{download_id}.{file_extension}")

    try:
        with open(file_path, 'wb') as f:
            buf = BytesIO(file_bytes)
            while True:
                chunk = buf.read(CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)

        file_size = os.path.getsize(file_path)
        logger.info(f"✅ Đã lưu file (chunk 8KB): {file_path} (download_id: {download_id}, size: {file_size} bytes)")
        return download_id, file_path
    except Exception as e:
        logger.error(f"❌ Lỗi khi lưu file: {e}")
        raise


def get_file_path(download_id: str, file_extension: str = 'xlsx') -> Optional[str]:
    """Lấy file path từ download_id."""
    file_path = os.path.join(STORAGE_DIR, f"{download_id}.{file_extension}")
    if os.path.exists(file_path):
        return file_path
    return None


def delete_file(download_id: str, file_extension: str = 'xlsx') -> bool:
    """Xóa file từ disk."""
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

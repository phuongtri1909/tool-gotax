
"""
Go Invoice Backend - Module-only
Tất cả xử lý phải qua API, không test offline tại đây.
Các utility functions được exported để sử dụng ở đây nếu cần.
"""
import os
import sys
import base64
from datetime import datetime, timedelta

# Thêm đường dẫn để import từ thư mục backend_
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend_'))
from InvoiceBackend import InvoiceBackend


def save_file_from_base64(base64_bytes: str, filename: str, output_dir: str = "output"):
    """Lưu file từ base64 string"""
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        file_bytes = base64.b64decode(base64_bytes)
        file_path = os.path.join(output_dir, filename)
        with open(file_path, 'wb') as f:
            f.write(file_bytes)
        return file_path
    except Exception as e:
        print(f"Lỗi lưu file {filename}: {e}")
        return None


# Tất cả request phải qua API
# Workflow:
# 1. Client GET /api/go-invoice/get-captcha -> nhận ckey + captcha SVG
# 2. Client POST /api/go-invoice/login -> nhận token/headers
# 3. Client POST /api/go-invoice/tongquat -> nhận excel data
# 4. Client POST /api/go-invoice/chitiet (optional)
# 5. Client POST /api/go-invoice/xmlhtml (optional)
# 6. Client POST /api/go-invoice/pdf (optional)


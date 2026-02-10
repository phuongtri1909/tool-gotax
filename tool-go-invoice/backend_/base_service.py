import requests
import uuid
import os
from PyQt5.QtGui import QPixmap

class BaseService:
    def __init__(self, proxy_url=None):
        # ✅ Khởi tạo session bằng requests.Session()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PostmanRuntime/7.43.4",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9",
            "Connection": "close"
        })
        self.tmp_dir = "temp"
        self.proxy_url = proxy_url  # ✅ Lưu proxy URL để recreate session
        self.session_id = str(uuid.uuid4())[:8]
        
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        
        # ✅ Setup proxy ONCE khi khởi tạo session
        if proxy_url:
            self.session.proxies = {
                'http': proxy_url
            }
            print(f"✅ Proxy configured: {proxy_url}")
    
    def _recreate_session_with_new_proxy(self):
        """
        ✅ Tạo session mới + add proxy lại (IP tự đổi)
        Thay vì delay/backoff khi 429
        """
        print(f"🔄 Recreating session + rotating proxy IP...")
        # ✅ Tạo session mới bằng requests.Session()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "PostmanRuntime/7.43.4",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "vi-VN,vi;q=0.9",
            "Connection": "close"
        })
        # Add proxy lại (Luna Proxy tự đổi IP)
        if self.proxy_url:
            self.session.proxies = {
                'http': self.proxy_url
            }
        
        return self.session
    
    def set_proxy(self, proxy_url):
        """✅ Thiết lập proxy cho tất cả HTTP requests. None nghĩa là không dùng proxy."""
        self.proxy_url = proxy_url
        if proxy_url:
            self.session.proxies = {
                'http': proxy_url
            }
            print(f"✅ Proxy updated: {proxy_url}")
    def save_captcha_svg_to_png(self, svg_content: str, filename: str = "captcha.png") -> str:
        """
        Nhận nội dung SVG (string) -> render ra PNG -> lưu trong temp.
        Trả về full path tới file PNG.
        """
        pixmap = QPixmap()
        # content trả về thường là string svg xml
        pixmap.loadFromData(svg_content.encode("utf-8"))

        output_path = os.path.join(self.tmp_dir, filename)
        pixmap.save(output_path, "PNG")
        return output_path
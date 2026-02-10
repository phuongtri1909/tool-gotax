import os
from datetime import datetime, timedelta
from PIL import Image
from io import BytesIO
import base64
import json

# ✅ Không cần sys.path - relative imports hoạt động tốt


class InvoiceBackend:
    def __init__(self, proxy_url=None, job_id=None):
        # Lazy load services chỉ khi cần dùng
        self._auth_service = None
        self._backend_service = None
        self.captcha_dir = "captcha"
        self.proxy_url = proxy_url  # ✅ Lưu proxy URL
        self.job_id = job_id  # ✅ Lưu job_id để check cancelled flag
        
        if not os.path.exists(self.captcha_dir):
            os.makedirs(self.captcha_dir)
    
    def set_proxy(self, proxy_url):
        """✅ Thiết lập proxy cho tất cả requests - cập nhật cho services đã load"""
        self.proxy_url = proxy_url
        # Cập nhật proxy cho services đã load
        if self._auth_service:
            self._auth_service.set_proxy(proxy_url)
        if self._backend_service:
            self._backend_service.set_proxy(proxy_url)
    
    @property
    def auth_service(self):
        """Lazy load AuthService"""
        if self._auth_service is None:
            from backend_.auth_service import AuthService
            self._auth_service = AuthService(proxy_url=self.proxy_url)
        return self._auth_service
    
    @property
    def backend_service(self):
        """Lazy load BackendService"""
        if self._backend_service is None:
            from backend_.backend_service import BackendService
            self._backend_service = BackendService(proxy_url=self.proxy_url, job_id=self.job_id)
        return self._backend_service

    def get_and_save_captcha(self):
        """Lấy captcha từ API và lưu ảnh"""
        try:
            captcha_data = self.auth_service.getckey_captcha()
            # ✅ Kiểm tra captcha_data có phải là dict không
            if not isinstance(captcha_data, dict):
                return None, None
            
            ckey = captcha_data.get('ckey')
            svg_content = captcha_data.get('svg_content')
            
            if not ckey or not svg_content:
                return None, None
            
            captcha_path = self.save_svg_to_png(svg_content)
            
            return ckey, captcha_path
            
        except Exception as e:
            return None, None

    def save_svg_to_png(self, svg_content):
        """Lưu SVG dưới dạng PNG"""
        try:
            svg_bytes = svg_content.encode('utf-8') if isinstance(svg_content, str) else svg_content
            
            svg_path = os.path.join(self.captcha_dir, "captcha.svg")
            png_path = os.path.join(self.captcha_dir, "captcha.png")
            
            with open(svg_path, 'wb') as f:
                f.write(svg_bytes)
            
            try:
                from cairosvg import svg2png
                svg2png(bytestring=svg_bytes, write_to=png_path)
                return png_path
            except ImportError:
                try:
                    from svglib.svglib import svg2rlg
                    from reportlab.graphics import renderPM
                    drawing = svg2rlg(svg_path)
                    renderPM.drawToFile(drawing, png_path, fmt='PNG')
                    return png_path
                except ImportError:
                    return svg_path
        
        except Exception as e:
            return None

    def login(self, username, password, ckey, cvalue):
        """Đăng nhập và lấy token"""
        try:
            headers = self.auth_service.login_web(
                ckey=ckey,
                captcha_inp=cvalue,
                user=username,
                pass_=password
            )
            
            # ✅ Kiểm tra headers có phải là dict không
            if not isinstance(headers, dict):
                return None
            
            if headers.get('status') == 'success':
                return headers
            else:
                return None
        
        except Exception as e:
            return None

    def call_tongquat(self, task:dict):
        type_invoice = task.get("type_invoice", "1")    
        headers = task.get("headers", {})
        start_date = task.get("start_date", None)
        end_date = task.get("end_date", None)
        progress_callback = task.get("progress_callback", None)  # Lấy callback từ task
        
        print("Tổng quát>>>")
        if not start_date:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            start_date_str = start_date.strftime("%d/%m/%Y")
            end_date_str = end_date.strftime("%d/%m/%Y")
        else:
            start_date_str = start_date
            end_date_str = end_date
            
            # ✅ Validation: Kiểm tra ngày không vượt quá ngày hiện tại
            try:
                date_format = "%d/%m/%Y"
                today = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
                
                # Parse start_date và end_date
                start_date_obj = datetime.strptime(start_date_str, date_format)
                end_date_obj = datetime.strptime(end_date_str, date_format)
                
                # Kiểm tra không vượt quá ngày hiện tại
                if start_date_obj > today:
                    return {
                        "status": "error",
                        "message": "Ngày bắt đầu không được vượt quá ngày hiện tại"
                    }
                if end_date_obj > today:
                    return {
                        "status": "error",
                        "message": "Ngày kết thúc không được vượt quá ngày hiện tại"
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Lỗi định dạng ngày: {str(e)}"
                }
        
        try:
            result = self.backend_service.tongquat_(
                type_invoice=type_invoice,
                headers=headers,
                start_date=start_date_str,
                end_date=end_date_str,
                progress_callback=progress_callback  # Truyền callback tới backend service
            )
            
            # ✅ Kiểm tra result có phải là dict không trước khi gọi .get()
            if result and isinstance(result, dict) and result.get('status') == 'success': 
                result.update({
                    "type_invoice": type_invoice,
                    "start_date": start_date_str,
                    "end_date": end_date_str,
                    "headers": headers
                })
                return result
            else:
                # ✅ Nếu result không phải dict hoặc không có status='success'
                error_message = 'Lỗi không xác định'
                if isinstance(result, dict):
                    error_message = result.get('message', error_message)
                elif isinstance(result, str):
                    error_message = result
                return {
                    "status": "error",
                    "message": error_message
                }
        
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }
    def call_chitiet(self,raw_data:dict):
        if raw_data.get("status") == "success": 
            headers = raw_data.get("headers", {})
            raw = raw_data.get("datas", [])
        else:
            return raw_data
        print("Chi tiết>>>")
        try:
            progress_callback = raw_data.get("progress_callback", None)
            result = self.backend_service.chitiet_(
                datas_first={"datas": raw},headers=headers,progress_callback=progress_callback
            )
            
            # ✅ Kiểm tra result có phải là dict không trước khi gọi .get()
            if result and isinstance(result, dict) and result.get('status') == 'success': 
                return result
            else:
                # ✅ Nếu result không phải dict hoặc không có status='success'
                error_message = 'Lỗi không xác định'
                if isinstance(result, dict):
                    error_message = result.get('message', error_message)
                elif isinstance(result, str):
                    error_message = result
                return {
                    "status": "error",
                    "message": error_message
                }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }

    def call_xmlahtml(self, raw_data:dict,options:dict = {}):
            if raw_data.get("status") == "success":
                headers = raw_data.get("headers", {})
                raw = raw_data.get("datas", [])
            else:
                return raw_data
            print("Xml a html>>>")
            progress_callback = raw_data.get("progress_callback", None)
            # ✅ Truyền flag _is_pdf_context nếu có (từ getpdf)
            datas_first = {"datas": raw}
            if raw_data.get("_is_pdf_context") == True:
                datas_first["_is_pdf_context"] = True
            
            result = self.backend_service.xmlahtml(
                datas_first=datas_first,headers=headers,type_export=options,progress_callback=progress_callback
            )
            # ✅ Kiểm tra result có phải là dict không trước khi gọi .get()
            if result and isinstance(result, dict) and result.get('status') == 'success': 
                return result
            else:
                # ✅ Nếu result không phải dict hoặc không có status='success'
                error_message = 'Lỗi không xác định'
                if isinstance(result, dict):
                    error_message = result.get('message', error_message)
                elif isinstance(result, str):
                    error_message = result
                return {
                    "status": "error",
                    "message": error_message
                }
    def getpdf(self,raw_data:dict = {}):
        # ✅ Thêm flag để biết đang chạy PDF (không phải HTML thông thường)
        raw_data_with_pdf_flag = raw_data.copy()
        raw_data_with_pdf_flag["_is_pdf_context"] = True
        
        raw_html = self.call_xmlahtml(raw_data_with_pdf_flag,{"xml":False,"html":True})
        
        # ✅ Kiểm tra raw_html có phải là dict không
        if not isinstance(raw_html, dict):
            error_message = 'Lỗi không xác định'
            if isinstance(raw_html, str):
                error_message = raw_html
            return {
                "status": "error",
                "message": f"Lỗi khi lấy HTML: {error_message}"
            }
        
        html_list = raw_html.get("html_list", [])
        progress_callback = raw_data.get("progress_callback", None)
        results = self.backend_service.html2pdf(html_list,progress_callback=progress_callback)
        
        # ✅ Kiểm tra results có phải là dict không
        if not isinstance(results, dict):
            error_message = 'Lỗi không xác định'
            if isinstance(results, str):
                error_message = results
            return {
                "status": "error",
                "message": f"Lỗi khi chuyển đổi PDF: {error_message}"
            }
        
        raw_html.update({
            "message": results.get("message"),
            "data": results.get("data", {}),
            "status": results.get("status", "error"),
            "pdf_list": results.get("pdf_list", [])
        })
        return raw_html
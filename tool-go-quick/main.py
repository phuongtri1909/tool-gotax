import sys
from ultralytics import YOLO
import cv2
import numpy as np
import os
import math
import fitz
from PIL import Image
import sys
import pandas as pd 
from PIL import Image
from io import BytesIO
import zipfile
import base64
import requests
from requests import get
import subprocess
import sys
import uuid
import threading
import torch

_original_torch_load = torch.load
def _patched_torch_load(*args, **kwargs):
    if 'weights_only' not in kwargs:
        kwargs['weights_only'] = False
    return _original_torch_load(*args, **kwargs)
torch.load = _patched_torch_load

try:
        import vietocr
except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "vietocr"])
def count_files( folderPath):
        """ Đếm số file trong thư mục """
        return len([f for f in os.listdir(folderPath) if os.path.isfile(os.path.join(folderPath, f))])
import shutil

class DetectWorker():
    def __init__(self,input_path:str = None,type_:int = 0, cached_models=None, job_id=None, total_cccd=0):
        super().__init__()
        self.path_img = input_path
        self.path_rs = None
        self.type_ = type_
        # Lấy đường dẫn tuyệt đối của thư mục chứa main.py
        main_file_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_dir = os.path.join(main_file_dir, "__pycache__")
        # Cache models để tránh load lại mỗi lần
        self.cached_models = cached_models
        self.model1 = None
        self.model2 = None
        self.model3 = None
        self.vietocr_detector = None
        # Tổng số CCCD (mỗi CCCD = 2 ảnh: mt + ms)
        self.total_cccd = total_cccd
        # Job ID để publish progress
        self.job_id = job_id
        # Tạo unique session ID cho mỗi request để tránh conflict khi chạy đồng thời
        self.session_id = str(uuid.uuid4())[:8]
        self.work_dir = os.path.join(self.base_dir, f"work_{self.session_id}")
        
        # Import is_job_cancelled function
        self.is_job_cancelled_func = None
        if self.job_id:
            try:
                import sys
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                from shared.redis_client import is_job_cancelled
                self.is_job_cancelled_func = is_job_cancelled
            except Exception as e:
                pass
    
    def check_cancellation(self):
        """Check if job has been cancelled. Raise exception if cancelled."""
        if self.job_id and self.is_job_cancelled_func:
            try:
                if self.is_job_cancelled_func(self.job_id):
                    raise Exception("Job đã bị hủy")
            except Exception as e:
                if "đã bị hủy" in str(e):
                    raise
                # Ignore other errors (Redis connection issues, etc.)
                pass
        
    def init_temp_dirs(self):
        """Create temporary directories với unique session ID"""
        self.work_md1 = os.path.join(self.work_dir, "md1", "cropped_results")
        self.work_md2 = os.path.join(self.work_dir, "md2", "detect_results")
        self.work_md3 = os.path.join(self.work_dir, "md3", "detected_results", "crops")
        self.work_md4 = os.path.join(self.work_dir, "md4")
        self.work_temp_rs = os.path.join(self.work_dir, "temp_rs")
        
        os.makedirs(self.work_md1, exist_ok=True)
        os.makedirs(self.work_md2, exist_ok=True)
        os.makedirs(self.work_md3, exist_ok=True)
        os.makedirs(self.work_md4, exist_ok=True)
        os.makedirs(self.work_temp_rs, exist_ok=True)
    
    def cleanup_temp_dirs(self):
        """Clean up temporary directories - chỉ xóa thư mục của session này"""
        try:
            if os.path.exists(self.work_dir):
                shutil.rmtree(self.work_dir)
        except Exception as e:
            print(f"Lỗi xóa work_dir {self.work_dir}: {e}")
    
    def run(self):
        try:
            self.init_temp_dirs()
            
            if self.type_ == 1:
                if self.cached_models:
                    self.model1 = self.cached_models.get('yolo_model1')
                    self.model2 = self.cached_models.get('yolo_model2')
                    self.model3 = self.cached_models.get('yolo_model3')
                    self.vietocr_detector = self.cached_models.get('vietocr_detector')
                
                if self.model1 is None:
                    self.model1 = YOLO(os.path.join(self.base_dir, "best.pt"))
                if self.model2 is None:
                    self.model2 = YOLO(os.path.join(self.base_dir, "best2.pt"))
                if self.model3 is None:
                    self.model3 = YOLO(os.path.join(self.base_dir, "best3.pt"))
                
                self.detect_cccd()
                self.detect_corners()
                self.detect_lines()
                results = self.collect_cus_info()
                return results
            elif self.type_ == 2:
                results = self.pdf_to_png(self.path_img)
                return results
            elif self.type_ == 3:
                results = self.excel_to_png(self.path_img)
                return results
        finally:
            self.cleanup_temp_dirs()
    def pdf_to_png(self, pdf_bytes_input):
        """
        Convert PDF bytes to PNG images
        Args:
            pdf_bytes_input: bytes hoặc str base64 của file PDF duy nhất
        Returns:
            dict: status, message, total_images, zip_base64
        """
        try:
            # Convert base64 string to bytes if needed
            if isinstance(pdf_bytes_input, str):
                pdf_bytes = base64.b64decode(pdf_bytes_input)
            else:
                pdf_bytes = pdf_bytes_input
            
            # Read PDF from bytes
            pdf_stream = BytesIO(pdf_bytes)
            doc = fitz.open(stream=pdf_stream, filetype="pdf")
            output_zip = BytesIO()
            total_images = 0
            
            with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf_out:
                for i, page in enumerate(doc):
                    try:
                        pix = page.get_pixmap()
                        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                        
                        pair_num = (i // 2) + 1
                        side = "mt" if i % 2 == 0 else "ms"
                        img_name = f"{pair_num}{side}.png"
                        
                        img_buf = BytesIO()
                        img.save(img_buf, format="PNG")
                        img_buf.seek(0)
                        
                        zf_out.writestr(img_name, img_buf.getvalue())
                        total_images += 1
                    except Exception as e:
                        print(f"Lỗi xử lý trang {i}: {e}")
                        continue
            
            output_zip.seek(0)
            result_bytes = output_zip.getvalue()
            result_b64 = base64.b64encode(result_bytes).decode("ascii")
            
            # Tính tổng số CCCD: mỗi CCCD = 2 ảnh (mt + ms)
            total_cccd = total_images // 2
            
            return {
                "status": "success",
                "message": "Chuyển PDF → PNG và đóng gói ZIP thành công.",
                "total_images": total_images,
                "total_cccd": total_cccd,
                "zip_name": "images.zip",
                "zip_base64": result_b64
            }
        
        except Exception as e:
            return {
                "status": "error",
                "message": f"Lỗi khi xử lý PDF bytes: {e}"
            }
    def excel_to_png(self, excel_bytes_input):
        """
        Download images from Excel bytes (Google Drive URLs)
        Args:
            excel_bytes_input: bytes hoặc str base64 của file Excel
        Returns:
            dict: status, message, total_rows, total_images, zip_base64
        """
        try:
            # Convert base64 string to bytes if needed
            if isinstance(excel_bytes_input, str):
                excel_bytes = base64.b64decode(excel_bytes_input)
            else:
                excel_bytes = excel_bytes_input
            
            # Read Excel from bytes
            excel_stream = BytesIO(excel_bytes)
            df = pd.read_excel(excel_stream, header=0)  # hàng đầu là header
        except Exception as e:
            return {
                "status": "error",
                "message": f"Lỗi đọc file Excel bytes: {e}"
            }

        if df.shape[1] < 3:
            return {
                "status": "error",
                "message": "File Excel cần ít nhất 3 cột: file_name, mt_url, ms_url."
            }

        # Lấy 3 cột đầu, bỏ các hàng trắng
        df_sub = df.iloc[:, :3].fillna("")
        lines_excel = []
        for _, row in df_sub.iterrows():
            file_name = str(row.iloc[0]).strip()
            mt_url = str(row.iloc[1]).strip()
            ms_url = str(row.iloc[2]).strip()
            if any([file_name, mt_url, ms_url]):  # có dữ liệu
                lines_excel.append((file_name, mt_url, ms_url))

        if not lines_excel:
            return {
                "status": "error",
                "message": "Không có dữ liệu hợp lệ trong file Excel."
            }

        # --- Hàm phụ xử lý Google Drive URL ---
        def extract_file_id(url: str):
            file_id = None
            if "drive.google.com" in url:
                if "/file/d/" in url:
                    file_id = url.split("/file/d/")[1].split("/")[0]
                elif "id=" in url:
                    file_id = url.split("id=")[1].split("&")[0]
                elif "/open?id=" in url:
                    file_id = url.split("/open?id=")[1].split("&")[0]
            return file_id

        def download_from_drive(url: str) -> bytes:
            """
            Tải file từ Google Drive, trả về bytes.
            NẾU thất bại → raise Exception.
            """
            file_id = extract_file_id(url)
            if not file_id:
                raise Exception("Không thể trích xuất ID file từ URL")
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            session = requests.Session()
            response = session.get(download_url, stream=True)
            if response.status_code != 200:
                raise Exception(f"Không tải được file, status {response.status_code}")
            # xử lý token confirm nếu có
            for key, value in response.cookies.items():
                if key.startswith("download_warning"):
                    token = value
                    params = {"id": file_id, "confirm": token}
                    response = session.get(download_url, params=params, stream=True)
                    break
            if response.status_code != 200:
                raise Exception(f"Không tải được file (sau khi confirm), status {response.status_code}")

            buf = BytesIO()
            for chunk in response.iter_content(1024):
                if chunk:
                    buf.write(chunk)
            return buf.getvalue()
        mem_zip = BytesIO()
        total_images = 0

        try:
            count_lines = len(lines_excel)
            with zipfile.ZipFile(mem_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for index,(file_name, mt_url, ms_url) in enumerate(lines_excel,start=1):
                    file_name = file_name.strip()
                    if not file_name:
                        continue
                    print(f"[{index}|{count_lines}]Xử lý: {file_name}")
                    # tải mặt trước
                    if mt_url:
                        try:
                            mt_bytes = download_from_drive(mt_url)
                            mt_name = f"{file_name}mt.png"
                            zf.writestr(mt_name, mt_bytes)
                            total_images += 1
                        except Exception as e:
                            # log lỗi, nhưng không dừng toàn bộ
                            print(f"Lỗi tải mặt trước {file_name}: {e}")

                    # tải mặt sau
                    if ms_url:
                        try:
                            ms_bytes = download_from_drive(ms_url)
                            ms_name = f"{file_name}ms.png"
                            zf.writestr(ms_name, ms_bytes)
                            total_images += 1
                        except Exception as e:
                            print(f"Lỗi tải mặt sau {file_name}: {e}")

            mem_zip.seek(0)
            zip_bytes = mem_zip.getvalue()
            zip_b64 = base64.b64encode(zip_bytes).decode("ascii")

            # Tính tổng số CCCD: total_rows = số CCCD (mỗi dòng Excel = 1 CCCD)
            total_cccd = len(lines_excel)
            
            return {
                "status": "success",
                "message": "Đã tải ảnh từ Excel và đóng gói ZIP thành công.",
                "total_rows": len(lines_excel),
                "total_images": total_images,
                "total_cccd": total_cccd,
                "zip_name": "excel_images.zip",
                "zip_base64": zip_b64
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Lỗi trong quá trình xử lý Excel → ảnh: {e}"
            }
    
    def clear_filefolder(self, folder_path):
        import os
        if os.path.exists(folder_path):
            for file_name in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file_name)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                except Exception as e:
                    print(f"Lỗi khi xóa {file_name}: {e}")
    def collect_cus_info(self):
        print("4.Collect customer info")
        import os
        folder_txt = self.work_temp_rs
        all_files = [f for f in os.listdir(folder_txt) if f.endswith('.txt')]
        stt_set = set()
        for f in all_files:
            if f.endswith('mt.txt'):
                stt_set.add(f[:-6])
            elif f.endswith('ms.txt'):
                stt_set.add(f[:-6])
        # Gộp thành file stt.txt
        for stt in sorted(stt_set):
            mt_file = f"{stt}mt.txt"
            ms_file = f"{stt}ms.txt"
            combined_file = f"{stt}.txt"

            mt_path = os.path.join(folder_txt, mt_file)
            ms_path = os.path.join(folder_txt, ms_file)
            combined_path = os.path.join(folder_txt, combined_file)

            content_mt = ""
            content_ms = ""

            if os.path.isfile(mt_path):
                with open(mt_path, "r", encoding="utf-8") as f:
                    content_mt = f.read()

            if os.path.isfile(ms_path):
                with open(ms_path, "r", encoding="utf-8") as f:
                    content_ms = f.read()

            with open(combined_path, "w", encoding="utf-8") as f:
                f.write(content_mt.strip() + "\n" + content_ms.strip())

        # Duyệt file KHÔNG chứa mt/ms (tức là file gộp + file lẻ khác)
        final_files = [f for f in os.listdir(folder_txt) if f.endswith('.txt') and not f.endswith('mt.txt') and not f.endswith('ms.txt')]
        info_list = {"customer": []}
        
        # Import publish_progress nếu có job_id
        publish_progress_func = None
        if self.job_id:
            try:
                # Import từ shared.redis_client
                import sys
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                from shared.redis_client import publish_progress
                publish_progress_func = publish_progress
            except ImportError:
                publish_progress_func = None
        
        for index,file_name in enumerate(sorted(final_files),start=1):
            # Check cancellation trước khi xử lý mỗi CCCD
            try:
                self.check_cancellation()
            except Exception as e:
                if "đã bị hủy" in str(e):
                    return info_list  # Return partial results
                raise
            
            txt_path = os.path.join(folder_txt, file_name)
            with open(txt_path, "r", encoding="utf-8") as f:
                content = f.read()

            def get_value(content, key):
                for line in content.splitlines():
                    if line.lower().startswith(key + ":"):
                        return line.split(":", 1)[1].strip()
                return ""

            id_cccd = get_value(content, "id")
            name = get_value(content, "name")
            gioitinh = get_value(content, "gioi_tinh")
            sn = get_value(content, "sn")
            ngay_cap = get_value(content, "ngay_cap")
            noi_cap = get_value(content, "noi_cap")
            ngayhh = get_value(content, "ngay_hh")
            quequan = get_value(content, "que_quan")
            thuongtru = get_value(content, "thuong_tru")
            thuongtru2 = get_value(content, "thuong_tru2")
            noithuongtru = f"{thuongtru} ,{thuongtru2}".strip()

            '''headers = [
            "Tên file",
            "Tên", "Giới tính", "Ngày sinh", "Số CCCD",
            "Ngày cấp", "Nơi cấp", "Ngày hết hạn", "Quê quán", "Địa chỉ thường trú"
            ]'''
            info_list["customer"].append({
                "index": index,
                "file_name": file_name.replace(".txt", ""),
                "id_card": id_cccd,
                "name": name,
                "gender": gioitinh,
                "birth_date": sn,
                "created_date": ngay_cap,
                "place_created": noi_cap,
                "expiry_date": ngayhh,
                "hometown": quequan,
                "address": noithuongtru,
                "address2": thuongtru2})
            
            # Publish progress sau mỗi CCCD được xử lý (nếu có job_id và total_cccd)
            # Giai đoạn 4: collect_cus_info (99% → 100%)
            # processed_cccd tăng dần nhưng percent giữ ở 99% cho đến khi hoàn thành mới là 100%
            if publish_progress_func and self.job_id and self.total_cccd > 0:
                processed_cccd = len(info_list["customer"])
                base_percent = 99  # Base % của giai đoạn 4
                stage4_percent = 1  # 1% cho giai đoạn này
                
                # Chỉ update percent khi hoàn thành
                if processed_cccd == self.total_cccd:
                    # Hoàn thành: 100%
                    percent = base_percent + stage4_percent  # 99% + 1% = 100%
                else:
                    # Đang xử lý: giữ ở 99%
                    percent = base_percent  # 99%
                
                message = f"Đang xử lý OCR... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                publish_progress_func(self.job_id, percent, message, total_cccd=self.total_cccd, processed_cccd=processed_cccd)
        
        info_list["status"] = "success"
        info_list["message"] = "Đã trích xuất thông tin các CCCD"
        if len(info_list["customer"]) == 0:
            info_list["message"] = "Không tìm thấy thông tin CCCD nào."
        # Thêm total_cccd vào kết quả để worker có thể dùng
        info_list["total_cccd"] = self.total_cccd
        self.clear_filefolder(folder_txt)
        
        return info_list
    def detect_cccd(self):
        print("1.Detect cccd")
        def order_points(pts):
            rect = np.zeros((4, 2), dtype="float32")

            s = pts.sum(axis=1)
            diff = np.diff(pts, axis=1)

            rect[0] = pts[np.argmin(s)]     # top-left
            rect[2] = pts[np.argmax(s)]     # bottom-right
            rect[1] = pts[np.argmin(diff)]  # top-right
            rect[3] = pts[np.argmax(diff)]  # bottom-left

            return rect
        
        # Handle bytes input (zip or raw bytes with images)
        if isinstance(self.path_img, bytes) or (isinstance(self.path_img, str) and self.path_img.startswith('UEsDB')):  # base64 zip detection
            try:
                # Convert base64 string to bytes if needed
                if isinstance(self.path_img, str):
                    zip_bytes = base64.b64decode(self.path_img)
                else:
                    zip_bytes = self.path_img
                
                input_zip = BytesIO(zip_bytes)
                img_files = []
                
                with zipfile.ZipFile(input_zip, "r") as zf:
                    img_files = [f for f in zf.namelist() if f.lower().endswith(('.jpg', '.png', '.jpeg'))]
                    
                    total = len(img_files)
                    if total == 0:
                        print("Không có ảnh nào.")
                        self.total_cccd = 0
                        return
                    
                    # Tính tổng số CCCD: đếm số file name unique (bỏ phần mt/ms)
                    # Ví dụ: 1mt.jpg, 1ms.jpg, 2mt.jpg, 2ms.jpg, 3mt.jpg → có 3 CCCD (1, 2, 3)
                    base_names = set()
                    for img_file in img_files:
                        # Lấy tên file không có extension
                        file_name_no_ext = os.path.splitext(os.path.basename(img_file))[0]
                        # Bỏ phần mt hoặc ms ở cuối (nếu có)
                        # Ví dụ: "1mt" → "1", "CT00380305ms" → "CT00380305"
                        if file_name_no_ext.lower().endswith('mt'):
                            base_name = file_name_no_ext[:-2]  # Bỏ "mt"
                        elif file_name_no_ext.lower().endswith('ms'):
                            base_name = file_name_no_ext[:-2]  # Bỏ "ms"
                        else:
                            # Nếu không có mt/ms, dùng nguyên tên file
                            base_name = file_name_no_ext
                        base_names.add(base_name)
                    
                    self.total_cccd = len(base_names)
                    
                    # Publish progress ngay sau khi tính được total_cccd (nếu có job_id)
                    if self.job_id and self.total_cccd > 0:
                        try:
                            # Import publish_progress
                            import sys
                            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                            if project_root not in sys.path:
                                sys.path.insert(0, project_root)
                            from shared.redis_client import publish_progress
                            
                            # Lưu vào Redis
                            import redis
                            redis_client = redis.Redis(
                                host=os.getenv('REDIS_HOST', '127.0.0.1'),
                                port=int(os.getenv('REDIS_PORT', 6379)),
                                db=int(os.getenv('REDIS_DB', 0)),
                                password=os.getenv('REDIS_PASSWORD', None),
                                decode_responses=False
                            )
                            redis_client.set(f"job:{self.job_id}:total_cccd", str(self.total_cccd))
                            
                            # Publish progress với format 0/total_cccd và 0%
                            publish_progress(self.job_id, 0, f"Bắt đầu xử lý... (0/{self.total_cccd} CCCD - 0%)", 
                                           total_cccd=self.total_cccd, processed_cccd=0)
                        except Exception as e:
                            pass
                    
                    # Giai đoạn 1: detect_cccd (0% → 33%)
                    # Track CCCD đã xử lý trong giai đoạn này
                    processed_cccd_set = set()  # Reset về 0 cho giai đoạn này
                    base_percent = 0  # Base % của giai đoạn 1
                    percent_per_cccd = 33.0 / self.total_cccd if self.total_cccd > 0 else 0
                    
                    # Import publish_progress nếu có job_id
                    publish_progress_func = None
                    if self.job_id:
                        try:
                            import sys
                            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                            if project_root not in sys.path:
                                sys.path.insert(0, project_root)
                            from shared.redis_client import publish_progress
                            publish_progress_func = publish_progress
                        except Exception as e:
                            pass
                    
                    for i, img_file in enumerate(img_files):
                        # Check cancellation trước khi xử lý mỗi ảnh
                        try:
                            self.check_cancellation()
                        except Exception as e:
                            if "đã bị hủy" in str(e):
                                return
                            raise
                        
                        img_data = zf.read(img_file)
                        img_stream = BytesIO(img_data)
                        img_array = cv2.imdecode(np.frombuffer(img_stream.getvalue(), np.uint8), cv2.IMREAD_COLOR)
                        
                        results = self.model1.predict(source=img_array, conf=0.5, save=False)
                        r = results[0]
                        
                        if len(r.keypoints) == 0:
                            continue
                        
                        file_name = os.path.splitext(os.path.basename(img_file))[0]
                        image_bgr = img_array
                        kpts = r.keypoints.xy[0].cpu().numpy()
                        
                        if kpts.shape[0] != 4:
                            continue
                        
                        # Rest of detection logic...
                        conf = r.keypoints.conf[0].cpu().numpy()
                        avg_conf = np.mean(conf) * 100
                        
                        if avg_conf < 75:
                            h, w = image_bgr.shape[:2]
                            text = f"Không đạt ({avg_conf:.1f}%)"
                            font = cv2.FONT_HERSHEY_SIMPLEX
                            scale = 1.2
                            thickness = 3
                            text_size = cv2.getTextSize(text, font, scale, thickness)[0]
                            text_x = (w - text_size[0]) // 2
                            text_y = (h + text_size[1]) // 2
                            cv2.putText(image_bgr, text, (text_x, text_y), font, scale, (0, 0, 255), thickness)
                            cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), image_bgr)
                            continue
                        
                        pts = kpts.astype(np.float32)
                        ordered_pts = order_points(pts)
                        
                        text = f"Conf: {avg_conf:.1f}%"
                        text_org = (int(ordered_pts[0][0]), int(ordered_pts[0][1]) - 10)
                        cv2.putText(image_bgr, text, text_org, cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
                        
                        (tl, tr, br, bl) = ordered_pts
                        widthA = np.linalg.norm(br - bl)
                        widthB = np.linalg.norm(tr - tl)
                        maxWidth = int(max(widthA, widthB))
                        heightA = np.linalg.norm(tr - br)
                        heightB = np.linalg.norm(tl - bl)
                        maxHeight = int(max(heightA, heightB))
                        
                        dst = np.array([
                            [0, 0],
                            [maxWidth - 1, 0],
                            [maxWidth - 1, maxHeight - 1],
                            [0, maxHeight - 1]
                        ], dtype="float32")
                        M = cv2.getPerspectiveTransform(ordered_pts, dst)
                        warped = cv2.warpPerspective(image_bgr, M, (maxWidth, maxHeight))
                        cv2.polylines(image_bgr, [ordered_pts.astype(np.int32)], isClosed=True, color=(0, 255, 0), thickness=2)
                        
                        cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), warped)
                        cv2.imwrite(os.path.join(self.work_dir, "md1", f"boxed_{file_name}.jpg"), image_bgr)
                        
                        # Track CCCD đã xử lý (extract base_name)
                        file_name_no_ext = os.path.splitext(os.path.basename(img_file))[0]
                        if file_name_no_ext.lower().endswith('mt'):
                            base_name = file_name_no_ext[:-2]
                        elif file_name_no_ext.lower().endswith('ms'):
                            base_name = file_name_no_ext[:-2]
                        else:
                            base_name = file_name_no_ext
                        
                        processed_cccd_set.add(base_name)
                        processed_cccd = len(processed_cccd_set)
                        
                        # Tính % = base_percent + (processed_cccd × percent_per_cccd)
                        percent = int(base_percent + (processed_cccd * percent_per_cccd))
                        if percent > 33:
                            percent = 33
                        
                        # Publish progress
                        if publish_progress_func and self.job_id and self.total_cccd > 0:
                            message = f"Đang detect CCCD... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                            publish_progress_func(self.job_id, percent, message, 
                                               total_cccd=self.total_cccd, processed_cccd=processed_cccd)
            except Exception as e:
                return
        else:
            # Handle file path input
            img_files = [os.path.join(self.path_img, f) 
                        for f in os.listdir(self.path_img) 
                        if f.lower().endswith(('.jpg', '.png', '.jpeg'))]

            total = len(img_files)
            if total == 0:
                self.total_cccd = 0
                return
            
            # Tính tổng số CCCD: đếm số file name unique (bỏ phần mt/ms)
            # Ví dụ: 1mt.jpg, 1ms.jpg, 2mt.jpg, 2ms.jpg, 3mt.jpg → có 3 CCCD (1, 2, 3)
            base_names = set()
            for img_path in img_files:
                # Lấy tên file không có extension
                file_name_no_ext = os.path.splitext(os.path.basename(img_path))[0]
                # Bỏ phần mt hoặc ms ở cuối (nếu có)
                # Ví dụ: "1mt" → "1", "CT00380305ms" → "CT00380305"
                if file_name_no_ext.lower().endswith('mt'):
                    base_name = file_name_no_ext[:-2]  # Bỏ "mt"
                elif file_name_no_ext.lower().endswith('ms'):
                    base_name = file_name_no_ext[:-2]  # Bỏ "ms"
                else:
                    # Nếu không có mt/ms, dùng nguyên tên file
                    base_name = file_name_no_ext
                base_names.add(base_name)
            
            self.total_cccd = len(base_names)
            
            # Publish progress ngay sau khi tính được total_cccd (nếu có job_id)
            if self.job_id and self.total_cccd > 0:
                try:
                    # Import publish_progress
                    import sys
                    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    if project_root not in sys.path:
                        sys.path.insert(0, project_root)
                    from shared.redis_client import publish_progress
                    
                    # Lưu vào Redis
                    import redis
                    redis_client = redis.Redis(
                        host=os.getenv('REDIS_HOST', '127.0.0.1'),
                        port=int(os.getenv('REDIS_PORT', 6379)),
                        db=int(os.getenv('REDIS_DB', 0)),
                        password=os.getenv('REDIS_PASSWORD', None),
                        decode_responses=False
                    )
                    redis_client.set(f"job:{self.job_id}:total_cccd", str(self.total_cccd))
                    
                            # Publish progress với format 0/total_cccd và 0%
                    publish_progress(self.job_id, 0, f"Bắt đầu xử lý... (0/{self.total_cccd} CCCD - 0%)", 
                                   total_cccd=self.total_cccd, processed_cccd=0)
                except Exception as e:
                    pass
            
            # Giai đoạn 1: detect_cccd (0% → 33%)
            # Track CCCD đã xử lý trong giai đoạn này
            processed_cccd_set = set()  # Reset về 0 cho giai đoạn này
            base_percent = 0  # Base % của giai đoạn 1
            percent_per_cccd = 33.0 / self.total_cccd if self.total_cccd > 0 else 0
            
            # Import publish_progress nếu có job_id
            publish_progress_func = None
            if self.job_id:
                try:
                    import sys
                    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    if project_root not in sys.path:
                        sys.path.insert(0, project_root)
                    from shared.redis_client import publish_progress
                    publish_progress_func = publish_progress
                except Exception as e:
                    pass
            
            for i, img_path in enumerate(img_files):
                # Check cancellation trước khi xử lý mỗi ảnh
                try:
                    self.check_cancellation()
                except Exception as e:
                    if "đã bị hủy" in str(e):
                        return
                    raise
                
                results = self.model1.predict(source=img_path, conf=0.5, save=False)
                r = results[0]
                if len(r.keypoints) == 0:
                    continue
                image_bgr = cv2.imread(r.path)
                file_name = os.path.basename(r.path)
                file_name = os.path.splitext(file_name)[0] 
                kpts = r.keypoints.xy[0].cpu().numpy()  # (4, 2)
                if kpts.shape[0] != 4:
                    continue  

                # ==== TÍNH ĐỘ CHÍNH XÁC ====
                conf = r.keypoints.conf[0].cpu().numpy()
                avg_conf = np.mean(conf) * 100  # %

                if avg_conf < 75:
                    # VẼ CHỮ GIỮA ẢNH: "Không đạt"
                    h, w = image_bgr.shape[:2]
                    text = f"Không đạt ({avg_conf:.1f}%)"
                    font = cv2.FONT_HERSHEY_SIMPLEX
                    scale = 1.2
                    thickness = 3
                    text_size = cv2.getTextSize(text, font, scale, thickness)[0]
                    text_x = (w - text_size[0]) // 2
                    text_y = (h + text_size[1]) // 2
                    cv2.putText(image_bgr, text, (text_x, text_y), font, scale, (0, 0, 255), thickness)

                    # Lưu ảnh không đạt
                    cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), image_bgr)
                    continue

                # ==== Tiếp tục xử lý ảnh đạt yêu cầu ====
                pts = kpts.astype(np.float32)
                ordered_pts = order_points(pts)

                # Vẽ khung và độ chính xác lên ảnh
                text = f"Conf: {avg_conf:.1f}%"
                text_org = (int(ordered_pts[0][0]), int(ordered_pts[0][1]) - 10)
                cv2.putText(image_bgr, text, text_org, cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)

                (tl, tr, br, bl) = ordered_pts
                widthA = np.linalg.norm(br - bl)
                widthB = np.linalg.norm(tr - tl)
                maxWidth = int(max(widthA, widthB))
                heightA = np.linalg.norm(tr - br)
                heightB = np.linalg.norm(tl - bl)
                maxHeight = int(max(heightA, heightB))

                dst = np.array([
                    [0, 0],
                    [maxWidth - 1, 0],
                    [maxWidth - 1, maxHeight - 1],
                    [0, maxHeight - 1]
                ], dtype="float32")
                M = cv2.getPerspectiveTransform(ordered_pts, dst)
                warped = cv2.warpPerspective(image_bgr, M, (maxWidth, maxHeight))
                cv2.polylines(image_bgr, [ordered_pts.astype(np.int32)], isClosed=True, color=(0, 255, 0), thickness=2)

                cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), warped)
                cv2.imwrite(os.path.join(self.work_dir, "md1", f"boxed_{file_name}.jpg"), image_bgr)
                
                # Track CCCD đã xử lý (extract base_name)
                file_name_no_ext = os.path.splitext(os.path.basename(img_path))[0]
                if file_name_no_ext.lower().endswith('mt'):
                    base_name = file_name_no_ext[:-2]
                elif file_name_no_ext.lower().endswith('ms'):
                    base_name = file_name_no_ext[:-2]
                else:
                    base_name = file_name_no_ext
                
                processed_cccd_set.add(base_name)
                processed_cccd = len(processed_cccd_set)
                
                # Tính % = base_percent + (processed_cccd × percent_per_cccd)
                percent = int(base_percent + (processed_cccd * percent_per_cccd))
                if percent > 25:
                    percent = 25
                
                # Publish progress
                if publish_progress_func and self.job_id and self.total_cccd > 0:
                    message = f"Đang detect CCCD... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                    publish_progress_func(self.job_id, percent, message, 
                                       total_cccd=self.total_cccd, processed_cccd=processed_cccd)

    def detect_lines(self):
        print("3.Detect lines")
        import cv2
        import numpy as np
        import os
        import math
        folder = self.work_md2
        img_files = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(('.jpg', '.png', '.jpeg'))
        ]

        total = len(img_files)
        if total == 0:
            print("❌ Không tìm thấy ảnh!")
            return

        # Giai đoạn 3: detect_lines (66% → 99%)
        # Chia thành 3 công đoạn con: 16%, 16%, 1%
        stage3_base = 66  # Base % của giai đoạn 3
        stage3_range = 33  # 99 - 66
        sub_stage_range_1_2 = 16  # 16% cho công đoạn 1 và 2
        sub_stage_range_3 = 1  # 1% cho công đoạn 3
        
        # Công đoạn 3.1: Detect lines + crop (66% → 82%)
        sub_stage1_base = 66  # 66%
        sub_stage1_percent_per_cccd = sub_stage_range_1_2 / self.total_cccd if self.total_cccd > 0 else 0  # 16% / total_cccd
        
        # Track CCCD đã xử lý trong công đoạn 3.1
        processed_cccd_set = set()  # Reset về 0 cho công đoạn này
        
        # Import publish_progress nếu có job_id
        publish_progress_func = None
        if self.job_id:
            try:
                import sys
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                from shared.redis_client import publish_progress
                publish_progress_func = publish_progress
            except Exception as e:
                print(f"⚠️ Không thể import publish_progress: {e}")

        for i, img_path in enumerate(img_files):
            # Check cancellation trước khi xử lý mỗi ảnh
            try:
                self.check_cancellation()
            except Exception as e:
                if "đã bị hủy" in str(e):
                    print(f"⚠️ Job {self.job_id} đã bị hủy, dừng xử lý")
                    return
                raise
            
            results = self.model3.predict(source=img_path, conf=0.5, save=False)
            r = results[0]
            if r.masks is None or len(r.masks.xy) == 0:
                # Track CCCD đã xử lý (kể cả khi skip)
                file_name_no_ext = os.path.splitext(os.path.basename(img_path))[0]
                if file_name_no_ext.lower().endswith('mt'):
                    base_name = file_name_no_ext[:-2]
                elif file_name_no_ext.lower().endswith('ms'):
                    base_name = file_name_no_ext[:-2]
                else:
                    base_name = file_name_no_ext
                
                processed_cccd_set.add(base_name)
                processed_cccd = len(processed_cccd_set)
                
                # Tính % cho công đoạn 3.1: sub_stage1_base + (processed_cccd × sub_stage1_percent_per_cccd)
                # Đảm bảo mỗi CCCD có percent khác nhau: dùng processed_cccd làm phần thập phân nhỏ
                base_percent = sub_stage1_base + (processed_cccd * sub_stage1_percent_per_cccd)
                # Thêm offset nhỏ dựa trên processed_cccd để đảm bảo mỗi CCCD có percent khác nhau
                percent = base_percent + (processed_cccd * 0.5)  # 0.5% per CCCD để đảm bảo khác nhau
                if percent > 66 + sub_stage_range_1_2:
                    percent = 66 + sub_stage_range_1_2  # 82%
                percent = round(percent)  # Round về số nguyên gần nhất
                
                # Publish progress cho công đoạn 3.1
                if publish_progress_func and self.job_id and self.total_cccd > 0:
                    message = f"Đang detect lines (crop)... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                    publish_progress_func(self.job_id, percent, message, 
                                       total_cccd=self.total_cccd, processed_cccd=processed_cccd)
                continue

            image_bgr = cv2.imread(r.path)
            image_clean = image_bgr.copy()
            file_name = os.path.basename(r.path)
            file_name = os.path.splitext(file_name)[0] 
            for j, polygon in enumerate(r.masks.xy):
                points = polygon.astype(int)
                if points.shape[0] < 4:
                    continue
                pts = points.reshape((-1, 1, 2))
                cv2.polylines(image_bgr, [pts], isClosed=True, color=(0, 255, 0), thickness=2)
                class_id = int(r.boxes.cls[j]) if r.boxes is not None else 0
                conf = float(r.boxes.conf[j]) if r.boxes is not None else 0.0
                class_name = self.model3.names[class_id] if hasattr(self.model3, 'names') else str(class_id)
                label_text = f"{class_name} {conf:.2f}"

                # Vẽ label trên ảnh đã vẽ
                x, y = points[0]
                font = cv2.FONT_HERSHEY_SIMPLEX
                font_scale = 0.5
                thickness = 1
                text_size, _ = cv2.getTextSize(label_text, font, font_scale, thickness)
                text_w, text_h = text_size

                cv2.rectangle(image_bgr, (x, y - text_h - 4), (x + text_w, y), (0, 255, 0), -1)
                cv2.putText(image_bgr, label_text, (x, y - 2), font, font_scale, (0, 0, 0), thickness, cv2.LINE_AA)

                # === CROP đúng từ ảnh gốc ===
                mask = np.zeros(image_clean.shape[:2], dtype=np.uint8)
                cv2.fillPoly(mask, [pts], 255)

                masked = cv2.bitwise_and(image_clean, image_clean, mask=mask)

                x, y, w, h = cv2.boundingRect(pts)
                crop = masked[y:y+h, x:x+w]

                # === Tên file crop: tên gốc + tên label ===
                crop_name = os.path.join(self.work_md3, f"{file_name.replace('.jpg','')}-{class_name}.jpg")
                cv2.imwrite(crop_name, crop)
            original_ext = os.path.splitext(os.path.basename(r.path))[1] or '.jpg'
            boxed_name = os.path.join(self.work_dir, "md3", "detected_results", f"boxed_{file_name}{original_ext}")
            cv2.imwrite(boxed_name, image_bgr)
            
            # Track CCCD đã xử lý (extract base_name)
            file_name_no_ext = os.path.splitext(os.path.basename(img_path))[0]
            if file_name_no_ext.lower().endswith('mt'):
                base_name = file_name_no_ext[:-2]
            elif file_name_no_ext.lower().endswith('ms'):
                base_name = file_name_no_ext[:-2]
            else:
                base_name = file_name_no_ext
            
            processed_cccd_set.add(base_name)
            processed_cccd = len(processed_cccd_set)
            
            # Tính % cho công đoạn 3.1: sub_stage1_base + (processed_cccd × sub_stage1_percent_per_cccd)
            # Đảm bảo mỗi CCCD có percent khác nhau: dùng processed_cccd làm phần thập phân nhỏ
            # Ví dụ: CCCD 1 = 66.01%, CCCD 2 = 66.02%, ... để sau khi round() vẫn khác nhau
            base_percent = sub_stage1_base + (processed_cccd * sub_stage1_percent_per_cccd)
            # Thêm offset nhỏ dựa trên processed_cccd để đảm bảo mỗi CCCD có percent khác nhau
            percent = base_percent + (processed_cccd * 0.5)  # 0.5% per CCCD để đảm bảo khác nhau
            if percent > 66 + sub_stage_range_1_2:
                percent = 66 + sub_stage_range_1_2  # 82%
            percent = round(percent)  # Round về số nguyên gần nhất
            
            # Publish progress cho công đoạn 3.1
            if publish_progress_func and self.job_id and self.total_cccd > 0:
                message = f"Đang detect lines (crop)... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                publish_progress_func(self.job_id, percent, message, 
                                   total_cccd=self.total_cccd, processed_cccd=processed_cccd)
        import os
        from PIL import Image, ImageDraw, ImageFont
        from vietocr.tool.predictor import Predictor
        from vietocr.tool.config import Cfg
        import cv2
        import numpy as np

        if self.vietocr_detector is not None:
            detector = self.vietocr_detector
        elif self.cached_models and self.cached_models.get('vietocr_detector'):
            detector = self.cached_models['vietocr_detector']
            self.vietocr_detector = detector
        else:
            config = Cfg.load_config_from_name('vgg_transformer')
            config['weights'] = os.path.join(self.base_dir, 'vgg_transformer.pth')
            config['cnn']['pretrained'] = False
            config['device'] = 'cpu'
            detector = Predictor(config)
            if self.cached_models:
                self.cached_models['vietocr_detector'] = detector

        crop_folder = self.work_md3
        goc_folder = self.work_md2
        results_folder = self.work_temp_rs

        ocr_result_file = os.path.join(self.work_md4, 'ocr_results.txt')
        ocr_data = []
        
        # Đếm tổng số crop files để tính progress
        crop_files = [f for f in os.listdir(crop_folder) if f.lower().endswith('.jpg')]
        total_crops = len(crop_files)
        
        # Công đoạn 3.2: OCR từng field (82% → 98%)
        sub_stage2_base = 66 + sub_stage_range_1_2  # 82%
        sub_stage2_percent_per_cccd = sub_stage_range_1_2 / self.total_cccd if self.total_cccd > 0 else 0  # 16% / total_cccd
        
        # Track CCCD đã OCR xong (để publish progress trong phần OCR)
        ocr_cccd_tracker = {}  # {base_name: {'mt': set(), 'ms': set()}}
        ocr_cccd_set = set()  # Track CCCD đã OCR xong (cả mt và ms)
        
        with open(ocr_result_file, 'w', encoding='utf-8') as f_out:
            i = 0
            for file_name in os.listdir(crop_folder):
                # Check cancellation trước khi OCR mỗi file
                try:
                    self.check_cancellation()
                except Exception as e:
                    if "đã bị hủy" in str(e):
                        return
                    raise
                
                i+=1
                if file_name.lower().endswith('.jpg'):
                    img_path = os.path.join(crop_folder, file_name)
                    img = Image.open(img_path)
                    text = detector.predict(img).strip()
                    text_ki = [
                        "CỤC", "TRƯỞNG", "CỤC", "CẢNH", "SÁT",
                        "QUẢN", "LÝ", "HÀNH", "CHÍNH", "VỀ",
                        "TRẬT", "TỰ", "XÃ", "HỘI"
                    ]

                    if len(text) > 14 and text.isupper() and any(word in text for word in text_ki):
                        text = "CỤC TRƯỞNG CỤC CẢNH SÁT QUẢN LÝ HÀNH CHÍNH VỀ TRẬT TỰ XÃ HỘI"
                    f_out.write(f"{file_name}\t{text}\n")
                    file_name_no_ext = os.path.splitext(file_name)[0]
                    ocr_data.append((file_name_no_ext, text))
                    
                    # Track OCR progress: Extract base_name từ crop file name
                    # Ví dụ: "1mt-id.jpg" → base_name = "1", field = "id"
                    parts = file_name_no_ext.split('-')
                    if len(parts) >= 2:
                        goc_name = parts[0]  # "1mt" hoặc "1ms"
                        # Bỏ phần mt/ms để lấy base_name
                        if goc_name.lower().endswith('mt'):
                            base_name = goc_name[:-2]
                            side = 'mt'
                        elif goc_name.lower().endswith('ms'):
                            base_name = goc_name[:-2]
                            side = 'ms'
                        else:
                            base_name = goc_name
                            side = 'unknown'
                        
                        if base_name not in ocr_cccd_tracker:
                            ocr_cccd_tracker[base_name] = {'mt': set(), 'ms': set()}
                        
                        # Track field theo mt/ms
                        ocr_cccd_tracker[base_name][side].add(parts[1] if len(parts) > 1 else 'unknown')
                        
                        # Kiểm tra xem CCCD này đã OCR xong chưa (có ít nhất 1 field từ mt hoặc ms)
                        # Nếu đã có field từ cả mt và ms (hoặc chỉ có 1 trong 2 nếu thiếu), coi như đã OCR xong
                        has_mt = len(ocr_cccd_tracker[base_name]['mt']) > 0
                        has_ms = len(ocr_cccd_tracker[base_name]['ms']) > 0
                        
                        # Chỉ publish khi OCR xong 1 CCCD (có ít nhất 1 field từ mt hoặc ms)
                        # Và chỉ publish 1 lần cho mỗi CCCD
                        if (has_mt or has_ms) and base_name not in ocr_cccd_set:
                            ocr_cccd_set.add(base_name)
                            ocr_processed_cccd = len(ocr_cccd_set)
                            
                            # Tính % cho công đoạn 3.2: sub_stage2_base + (ocr_processed_cccd × sub_stage2_percent_per_cccd)
                            # Đảm bảo mỗi CCCD có percent khác nhau
                            base_percent = sub_stage2_base + (ocr_processed_cccd * sub_stage2_percent_per_cccd)
                            percent = base_percent + (ocr_processed_cccd * 0.5)  # 0.5% per CCCD để đảm bảo khác nhau
                            if percent > 66 + (sub_stage_range_1_2 * 2):
                                percent = 66 + (sub_stage_range_1_2 * 2)  # 98%
                            percent = round(percent)  # Round về số nguyên gần nhất
                            
                            # Publish progress cho công đoạn 3.2
                            if publish_progress_func and self.job_id and self.total_cccd > 0:
                                message = f"Đang detect lines (OCR)... ({ocr_processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                                publish_progress_func(self.job_id, percent, message, 
                                                   total_cccd=self.total_cccd, processed_cccd=ocr_processed_cccd)

        goc_dict = {}
        for crop_name, text in ocr_data:
            parts = crop_name.replace('.jpg', '').split('-')
            goc_name = parts[0]
            try:
                label = parts[1]  # thuong_tru2
            except:
                label = parts[2]
            if goc_name not in goc_dict:
                goc_dict[goc_name] = []
            if label == "noi_cap":
                text = "CỤC TRƯỞNG CỤC CẢNH SÁT QUẢN LÝ HÀNH CHÍNH VỀ TRẬT TỰ XÃ HỘI"
            goc_dict[goc_name].append((label, text))
        
        # Công đoạn 3.3: Vẽ text + lưu file (98% → 99%)
        # Chỉ update percent khi hoàn thành (98% → 99%)
        sub_stage3_base = 66 + (sub_stage_range_1_2 * 2)  # 98%
        sub_stage3_percent = 1  # 1% cho công đoạn này
        
        # Track CCCD đã vẽ text và lưu file xong
        # {base_name: {'mt': bool, 'ms': bool}}
        saved_cccd_tracker = {}
        saved_cccd_set = set()  # Track CCCD đã lưu xong (cả mt và ms, hoặc chỉ có 1 trong 2 nếu thiếu)
        
        for goc_name, infos in goc_dict.items():
            # Check cancellation trước khi vẽ text và lưu file cho mỗi CCCD
            try:
                self.check_cancellation()
            except Exception as e:
                if "đã bị hủy" in str(e):
                    print(f"⚠️ Job {self.job_id} đã bị hủy, dừng xử lý")
                    return
                raise
            
            img_path = os.path.join(goc_folder, f"{goc_name}.jpg")
            if not os.path.exists(img_path):
                continue

            # Dùng OpenCV để vẽ nền mờ
            image_cv = cv2.imread(img_path)
            h, w = image_cv.shape[:2]

            box_w = int(w)
            box_h = int(h)
            box_x = 10
            box_y = 10

            overlay = image_cv.copy()
            cv2.rectangle(overlay, (box_x, box_y), (box_x + box_w, box_y + box_h), (255, 255, 255), -1)
            alpha = 0.3
            image_cv = cv2.addWeighted(overlay, alpha, image_cv, 1 - alpha, 0)

            # Convert BGR OpenCV => RGB PIL
            image_pil = cv2.cvtColor(image_cv, cv2.COLOR_BGR2RGB)
            image_pil = Image.fromarray(image_pil)

            draw = ImageDraw.Draw(image_pil)

            # === Load font Tiếng Việt ===
            # ⚠️ Đường dẫn font Unicode, ví dụ Arial Unicode MS hoặc Roboto
            font_path = os.path.join(self.base_dir, "arial.ttf")  # Bạn thay đường dẫn đúng của bạn!
            font_size = max(20, int(h * 0.02))  # Auto scale size

            try:
                font = ImageFont.truetype(font_path, font_size)
            except:
                font = ImageFont.load_default()

            # Vẽ text từng dòng
            y0 = box_y + 20
            dy = int(font_size * 1.5)
            for label, text in infos:
                
                line = f"{label}: {text}"
                draw.text((box_x + 10, y0), line, font=font, fill=(0, 0, 0))
                y0 += dy

            # Convert lại về BGR OpenCV
            image_result = cv2.cvtColor(np.array(image_pil), cv2.COLOR_RGB2BGR)

            # Lưu ảnh kết quả
            result_img = os.path.join(results_folder, f"{goc_name}.jpg")
            cv2.imwrite(result_img, image_result)
            # Lưu file txt kèm
            result_txt = os.path.join(results_folder, f"{goc_name}.txt")
            with open(result_txt, 'w', encoding='utf-8') as f:
                for label, text in infos:
                    f.write(f"{label}: {text}\n")
            
            # Track CCCD đã vẽ text và lưu file xong (extract base_name)
            if goc_name.lower().endswith('mt'):
                base_name = goc_name[:-2]
                side = 'mt'
            elif goc_name.lower().endswith('ms'):
                base_name = goc_name[:-2]
                side = 'ms'
            else:
                base_name = goc_name
                side = 'unknown'
            
            # Track từng side (mt/ms) đã lưu
            if base_name not in saved_cccd_tracker:
                saved_cccd_tracker[base_name] = {'mt': False, 'ms': False}
            
            saved_cccd_tracker[base_name][side] = True
            
            # Kiểm tra xem CCCD này đã lưu xong chưa (cả mt và ms đã lưu, hoặc chỉ có 1 trong 2 nếu thiếu)
            has_mt = saved_cccd_tracker[base_name]['mt']
            has_ms = saved_cccd_tracker[base_name]['ms']
            
            # Chỉ publish khi đã lưu xong cả mt và ms của 1 CCCD (hoặc chỉ có 1 trong 2 nếu thiếu)
            # Và chỉ publish 1 lần cho mỗi CCCD
            if (has_mt or has_ms) and base_name not in saved_cccd_set:
                saved_cccd_set.add(base_name)
                saved_processed_cccd = len(saved_cccd_set)
                
                # Công đoạn 3.3: processed_cccd tăng dần nhưng percent giữ ở 98% cho đến khi hoàn thành mới là 99%
                if saved_processed_cccd == self.total_cccd:
                    # Hoàn thành: 99%
                    percent = sub_stage3_base + sub_stage3_percent  # 98% + 1% = 99%
                else:
                    # Đang xử lý: giữ ở 98%
                    percent = sub_stage3_base  # 98%
                
                # Publish progress cho công đoạn 3.3
                if publish_progress_func and self.job_id and self.total_cccd > 0:
                    message = f"Đang detect lines (lưu file)... ({saved_processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                    publish_progress_func(self.job_id, percent, message, 
                                       total_cccd=self.total_cccd, processed_cccd=saved_processed_cccd)

    def detect_corners(self):
        print("2.Detect corners")
        folder = self.work_md1
        img_files = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(('.jpg', '.jpeg', '.png'))
        ]
        total = len(img_files)
        if total == 0:
            print("❌ Không tìm thấy ảnh!")
            return

        # Giai đoạn 2: detect_corners (33% → 66%)
        # Track CCCD đã xử lý trong giai đoạn này
        processed_cccd_set = set()  # Reset về 0 cho giai đoạn này
        base_percent = 33  # Base % của giai đoạn 2
        percent_per_cccd = 33.0 / self.total_cccd if self.total_cccd > 0 else 0
        
        # Import publish_progress nếu có job_id
        publish_progress_func = None
        if self.job_id:
            try:
                import sys
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                from shared.redis_client import publish_progress
                publish_progress_func = publish_progress
            except Exception as e:
                print(f"⚠️ Không thể import publish_progress: {e}")
        
        for i, img_path in enumerate(img_files):
            # Check cancellation trước khi xử lý mỗi ảnh
            try:
                self.check_cancellation()
            except Exception as e:
                if "đã bị hủy" in str(e):
                    print(f"⚠️ Job {self.job_id} đã bị hủy, dừng xử lý")
                    return
                raise
            
            results = self.model2.predict(source=img_path, conf=0.5, save=False)
            r = results[0]
            img = cv2.imread(r.path)
            file_name = os.path.basename(r.path)  
            file_name = os.path.splitext(file_name)[0] 
            centers = {}
            for box in r.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0].cpu().numpy())
                cls = int(box.cls[0].cpu().item())
                name = self.model2.names[cls]
                cx = int((x1 + x2) / 2)
                cy = int((y1 + y2) / 2)
                centers[name] = (cx, cy)

            if 'quoc_huy' in centers and 'qr' in centers:
                ptA, ptB = centers['quoc_huy'], centers['qr']
            elif 'chip' in centers and 'm_red' in centers:
                ptA, ptB = centers['chip'], centers['m_red']
            else:
                cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), img)  # Lưu nguyên gốc
                # Track CCCD đã xử lý (kể cả khi skip)
                file_name_no_ext = os.path.splitext(os.path.basename(img_path))[0]
                if file_name_no_ext.lower().endswith('mt'):
                    base_name = file_name_no_ext[:-2]
                elif file_name_no_ext.lower().endswith('ms'):
                    base_name = file_name_no_ext[:-2]
                else:
                    base_name = file_name_no_ext
                
                processed_cccd_set.add(base_name)
                processed_cccd = len(processed_cccd_set)
                
                # Tính % = base_percent + (processed_cccd × percent_per_cccd)
                percent = int(base_percent + (processed_cccd * percent_per_cccd))
                if percent > 66:
                    percent = 66
                
                # Publish progress
                if publish_progress_func and self.job_id and self.total_cccd > 0:
                    message = f"Đang detect corners... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                    publish_progress_func(self.job_id, percent, message, 
                                       total_cccd=self.total_cccd, processed_cccd=processed_cccd)
                continue

            dx, dy = ptB[0] - ptA[0], ptB[1] - ptA[1]
            angle = math.degrees(math.atan2(dy, dx))
            rotate_angle = -angle   # CHUẨN: Luôn lấy -angle để vector nằm ngang
            if abs(rotate_angle) < 10:
                cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), img)  # Lưu nguyên gốc
                # Track CCCD đã xử lý (kể cả khi skip)
                file_name_no_ext = os.path.splitext(os.path.basename(img_path))[0]
                if file_name_no_ext.lower().endswith('mt'):
                    base_name = file_name_no_ext[:-2]
                elif file_name_no_ext.lower().endswith('ms'):
                    base_name = file_name_no_ext[:-2]
                else:
                    base_name = file_name_no_ext
                
                processed_cccd_set.add(base_name)
                processed_cccd = len(processed_cccd_set)
                
                # Tính % = base_percent + (processed_cccd × percent_per_cccd)
                percent = int(base_percent + (processed_cccd * percent_per_cccd))
                if percent > 66:
                    percent = 66
                
                # Publish progress
                if publish_progress_func and self.job_id and self.total_cccd > 0:
                    message = f"Đang detect corners... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                    publish_progress_func(self.job_id, percent, message, 
                                       total_cccd=self.total_cccd, processed_cccd=processed_cccd)
                continue
            h, w = img.shape[:2]
            center_img = (w // 2, h // 2)
            M = cv2.getRotationMatrix2D(center_img, rotate_angle, 1.0)

            cos = abs(M[0, 0])
            sin = abs(M[0, 1])
            new_w = int(h * sin + w * cos)
            new_h = int(h * cos + w * sin)
            M[0, 2] += (new_w / 2) - center_img[0]
            M[1, 2] += (new_h / 2) - center_img[1]
            def transform(pt):
                x, y = pt
                new_x = M[0,0]*x + M[0,1]*y + M[0,2]
                new_y = M[1,0]*x + M[1,1]*y + M[1,2]
                return (new_x, new_y)

            ptA_new = transform(ptA)
            ptB_new = transform(ptB)

            # Nếu vector AB sau xoay mà B bên trái A → thêm 180°
            if ptB_new[0] < ptA_new[0]:
                rotate_angle += 180

            # Tính lại ma trận FINAL duy nhất
            M_final = cv2.getRotationMatrix2D(center_img, rotate_angle, 1.0)
            cos = abs(M_final[0, 0])
            sin = abs(M_final[0, 1])
            new_w = int(h * sin + w * cos)
            new_h = int(h * cos + w * sin)
            M_final[0, 2] += (new_w / 2) - center_img[0]
            M_final[1, 2] += (new_h / 2) - center_img[1]

            rotated = cv2.warpAffine(
                img,
                M_final,
                (new_w, new_h),
                flags=cv2.INTER_LINEAR,
                borderMode=cv2.BORDER_CONSTANT,
                borderValue=(255, 255, 255)
            )
            cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), rotated)
            
            # Track CCCD đã xử lý (extract base_name)
            file_name_no_ext = os.path.splitext(os.path.basename(img_path))[0]
            if file_name_no_ext.lower().endswith('mt'):
                base_name = file_name_no_ext[:-2]
            elif file_name_no_ext.lower().endswith('ms'):
                base_name = file_name_no_ext[:-2]
            else:
                base_name = file_name_no_ext
            
            processed_cccd_set.add(base_name)
            processed_cccd = len(processed_cccd_set)
            
            # Tính % = base_percent + (processed_cccd × percent_per_cccd)
            percent = int(base_percent + (processed_cccd * percent_per_cccd))
            if percent > 50:
                percent = 50
            
            # Publish progress
            if publish_progress_func and self.job_id and self.total_cccd > 0:
                message = f"Đang detect corners... ({processed_cccd}/{self.total_cccd} CCCD - {percent}%)"
                publish_progress_func(self.job_id, percent, message, 
                                   total_cccd=self.total_cccd, processed_cccd=processed_cccd)
        
class CCCDExtractor():
    def __init__(self, config=None, cached_models=None):
        self.config = config or {}
        self.cached_models = cached_models
        super().__init__()
    def handle_task(self,data_inp:dict):  
        func_type = data_inp.get("func_type")      
        inp_path = data_inp.get("inp_path")
        results = DetectWorker(input_path = inp_path, type_ = func_type, cached_models=self.cached_models).run()
        return results


class DetectWorkerStreaming(DetectWorker):
    """DetectWorker với khả năng streaming progress events"""
    
    def __init__(self, input_path: str = None, type_: int = 0, cached_models=None, base_percent: int = 0):
        super().__init__(input_path, type_, cached_models)
        self.base_percent = base_percent  # Starting percent for progress
        self.progress_events = []  # Store progress events for generator
    
    def run_streaming(self):
        """Run with streaming progress - returns generator"""
        try:
            self.init_temp_dirs()
            
            if self.type_ == 1:
                if self.cached_models:
                    self.model1 = self.cached_models.get('yolo_model1')
                    self.model2 = self.cached_models.get('yolo_model2')
                    self.model3 = self.cached_models.get('yolo_model3')
                    self.vietocr_detector = self.cached_models.get('vietocr_detector')
                
                if self.model1 is None:
                    self.model1 = YOLO(os.path.join(self.base_dir, "best.pt"))
                if self.model2 is None:
                    self.model2 = YOLO(os.path.join(self.base_dir, "best2.pt"))
                if self.model3 is None:
                    self.model3 = YOLO(os.path.join(self.base_dir, "best3.pt"))
                
                # Process with progress events
                try:
                    for event in self.detect_cccd_streaming():
                        yield event
                    
                    for event in self.detect_corners_streaming():
                        yield event
                    
                    for event in self.detect_lines_streaming():
                        yield event
                    
                    results = self.collect_cus_info()
                    
                    # Yield complete event
                    yield {
                        "type": "complete",
                        "percent": 100,
                        "message": "Hoàn thành trích xuất CCCD",
                        "data": results
                    }
                except GeneratorExit:
                    # Generator was closed by client, just re-raise
                    raise
                except Exception as e:
                    import traceback
                    print(f"Error in run_streaming type 1: {e}")
                    traceback.print_exc()
                    yield {
                        "type": "error",
                        "percent": 100,
                        "message": f"Lỗi xử lý: {str(e)}",
                        "data": None
                    }
                
            elif self.type_ == 2:
                try:
                    results = self.pdf_to_png(self.path_img)
                    yield {
                        "type": "complete",
                        "percent": 100,
                        "message": "Hoàn thành chuyển PDF sang ảnh",
                        "data": results
                    }
                except Exception as e:
                    import traceback
                    print(f"Error in run_streaming type 2: {e}")
                    traceback.print_exc()
                    yield {
                        "type": "error",
                        "percent": 100,
                        "message": f"Lỗi xử lý PDF: {str(e)}",
                        "data": None
                    }
            elif self.type_ == 3:
                try:
                    results = self.excel_to_png(self.path_img)
                    yield {
                        "type": "complete",
                        "percent": 100,
                        "message": "Hoàn thành tải ảnh từ Excel",
                        "data": results
                    }
                except Exception as e:
                    import traceback
                    print(f"Error in run_streaming type 3: {e}")
                    traceback.print_exc()
                    yield {
                        "type": "error",
                        "percent": 100,
                        "message": f"Lỗi xử lý Excel: {str(e)}",
                        "data": None
                    }
        except GeneratorExit:
            # Client disconnected, cleanup and re-raise
            self.cleanup_temp_dirs()
            raise
        except Exception as e:
            import traceback
            print(f"Error in run_streaming: {e}")
            traceback.print_exc()
            try:
                yield {
                    "type": "error",
                    "percent": 100,
                    "message": f"Lỗi hệ thống: {str(e)}",
                    "data": None
                }
            except:
                pass
        finally:
            self.cleanup_temp_dirs()
    
    def detect_cccd_streaming(self):
        """Detect CCCD với streaming progress"""
        print("1.Detect cccd (streaming)")
        
        def order_points(pts):
            rect = np.zeros((4, 2), dtype="float32")
            s = pts.sum(axis=1)
            diff = np.diff(pts, axis=1)
            rect[0] = pts[np.argmin(s)]
            rect[2] = pts[np.argmax(s)]
            rect[1] = pts[np.argmin(diff)]
            rect[3] = pts[np.argmax(diff)]
            return rect
        
        # Count total images first
        total_images = 0
        processed_images = 0
        
        if isinstance(self.path_img, bytes) or (isinstance(self.path_img, str) and self.path_img.startswith('UEsDB')):
            try:
                if isinstance(self.path_img, str):
                    zip_bytes = base64.b64decode(self.path_img)
                else:
                    zip_bytes = self.path_img
                
                input_zip = BytesIO(zip_bytes)
                
                with zipfile.ZipFile(input_zip, "r") as zf:
                    img_files = [f for f in zf.namelist() if f.lower().endswith(('.jpg', '.png', '.jpeg'))]
                    total_images = len(img_files)
                
                if total_images == 0:
                    yield {"type": "warning", "message": "Không có ảnh nào trong file"}
                    return
                
                # Calculate estimated CCCD count (pairs of images)
                estimated_cccd = (total_images + 1) // 2
                
                yield {
                    "type": "progress",
                    "step": "detect_cccd",
                    "message": f"Đang phát hiện CCCD từ {total_images} ảnh...",
                    "percent": self.base_percent + 10,
                    "total_images": total_images,
                    "estimated_cccd": estimated_cccd,
                    "processed": 0
                }
                
                input_zip = BytesIO(zip_bytes)  # Reset stream
                
                with zipfile.ZipFile(input_zip, "r") as zf:
                    for i, img_file in enumerate(img_files):
                        try:
                            img_data = zf.read(img_file)
                            img_stream = BytesIO(img_data)
                            img_array = cv2.imdecode(np.frombuffer(img_stream.getvalue(), np.uint8), cv2.IMREAD_COLOR)
                            
                            results = self.model1.predict(source=img_array, conf=0.5, save=False)
                            r = results[0]
                            
                            if len(r.keypoints) == 0:
                                processed_images += 1
                                progress_pct = self.base_percent + 10 + int((processed_images / total_images) * 20)
                                yield {
                                    "type": "progress",
                                    "step": "detect_cccd",
                                    "message": f"Đang phát hiện CCCD: {processed_images}/{total_images} ảnh",
                                    "percent": min(progress_pct, self.base_percent + 30),
                                    "processed": processed_images,
                                    "total_images": total_images
                                }
                                continue
                            
                            file_name = os.path.splitext(os.path.basename(img_file))[0]
                            image_bgr = img_array
                            kpts = r.keypoints.xy[0].cpu().numpy()
                            
                            if kpts.shape[0] != 4:
                                continue
                            
                            conf = r.keypoints.conf[0].cpu().numpy()
                            avg_conf = np.mean(conf) * 100
                            
                            if avg_conf < 75:
                                h, w = image_bgr.shape[:2]
                                text = f"Không đạt ({avg_conf:.1f}%)"
                                font = cv2.FONT_HERSHEY_SIMPLEX
                                scale = 1.2
                                thickness = 3
                                text_size = cv2.getTextSize(text, font, scale, thickness)[0]
                                text_x = (w - text_size[0]) // 2
                                text_y = (h + text_size[1]) // 2
                                cv2.putText(image_bgr, text, (text_x, text_y), font, scale, (0, 0, 255), thickness)
                                cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), image_bgr)
                                continue
                            
                            pts = kpts.astype(np.float32)
                            ordered_pts = order_points(pts)
                            
                            text = f"Conf: {avg_conf:.1f}%"
                            text_org = (int(ordered_pts[0][0]), int(ordered_pts[0][1]) - 10)
                            cv2.putText(image_bgr, text, text_org, cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)
                            
                            (tl, tr, br, bl) = ordered_pts
                            widthA = np.linalg.norm(br - bl)
                            widthB = np.linalg.norm(tr - tl)
                            maxWidth = int(max(widthA, widthB))
                            heightA = np.linalg.norm(tr - br)
                            heightB = np.linalg.norm(tl - bl)
                            maxHeight = int(max(heightA, heightB))
                            
                            dst = np.array([
                                [0, 0],
                                [maxWidth - 1, 0],
                                [maxWidth - 1, maxHeight - 1],
                                [0, maxHeight - 1]
                            ], dtype="float32")
                            M = cv2.getPerspectiveTransform(ordered_pts, dst)
                            warped = cv2.warpPerspective(image_bgr, M, (maxWidth, maxHeight))
                            cv2.polylines(image_bgr, [ordered_pts.astype(np.int32)], isClosed=True, color=(0, 255, 0), thickness=2)
                            
                            cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), warped)
                            cv2.imwrite(os.path.join(self.work_dir, "md1", f"boxed_{file_name}.jpg"), image_bgr)
                            
                            processed_images += 1
                            
                            progress_percent = self.base_percent + 10 + int((processed_images / total_images) * 20)
                            yield {
                                "type": "progress",
                                "step": "detect_cccd",
                                "message": f"Đã phát hiện {processed_images}/{total_images} ảnh",
                                "percent": progress_percent,
                                "processed": processed_images,
                                "total_images": total_images
                            }
                            
                        except Exception as e:
                            print(f"Lỗi xử lý ảnh {img_file}: {e}")
                            processed_images += 1
                            continue
                            
            except Exception as e:
                print(f"Lỗi xử lý zip bytes: {e}")
                yield {"type": "error", "message": f"Lỗi xử lý file: {str(e)}"}
                return
        else:
            # Handle file path input
            img_files = [os.path.join(self.path_img, f) 
                        for f in os.listdir(self.path_img) 
                        if f.lower().endswith(('.jpg', '.png', '.jpeg'))]
            
            total_images = len(img_files)
            if total_images == 0:
                yield {"type": "warning", "message": "Không có ảnh nào"}
                return
            
            estimated_cccd = (total_images + 1) // 2
            
            yield {
                "type": "progress",
                "step": "detect_cccd",
                "message": f"Đang phát hiện CCCD từ {total_images} ảnh...",
                "percent": self.base_percent + 10,
                "total_images": total_images,
                "estimated_cccd": estimated_cccd,
                "processed": 0
            }
            
            for i, img_path in enumerate(img_files):
                results = self.model1.predict(source=img_path, conf=0.5, save=False)
                r = results[0]
                if len(r.keypoints) == 0:
                    processed_images += 1
                    continue
                image_bgr = cv2.imread(r.path)
                file_name = os.path.basename(r.path)
                file_name = os.path.splitext(file_name)[0]
                kpts = r.keypoints.xy[0].cpu().numpy()
                if kpts.shape[0] != 4:
                    processed_images += 1
                    continue

                conf = r.keypoints.conf[0].cpu().numpy()
                avg_conf = np.mean(conf) * 100

                if avg_conf < 75:
                    h, w = image_bgr.shape[:2]
                    text = f"Không đạt ({avg_conf:.1f}%)"
                    font = cv2.FONT_HERSHEY_SIMPLEX
                    scale = 1.2
                    thickness = 3
                    text_size = cv2.getTextSize(text, font, scale, thickness)[0]
                    text_x = (w - text_size[0]) // 2
                    text_y = (h + text_size[1]) // 2
                    cv2.putText(image_bgr, text, (text_x, text_y), font, scale, (0, 0, 255), thickness)
                    cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), image_bgr)
                    processed_images += 1
                    continue

                pts = kpts.astype(np.float32)
                ordered_pts = order_points(pts)

                text = f"Conf: {avg_conf:.1f}%"
                text_org = (int(ordered_pts[0][0]), int(ordered_pts[0][1]) - 10)
                cv2.putText(image_bgr, text, text_org, cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 255, 0), 2)

                (tl, tr, br, bl) = ordered_pts
                widthA = np.linalg.norm(br - bl)
                widthB = np.linalg.norm(tr - tl)
                maxWidth = int(max(widthA, widthB))
                heightA = np.linalg.norm(tr - br)
                heightB = np.linalg.norm(tl - bl)
                maxHeight = int(max(heightA, heightB))

                dst = np.array([
                    [0, 0],
                    [maxWidth - 1, 0],
                    [maxWidth - 1, maxHeight - 1],
                    [0, maxHeight - 1]
                ], dtype="float32")
                M = cv2.getPerspectiveTransform(ordered_pts, dst)
                warped = cv2.warpPerspective(image_bgr, M, (maxWidth, maxHeight))
                cv2.polylines(image_bgr, [ordered_pts.astype(np.int32)], isClosed=True, color=(0, 255, 0), thickness=2)

                cv2.imwrite(os.path.join(self.work_md1, f"{file_name}.jpg"), warped)
                cv2.imwrite(os.path.join(self.work_dir, "md1", f"boxed_{file_name}.jpg"), image_bgr)
                
                processed_images += 1
                
                progress_percent = self.base_percent + 10 + int((processed_images / total_images) * 20)
                yield {
                    "type": "progress",
                    "step": "detect_cccd",
                    "message": f"Đã phát hiện {processed_images}/{total_images} ảnh",
                    "percent": progress_percent,
                    "processed": processed_images,
                    "total_images": total_images
                }
    
    def detect_corners_streaming(self):
        """Detect corners với streaming progress"""
        folder = self.work_md1
        img_files = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(('.jpg', '.jpeg', '.png'))
        ]
        total = len(img_files)
        if total == 0:
            yield {"type": "warning", "message": "Không có ảnh CCCD để căn chỉnh"}
            return
        
        yield {
            "type": "progress",
            "step": "detect_corners",
            "message": f"Đang căn chỉnh {total} ảnh CCCD...",
            "percent": self.base_percent + 35,
            "total": total,
            "processed": 0
        }
        
        for i, img_path in enumerate(img_files):
            results = self.model2.predict(source=img_path, conf=0.5, save=False)
            r = results[0]
            img = cv2.imread(r.path)
            file_name = os.path.basename(r.path)
            file_name = os.path.splitext(file_name)[0]
            centers = {}
            for box in r.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0].cpu().numpy())
                cls = int(box.cls[0].cpu().item())
                name = self.model2.names[cls]
                cx = int((x1 + x2) / 2)
                cy = int((y1 + y2) / 2)
                centers[name] = (cx, cy)

            if 'quoc_huy' in centers and 'qr' in centers:
                ptA, ptB = centers['quoc_huy'], centers['qr']
            elif 'chip' in centers and 'm_red' in centers:
                ptA, ptB = centers['chip'], centers['m_red']
            else:
                cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), img)
                # Yield progress ngay cả khi skip
                progress_percent = self.base_percent + 35 + int(((i + 1) / total) * 15)
                yield {
                    "type": "progress",
                    "step": "detect_corners",
                    "message": f"Đang căn chỉnh {i + 1}/{total} ảnh",
                    "percent": progress_percent,
                    "processed": i + 1,
                    "total": total
                }
                continue

            dx, dy = ptB[0] - ptA[0], ptB[1] - ptA[1]
            angle = math.degrees(math.atan2(dy, dx))
            rotate_angle = -angle
            
            if abs(rotate_angle) < 10:
                cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), img)
                # Yield progress ngay cả khi skip
                progress_percent = self.base_percent + 35 + int(((i + 1) / total) * 15)
                yield {
                    "type": "progress",
                    "step": "detect_corners",
                    "message": f"Đang căn chỉnh {i + 1}/{total} ảnh",
                    "percent": progress_percent,
                    "processed": i + 1,
                    "total": total
                }
                continue
                
            h, w = img.shape[:2]
            center_img = (w // 2, h // 2)
            M = cv2.getRotationMatrix2D(center_img, rotate_angle, 1.0)

            cos = abs(M[0, 0])
            sin = abs(M[0, 1])
            new_w = int(h * sin + w * cos)
            new_h = int(h * cos + w * sin)
            M[0, 2] += (new_w / 2) - center_img[0]
            M[1, 2] += (new_h / 2) - center_img[1]
            
            def transform(pt):
                x, y = pt
                new_x = M[0,0]*x + M[0,1]*y + M[0,2]
                new_y = M[1,0]*x + M[1,1]*y + M[1,2]
                return (new_x, new_y)

            ptA_new = transform(ptA)
            ptB_new = transform(ptB)

            if ptB_new[0] < ptA_new[0]:
                rotate_angle += 180

            M_final = cv2.getRotationMatrix2D(center_img, rotate_angle, 1.0)
            cos = abs(M_final[0, 0])
            sin = abs(M_final[0, 1])
            new_w = int(h * sin + w * cos)
            new_h = int(h * cos + w * sin)
            M_final[0, 2] += (new_w / 2) - center_img[0]
            M_final[1, 2] += (new_h / 2) - center_img[1]

            rotated = cv2.warpAffine(
                img,
                M_final,
                (new_w, new_h),
                flags=cv2.INTER_LINEAR,
                borderMode=cv2.BORDER_CONSTANT,
                borderValue=(255, 255, 255)
            )
            cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), rotated)
            
            progress_percent = self.base_percent + 35 + int(((i + 1) / total) * 15)
            yield {
                "type": "progress",
                "step": "detect_corners",
                "message": f"Đã căn chỉnh {i + 1}/{total} ảnh",
                "percent": progress_percent,
                "processed": i + 1,
                "total": total
            }
    
    def detect_lines_streaming(self):
        """Detect lines và OCR với streaming progress"""
        folder = self.work_md2
        img_files = [
            os.path.join(folder, f)
            for f in os.listdir(folder)
            if f.lower().endswith(('.jpg', '.png', '.jpeg'))
        ]

        total = len(img_files)
        if total == 0:
            yield {"type": "warning", "message": "Không có ảnh để OCR"}
            return

        yield {
            "type": "progress",
            "step": "detect_lines",
            "message": f"Đang nhận dạng văn bản từ {total} ảnh...",
            "percent": self.base_percent + 55,
            "total": total,
            "processed": 0
        }

        for i, img_path in enumerate(img_files):
            results = self.model3.predict(source=img_path, conf=0.5, save=False)
            r = results[0]
            
            # Yield progress sau mỗi ảnh để giữ connection sống
            progress_percent = self.base_percent + 55 + int(((i + 1) / total) * 15)
            yield {
                "type": "progress",
                "step": "detect_lines",
                "message": f"Đang phát hiện văn bản: {i + 1}/{total} ảnh",
                "percent": progress_percent,
                "processed": i + 1,
                "total": total
            }
            
            if r.masks is None or len(r.masks.xy) == 0:
                continue

            image_bgr = cv2.imread(r.path)
            image_clean = image_bgr.copy()
            file_name = os.path.basename(r.path)
            file_name = os.path.splitext(file_name)[0]
            
            for j, polygon in enumerate(r.masks.xy):
                points = polygon.astype(int)
                if points.shape[0] < 4:
                    continue
                pts = points.reshape((-1, 1, 2))
                cv2.polylines(image_bgr, [pts], isClosed=True, color=(0, 255, 0), thickness=2)
                class_id = int(r.boxes.cls[j]) if r.boxes is not None else 0
                conf = float(r.boxes.conf[j]) if r.boxes is not None else 0.0
                class_name = self.model3.names[class_id] if hasattr(self.model3, 'names') else str(class_id)
                label_text = f"{class_name} {conf:.2f}"

                x, y = points[0]
                font = cv2.FONT_HERSHEY_SIMPLEX
                font_scale = 0.5
                thickness = 1
                text_size, _ = cv2.getTextSize(label_text, font, font_scale, thickness)
                text_w, text_h = text_size

                cv2.rectangle(image_bgr, (x, y - text_h - 4), (x + text_w, y), (0, 255, 0), -1)
                cv2.putText(image_bgr, label_text, (x, y - 2), font, font_scale, (0, 0, 0), thickness, cv2.LINE_AA)

                mask = np.zeros(image_clean.shape[:2], dtype=np.uint8)
                cv2.fillPoly(mask, [pts], 255)

                masked = cv2.bitwise_and(image_clean, image_clean, mask=mask)

                x, y, w, h = cv2.boundingRect(pts)
                crop = masked[y:y+h, x:x+w]

                crop_name = os.path.join(self.work_md3, f"{file_name.replace('.jpg','')}-{class_name}.jpg")
                cv2.imwrite(crop_name, crop)
            
            original_ext = os.path.splitext(os.path.basename(r.path))[1] or '.jpg'
            boxed_name = os.path.join(self.work_dir, "md3", "detected_results", f"boxed_{file_name}{original_ext}")
            cv2.imwrite(boxed_name, image_bgr)
        
        # OCR phase
        yield {
            "type": "progress",
            "step": "ocr",
            "message": "Đang nhận dạng văn bản (OCR)...",
            "percent": self.base_percent + 75
        }
        
        from PIL import Image, ImageDraw, ImageFont
        from vietocr.tool.predictor import Predictor
        from vietocr.tool.config import Cfg

        if self.vietocr_detector is not None:
            detector = self.vietocr_detector
        elif self.cached_models and self.cached_models.get('vietocr_detector'):
            detector = self.cached_models['vietocr_detector']
            self.vietocr_detector = detector
        else:
            config = Cfg.load_config_from_name('vgg_transformer')
            config['weights'] = os.path.join(self.base_dir, 'vgg_transformer.pth')
            config['cnn']['pretrained'] = False
            config['device'] = 'cpu'
            detector = Predictor(config)
            if self.cached_models:
                self.cached_models['vietocr_detector'] = detector

        crop_folder = self.work_md3
        goc_folder = self.work_md2
        results_folder = self.work_temp_rs

        ocr_result_file = os.path.join(self.work_md4, 'ocr_results.txt')
        ocr_data = []
        
        crop_files = [f for f in os.listdir(crop_folder) if f.lower().endswith('.jpg')]
        total_crops = len(crop_files)
        
        with open(ocr_result_file, 'w', encoding='utf-8') as f_out:
            for i, file_name in enumerate(crop_files):
                try:
                    img_path = os.path.join(crop_folder, file_name)
                    img = Image.open(img_path)
                    text = detector.predict(img).strip()
                    text_ki = [
                        "CỤC", "TRƯỞNG", "CỤC", "CẢNH", "SÁT",
                        "QUẢN", "LÝ", "HÀNH", "CHÍNH", "VỀ",
                        "TRẬT", "TỰ", "XÃ", "HỘI"
                    ]

                    if len(text) > 14 and text.isupper() and any(word in text for word in text_ki):
                        text = "CỤC TRƯỞNG CỤC CẢNH SÁT QUẢN LÝ HÀNH CHÍNH VỀ TRẬT TỰ XÃ HỘI"
                    f_out.write(f"{file_name}\t{text}\n")
                    file_name_no_ext = os.path.splitext(file_name)[0]
                    ocr_data.append((file_name_no_ext, text))
                except Exception as e:
                    print(f"Error processing OCR for {file_name}: {e}")
                    file_name_no_ext = os.path.splitext(file_name)[0]
                    ocr_data.append((file_name_no_ext, ""))
                
                # Yield progress sau mỗi crop để giữ connection sống
                progress_percent = self.base_percent + 75 + int(((i + 1) / max(1, total_crops)) * 15)
                yield {
                    "type": "progress",
                    "step": "ocr",
                    "message": f"Đang OCR: {i + 1}/{total_crops} vùng",
                    "percent": min(progress_percent, self.base_percent + 90),
                    "processed": i + 1,
                    "total": total_crops
                }

        # Build results
        goc_dict = {}
        for crop_name, text in ocr_data:
            parts = crop_name.replace('.jpg', '').split('-')
            goc_name = parts[0]
            try:
                label = parts[1]
            except:
                label = parts[2] if len(parts) > 2 else 'unknown'
            if goc_name not in goc_dict:
                goc_dict[goc_name] = []
            if label == "noi_cap":
                text = "CỤC TRƯỞNG CỤC CẢNH SÁT QUẢN LÝ HÀNH CHÍNH VỀ TRẬT TỰ XÃ HỘI"
            goc_dict[goc_name].append((label, text))
        
        for goc_name, infos in goc_dict.items():
            img_path = os.path.join(goc_folder, f"{goc_name}.jpg")
            if not os.path.exists(img_path):
                continue

            image_cv = cv2.imread(img_path)
            h, w = image_cv.shape[:2]

            box_w = int(w)
            box_h = int(h)
            box_x = 10
            box_y = 10

            overlay = image_cv.copy()
            cv2.rectangle(overlay, (box_x, box_y), (box_x + box_w, box_y + box_h), (255, 255, 255), -1)
            alpha = 0.3
            image_cv = cv2.addWeighted(overlay, alpha, image_cv, 1 - alpha, 0)

            image_pil = cv2.cvtColor(image_cv, cv2.COLOR_BGR2RGB)
            image_pil = Image.fromarray(image_pil)

            draw = ImageDraw.Draw(image_pil)

            font_path = os.path.join(self.base_dir, "arial.ttf")
            font_size = max(20, int(h * 0.02))

            try:
                font = ImageFont.truetype(font_path, font_size)
            except:
                font = ImageFont.load_default()

            y0 = box_y + 20
            dy = int(font_size * 1.5)
            for label, text in infos:
                line = f"{label}: {text}"
                draw.text((box_x + 10, y0), line, font=font, fill=(0, 0, 0))
                y0 += dy

            image_result = cv2.cvtColor(np.array(image_pil), cv2.COLOR_RGB2BGR)

            result_img = os.path.join(results_folder, f"{goc_name}.jpg")
            cv2.imwrite(result_img, image_result)
            
            result_txt = os.path.join(results_folder, f"{goc_name}.txt")
            with open(result_txt, 'w', encoding='utf-8') as f:
                for label, text in infos:
                    f.write(f"{label}: {text}\n")
        
        yield {
            "type": "progress",
            "step": "ocr_complete",
            "message": "Hoàn thành nhận dạng văn bản",
            "percent": self.base_percent + 95
        }
    
    def excel_to_png_streaming(self, excel_bytes_input):
        """
        Download images from Excel bytes với streaming progress
        """
        try:
            if isinstance(excel_bytes_input, str):
                excel_bytes = base64.b64decode(excel_bytes_input)
            else:
                excel_bytes = excel_bytes_input
            
            excel_stream = BytesIO(excel_bytes)
            df = pd.read_excel(excel_stream, header=0)
        except Exception as e:
            yield {
                "type": "error",
                "message": f"Lỗi đọc file Excel bytes: {e}"
            }
            return

        if df.shape[1] < 3:
            yield {
                "type": "error",
                "message": "File Excel cần ít nhất 3 cột: file_name, mt_url, ms_url."
            }
            return

        df_sub = df.iloc[:, :3].fillna("")
        lines_excel = []
        for _, row in df_sub.iterrows():
            file_name = str(row.iloc[0]).strip()
            mt_url = str(row.iloc[1]).strip()
            ms_url = str(row.iloc[2]).strip()
            if any([file_name, mt_url, ms_url]):
                lines_excel.append((file_name, mt_url, ms_url))

        if not lines_excel:
            yield {
                "type": "error",
                "message": "Không có dữ liệu hợp lệ trong file Excel."
            }
            return

        def extract_file_id(url: str):
            file_id = None
            if "drive.google.com" in url:
                if "/file/d/" in url:
                    file_id = url.split("/file/d/")[1].split("/")[0]
                elif "id=" in url:
                    file_id = url.split("id=")[1].split("&")[0]
                elif "/open?id=" in url:
                    file_id = url.split("/open?id=")[1].split("&")[0]
            return file_id

        def download_from_drive(url: str) -> bytes:
            import requests
            file_id = extract_file_id(url)
            if not file_id:
                raise Exception("Không thể trích xuất ID file từ URL")
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            session = requests.Session()
            response = session.get(download_url, stream=True)
            if response.status_code != 200:
                raise Exception(f"Không tải được file, status {response.status_code}")
            for key, value in response.cookies.items():
                if key.startswith("download_warning"):
                    token = value
                    params = {"id": file_id, "confirm": token}
                    response = session.get(download_url, params=params, stream=True)
                    break
            if response.status_code != 200:
                raise Exception(f"Không tải được file (sau khi confirm), status {response.status_code}")

            buf = BytesIO()
            for chunk in response.iter_content(1024):
                if chunk:
                    buf.write(chunk)
            return buf.getvalue()
        
        total_rows = len(lines_excel)
        # Tính total_cccd từ số dòng Excel (mỗi dòng = 1 CCCD = 2 ảnh)
        total_cccd = total_rows
        estimated_total_images = total_rows * 2
        
        yield {
            "type": "progress",
            "step": "excel_download",
            "message": f"Đang tải và xử lý ảnh từ Excel ({total_cccd} CCCD)...",
            "percent": 5,
            "total_rows": total_rows,
            "total_cccd": total_cccd,
            "estimated_total_images": estimated_total_images,
            "processed": 0,
            "processed_cccd": 0
        }
        
        mem_zip = BytesIO()
        total_images = 0

        # Xử lý từng cặp ảnh ngay sau khi download
        processed_cccd = 0
        all_results = {"customer": []}
        
        try:
            # Xử lý từng cặp ảnh (mt + ms)
            for index, (file_name, mt_url, ms_url) in enumerate(lines_excel, start=1):
                file_name = file_name.strip()
                if not file_name:
                    continue
                
                # Tạo ZIP tạm cho cặp ảnh này
                pair_zip = BytesIO()
                pair_images = []
                
                # Tải mặt trước
                if mt_url:
                    try:
                        mt_bytes = download_from_drive(mt_url)
                        mt_name = f"{file_name}mt.png"
                        pair_images.append((mt_name, mt_bytes))
                        total_images += 1
                    except Exception as e:
                        print(f"Lỗi tải mặt trước {file_name}: {e}")

                # Tải mặt sau
                if ms_url:
                    try:
                        ms_bytes = download_from_drive(ms_url)
                        ms_name = f"{file_name}ms.png"
                        pair_images.append((ms_name, ms_bytes))
                        total_images += 1
                    except Exception as e:
                        print(f"Lỗi tải mặt sau {file_name}: {e}")
                
                # Nếu có ít nhất 1 ảnh, xử lý ngay (detect CCCD)
                if pair_images:
                    with zipfile.ZipFile(pair_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                        for img_name, img_bytes in pair_images:
                            zf.writestr(img_name, img_bytes)
                    
                    pair_zip.seek(0)
                    pair_zip_bytes = pair_zip.getvalue()
                    
                    # Xử lý cặp ảnh này ngay (detect CCCD)
                    try:
                        # Tạo worker mới để xử lý cặp ảnh này (tránh conflict)
                        pair_worker = DetectWorkerStreaming(
                            input_path=pair_zip_bytes,
                            type_=1,
                            cached_models=self.cached_models,
                            base_percent=5 + int((index / total_rows) * 90)  # Progress từ 5% đến 95%
                        )
                        pair_worker.init_temp_dirs()
                        
                        # Xử lý cặp ảnh này
                        pair_result = None
                        for event in pair_worker.run_streaming():
                            if event.get('type') == 'progress':
                                # Cập nhật progress với total_cccd và processed_cccd
                                event_progress = event.get('percent', 0)
                                event_message = event.get('message', 'Đang xử lý...')
                                
                                # Yield progress với thông tin CCCD
                                yield {
                                    "type": "progress",
                                    "step": "excel_process",
                                    "message": f"Đang xử lý CCCD {index}/{total_rows}: {event_message}",
                                    "percent": min(95, max(5, event_progress)),
                                    "processed": index,
                                    "total_rows": total_rows,
                                    "total_images": total_images,
                                    "total_cccd": total_cccd,
                                    "processed_cccd": processed_cccd
                                }
                            elif event.get('type') == 'complete':
                                # Đã xử lý xong cặp ảnh này
                                pair_result = event.get('data')
                                processed_cccd = index
                                
                                # Merge kết quả vào all_results
                                if pair_result and isinstance(pair_result, dict) and 'customer' in pair_result:
                                    all_results['customer'].extend(pair_result['customer'])
                                
                                yield {
                                    "type": "progress",
                                    "step": "excel_process",
                                    "message": f"Đã xử lý {index}/{total_rows} CCCD",
                                    "percent": 5 + int((index / total_rows) * 90),
                                    "processed": index,
                                    "total_rows": total_rows,
                                    "total_images": total_images,
                                    "total_cccd": total_cccd,
                                    "processed_cccd": processed_cccd
                                }
                                break
                        
                        # Cleanup worker tạm
                        pair_worker.cleanup_temp_dirs()
                        
                    except Exception as e:
                        print(f"Lỗi xử lý cặp ảnh {file_name}: {e}")
                        # Tiếp tục với cặp tiếp theo
                        continue
                
                # Yield progress sau mỗi cặp
                progress_percent = 5 + int((index / total_rows) * 90)
                yield {
                    "type": "progress",
                    "step": "excel_process",
                    "message": f"Đã xử lý {index}/{total_rows} CCCD",
                    "percent": min(95, progress_percent),
                    "processed": index,
                    "total_rows": total_rows,
                    "total_images": total_images,
                    "total_cccd": total_cccd,
                    "processed_cccd": processed_cccd
                }
            
            # Hoàn thành - trả về kết quả tổng hợp
            all_results['status'] = "success"
            all_results['message'] = f"Đã xử lý {processed_cccd}/{total_cccd} CCCD từ Excel"
            
            yield {
                "type": "complete",
                "data": all_results
            }
        except Exception as e:
            yield {
                "type": "error",
                "message": f"Lỗi trong quá trình xử lý Excel → ảnh: {e}"
            }
    
    def pdf_to_png_streaming(self, pdf_bytes_input):
        """
        Convert PDF bytes to PNG images và xử lý từng cặp trang ngay sau khi convert
        Mỗi cặp trang (mt + ms) được convert → detect CCCD ngay → tiếp tục cặp tiếp theo
        """
        try:
            # Convert base64 string to bytes if needed
            if isinstance(pdf_bytes_input, str):
                pdf_bytes = base64.b64decode(pdf_bytes_input)
            else:
                pdf_bytes = pdf_bytes_input
            
            # Read PDF from bytes
            pdf_stream = BytesIO(pdf_bytes)
            doc = fitz.open(stream=pdf_stream, filetype="pdf")
            total_pages = len(doc)
            
            # Tính total_cccd từ số trang PDF (mỗi 2 trang = 1 CCCD)
            total_cccd = (total_pages + 1) // 2
            estimated_total_images = total_pages
            
            yield {
                "type": "progress",
                "step": "pdf_convert",
                "message": f"Đang chuyển và xử lý PDF ({total_cccd} CCCD)...",
                "percent": 5,
                "total_pages": total_pages,
                "total_cccd": total_cccd,
                "estimated_total_images": estimated_total_images,
                "processed": 0,
                "processed_cccd": 0
            }
            
            # Xử lý từng cặp trang (mt + ms)
            processed_cccd = 0
            total_images = 0
            all_results = {"customer": []}
            
            try:
                # Xử lý từng cặp trang (2 trang = 1 CCCD)
                for pair_index in range(1, (total_pages + 1) // 2 + 1):
                    # Tạo ZIP tạm cho cặp trang này
                    pair_zip = BytesIO()
                    pair_images = []
                    
                    # Convert trang mt (trang chẵn: 0, 2, 4...)
                    mt_page_index = (pair_index - 1) * 2
                    if mt_page_index < total_pages:
                        try:
                            page = doc[mt_page_index]
                            pix = page.get_pixmap()
                            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                            
                            mt_name = f"{pair_index}mt.png"
                            img_buf = BytesIO()
                            img.save(img_buf, format="PNG")
                            img_buf.seek(0)
                            pair_images.append((mt_name, img_buf.getvalue()))
                            total_images += 1
                        except Exception as e:
                            print(f"Lỗi convert trang mt {mt_page_index}: {e}")
                    
                    # Convert trang ms (trang lẻ: 1, 3, 5...)
                    ms_page_index = (pair_index - 1) * 2 + 1
                    if ms_page_index < total_pages:
                        try:
                            page = doc[ms_page_index]
                            pix = page.get_pixmap()
                            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                            
                            ms_name = f"{pair_index}ms.png"
                            img_buf = BytesIO()
                            img.save(img_buf, format="PNG")
                            img_buf.seek(0)
                            pair_images.append((ms_name, img_buf.getvalue()))
                            total_images += 1
                        except Exception as e:
                            print(f"Lỗi convert trang ms {ms_page_index}: {e}")
                    
                    # Nếu có ít nhất 1 ảnh, xử lý ngay (detect CCCD)
                    if pair_images:
                        with zipfile.ZipFile(pair_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                            for img_name, img_bytes in pair_images:
                                zf.writestr(img_name, img_bytes)
                        
                        pair_zip.seek(0)
                        pair_zip_bytes = pair_zip.getvalue()
                        
                        # Xử lý cặp ảnh này ngay (detect CCCD)
                        try:
                            # Tạo worker mới để xử lý cặp ảnh này (tránh conflict)
                            pair_worker = DetectWorkerStreaming(
                                input_path=pair_zip_bytes,
                                type_=1,
                                cached_models=self.cached_models,
                                base_percent=5 + int((pair_index / total_cccd) * 90)  # Progress từ 5% đến 95%
                            )
                            pair_worker.init_temp_dirs()
                            
                            # Xử lý cặp ảnh này
                            pair_result = None
                            for event in pair_worker.run_streaming():
                                if event.get('type') == 'progress':
                                    # Cập nhật progress với total_cccd và processed_cccd
                                    event_progress = event.get('percent', 0)
                                    event_message = event.get('message', 'Đang xử lý...')
                                    
                                    # Yield progress với thông tin CCCD
                                    yield {
                                        "type": "progress",
                                        "step": "pdf_process",
                                        "message": f"Đang xử lý CCCD {pair_index}/{total_cccd}: {event_message}",
                                        "percent": min(95, max(5, event_progress)),
                                        "processed": pair_index,
                                        "total_pages": total_pages,
                                        "total_images": total_images,
                                        "total_cccd": total_cccd,
                                        "processed_cccd": processed_cccd
                                    }
                                elif event.get('type') == 'complete':
                                    # Đã xử lý xong cặp ảnh này
                                    pair_result = event.get('data')
                                    processed_cccd = pair_index
                                    
                                    # Merge kết quả vào all_results
                                    if pair_result and isinstance(pair_result, dict) and 'customer' in pair_result:
                                        all_results['customer'].extend(pair_result['customer'])
                                    
                                    yield {
                                        "type": "progress",
                                        "step": "pdf_process",
                                        "message": f"Đã xử lý {pair_index}/{total_cccd} CCCD",
                                        "percent": 5 + int((pair_index / total_cccd) * 90),
                                        "processed": pair_index,
                                        "total_pages": total_pages,
                                        "total_images": total_images,
                                        "total_cccd": total_cccd,
                                        "processed_cccd": processed_cccd
                                    }
                                    break
                            
                            # Cleanup worker tạm
                            pair_worker.cleanup_temp_dirs()
                            
                        except Exception as e:
                            print(f"Lỗi xử lý cặp trang {pair_index}: {e}")
                            # Tiếp tục với cặp tiếp theo
                            continue
                    
                    # Yield progress sau mỗi cặp
                    progress_percent = 5 + int((pair_index / total_cccd) * 90)
                    yield {
                        "type": "progress",
                        "step": "pdf_process",
                        "message": f"Đã xử lý {pair_index}/{total_cccd} CCCD",
                        "percent": min(95, progress_percent),
                        "processed": pair_index,
                        "total_pages": total_pages,
                        "total_images": total_images,
                        "total_cccd": total_cccd,
                        "processed_cccd": processed_cccd
                    }
                
                # Hoàn thành - trả về kết quả tổng hợp
                all_results['status'] = "success"
                all_results['message'] = f"Đã xử lý {processed_cccd}/{total_cccd} CCCD từ PDF"
                
                yield {
                    "type": "complete",
                    "data": all_results
                }
            except Exception as e:
                yield {
                    "type": "error",
                    "message": f"Lỗi trong quá trình xử lý PDF: {e}"
                }
            finally:
                doc.close()
                
        except Exception as e:
            yield {
                "type": "error",
                "message": f"Lỗi đọc file PDF: {e}"
            }


class CCCDExtractorStreaming():
    """CCCDExtractor với khả năng streaming progress events"""
    
    def __init__(self, config=None, cached_models=None):
        self.config = config or {}
        self.cached_models = cached_models
    
    def handle_task(self, data_inp: dict):
        """Non-streaming task handler (for PDF/Excel conversion)"""
        func_type = data_inp.get("func_type")
        inp_path = data_inp.get("inp_path")
        job_id = data_inp.get("job_id")
        total_cccd = data_inp.get("total_cccd", 0)
        results = DetectWorker(input_path=inp_path, type_=func_type, cached_models=self.cached_models, job_id=job_id, total_cccd=total_cccd).run()
        return results
    
    def handle_task_streaming(self, data_inp: dict, base_percent: int = 0):
        """Streaming task handler - yields progress events"""
        func_type = data_inp.get("func_type")
        inp_path = data_inp.get("inp_path")
        
        worker = DetectWorkerStreaming(
            input_path=inp_path, 
            type_=func_type, 
            cached_models=self.cached_models,
            base_percent=base_percent
        )
        
        for event in worker.run_streaming():
            yield event
    
    def handle_excel_streaming(self, data_inp: dict):
        """Handle Excel with streaming progress for downloads"""
        inp_path = data_inp.get("inp_path")
        
        worker = DetectWorkerStreaming(
            input_path=inp_path, 
            type_=3, 
            cached_models=self.cached_models,
            base_percent=0
        )
        worker.init_temp_dirs()
        
        try:
            for event in worker.excel_to_png_streaming(inp_path):
                yield event
        finally:
            worker.cleanup_temp_dirs()
    
    def handle_pdf_streaming(self, data_inp: dict):
        """Handle PDF with streaming progress - convert và xử lý từng cặp trang ngay"""
        inp_path = data_inp.get("inp_path")
        
        worker = DetectWorkerStreaming(
            input_path=inp_path, 
            type_=2, 
            cached_models=self.cached_models,
            base_percent=0
        )
        worker.init_temp_dirs()
        
        try:
            for event in worker.pdf_to_png_streaming(inp_path):
                yield event
        finally:
            worker.cleanup_temp_dirs()


if __name__ == "__main__":
    import os
    import shutil
    # Lấy đường dẫn tuyệt đối của thư mục chứa main.py
    main_file_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.join(main_file_dir, "__pycache__")
    folders = [
        os.path.join(base_dir, "md1"),
        os.path.join(base_dir, "md2"),
        os.path.join(base_dir, "md3"),
        os.path.join(base_dir, "md4")
    ]
    for folder in folders:
        if os.path.exists(folder):
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Xoá file
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Xoá folder con
                except Exception as e:
                    print(f"Không thể xoá {file_path}. Lỗi: {e}")
        else:
            print(f"Không tìm thấy folder: {folder}")
    CCCDExtractor()

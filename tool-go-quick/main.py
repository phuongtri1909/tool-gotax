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

try:
        import vietocr
except ImportError:
        print("❌ VietOCR chưa được cài. Đang tiến hành cài đặt...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--user", "vietocr"])
def count_files( folderPath):
        """ Đếm số file trong thư mục """
        return len([f for f in os.listdir(folderPath) if os.path.isfile(os.path.join(folderPath, f))])
import shutil

class DetectWorker():
    def __init__(self,input_path:str = None,type_:int = 0, cached_models=None):
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
        # Tạo unique session ID cho mỗi request để tránh conflict khi chạy đồng thời
        self.session_id = str(uuid.uuid4())[:8]
        self.work_dir = os.path.join(self.base_dir, f"work_{self.session_id}")
        
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
            
            return {
                "status": "success",
                "message": "Chuyển PDF → PNG và đóng gói ZIP thành công.",
                "total_images": total_images,
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

            return {
                "status": "success",
                "message": "Đã tải ảnh từ Excel và đóng gói ZIP thành công.",
                "total_rows": len(lines_excel),
                "total_images": total_images,
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
        for index,file_name in enumerate(sorted(final_files),start=1):
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
        info_list["status"] = "success"
        info_list["message"] = "Đã trích xuất thông tin các CCCD"
        if len(info_list["customer"]) == 0:
            info_list["message"] = "Không tìm thấy thông tin CCCD nào."
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
                        return
                    
                    for i, img_file in enumerate(img_files):
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
                            print(f"[!] {file_name} độ chính xác thấp ({avg_conf:.1f}%)")
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
            except Exception as e:
                print(f"Lỗi xử lý zip bytes: {e}")
                return
        else:
            # Handle file path input
            img_files = [os.path.join(self.path_img, f) 
                        for f in os.listdir(self.path_img) 
                        if f.lower().endswith(('.jpg', '.png', '.jpeg'))]

            total = len(img_files)
            if total == 0:
                print("Không có ảnh nào.")
                return
            for i, img_path in enumerate(img_files):
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
                    print(f"[!] {file_name} độ chính xác thấp ({avg_conf:.1f}%)")
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

    def detect_lines(self):
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

        step = 30 / total

        for i, img_path in enumerate(img_files):
            results = self.model3.predict(source=img_path, conf=0.5, save=False)
            r = results[0]
            if r.masks is None or len(r.masks.xy) == 0:
                print(f"[!] Bỏ qua {r.path} vì không detect được mask nào")
                continue

            image_bgr = cv2.imread(r.path)
            image_clean = image_bgr.copy()
            file_name = os.path.basename(r.path)
            file_name = os.path.splitext(file_name)[0] 
            for j, polygon in enumerate(r.masks.xy):
                points = polygon.astype(int)
                if points.shape[0] < 4:
                    print(f"[!] {file_name} => Bỏ qua mask vì <4 điểm")
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
            boxed_name = os.path.join(self.work_dir, "md3", "detected_results", f"boxed_{file_name}")
            cv2.imwrite(boxed_name, image_bgr)
            print(f"[✓] {file_name} => Vẽ + crop {len(r.masks.xy)} polygon")
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
        step_ = 30/len(crop_folder)
        with open(ocr_result_file, 'w', encoding='utf-8') as f_out:
            i = 0
            for file_name in os.listdir(crop_folder):
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
                    file_name = os.path.splitext(file_name)[0] 
                    ocr_data.append((file_name, text))
                    print(f"[✓] {file_name}: {text}")
        print(f"\n🎉 DONE OCR! Kết quả đã lưu: {ocr_result_file}")

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
        for goc_name, infos in goc_dict.items():
            img_path = os.path.join(goc_folder, f"{goc_name}.jpg")
            if not os.path.exists(img_path):
                print(f"[!] Không tìm thấy ảnh gốc: {img_path}")
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
                print("[!] Không tìm thấy font, dùng mặc định.")
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
            print(f"[✓]- Đã lưu: {result_img} & {result_txt}")
        print("\n✅ DONE! Tất cả ảnh + txt đã lưu vào:", results_folder)

    def detect_corners(self):
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

        step = 20 / total
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
                print(f"🟢 quoc_huy - qr | {file_name}")
            elif 'chip' in centers and 'm_red' in centers:
                ptA, ptB = centers['chip'], centers['m_red']
                print(f"🟢 chip - m-red | {file_name}")
            else:
                print(f"❌ Thiếu điểm | {file_name}")
                cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), img)  # Lưu nguyên gốc
                continue

            dx, dy = ptB[0] - ptA[0], ptB[1] - ptA[1]
            angle = math.degrees(math.atan2(dy, dx))
            rotate_angle = -angle   # CHUẨN: Luôn lấy -angle để vector nằm ngang
            print(f"Góc ban đầu: {angle:.2f}° => Xoay trước: {rotate_angle:.2f}°")
            if abs(rotate_angle) < 10:
                cv2.imwrite(os.path.join(self.work_md2, f"{file_name}.jpg"), img)  # Lưu nguyên gốc
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



from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Any, Callable, Protocol
import subprocess
import platform
import hashlib
import logging
import os
import math
import cv2
import numpy as np
import fitz  # PyMuPDF
from PIL import Image
import requests
from ultralytics import YOLO
from vietocr.tool.predictor import Predictor
from vietocr.tool.config import Cfg
import openpyxl
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Version control
VERSION = "1.0"  # Matches original pb_t

class ProcessCallbacks(Protocol):
    """Protocol defining callback methods for progress and status updates."""
    
    def on_progress(self, percent: int) -> None:
        """Called to update progress percentage."""
        ...
    
    def on_step(self, step: str) -> None:
        """Called with current processing step description."""
        ...
    
    def on_image(self, image_path: str) -> None:
        """Called when a new image is processed/generated."""
        ...
    
    def on_text(self, text: str) -> None:
        """Called with OCR or other text results."""
        ...
    
    def on_warning(self, message: str) -> None:
        """Called for warning messages that would have been dialogs."""
        ...

@dataclass
class DefaultCallbacks:
    """Default implementation of ProcessCallbacks that logs to console."""
    
    def on_progress(self, percent: int) -> None:
        logger.info(f"Progress: {percent}%")
    
    def on_step(self, step: str) -> None:
        logger.info(f"Step: {step}")
    
    def on_image(self, image_path: str) -> None:
        logger.info(f"Generated image: {image_path}")
    
    def on_text(self, text: str) -> None:
        logger.info(f"Text result: {text}")
    
    def on_warning(self, message: str) -> None:
        logger.warning(message)

# Security and license validation
def get_machine_id() -> Dict[str, str]:
    """Get unique hardware identifier for license validation."""
    if platform.system() == "Windows":
        try:
            output = subprocess.check_output("wmic diskdrive get SerialNumber", shell=True)
            serial = output.decode().split("\n")[1].strip()
            return {'serial_number': serial}
        except Exception as e:
            logger.error(f"Error getting disk serial: {e}")
            return {'serial_number': 'UNKNOWN'}
    else:
        try:
            output = subprocess.check_output(
                "udevadm info --query=property --name=sda | grep ID_SERIAL", 
                shell=True
            )
            serial = output.decode().split('=')[1].strip()
            return {'serial_number': serial}
        except Exception as e:
            logger.error(f"Error getting disk serial: {e}")
            return {'serial_number': 'UNKNOWN'}

class IDCardDetector:
    """Core engine for ID card detection, OCR, and data extraction."""
    
    def __init__(
        self,
        model_dir: str = "./__pycache__",
        callbacks: Optional[ProcessCallbacks] = None
    ):
        """
        Initialize detector with models and callbacks.
        
        Args:
            model_dir: Directory containing YOLO and VietOCR models
            callbacks: Optional callbacks for progress/status updates
        """
        self.callbacks = callbacks or DefaultCallbacks()
        
        # Initialize models
        self.model_card = YOLO(f"{model_dir}/best.pt")
        self.model_corners = YOLO(f"{model_dir}/best2.pt")
        self.model_lines = YOLO(f"{model_dir}/best3.pt")
        
        # Initialize OCR
        config = Cfg.load_config_from_name('vgg_transformer')
        config['weights'] = f'{model_dir}/vgg_transformer.pth'
        config['cnn']['pretrained'] = False
        config['device'] = 'cpu'
        self.ocr = Predictor(config)
        
        self._progress = 0
    
    def _update_progress(self, increment: float) -> None:
        """Update progress percentage."""
        self._progress += increment
        self.callbacks.on_progress(int(self._progress))
    
    def detect_cards(self, input_path: str, output_path: str) -> List[str]:
        """
        Detect and extract ID cards from images.
        
        Args:
            input_path: Path containing input images (.jpg, .png)
            output_path: Path for processed results
            
        Returns:
            List of processed file paths
        """
        self._progress = 0
        results_dir = os.path.join(output_path, "detected_cards")
        os.makedirs(results_dir, exist_ok=True)
        
        # Get input images
        img_files = [
            f for f in os.listdir(input_path)
            if f.lower().endswith(('.jpg', '.png', '.jpeg'))
        ]
        
        if not img_files:
            logger.warning("No images found in input directory")
            return []
            
        processed = []
        progress_step = 20 / len(img_files)  # 20% for this phase
        
        def order_points(pts: np.ndarray) -> np.ndarray:
            """Order points: top-left, top-right, bottom-right, bottom-left."""
            rect = np.zeros((4, 2), dtype="float32")
            s = pts.sum(axis=1)
            diff = np.diff(pts, axis=1)
            rect[0] = pts[np.argmin(s)]     # top-left
            rect[2] = pts[np.argmax(s)]     # bottom-right 
            rect[1] = pts[np.argmin(diff)]  # top-right
            rect[3] = pts[np.argmax(diff)]  # bottom-left
            return rect
        
        for img_file in img_files:
            img_path = os.path.join(input_path, img_file)
            self.callbacks.on_step(f"Processing {img_file}")
            
            # Detect card
            results = self.model_card.predict(source=img_path, conf=0.5, save=False)
            result = results[0]
            
            if len(result.keypoints) == 0:
                logger.warning(f"No keypoints detected in {img_file}")
                continue
                
            image = cv2.imread(result.path)
            kpts = result.keypoints.xy[0].cpu().numpy()
            
            if kpts.shape[0] != 4:
                logger.warning(f"Invalid keypoints in {img_file}")
                continue
                
            # Check confidence
            conf = result.keypoints.conf[0].cpu().numpy()
            avg_conf = float(np.mean(conf) * 100)
            
            if avg_conf < 75:
                logger.warning(f"Low confidence ({avg_conf:.1f}%) in {img_file}")
                continue
                
            # Transform perspective
            pts = kpts.astype(np.float32)
            ordered_pts = order_points(pts)
            
            (tl, tr, br, bl) = ordered_pts
            width_a = np.linalg.norm(br - bl)
            width_b = np.linalg.norm(tr - tl)
            max_width = int(max(width_a, width_b))
            
            height_a = np.linalg.norm(tr - br)
            height_b = np.linalg.norm(tl - bl)
            max_height = int(max(height_a, height_b))
            
            dst = np.array([
                [0, 0],
                [max_width - 1, 0],
                [max_width - 1, max_height - 1],
                [0, max_height - 1]
            ], dtype="float32")
            
            M = cv2.getPerspectiveTransform(ordered_pts, dst)
            warped = cv2.warpPerspective(image, M, (max_width, max_height))
            
            # Save results
            base_name = os.path.splitext(img_file)[0]
            out_path = os.path.join(results_dir, f"{base_name}.jpg")
            cv2.imwrite(out_path, warped)
            
            processed.append(out_path)
            self.callbacks.on_image(out_path)
            self._update_progress(progress_step)
            
        return processed
    
    def detect_corners_and_align(self, card_paths: List[str], output_path: str) -> List[str]:
        """
        Detect corners and align ID cards.
        
        Args:
            card_paths: Paths to detected card images
            output_path: Path for aligned results
            
        Returns:
            List of aligned image paths
        """
        results_dir = os.path.join(output_path, "aligned_cards")
        os.makedirs(results_dir, exist_ok=True)
        
        if not card_paths:
            return []
            
        aligned = []
        progress_step = 20 / len(card_paths)  # Another 20%
        
        for card_path in card_paths:
            results = self.model_corners.predict(source=card_path, conf=0.5, save=False)
            result = results[0]
            
            img = cv2.imread(card_path)
            base_name = os.path.basename(card_path)
            name = os.path.splitext(base_name)[0]
            
            # Find key points
            centers = {}
            for box in result.boxes:
                x1, y1, x2, y2 = map(int, box.xyxy[0].cpu().numpy())
                cls = int(box.cls[0].cpu().item())
                point_name = self.model_corners.names[cls]
                cx = int((x1 + x2) / 2)
                cy = int((y1 + y2) / 2)
                centers[point_name] = (cx, cy)
            
            # Determine alignment points
            if 'quoc_huy' in centers and 'qr' in centers:
                pt_a, pt_b = centers['quoc_huy'], centers['qr']
            elif 'chip' in centers and 'm_red' in centers:
                pt_a, pt_b = centers['chip'], centers['m_red']
            else:
                logger.warning(f"Missing alignment points in {base_name}")
                continue
                
            # Calculate rotation
            dx, dy = pt_b[0] - pt_a[0], pt_b[1] - pt_a[1]
            angle = math.degrees(math.atan2(dy, dx))
            rotate_angle = -angle
            
            if abs(rotate_angle) < 10:
                logger.info(f"Skipping small rotation ({rotate_angle:.1f}°) for {base_name}")
                aligned.append(card_path)  # Use original
                continue
                
            # Rotate image
            h, w = img.shape[:2]
            center = (w // 2, h // 2)
            
            M = cv2.getRotationMatrix2D(center, rotate_angle, 1.0)
            cos = abs(M[0, 0])
            sin = abs(M[0, 1])
            new_w = int(h * sin + w * cos)
            new_h = int(h * cos + w * sin)
            
            M[0, 2] += (new_w / 2) - center[0]
            M[1, 2] += (new_h / 2) - center[1]
            
            # Check if we need 180° flip
            def transform_point(pt):
                x, y = pt
                new_x = M[0,0]*x + M[0,1]*y + M[0,2]
                new_y = M[1,0]*x + M[1,1]*y + M[1,2]
                return (new_x, new_y)
                
            pt_a_new = transform_point(pt_a)
            pt_b_new = transform_point(pt_b)
            
            if pt_b_new[0] < pt_a_new[0]:
                rotate_angle += 180
                M = cv2.getRotationMatrix2D(center, rotate_angle, 1.0)
                M[0, 2] += (new_w / 2) - center[0]
                M[1, 2] += (new_h / 2) - center[1]
            
            # Apply rotation
            rotated = cv2.warpAffine(
                img,
                M,
                (new_w, new_h),
                flags=cv2.INTER_LINEAR,
                borderMode=cv2.BORDER_CONSTANT,
                borderValue=(255, 255, 255)
            )
            
            out_path = os.path.join(results_dir, f"{name}.jpg")
            cv2.imwrite(out_path, rotated)
            
            aligned.append(out_path)
            self.callbacks.on_image(out_path)
            self._update_progress(progress_step)
            
        return aligned
    
    def detect_text(self, aligned_paths: List[str], output_path: str) -> Dict[str, Dict[str, str]]:
        """
        Detect and OCR text from aligned ID cards.
        
        Args:
            aligned_paths: Paths to aligned card images
            output_path: Path for results
            
        Returns:
            Dict mapping filenames to extracted field data
        """
        results_dir = os.path.join(output_path, "extracted_text")
        crops_dir = os.path.join(results_dir, "crops")
        os.makedirs(results_dir, exist_ok=True)
        os.makedirs(crops_dir, exist_ok=True)
        
        if not aligned_paths:
            return {}
            
        ocr_results = {}
        progress_step = 30 / len(aligned_paths)  # 30% for OCR
        
        for img_path in aligned_paths:
            results = self.model_lines.predict(source=img_path, conf=0.5, save=False)
            result = results[0]
            
            if result.masks is None or len(result.masks.xy) == 0:
                logger.warning(f"No text regions found in {img_path}")
                continue
                
            image = cv2.imread(img_path)
            base_name = os.path.splitext(os.path.basename(img_path))[0]
            fields = {}
            
            # Process each detected region
            for i, polygon in enumerate(result.masks.xy):
                points = polygon.astype(int)
                if points.shape[0] < 4:
                    continue
                    
                # Get field type
                cls_id = int(result.boxes.cls[i])
                field_name = self.model_lines.names[cls_id]
                
                # Extract region
                pts = points.reshape((-1, 1, 2))
                mask = np.zeros(image.shape[:2], dtype=np.uint8)
                cv2.fillPoly(mask, [pts], 255)
                
                masked = cv2.bitwise_and(image, image, mask=mask)
                x, y, w, h = cv2.boundingRect(pts)
                crop = masked[y:y+h, x:x+w]
                
                # Save crop for debugging
                crop_path = os.path.join(crops_dir, f"{base_name}-{field_name}.jpg")
                cv2.imwrite(crop_path, crop)
                
                # OCR
                pil_crop = Image.fromarray(cv2.cvtColor(crop, cv2.COLOR_BGR2RGB))
                text = self.ocr.predict(pil_crop).strip()
                
                # Clean up special cases
                if field_name == "noi_cap" and len(text) > 14 and text.isupper():
                    text = "CỤC TRƯỞNG CỤC CẢNH SÁT QUẢN LÝ HÀNH CHÍNH VỀ TRẬT TỰ XÃ HỘI"
                
                fields[field_name] = text
                self.callbacks.on_text(f"{field_name}: {text}")
            
            ocr_results[base_name] = fields
            self._update_progress(progress_step)
            
            # Save individual results
            result_path = os.path.join(results_dir, f"{base_name}.txt")
            with open(result_path, "w", encoding="utf-8") as f:
                for field, text in fields.items():
                    f.write(f"{field}: {text}\n")
        
        return ocr_results
    
    def save_results_xlsx(self, text_results: Dict[str, Dict[str, str]], output_path: str) -> str:
        """
        Save extracted text results to Excel file.
        
        Args:
            text_results: Dict of filename -> field data mappings
            output_path: Output directory
            
        Returns:
            Path to generated Excel file
        """
        if not text_results:
            return ""
            
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Thông tin CCCD"
        
        # Headers
        headers = [
            "Tên file", "Tên", "Giới tính", "Ngày sinh", "Số CCCD",
            "Ngày cấp", "Nơi cấp", "Ngày hết hạn", "Quê quán", 
            "Địa chỉ thường trú"
        ]
        ws.append(headers)
        
        # Data rows
        field_map = {
            "name": 1,
            "gioi_tinh": 2,
            "sn": 3,
            "id": 4,
            "ngay_cap": 5,
            "noi_cap": 6,
            "ngay_hh": 7,
            "que_quan": 8,
            "thuong_tru": 9
        }
        
        for filename, fields in text_results.items():
            row = [""] * len(headers)
            row[0] = filename
            
            for field, text in fields.items():
                if field in field_map:
                    row[field_map[field]] = text
                elif field == "thuong_tru2" and row[9]:
                    row[9] = f"{row[9]}, {text}".strip()
            
            ws.append(row)
        
        # Formatting
        from openpyxl.styles import Border, Side
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for column_cells in ws.columns:
            max_length = 0
            column = column_cells[0].column_letter
            
            for cell in column_cells:
                cell.border = thin_border
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            
            ws.column_dimensions[column].width = max_length + 2
        
        # Save
        excel_path = os.path.join(output_path, "CCCD_Results.xlsx")
        wb.save(excel_path)
        logger.info(f"Results saved to {excel_path}")
        
        self._update_progress(10)  # Final 10%
        return excel_path

def convert_pdf_to_images(
    pdf_path: str,
    output_dir: str,
    callbacks: Optional[ProcessCallbacks] = None
) -> List[str]:
    """
    Convert a PDF document to images.
    
    Args:
        pdf_path: Path to PDF file or directory containing PDFs
        output_dir: Directory to save image outputs
        callbacks: Optional callbacks for progress/status
        
    Returns:
        List of generated image paths
    """
    callbacks = callbacks or DefaultCallbacks()
    os.makedirs(output_dir, exist_ok=True)
    
    # Handle both single file and directory
    if os.path.isfile(pdf_path):
        pdf_files = [pdf_path]
    else:
        pdf_files = [
            os.path.join(pdf_path, f)
            for f in os.listdir(pdf_path)
            if f.lower().endswith('.pdf')
        ]
    
    if not pdf_files:
        logger.warning("No PDF files found")
        return []
    
    generated = []
    progress_step = 100 / len(pdf_files)
    progress = 0
    
    for pdf_file in pdf_files:
        try:
            callbacks.on_step(f"Processing {os.path.basename(pdf_file)}")
            doc = fitz.open(pdf_file)
            
            for i, page in enumerate(doc):
                # Get page image
                pix = page.get_pixmap()
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                
                # Determine output name - pair numbering for ID cards
                pair_num = (i // 2) + 1
                side = "mt" if i % 2 == 0 else "ms"  # Front/back designation
                
                out_path = os.path.join(output_dir, f"{pair_num}{side}.png")
                img.save(out_path, "PNG")
                generated.append(out_path)
                
                callbacks.on_image(out_path)
            
            progress += progress_step
            callbacks.on_progress(int(progress))
            
        except Exception as e:
            logger.error(f"Error processing {pdf_file}: {e}")
            continue
            
    return generated

def process_excel_urls(
    excel_path: str,
    output_dir: str,
    callbacks: Optional[ProcessCallbacks] = None
) -> List[str]:
    """
    Process an Excel file containing Google Drive image URLs.
    
    Args:
        excel_path: Path to Excel file with image URLs
        output_dir: Directory to save downloaded images
        callbacks: Optional callbacks for progress/status
        
    Returns:
        List of downloaded image paths
    """
    callbacks = callbacks or DefaultCallbacks()
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        return []
    
    if len(df.columns) < 3:
        logger.error("Excel file must have at least 3 columns")
        return []
    
    # Skip header row
    lines = df.iloc[1:, :3].values.tolist()
    if not lines:
        logger.warning("No data found in Excel file")
        return []
    
    downloaded = []
    progress_step = 100 / len(lines)
    progress = 0
    
    for row in lines:
        if not any(row):  # Skip empty rows
            continue
            
        file_name, mt_url, ms_url = row
        callbacks.on_step(f"Processing {file_name}")
        
        # Handle front image
        try:
            mt_path = os.path.join(output_dir, f"{file_name}mt.png")
            download_drive_file(mt_url, mt_path)
            downloaded.append(mt_path)
            callbacks.on_image(mt_path)
        except Exception as e:
            logger.error(f"Failed to download front image for {file_name}: {e}")
        
        # Handle back image
        try:
            ms_path = os.path.join(output_dir, f"{file_name}ms.png")
            download_drive_file(ms_url, ms_path)
            downloaded.append(ms_path)
            callbacks.on_image(ms_path)
        except Exception as e:
            logger.error(f"Failed to download back image for {file_name}: {e}")
        
        progress += progress_step
        callbacks.on_progress(int(progress))
    
    return downloaded

def download_drive_file(url: str, output_path: str) -> None:
    """
    Download a file from Google Drive.
    
    Args:
        url: Google Drive file URL
        output_path: Local path to save file
    
    Raises:
        ValueError: If URL is invalid
        requests.RequestException: If download fails
    """
    # Extract file ID
    file_id = None
    if 'drive.google.com' in url:
        if '/file/d/' in url:
            file_id = url.split('/file/d/')[1].split('/')[0]
        elif 'id=' in url:
            file_id = url.split('id=')[1].split('&')[0]
        elif '/open?id=' in url:
            file_id = url.split('/open?id=')[1].split('&')[0]
    
    if not file_id:
        raise ValueError(f"Invalid Google Drive URL: {url}")
    
    # Create download URL
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    
    # Start session for large file handling
    session = requests.Session()
    response = session.get(download_url, stream=True)
    
    # Handle confirmation token for large files
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            params = {'id': file_id, 'confirm': value}
            response = session.get(download_url, params=params, stream=True)
            break
    
    # Download with chunking
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

# Public API

def initialize_system(
    model_dir: str = "./__pycache__",
    callbacks: Optional[ProcessCallbacks] = None
) -> Tuple[bool, str]:
    """
    Initialize the ID card processing system.
    
    Args:
        model_dir: Directory containing models and weights
        callbacks: Optional callbacks for progress/status
        
    Returns:
        Tuple of (success, message)
    """
    try:
        # Validate license
        license_mgr = LicenseManager()
        if not license_mgr.validate():
            return False, "License validation failed"
            
        # Initialize detection engine
        detector = IDCardDetector(model_dir, callbacks)
        
        return True, "System initialized successfully"
        
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return False, str(e)

def process_id_cards(
    input_path: str,
    output_path: str,
    callbacks: Optional[ProcessCallbacks] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Process ID cards from images.
    
    Args:
        input_path: Directory with input images
        output_path: Directory for results
        callbacks: Optional callbacks for progress/status
        
    Returns:
        Tuple of (success, results_dict) where results_dict contains:
        - excel_path: Path to results Excel file
        - processed_count: Number of cards processed
        - text_results: Dictionary of extracted text by filename
    """
    try:
        detector = IDCardDetector(callbacks=callbacks)
        
        # Verify license and record usage
        license_mgr = LicenseManager()
        if not license_mgr.validate():
            return False, {"error": "License validation failed"}
            
        # Count files for license tracking
        input_files = len([
            f for f in os.listdir(input_path)
            if f.lower().endswith(('.jpg', '.png', '.jpeg'))
        ])
        
        success, remaining = license_mgr.count_usage(input_files)
        if not success:
            return False, {"error": "Usage limit exceeded"}
            
        # Process images
        detected = detector.detect_cards(input_path, output_path)
        if not detected:
            return False, {"error": "No valid cards detected"}
            
        aligned = detector.detect_corners_and_align(detected, output_path)
        text_results = detector.detect_text(aligned, output_path)
        excel_path = detector.save_results_xlsx(text_results, output_path)
        
        return True, {
            "excel_path": excel_path,
            "processed_count": len(text_results),
            "text_results": text_results,
            "remaining_uses": remaining
        }
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return False, {"error": str(e)}

def process_pdf_batch(
    pdf_path: str,
    output_path: str,
    callbacks: Optional[ProcessCallbacks] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Convert PDF documents to images and process ID cards.
    
    Args:
        pdf_path: PDF file or directory of PDFs
        output_path: Directory for results
        callbacks: Optional callbacks for progress/status
        
    Returns:
        Tuple of (success, results_dict)
    """
    try:
        # Convert PDFs to images
        image_paths = convert_pdf_to_images(pdf_path, output_path, callbacks)
        if not image_paths:
            return False, {"error": "PDF conversion failed"}
            
        # Process converted images
        return process_id_cards(output_path, output_path, callbacks)
        
    except Exception as e:
        logger.error(f"PDF batch processing failed: {e}")
        return False, {"error": str(e)}

def process_excel_batch(
    excel_path: str,
    output_path: str,
    callbacks: Optional[ProcessCallbacks] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Process ID cards from Google Drive URLs in Excel.
    
    Args:
        excel_path: Excel file with image URLs
        output_path: Directory for results
        callbacks: Optional callbacks for progress/status
        
    Returns:
        Tuple of (success, results_dict)
    """
    try:
        # Download images from URLs
        image_paths = process_excel_urls(excel_path, output_path, callbacks)
        if not image_paths:
            return False, {"error": "Failed to download images"}
            
        # Process downloaded images
        return process_id_cards(output_path, output_path, callbacks)
        
    except Exception as e:
        logger.error(f"Excel batch processing failed: {e}")
        return False, {"error": str(e)}

def get_system_info() -> Dict[str, Any]:
    """
    Get system information and license status.
    
    Returns:
        Dict with system information:
        - version: Software version
        - machine_id: Hardware identifier
        - license_status: License validation status
        - remaining_uses: Number of remaining uses
    """
    try:
        license_mgr = LicenseManager()
        is_valid = license_mgr.validate()
        
        return {
            "version": VERSION,
            "machine_id": get_machine_id()['serial_number'],
            "license_status": "valid" if is_valid else "invalid",
            "license_expiry": license_mgr.expiry_date if is_valid else None,
            "remaining_uses": license_mgr.max_usages if is_valid else 0
        }
    except Exception as e:
        logger.error(f"Failed to get system info: {e}")
        return {
            "version": VERSION,
            "error": str(e)
        }

class LicenseManager:
    """Handles license validation and usage tracking."""
    
    VIP_URL = "https://severtmoclan.click/SERVER2/QR/vip.txt"
    COUNTER_URL = "https://severtmoclan.click/SERVER2/QR/counter.php"
    
    def __init__(self):
        self.machine_id = get_machine_id()['serial_number']
        self.key = self._generate_key()
        self._license_data = None
        self.max_usages = 0
        self.expiry_date = None
    
    def _generate_key(self) -> str:
        """Generate authentication key from machine ID."""
        data = str(self.machine_id)
        hash_object = hashlib.sha256(data.encode())
        key = 'key' + hash_object.hexdigest()
        return key[:-35]  # Match original truncation
    
    def validate(self) -> bool:
        """
        Validate license against remote server.
        Returns True if license is valid, False otherwise.
        """
        try:
            response = requests.get(self.VIP_URL)
            if not response.ok:
                logger.error("Failed to fetch license data")
                return False
            
            lines = response.text.split('\n')
            
            # Check for updates
            if lines[0].startswith("update|"):
                parts = lines[0].split("|")
                if len(parts) >= 3 and parts[2] != VERSION:
                    logger.warning(f"New version {parts[2]} available")
                
            # Find matching license
            for line in lines[1:]:  # Skip update line
                parts = line.split('|')
                if len(parts) < 7:
                    continue
                    
                if parts[0] == self.key:
                    self._license_data = {
                        'key': parts[0],
                        'expiry': parts[2],
                        'limit': parts[6]
                    }
                    self.max_usages = int(parts[6])
                    self.expiry_date = parts[2]
                    
                    # Check expiration
                    if not self._check_expiry():
                        logger.error("License expired")
                        return False
                        
                    logger.info(f"License validated. Expires: {self.expiry_date}, Max uses: {self.max_usages}")
                    return True
            
            logger.error("No matching license found")
            return False
            
        except Exception as e:
            logger.error(f"License validation error: {e}")
            return False
    
    def _check_expiry(self) -> bool:
        """Check if license has expired."""
        if not self.expiry_date:
            return False
        try:
            expiry = datetime.strptime(self.expiry_date, "%d/%m/%Y")
            return datetime.now() <= expiry
        except ValueError:
            logger.error(f"Invalid expiry date format: {self.expiry_date}")
            return False
    
    def count_usage(self, count: int) -> Tuple[bool, Optional[int]]:
        """
        Track usage count against server.
        Returns (success, remaining_uses) tuple.
        """
        if not self._license_data:
            return False, None
            
        try:
            response = requests.get(
                self.COUNTER_URL,
                params={
                    "num": count,
                    "key": self.key,
                    "max": self.max_usages
                }
            )
            
            result = response.text.strip()
            
            if "LIMIT REACHED" in result:
                logger.warning("Usage limit reached")
                return False, 0
                
            try:
                remaining = int(result.split("Remaining:")[1])
                return True, remaining
            except (IndexError, ValueError):
                logger.error(f"Invalid counter response: {result}")
                return False, None
                
        except Exception as e:
            logger.error(f"Usage counting error: {e}")
            return False, None

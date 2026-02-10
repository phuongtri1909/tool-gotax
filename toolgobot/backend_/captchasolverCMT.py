import requests
import base64
import time
import os
import cv2
import numpy as np

# TF 2.18 tren Windows co the khong co tensorflow.keras -> fallback tf_keras
try:
    from tensorflow.keras.models import load_model
except ModuleNotFoundError:
    from tf_keras.models import load_model

# ================= CONFIG ================= #
IMG_W, IMG_H = 120, 40
MAX_LEN = 5
CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
CAPTCHA_IMAGE_URL = "https://canhantmdt.gdt.gov.vn/ICanhan/servlet/ImageServlet"

# ================= LOAD MODEL ================= #
_gobot_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_captcha_model_path = os.path.join(_gobot_root, "captcha_model.keras")
try:
    model = load_model(_captcha_model_path)
except Exception as e:
    print("Error loading model: %s" % e)
    model = None

# ================= HELPER ================= #
def preprocess_image_from_bytes(img_bytes):
    """Chuyển đổi bytes image thành input cho model"""
    img_array = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(img_array, cv2.IMREAD_GRAYSCALE)
    img = cv2.resize(img, (IMG_W, IMG_H))
    img = img / 255.0
    return img.reshape(1, IMG_H, IMG_W, 1)

def decode_prediction(preds):
    """Decode model predictions thành text"""
    result = ""
    for p in preds:
        idx = np.argmax(p, axis=1)[0]
        result += CHARS[idx]
    return result

def download_captcha(url, save_folder="img"):
    """Download captcha image, save to save_folder and return (bytes, filepath)."""
    os.makedirs(save_folder, exist_ok=True)
    r = requests.get(url)
    r.raise_for_status()
    filename = f"captcha_{int(time.time())}.jpg"
    filepath = os.path.join(save_folder, filename)
    with open(filepath, "wb") as f:
        f.write(r.content)
    return r.content, filepath


class CaptchaSolver:
    def solve_captcha(self, img_bytes):
        """Giải captcha bằng model thay vì API"""
        if model is None:
            return {
                "status": "error",
                "message": "Model not loaded"
            }
        
        try:
            # Preprocess image
            img = preprocess_image_from_bytes(img_bytes)
            
            # Predict
            preds = model.predict(img, verbose=0)
            captcha_text = decode_prediction(preds)
            return {
                "status": "success",
                "text": captcha_text
            }
        except Exception as e:
            print(f"Error solving captcha: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
from captcha_solver import CaptchaSolver
import numpy as np
import cv2, requests

def test_captcha_solver():
    solver = CaptchaSolver(model_path="model.onnx")
    res = requests.get("https://tracuunnt.gdt.gov.vn/tcnnt/captcha.png").content
    
    # Lưu ảnh captcha ra file
    with open("captcha_test.png", "wb") as f:
        f.write(res)
    
    image_bytes = res
    result = solver.solve(image_bytes)
    print("Kết quả giải mã captcha:", result)
    
test_captcha_solver()
# 📝 Template: Thêm Tool Mới

Khi tạo tool mới (ví dụ: `tool-go-bot`), làm theo các bước sau:

## Bước 1: Tạo cấu trúc folder

```bash
mkdir -p tool-go-bot/api
```

## Bước 2: Tạo `tool-go-bot/api/routes.py`

Copy template từ `tool-go-quick/api/routes.py` và sửa:

```python
"""
Routes cho tool-go-bot
Được gọi từ api_server.py chung
"""
import os
import sys
import base64
from flask import request, jsonify

# Thêm parent directory vào path để import main
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import main module của tool này
from main import YourExtractorClass  # Sửa tên class

# Cấu hình
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

def decode_input_data(inp_path):
    """Decode input data từ base64 hoặc bytes"""
    if isinstance(inp_path, str):
        try:
            return base64.b64decode(inp_path)
        except:
            if os.path.exists(inp_path):
                with open(inp_path, 'rb') as f:
                    return f.read()
            else:
                raise ValueError("Invalid input: not base64 and path not found")
    elif isinstance(inp_path, (bytes, bytearray)):
        return bytes(inp_path)
    else:
        raise ValueError("Invalid input type. Expected string (base64) or bytes")

def register_routes(app, prefix):
    """
    Đăng ký routes cho tool này
    
    Args:
        app: Flask app instance
        prefix: URL prefix (ví dụ: '/api/go-bot')
    """
    
    @app.route(f'{prefix}/health', methods=['GET'])
    def health_check():
        """Health check cho tool này"""
        return jsonify({
            "status": "success",
            "message": "Go Bot API is running",
            "version": "1.0"
        })
    
    @app.route(f'{prefix}/process', methods=['POST'])
    def process():
        """Xử lý chính"""
        try:
            inp_data = None
            
            # Cách 1: Upload file
            if 'file' in request.files:
                file = request.files['file']
                if file.filename == '':
                    return jsonify({
                        "status": "error",
                        "message": "No file selected"
                    }), 400
                
                file_bytes = file.read()
                if len(file_bytes) > MAX_FILE_SIZE:
                    return jsonify({
                        "status": "error",
                        "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB"
                    }), 400
                
                inp_data = file_bytes
            
            # Cách 2: JSON với base64 hoặc bytes
            elif request.is_json:
                data = request.get_json()
                inp_path = data.get("inp_path") if data else None
                
                if not inp_path:
                    return jsonify({
                        "status": "error",
                        "message": "Missing inp_path"
                    }), 400
                
                inp_data = decode_input_data(inp_path)
            else:
                return jsonify({
                    "status": "error",
                    "message": "No file or data provided"
                }), 400
            
            # Xử lý
            extractor = YourExtractorClass()
            results = extractor.handle_task({"inp_path": inp_data})
            
            return jsonify(results)
            
        except Exception as e:
            import traceback
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    # Thêm các routes khác tùy theo tool...
```

## Bước 3: Tạo `tool-go-bot/requirements.txt`

**Lưu ý:** Đặt ở thư mục gốc của tool, KHÔNG phải trong `api/`

```txt
# Dependencies riêng của tool này (core logic)
# Ví dụ:
# numpy==2.0.2
# opencv-python==4.12.0.88
# ultralytics==8.1.37
```

**Không cần:** flask, flask-cors, gunicorn (đã có trong `requirements.txt` root)

## Bước 4: Đăng ký trong `api_server.py`

Thêm vào dict `TOOLS`:

```python
TOOLS = {
    'go-quick': {
        'path': 'tool-go-quick',
        'module': 'tool_go_quick',
        'name': 'ID Quick API'
    },
    'go-bot': {  # ← Thêm tool mới
        'path': 'tool-go-bot',
        'module': 'tool_go_bot',
        'name': 'Go Bot API'
    },
}
```

## Bước 5: Tạo Laravel Controller (tùy chọn)

```bash
mkdir -p tool-go-bot/laravel
```

Tạo `tool-go-bot/laravel/GoBotController.php` (copy từ `IDQuickController.php` và sửa)

## Bước 6: Test

```bash
# Restart API server
python api_server.py

# Test health check
curl http://localhost:5000/api/go-bot/health
```

## Checklist

- [ ] Tạo folder `tool-xxx/api/`
- [ ] Tạo `tool-xxx/api/routes.py` với function `register_routes`
- [ ] Tạo `tool-xxx/requirements.txt` (dependencies cho core logic)
- [ ] Đăng ký tool trong `api_server.py` → `TOOLS` dict
- [ ] Cài dependencies: `pip install -r tool-xxx/requirements.txt`
- [ ] Test: `curl http://localhost:5000/api/xxx/health`
- [ ] Tạo Laravel Controller (nếu cần)


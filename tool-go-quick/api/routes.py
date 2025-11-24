"""
Routes cho tool-go-quick
Được gọi từ api_server.py chung
"""
import os
import sys
import base64
from flask import request, jsonify, Response

# Thêm parent directory vào path để import main
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Lazy import - chỉ import khi cần dùng
CCCDExtractor = None
def get_cccd_extractor():
    global CCCDExtractor
    if CCCDExtractor is None:
        from main import CCCDExtractor as _CCCDExtractor
        CCCDExtractor = _CCCDExtractor
    return CCCDExtractor

# Cấu hình
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png', 'pdf', 'xlsx', 'xls', 'zip'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
        prefix: URL prefix (ví dụ: '/api/go-quick')
    """
    
    @app.route(f'{prefix}/health', methods=['GET'])
    def go_quick_health_check():
        """Health check cho tool này"""
        return jsonify({
            "status": "success",
            "message": "ID Quick API is running",
            "version": "1.0"
        })
    
    @app.route(f'{prefix}/process-cccd', methods=['POST'])
    def go_quick_process_cccd():
        """Xử lý CCCD Extractor"""
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
            CCCDExtractorClass = get_cccd_extractor()
            extractor = CCCDExtractorClass()
            task = {
                "func_type": 1,
                "inp_path": inp_data
            }
            
            results = extractor.handle_task(task)
            return jsonify(results)
            
        except Exception as e:
            import traceback
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    @app.route(f'{prefix}/process-pdf', methods=['POST'])
    def go_quick_process_pdf():
        """Xử lý PDF to PNG"""
        try:
            inp_data = None
            
            if 'file' in request.files:
                file = request.files['file']
                if file.filename == '':
                    return jsonify({
                        "status": "error",
                        "message": "No file selected"
                    }), 400
                
                if not file.filename.lower().endswith('.pdf'):
                    return jsonify({
                        "status": "error",
                        "message": "File phải là PDF (.pdf)"
                    }), 400
                
                file_bytes = file.read()
                if len(file_bytes) > MAX_FILE_SIZE:
                    return jsonify({
                        "status": "error",
                        "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB"
                    }), 400
                
                inp_data = file_bytes
            
            elif request.is_json:
                data = request.get_json()
                inp_path = data.get("inp_path")
                
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
            
            CCCDExtractorClass = get_cccd_extractor()
            extractor = CCCDExtractorClass()
            task = {
                "func_type": 2,
                "inp_path": inp_data
            }
            
            results = extractor.handle_task(task)
            return jsonify(results)
            
        except Exception as e:
            import traceback
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    @app.route(f'{prefix}/process-excel', methods=['POST'])
    def go_quick_process_excel():
        """Xử lý Excel to PNG"""
        try:
            inp_data = None
            
            if 'file' in request.files:
                file = request.files['file']
                if file.filename == '':
                    return jsonify({
                        "status": "error",
                        "message": "No file selected"
                    }), 400
                
                if not allowed_file(file.filename):
                    return jsonify({
                        "status": "error",
                        "message": "File type not allowed. Expected .xlsx or .xls"
                    }), 400
                
                file_bytes = file.read()
                if len(file_bytes) > MAX_FILE_SIZE:
                    return jsonify({
                        "status": "error",
                        "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB"
                    }), 400
                
                inp_data = file_bytes
            
            elif request.is_json:
                data = request.get_json()
                inp_path = data.get("inp_path")
                
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
            
            CCCDExtractorClass = get_cccd_extractor()
            extractor = CCCDExtractorClass()
            task = {
                "func_type": 3,
                "inp_path": inp_data
            }
            
            results = extractor.handle_task(task)
            return jsonify(results)
            
        except Exception as e:
            import traceback
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500


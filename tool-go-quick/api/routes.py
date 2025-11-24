"""
Routes cho tool-go-quick
Được gọi từ api_server.py chung
"""
import os
import sys
import base64
from flask import request, jsonify, Response
import threading

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

_model_cache = {
    'yolo_model1': None,
    'yolo_model2': None,
    'yolo_model3': None,
    'vietocr_detector': None,
    'base_dir': None,
    'lock': threading.Lock()
}

def get_model_cache():
    """Lấy hoặc khởi tạo model cache"""
    global _model_cache
    
    if _model_cache['yolo_model1'] is not None:
        return _model_cache
    
    with _model_cache['lock']:
        if _model_cache['yolo_model1'] is not None:
            return _model_cache
        
        print("🔄 Đang load models lần đầu (sẽ cache để tái sử dụng)...")
        
        import main
        main_file_dir = os.path.dirname(os.path.abspath(main.__file__))
        base_dir = os.path.join(main_file_dir, "__pycache__")
        _model_cache['base_dir'] = base_dir
        
        try:
            from ultralytics import YOLO
            print("  ⏳ Loading YOLO model1 (best.pt)...")
            _model_cache['yolo_model1'] = YOLO(os.path.join(base_dir, "best.pt"))
            print("  ✅ YOLO model1 loaded")
            
            print("  ⏳ Loading YOLO model2 (best2.pt)...")
            _model_cache['yolo_model2'] = YOLO(os.path.join(base_dir, "best2.pt"))
            print("  ✅ YOLO model2 loaded")
            
            print("  ⏳ Loading YOLO model3 (best3.pt)...")
            _model_cache['yolo_model3'] = YOLO(os.path.join(base_dir, "best3.pt"))
            print("  ✅ YOLO model3 loaded")
        except Exception as e:
            print(f"  ❌ Lỗi load YOLO models: {e}")
        
        _model_cache['vietocr_detector'] = None
        
        print("✅ Models đã được cache, sẵn sàng xử lý requests!")
    
    return _model_cache

def get_vietocr_detector():
    """Lazy load VietOCR detector - sẽ được load khi cần trong detect_lines()"""
    global _model_cache
    
    if _model_cache['vietocr_detector'] is not None:
        return _model_cache['vietocr_detector']
    
    with _model_cache['lock']:
        if _model_cache['vietocr_detector'] is not None:
            return _model_cache['vietocr_detector']
        
        print("  ⏳ Loading VietOCR detector...")
        try:
            from vietocr.tool.predictor import Predictor
            from vietocr.tool.config import Cfg
            
            config = Cfg.load_config_from_name('vgg_transformer')
            config['weights'] = os.path.join(_model_cache['base_dir'], 'vgg_transformer.pth')
            config['cnn']['pretrained'] = False
            config['device'] = 'cpu'
            _model_cache['vietocr_detector'] = Predictor(config)
            print("  ✅ VietOCR detector loaded")
        except Exception as e:
            print(f"  ❌ Lỗi load VietOCR: {e}")
            raise
    
    return _model_cache['vietocr_detector']

def ensure_vietocr_loaded():
    """Đảm bảo VietOCR đã được load"""
    get_vietocr_detector()

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
            
            model_cache = get_model_cache()
            CCCDExtractorClass = get_cccd_extractor()
            extractor = CCCDExtractorClass(cached_models=model_cache)
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
            
            model_cache = get_model_cache()
            CCCDExtractorClass = get_cccd_extractor()
            extractor = CCCDExtractorClass(cached_models=model_cache)
            
            task = {
                "func_type": 2,
                "inp_path": inp_data
            }
            results = extractor.handle_task(task)
            
            if results.get("status") == "success" and results.get("zip_base64"):
                zip_base64 = results.get("zip_base64")
                zip_bytes = base64.b64decode(zip_base64)
                task2 = {
                    "func_type": 1,
                    "inp_path": zip_bytes
                }
                results = extractor.handle_task(task2)
            
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
            
            model_cache = get_model_cache()
            CCCDExtractorClass = get_cccd_extractor()
            extractor = CCCDExtractorClass(cached_models=model_cache)
            
            task = {
                "func_type": 3,
                "inp_path": inp_data
            }
            results = extractor.handle_task(task)
            
            if results.get("status") == "success" and results.get("zip_base64"):
                zip_base64 = results.get("zip_base64")
                zip_bytes = base64.b64decode(zip_base64)
                task2 = {
                    "func_type": 1,
                    "inp_path": zip_bytes
                }
                results = extractor.handle_task(task2)
            
            return jsonify(results)
            
        except Exception as e:
            import traceback
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500


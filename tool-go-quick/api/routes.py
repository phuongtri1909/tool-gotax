"""
Routes cho tool-go-quick
Được gọi từ api_server.py chung
"""
import os
import sys
import base64
import threading
import json
import asyncio
import importlib.util

# Thư mục gốc của tool-go-quick (không phụ thuộc sys.path để tránh nhầm với tool-go-invoice)
_GO_QUICK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Lazy import - chỉ import khi cần dùng
CCCDExtractor = None
CCCDExtractorStreaming = None

def _load_go_quick_main():
    """Load module main từ tool-go-quick (tránh import nhầm main của tool-go-invoice)."""
    main_path = os.path.join(_GO_QUICK_DIR, "main.py")
    spec = importlib.util.spec_from_file_location("go_quick_main", main_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def get_cccd_extractor():
    global CCCDExtractor, CCCDExtractorStreaming
    if CCCDExtractor is None:
        go_quick_main = _load_go_quick_main()
        CCCDExtractor = go_quick_main.CCCDExtractor
        CCCDExtractorStreaming = go_quick_main.CCCDExtractorStreaming
    return CCCDExtractor

def get_cccd_extractor_streaming():
    global CCCDExtractor, CCCDExtractorStreaming
    if CCCDExtractorStreaming is None:
        go_quick_main = _load_go_quick_main()
        CCCDExtractor = go_quick_main.CCCDExtractor
        CCCDExtractorStreaming = go_quick_main.CCCDExtractorStreaming
    return CCCDExtractorStreaming

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
    
    import logging
    logger = logging.getLogger(__name__)
    
    if _model_cache['yolo_model1'] is not None:
        logger.debug("Model cache đã có sẵn, trả về ngay")
        return _model_cache
    
    logger.info("Model cache chưa có, đang vào lock để load...")
    with _model_cache['lock']:
        # Double-check sau khi vào lock
        if _model_cache['yolo_model1'] is not None:
            logger.info("Model cache đã được load bởi thread khác, trả về")
            return _model_cache
        
        logger.info("🔄 Đang load models lần đầu (sẽ cache để tái sử dụng)...")
        
        # Dùng thư mục tool-go-quick (tránh nhầm với tool-go-invoice khi api_server thêm nhiều path)
        base_dir = os.path.join(_GO_QUICK_DIR, "__pycache__")
        _model_cache['base_dir'] = base_dir
        
        try:
            from ultralytics import YOLO
            logger.info("  ⏳ Loading YOLO model1 (best.pt)...")
            _model_cache['yolo_model1'] = YOLO(os.path.join(base_dir, "best.pt"))
            logger.info("  ✅ YOLO model1 loaded")
            
            logger.info("  ⏳ Loading YOLO model2 (best2.pt)...")
            _model_cache['yolo_model2'] = YOLO(os.path.join(base_dir, "best2.pt"))
            logger.info("  ✅ YOLO model2 loaded")
            
            logger.info("  ⏳ Loading YOLO model3 (best3.pt)...")
            _model_cache['yolo_model3'] = YOLO(os.path.join(base_dir, "best3.pt"))
            logger.info("  ✅ YOLO model3 loaded")
        except Exception as e:
            logger.error(f"  ❌ Lỗi load YOLO models: {e}", exc_info=True)
        
        _model_cache['vietocr_detector'] = None
        
        logger.info("✅ Models đã được cache, sẵn sàng xử lý requests!")
    
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
        app: Quart app instance (async) hoặc Flask app (sync)
        prefix: URL prefix (ví dụ: '/api/go-quick')
    """
    
    # Helper to check if app is Quart (async) or Flask (sync)
    is_async = hasattr(app, 'ensure_async')
    
    if is_async:
        from quart import request, jsonify
    else:
        from flask import request, jsonify
    
    # Helper functions để xử lý request
    async def get_request_json():
        """Get JSON from request, async-safe"""
        if is_async:
            return await request.get_json()
        else:
            return request.get_json()
    
    async def get_request_file(filename='file'):
        """Get file from request, async-safe"""
        try:
            if is_async:
                files = await request.files
                if files:
                    if hasattr(files, 'get'):
                        return files.get(filename)
                    elif hasattr(files, '__getitem__'):
                        try:
                            return files[filename]
                        except KeyError:
                            return None
                    else:
                        for key, value in files.items():
                            if key == filename:
                                return value
                return None
            else:
                if hasattr(request, 'files') and request.files:
                    return request.files.get(filename)
                return None
        except Exception as e:
            print(f"Error getting file: {e}")
            return None
    
    async def read_file_bytes(file):
        """Read file bytes, async-safe"""
        if not file:
            return None
        try:
            if is_async:
                if hasattr(file, 'read'):
                    result = file.read()
                    if hasattr(result, '__await__'):
                        return await result
                    return result
                elif hasattr(file, 'stream'):
                    stream = file.stream
                    if hasattr(stream, 'read'):
                        result = stream.read()
                        if hasattr(result, '__await__'):
                            return await result
                        return result
                return None
            else:
                if hasattr(file, 'read'):
                    return file.read()
                return None
        except Exception as e:
            print(f"Error reading file bytes: {e}")
            return None
    
    # ==================== ROUTES ====================
    
    @app.route(f'{prefix}/health', methods=['GET'])
    async def go_quick_health_check():
        """Health check cho tool này"""
        return jsonify({
            "status": "success",
            "message": "ID Quick API is running",
            "version": "1.0"
        })
    
    @app.route(f'{prefix}/read-quick', methods=['POST'])
    async def go_quick_read_quick():
        """API đọc nhanh 1 CCCD (2 ảnh: mt và ms) - không cần tạo job"""
        try:
            # Get files from request
            if is_async:
                files = await request.files
            else:
                files = request.files if hasattr(request, 'files') else {}
            
            mt_file = None
            ms_file = None
            
            # Try to get mt and ms files
            if files:
                if hasattr(files, 'get'):
                    mt_file = files.get('mt')
                    ms_file = files.get('ms')
                elif hasattr(files, '__getitem__'):
                    try:
                        mt_file = files['mt']
                        ms_file = files['ms']
                    except KeyError:
                        pass
                else:
                    for key, value in files.items():
                        if key == 'mt':
                            mt_file = value
                        elif key == 'ms':
                            ms_file = value
            
            if not mt_file or not ms_file:
                return jsonify({
                    "status": "error",
                    "message": "Vui lòng cung cấp cả ảnh mặt trước (mt) và mặt sau (ms)"
                }), 400
            
            # Read file bytes
            mt_bytes = await read_file_bytes(mt_file)
            ms_bytes = await read_file_bytes(ms_file)
            
            if not mt_bytes or not ms_bytes:
                return jsonify({
                    "status": "error",
                    "message": "Không thể đọc file ảnh"
                }), 400
            
            # Check file size
            if len(mt_bytes) > MAX_FILE_SIZE or len(ms_bytes) > MAX_FILE_SIZE:
                return jsonify({
                    "status": "error",
                    "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB mỗi file"
                }), 400
            
            # Create ZIP in memory
            import zipfile
            import io
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Get file extensions
                mt_filename = getattr(mt_file, 'filename', 'mt.jpg')
                ms_filename = getattr(ms_file, 'filename', 'ms.jpg')
                
                # Ensure proper extensions
                if not mt_filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                    mt_filename = 'mt.jpg'
                if not ms_filename.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                    ms_filename = 'ms.jpg'
                
                zip_file.writestr(mt_filename, mt_bytes)
                zip_file.writestr(ms_filename, ms_bytes)
            
            zip_bytes = zip_buffer.getvalue()
            
            # Load models and process
            try:
                model_cache = get_model_cache()
                CCCDExtractorClass = get_cccd_extractor()
                extractor = CCCDExtractorClass(cached_models=model_cache)
                
                # Create task
                task = {
                    "func_type": 1,  # CCCD processing
                    "inp_path": zip_bytes,
                }
                
                # Process
                result = extractor.handle_task(task)
                
                return jsonify({
                    "status": "success",
                    "data": result
                })
                
            except Exception as e:
                import traceback
                error_trace = traceback.format_exc()
                print(f"Error processing quick read: {e}")
                print(error_trace)
                return jsonify({
                    "status": "error",
                    "message": f"Lỗi xử lý: {str(e)}"
                }), 500
                
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            print(f"Error in read-quick endpoint: {e}")
            print(error_trace)
            return jsonify({
                "status": "error",
                "message": f"Lỗi: {str(e)}"
            }), 500
    
    @app.route(f'{prefix}/process-cccd', methods=['POST'])
    async def go_quick_process_cccd():
        """Xử lý CCCD Extractor - Worker gọi để xử lý batch jobs từ queue"""
        try:
            inp_data = None
            
            content_type = request.headers.get('Content-Type', '')
            
            # Cách 1: Upload file
            try:
                if is_async:
                    if 'multipart/form-data' in content_type:
                        files = await request.files
                        file = None
                        if files:
                            if hasattr(files, 'get'):
                                file = files.get('file')
                            elif hasattr(files, '__getitem__'):
                                try:
                                    file = files['file']
                                except KeyError:
                                    pass
                            else:
                                for key, value in files.items():
                                    if key == 'file':
                                        file = value
                                        break
                    else:
                        file = None
                else:
                    file = request.files.get('file') if hasattr(request, 'files') and request.files else None
                
                if file:
                    filename = getattr(file, 'filename', None) or getattr(file, 'name', None) or ''
                    if filename and filename != '':
                        file_bytes = await read_file_bytes(file)
                        if file_bytes:
                            if len(file_bytes) > MAX_FILE_SIZE:
                                return jsonify({
                                    "status": "error",
                                    "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB"
                                }), 400
                            inp_data = file_bytes
            except Exception as e:
                pass  # Continue to try JSON method
            
            # Cách 2: JSON với base64 hoặc bytes
            if not inp_data:
                try:
                    if 'application/json' in content_type or (is_async and hasattr(request, 'is_json') and request.is_json):
                        data = await get_request_json()
                        if data:
                            inp_path = data.get("inp_path")
                            if inp_path:
                                inp_data = decode_input_data(inp_path)
                except Exception as e:
                    pass
            
            if not inp_data:
                return jsonify({
                    "status": "error",
                    "message": "No file or data provided"
                }), 400
            
            # Load models and process
            try:
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
                traceback.print_exc()
                raise
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc() if app.config.get('DEBUG', False) else None
            print(f"Error in process-cccd: {e}")
            if error_detail:
                print(error_detail)
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": error_detail
            }), 500

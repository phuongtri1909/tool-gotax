"""
Routes cho tool-go-quick
Được gọi từ api_server.py chung
"""
import os
import sys
import base64
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
        app: Quart app instance (async) hoặc Flask app (sync)
        prefix: URL prefix (ví dụ: '/api/go-quick')
    """
    
    # Helper to check if app is Quart (async) or Flask (sync)
    is_async = hasattr(app, 'ensure_async')
    
    if is_async:
        from quart import request, jsonify, Response
    else:
        from flask import request, jsonify, Response
    
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
                # IMPORTANT: In Quart, request.files is a COROUTINE, need to await!
                files = await request.files
                print(f"DEBUG: request.files (after await) type: {type(files)}, value: {files}")
                
                if files:
                    # Try different ways to access the file
                    if hasattr(files, 'get'):
                        file_obj = files.get(filename)
                    elif hasattr(files, '__getitem__'):
                        try:
                            file_obj = files[filename]
                        except KeyError:
                            file_obj = None
                    else:
                        # Try to iterate
                        file_obj = None
                        for key, value in files.items():
                            if key == filename:
                                file_obj = value
                                break
                    
                    if file_obj:
                        print(f"DEBUG: Found file object: {type(file_obj)}, filename: {getattr(file_obj, 'filename', 'N/A')}")
                    return file_obj
                return None
            else:
                # Flask
                if hasattr(request, 'files') and request.files:
                    return request.files.get(filename)
                return None
        except Exception as e:
            print(f"Error getting file: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def read_file_bytes(file):
        """Read file bytes, async-safe"""
        if not file:
            return None
        try:
            if is_async:
                # Quart file object - read() is async
                if hasattr(file, 'read'):
                    # In Quart, file.read() returns a coroutine
                    try:
                        result = file.read()
                        # Check if it's a coroutine (async function)
                        if hasattr(result, '__await__'):
                            bytes_data = await result
                        else:
                            bytes_data = result
                        return bytes_data
                    except Exception as e:
                        print(f"Error reading file (async): {e}")
                        import traceback
                        traceback.print_exc()
                        return None
                # Try stream attribute if available
                elif hasattr(file, 'stream'):
                    stream = file.stream
                    if hasattr(stream, 'read'):
                        result = stream.read()
                        if hasattr(result, '__await__'):
                            return await result
                        return result
                return None
            else:
                # Flask file object - read() is sync
                if hasattr(file, 'read'):
                    return file.read()
                return None
        except Exception as e:
            print(f"Error reading file bytes: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    @app.route(f'{prefix}/health', methods=['GET'])
    async def go_quick_health_check():
        """Health check cho tool này"""
        return jsonify({
            "status": "success",
            "message": "ID Quick API is running",
            "version": "1.0"
        })
    
    @app.route(f'{prefix}/process-cccd', methods=['POST'])
    async def go_quick_process_cccd():
        """Xử lý CCCD Extractor"""
        try:
            inp_data = None
            
            # Debug: Check request content type
            content_type = request.headers.get('Content-Type', '')
            print(f"DEBUG: Content-Type: {content_type}")
            print(f"DEBUG: Request method: {request.method}")
            
            # Cách 1: Upload file
            try:
                # In Quart, request.files is a COROUTINE, need to await!
                if is_async:
                    # Check if multipart/form-data
                    if 'multipart/form-data' in content_type:
                        # IMPORTANT: await request.files in Quart!
                        files = await request.files
                        print(f"DEBUG: request.files (after await) type: {type(files)}, content: {files}")
                        
                        # Try different ways to access
                        file = None
                        if files:
                            # Method 1: Direct key access
                            try:
                                if 'file' in files:
                                    file = files['file']
                                    print(f"DEBUG: Found file via ['file']")
                            except (KeyError, TypeError):
                                pass
                            
                            # Method 2: get() method
                            if not file and hasattr(files, 'get'):
                                file = files.get('file')
                                if file:
                                    print(f"DEBUG: Found file via .get('file')")
                            
                            # Method 3: Iterate through files
                            if not file:
                                try:
                                    for key, value in files.items():
                                        print(f"DEBUG: File key: {key}, value type: {type(value)}")
                                        if key == 'file':
                                            file = value
                                            break
                                except Exception as e:
                                    print(f"DEBUG: Error iterating files: {e}")
                    else:
                        print(f"DEBUG: Not multipart/form-data, content-type: {content_type}")
                        file = None
                else:
                    # Flask
                    file = request.files.get('file') if hasattr(request, 'files') and request.files else None
                
                if file:
                    filename = getattr(file, 'filename', None) or getattr(file, 'name', None) or ''
                    print(f"DEBUG: File found, filename: {filename}")
                    if filename and filename != '':
                        file_bytes = await read_file_bytes(file)
                        if file_bytes:
                            print(f"DEBUG: File bytes length: {len(file_bytes)}")
                            if len(file_bytes) > MAX_FILE_SIZE:
                                return jsonify({
                                    "status": "error",
                                    "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB"
                                }), 400
                            inp_data = file_bytes
                else:
                    print("DEBUG: No file found in request.files")
            except Exception as e:
                print(f"Error processing file upload: {e}")
                import traceback
                traceback.print_exc()
                # Continue to try JSON method
            
            # Cách 2: JSON với base64 hoặc bytes
            if not inp_data:
                try:
                    # Check if request is JSON
                    content_type = request.headers.get('Content-Type', '')
                    if 'application/json' in content_type or (is_async and hasattr(request, 'is_json') and request.is_json):
                        data = await get_request_json()
                        if data:
                            inp_path = data.get("inp_path")
                            if inp_path:
                                inp_data = decode_input_data(inp_path)
                except Exception as e:
                    print(f"Error processing JSON: {e}")
            
            if not inp_data:
                return jsonify({
                    "status": "error",
                    "message": "No file or data provided"
                }), 400
            
            # Load models and process (this might take time, but should not crash)
            try:
                print("DEBUG: Loading model cache...")
                model_cache = get_model_cache()
                print("DEBUG: Model cache loaded")
                
                print("DEBUG: Getting CCCDExtractor class...")
                CCCDExtractorClass = get_cccd_extractor()
                print("DEBUG: Creating extractor instance...")
                extractor = CCCDExtractorClass(cached_models=model_cache)
                print("DEBUG: Extractor created")
                
                task = {
                    "func_type": 1,
                    "inp_path": inp_data
                }
                
                print("DEBUG: Processing task...")
                results = extractor.handle_task(task)
                print("DEBUG: Task completed")
                
                return jsonify(results)
            except Exception as e:
                print(f"Error in model processing: {e}")
                import traceback
                traceback.print_exc()
                raise  # Re-raise to be caught by outer try-except
            
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
    
    @app.route(f'{prefix}/process-pdf', methods=['POST'])
    async def go_quick_process_pdf():
        """Xử lý PDF to PNG"""
        try:
            inp_data = None
            
            try:
                file = await get_request_file('file')
                if file:
                    filename = getattr(file, 'filename', None) or getattr(file, 'name', None)
                    if filename and filename != '':
                        if not filename.lower().endswith('.pdf'):
                            return jsonify({
                                "status": "error",
                                "message": "File phải là PDF (.pdf)"
                            }), 400
                        
                        file_bytes = await read_file_bytes(file)
                        if file_bytes:
                            if len(file_bytes) > MAX_FILE_SIZE:
                                return jsonify({
                                    "status": "error",
                                    "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB"
                                }), 400
                            inp_data = file_bytes
            except Exception as e:
                print(f"Error processing file upload: {e}")
            
            if not inp_data:
                try:
                    content_type = request.headers.get('Content-Type', '')
                    if 'application/json' in content_type or (is_async and hasattr(request, 'is_json') and request.is_json):
                        data = await get_request_json()
                        if data:
                            inp_path = data.get("inp_path")
                            if inp_path:
                                inp_data = decode_input_data(inp_path)
                except Exception as e:
                    print(f"Error processing JSON: {e}")
            
            if not inp_data:
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
            error_detail = traceback.format_exc() if app.config.get('DEBUG', False) else None
            print(f"Error in process-pdf: {e}")
            if error_detail:
                print(error_detail)
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": error_detail
            }), 500
    
    @app.route(f'{prefix}/process-excel', methods=['POST'])
    async def go_quick_process_excel():
        """Xử lý Excel to PNG"""
        try:
            inp_data = None
            
            try:
                file = await get_request_file('file')
                if file:
                    filename = getattr(file, 'filename', None) or getattr(file, 'name', None)
                    if filename and filename != '':
                        if not allowed_file(filename):
                            return jsonify({
                                "status": "error",
                                "message": "File type not allowed. Expected .xlsx or .xls"
                            }), 400
                        
                        file_bytes = await read_file_bytes(file)
                        if file_bytes:
                            if len(file_bytes) > MAX_FILE_SIZE:
                                return jsonify({
                                    "status": "error",
                                    "message": f"File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB"
                                }), 400
                            inp_data = file_bytes
            except Exception as e:
                print(f"Error processing file upload: {e}")
            
            if not inp_data:
                try:
                    content_type = request.headers.get('Content-Type', '')
                    if 'application/json' in content_type or (is_async and hasattr(request, 'is_json') and request.is_json):
                        data = await get_request_json()
                        if data:
                            inp_path = data.get("inp_path")
                            if inp_path:
                                inp_data = decode_input_data(inp_path)
                except Exception as e:
                    print(f"Error processing JSON: {e}")
            
            if not inp_data:
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
            error_detail = traceback.format_exc() if app.config.get('DEBUG', False) else None
            print(f"Error in process-excel: {e}")
            if error_detail:
                print(error_detail)
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": error_detail
            }), 500


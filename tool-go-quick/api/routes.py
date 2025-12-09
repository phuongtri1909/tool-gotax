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

# Thêm parent directory vào path để import main
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import job manager
from api.job_manager import job_manager, JobStatus

# Lazy import - chỉ import khi cần dùng
CCCDExtractor = None
CCCDExtractorStreaming = None

def get_cccd_extractor():
    global CCCDExtractor
    if CCCDExtractor is None:
        from main import CCCDExtractor as _CCCDExtractor
        CCCDExtractor = _CCCDExtractor
    return CCCDExtractor

def get_cccd_extractor_streaming():
    global CCCDExtractorStreaming
    if CCCDExtractorStreaming is None:
        from main import CCCDExtractorStreaming as _CCCDExtractorStreaming
        CCCDExtractorStreaming = _CCCDExtractorStreaming
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
    
    # Helper function để wrap generator với flush
    def create_sse_response(generator_func):
        """Tạo SSE Response với proper flushing"""
        async def wrapped_generator():
            try:
                async for chunk in generator_func():
                    yield chunk
                    # Small delay để đảm bảo flush
                    await asyncio.sleep(0.01)
            except GeneratorExit:
                # Client disconnected
                pass
            except Exception as e:
                import traceback
                print(f"Error in SSE generator: {e}")
                traceback.print_exc()
                try:
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                except:
                    pass
        
        return Response(
            wrapped_generator(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no',
                'Transfer-Encoding': 'chunked'
            }
        )
    
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
    
    # ==================== SSE STREAMING ROUTES ====================
    
    @app.route(f'{prefix}/process-cccd-stream', methods=['POST'])
    async def go_quick_process_cccd_stream():
        """Xử lý CCCD Extractor với SSE streaming để báo tiến trình"""
        
        # ĐỌC FILE TRƯỚC KHI TẠO GENERATOR (để tránh lỗi request context)
        inp_data = None
        error_message = None
        
        content_type = request.headers.get('Content-Type', '')
        
        # Cách 1: Upload file
        try:
            if is_async:
                if 'multipart/form-data' in content_type:
                    files = await request.files
                    file = None
                    if files:
                        try:
                            if 'file' in files:
                                file = files['file']
                        except (KeyError, TypeError):
                            pass
                        
                        if not file and hasattr(files, 'get'):
                            file = files.get('file')
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
                            error_message = f'File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB'
                        else:
                            inp_data = file_bytes
        except Exception as e:
            print(f"Error processing file upload: {e}")
            import traceback
            traceback.print_exc()
        
        # Cách 2: JSON với base64 hoặc bytes
        if not inp_data and not error_message:
            try:
                if 'application/json' in content_type:
                    data = await get_request_json()
                    if data:
                        inp_path = data.get("inp_path")
                        if inp_path:
                            inp_data = decode_input_data(inp_path)
            except Exception as e:
                print(f"Error processing JSON: {e}")
        
        if not inp_data and not error_message:
            error_message = 'Không có file hoặc dữ liệu được cung cấp'
        
        # Tạo generator với data đã đọc
        async def generate_sse(inp_data, error_message):
            try:
                if error_message:
                    yield f"data: {json.dumps({'type': 'error', 'message': error_message})}\n\n"
                    return
                
                # Yield start event
                yield f"data: {json.dumps({'type': 'start', 'message': 'Bắt đầu xử lý CCCD...'})}\n\n"
                
                # Load models
                yield f"data: {json.dumps({'type': 'progress', 'step': 'loading_models', 'message': 'Đang tải models...', 'percent': 5})}\n\n"
                
                model_cache = get_model_cache()
                
                # Get streaming extractor
                CCCDExtractorStreamingClass = get_cccd_extractor_streaming()
                extractor = CCCDExtractorStreamingClass(cached_models=model_cache)
                
                task = {
                    "func_type": 1,
                    "inp_path": inp_data
                }
                
                # Process with streaming - yield progress events
                try:
                    for event in extractor.handle_task_streaming(task):
                        yield f"data: {json.dumps(event)}\n\n"
                        # Allow other coroutines to run
                        await asyncio.sleep(0)
                except GeneratorExit:
                    # Generator was closed, don't yield anything
                    raise
                except Exception as e:
                    import traceback
                    print(f"Error in extractor.handle_task_streaming: {e}")
                    traceback.print_exc()
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Lỗi xử lý: {str(e)}'})}\n\n"
                
            except GeneratorExit:
                # Client disconnected, just exit
                raise
            except Exception as e:
                import traceback
                print(f"Error in process-cccd-stream: {e}")
                traceback.print_exc()
                try:
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                except:
                    pass
        
        # Wrap generator để đảm bảo flush
        async def wrapped_generator():
            try:
                async for chunk in generate_sse(inp_data, error_message):
                    yield chunk
                    # Small delay để đảm bảo flush
                    await asyncio.sleep(0.01)
            except GeneratorExit:
                pass
            except Exception as e:
                import traceback
                print(f"Error in wrapped generator: {e}")
                traceback.print_exc()
                try:
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                except:
                    pass
        
        return Response(
            wrapped_generator(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no',
                'Transfer-Encoding': 'chunked'
            }
        )
    
    @app.route(f'{prefix}/process-pdf-stream', methods=['POST'])
    async def go_quick_process_pdf_stream():
        """Xử lý PDF với SSE streaming để báo tiến trình"""
        
        # ĐỌC FILE TRƯỚC KHI TẠO GENERATOR
        inp_data = None
        error_message = None
        
        content_type = request.headers.get('Content-Type', '')
        
        try:
            if is_async:
                if 'multipart/form-data' in content_type:
                    files = await request.files
                    file = files.get('file') if files else None
                else:
                    file = None
            else:
                file = request.files.get('file') if hasattr(request, 'files') and request.files else None
            
            if file:
                filename = getattr(file, 'filename', None) or getattr(file, 'name', None)
                if filename and filename != '':
                    if not filename.lower().endswith('.pdf'):
                        error_message = 'File phải là PDF (.pdf)'
                    else:
                        file_bytes = await read_file_bytes(file)
                        if file_bytes:
                            if len(file_bytes) > MAX_FILE_SIZE:
                                error_message = f'File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB'
                            else:
                                inp_data = file_bytes
        except Exception as e:
            print(f"Error processing file upload: {e}")
            import traceback
            traceback.print_exc()
        
        if not inp_data and not error_message:
            try:
                if 'application/json' in content_type:
                    data = await get_request_json()
                    if data:
                        inp_path = data.get("inp_path")
                        if inp_path:
                            inp_data = decode_input_data(inp_path)
            except Exception as e:
                print(f"Error processing JSON: {e}")
        
        if not inp_data and not error_message:
            error_message = 'Không có file hoặc dữ liệu được cung cấp'
        
        async def generate_sse(inp_data, error_message):
            try:
                if error_message:
                    yield f"data: {json.dumps({'type': 'error', 'message': error_message})}\n\n"
                    return
                
                # Yield start event
                yield f"data: {json.dumps({'type': 'start', 'message': 'Bắt đầu xử lý PDF...'})}\n\n"
                
                # Load models
                yield f"data: {json.dumps({'type': 'progress', 'step': 'loading_models', 'message': 'Đang tải models...', 'percent': 5})}\n\n"
                
                model_cache = get_model_cache()
                CCCDExtractorStreamingClass = get_cccd_extractor_streaming()
                extractor = CCCDExtractorStreamingClass(cached_models=model_cache)
                
                # Step 1: Convert PDF to images
                yield f"data: {json.dumps({'type': 'progress', 'step': 'pdf_to_images', 'message': 'Đang chuyển PDF sang ảnh...', 'percent': 10})}\n\n"
                
                task = {
                    "func_type": 2,
                    "inp_path": inp_data
                }
                results = extractor.handle_task(task)  # Non-streaming for PDF conversion
                
                if results.get("status") != "success" or not results.get("zip_base64"):
                    yield f"data: {json.dumps({'type': 'error', 'message': results.get('message', 'Lỗi chuyển PDF sang ảnh')})}\n\n"
                    return
                
                total_imgs = results.get('total_images', 0)
                msg = f'Đã chuyển {total_imgs} trang từ PDF'
                yield f"data: {json.dumps({'type': 'progress', 'step': 'pdf_converted', 'message': msg, 'percent': 20, 'total_images': total_imgs})}\n\n"
                
                # Step 2: Process images with streaming
                zip_bytes = base64.b64decode(results.get("zip_base64"))
                task2 = {
                    "func_type": 1,
                    "inp_path": zip_bytes
                }
                
                # Process with streaming
                try:
                    for event in extractor.handle_task_streaming(task2, base_percent=20):
                        yield f"data: {json.dumps(event)}\n\n"
                        await asyncio.sleep(0)
                except GeneratorExit:
                    raise
                except Exception as e:
                    import traceback
                    print(f"Error in extractor.handle_task_streaming: {e}")
                    traceback.print_exc()
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Lỗi xử lý: {str(e)}'})}\n\n"
                
            except GeneratorExit:
                # Client disconnected, just exit
                raise
            except Exception as e:
                import traceback
                print(f"Error in process-pdf-stream: {e}")
                traceback.print_exc()
                try:
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                except:
                    pass
        
        # Wrap generator để đảm bảo flush
        async def wrapped_generator():
            try:
                async for chunk in generate_sse(inp_data, error_message):
                    yield chunk
                    # Small delay để đảm bảo flush
                    await asyncio.sleep(0.01)
            except GeneratorExit:
                pass
            except Exception as e:
                import traceback
                print(f"Error in wrapped generator: {e}")
                traceback.print_exc()
                try:
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                except:
                    pass
        
        return Response(
            wrapped_generator(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no',
                'Transfer-Encoding': 'chunked'
            }
        )
    
    @app.route(f'{prefix}/process-excel-stream', methods=['POST'])
    async def go_quick_process_excel_stream():
        """Xử lý Excel với SSE streaming để báo tiến trình"""
        
        # ĐỌC FILE TRƯỚC KHI TẠO GENERATOR
        inp_data = None
        error_message = None
        
        content_type = request.headers.get('Content-Type', '')
        
        try:
            if is_async:
                if 'multipart/form-data' in content_type:
                    files = await request.files
                    file = files.get('file') if files else None
                else:
                    file = None
            else:
                file = request.files.get('file') if hasattr(request, 'files') and request.files else None
            
            if file:
                filename = getattr(file, 'filename', None) or getattr(file, 'name', None)
                if filename and filename != '':
                    if not allowed_file(filename):
                        error_message = 'Định dạng file không hỗ trợ. Chấp nhận .xlsx hoặc .xls'
                    else:
                        file_bytes = await read_file_bytes(file)
                        if file_bytes:
                            if len(file_bytes) > MAX_FILE_SIZE:
                                error_message = f'File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB'
                            else:
                                inp_data = file_bytes
        except Exception as e:
            print(f"Error processing file upload: {e}")
            import traceback
            traceback.print_exc()
        
        if not inp_data and not error_message:
            try:
                if 'application/json' in content_type:
                    data = await get_request_json()
                    if data:
                        inp_path = data.get("inp_path")
                        if inp_path:
                            inp_data = decode_input_data(inp_path)
            except Exception as e:
                print(f"Error processing JSON: {e}")
        
        if not inp_data and not error_message:
            error_message = 'Không có file hoặc dữ liệu được cung cấp'
        
        async def generate_sse(inp_data, error_message):
            try:
                if error_message:
                    yield f"data: {json.dumps({'type': 'error', 'message': error_message})}\n\n"
                    return
                
                # Yield start event
                yield f"data: {json.dumps({'type': 'start', 'message': 'Bắt đầu xử lý Excel...'})}\n\n"
                
                # Load models
                yield f"data: {json.dumps({'type': 'progress', 'step': 'loading_models', 'message': 'Đang tải models...', 'percent': 5})}\n\n"
                
                model_cache = get_model_cache()
                CCCDExtractorStreamingClass = get_cccd_extractor_streaming()
                extractor = CCCDExtractorStreamingClass(cached_models=model_cache)
                
                # Step 1: Download images from Excel URLs with streaming
                yield f"data: {json.dumps({'type': 'progress', 'step': 'excel_download', 'message': 'Đang tải ảnh từ Excel...', 'percent': 10})}\n\n"
                
                task = {
                    "func_type": 3,
                    "inp_path": inp_data
                }
                
                # Process Excel with streaming (download progress)
                results = None
                try:
                    for event in extractor.handle_excel_streaming(task):
                        if event.get('type') == 'complete':
                            results = event.get('data')
                        else:
                            yield f"data: {json.dumps(event)}\n\n"
                        await asyncio.sleep(0)
                except GeneratorExit:
                    raise
                except Exception as e:
                    import traceback
                    print(f"Error in extractor.handle_excel_streaming: {e}")
                    traceback.print_exc()
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Lỗi tải ảnh từ Excel: {str(e)}'})}\n\n"
                    return
                
                if not results or results.get("status") != "success" or not results.get("zip_base64"):
                    error_msg = results.get('message', 'Lỗi tải ảnh từ Excel') if results else 'Lỗi tải ảnh từ Excel'
                    yield f"data: {json.dumps({'type': 'error', 'message': error_msg})}\n\n"
                    return
                
                total_imgs_excel = results.get('total_images', 0)
                msg = f'Đã tải {total_imgs_excel} ảnh'
                yield f"data: {json.dumps({'type': 'progress', 'step': 'excel_downloaded', 'message': msg, 'percent': 30, 'total_images': total_imgs_excel})}\n\n"
                
                # Step 2: Process images with streaming
                zip_bytes = base64.b64decode(results.get("zip_base64"))
                task2 = {
                    "func_type": 1,
                    "inp_path": zip_bytes
                }
                
                # Process with streaming
                try:
                    for event in extractor.handle_task_streaming(task2, base_percent=30):
                        yield f"data: {json.dumps(event)}\n\n"
                        await asyncio.sleep(0)
                except GeneratorExit:
                    raise
                except Exception as e:
                    import traceback
                    print(f"Error in extractor.handle_task_streaming: {e}")
                    traceback.print_exc()
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Lỗi xử lý: {str(e)}'})}\n\n"
                
            except GeneratorExit:
                # Client disconnected, just exit
                raise
            except Exception as e:
                import traceback
                print(f"Error in process-excel-stream: {e}")
                traceback.print_exc()
                try:
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                except:
                    pass
        
        # Wrap generator để đảm bảo flush
        async def wrapped_generator():
            try:
                async for chunk in generate_sse(inp_data, error_message):
                    yield chunk
                    # Small delay để đảm bảo flush
                    await asyncio.sleep(0.01)
            except GeneratorExit:
                pass
            except Exception as e:
                import traceback
                print(f"Error in wrapped generator: {e}")
                traceback.print_exc()
                try:
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                except:
                    pass
        
        return Response(
            wrapped_generator(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no',
                'Transfer-Encoding': 'chunked'
            }
        )
    
    @app.route(f'{prefix}/process-images-stream', methods=['POST'])
    async def go_quick_process_images_stream():
        """Xử lý multiple images với SSE streaming để báo tiến trình"""
        
        # ĐỌC FILE TRƯỚC KHI TẠO GENERATOR
        images_data = []
        
        content_type = request.headers.get('Content-Type', '')
        
        try:
            if is_async and 'multipart/form-data' in content_type:
                files = await request.files
                if files:
                    # Get all images[] files
                    for key in files:
                        if key.startswith('images'):
                            file = files[key]
                            if file:
                                file_bytes = await read_file_bytes(file)
                                if file_bytes:
                                    images_data.append({
                                        'filename': getattr(file, 'filename', f'image_{len(images_data)}.jpg'),
                                        'data': file_bytes
                                    })
        except Exception as e:
            print(f"Error processing file upload: {e}")
            import traceback
            traceback.print_exc()
        
        async def generate_sse(images_data):
            try:
                if not images_data:
                    yield f"data: {json.dumps({'type': 'error', 'message': 'Không có ảnh được cung cấp'})}\n\n"
                    return
                
                # Yield start event
                total_images = len(images_data)
                msg = f'Bắt đầu xử lý {total_images} ảnh...'
                yield f"data: {json.dumps({'type': 'start', 'message': msg, 'total_images': total_images})}\n\n"
                
                # Load models
                yield f"data: {json.dumps({'type': 'progress', 'step': 'loading_models', 'message': 'Đang tải models...', 'percent': 5})}\n\n"
                
                model_cache = get_model_cache()
                CCCDExtractorStreamingClass = get_cccd_extractor_streaming()
                extractor = CCCDExtractorStreamingClass(cached_models=model_cache)
                
                # Create zip from images
                import zipfile
                from io import BytesIO
                
                mem_zip = BytesIO()
                with zipfile.ZipFile(mem_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for img_info in images_data:
                        zf.writestr(img_info['filename'], img_info['data'])
                
                mem_zip.seek(0)
                zip_bytes = mem_zip.getvalue()
                
                task = {
                    "func_type": 1,
                    "inp_path": zip_bytes
                }
                
                # Process with streaming
                for event in extractor.handle_task_streaming(task, base_percent=10):
                    yield f"data: {json.dumps(event)}\n\n"
                    await asyncio.sleep(0)
                
            except Exception as e:
                import traceback
                print(f"Error in process-images-stream: {e}")
                traceback.print_exc()
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
        
        return Response(
            generate_sse(images_data),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no'
            }
        )
    
    # ==================== ASYNC JOB ROUTES ====================
    
    @app.route(f'{prefix}/process-cccd-async', methods=['POST'])
    async def go_quick_process_cccd_async():
        """Start async job để xử lý CCCD - trả về job_id ngay lập tức"""
        try:
            inp_data = None
            error_message = None
            
            content_type = request.headers.get('Content-Type', '')
            
            # Cách 1: Upload file
            try:
                if is_async:
                    if 'multipart/form-data' in content_type:
                        files = await request.files
                        file = None
                        if files:
                            try:
                                if 'file' in files:
                                    file = files['file']
                            except (KeyError, TypeError):
                                pass
                            
                            if not file and hasattr(files, 'get'):
                                file = files.get('file')
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
                                error_message = f'File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB'
                            else:
                                inp_data = file_bytes
            except Exception as e:
                print(f"Error processing file upload: {e}")
            
            # Cách 2: JSON với base64
            if not inp_data and not error_message:
                try:
                    if 'application/json' in content_type:
                        data = await get_request_json()
                        if data:
                            inp_path = data.get("inp_path")
                            if inp_path:
                                inp_data = decode_input_data(inp_path)
                except Exception as e:
                    print(f"Error processing JSON: {e}")
            
            if not inp_data and not error_message:
                error_message = 'Không có file hoặc dữ liệu được cung cấp'
            
            if error_message:
                return jsonify({
                    "status": "error",
                    "message": error_message
                }), 400
            
            # Tạo job
            job_id = job_manager.create_job(func_type=1, inp_data=inp_data)
            
            # Start job trong background
            def worker_func(func_type, inp_data, progress_callback):
                """Worker function để chạy job"""
                model_cache = get_model_cache()
                CCCDExtractorClass = get_cccd_extractor()
                extractor = CCCDExtractorClass(cached_models=model_cache)
                
                task = {
                    "func_type": func_type,
                    "inp_path": inp_data
                }
                
                # Sử dụng streaming để track progress
                CCCDExtractorStreamingClass = get_cccd_extractor_streaming()
                streaming_extractor = CCCDExtractorStreamingClass(cached_models=model_cache)
                
                # Process với streaming để có progress updates
                total_cccd = 0
                processed_cccd = 0
                total_images = 0
                processed_images = 0
                
                for event in streaming_extractor.handle_task_streaming(task):
                    if event.get('type') == 'progress':
                        progress = min(100, max(0, event.get('percent', 0)))  # Giới hạn 0-100%
                        message = event.get('message', 'Đang xử lý...')
                        
                        # Extract tracking info
                        if 'total_images' in event:
                            total_images = event.get('total_images', 0)
                        if 'processed' in event:
                            processed_images = event.get('processed', 0)
                        if 'estimated_cccd' in event:
                            total_cccd = event.get('estimated_cccd', 0)
                        if 'processed_cccd' in event:
                            processed_cccd = event.get('processed_cccd', 0)
                        
                        # Call progress callback với đầy đủ thông tin
                        progress_callback(
                            progress, 
                            message,
                            total_cccd=total_cccd,
                            processed_cccd=processed_cccd,
                            total_images=total_images,
                            processed_images=processed_images
                        )
                    elif event.get('type') == 'complete':
                        result = event.get('data')
                        # Tính số CCCD thực tế từ kết quả
                        if result and isinstance(result, dict) and 'customer' in result:
                            actual_cccd = len(result.get('customer', []))
                            total_cccd = max(total_cccd, actual_cccd)
                            processed_cccd = actual_cccd
                        
                        progress_callback(
                            100, 
                            'Hoàn thành',
                            total_cccd=total_cccd,
                            processed_cccd=processed_cccd,
                            total_images=total_images,
                            processed_images=processed_images
                        )
                        return result
                    elif event.get('type') == 'error':
                        raise Exception(event.get('message', 'Lỗi xử lý'))
                
                # Fallback: nếu không có streaming, dùng sync
                return extractor.handle_task(task)
            
            job_manager.start_job(job_id, worker_func)
            
            return jsonify({
                "status": "success",
                "message": "Job đã được tạo và đang xử lý",
                "job_id": job_id
            })
            
        except Exception as e:
            import traceback
            print(f"Error in process-cccd-async: {e}")
            traceback.print_exc()
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/process-pdf-async', methods=['POST'])
    async def go_quick_process_pdf_async():
        """Start async job để xử lý PDF"""
        try:
            inp_data = None
            error_message = None
            
            content_type = request.headers.get('Content-Type', '')
            
            try:
                if is_async:
                    if 'multipart/form-data' in content_type:
                        files = await request.files
                        file = files.get('file') if files else None
                    else:
                        file = None
                else:
                    file = request.files.get('file') if hasattr(request, 'files') and request.files else None
                
                if file:
                    filename = getattr(file, 'filename', None) or getattr(file, 'name', None)
                    if filename and filename != '':
                        if not filename.lower().endswith('.pdf'):
                            error_message = 'File phải là PDF (.pdf)'
                        else:
                            file_bytes = await read_file_bytes(file)
                            if file_bytes:
                                if len(file_bytes) > MAX_FILE_SIZE:
                                    error_message = f'File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB'
                                else:
                                    inp_data = file_bytes
            except Exception as e:
                print(f"Error processing file upload: {e}")
            
            if not inp_data and not error_message:
                try:
                    if 'application/json' in content_type:
                        data = await get_request_json()
                        if data:
                            inp_path = data.get("inp_path")
                            if inp_path:
                                inp_data = decode_input_data(inp_path)
                except Exception as e:
                    print(f"Error processing JSON: {e}")
            
            if not inp_data and not error_message:
                error_message = 'Không có file hoặc dữ liệu được cung cấp'
            
            if error_message:
                return jsonify({
                    "status": "error",
                    "message": error_message
                }), 400
            
            # Tạo job
            job_id = job_manager.create_job(func_type=2, inp_data=inp_data)
            
            def worker_func(func_type, inp_data, progress_callback):
                model_cache = get_model_cache()
                CCCDExtractorStreamingClass = get_cccd_extractor_streaming()
                streaming_extractor = CCCDExtractorStreamingClass(cached_models=model_cache)
                
                # Xử lý PDF: Convert và xử lý từng cặp trang ngay (không cần Step 2)
                task = {"func_type": 2, "inp_path": inp_data}
                
                results = None
                total_cccd = 0
                processed_cccd = 0
                total_images = 0
                processed_images = 0
                
                for event in streaming_extractor.handle_pdf_streaming(task):
                    if event.get('type') == 'progress':
                        progress = min(100, max(0, event.get('percent', 0)))
                        message = event.get('message', 'Đang xử lý...')
                        
                        if 'total_cccd' in event:
                            total_cccd = event.get('total_cccd', total_cccd)
                        if 'processed_cccd' in event and event.get('processed_cccd') is not None:
                            processed_cccd = max(processed_cccd, event.get('processed_cccd', 0))
                        if 'total_images' in event:
                            total_images = max(total_images, event.get('total_images', 0))
                        if 'processed' in event or 'processed_images' in event:
                            processed_images = event.get('processed', event.get('processed_images', processed_images))
                        
                        progress_callback(
                            progress, 
                            message,
                            total_cccd=total_cccd,
                            processed_cccd=processed_cccd,
                            total_images=total_images,
                            processed_images=processed_images
                        )
                    elif event.get('type') == 'complete':
                        results = event.get('data')
                        if results and isinstance(results, dict):
                            if 'total_cccd' in results:
                                total_cccd = results.get('total_cccd', total_cccd)
                            if 'processed_cccd' in results:
                                processed_cccd = results.get('processed_cccd', processed_cccd)
                            if 'total_images' in results:
                                total_images = results.get('total_images', total_images)
                            if 'customer' in results:
                                actual_cccd = len(results.get('customer', []))
                                processed_cccd = actual_cccd
                                if total_cccd == 0:
                                    total_cccd = actual_cccd
                        
                        progress_callback(
                            100, 
                            'Hoàn thành',
                            total_images=total_images,
                            processed_images=processed_images,
                            total_cccd=total_cccd,
                            processed_cccd=processed_cccd
                        )
                        return results
                    elif event.get('type') == 'error':
                        raise Exception(event.get('message', 'Lỗi xử lý PDF'))
                
                if not results:
                    raise Exception('Không có kết quả từ PDF')
                
                return results
            
            job_manager.start_job(job_id, worker_func)
            
            return jsonify({
                "status": "success",
                "message": "Job đã được tạo và đang xử lý",
                "job_id": job_id
            })
            
        except Exception as e:
            import traceback
            print(f"Error in process-pdf-async: {e}")
            traceback.print_exc()
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/process-excel-async', methods=['POST'])
    async def go_quick_process_excel_async():
        """Start async job để xử lý Excel"""
        try:
            inp_data = None
            error_message = None
            
            content_type = request.headers.get('Content-Type', '')
            
            try:
                if is_async:
                    if 'multipart/form-data' in content_type:
                        files = await request.files
                        file = files.get('file') if files else None
                    else:
                        file = None
                else:
                    file = request.files.get('file') if hasattr(request, 'files') and request.files else None
                
                if file:
                    filename = getattr(file, 'filename', None) or getattr(file, 'name', None)
                    if filename and filename != '':
                        if not allowed_file(filename):
                            error_message = 'Định dạng file không hỗ trợ. Chấp nhận .xlsx hoặc .xls'
                        else:
                            file_bytes = await read_file_bytes(file)
                            if file_bytes:
                                if len(file_bytes) > MAX_FILE_SIZE:
                                    error_message = f'File quá lớn. Tối đa {MAX_FILE_SIZE / 1024 / 1024}MB'
                                else:
                                    inp_data = file_bytes
            except Exception as e:
                print(f"Error processing file upload: {e}")
            
            if not inp_data and not error_message:
                try:
                    if 'application/json' in content_type:
                        data = await get_request_json()
                        if data:
                            inp_path = data.get("inp_path")
                            if inp_path:
                                inp_data = decode_input_data(inp_path)
                except Exception as e:
                    print(f"Error processing JSON: {e}")
            
            if not inp_data and not error_message:
                error_message = 'Không có file hoặc dữ liệu được cung cấp'
            
            if error_message:
                return jsonify({
                    "status": "error",
                    "message": error_message
                }), 400
            
            # Tạo job
            job_id = job_manager.create_job(func_type=3, inp_data=inp_data)
            
            def worker_func(func_type, inp_data, progress_callback):
                model_cache = get_model_cache()
                CCCDExtractorStreamingClass = get_cccd_extractor_streaming()
                streaming_extractor = CCCDExtractorStreamingClass(cached_models=model_cache)
                
                # Xử lý Excel: Download và xử lý từng cặp ảnh ngay (không cần Step 2)
                task = {"func_type": 3, "inp_path": inp_data}
                
                results = None
                total_cccd = 0
                processed_cccd = 0
                total_images = 0
                processed_images = 0
                
                for event in streaming_extractor.handle_excel_streaming(task):
                    if event.get('type') == 'progress':
                        progress = min(100, max(0, event.get('percent', 0)))
                        message = event.get('message', 'Đang xử lý...')
                        
                        if 'total_cccd' in event:
                            total_cccd = event.get('total_cccd', total_cccd)
                        if 'processed_cccd' in event and event.get('processed_cccd') is not None:
                            processed_cccd = max(processed_cccd, event.get('processed_cccd', 0))
                        if 'total_images' in event:
                            total_images = max(total_images, event.get('total_images', 0))
                        if 'processed' in event or 'processed_images' in event:
                            processed_images = event.get('processed', event.get('processed_images', processed_images))
                        
                        progress_callback(
                            progress, 
                            message,
                            total_cccd=total_cccd,
                            processed_cccd=processed_cccd,
                            total_images=total_images,
                            processed_images=processed_images
                        )
                    elif event.get('type') == 'complete':
                        results = event.get('data')
                        if results and isinstance(results, dict):
                            if 'total_cccd' in results:
                                total_cccd = results.get('total_cccd', total_cccd)
                            if 'processed_cccd' in results:
                                processed_cccd = results.get('processed_cccd', processed_cccd)
                            if 'total_images' in results:
                                total_images = results.get('total_images', total_images)
                            if 'customer' in results:
                                actual_cccd = len(results.get('customer', []))
                                processed_cccd = actual_cccd
                                if total_cccd == 0:
                                    total_cccd = actual_cccd
                        
                        progress_callback(
                            100, 
                            'Hoàn thành',
                            total_images=total_images,
                            processed_images=processed_images,
                            total_cccd=total_cccd,
                            processed_cccd=processed_cccd
                        )
                        return results
                    elif event.get('type') == 'error':
                        raise Exception(event.get('message', 'Lỗi xử lý Excel'))
                
                if not results:
                    raise Exception('Không có kết quả từ Excel')
                
                return results
            
            job_manager.start_job(job_id, worker_func)
            
            return jsonify({
                "status": "success",
                "message": "Job đã được tạo và đang xử lý",
                "job_id": job_id
            })
            
        except Exception as e:
            import traceback
            print(f"Error in process-excel-async: {e}")
            traceback.print_exc()
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/job-status/<job_id>', methods=['GET'])
    async def go_quick_job_status(job_id):
        """Lấy status của job"""
        try:
            job = job_manager.get_job(job_id)
            
            if not job:
                return jsonify({
                    "status": "error",
                    "message": "Job không tồn tại"
                }), 404
            
            return jsonify({
                "status": "success",
                "data": job.to_dict()
            })
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/job-result/<job_id>', methods=['GET'])
    async def go_quick_job_result(job_id):
        """Lấy kết quả của job (chỉ khi completed)"""
        try:
            job = job_manager.get_job(job_id)
            
            if not job:
                return jsonify({
                    "status": "error",
                    "message": "Job không tồn tại"
                }), 404
            
            if job.status == JobStatus.COMPLETED:
                return jsonify({
                    "status": "success",
                    "data": job.result
                })
            elif job.status == JobStatus.FAILED:
                return jsonify({
                    "status": "error",
                    "message": job.error or "Job thất bại"
                }), 500
            else:
                return jsonify({
                    "status": "pending",
                    "message": f"Job đang ở trạng thái: {job.status.value}"
                }), 202
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/queue-info', methods=['GET'])
    async def go_quick_queue_info():
        """Lấy thông tin về queue và running jobs"""
        try:
            info = job_manager.get_queue_info()
            return jsonify({
                "status": "success",
                "data": info
            })
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500


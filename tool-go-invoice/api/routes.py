"""
Routes cho tool-go-invoice
Được gọi từ api_server.py chung

Luồng xử lý:
1. GET /api/go-invoice/get-captcha: Lấy captcha SVG (return base64 + ckey)
2. POST /api/go-invoice/login: Đăng nhập (ckey + captcha_value + username + password)
3. POST /api/go-invoice/tongquat: Tổng quát (headers + type_invoice + date range)
4. POST /api/go-invoice/chitiet: Chi tiết (result từ tongquat)
5. POST /api/go-invoice/xmlhtml: XML + HTML (result từ tongquat + options)
6. POST /api/go-invoice/pdf: PDF (result từ tongquat)
"""

import os
import sys
import base64
import logging
import traceback
import uuid
import threading
import json
from datetime import datetime

logger = logging.getLogger(__name__)

# Thêm parent directory vào path để import main
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import InvoiceBackend
from InvoiceBackend import InvoiceBackend

# Import ProgressTracker
from backend_.progress_tracker import ProgressTracker

# Model cache
_invoice_cache = {
    'backend': None,
    'lock': threading.Lock()
}

def get_invoice_backend(proxy_url=None, job_id=None):
    """
    ✅ Lazy load InvoiceBackend instance - tạo mới nếu có proxy
    Nếu proxy_url được cung cấp, luôn tạo instance mới để sử dụng proxy đó
    """
    global _invoice_cache
    
    # Nếu có proxy hoặc job_id, luôn tạo instance mới
    if proxy_url or job_id:
        print(f"🔄 Đang khởi tạo InvoiceBackend với proxy...")
        try:
            return InvoiceBackend(proxy_url=proxy_url, job_id=job_id)
        except Exception as e:
            print(f"⚠️  Cảnh báo: InvoiceBackend khởi tạo có vấn đề: {e}")
            return InvoiceBackend(proxy_url=proxy_url, job_id=job_id)
    
    # Nếu không có proxy, sử dụng cache singleton
    if _invoice_cache['backend'] is not None:
        return _invoice_cache['backend']
    
    with _invoice_cache['lock']:
        if _invoice_cache['backend'] is not None:
            return _invoice_cache['backend']
        
        print("🔄 Đang khởi tạo InvoiceBackend...")
        try:
            _invoice_cache['backend'] = InvoiceBackend()
            print("✅ InvoiceBackend đã được khởi tạo")
        except Exception as e:
            print(f"⚠️  Cảnh báo: InvoiceBackend khởi tạo có vấn đề: {e}")
            # Vẫn return instance, chỉ warn không crash
            _invoice_cache['backend'] = InvoiceBackend()
    
    return _invoice_cache['backend']

def register_routes(app, prefix):
    """
    Đăng ký routes cho tool này
    
    Args:
        app: Quart app instance (hoặc Flask)
        prefix: URL prefix (ví dụ: '/api/go-invoice')
    """
    
    # Helper to check if app is Quart (async) or Flask (sync)
    is_async = hasattr(app, 'ensure_async')
    
    if is_async:
        from quart import request, jsonify, Response
    else:
        from flask import request, jsonify, Response
    
    # Helper function để get JSON từ request (xử lý cả Quart async và Flask sync)
    def get_request_json_sync():
        """Get JSON from request - sync wrapper for Quart async request.get_json()"""
        try:
            if is_async:
                # Quart - request.get_json() là coroutine, cần await
                # Nhưng function này sync, dùng asyncio trong thread riêng
                import asyncio
                import concurrent.futures
                
                def run_in_new_loop():
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(request.get_json())
                    except Exception as e:
                        print(f"Error in run_in_new_loop: {e}")
                        return None
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_new_loop)
                    return future.result(timeout=10)
            else:
                # Flask - sync
                return request.get_json()
        except Exception as e:
            print(f"Error in get_request_json_sync: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    # ✅ Validation function: Kiểm tra ngày không vượt quá ngày hiện tại
    def validate_date_range(start_date, end_date):
        """Validate date range - không cho phép chọn ngày tương lai"""
        if not start_date or not end_date:
            return None, "Vui lòng chọn ngày bắt đầu và kết thúc"
        
        try:
            date_format = "%d/%m/%Y"
            today = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
            
            start_date_obj = datetime.strptime(start_date, date_format)
            end_date_obj = datetime.strptime(end_date, date_format)
            
            if start_date_obj > today:
                return None, "Ngày bắt đầu không được vượt quá ngày hiện tại"
            if end_date_obj > today:
                return None, "Ngày kết thúc không được vượt quá ngày hiện tại"
            if start_date_obj > end_date_obj:
                return None, "Ngày bắt đầu không được lớn hơn ngày kết thúc"
            
            return True, None
        except ValueError as e:
            return None, f"Lỗi định dạng ngày: {str(e)}"
        except Exception as e:
            return None, f"Lỗi khi kiểm tra ngày: {str(e)}"
    
    @app.route(f'{prefix}/health', methods=['GET'])
    def go_invoice_health_check():
        """Health check cho tool này"""
        return jsonify({
            "status": "success",
            "message": "Invoice Backend API is running",
            "version": "1.0"
        })
    
    @app.route(f'{prefix}/progress/<token>', methods=['GET'])
    def go_invoice_progress(token):
        """
        Xem tiến trình xử lý dựa trên token người dùng
        
        URL: GET /api/go-invoice/progress/{token}
        
        Returns:
        {
            "status": "success",
            "data": {
                "token": "...",
                "status": "processing|completed|failed",
                "progress_percentage": 0-100,
                "current_step": "...",
                "processed_invoices": int,
                "total_invoices": int,
                "elapsed_seconds": int,
                "start_time": "ISO format",
                "estimated_remaining_seconds": int (nếu có dữ liệu)
            }
        }
        """
        try:
            tracker = ProgressTracker.get(token)
            
            if tracker is None:
                return jsonify({
                    "status": "error",
                    "message": f"Token '{token}' not found or has expired"
                }), 404
            
            return jsonify({
                "status": "success",
                "data": tracker.get_status()
            })
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    @app.route(f'{prefix}/get-captcha', methods=['GET', 'POST'])
    def go_invoice_get_captcha():
        """
        Lấy captcha từ server - trả về SVG binary image + ckey trong header
        
        Optional JSON body:
        {
            "proxy": "http://proxy:port" or null
        }
        
        Returns:
            - Response body: SVG binary image
            - Response header: X-Captcha-Key = ckey (để client lưu dùng cho login)
        """
        try:
            proxy_url = None
            
            # ✅ Extract proxy từ query params (GET) hoặc JSON body (POST)
            # Quart và Flask đều có request.args
            if hasattr(request, 'args'):
                proxy_url = request.args.get("proxy")
            
            # ✅ Tạo backend với proxy nếu có
            backend = get_invoice_backend(proxy_url=proxy_url)
            
            captcha_data = backend.auth_service.getckey_captcha()
            
            ckey = captcha_data['ckey']
            svg_content = captcha_data['svg_content']
            
            # Convert SVG string to bytes
            if isinstance(svg_content, str):
                svg_bytes = svg_content.encode('utf-8')
            else:
                svg_bytes = svg_content
            
            # Return binary SVG image with ckey in header
            response = Response(svg_bytes, mimetype='image/svg+xml')
            response.headers['X-Captcha-Key'] = ckey
            response.headers['Content-Disposition'] = 'inline; filename=captcha.svg'
            return response
            
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc() if (hasattr(app, 'config') and app.config.get('DEBUG')) else None
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": error_detail
            }), 500
    
    # Tạo wrapper function cho login (xử lý async/sync)
    if is_async:
        @app.route(f'{prefix}/login', methods=['POST'])
        async def go_invoice_login():
            """Đăng nhập và lấy token (Async version for Quart)"""
            try:
                data = await request.get_json()
                
                if not data:
                    return jsonify({
                        "status": "error",
                        "message": "Request must be JSON"
                    }), 400
                
                ckey = data.get("ckey")
                captcha_value = data.get("captcha_value")
                username = data.get("username")
                password = data.get("password")
                proxy_url = data.get("proxy")
                
                # Validate required fields
                if not all([ckey, captcha_value, username, password]):
                    return jsonify({
                        "status": "error",
                        "message": "Missing required fields: ckey, captcha_value, username, password"
                    }), 400
                
                # ✅ Tạo backend với proxy nếu có
                backend = get_invoice_backend(proxy_url=proxy_url)
                
                headers = backend.auth_service.login_web(
                    ckey=ckey,
                    captcha_inp=captcha_value,
                    user=username,
                    pass_=password
                )
                
                if headers.get('status') == 'success':
                    token = headers.get('token', '')
                    return jsonify({
                        "status": "success",
                        "token": token,
                        "headers": headers
                    })
                else:
                    return jsonify({
                        "status": "error",
                        "message": headers.get('message', 'Login failed')
                    }), 401
                
            except Exception as e:
                return jsonify({
                    "status": "error",
                    "message": str(e),
                    "detail": traceback.format_exc() if (hasattr(app, 'config') and app.config.get('DEBUG')) else None
                }), 500
    else:
        @app.route(f'{prefix}/login', methods=['POST'])
        def go_invoice_login():
            """Đăng nhập và lấy token (Sync version for Flask)"""
            try:
                if not request.is_json:
                    return jsonify({
                        "status": "error",
                        "message": "Request must be JSON"
                    }), 400
                
                data = get_request_json_sync()
                ckey = data.get("ckey")
                captcha_value = data.get("captcha_value")
                username = data.get("username")
                password = data.get("password")
                proxy_url = data.get("proxy")
                
                # Validate required fields
                if not all([ckey, captcha_value, username, password]):
                    return jsonify({
                        "status": "error",
                        "message": "Missing required fields: ckey, captcha_value, username, password"
                    }), 400
                
                # ✅ Tạo backend với proxy nếu có
                backend = get_invoice_backend(proxy_url=proxy_url)
                
                headers = backend.auth_service.login_web(
                    ckey=ckey,
                    captcha_inp=captcha_value,
                    user=username,
                    pass_=password
                )
                
                if headers.get('status') == 'success':
                    token = headers.get('token', '')
                    return jsonify({
                        "status": "success",
                        "token": token,
                        "headers": headers
                    })
                else:
                    return jsonify({
                        "status": "error",
                        "message": headers.get('message', 'Login failed')
                    }), 401
                
            except Exception as e:
                return jsonify({
                    "status": "error",
                    "message": str(e),
                    "detail": traceback.format_exc() if (hasattr(app, 'config') and app.config.get('DEBUG')) else None
                }), 500
    
    # Tạo wrapper function cho validate-token (xử lý async/sync)
    if is_async:
        @app.route(f'{prefix}/validate-token', methods=['POST'])
        async def go_invoice_validate_token():
            """Validate token - kiểm tra token có còn hợp lệ không (Async version for Quart)"""
            try:
                data = await request.get_json()
                
                if not data:
                    return jsonify({
                        "status": "error",
                        "error_code": "INVALID_REQUEST",
                        "message": "Request must be JSON",
                        "is_valid": False
                    }), 400
                
                auth_header = data.get("Authorization")
                proxy_url = data.get("proxy")
                
                if not auth_header:
                    return jsonify({
                        "status": "error",
                        "error_code": "MISSING_AUTHORIZATION",
                        "message": "Missing Authorization header",
                        "is_valid": False
                    }), 400
                
                # Extract token từ Authorization header
                token = auth_header.replace("Bearer ", "").strip() if auth_header else None
                if not token:
                    return jsonify({
                        "status": "error",
                        "error_code": "INVALID_AUTHORIZATION_FORMAT",
                        "message": "Invalid Authorization format (should be 'Bearer token')",
                        "is_valid": False
                    }), 400
                
                # Basic format validation
                # Chỉ check format token, không gọi API lớn để tránh lỗi request context
                # Token validation thực sự sẽ được thực hiện khi gọi các API khác (tongquat, chitiet, etc.)
                if not token or len(token) < 10:
                    return jsonify({
                        "status": "error",
                        "error_code": "TOKEN_INVALID",
                        "message": "Token format is invalid",
                        "is_valid": False
                    }), 401
                
                # Token có format hợp lệ
                return jsonify({
                    "status": "success",
                    "is_valid": True,
                    "message": "Token format is valid"
                })
                
            except Exception as e:
                return jsonify({
                    "status": "error",
                    "error_code": "VALIDATE_TOKEN_ERROR",
                    "message": str(e),
                    "is_valid": False,
                    "detail": traceback.format_exc() if (hasattr(app, 'config') and app.config.get('DEBUG')) else None
                }), 500
    else:
        @app.route(f'{prefix}/validate-token', methods=['POST'])
        def go_invoice_validate_token():
            """Validate token - kiểm tra token có còn hợp lệ không (Sync version for Flask)"""
            try:
                if not request.is_json:
                    return jsonify({
                        "status": "error",
                        "error_code": "INVALID_REQUEST",
                        "message": "Request must be JSON",
                        "is_valid": False
                    }), 400
                
                data = get_request_json_sync()
                
                if not data:
                    return jsonify({
                        "status": "error",
                        "error_code": "INVALID_JSON",
                        "message": "Invalid JSON format",
                        "is_valid": False
                    }), 400
                
                auth_header = data.get("Authorization")
                proxy_url = data.get("proxy")
                
                if not auth_header:
                    return jsonify({
                        "status": "error",
                        "error_code": "MISSING_AUTHORIZATION",
                        "message": "Missing Authorization header",
                        "is_valid": False
                    }), 400
                
                # Extract token từ Authorization header
                token = auth_header.replace("Bearer ", "").strip() if auth_header else None
                if not token:
                    return jsonify({
                        "status": "error",
                        "error_code": "INVALID_AUTHORIZATION_FORMAT",
                        "message": "Invalid Authorization format (should be 'Bearer token')",
                        "is_valid": False
                    }), 400
                
                # Basic format validation
                if not token or len(token) < 10:
                    return jsonify({
                        "status": "error",
                        "error_code": "TOKEN_INVALID",
                        "message": "Token format is invalid",
                        "is_valid": False
                    }), 401
                
                # Token có format hợp lệ
                return jsonify({
                    "status": "success",
                    "is_valid": True,
                    "message": "Token format is valid"
                })
                
            except Exception as e:
                return jsonify({
                    "status": "error",
                    "error_code": "VALIDATE_TOKEN_ERROR",
                    "message": str(e),
                    "is_valid": False,
                    "detail": traceback.format_exc() if (hasattr(app, 'config') and app.config.get('DEBUG')) else None
                }), 500
    
    @app.route(f'{prefix}/tongquat', methods=['POST'])
    def go_invoice_tongquat():
        """
        Xuất tổng quát
        
        Request JSON:
        {
            "Authorization": "Bearer token...",
            "type_invoice": 1 or 2,  # 1: bán ra, 2: mua vào
            "start_date": "DD/MM/YYYY",
            "end_date": "DD/MM/YYYY",
            "proxy": "http://proxy:port" or null (optional)
        }
        
        Returns: Excel file in base64 + metadata + token (để tracking progress)
        """
        try:
            if not request.is_json:
                return jsonify({
                    "status": "error",
                    "message": "Request must be JSON"
                }), 400
            
            data = get_request_json_sync()
            auth_header = data.get("Authorization")
            type_invoice = data.get("type_invoice", 1)
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            proxy_url = data.get("proxy")  # ✅ Rename to proxy_url
            
            if not auth_header:
                return jsonify({
                    "status": "error",
                    "message": "Missing Authorization (need to login first)"
                }), 400
            
            # Extract token từ Authorization header (Bearer token)
            token = auth_header.replace("Bearer ", "").strip() if auth_header else None
            if not token:
                return jsonify({
                    "status": "error",
                    "message": "Invalid Authorization format (should be 'Bearer token')"
                }), 400
            
            # Convert Authorization to headers format for backend
            headers = {
                "status": "success",
                "Authorization": auth_header
            }
            
            # ✅ Tạo backend với proxy nếu có
            backend = get_invoice_backend(proxy_url=proxy_url)
            
            # Sử dụng token làm identifier cho tracking progress
            tracker = ProgressTracker.get_or_create(token)
            tracker.update(current_step="Preparing tongquat request...")
            
            # Định nghĩa progress callback cho backend
            def progress_callback(current_step, processed, total):
                """Callback từ backend để báo tiến trình"""
                tracker.update(
                    current_step=current_step,
                    processed=processed,
                    total=total
                )
            
            task = {
                "headers": headers,
                "type_invoice": type_invoice,
                "start_date": start_date,
                "end_date": end_date,
                "progress_callback": progress_callback  # Truyền callback vào task
            }
            
            try:
                # Gọi tongquat (progress tracking sẽ được thực hiện trong backend)
                tracker.update(current_step="Processing invoices (tongquat)...")
                result = backend.call_tongquat(task)
                
                # ✅ Kiểm tra result có phải là dict không trước khi gọi .get()
                if not isinstance(result, dict):
                    error_msg = f"Unexpected result type: {type(result).__name__}. Expected dict."
                    if isinstance(result, str):
                        error_msg = result
                    tracker.fail(error_msg)
                    return jsonify({
                        "status": "error",
                        "message": error_msg
                    })
                
                # Thêm token vào response thay vì request_id
                result['token'] = token
                
                # Đánh dấu hoàn thành
                if result.get('status') == 'success':
                    tracker.complete(result)
                else:
                    error_msg = result.get('message', 'Unknown error')
                    tracker.fail(error_msg)
                
                return jsonify(result)
            except Exception as e:
                error_msg = str(e)
                tracker.fail(error_msg)
                raise
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    # ========== Queue-based endpoint cho Worker ==========
    # Import Redis client ở đầu để dùng cho tất cả queue endpoints
    import sys as _sys
    import os as _os
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
    from shared.redis_client import get_redis_client, publish_progress
    
    # ✅ Import asyncio để dùng create_task
    import asyncio
    
    if is_async:
        @app.route(f'{prefix}/tongquat/queue', methods=['POST'])
        async def go_invoice_tongquat_queue():
            """
            Xuất tổng quát với Queue - được gọi từ Worker (Async version)
            ✅ Trả về "accepted" ngay lập tức, xử lý trong background (giống Go Soft)
            """
            try:
                data = await request.get_json()
                if not data:
                    return jsonify({"status": "error", "message": "Request must be JSON"}), 400
                
                job_id = data.get("job_id")
                auth_header = data.get("Authorization")
                type_invoice = data.get("type_invoice", 2)
                start_date = data.get("start_date")
                end_date = data.get("end_date")
                proxy_url = data.get("proxy")
                
                if not job_id:
                    return jsonify({"status": "error", "message": "Missing job_id"}), 400
                
                if not auth_header:
                    return jsonify({"status": "error", "error_code": "MISSING_AUTHORIZATION", "message": "Missing Authorization"}), 400
                
                # ✅ Validation: Kiểm tra ngày không vượt quá ngày hiện tại
                if start_date and end_date:
                    is_valid, error_msg = validate_date_range(start_date, end_date)
                    if not is_valid:
                        publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                        redis_client = get_redis_client()
                        redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                        redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                        return jsonify({
                            "status": "error",
                            "message": error_msg
                        }), 400
                
                token = auth_header.replace("Bearer ", "").strip() if auth_header else None
                if not token:
                    return jsonify({"status": "error", "error_code": "INVALID_AUTHORIZATION_FORMAT", "message": "Invalid Authorization format"}), 400
                
                headers = {"status": "success", "Authorization": auth_header}
                redis_client = get_redis_client()
                
                # ✅ Định nghĩa async function xử lý trong background
                async def process_tongquat():
                    try:
                        publish_progress(job_id, 0, "Bắt đầu đồng bộ hóa đơn...")
                        backend = get_invoice_backend(proxy_url=proxy_url, job_id=job_id)
                        
                        def progress_callback(current_step, processed, total):
                            # Check cancelled flag trong progress callback
                            try:
                                cancelled = redis_client.get(f"job:{job_id}:cancelled")
                                if cancelled:
                                    cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                                    if cancelled == '1':
                                        raise Exception("Job đã bị hủy (Ctrl+C)")
                            except:
                                pass
                            
                            percent = int((processed / total * 100)) if total > 0 else 0
                            publish_progress(job_id, percent, current_step, {'processed': processed, 'total': total})
                        
                        task = {
                            "headers": headers,
                            "type_invoice": type_invoice,
                            "start_date": start_date,
                            "end_date": end_date,
                            "progress_callback": progress_callback
                        }
                        
                        # Check cancelled flag trước khi bắt đầu xử lý
                        cancelled = redis_client.get(f"job:{job_id}:cancelled")
                        if cancelled:
                            cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                            if cancelled == '1':
                                raise Exception("Job đã bị hủy (Ctrl+C)")
                        
                        publish_progress(job_id, 10, "Đang kết nối đến hệ thống hóa đơn...")
                        
                        # ✅ Wrap sync function trong asyncio.to_thread() để không block event loop (giống Go Soft pattern)
                        result = await asyncio.to_thread(backend.call_tongquat, task)
                        
                        # ✅ Kiểm tra result có phải là dict không trước khi gọi .get()
                        if not isinstance(result, dict):
                            error_msg = f"Unexpected result type: {type(result).__name__}. Expected dict."
                            if isinstance(result, str):
                                error_msg = result
                            logger.error(f"[Job {job_id}] {error_msg}")
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}", {'type': 'error', 'error': error_msg, 'error_code': 'INVALID_RESULT_TYPE'})
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                        elif result.get('status') == 'success':
                            publish_progress(job_id, 100, "Hoàn thành!")
                            
                            # ✅ Extract download_id từ nested data structure (giống Go-Soft pattern)
                            data_obj = result.get('data', {})
                            download_id = data_obj.get('download_id', '') if isinstance(data_obj, dict) else ''
                            excel_filename = data_obj.get('filename', 'invoices.xlsx') if isinstance(data_obj, dict) else 'invoices.xlsx'
                            
                            # ✅ Backward compatibility: vẫn có excel_base64 nếu không lưu được vào disk
                            excel_base64 = data_obj.get('excel_bytes', '') if isinstance(data_obj, dict) else ''
                            
                            result_data = {
                                'status': 'success',
                                'total': result.get('total', data_obj.get('total_records', 0) if isinstance(data_obj, dict) else 0),
                                # ✅ Cần có datas để client gửi cho bước tải XML/HTML/PDF
                                'datas': result.get('datas', []),
                                'download_id': download_id,
                                'excel_filename': excel_filename,
                                'excel_base64': excel_base64 if not download_id else None,
                            }
                            
                            redis_client.set(f"job:{job_id}:result", json.dumps(result_data, ensure_ascii=False).encode('utf-8'))
                            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                        else:
                            error_msg = result.get('message', 'Unknown error') if isinstance(result, dict) else str(result)
                            error_code = result.get('error_code', 'TONGQUAT_ERROR') if isinstance(result, dict) else 'TONGQUAT_ERROR'
                            
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}", {'type': 'error', 'error': error_msg, 'error_code': error_code})
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            
                    except Exception as e:
                        error_msg = str(e)
                        # Check if job was cancelled
                        if "Job đã bị hủy" in error_msg or "cancelled" in error_msg.lower():
                            # ✅ Client message: thân thiện với người dùng
                            publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                            # ✅ Log: giữ thuật ngữ kỹ thuật
                            logger.info(f"[Job {job_id}] Job đã bị hủy (Ctrl+C hoặc client disconnect)")
                            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", "Yêu cầu đã bị hủy".encode('utf-8'))
                        else:
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                
                # ✅ Chạy xử lý trong background và trả về "accepted" ngay (giống Go Soft)
                asyncio.create_task(process_tongquat())
                
                return jsonify({
                    "status": "accepted",
                    "job_id": job_id,
                    "message": "Tongquat đã được bắt đầu, events sẽ được publish vào Redis"
                })
                
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

        @app.route(f'{prefix}/chitiet/queue', methods=['POST'])
        async def go_invoice_chitiet_queue():
            try:
                data = await request.get_json()
                if not data:
                    return jsonify({"status": "error", "message": "Request must be JSON"}), 400
                job_id = data.get("job_id")
                auth_header = data.get("Authorization")
                type_invoice = data.get("type_invoice", 2)
                start_date = data.get("start_date")
                end_date = data.get("end_date")
                proxy_url = data.get("proxy")
                if not job_id:
                    return jsonify({"status": "error", "message": "Missing job_id"}), 400
                if not auth_header:
                    return jsonify({"status": "error", "error_code": "MISSING_AUTHORIZATION", "message": "Missing Authorization"}), 400
                if start_date and end_date:
                    is_valid, error_msg = validate_date_range(start_date, end_date)
                    if not is_valid:
                        publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                        redis_client = get_redis_client()
                        redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                        redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                        return jsonify({"status": "error", "message": error_msg}), 400
                token = auth_header.replace("Bearer ", "").strip() if auth_header else None
                if not token:
                    return jsonify({"status": "error", "error_code": "INVALID_AUTHORIZATION_FORMAT", "message": "Invalid Authorization format"}), 400
                headers = {"status": "success", "Authorization": auth_header}
                redis_client = get_redis_client()

                async def process_chitiet():
                    try:
                        publish_progress(job_id, 0, "Bắt đầu đồng bộ hóa đơn (chi tiết)...")
                        backend = get_invoice_backend(proxy_url=proxy_url, job_id=job_id)

                        def progress_callback(current_step, processed, total):
                            try:
                                cancelled = redis_client.get(f"job:{job_id}:cancelled")
                                if cancelled:
                                    cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                                    if cancelled == '1':
                                        raise Exception("Job đã bị hủy (Ctrl+C)")
                            except:
                                pass
                            percent = int((processed / total * 100)) if total > 0 else 0
                            publish_progress(job_id, percent, current_step, {'processed': processed, 'total': total})

                        task = {
                            "headers": headers,
                            "type_invoice": type_invoice,
                            "start_date": start_date,
                            "end_date": end_date,
                            "progress_callback": progress_callback
                        }
                        cancelled = redis_client.get(f"job:{job_id}:cancelled")
                        if cancelled:
                            cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                            if cancelled == '1':
                                raise Exception("Job đã bị hủy (Ctrl+C)")
                        publish_progress(job_id, 10, "Đang kết nối đến hệ thống hóa đơn...")
                        tongquat_result = await asyncio.to_thread(backend.call_tongquat, task)
                        if not isinstance(tongquat_result, dict):
                            error_msg = f"Unexpected result type: {type(tongquat_result).__name__}." if not isinstance(tongquat_result, str) else tongquat_result
                            logger.error(f"[Job {job_id}] {error_msg}")
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}", {'type': 'error', 'error': error_msg})
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            return
                        if tongquat_result.get("status") != "success":
                            error_msg = tongquat_result.get("message", "Tongquat failed")
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            return
                        publish_progress(job_id, 50, "Đang xuất chi tiết...")
                        chitiet_input = {**tongquat_result, "headers": headers, "progress_callback": progress_callback}
                        chitiet_result = await asyncio.to_thread(backend.call_chitiet, chitiet_input)
                        if not isinstance(chitiet_result, dict):
                            error_msg = str(chitiet_result) if isinstance(chitiet_result, str) else f"Unexpected result type: {type(chitiet_result).__name__}"
                            logger.error(f"[Job {job_id}] {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            return
                        if chitiet_result.get('status') == 'success':
                            publish_progress(job_id, 100, "Hoàn thành!")
                            data_obj = chitiet_result.get('data', {}) or {}
                            download_id = data_obj.get('download_id', '') if isinstance(data_obj, dict) else ''
                            excel_filename = data_obj.get('filename', 'Chi_tiet_hoa_don.xlsx') if isinstance(data_obj, dict) else 'Chi_tiet_hoa_don.xlsx'
                            excel_base64 = data_obj.get('excel_bytes', '') if isinstance(data_obj, dict) else ''
                            result_data = {
                                'status': 'success',
                                'total': chitiet_result.get('total', data_obj.get('total_records', 0) if isinstance(data_obj, dict) else 0),
                                'datas': tongquat_result.get('datas', []),
                                'download_id': download_id,
                                'excel_filename': excel_filename,
                                'excel_base64': excel_base64 if not download_id else None,
                            }
                            redis_client.set(f"job:{job_id}:result", json.dumps(result_data, ensure_ascii=False).encode('utf-8'))
                            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                        else:
                            error_msg = chitiet_result.get('message', 'Chi tiết extraction failed')
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                    except Exception as e:
                        error_msg = str(e)
                        if "Job đã bị hủy" in error_msg or "cancelled" in error_msg.lower():
                            publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                            logger.info(f"[Job {job_id}] Job đã bị hủy (Ctrl+C hoặc client disconnect)")
                            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", "Yêu cầu đã bị hủy".encode('utf-8'))
                        else:
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))

                asyncio.create_task(process_chitiet())
                return jsonify({
                    "status": "accepted",
                    "job_id": job_id,
                    "message": "Chitiet đã được bắt đầu, events sẽ được publish vào Redis"
                })
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500

    else:
        @app.route(f'{prefix}/tongquat/queue', methods=['POST'])
        def go_invoice_tongquat_queue():
            """Xuất tổng quát với Queue - Sync version"""
            try:
                if not request.is_json:
                    return jsonify({"status": "error", "message": "Request must be JSON"}), 400
                data = get_request_json_sync()
                # Sync mode not fully tested, use async mode
                return jsonify({"status": "error", "message": "Sync mode not fully implemented, use async mode"}), 500
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500
    
    # ========== XMLHTML Queue ==========
    if is_async:
        @app.route(f'{prefix}/xmlhtml/queue', methods=['POST'])
        async def go_invoice_xmlhtml_queue():
            """
            Xuất XML/HTML với Queue - Async version
            ✅ Trả về "accepted" ngay lập tức, xử lý trong background (giống Go Soft)
            """
            try:
                data = await request.get_json()
                if not data:
                    return jsonify({"status": "error", "message": "Request must be JSON"}), 400
                
                job_id = data.get("job_id")
                auth_header = data.get("Authorization")
                type_invoice = data.get("type_invoice", 2)
                start_date = data.get("start_date")
                end_date = data.get("end_date")
                datas = data.get("datas", [])
                options = data.get("options", {"xml": True, "html": True})
                proxy_url = data.get("proxy")
                
                if not job_id:
                    return jsonify({"status": "error", "message": "Missing job_id"}), 400
                if not auth_header:
                    return jsonify({"status": "error", "error_code": "MISSING_AUTHORIZATION", "message": "Missing Authorization"}), 400
                if not datas:
                    return jsonify({"status": "error", "message": "Missing datas from tongquat"}), 400
                
                token = auth_header.replace("Bearer ", "").strip() if auth_header else None
                if not token:
                    return jsonify({"status": "error", "error_code": "INVALID_AUTHORIZATION_FORMAT", "message": "Invalid Authorization format"}), 400
                
                headers = {"status": "success", "Authorization": auth_header}
                redis_client = get_redis_client()
                
                # ✅ Định nghĩa async function xử lý trong background
                async def process_xmlhtml():
                    try:
                        # ✅ Xác định message dựa trên options
                        if options.get("xml") == True and options.get("html") == True:
                            action_name = "XML/HTML"
                        elif options.get("xml") == True:
                            action_name = "XML"
                        elif options.get("html") == True:
                            action_name = "HTML"
                        else:
                            action_name = "XML/HTML"
                        
                        publish_progress(job_id, 0, f"Bắt đầu xuất {action_name}...")
                        backend = get_invoice_backend(proxy_url=proxy_url, job_id=job_id)
                        
                        def progress_callback(current_step, processed, total):
                            # Check cancelled flag trong progress callback
                            try:
                                cancelled = redis_client.get(f"job:{job_id}:cancelled")
                                if cancelled:
                                    cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                                    if cancelled == '1':
                                        raise Exception("Job đã bị hủy (Ctrl+C)")
                            except:
                                pass
                            
                            # Tính progress từ 40% đến 100%
                            base_percent = 40
                            processing_percent = int(60 * (processed / total)) if total > 0 else 0
                            percent = base_percent + processing_percent
                            publish_progress(job_id, percent, current_step, {'processed': processed, 'total': total})
                        
                        # Check cancelled flag trước khi bắt đầu xử lý
                        cancelled = redis_client.get(f"job:{job_id}:cancelled")
                        if cancelled:
                            cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                            if cancelled == '1':
                                raise Exception("Job đã bị hủy (Ctrl+C)")
                        
                        publish_progress(job_id, 20, "Đang chuẩn bị dữ liệu...")
                        
                        tongquat_result = {
                            "status": "success", "headers": headers, "type_invoice": type_invoice,
                            "start_date": start_date, "end_date": end_date, "datas": datas,
                            "progress_callback": progress_callback
                        }
                        
                        publish_progress(job_id, 40, f"Đang xuất {action_name}...")
                        
                        # ✅ Wrap sync function trong asyncio.to_thread() để không block event loop
                        xmlhtml_result = await asyncio.to_thread(backend.call_xmlahtml, tongquat_result, options)
                        
                        # ✅ Kiểm tra xmlhtml_result có phải là dict không trước khi gọi .get()
                        if not isinstance(xmlhtml_result, dict):
                            error_msg = f"Unexpected result type: {type(xmlhtml_result).__name__}. Expected dict."
                            if isinstance(xmlhtml_result, str):
                                error_msg = xmlhtml_result
                            logger.error(f"[Job {job_id}] {error_msg}")
                            publish_progress(job_id, 0, f"Lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                        elif xmlhtml_result.get('status') == 'success':
                            publish_progress(job_id, 100, "Hoàn thành!")
                            
                            # ✅ Extract download_id từ nested data structure (giống Go-Soft pattern)
                            data_obj = xmlhtml_result.get('data', {})
                            
                            # ✅ Lấy riêng XML và HTML download_id
                            xml_download_id = data_obj.get('xml_download_id', '') if isinstance(data_obj, dict) else ''
                            html_download_id = data_obj.get('html_download_id', '') if isinstance(data_obj, dict) else ''
                            xml_filename = data_obj.get('xml_filename', 'invoices_xml.zip') if isinstance(data_obj, dict) else 'invoices_xml.zip'
                            html_filename = data_obj.get('html_filename', 'invoices_html.zip') if isinstance(data_obj, dict) else 'invoices_html.zip'
                            
                            # ✅ Combined download_id (nếu cả 2 đều có)
                            combined_download_id = data_obj.get('download_id', '') if isinstance(data_obj, dict) else ''
                            zip_filename = data_obj.get('zip_filename', 'invoices_xmlhtml.zip') if isinstance(data_obj, dict) else 'invoices_xmlhtml.zip'
                            
                            # ✅ Backward compatibility: vẫn có base64 nếu không lưu được vào disk
                            xml_base64 = data_obj.get('xml_base64', '') if isinstance(data_obj, dict) else ''
                            html_base64 = data_obj.get('html_base64', '') if isinstance(data_obj, dict) else ''
                            zip_base64 = data_obj.get('zip_bytes', '') if isinstance(data_obj, dict) else ''
                            
                            result_data = {
                                'status': 'success',
                                'total': data_obj.get('total_xml', 0) + data_obj.get('total_html', 0) if isinstance(data_obj, dict) else 0,
                                'total_xml': data_obj.get('total_xml', 0) if isinstance(data_obj, dict) else 0,
                                'total_html': data_obj.get('total_html', 0) if isinstance(data_obj, dict) else 0,
                                # ✅ Trả về riêng XML và HTML download_id
                                'xml_download_id': xml_download_id,
                                'html_download_id': html_download_id,
                                'xml_filename': xml_filename,
                                'html_filename': html_filename,
                                # ✅ Combined download_id (nếu cả 2 đều có)
                                'download_id': combined_download_id or xml_download_id or html_download_id,
                                'zip_filename': zip_filename,
                                # ✅ Backward compatibility: vẫn có base64 nếu không lưu được vào disk
                                'xml_base64': xml_base64 if not xml_download_id else None,
                                'html_base64': html_base64 if not html_download_id else None,
                                'zip_base64': zip_base64 if not combined_download_id else None,
                            }
                            
                            # ✅ Lưu vào Redis (chỉ lưu download_id, không lưu base64 lớn)
                            redis_client.set(f"job:{job_id}:result", json.dumps(result_data, ensure_ascii=False).encode('utf-8'))
                            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                        else:
                            error_msg = xmlhtml_result.get('message', 'Unknown error')
                            publish_progress(job_id, 0, f"Lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            
                    except Exception as e:
                        error_msg = str(e)
                        # Check if job was cancelled
                        if "Job đã bị hủy" in error_msg or "cancelled" in error_msg.lower():
                            # ✅ Client message: thân thiện với người dùng
                            publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                            # ✅ Log: giữ thuật ngữ kỹ thuật
                            logger.info(f"[Job {job_id}] Job đã bị hủy (Ctrl+C hoặc client disconnect)")
                            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", "Yêu cầu đã bị hủy".encode('utf-8'))
                        else:
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                
                # ✅ Chạy xử lý trong background và trả về "accepted" ngay (giống Go Soft)
                asyncio.create_task(process_xmlhtml())
                
                return jsonify({
                    "status": "accepted",
                    "job_id": job_id,
                    "message": "XML/HTML đã được bắt đầu, events sẽ được publish vào Redis"
                })
                
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500
    else:
        @app.route(f'{prefix}/xmlhtml/queue', methods=['POST'])
        def go_invoice_xmlhtml_queue():
            """Xuất XML/HTML với Queue - Sync version"""
            try:
                if not request.is_json:
                    return jsonify({"status": "error", "message": "Request must be JSON"}), 400
                data = get_request_json_sync()
                # ... sync implementation same as async but with get_request_json_sync()
                return jsonify({"status": "error", "message": "Sync mode not fully implemented"}), 500
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500
    
    # ========== PDF Queue ==========
    if is_async:
        @app.route(f'{prefix}/pdf/queue', methods=['POST'])
        async def go_invoice_pdf_queue():
            """
            Xuất PDF với Queue - Async version
            ✅ Trả về "accepted" ngay lập tức, xử lý trong background (giống Go Soft)
            """
            try:
                data = await request.get_json()
                if not data:
                    return jsonify({"status": "error", "message": "Request must be JSON"}), 400
                
                job_id = data.get("job_id")
                auth_header = data.get("Authorization")
                type_invoice = data.get("type_invoice", 2)
                start_date = data.get("start_date")
                end_date = data.get("end_date")
                datas = data.get("datas", [])
                proxy_url = data.get("proxy")
                
                if not job_id:
                    return jsonify({"status": "error", "message": "Missing job_id"}), 400
                if not auth_header:
                    return jsonify({"status": "error", "error_code": "MISSING_AUTHORIZATION", "message": "Missing Authorization"}), 400
                if not datas:
                    return jsonify({"status": "error", "message": "Missing datas from tongquat"}), 400
                
                token = auth_header.replace("Bearer ", "").strip() if auth_header else None
                if not token:
                    return jsonify({"status": "error", "error_code": "INVALID_AUTHORIZATION_FORMAT", "message": "Invalid Authorization format"}), 400
                
                headers = {"status": "success", "Authorization": auth_header}
                redis_client = get_redis_client()
                
                # ✅ Định nghĩa async function xử lý trong background
                async def process_pdf():
                    try:
                        publish_progress(job_id, 0, "Bắt đầu xuất PDF...")
                        backend = get_invoice_backend(proxy_url=proxy_url, job_id=job_id)
                        
                        def progress_callback(current_step, processed, total):
                            # Check cancelled flag trong progress callback
                            try:
                                cancelled = redis_client.get(f"job:{job_id}:cancelled")
                                if cancelled:
                                    cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                                    if cancelled == '1':
                                        raise Exception("Job đã bị hủy (Ctrl+C)")
                            except:
                                pass
                            
                            # Tính progress từ 40% đến 100%
                            base_percent = 40
                            processing_percent = int(60 * (processed / total)) if total > 0 else 0
                            percent = base_percent + processing_percent
                            publish_progress(job_id, percent, current_step, {'processed': processed, 'total': total})
                        
                        # Check cancelled flag trước khi bắt đầu xử lý
                        cancelled = redis_client.get(f"job:{job_id}:cancelled")
                        if cancelled:
                            cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                            if cancelled == '1':
                                raise Exception("Job đã bị hủy (Ctrl+C)")
                        
                        publish_progress(job_id, 20, "Đang chuẩn bị dữ liệu...")
                        
                        tongquat_result = {
                            "status": "success", "headers": headers, "type_invoice": type_invoice,
                            "start_date": start_date, "end_date": end_date, "datas": datas,
                            "progress_callback": progress_callback
                        }
                        
                        # ✅ PDF cần HTML trước, nên message rõ ràng hơn
                        publish_progress(job_id, 40, "Đang lấy HTML để chuyển đổi sang PDF...")
                        
                        # ✅ Wrap sync function trong asyncio.to_thread() để không block event loop
                        pdf_result = await asyncio.to_thread(backend.getpdf, tongquat_result)
                        
                        # ✅ Kiểm tra pdf_result có phải là dict không trước khi gọi .get()
                        if not isinstance(pdf_result, dict):
                            error_msg = f"Unexpected result type: {type(pdf_result).__name__}. Expected dict."
                            if isinstance(pdf_result, str):
                                error_msg = pdf_result
                            logger.error(f"[Job {job_id}] {error_msg}")
                            publish_progress(job_id, 0, f"Lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                        elif pdf_result.get('status') == 'success':
                            publish_progress(job_id, 100, "Hoàn thành!")
                            
                            # ✅ Extract download_id từ nested data structure (giống Go-Soft pattern)
                            data_obj = pdf_result.get('data', {})
                            download_id = data_obj.get('download_id', '') if isinstance(data_obj, dict) else ''
                            zip_filename = data_obj.get('filename', 'invoices_pdf.zip') if isinstance(data_obj, dict) else 'invoices_pdf.zip'
                            
                            # ✅ Backward compatibility: vẫn có zip_base64 nếu không lưu được vào disk
                            zip_base64 = data_obj.get('zip_bytes', '') if isinstance(data_obj, dict) else ''
                            
                            result_data = {
                                'status': 'success',
                                'total': data_obj.get('total_pdf', 0) if isinstance(data_obj, dict) else 0,
                                'download_id': download_id,  # ✅ Trả về download_id thay vì zip_base64
                                'zip_filename': zip_filename,
                                # ✅ Backward compatibility
                                'zip_base64': zip_base64 if not download_id else None,
                            }
                            redis_client.set(f"job:{job_id}:result", json.dumps(result_data, ensure_ascii=False).encode('utf-8'))
                            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                        else:
                            error_msg = pdf_result.get('message', 'Unknown error')
                            publish_progress(job_id, 0, f"Lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            
                    except Exception as e:
                        error_msg = str(e)
                        # Check if job was cancelled
                        if "Job đã bị hủy" in error_msg or "cancelled" in error_msg.lower():
                            # ✅ Client message: thân thiện với người dùng
                            publish_progress(job_id, 0, "Yêu cầu đã bị hủy")
                            # ✅ Log: giữ thuật ngữ kỹ thuật
                            logger.info(f"[Job {job_id}] Job đã bị hủy (Ctrl+C hoặc client disconnect)")
                            redis_client.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", "Yêu cầu đã bị hủy".encode('utf-8'))
                        else:
                            publish_progress(job_id, 0, f"Đã xảy ra lỗi: {error_msg}")
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                
                # ✅ Chạy xử lý trong background và trả về "accepted" ngay (giống Go Soft)
                asyncio.create_task(process_pdf())
                
                return jsonify({
                    "status": "accepted",
                    "job_id": job_id,
                    "message": "PDF đã được bắt đầu, events sẽ được publish vào Redis"
                })
                
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500
    else:
        @app.route(f'{prefix}/pdf/queue', methods=['POST'])
        def go_invoice_pdf_queue():
            """Xuất PDF với Queue - Sync version"""
            try:
                if not request.is_json:
                    return jsonify({"status": "error", "message": "Request must be JSON"}), 400
                data = get_request_json_sync()
                # ... sync implementation
                return jsonify({"status": "error", "message": "Sync mode not fully implemented"}), 500
            except Exception as e:
                return jsonify({"status": "error", "message": str(e)}), 500
    
    @app.route(f'{prefix}/chitiet', methods=['POST'])
    def go_invoice_chitiet():
        """
        Xuất chi tiết - TỰ ĐỘNG gọi tongquat trước
        
        Request JSON:
        {
            "Authorization": "Bearer token...",
            "type_invoice": 1 or 2,
            "start_date": "DD/MM/YYYY",
            "end_date": "DD/MM/YYYY",
            "proxy": "http://proxy:port" or null (optional)
        }
        
        Returns: Excel file in base64 + token (để tracking progress)
        """
        try:
            if not request.is_json:
                return jsonify({
                    "status": "error",
                    "message": "Request must be JSON"
                }), 400
            
            data = get_request_json_sync()
            auth_header = data.get("Authorization")
            type_invoice = data.get("type_invoice", 1)
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            proxy_url = data.get("proxy")  # ✅ Rename to proxy_url
            
            if not auth_header:
                return jsonify({
                    "status": "error",
                    "message": "Missing Authorization (need to login first)"
                }), 400
            
            # Extract token từ Authorization header (Bearer token)
            token = auth_header.replace("Bearer ", "").strip() if auth_header else None
            if not token:
                return jsonify({
                    "status": "error",
                    "message": "Invalid Authorization format (should be 'Bearer token')"
                }), 400
            
            if not all([start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "message": "Missing start_date or end_date"
                }), 400
            
            # ✅ Validation: Kiểm tra ngày không vượt quá ngày hiện tại
            is_valid, error_msg = validate_date_range(start_date, end_date)
            if not is_valid:
                return jsonify({
                    "status": "error",
                    "message": error_msg
                }), 400
            
            # Convert Authorization to headers format for backend
            headers = {
                "status": "success",
                "Authorization": auth_header
            }
            
            # ✅ Tạo backend với proxy nếu có
            backend = get_invoice_backend(proxy_url=proxy_url)
            
            # Sử dụng token làm identifier cho tracking progress
            tracker = ProgressTracker.get_or_create(token)
            tracker.update(current_step="Preparing chi tiết request...")
            
            try:
                # 1️⃣ Gọi tongquat tự động
                tracker.update(current_step="Calling tongquat (step 1/2)...")
                
                def progress_callback(current_step, processed, total):
                    """Callback từ backend để báo tiến trình"""
                    tracker.update(
                        current_step=current_step,
                        processed=processed,
                        total=total
                    )
                
                tongquat_task = {
                    "headers": headers,
                    "type_invoice": type_invoice,
                    "start_date": start_date,
                    "end_date": end_date,
                    "progress_callback": progress_callback
                }
                tongquat_result = backend.call_tongquat(tongquat_task)
                
                # ✅ Kiểm tra tongquat_result có phải là dict không
                if not isinstance(tongquat_result, dict):
                    error_msg = f"Unexpected result type: {type(tongquat_result).__name__}. Expected dict."
                    if isinstance(tongquat_result, str):
                        error_msg = tongquat_result
                    tracker.fail(error_msg)
                    return jsonify({
                        "status": "error",
                        "message": error_msg
                    })
                
                if tongquat_result.get("status") != "success":
                    tracker.fail(tongquat_result.get("message", "Tongquat failed"))
                    tongquat_result['token'] = token
                    return jsonify(tongquat_result)
                
                # 2️⃣ Gọi chitiet với kết quả từ tongquat
                tracker.update(current_step="Extracting chi tiết (step 2/2)...")
                tongquat_result['progress_callback'] = progress_callback
                chitiet_result = backend.call_chitiet(tongquat_result)
                
                # ✅ Kiểm tra chitiet_result có phải là dict không
                if not isinstance(chitiet_result, dict):
                    error_msg = f"Unexpected result type: {type(chitiet_result).__name__}. Expected dict."
                    if isinstance(chitiet_result, str):
                        error_msg = chitiet_result
                    tracker.fail(error_msg)
                    return jsonify({
                        "status": "error",
                        "message": error_msg
                    })
                
                # Thêm token vào response
                chitiet_result['token'] = token
                
                # Đánh dấu hoàn thành
                if chitiet_result.get('status') == 'success':
                    tracker.complete(chitiet_result)
                else:
                    tracker.fail(chitiet_result.get('message', 'Chi tiết extraction failed'))
                
                return jsonify(chitiet_result)
            except Exception as e:
                error_msg = str(e)
                tracker.fail(error_msg)
                raise
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    @app.route(f'{prefix}/xmlhtml', methods=['POST'])
    def go_invoice_xmlhtml():
        """
        Xuất XML + HTML - TỰ ĐỘNG gọi tongquat trước
        
        Request JSON:
        {
            "Authorization": "Bearer token...",
            "type_invoice": 1 or 2,
            "start_date": "DD/MM/YYYY",
            "end_date": "DD/MM/YYYY",
            "options": {
                "xml": true,
                "html": true
            },
            "proxy": "http://proxy:port" or null (optional)
        }
        
        Returns: ZIP file in base64 + token (để tracking progress)
        """
        try:
            if not request.is_json:
                return jsonify({
                    "status": "error",
                    "message": "Request must be JSON"
                }), 400
            
            data = get_request_json_sync()
            auth_header = data.get("Authorization")
            type_invoice = data.get("type_invoice", 1)
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            options = data.get("options", {"xml": True, "html": True})
            proxy_url = data.get("proxy")  # ✅ Rename to proxy_url
            
            if not auth_header:
                return jsonify({
                    "status": "error",
                    "message": "Missing Authorization (need to login first)"
                }), 400
            
            # Extract token từ Authorization header (Bearer token)
            token = auth_header.replace("Bearer ", "").strip() if auth_header else None
            if not token:
                return jsonify({
                    "status": "error",
                    "message": "Invalid Authorization format (should be 'Bearer token')"
                }), 400
            
            if not all([start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "message": "Missing start_date or end_date"
                }), 400
            
            # ✅ Validation: Kiểm tra ngày không vượt quá ngày hiện tại
            is_valid, error_msg = validate_date_range(start_date, end_date)
            if not is_valid:
                return jsonify({
                    "status": "error",
                    "message": error_msg
                }), 400
            
            # Convert Authorization to headers format for backend
            headers = {
                "status": "success",
                "Authorization": auth_header
            }
            
            # ✅ Tạo backend với proxy nếu có
            backend = get_invoice_backend(proxy_url=proxy_url)
            
            # Sử dụng token làm identifier cho tracking progress
            tracker = ProgressTracker.get_or_create(token)
            tracker.update(current_step="Preparing XML+HTML request...")
            
            try:
                # 1️⃣ Gọi tongquat tự động
                tracker.update(current_step="Calling tongquat (step 1/2)...")
                
                def progress_callback(current_step, processed, total):
                    """Callback từ backend để báo tiến trình"""
                    tracker.update(
                        current_step=current_step,
                        processed=processed,
                        total=total
                    )
                
                tongquat_task = {
                    "headers": headers,
                    "type_invoice": type_invoice,
                    "start_date": start_date,
                    "end_date": end_date,
                    "progress_callback": progress_callback
                }
                tongquat_result = backend.call_tongquat(tongquat_task)
                
                # ✅ Kiểm tra tongquat_result có phải là dict không
                if not isinstance(tongquat_result, dict):
                    error_msg = f"Unexpected result type: {type(tongquat_result).__name__}. Expected dict."
                    if isinstance(tongquat_result, str):
                        error_msg = tongquat_result
                    tracker.fail(error_msg)
                    return jsonify({
                        "status": "error",
                        "message": error_msg
                    })
                
                if tongquat_result.get("status") != "success":
                    tracker.fail(tongquat_result.get("message", "Tongquat failed"))
                    tongquat_result['token'] = token
                    return jsonify(tongquat_result)
                
                # 2️⃣ Thêm options vào tongquat result
                tongquat_result_with_options = {**tongquat_result}
                
                # 3️⃣ Gọi xmlhtml với kết quả từ tongquat
                tracker.update(current_step="Generating XML+HTML (step 2/2)...")
                tongquat_result_with_options['progress_callback'] = progress_callback
                xmlhtml_result = backend.call_xmlahtml(tongquat_result_with_options, options)
                
                # ✅ Kiểm tra xmlhtml_result có phải là dict không
                if not isinstance(xmlhtml_result, dict):
                    error_msg = f"Unexpected result type: {type(xmlhtml_result).__name__}. Expected dict."
                    if isinstance(xmlhtml_result, str):
                        error_msg = xmlhtml_result
                    tracker.fail(error_msg)
                    return jsonify({
                        "status": "error",
                        "message": error_msg
                    })
                
                # Thêm token vào response
                xmlhtml_result['token'] = token
                
                # Đánh dấu hoàn thành
                if xmlhtml_result.get('status') == 'success':
                    tracker.complete(xmlhtml_result)
                else:
                    tracker.fail(xmlhtml_result.get('message', 'XML+HTML generation failed'))
                
                # Remove xml_list and html_list (can be large) before JSON serialization
                # Keep only the base64-encoded zip in data.zip_bytes
                if 'xml_list' in xmlhtml_result:
                    del xmlhtml_result['xml_list']
                if 'html_list' in xmlhtml_result:
                    del xmlhtml_result['html_list']
                if 'datas' in xmlhtml_result:
                    del xmlhtml_result['datas']
                
                return jsonify(xmlhtml_result)
            except Exception as e:
                error_msg = str(e)
                tracker.fail(error_msg)
                raise
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    @app.route(f'{prefix}/pdf', methods=['POST'])
    def go_invoice_pdf():
        """
        Xuất PDF - TỰ ĐỘNG gọi tongquat trước
        
        Request JSON:
        {
            "Authorization": "Bearer token...",
            "type_invoice": 1 or 2,
            "start_date": "DD/MM/YYYY",
            "end_date": "DD/MM/YYYY",
            "proxy": "http://proxy:port" or null (optional)
        }
        
        Returns: ZIP file with PDFs in base64 + token (để tracking progress)
        """
        try:
            if not request.is_json:
                return jsonify({
                    "status": "error",
                    "message": "Request must be JSON"
                }), 400
            
            data = get_request_json_sync()
            auth_header = data.get("Authorization")
            type_invoice = data.get("type_invoice", 1)
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            proxy_url = data.get("proxy")  # ✅ Rename to proxy_url
            
            if not auth_header:
                return jsonify({
                    "status": "error",
                    "message": "Missing Authorization (need to login first)"
                }), 400
            
            # Extract token từ Authorization header (Bearer token)
            token = auth_header.replace("Bearer ", "").strip() if auth_header else None
            if not token:
                return jsonify({
                    "status": "error",
                    "message": "Invalid Authorization format (should be 'Bearer token')"
                }), 400
            
            if not all([start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "message": "Missing start_date or end_date"
                }), 400
            
            # ✅ Validation: Kiểm tra ngày không vượt quá ngày hiện tại
            is_valid, error_msg = validate_date_range(start_date, end_date)
            if not is_valid:
                return jsonify({
                    "status": "error",
                    "message": error_msg
                }), 400
            
            # Convert Authorization to headers format for backend
            headers = {
                "status": "success",
                "Authorization": auth_header
            }
            
            # ✅ Tạo backend với proxy nếu có
            backend = get_invoice_backend(proxy_url=proxy_url)
            
            # Sử dụng token làm identifier cho tracking progress
            tracker = ProgressTracker.get_or_create(token)
            tracker.update(current_step="Preparing PDF request...")
            
            try:
                # 1️⃣ Gọi tongquat tự động
                tracker.update(current_step="Calling tongquat (step 1/2)...")
                
                def progress_callback(current_step, processed, total):
                    """Callback từ backend để báo tiến trình"""
                    tracker.update(
                        current_step=current_step,
                        processed=processed,
                        total=total
                    )
                
                tongquat_task = {
                    "headers": headers,
                    "type_invoice": type_invoice,
                    "start_date": start_date,
                    "end_date": end_date,
                    "progress_callback": progress_callback
                }
                tongquat_result = backend.call_tongquat(tongquat_task)
                
                # ✅ Kiểm tra tongquat_result có phải là dict không
                if not isinstance(tongquat_result, dict):
                    error_msg = f"Unexpected result type: {type(tongquat_result).__name__}. Expected dict."
                    if isinstance(tongquat_result, str):
                        error_msg = tongquat_result
                    tracker.fail(error_msg)
                    return jsonify({
                        "status": "error",
                        "message": error_msg
                    })
                
                if tongquat_result.get("status") != "success":
                    tracker.fail(tongquat_result.get("message", "Tongquat failed"))
                    tongquat_result['token'] = token
                    return jsonify(tongquat_result)
                
                # 2️⃣ Gọi getpdf với kết quả từ tongquat
                tracker.update(current_step="Converting to PDF (step 2/2)...")
                tongquat_result['progress_callback'] = progress_callback
                pdf_result = backend.getpdf(tongquat_result)
                
                # ✅ Kiểm tra pdf_result có phải là dict không
                if not isinstance(pdf_result, dict):
                    error_msg = f"Unexpected result type: {type(pdf_result).__name__}. Expected dict."
                    if isinstance(pdf_result, str):
                        error_msg = pdf_result
                    tracker.fail(error_msg)
                    return jsonify({
                        "status": "error",
                        "message": error_msg
                    })
                
                # Thêm token vào response
                pdf_result['token'] = token
                
                # Đánh dấu hoàn thành
                if pdf_result.get('status') == 'success':
                    tracker.complete(pdf_result)
                else:
                    tracker.fail(pdf_result.get('message', 'PDF conversion failed'))
                
                # Remove pdf_list (contains bytes) before JSON serialization
                # Keep only the base64-encoded zip in data.zip_bytes
                if 'pdf_list' in pdf_result:
                    del pdf_result['pdf_list']
                if 'html_list' in pdf_result:
                    del pdf_result['html_list']
                
                return jsonify(pdf_result)
            except Exception as e:
                error_msg = str(e)
                tracker.fail(error_msg)
                raise
            
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e),
                "detail": traceback.format_exc() if app.config.get('DEBUG') else None
            }), 500
    
    # ==================== DOWNLOAD ENDPOINT (Streaming) ====================
    @app.route(f'{prefix}/download/<download_id>', methods=['GET'])
    async def download_file(download_id: str):
        """
        Download file từ disk storage (giống Go-Soft pattern)
        Stream file theo chunks để tránh load toàn bộ vào memory
        """
        try:
            from quart import request, Response
            import sys
            import os as os_module
            sys.path.insert(0, os_module.path.dirname(os_module.path.dirname(os_module.path.abspath(__file__))))
            from shared.download_service import get_file_path
            
            # Lấy file extension từ query param (default: zip)
            file_extension = request.args.get('ext', 'zip')
            
            # Lấy filename từ query param (optional)
            filename = request.args.get('filename', f'{download_id}.{file_extension}')
            
            # Đường dẫn file
            file_path = get_file_path(download_id, file_extension)
            
            logger.info(f"Download request for {download_id}, checking file: {file_path}")
            
            if not file_path or not os_module.path.exists(file_path):
                return jsonify({
                    "status": "error",
                    "message": f"File not found for download_id: {download_id}"
                }), 404
            
            # ✅ Streaming file để tránh load toàn bộ vào memory (quan trọng cho file lớn)
            file_size = os_module.path.getsize(file_path)
            logger.info(f"Sending file: {file_path} as {filename} (size: {file_size} bytes)")
            
            # Xác định MIME type
            mime_types = {
                'zip': 'application/zip',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'pdf': 'application/pdf'
            }
            mime_type = mime_types.get(file_extension, 'application/octet-stream')
            
            async def generate():
                """Generator để stream file theo chunk"""
                chunk_size = 8192  # 8KB chunks (giống Go-Soft)
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
            
            # Trả về streaming response
            response = Response(
                generate(),
                mimetype=mime_type,
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"',
                    'Content-Length': str(file_size)
                }
            )
            return response
            
        except Exception as e:
            logger.error(f"Error downloading file {download_id}: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500

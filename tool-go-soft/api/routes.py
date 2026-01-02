"""
Routes cho tool-go-soft (Tax Crawler API)
Đã migrate sang Quart (async) thay Flask để support async operations

Được gọi từ api_server.py chung
"""
import os
import sys
import json
import logging
import base64
from functools import wraps

# Thêm parent directory vào path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Lazy imports
_session_manager = None
_tax_crawler = None


def get_session_manager():
    global _session_manager
    if _session_manager is None:
        from services.session_manager import session_manager
        _session_manager = session_manager
    return _session_manager


def get_tax_crawler():
    global _tax_crawler
    if _tax_crawler is None:
        from services.tax_crawler import get_tax_crawler as gtc
        _tax_crawler = gtc()
    return _tax_crawler


def check_session_exists(session_id: str) -> tuple[bool, dict]:
    """
    Kiểm tra session có tồn tại không
    
    Returns:
        (exists, error_response): 
        - exists: True nếu session tồn tại, False nếu không
        - error_response: Dict error response nếu session không tồn tại, None nếu tồn tại
    """
    try:
        if not session_id:
            return False, {
                "status": "error",
                "error_code": "MISSING_SESSION_ID",
                "message": "Thiếu session_id. Vui lòng đăng nhập lại."
            }
        
        sm = get_session_manager()
        session = sm.get_session(session_id)
        
        if not session:
            return False, {
                "status": "error",
                "error_code": "SESSION_NOT_FOUND",
                "message": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."
            }
        
        return True, None
    except Exception as e:
        logger.error(f"Error in check_session_exists: {e}", exc_info=True)
        # Nếu có lỗi, trả về session not found để an toàn
        return False, {
            "status": "error",
            "error_code": "SESSION_NOT_FOUND",
            "message": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."
        }


async def check_session_before_crawl(session_id: str) -> tuple[bool, dict]:
    """
    ✅ Hàm check session chung cho tất cả các loại crawl (tờ khai, giấy nộp tiền, thông báo)
    Kiểm tra:
    1. Session có tồn tại không
    2. Session có hợp lệ không (JSESSIONID)
    
    Returns:
        (is_valid, error_response):
        - is_valid: True nếu session hợp lệ, False nếu không
        - error_response: Dict error response nếu session không hợp lệ, None nếu hợp lệ
    """
    try:
        # Bước 1: Check session exists
        session_exists, error_response = check_session_exists(session_id)
        if not session_exists:
            logger.warning(f"Session check failed (not exists): {session_id[:8]}... - {error_response.get('error_code')}")
            return False, error_response
        
        # Bước 2: Check session validity (JSESSIONID)
        sm = get_session_manager()
        session_validity = await sm.check_session_validity(session_id)
        if not session_validity.get("valid", False):
            error_code = session_validity.get("error_code", "SESSION_EXPIRED")
            error_message = session_validity.get("error", "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.")
            logger.warning(f"Session check failed (invalid): {session_id[:8]}... - {error_code}")
            return False, {
                "status": "error",
                "error_code": error_code,
                "message": error_message
            }
        
        logger.debug(f"Session check passed: {session_id[:8]}...")
        return True, None
    except Exception as e:
        logger.error(f"Error in check_session_before_crawl: {e}")
        # Nếu có lỗi khi check, trả về session expired để an toàn
        return False, {
            "status": "error",
            "error_code": "SESSION_EXPIRED",
            "message": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."
        }


def register_routes(app, prefix):
    """
    Đăng ký routes cho tool này
    
    Args:
        app: Quart app instance
        prefix: URL prefix (ví dụ: '/api/go-soft')
    """
    
    # Helper to check if app is Quart (async) or Flask (sync)
    is_async = hasattr(app, 'ensure_async')
    
    if is_async:
        from quart import request, jsonify, Response
        
        async def make_response(data, status=200):
            return jsonify(data), status
        
        async def stream_response(generator):
            async def generate():
                async for event in generator:
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            
            return Response(
                generate(),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'
                }
            )
    else:
        from flask import request, jsonify, Response, stream_with_context
        
        def make_response(data, status=200):
            return jsonify(data), status
        
        def stream_response(generator):
            def generate():
                for event in generator:
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            
            return Response(
                stream_with_context(generate()),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'
                }
            )
    
    # ==================== HEALTH CHECK ====================
    
    @app.route(f'{prefix}/health', methods=['GET'])
    async def go_soft_health_check():
        """Health check"""
        sm = get_session_manager()
        return jsonify({
            "status": "success",
            "message": "Tax Crawler API is running (Playwright + httpx async)",
            "version": "2.0",
            "active_sessions": sm.get_active_session_count()
        })
    
    # ==================== SESSION MANAGEMENT ====================
    
    @app.route(f'{prefix}/session/create', methods=['POST'])
    async def create_session():
        """
        Tạo session mới với Playwright
        Returns: session_id
        """
        try:
            from quart import request
            sm = get_session_manager()
            session_id = await sm.create_session()
            
            return jsonify({
                "status": "success",
                "session_id": session_id
            })
        except Exception as e:
            logger.error(f"Error creating session: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/session/close', methods=['POST'])
    async def close_session():
        """
        Đóng session
        Body: { "session_id": "..." }
        """
        try:
            from quart import request
            data = await request.get_json()
            session_id = data.get("session_id")
            
            if not session_id:
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_SESSION_ID",
                    "message": "Missing session_id"
                }), 400
            
            sm = get_session_manager()
            tc = get_tax_crawler()
            
            # Close httpx client too
            await tc.close_http_client(session_id)
            success = await sm.close_session(session_id)
            
            return jsonify({
                "status": "success" if success else "error",
                "message": "Session closed" if success else "Session not found"
            })
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/session/status', methods=['GET'])
    async def session_status():
        """
        Kiểm tra trạng thái session
        Query: session_id
        """
        from quart import request
        session_id = request.args.get("session_id")
        
        if not session_id:
            return jsonify({
                "status": "error",
                "message": "Missing session_id"
            }), 400
        
        sm = get_session_manager()
        session = sm.get_session(session_id)
        
        if not session:
            return jsonify({
                "status": "error",
                "error_code": "SESSION_NOT_FOUND",
                "message": "Session not found or expired"
            }), 404
        
        return jsonify({
            "status": "success",
            "session_id": session_id,
            "is_logged_in": session.is_logged_in,
            "username": session.username,
            "created_at": session.created_at.isoformat(),
            "last_active": session.last_active.isoformat()
        })
    
    # ==================== LOGIN FLOW ====================
    
    @app.route(f'{prefix}/login/init', methods=['POST'])
    async def init_login():
        """
        Khởi tạo trang login và lấy captcha
        Body: { "session_id": "..." }
        Returns: { captcha_base64: "..." }
        """
        try:
            from quart import request
            data = await request.get_json()
            session_id = data.get("session_id")
            
            if not session_id:
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_SESSION_ID",
                    "message": "Missing session_id"
                }), 400
            
            sm = get_session_manager()
            result = await sm.init_login_page(session_id)
            
            if result["success"]:
                return jsonify({
                    "status": "success",
                    "captcha_base64": result["captcha_base64"]
                })
            else:
                return jsonify({
                    "status": "error",
                    "message": result.get("error", "Unknown error")
                }), 400
                
        except Exception as e:
            logger.error(f"Error in init_login: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/login/submit', methods=['POST'])
    async def submit_login():
        """
        Submit login với username, password và captcha
        Body: {
            "session_id": "...",
            "username": "...",
            "password": "...",
            "captcha": "..."
        }
        """
        try:
            from quart import request
            data = await request.get_json()
            session_id = data.get("session_id")
            username = data.get("username")
            password = data.get("password")
            captcha = data.get("captcha")
            
            # Login mới không cần captcha nữa
            if not all([session_id, username, password]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields: session_id, username, password"
                }), 400
            
            sm = get_session_manager()
            # Login mới không cần captcha, gửi rỗng
            captcha = captcha or ""
            result = await sm.submit_login(session_id, username, password, captcha)
            
            if result["success"]:
                return jsonify({
                    "status": "success",
                    "message": "Login successful",
                    "dse_session_id": result.get("dse_session_id")
                })
            else:
                return jsonify({
                    "status": "error",
                    "message": result.get("error", "Login failed")
                }), 401
                
        except Exception as e:
            logger.error(f"Error in submit_login: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # ==================== CRAWL APIs ====================
    
    @app.route(f'{prefix}/tokhai/types', methods=['GET'])
    async def get_tokhai_types():
        """
        Lấy danh sách loại tờ khai
        Query: session_id
        """
        from quart import request
        session_id = request.args.get("session_id")
        
        if not session_id:
            return jsonify({
                "status": "error",
                "message": "Missing session_id"
            }), 400
        
        tc = get_tax_crawler()
        result = await tc.get_tokhai_types(session_id)
        
        if result["success"]:
            return jsonify({
                "status": "success",
                "tokhai_types": result["tokhai_types"]
            })
        else:
            return jsonify({
                "status": "error",
                "message": result.get("error", "Failed to get tokhai types")
            }), 400
    
    @app.route(f'{prefix}/crawl/tokhai', methods=['POST'])
    async def crawl_tokhai():
        """
        Crawl tờ khai (publish events to Redis)
        Body: {
            "job_id": "...",  # Job ID để publish events
            "session_id": "...",
            "tokhai_type": "842" hoặc "01/GTGT" hoặc "00" (Tất cả) hoặc null,
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        Returns: { "status": "accepted", "job_id": "..." }
        
        API sẽ publish events vào Redis, worker sẽ lắng nghe từ Redis
        Note: Nếu tokhai_type = "00", null, hoặc không có → crawl TẤT CẢ loại tờ khai
        """
        try:
            from quart import request
            import asyncio
            from shared.redis_client import publish_progress
            
            data = await request.get_json()
            job_id = data.get("job_id")
            session_id = data.get("session_id")
            tokhai_type = data.get("tokhai_type")  # Có thể là None, "00", hoặc giá trị cụ thể
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            if not all([job_id, session_id, start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields: job_id, session_id, start_date, end_date"
                }), 400
            
            # Nếu không có tokhai_type hoặc rỗng → mặc định là "Tất cả"
            if not tokhai_type or tokhai_type.strip() == "":
                tokhai_type = "00"
            
            # ✅ Check session trước khi crawl (dùng hàm chung)
            is_valid, error_response = await check_session_before_crawl(session_id)
            if not is_valid:
                return jsonify(error_response), 401
            
            # Chạy crawl trong background task và publish events vào Redis
            async def crawl_and_publish():
                try:
                    tc = get_tax_crawler()
                    results = []
                    total_count = 0
                    zip_filename = None
                    download_id = None
                    accumulated_total = 0
                    accumulated_downloaded = 0
                    
                    async for event in tc.crawl_tokhai(session_id, tokhai_type, start_date, end_date, job_id=job_id):
                        # ✅ Check cancelled trước khi xử lý event tiếp theo
                        from shared.redis_client import get_redis_client
                        check_redis = get_redis_client()
                        cancelled = check_redis.get(f"job:{job_id}:cancelled")
                        if cancelled:
                            cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                            if cancelled == '1':
                                logger.info(f"[API] Job {job_id} đã bị cancel, dừng crawl")
                                check_redis.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                                publish_progress(job_id, 0, "Job đã bị hủy")
                                break
                        
                        event_type = event.get('type', 'unknown')
                        
                        # ✅ Nếu event là error với JOB_CANCELLED, dừng ngay
                        if event_type == 'error' and event.get('error_code') == 'JOB_CANCELLED':
                            logger.info(f"[API] Job {job_id} đã bị cancel từ crawler")
                            check_redis.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                            publish_progress(job_id, 0, "Job đã bị hủy", event)
                            break
                        
                        if event_type == 'progress':
                            percent = event.get('percent', 0)
                            message = event.get('message', 'Đang xử lý...')
                            publish_progress(job_id, percent, message, event)
                            
                        elif event_type == 'info':
                            message = event.get('message', '')
                            # ✅ Forward accumulated_percent và các field khác từ event để không reset về 0%
                            percent = event.get('accumulated_percent', event.get('percent', 0))
                            if isinstance(percent, float):
                                percent = int(percent)
                            publish_progress(job_id, percent, message, event)
                            
                        elif event_type == 'special_items':
                            # ✅ Forward accumulated_percent và các field khác từ event để không reset về 0%
                            percent = event.get('accumulated_percent', event.get('percent', 0))
                            if isinstance(percent, float):
                                percent = int(percent)
                            message = event.get('message', '')
                            publish_progress(job_id, percent, message, event)
                            
                        elif event_type == 'download_start':
                            total = event.get('accumulated_total', event.get('total', 0))
                            accumulated_total = total
                            publish_progress(job_id, 0, f"Bắt đầu tải {total} file...", event)
                            
                        elif event_type == 'download_progress':
                            current = event.get('accumulated_downloaded', event.get('current', 0))
                            total = event.get('accumulated_total', event.get('total', 0))
                            accumulated_total = total
                            accumulated_downloaded = current
                            # ✅ Dùng % tích lũy từ event (không tính lại để tránh thụt lùi)
                            percent = event.get('accumulated_percent', event.get('percent', 0))
                            if isinstance(percent, float):
                                percent = int(percent)
                            
                            # ✅ Lấy thông tin tờ thuyết minh từ event
                            thuyet_minh_downloaded = event.get('thuyet_minh_downloaded', 0)
                            thuyet_minh_total = event.get('thuyet_minh_total', 0)
                            
                            # ✅ Tạo message với tờ thuyết minh nếu có
                            if thuyet_minh_total > 0:
                                message = f"Đã tải {current}/{total} file - {thuyet_minh_downloaded}/{thuyet_minh_total} tm"
                            else:
                                message = f"Đã tải {current}/{total} file"
                            
                            # ✅ Thêm TẤT CẢ thông tin cần thiết vào event data để frontend nhận được
                            event['accumulated_percent'] = percent
                            event['accumulated_total'] = accumulated_total
                            event['accumulated_downloaded'] = accumulated_downloaded
                            event['thuyet_minh_downloaded'] = thuyet_minh_downloaded
                            event['thuyet_minh_total'] = thuyet_minh_total
                            
                            # LOG: Kiểm tra event trước khi publish
                            logger.info(f"[API] download_progress event before publish: accumulated_percent={event.get('accumulated_percent')}, accumulated_total={event.get('accumulated_total')}, accumulated_downloaded={event.get('accumulated_downloaded')}, thuyet_minh_downloaded={event.get('thuyet_minh_downloaded')}, thuyet_minh_total={event.get('thuyet_minh_total')}")
                            
                            publish_progress(job_id, percent, message, event)
                            
                        elif event_type == 'item':
                            results.append(event.get('data'))
                            
                        elif event_type == 'complete':
                            # ✅ Số file đã tải = tờ khai + tờ thuyết minh (từ event complete)
                            total_from_event = event.get('total', 0)  # Đây là total_files_downloaded từ backend
                            download_id = event.get('download_id')
                            zip_filename = event.get('zip_filename')
                            
                            # ✅ LUÔN dùng total_from_event (số file đã tải), KHÔNG dùng accumulated_total (tổng tìm thấy)
                            total_count = total_from_event
                            
                            # Publish complete event
                            from shared.redis_client import get_redis_client
                            redis_client = get_redis_client()
                            
                            result_data = {
                                'total': total_count,  # ✅ Số file đã tải (tờ khai + tờ thuyết minh)
                                'zip_filename': zip_filename,
                                'has_zip': False,
                                'download_id': download_id,
                                # ✅ Forward thêm các field từ event để frontend có thể hiển thị chi tiết
                                'tokhai_downloaded': event.get('tokhai_downloaded'),
                                'tokhai_total': event.get('tokhai_total'),
                                'thuyet_minh_downloaded': event.get('thuyet_minh_downloaded'),
                                'thuyet_minh_total': event.get('thuyet_minh_total'),
                                'special_items_count': event.get('special_items_count'),
                                'message': event.get('message')
                            }
                            redis_client.set(f"job:{job_id}:result", json.dumps(result_data).encode('utf-8'))
                            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                            
                            publish_progress(job_id, 100, "Hoàn thành crawl", event)
                            logger.info(f"[API] Job {job_id} completed: {total_count} file (tokhai: {event.get('tokhai_downloaded', 0)}, thuyet_minh: {event.get('thuyet_minh_downloaded', 0)}), download_id: {download_id}")
                            
                        elif event_type == 'error':
                            error_msg = event.get('error', 'Lỗi không xác định')
                            from shared.redis_client import get_redis_client
                            redis_client = get_redis_client()
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            publish_progress(job_id, 0, f"Lỗi: {error_msg}")
                            logger.error(f"[API] Job {job_id} error: {error_msg}")
                            
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"[API] Error in crawl_and_publish for job {job_id}: {error_msg}")
                    from shared.redis_client import get_redis_client
                    redis_client = get_redis_client()
                    redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                    redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                    publish_progress(job_id, 0, f"Lỗi: {error_msg}")
            
            # Chạy crawl trong background
            asyncio.create_task(crawl_and_publish())
            
            return jsonify({
                "status": "accepted",
                "job_id": job_id,
                "message": "Crawl đã được bắt đầu, events sẽ được publish vào Redis"
            })
            
        except Exception as e:
            logger.error(f"Error in crawl_tokhai: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/crawl/thongbao', methods=['POST'])
    async def crawl_thongbao():
        """
        Crawl thông báo (publish events to Redis)
        Body: {
            "job_id": "...",  # Job ID để publish events
            "session_id": "...",
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        Returns: { "status": "accepted", "job_id": "..." }
        
        API sẽ publish events vào Redis, worker sẽ lắng nghe từ Redis
        """
        try:
            from quart import request
            import asyncio
            from shared.redis_client import publish_progress
            
            data = await request.get_json()
            job_id = data.get("job_id")
            session_id = data.get("session_id")
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            # ✅ job_id là required (giống tờ khai)
            if not all([job_id, session_id, start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields: job_id, session_id, start_date, end_date"
                }), 400
            
            # ✅ Check session trước khi crawl (dùng hàm chung)
            is_valid, error_response = await check_session_before_crawl(session_id)
            if not is_valid:
                return jsonify(error_response), 401
            
            # Chạy crawl trong background task và publish events vào Redis
            # Chạy crawl trong background task và publish events vào Redis
            async def crawl_and_publish():
                try:
                    tc = get_tax_crawler()
                    results = []
                    total_count = 0
                    zip_filename = None
                    download_id = None
                    accumulated_total = 0
                    accumulated_downloaded = 0
                    
                    async for event in tc.crawl_thongbao(session_id, start_date, end_date, job_id=job_id):
                        # ✅ Check cancelled trước khi xử lý event tiếp theo (giống tờ khai)
                        from shared.redis_client import get_redis_client
                        check_redis = get_redis_client()
                        cancelled = check_redis.get(f"job:{job_id}:cancelled")
                        if cancelled:
                            cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                            if cancelled == '1':
                                logger.info(f"[API] Job {job_id} đã bị cancel, dừng crawl")
                                check_redis.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                                publish_progress(job_id, 0, "Job đã bị hủy")
                                break
                        
                        event_type = event.get('type', 'unknown')
                        
                        # ✅ Nếu event là error với JOB_CANCELLED, dừng ngay
                        if event_type == 'error' and event.get('error_code') == 'JOB_CANCELLED':
                            logger.info(f"[API] Job {job_id} đã bị cancel từ crawler")
                            check_redis.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                            publish_progress(job_id, 0, "Job đã bị hủy", event)
                            break
                        
                        if event_type == 'progress':
                            percent = event.get('percent', 0)
                            accumulated_percent = event.get('accumulated_percent', percent)
                            message = event.get('message', 'Đang xử lý...')
                            logger.debug(f"📤 [ROUTES] [THONGBAO] Publish progress: {percent}% (accumulated: {accumulated_percent}%)")
                            publish_progress(job_id, accumulated_percent if accumulated_percent is not None else percent, message, event)
                            
                        elif event_type == 'info':
                            message = event.get('message', '')
                            accumulated_percent = event.get('accumulated_percent')
                            logger.debug(f"📤 [ROUTES] [THONGBAO] Publish info: {message}")
                            publish_progress(job_id, accumulated_percent if accumulated_percent is not None else 0, message, event)
                            
                        elif event_type == 'download_start':
                            total = event.get('accumulated_total', event.get('total', 0))
                            accumulated_total = total
                            accumulated_percent = event.get('accumulated_percent', 0)
                            range_index = event.get('range_index', '?')
                            total_ranges = event.get('total_ranges', '?')
                            date_range = event.get('date_range', '?')
                            logger.debug(f"📤 [ROUTES] [THONGBAO] Publish download_start: Range {range_index}/{total_ranges} ({date_range}), Total: {total}")
                            publish_progress(job_id, accumulated_percent if accumulated_percent is not None else 0, f"Bắt đầu tải {total} file...", event)
                            
                        elif event_type == 'download_progress':
                            current = event.get('accumulated_downloaded', event.get('current', 0))
                            total = event.get('accumulated_total', event.get('total', 0))
                            accumulated_total = total
                            accumulated_downloaded = current
                            accumulated_percent = event.get('accumulated_percent')
                            percent = accumulated_percent if accumulated_percent is not None else (int((current / total) * 100) if total > 0 else 0)
                            logger.debug(f"📤 [ROUTES] [THONGBAO] Publish download_progress: {current}/{total} files, Accumulated %: {accumulated_percent}%")
                            publish_progress(job_id, percent, f"Đã tải {current}/{total} file", event)
                            
                        elif event_type == 'item':
                            results.append(event.get('data'))
                            
                        elif event_type == 'complete':
                            total_from_event = event.get('total', 0)
                            download_id = event.get('download_id')
                            zip_filename = event.get('zip_filename')
                            
                            if accumulated_total > 0:
                                total_count = accumulated_total
                            else:
                                total_count = total_from_event
                            
                            from shared.redis_client import get_redis_client
                            redis_client = get_redis_client()
                            
                            result_data = {
                                'total': total_count,
                                'zip_filename': zip_filename,
                                'has_zip': False,
                                'download_id': download_id
                            }
                            redis_client.set(f"job:{job_id}:result", json.dumps(result_data).encode('utf-8'))
                            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                            
                            publish_progress(job_id, 100, "Hoàn thành crawl", event)
                            
                        elif event_type == 'error':
                            error_msg = event.get('error', 'Lỗi không xác định')
                            from shared.redis_client import get_redis_client
                            redis_client = get_redis_client()
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            publish_progress(job_id, 0, f"Lỗi: {error_msg}")
                            
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"[API] Lỗi trong quá trình crawl thông báo cho job {job_id}: {error_msg}")
                    from shared.redis_client import get_redis_client
                    redis_client = get_redis_client()
                    redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                    redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                    publish_progress(job_id, 0, f"Lỗi: {error_msg}")
            
            asyncio.create_task(crawl_and_publish())
            
            return jsonify({
                "status": "accepted",
                "job_id": job_id,
                "message": "Crawl đã được bắt đầu, events sẽ được publish vào Redis"
            })
            
        except Exception as e:
            logger.error(f"Error in crawl_thongbao: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/crawl/giaynoptien', methods=['POST'])
    async def crawl_giaynoptien():
        """
        Crawl giấy nộp tiền
        - Nếu có job_id: publish events to Redis (queue mode)
        - Nếu không có job_id: streaming response (SSE mode - backward compatible)
        Body: {
            "job_id": "...",  # Optional - nếu có thì dùng queue mode
            "session_id": "...",
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        """
        try:
            from quart import request, Response
            import asyncio
            from shared.redis_client import publish_progress
            
            data = await request.get_json()
            job_id = data.get("job_id")
            session_id = data.get("session_id")
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            # Validate required fields (job_id is optional)
            if not all([session_id, start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields: session_id, start_date, end_date"
                }), 400
            
            # ✅ Check session exists (nếu backend restart, session sẽ không tồn tại)
            session_exists, error_response = check_session_exists(session_id)
            if not session_exists:
                # Trả về 401 (Unauthorized) thay vì 404 để frontend biết cần login lại
                return jsonify(error_response), 401
            
            # ✅ Check session validity trước khi bắt đầu crawl (giống như check trong login)
            # Check JSESSIONID hiện tại so với JSESSIONID đã lưu
            sm = get_session_manager()
            session_validity = await sm.check_session_validity(session_id)
            if not session_validity.get("valid", False):
                error_code = session_validity.get("error_code", "SESSION_EXPIRED")
                error_message = session_validity.get("error", "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.")
                return jsonify({
                    "status": "error",
                    "error_code": error_code,
                    "message": error_message
                }), 401
            
            # Nếu không có job_id → dùng streaming mode (backward compatible)
            if not job_id:
                tc = get_tax_crawler()
                
                async def generate():
                    async for event in tc.crawl_giay_nop_tien(session_id, start_date, end_date, job_id=None):
                        yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                
                return Response(
                    generate(),
                    mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache',
                        'X-Accel-Buffering': 'no'
                    }
                )
            
            # Nếu có job_id → dùng queue mode (publish to Redis)
            # Chạy crawl trong background task và publish events vào Redis
            async def crawl_and_publish():
                try:
                    tc = get_tax_crawler()
                    results = []
                    total_count = 0
                    zip_filename = None
                    download_id = None
                    accumulated_total = 0
                    accumulated_downloaded = 0
                    
                    async for event in tc.crawl_giay_nop_tien(session_id, start_date, end_date):
                        # ✅ Check cancelled trước khi xử lý event tiếp theo
                        from shared.redis_client import get_redis_client
                        check_redis = get_redis_client()
                        cancelled = check_redis.get(f"job:{job_id}:cancelled")
                        if cancelled:
                            cancelled = cancelled.decode('utf-8') if isinstance(cancelled, bytes) else str(cancelled).strip()
                            if cancelled == '1':
                                logger.info(f"[API] Job {job_id} đã bị cancel, dừng crawl")
                                check_redis.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                                publish_progress(job_id, 0, "Job đã bị hủy")
                                break
                        
                        event_type = event.get('type', 'unknown')
                        
                        # ✅ Nếu event là error với JOB_CANCELLED, dừng ngay
                        if event_type == 'error' and event.get('error_code') == 'JOB_CANCELLED':
                            logger.info(f"[API] Job {job_id} đã bị cancel từ crawler")
                            check_redis.set(f"job:{job_id}:status", "cancelled".encode('utf-8'))
                            publish_progress(job_id, 0, "Job đã bị hủy", event)
                            break
                        
                        if event_type == 'progress':
                            # ✅ Forward accumulated_percent và các field khác từ event để không reset về 0%
                            percent = event.get('accumulated_percent', event.get('percent', 0))
                            if isinstance(percent, float):
                                percent = int(percent)
                            message = event.get('message', 'Đang xử lý...')
                            publish_progress(job_id, percent, message, event)
                            
                        elif event_type == 'info':
                            message = event.get('message', '')
                            # ✅ Forward accumulated_percent và các field khác từ event để không reset về 0%
                            percent = event.get('accumulated_percent', event.get('percent', 0))
                            if isinstance(percent, float):
                                percent = int(percent)
                            publish_progress(job_id, percent, message, event)
                            
                        elif event_type == 'download_start':
                            total = event.get('accumulated_total', event.get('total', 0))
                            accumulated_total = total
                            publish_progress(job_id, 0, f"Bắt đầu tải {total} file...", event)
                            
                        elif event_type == 'download_progress':
                            current = event.get('accumulated_downloaded', event.get('current', 0))
                            total = event.get('accumulated_total', event.get('total', 0))
                            accumulated_total = total
                            accumulated_downloaded = current
                            percent = int((current / total) * 100) if total > 0 else 0
                            publish_progress(job_id, percent, f"Đã tải {current}/{total} file", event)
                            
                        elif event_type == 'item':
                            results.append(event.get('data'))
                            
                        elif event_type == 'complete':
                            total_from_event = event.get('total', 0)
                            download_id = event.get('download_id')
                            zip_filename = event.get('zip_filename')
                            
                            if accumulated_total > 0:
                                total_count = accumulated_total
                            else:
                                total_count = total_from_event
                            
                            from shared.redis_client import get_redis_client
                            redis_client = get_redis_client()
                            
                            result_data = {
                                'total': total_count,
                                'zip_filename': zip_filename,
                                'has_zip': False,
                                'download_id': download_id
                            }
                            redis_client.set(f"job:{job_id}:result", json.dumps(result_data).encode('utf-8'))
                            redis_client.set(f"job:{job_id}:status", "completed".encode('utf-8'))
                            
                            publish_progress(job_id, 100, "Hoàn thành crawl", event)
                            
                        elif event_type == 'error':
                            error_msg = event.get('error', 'Lỗi không xác định')
                            from shared.redis_client import get_redis_client
                            redis_client = get_redis_client()
                            redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                            redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                            publish_progress(job_id, 0, f"Lỗi: {error_msg}")
                
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"[API] Lỗi trong quá trình crawl giấy nộp tiền cho job {job_id}: {error_msg}")
                    from shared.redis_client import get_redis_client
                    redis_client = get_redis_client()
                    redis_client.set(f"job:{job_id}:status", "failed".encode('utf-8'))
                    redis_client.set(f"job:{job_id}:error", error_msg.encode('utf-8'))
                    publish_progress(job_id, 0, f"Lỗi: {error_msg}")
            
            asyncio.create_task(crawl_and_publish())
            
            return jsonify({
                "status": "accepted",
                "job_id": job_id,
                "message": "Crawl đã được bắt đầu, events sẽ được publish vào Redis"
            })
            
        except Exception as e:
            logger.error(f"Error in crawl_giaynoptien: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/download/<download_id>', methods=['GET'])
    async def download_zip(download_id: str):
        """
        Download zip file từ disk storage
        Worker sẽ gọi endpoint này để download zip file
        """
        try:
            from quart import request, Response
            tc = get_tax_crawler()
            
            # Lấy filename từ query param (optional)
            filename = request.args.get('filename', f'{download_id}.zip')
            
            # Đường dẫn file
            zip_file_path = os.path.join(tc.ZIP_STORAGE_DIR, f"{download_id}.zip")
            
            logger.info(f"Download request for {download_id}, checking file: {zip_file_path}")
            
            if not os.path.exists(zip_file_path):
                return jsonify({
                    "status": "error",
                    "message": f"File not found for download_id: {download_id}"
                }), 404
            
            # ✅ Streaming file để tránh load toàn bộ vào memory (quan trọng cho file lớn)
            file_size = os.path.getsize(zip_file_path)
            logger.info(f"Sending file: {zip_file_path} as {filename} (size: {file_size} bytes)")
            
            async def generate():
                """Generator để stream file theo chunk"""
                chunk_size = 8192  # 8KB chunks
                with open(zip_file_path, 'rb') as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        yield chunk
            
            # Trả về streaming response
            response = Response(
                generate(),
                mimetype='application/zip',
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"',
                    'Content-Length': str(file_size)
                }
            )
            return response
            
        except Exception as e:
            logger.error(f"Error downloading zip {download_id}: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/crawl/tokhai/sync', methods=['POST'])
    async def crawl_tokhai_sync():
        """
        Crawl tờ khai (synchronous response)
        Dùng khi client không hỗ trợ SSE
        
        Body: {
            "session_id": "...",
            "tokhai_type": "842" hoặc "01/GTGT" hoặc "00" (Tất cả) hoặc null,
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        
        Note: Nếu tokhai_type = "00", null, hoặc không có → crawl TẤT CẢ loại tờ khai
        """
        try:
            from quart import request
            data = await request.get_json()
            session_id = data.get("session_id")
            tokhai_type = data.get("tokhai_type")  # Có thể là None, "00", hoặc giá trị cụ thể
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            if not all([session_id, start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields: session_id, start_date, end_date"
                }), 400
            
            # Nếu không có tokhai_type hoặc rỗng → mặc định là "Tất cả"
            if not tokhai_type or tokhai_type.strip() == "":
                tokhai_type = "00"
            
            # Check session exists
            session_exists, error_response = check_session_exists(session_id)
            if not session_exists:
                return jsonify(error_response), 404
            
            tc = get_tax_crawler()
            
            results = []
            final_result = None
            
            async for event in tc.crawl_tokhai(session_id, tokhai_type, start_date, end_date):
                if event["type"] == "item":
                    results.append(event["data"])
                elif event["type"] == "complete":
                    final_result = event
                elif event["type"] == "error":
                    return jsonify({
                        "status": "error",
                        "message": event.get("error", "Unknown error")
                    }), 500
            
            if final_result:
                return jsonify({
                    "status": "success",
                    "total": final_result.get("total", len(results)),
                    "results": results,
                    "zip_base64": final_result.get("zip_base64")
                })
            else:
                return jsonify({
                    "status": "success",
                    "total": len(results),
                    "results": results
                })
            
        except Exception as e:
            logger.error(f"Error in crawl_tokhai_sync: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # ==================== TOKHAI INFO & DOWNLOAD APIs ====================
    
    @app.route(f'{prefix}/crawl/tokhai/info', methods=['POST'])
    async def crawl_tokhai_info():
        """
        Chỉ lấy thông tin tờ khai (KHÔNG download file)
        Dùng để hiển thị danh sách trước, user chọn tải sau
        
        Body: {
            "session_id": "...",
            "tokhai_type": "842" hoặc "01/GTGT" hoặc "00" (Tất cả) hoặc null,
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        
        Returns: {
            "status": "success",
            "total": 10,
            "results": [
                {
                    "id": "11320250305601017",
                    "name": "01/GTGT (TT80/2021)",
                    "ky_tinh_thue": "Q1/2024",
                    "loai": "Chính thức",
                    "lan_nop": "1",
                    "lan_bo_sung": "",
                    "ngay_nop": "25/03/2025 15:22:00",
                    "noi_nop": "...",
                    "trang_thai": "accepted",
                    "trang_thai_text": "[Chap nhan]",
                    "file_name": "01_GTGT (TT80_2021) -Q1_2024 -L1 -Chinh thuc -(11320250305601017) -[25-03-2025 15-22-00] [Chap nhan].xml",
                    "has_download_link": true
                },
                ...
            ]
        }
        """
        try:
            from quart import request
            data = await request.get_json()
            session_id = data.get("session_id")
            tokhai_type = data.get("tokhai_type")
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            if not all([session_id, start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields: session_id, start_date, end_date"
                }), 400
            
            if not tokhai_type or tokhai_type.strip() == "":
                tokhai_type = "00"
            
            # Check session exists
            session_exists, error_response = check_session_exists(session_id)
            if not session_exists:
                return jsonify(error_response), 404
            
            tc = get_tax_crawler()
            
            results = []
            final_result = None
            
            async for event in tc.crawl_tokhai_info(session_id, tokhai_type, start_date, end_date):
                if event["type"] == "item":
                    results.append(event["data"])
                elif event["type"] == "complete":
                    final_result = event
                elif event["type"] == "error":
                    return jsonify({
                        "status": "error",
                        "message": event.get("error", "Unknown error")
                    }), 500
            
            if final_result:
                return jsonify({
                    "status": "success",
                    "total": final_result.get("total", len(results)),
                    "results": results
                })
            else:
                return jsonify({
                    "status": "success",
                    "total": len(results),
                    "results": results
                })
            
        except Exception as e:
            logger.error(f"Error in crawl_tokhai_info: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/crawl/thongbao/sync', methods=['POST'])
    async def crawl_thongbao_sync():
        """Crawl thông báo (synchronous response)"""
        try:
            from quart import request
            data = await request.get_json()
            session_id = data.get("session_id")
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            if not all([session_id, start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields"
                }), 400
            
            # Check session exists
            session_exists, error_response = check_session_exists(session_id)
            if not session_exists:
                return jsonify(error_response), 404
            
            tc = get_tax_crawler()
            
            async for event in tc.crawl_thongbao(session_id, start_date, end_date):
                if event["type"] == "complete":
                    return jsonify({
                        "status": "success",
                        "total": event.get("total", 0),
                        "results_count": event.get("results_count", 0),
                        "files_count": event.get("files_count", 0),
                        "total_size": event.get("total_size", 0),
                        "results": event.get("results", []),
                        "files": event.get("files", []),
                        "zip_base64": event.get("zip_base64"),
                        "zip_filename": event.get("zip_filename")
                    })
                elif event["type"] == "error":
                    return jsonify({
                        "status": "error",
                        "message": event.get("error", "Unknown error")
                    }), 500
            
            return jsonify({
                "status": "success",
                "total": 0,
                "results": []
            })
            
        except Exception as e:
            logger.error(f"Error in crawl_thongbao_sync: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/crawl/giaynoptien', methods=['POST'])
    async def crawl_giay_nop_tien():
        """
        Crawl giấy nộp tiền thuế (streaming response)
        Body: {
            "session_id": "...",
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        """
        from quart import request, Response
        try:
            data = await request.get_json()
            if not data:
                async def generate_error():
                    yield f"data: {json.dumps({'type': 'error', 'error_code': 'INVALID_REQUEST', 'error': 'Invalid request body'}, ensure_ascii=False)}\n\n"
                return Response(
                    generate_error(),
                    mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache',
                        'X-Accel-Buffering': 'no'
                    }
                )
            
            session_id = data.get("session_id")
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            if not all([session_id, start_date, end_date]):
                async def generate_error():
                    yield f"data: {json.dumps({'type': 'error', 'error_code': 'MISSING_REQUIRED_FIELDS', 'error': 'Missing required fields'}, ensure_ascii=False)}\n\n"
                return Response(
                    generate_error(),
                    mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache',
                        'X-Accel-Buffering': 'no'
                    }
                )
            
            # ✅ Check session trước khi crawl (dùng hàm chung)
            logger.info(f"[crawl_giaynoptien] Checking session: {session_id[:8]}...")
            try:
                is_valid, error_response = await check_session_before_crawl(session_id)
                if not is_valid:
                    # Trả về error event trong SSE stream (status 200, không phải 401)
                    error_code = error_response.get("error_code", "SESSION_EXPIRED")
                    error_message = error_response.get("message", "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.")
                    logger.warning(f"[crawl_giaynoptien] Session invalid: {error_code} - {error_message}")
                    async def generate_error():
                        yield f"data: {json.dumps({'type': 'error', 'error_code': error_code, 'error': error_message}, ensure_ascii=False)}\n\n"
                    return Response(
                        generate_error(),
                        mimetype='text/event-stream',
                        headers={
                            'Cache-Control': 'no-cache',
                            'X-Accel-Buffering': 'no'
                        }
                    )
            except Exception as check_error:
                logger.error(f"[crawl_giaynoptien] Error checking session: {check_error}", exc_info=True)
                # Nếu có lỗi khi check, trả về error event trong SSE stream
                async def generate_error():
                    yield f"data: {json.dumps({'type': 'error', 'error_code': 'SESSION_EXPIRED', 'error': 'Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.'}, ensure_ascii=False)}\n\n"
                return Response(
                    generate_error(),
                    mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache',
                        'X-Accel-Buffering': 'no'
                    }
                )
            
            tc = get_tax_crawler()
            
            async def generate():
                async for event in tc.crawl_giay_nop_tien(session_id, start_date, end_date):
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            
            return Response(
                generate(),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'
                }
            )
            
        except Exception as e:
            logger.error(f"Error in crawl_giay_nop_thue: {e}", exc_info=True)
            # ✅ Trả về error event trong SSE stream thay vì HTTP error
            error_message = str(e)
            # Check xem có phải là session error không
            if "session" in error_message.lower() or "Session" in error_message:
                error_code = "SESSION_EXPIRED"
            else:
                error_code = "CRAWL_ERROR"
            
            async def generate_error():
                yield f"data: {json.dumps({'type': 'error', 'error_code': error_code, 'error': error_message}, ensure_ascii=False)}\n\n"
            return Response(
                generate_error(),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'
                }
            )
    
    @app.route(f'{prefix}/crawl/giaynoptien/sync', methods=['POST'])
    async def crawl_giay_nop_tien_sync():
        """Crawl giấy nộp tiền thuế (synchronous response)"""
        try:
            from quart import request
            data = await request.get_json()
            session_id = data.get("session_id")
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            
            if not all([session_id, start_date, end_date]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields"
                }), 400
            
            # ✅ Check session trước khi crawl (dùng hàm chung)
            is_valid, error_response = await check_session_before_crawl(session_id)
            if not is_valid:
                return jsonify(error_response), 401
            
            tc = get_tax_crawler()
            
            async for event in tc.crawl_giay_nop_tien(session_id, start_date, end_date):
                if event["type"] == "complete":
                    return jsonify({
                        "status": "success",
                        "total": event.get("total", 0),
                        "results_count": event.get("results_count", 0),
                        "files_count": event.get("files_count", 0),
                        "total_size": event.get("total_size", 0),
                        "results": event.get("results", []),
                        "files": event.get("files", []),
                        "zip_base64": event.get("zip_base64"),
                        "zip_filename": event.get("zip_filename")
                    })
                elif event["type"] == "error":
                    return jsonify({
                        "status": "error",
                        "message": event.get("error", "Unknown error")
                    }), 500
            
            return jsonify({
                "status": "success",
                "total": 0,
                "results": []
            })
            
        except Exception as e:
            logger.error(f"Error in crawl_giay_nop_thue_sync: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # ==================== CONVERT APIs ====================
    
    @app.route(f'{prefix}/convert/xml2xlsx', methods=['POST'])
    async def convert_xml_to_xlsx():
        """
        Chuyển đổi XML sang Excel
        Body: { "zip_base64": "..." } hoặc upload file
        """
        try:
            from quart import request
            zip_base64 = None
            
            # Cách 1: Upload file
            files = await request.files
            if 'file' in files:
                file = files['file']
                if file.filename == '':
                    return jsonify({
                        "status": "error",
                        "message": "No file selected"
                    }), 400
                
                file_content = await file.read()
                zip_base64 = base64.b64encode(file_content).decode('utf-8')
            
            # Cách 2: JSON với base64
            elif request.is_json:
                data = await request.get_json()
                zip_base64 = data.get("zip_base64")
            
            if not zip_base64:
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing zip_base64 or file"
                }), 400
            
            tc = get_tax_crawler()
            result = await tc.convert_xml_to_xlsx(zip_base64)
            
            if result["success"]:
                return jsonify({
                    "status": "success",
                    "xlsx_base64": result["xlsx_base64"],
                    "row_count": result["row_count"]
                })
            else:
                return jsonify({
                    "status": "error",
                    "message": result.get("error", "Conversion failed")
                }), 500
                
        except Exception as e:
            logger.error(f"Error in convert_xml_to_xlsx: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # ==================== DEBUG APIs ====================
    
    @app.route(f'{prefix}/debug/screenshot', methods=['POST'])
    async def debug_screenshot():
        """
        Lấy screenshot của page hiện tại để debug
        Body: { "session_id": "..." }
        """
        try:
            from quart import request
            import base64
            
            data = await request.get_json()
            session_id = data.get("session_id")
            
            if not session_id:
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_SESSION_ID",
                    "message": "Missing session_id"
                }), 400
            
            sm = get_session_manager()
            session = sm.get_session(session_id)
            
            if not session:
                return jsonify({
                    "status": "error",
                    "error_code": "SESSION_NOT_FOUND",
                    "message": "Session not found"
                }), 404
            
            page = session.page
            screenshot = await page.screenshot(full_page=True)
            screenshot_base64 = base64.b64encode(screenshot).decode('utf-8')
            
            return jsonify({
                "status": "success",
                "current_url": page.url,
                "screenshot_base64": screenshot_base64
            })
            
        except Exception as e:
            logger.error(f"Error in debug_screenshot: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    @app.route(f'{prefix}/debug/navigate', methods=['POST'])
    async def debug_navigate():
        """
        Navigate đến một URL cụ thể để debug
        Body: { "session_id": "...", "url": "..." }
        """
        try:
            from quart import request
            
            data = await request.get_json()
            session_id = data.get("session_id")
            url = data.get("url")
            
            if not session_id or not url:
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing session_id or url"
                }), 400
            
            sm = get_session_manager()
            session = sm.get_session(session_id)
            
            if not session:
                return jsonify({
                    "status": "error",
                    "error_code": "SESSION_NOT_FOUND",
                    "message": "Session not found"
                }), 404
            
            page = session.page
            await page.goto(url, wait_until='networkidle')
            
            return jsonify({
                "status": "success",
                "current_url": page.url
            })
            
        except Exception as e:
            logger.error(f"Error in debug_navigate: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # ==================== BATCH CRAWL (Parallel) ====================
    
    @app.route(f'{prefix}/crawl/batch', methods=['POST'])
    async def batch_crawl():
        """
        Crawl nhiều loại dữ liệu đồng thời (streaming response)
        
        Body: {
            "session_id": "...",
            "start_date": "01/01/2023",
            "end_date": "31/12/2023",
            "crawl_types": ["tokhai", "thongbao", "giaynoptien"],
            "tokhai_type": "01/GTGT" hoặc "00" (Tất cả) hoặc null
        }
        Returns: Server-Sent Events stream
        """
        try:
            from quart import request, Response
            data = await request.get_json()
            session_id = data.get("session_id")
            tokhai_type = data.get("tokhai_type", "00")  # Mặc định "Tất cả"
            start_date = data.get("start_date")
            end_date = data.get("end_date")
            crawl_types = data.get("crawl_types", [])
            
            if not all([session_id, start_date, end_date, crawl_types]):
                return jsonify({
                    "status": "error",
                    "error_code": "MISSING_REQUIRED_FIELDS",
                    "message": "Missing required fields: session_id, start_date, end_date, crawl_types"
                }), 400
            
            # Validate crawl_types
            valid_types = ["tokhai", "thongbao", "giaynoptien"]
            crawl_types = [t for t in crawl_types if t in valid_types]
            
            if not crawl_types:
                return jsonify({
                    "status": "error",
                    "error_code": "INVALID_CRAWL_TYPES",
                    "message": "Không có loại crawl hợp lệ. Chọn từ: tokhai, thongbao, giaynoptien"
                }), 400
            
            # Check session exists
            session_exists, error_response = check_session_exists(session_id)
            if not session_exists:
                return jsonify(error_response), 404
            
            tc = get_tax_crawler()
            
            async def generate():
                async for event in tc.crawl_batch(session_id, start_date, end_date, crawl_types, tokhai_type):
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
            
            return Response(
                generate(),
                mimetype='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'
                }
            )
            
        except Exception as e:
            logger.error(f"Error in batch_crawl: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # ==================== DOWNLOAD ZIP FILE ====================
    
    @app.route(f'{prefix}/download/<download_id>', methods=['GET'], endpoint='go_soft_download_zip')
    async def download_zip(download_id):
        """
        Download ZIP file bằng download_id
        
        Query params:
            - filename: Tên file ZIP (optional)
        """
        try:
            from services.tax_crawler import TaxCrawlerService
            from quart import Response
            import os
            
            # Lấy ZIP_STORAGE_DIR từ TaxCrawlerService
            zip_storage_dir = TaxCrawlerService.ZIP_STORAGE_DIR
            zip_file_path = os.path.join(zip_storage_dir, f"{download_id}.zip")
            
            # Kiểm tra file tồn tại
            if not os.path.exists(zip_file_path):
                logger.warning(f"Download request for {download_id}, file not found: {zip_file_path}")
                return jsonify({
                    "status": "error",
                    "error_code": "FILE_NOT_FOUND",
                    "message": "ZIP file not found"
                }), 404
            
            # Lấy filename từ query params hoặc dùng default
            filename = request.args.get('filename', f"{download_id}.zip")
            
            # Đọc file và trả về
            with open(zip_file_path, 'rb') as f:
                zip_content = f.read()
            
            logger.info(f"Download request for {download_id}, sending file: {zip_file_path} as {filename}")
            
            # ✅ Thêm CORS headers để frontend có thể download
            headers = {
                'Content-Type': 'application/zip',
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Content-Length': str(len(zip_content)),
                'Access-Control-Allow-Origin': '*',  # ✅ Cho phép tất cả origins (hoặc set cụ thể: 'https://gotax.vn')
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
            }
            
            return Response(
                zip_content,
                mimetype='application/zip',
                headers=headers
            )
            
        except Exception as e:
            logger.error(f"Error in download_zip: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
    # ✅ Handle OPTIONS request cho CORS preflight
    @app.route(f'{prefix}/download/<download_id>', methods=['OPTIONS'])
    async def download_zip_options(download_id):
        """Handle CORS preflight request"""
        from quart import Response
        return Response(
            '',
            status=200,
            headers={
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
            }
        )


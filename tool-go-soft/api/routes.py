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
            "message": "Session không tồn tại hoặc đã hết hạn. Vui lòng đăng nhập lại."
        }
    
    return True, None


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
        Crawl tờ khai (streaming response)
        Body: {
            "session_id": "...",
            "tokhai_type": "842" hoặc "01/GTGT" hoặc "00" (Tất cả) hoặc null,
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        Returns: Server-Sent Events stream
        
        Note: Nếu tokhai_type = "00", null, hoặc không có → crawl TẤT CẢ loại tờ khai
        """
        try:
            from quart import request, Response
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
            
            async def generate():
                async for event in tc.crawl_tokhai(session_id, tokhai_type, start_date, end_date):
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
            logger.error(f"Error in crawl_tokhai: {e}")
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
    
    @app.route(f'{prefix}/crawl/thongbao', methods=['POST'])
    async def crawl_thongbao():
        """
        Crawl thông báo (streaming response)
        Body: {
            "session_id": "...",
            "start_date": "01/01/2023",
            "end_date": "31/12/2023"
        }
        """
        try:
            from quart import request, Response
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
            
            async def generate():
                async for event in tc.crawl_thongbao(session_id, start_date, end_date):
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
            logger.error(f"Error in crawl_thongbao: {e}")
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
        try:
            from quart import request, Response
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
            logger.error(f"Error in crawl_giay_nop_thue: {e}")
            return jsonify({
                "status": "error",
                "message": str(e)
            }), 500
    
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
            
            # Check session exists
            session_exists, error_response = check_session_exists(session_id)
            if not session_exists:
                return jsonify(error_response), 404
            
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

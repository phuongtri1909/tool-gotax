"""
API Server chung cho tất cả tools
Đã migrate sang Quart (async) để support Playwright + httpx

Tất cả tools sẽ được gọi qua: /api/go-quick/..., /api/go-soft/..., /api/go-invoice/..., /api/go-bot/...

Run với:
  python api_server.py  (dev mode)
  hypercorn api_server:app --bind 0.0.0.0:5000  (production)
"""
import os
import sys
import asyncio
import signal

# Quart = async Flask (API tương tự 99%)
from quart import Quart, jsonify, request
from quart_cors import cors

# ✅ Import ProxyManager
try:
    from proxy_manager import get_proxy_manager
    PROXY_MANAGER_AVAILABLE = True
except ImportError:
    PROXY_MANAGER_AVAILABLE = False
    print("⚠️  ProxyManager không khả dụng (file proxy_manager.py không tồn tại)")

# Thử load từ .env file (tùy chọn)
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Đã load .env file (nếu có)")
except ImportError:
    pass

app = Quart(__name__)
app = cors(app)  # Cho phép Laravel gọi API

# Cấu hình
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
app.config['DEBUG'] = os.environ.get('DEBUG', 'False').lower() == 'true'

# 🔐 API Key Authentication
API_KEY = os.environ.get('API_KEY', None)


@app.before_request
async def inject_proxy_into_request():
    """
    ✅ Trước mỗi request, lấy proxy tiếp theo từ proxy_manager
    và lưu vào request context để các tool có thể sử dụng
    """
    if not PROXY_MANAGER_AVAILABLE:
        return None
    
    # Bỏ qua health check và proxy endpoints
    if request.path in ['/api/health', '/api/proxy/info', '/api/proxy/reload', '/api/proxy/reset']:
        return None
    
    try:
        # Lấy proxy tiếp theo (round-robin)
        proxy_manager = get_proxy_manager()
        proxy_url = proxy_manager.get_next_proxy()
        
        if proxy_url:
            # Lưu proxy vào request context (các tool có thể lấy bằng request.proxy)
            # Note: Quart không có request context như Flask, dùng g để lưu
            from quart import g
            g.proxy = proxy_url
            
            # Cũng thử inject vào JSON body nếu có thể
            if request.content_type and 'application/json' in request.content_type:
                try:
                    # Đọc body hiện tại
                    body = await request.get_data()
                    if body:
                        import json
                        data = json.loads(body.decode('utf-8'))
                        if isinstance(data, dict):
                            data['proxy'] = proxy_url
                            # Lưu lại vào g để tool có thể dùng
                            g.request_data = data
                except Exception:
                    # Nếu không parse được JSON, bỏ qua
                    pass
    except Exception as e:
        # Nếu có lỗi với proxy manager, bỏ qua (không ảnh hưởng request)
        pass
    
    return None


@app.before_request
async def check_api_key():
    """
    Kiểm tra API key nếu được bật
    """
    if API_KEY is None:
        return None
    
    if request.path == '/api/health':
        return None
    
    api_key = request.headers.get('X-API-Key')
    if api_key != API_KEY:
        return jsonify({
            "status": "error",
            "message": "Invalid or missing API key"
        }), 401
    
    return None


# Danh sách tools
TOOLS = {
    'go-quick': {
        'path': 'tool-go-quick',
        'module': 'tool_go_quick',
        'name': 'ID Quick API',
        'async': False
    },
    'go-soft': {
        'path': 'tool-go-soft',
        'module': 'tool_go_soft',
        'name': 'Tax Crawler API (Playwright + httpx)',
        'async': True
    },
    'go-invoice': {
        'path': 'tool-go-invoice',
        'module': 'tool_go_invoice',
        'name': 'Invoice Backend API',
        'async': False
    },
    'go-bot': {
        'path': 'toolgobot',
        'module': 'tool_go_bot',
        'name': 'Go Bot API',
        'async': False
    },
}


def register_tool_routes(tool_name, tool_config):
    """Đăng ký routes cho một tool"""
    try:
        tool_path = tool_config['path']
        
        tool_abs_path = os.path.abspath(tool_path)
        if tool_abs_path not in sys.path:
            sys.path.insert(0, tool_abs_path)
        
        api_routes_path = os.path.join(tool_path, 'api', 'routes.py')
        if os.path.exists(api_routes_path):
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                f"{tool_name}_routes",
                api_routes_path
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                if hasattr(module, 'register_routes'):
                    module.register_routes(app, f'/api/{tool_name}')
                    async_tag = " (async)" if tool_config.get('async') else ""
                    return True
                else:
                    print("⚠️ Module %s không có register_routes" % tool_name)
            else:
                print(f"⚠️  Không thể load spec từ {api_routes_path}")
        else:
            print(f"⚠️  Không tìm thấy file: {api_routes_path}")
        
        print(f"❌ Không thể đăng ký routes cho tool: {tool_name}")
        return False
        
    except Exception as e:
        print(f"❌ Lỗi khi đăng ký tool {tool_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


# Đăng ký routes cho tất cả tools
registered = []
for tool_name, tool_config in TOOLS.items():
    if register_tool_routes(tool_name, tool_config):
        registered.append(tool_name)
print("🚀 API Server (Quart) | Routes: %s" % ", ".join(registered))


@app.route('/api/health', methods=['GET'])
async def api_health_check():
    """Health check cho toàn bộ API server"""
    tools_status = {}
    for tool_name, tool_config in TOOLS.items():
        tools_status[tool_name] = {
            'name': tool_config['name'],
            'status': 'registered',
            'async': tool_config.get('async', False)
        }
    
    # Proxy info (nếu có)
    proxy_info = None
    if PROXY_MANAGER_AVAILABLE:
        try:
            proxy_manager = get_proxy_manager()
            proxy_info = {
                'total_proxies': proxy_manager.get_proxy_count(),
                'current_index': proxy_manager.get_current_index(),
                'proxies': proxy_manager.get_all_proxies()
            }
        except Exception:
            pass
    
    response_data = {
        "status": "success",
        "message": "API Server is running (Async mode)",
        "tools": tools_status,
        "version": "2.0",
        "engine": "Quart + Playwright + httpx"
    }
    
    if proxy_info:
        response_data["proxy_info"] = proxy_info
    
    return jsonify(response_data)


@app.route('/api/proxy/info', methods=['GET'])
async def get_proxy_info():
    """Xem thông tin proxy manager"""
    if not PROXY_MANAGER_AVAILABLE:
        return jsonify({
            "status": "error",
            "message": "ProxyManager không khả dụng"
        }), 503
    
    try:
        proxy_manager = get_proxy_manager()
        return jsonify({
            "status": "success",
            "data": {
                "total_proxies": proxy_manager.get_proxy_count(),
                "current_index": proxy_manager.get_current_index(),
                "proxies": proxy_manager.get_all_proxies()
            }
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/api/proxy/reload', methods=['POST'])
async def reload_proxy_list():
    """Tải lại danh sách proxy từ file (sau khi update proxylist.txt)"""
    if not PROXY_MANAGER_AVAILABLE:
        return jsonify({
            "status": "error",
            "message": "ProxyManager không khả dụng"
        }), 503
    
    try:
        proxy_manager = get_proxy_manager()
        proxy_manager.reload_proxies()
        return jsonify({
            "status": "success",
            "message": "Proxy list reloaded",
            "data": {
                "total_proxies": proxy_manager.get_proxy_count(),
                "proxies": proxy_manager.get_all_proxies()
            }
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.route('/api/proxy/reset', methods=['POST'])
async def reset_proxy_index():
    """Reset proxy index về 0 (restart round-robin)"""
    if not PROXY_MANAGER_AVAILABLE:
        return jsonify({
            "status": "error",
            "message": "ProxyManager không khả dụng"
        }), 503
    
    try:
        proxy_manager = get_proxy_manager()
        proxy_manager.reset_index()
        return jsonify({
            "status": "success",
            "message": "Proxy index reset to 0",
            "data": {
                "current_index": proxy_manager.get_current_index()
            }
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


@app.errorhandler(404)
async def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint not found"
    }), 404


@app.errorhandler(500)
async def internal_error(error):
    import traceback
    return jsonify({
        "status": "error",
        "message": "Internal server error",
        "detail": traceback.format_exc() if app.config['DEBUG'] else None
    }), 500


# Graceful shutdown
async def shutdown():
    """Cleanup khi shutdown"""
    print("\n🛑 Đang shutdown...")
    try:
        # Cleanup tool-go-soft sessions
        from importlib import import_module
        go_soft_path = os.path.abspath('tool-go-soft')
        if go_soft_path not in sys.path:
            sys.path.insert(0, go_soft_path)
        
        from services.session_manager import session_manager
        await session_manager.shutdown()
    except Exception as e:
        print(f"⚠️  Lỗi khi cleanup: {e}")
    
    print("✅ Shutdown hoàn tất")


@app.before_serving
async def startup():
    """Khởi tạo khi server start"""
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            await browser.close()
        print("✅ Server sẵn sàng | Playwright OK")
    except Exception:
        print("✅ Server sẵn sàng | Playwright chưa cài (playwright install chromium)")


@app.after_serving
async def after_shutdown():
    """Cleanup sau khi server shutdown"""
    await shutdown()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    host = os.environ.get('HOST', '127.0.0.1')
    
    print("🌐 http://%s:%s | Tools: %s\n" % (host, port, ", ".join(TOOLS.keys())))
    
    # Chạy với Quart dev server
    app.run(host=host, port=port, debug=debug)

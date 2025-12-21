"""
API Server chung cho tất cả tools
Đã migrate sang Quart (async) để support Playwright + httpx

Tất cả tools sẽ được gọi qua: /api/go-quick/..., /api/go-soft/...

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
                    print(f"✅ Đã đăng ký routes cho tool: {tool_name}{async_tag}")
                    return True
                else:
                    print(f"⚠️  Module {tool_name} không có function register_routes")
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
print("🚀 Đang khởi tạo API Server (Async mode)...")
print("📦 Tech stack: Quart + Playwright + httpx")
for tool_name, tool_config in TOOLS.items():
    register_tool_routes(tool_name, tool_config)


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
    
    return jsonify({
        "status": "success",
        "message": "API Server is running (Async mode)",
        "tools": tools_status,
        "version": "2.0",
        "engine": "Quart + Playwright + httpx"
    })


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
    print("🎯 Server đã sẵn sàng nhận requests")
    
    # Cài đặt Playwright browsers nếu chưa có
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            # Test browser launch
            browser = await p.chromium.launch(headless=True)
            await browser.close()
        print("✅ Playwright browsers đã sẵn sàng")
    except Exception as e:
        print(f"⚠️  Playwright chưa được cài đặt. Chạy: playwright install chromium")


@app.after_serving
async def after_shutdown():
    """Cleanup sau khi server shutdown"""
    await shutdown()


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    host = os.environ.get('HOST', '127.0.0.1')
    
    print(f"\n🌐 API Server đang chạy tại: http://{host}:{port}")
    print(f"📋 Các tools đã đăng ký: {', '.join(TOOLS.keys())}")
    print(f"🔧 Debug mode: {debug}")
    print("\n📖 Để chạy production, dùng:")
    print(f"   hypercorn api_server:app --bind {host}:{port}")
    print("")
    
    # Chạy với Quart dev server
    app.run(host=host, port=port, debug=debug)

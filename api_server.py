"""
API Server chung cho tất cả tools
Tất cả tools sẽ được gọi qua: /api/go-quick/..., /api/go-bot/...
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import sys

# Thử load từ .env file (tùy chọn)
try:
    from dotenv import load_dotenv
    load_dotenv()  # Load từ file .env nếu có
    print("✅ Đã load .env file (nếu có)")
except ImportError:
    # Không có python-dotenv, bỏ qua
    pass

app = Flask(__name__)
CORS(app)  # Cho phép Laravel gọi API

# Cấu hình
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
app.config['DEBUG'] = os.environ.get('DEBUG', 'False').lower() == 'true'

# 🔐 API Key Authentication (chỉ cần khi dùng domain/public)
# Có thể set bằng nhiều cách:
# 1. File .env: API_KEY=your-secret-key
# 2. Export: export API_KEY=your-secret-key
# 3. Systemd service: Environment="API_KEY=your-secret-key"
# Nếu None = không bật API key (phù hợp cho local deployment)
API_KEY = os.environ.get('API_KEY', None)

@app.before_request
def check_api_key():
    """
    Kiểm tra API key nếu được bật
    - Nếu API_KEY = None: Không kiểm tra (local deployment)
    - Nếu API_KEY được set: Kiểm tra header X-API-Key
    """
    # Bỏ qua nếu không set API_KEY (local deployment)
    if API_KEY is None:
        return None
    
    # Bỏ qua health check
    if request.path == '/api/health':
        return None
    
    # Kiểm tra API key trong header
    api_key = request.headers.get('X-API-Key')
    if api_key != API_KEY:
        return jsonify({
            "status": "error",
            "message": "Invalid or missing API key"
        }), 401
    
    return None

# Danh sách tools (tự động load)
TOOLS = {
    'go-quick': {
        'path': 'tool-go-quick',
        'module': 'tool_go_quick',
        'name': 'ID Quick API'
    },
    # Thêm tool mới ở đây:
    # 'go-bot': {
    #     'path': 'tool-go-bot',
    #     'module': 'tool_go_bot',
    #     'name': 'Go Bot API'
    # },
}

def register_tool_routes(tool_name, tool_config):
    """Đăng ký routes cho một tool"""
    try:
        tool_path = tool_config['path']
        
        # Thêm tool path vào sys.path
        tool_abs_path = os.path.abspath(tool_path)
        if tool_abs_path not in sys.path:
            sys.path.insert(0, tool_abs_path)
        
        # Thử import routes từ api/routes.py
        api_routes_path = os.path.join(tool_path, 'api', 'routes.py')
        if os.path.exists(api_routes_path):
            # Import bằng cách load file trực tiếp
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
                    print(f"✅ Đã đăng ký routes cho tool: {tool_name}")
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
print("🚀 Đang khởi tạo API Server...")
for tool_name, tool_config in TOOLS.items():
    register_tool_routes(tool_name, tool_config)

@app.route('/api/health', methods=['GET'])
def api_health_check():
    """Health check cho toàn bộ API server"""
    tools_status = {}
    for tool_name, tool_config in TOOLS.items():
        tools_status[tool_name] = {
            'name': tool_config['name'],
            'status': 'registered'
        }
    
    return jsonify({
        "status": "success",
        "message": "API Server is running",
        "tools": tools_status,
        "version": "1.0"
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint not found"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    import traceback
    return jsonify({
        "status": "error",
        "message": "Internal server error",
        "detail": traceback.format_exc() if app.config['DEBUG'] else None
    }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    host = os.environ.get('HOST', '127.0.0.1')  # Mặc định localhost
    
    print(f"🌐 API Server đang chạy tại: http://{host}:{port}")
    print(f"📋 Các tools đã đăng ký: {', '.join(TOOLS.keys())}")
    
    app.run(host=host, port=port, debug=debug)


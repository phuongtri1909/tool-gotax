# 🚀 Tool APIs - Hướng dẫn sử dụng

## 📁 Cấu trúc Project

```
tool-gotax/
├── gotax/                    # Laravel app (source code)
│   └── ...
│
├── api_server.py            # ⭐ API Server chung (1 server cho tất cả tools)
├── requirements.txt          # Dependencies chung
│
├── tool-go-quick/           # Tool 1: ID Quick
│   ├── api/
│   │   └── routes.py        # Routes cho tool này
│   ├── main.py              # Core logic
│   ├── requirements.txt     # Dependencies riêng của tool
│   └── laravel/             # Laravel integration (tùy chọn)
│
├── tool-go-bot/             # Tool 2: Go Bot (sẽ có sau)
│   └── ...
│
└── README.md                # File này
```

## 🎯 Cách hoạt động

- **1 API Server chung** (`api_server.py`) chạy trên port 5000
- **Tất cả tools** được gọi qua prefix:
  - `/api/go-quick/health` → Tool ID Quick
  - `/api/go-bot/health` → Tool Go Bot (sẽ có)
- **Laravel** gọi qua: `http://127.0.0.1:5000/api/go-quick/...`

---

## 🧪 PHẦN 1: CHẠY LOCAL

### Bước 1: Cài đặt Dependencies

```bash
# Cài dependencies chung (cho API server)
pip install -r requirements.txt

# Cài dependencies cho từng tool (core logic)
pip install -r tool-go-quick/requirements.txt
# pip install -r tool-go-bot/requirements.txt  # Khi có tool mới
```

### Bước 2: Chạy API Server

```bash
# Từ thư mục gốc
python api_server.py
```

Server sẽ chạy tại: `http://localhost:5000`

### Bước 3: Test với Postman/curl

#### ✅ Health Check (Tất cả tools)
```bash
curl http://localhost:5000/api/health
```

#### ✅ Health Check (Tool cụ thể)
```bash
curl http://localhost:5000/api/go-quick/health
```

#### ✅ Process CCCD (ZIP hoặc base64)

**Cách 1: Upload file ZIP trực tiếp (multipart/form-data)**
```bash
curl -X POST http://localhost:5000/api/go-quick/process-cccd \
  -F "file=@datatest.zip"
```

**Cách 2: Gửi base64 qua JSON**
```bash
curl -X POST http://localhost:5000/api/go-quick/process-cccd \
  -H "Content-Type: application/json" \
  -d '{"inp_path": "base64_string_here"}'
```

**Lưu ý:**
- File upload: Gửi file ZIP chứa ảnh CCCD (ví dụ: `1mt.png`, `1ms.png`, `2mt.png`, `2ms.png`...)
- Base64: Encode file ZIP thành base64 string và gửi trong JSON với key `inp_path`
- Response: JSON với thông tin CCCD đã extract

**Response mẫu:**
```json
{
  "status": "success",
  "message": "Đã trích xuất thông tin các CCCD",
  "customer": [
    {
      "index": 1,
      "file_name": "1",
      "id_card": "001234567890",
      "name": "NGUYEN VAN A",
      "gender": "Nam",
      "birth_date": "01/01/1990",
      "created_date": "01/01/2020",
      "place_created": "CỤC TRƯỞNG...",
      "expiry_date": "01/01/2035",
      "hometown": "Hà Nội",
      "address": "123 Đường ABC",
      "address2": "Quận 1"
    }
  ]
}
```

#### ✅ Process CCCD Images (2 ảnh riêng)

**Upload 2 ảnh mặt trước và mặt sau:**
```bash
curl -X POST http://localhost:5000/api/go-quick/process-cccd-images \
  -F "mt=@front.jpg" \
  -F "ms=@back.jpg"
```

**Lưu ý:**
- `mt`: File ảnh mặt trước (mặt trước CCCD)
- `ms`: File ảnh mặt sau (mặt sau CCCD)
- Response: JSON với thông tin CCCD đã extract (giống như process-cccd)

#### ✅ Process PDF
```bash
curl -X POST http://localhost:5000/api/go-quick/process-pdf \
  -F "file=@document.pdf"
```

**Lưu ý:**
- Input: File PDF
- Process: Convert PDF → PNG (1mt.png, 1ms.png, 2mt.png, 2ms.png...) → Đưa vào CCCD extractor
- Response: JSON với thông tin CCCD đã extract (giống như process-cccd)

#### ✅ Process Excel
```bash
curl -X POST http://localhost:5000/api/go-quick/process-excel \
  -F "file=@data.xlsx"
```

**Lưu ý:**
- Input: File Excel chứa Google Drive URLs (cột 1: file_name, cột 2: mt_url, cột 3: ms_url)
- Process: Download ảnh từ Google Drive → ZIP (1mt.png, 1ms.png...) → Đưa vào CCCD extractor
- Response: JSON với thông tin CCCD đã extract (giống như process-cccd)

---

## 🖥️ PHẦN 2: DEPLOY LÊN AAPANEL

### Bước 1: Upload Files lên Server

```bash
# SSH vào server
ssh user@your-server

# Tạo thư mục project
mkdir -p /www/wwwroot/tool-apis
cd /www/wwwroot/tool-apis
```

Upload các file sau:

```
/www/wwwroot/tool-apis/
├── api_server.py            # API Server chung
├── requirements.txt         # Dependencies chung (flask, flask-cors, gunicorn)
│
├── tool-go-quick/           # Tool 1
│   ├── api/
│   │   └── routes.py        # Routes cho tool
│   ├── main.py              # Core logic
│   ├── requirements.txt     # Dependencies riêng của tool (ultralytics, opencv...)
│   └── __pycache__/         # Model files
│       ├── best.pt
│       └── ...
│
└── tool-go-bot/             # Tool 2 (sẽ có)
    └── ...
```

### Bước 2: Cài đặt Python & Dependencies

```bash
# Tạo virtual environment
python3 -m venv venv
source venv/bin/activate

# Cài dependencies chung (cho API server)
pip install -r requirements.txt

# Cài dependencies cho từng tool (core logic)
pip install -r tool-go-quick/requirements.txt
```

### Bước 3: Sửa đường dẫn trong main.py (nếu cần)

Nếu `main.py` có đường dẫn hardcode, sửa thành:

```python
# Trước
self.model1 = YOLO(".\\__pycache__\\best.pt")

# Sau
import os
base_dir = os.path.dirname(os.path.abspath(__file__))
self.model1 = YOLO(os.path.join(base_dir, "__pycache__", "best.pt"))
```

### Bước 4: Cấu hình Supervisor (aaPanel)

1. Vào **Supervisor** trong aaPanel
2. Tạo process mới:
   - **Name:** tool-apis-server
   - **Command:** `/www/wwwroot/tool-apis/venv/bin/gunicorn -w 2 -b 127.0.0.1:5000 api_server:app`
   - **Directory:** `/www/wwwroot/tool-apis`
   - **User:** www

**Hoặc dùng Systemd:**

```bash
# Tạo service file
sudo nano /etc/systemd/system/tool-apis.service
```

Paste nội dung:

```ini
[Unit]
Description=Tool APIs Server
After=network.target

[Service]
Type=simple
User=www
WorkingDirectory=/www/wwwroot/tool-apis
Environment="PATH=/www/wwwroot/tool-apis/venv/bin"
ExecStart=/www/wwwroot/tool-apis/venv/bin/gunicorn -w 2 -b 127.0.0.1:5000 api_server:app
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
# Khởi động service
sudo systemctl daemon-reload
sudo systemctl start tool-apis
sudo systemctl enable tool-apis

# Kiểm tra
sudo systemctl status tool-apis
```

### Bước 5: Cấu hình Nginx Reverse Proxy

Thêm vào config Nginx của Laravel:

```nginx
location /api/ {
    proxy_pass http://127.0.0.1:5000/api/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    
    # Timeout cho xử lý lâu
    proxy_read_timeout 600s;
    proxy_connect_timeout 600s;
    proxy_send_timeout 600s;
}
```

**🔒 Lưu ý bảo mật:**

**Khi deploy local (127.0.0.1) - KHÔNG CẦN API KEY:**
- API server bind vào `127.0.0.1:5000` (localhost only)
- Chỉ Laravel/Nginx trên server mới gọi được
- Public không thể truy cập trực tiếp port 5000
- **Không cần API key** vì chỉ có Laravel trên cùng server mới gọi được

**Khi dùng domain (public) - CẦN API KEY:**
- Khi expose API ra internet qua domain
- Cần thêm API key authentication để bảo mật
- Xem phần "API Key Authentication" bên dưới

### Bước 6: Set Permissions

```bash
chmod -R 755 /www/wwwroot/tool-apis
chown -R www:www /www/wwwroot/tool-apis
```

### Bước 7: Test trên Server

```bash
# Test từ localhost
curl http://127.0.0.1:5000/api/health
curl http://127.0.0.1:5000/api/go-quick/health

# Test qua Nginx (nếu đã config domain)
curl http://your-domain.com/api/go-quick/health
```

---

## 🔗 PHẦN 3: TÍCH HỢP VỚI LARAVEL

### Bước 1: Copy Controller vào Laravel (nếu có)

**Lưu ý:** Controller là tùy chọn, bạn có thể tự tạo hoặc dùng trực tiếp Http facade.

Nếu có file controller trong `tool-go-quick/laravel/`:
```bash
# Copy Controller
cp tool-go-quick/laravel/IDQuickController.php \
   /www/wwwroot/gotax/app/Http/Controllers/
```

### Bước 2: Cấu hình Laravel

**Thêm vào `config/services.php`:**
```php
'id_quick' => [
    'url' => env('ID_QUICK_API_URL', 'http://127.0.0.1:5000/api/go-quick'),
],
```

**Thêm vào `.env`:**
```env
ID_QUICK_API_URL=http://127.0.0.1:5000/api/go-quick
```

**Thêm routes vào `routes/api.php` (nếu dùng Controller):**
```php
use App\Http\Controllers\IDQuickController;

Route::prefix('go-quick')->group(function () {
    Route::get('/health', [IDQuickController::class, 'healthCheck']);
    Route::post('/process-cccd', [IDQuickController::class, 'processCCCD']);
    Route::post('/process-pdf', [IDQuickController::class, 'processPDF']);
    Route::post('/process-excel', [IDQuickController::class, 'processExcel']);
});
```

### Bước 3: Sử dụng trong Laravel

**Cách 1: Upload file ZIP (process-cccd)**
```php
use Illuminate\Support\Facades\Http;

// Upload file zip
$response = Http::attach('file', $zipFileContent, 'images.zip')
    ->timeout(600)
    ->post('http://127.0.0.1:5000/api/go-quick/process-cccd');

$result = $response->json();
```

**Cách 2: Upload 2 ảnh riêng (process-cccd-images)**
```php
use Illuminate\Support\Facades\Http;

$response = Http::attach('mt', $request->file('mt'), 'front.jpg')
    ->attach('ms', $request->file('ms'), 'back.jpg')
    ->timeout(600)
    ->post('http://127.0.0.1:5000/api/go-quick/process-cccd-images');

$result = $response->json();
```

**Cách 3: Gửi base64**
```php
// Encode file thành base64
$zipBytes = file_get_contents($zipPath);
$base64 = base64_encode($zipBytes);

// Gọi API
$response = Http::timeout(600)
    ->post('http://127.0.0.1:5000/api/go-quick/process-cccd', [
        'inp_path' => $base64
    ]);

$result = $response->json();
```

**Cách 4: Process PDF**
```php
$response = Http::attach('file', $request->file('pdf'), 'document.pdf')
    ->timeout(600)
    ->post('http://127.0.0.1:5000/api/go-quick/process-pdf');

$result = $response->json(); // JSON với thông tin CCCD
```

**Cách 5: Process Excel**
```php
$response = Http::attach('file', $request->file('excel'), 'data.xlsx')
    ->timeout(600)
    ->post('http://127.0.0.1:5000/api/go-quick/process-excel');

$result = $response->json(); // JSON với thông tin CCCD
```

**Lưu ý:** Tất cả endpoints đều trả về JSON với thông tin CCCD đã extract (giống nhau):
```php
// Trong route hoặc controller khác
$request->validate([
    'file' => 'required|file|mimes:zip|max:102400'
]);

$response = Http::attach('file', $request->file('file'), 'images.zip')
    ->timeout(600)
    ->post('http://127.0.0.1:5000/api/go-quick/process-cccd');

return response()->json($response->json());
```

---

## 📋 API Endpoints

### Tất cả Tools

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `/api/health` | GET | Health check tất cả tools |

### Tool: go-quick

| Endpoint | Method | Mô tả | Input | Output |
|----------|--------|-------|-------|--------|
| `/api/go-quick/health` | GET | Health check | - | JSON status |
| `/api/go-quick/process-cccd` | POST | Trích xuất CCCD từ ZIP | ZIP file hoặc base64 | JSON với thông tin CCCD |
| `/api/go-quick/process-cccd-images` | POST | Trích xuất CCCD từ 2 ảnh | 2 files: mt, ms | JSON với thông tin CCCD |
| `/api/go-quick/process-pdf` | POST | PDF → PNG → CCCD extractor | PDF file | JSON với thông tin CCCD |
| `/api/go-quick/process-excel` | POST | Excel → Download ảnh → CCCD extractor | Excel file | JSON với thông tin CCCD |

### Tool: go-bot (sẽ có)

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `/api/go-bot/health` | GET | Health check |
| `/api/go-bot/...` | POST | Các endpoints khác |

---

## ➕ Thêm Tool Mới

### Bước 1: Tạo folder tool mới

```bash
mkdir -p tool-go-bot/api
```

### Bước 2: Tạo `tool-go-bot/api/routes.py`

```python
def register_routes(app, prefix):
    @app.route(f'{prefix}/health', methods=['GET'])
    def health_check():
        return jsonify({"status": "success", "message": "Go Bot API is running"})
    
    # Thêm các routes khác...
```

### Bước 3: Đăng ký trong `api_server.py`

Thêm vào dict `TOOLS`:

```python
TOOLS = {
    'go-quick': {...},
    'go-bot': {
        'path': 'tool-go-bot',
        'module': 'tool_go_bot',
        'name': 'Go Bot API'
    },
}
```

### Bước 4: Restart API Server

```bash
sudo systemctl restart tool-apis
```

---

## 🔐 API Key Authentication (Khi dùng domain/public)

**Lưu ý:** Chỉ cần khi expose API ra internet qua domain. Nếu deploy local (127.0.0.1) thì **KHÔNG CẦN**.

### Cách thêm API Key

**1. Thêm vào `api_server.py`:**

```python
# Thêm ở đầu file
API_KEY = os.environ.get('API_KEY', None)  # None = không bật API key

@app.before_request
def check_api_key():
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
```

**2. Set API key trong environment (có 3 cách):**

**Cách 1: Dùng file .env (khuyến nghị cho local):**

```bash
# Tạo file .env trong thư mục gốc
cp .env.example .env

# Sửa file .env
API_KEY=your-secret-key-here
```

**Cách 2: Export trong shell:**

```bash
export API_KEY=your-secret-key-here
```

**Cách 3: Set trong systemd service:**

```ini
[Service]
Environment="API_KEY=your-secret-key-here"
```

**3. Laravel gọi với API key:**

```php
$response = Http::withHeaders([
    'X-API-Key' => env('TOOL_API_KEY')
])->post('http://your-domain.com/api/go-quick/process-cccd', [
    'inp_path' => $base64
]);
```

**4. Test với curl:**

```bash
curl -X POST http://your-domain.com/api/go-quick/process-cccd \
  -H "X-API-Key: your-secret-key" \
  -H "Content-Type: application/json" \
  -d '{"inp_path": "..."}'
```

---

## 🐛 Troubleshooting

### Lỗi: Module not found

```bash
# Kiểm tra dependencies
pip list

# Cài lại dependencies
pip install -r requirements.txt
pip install -r tool-go-quick/api/requirements.txt
```

### Lỗi: Port đã sử dụng

```bash
# Tìm process
lsof -i :5000

# Kill process
kill -9 <PID>
```

### Lỗi: Tool không load được

- Kiểm tra `api_server.py` có đăng ký tool trong `TOOLS` dict
- Kiểm tra `tool-xxx/api/routes.py` có function `register_routes`
- Xem logs: `journalctl -u tool-apis -f`

---

## ✅ Checklist Deploy

- [ ] Upload tất cả files lên server
- [ ] Cài đặt Python và dependencies
- [ ] Sửa đường dẫn trong main.py (nếu cần)
- [ ] Cấu hình Supervisor/Systemd
- [ ] Cấu hình Nginx reverse proxy
- [ ] Set permissions
- [ ] Test API: `curl http://127.0.0.1:5000/api/health`
- [ ] Copy Laravel files
- [ ] Cấu hình Laravel .env
- [ ] Test từ Laravel

---

**Lưu ý:** 
- Model files có thể rất lớn (~500MB-1GB)
- Xử lý có thể mất vài phút, cần timeout đủ lớn
- API server chỉ chạy trên localhost (127.0.0.1) để bảo mật


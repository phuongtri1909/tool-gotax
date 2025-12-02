# Tool Go-Soft: Tax Crawler API v2.0

API crawl dữ liệu từ hệ thống thuế điện tử (thuedientu.gdt.gov.vn).

## 🚀 Đã Migrate sang Playwright + httpx

### Công nghệ mới (v2.0)

| Thành phần | Cũ (v1.0) | Mới (v2.0) | Cải thiện |
|------------|-----------|------------|-----------|
| Browser Automation | Selenium | **Playwright** | Nhanh hơn 2-3x |
| HTTP Client | Browser | **httpx** | Nhanh hơn 10-50x |
| Web Framework | Flask | **Quart** | Async native |
| Concurrency | Threading | **asyncio** | Hiệu quả hơn |

### Tại sao thay đổi?

1. **Tốc độ**: 
   - Playwright nhanh hơn Selenium 2-3x
   - httpx cho HTTP requests nhanh hơn browser 10-50x
   - Downloads song song (parallel)

2. **RAM**: 
   - Selenium: ~300-500MB/session
   - Playwright: ~200-300MB/session
   - httpx: ~10MB/session

3. **Async**: 
   - Xử lý nhiều requests đồng thời
   - Không blocking I/O

## 📦 Cài đặt

```bash
# Cài dependencies
pip install -r requirements.txt

# Cài Playwright browsers
playwright install chromium
```

## 🏃 Chạy Server

### Development
```bash
python api_server.py
```

### Production
```bash
hypercorn api_server:app --bind 0.0.0.0:5000
```

## 📚 API Endpoints

### Session Management

#### Tạo Session
```http
POST /api/go-soft/session/create
```

Response:
```json
{
    "status": "success",
    "session_id": "uuid-here"
}
```

#### Đóng Session
```http
POST /api/go-soft/session/close
Content-Type: application/json

{
    "session_id": "uuid-here"
}
```

#### Kiểm tra Session
```http
GET /api/go-soft/session/status?session_id=uuid-here
```

### Login Flow

#### 1. Khởi tạo Login (lấy Captcha)
```http
POST /api/go-soft/login/init
Content-Type: application/json

{
    "session_id": "uuid-here"
}
```

Response:
```json
{
    "status": "success",
    "captcha_base64": "base64-image-data"
}
```

#### 2. Submit Login
```http
POST /api/go-soft/login/submit
Content-Type: application/json

{
    "session_id": "uuid-here",
    "username": "mst",
    "password": "pass",
    "captcha": "captcha-text"
}
```

### Crawl APIs

#### 🆕 Lấy danh sách loại tờ khai
```http
GET /api/go-soft/tokhai/types?session_id=uuid-here
```

Response:
```json
{
    "status": "success",
    "tokhai_types": [
        {"value": "00", "label": "--Tất cả--"},
        {"value": "842", "label": "01/GTGT - TỜ KHAI THUẾ GIÁ TRỊ GIA TĂNG (TT80/2021)"},
        {"value": "892", "label": "03/TNDN - Tờ khai quyết toán thuế TNDN (TT80/2021)"},
        ...
    ]
}
```

#### 🆕 Lấy thông tin tờ khai (KHÔNG download)
**API mới - Tách riêng để hiển thị danh sách trước, user chọn tải sau**

```http
POST /api/go-soft/crawl/tokhai/info
Content-Type: application/json

{
    "session_id": "uuid-here",
    "tokhai_type": "842",  // hoặc "01/GTGT", "00" (Tất cả), hoặc null
    "start_date": "01/01/2023",
    "end_date": "31/12/2023"
}
```

**Lưu ý**: 
- `tokhai_type` có thể là:
  - `"00"` hoặc `null` → Crawl **TẤT CẢ** loại tờ khai
  - `"842"` → Loại cụ thể (dùng value từ `/tokhai/types`)
  - `"01/GTGT"` → Tên loại tờ khai

Response:
```json
{
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
        }
    ]
}
```

**Ưu điểm**:
- ✅ Nhanh - chỉ parse thông tin, không download file
- ✅ Hiển thị danh sách ngay, user chọn tải sau
- ✅ Dùng để check thiếu tờ khai nào (tương lai)

#### Crawl Tờ Khai (Streaming) - API cũ (vẫn hoạt động)
```http
POST /api/go-soft/crawl/tokhai
Content-Type: application/json

{
    "session_id": "uuid-here",
    "tokhai_type": "842",  // hoặc "01/GTGT", "00" (Tất cả), hoặc null
    "start_date": "01/01/2023",
    "end_date": "31/12/2023"
}
```

**Lưu ý**: 
- `tokhai_type` có thể là:
  - `"00"` hoặc `null` → Crawl **TẤT CẢ** loại tờ khai
  - `"842"` → Loại cụ thể (dùng value từ `/tokhai/types`)
  - `"01/GTGT"` → Tên loại tờ khai

Returns: Server-Sent Events (SSE)
```
data: {"type": "progress", "current": 10, "message": "..."}
data: {"type": "item", "data": {...}}
data: {"type": "complete", "total": 100, "zip_base64": "...", "files_count": 100}
```

#### Crawl Tờ Khai (Sync) - API cũ (vẫn hoạt động)
```http
POST /api/go-soft/crawl/tokhai/sync
```
Trả về JSON thay vì SSE.

Response:
```json
{
    "status": "success",
    "total": 39,  // Số file thực tế trong ZIP
    "results_count": 44,  // Số items đã tìm thấy
    "files_count": 39,
    "files": [{"name": "...", "size": 1234}],
    "zip_base64": "...",
    "zip_filename": "tokhai_842_01012023_31122023.zip",
    "tokhai_type": "842",
    "is_all_types": false
}
```

**⚠️ Lưu ý**: 
- Khi cần **tải file XML** → Dùng API này (`/crawl/tokhai/sync` hoặc `/crawl/tokhai`)
- Khi chỉ cần **xem thông tin** (không tải) → Dùng `/crawl/tokhai/info` để nhanh hơn

#### Crawl Thông Báo
```http
POST /api/go-soft/crawl/thongbao
POST /api/go-soft/crawl/thongbao/sync

{
    "session_id": "uuid-here",
    "start_date": "01/01/2023",
    "end_date": "31/12/2023"
}
```

**Tính năng**:
- ✅ Hỗ trợ phân trang (tự động crawl tất cả trang)
- ✅ Batch download (5 file cùng lúc)
- ✅ Phân loại thông báo trong kết quả:
  - "Tiếp nhận"
  - "Xác nhận"
  - "Chấp nhận"
  - "Không chấp nhận"

Response:
```json
{
    "status": "success",
    "total": 20,  // Số file thực tế
    "files_count": 20,
    "results": [
        {
            "ma_giao_dich": "...",
            "ten_thong_bao": "V/v: Tiếp nhận hồ sơ thuế điện tử TT19",
            "ma_thong_bao": "...",
            "ngay_thong_bao": "..."
        }
    ],
    "zip_base64": "...",
    "zip_filename": "thongbao_01012023_31122023.zip"
}
```

#### Crawl Giấy Nộp Tiền
```http
POST /api/go-soft/crawl/giaynoptien
POST /api/go-soft/crawl/giaynoptien/sync

{
    "session_id": "uuid-here",
    "start_date": "01/01/2023",
    "end_date": "31/12/2023"
}
```

**Tính năng**:
- ✅ Hỗ trợ phân trang
- ✅ Batch download từ nhiều cột (17-20)

### 🆕 Batch Crawl (Parallel - v2.0)
```http
POST /api/go-soft/crawl/batch
Content-Type: application/json

{
    "session_id": "uuid-here",
    "start_date": "01/01/2023",
    "end_date": "31/12/2023",
    "crawl_types": ["tokhai", "thongbao", "giaynoptien"],  // Phải có ít nhất 2 types
    "tokhai_type": "842",  // hoặc "00" (Tất cả) - chỉ cần khi crawl tokhai
    "download_files": true  // Optional: true để download file (trả về zip_base64), false chỉ lấy thông tin
}
```

**⚠️ Lưu ý quan trọng**:
- **API này chỉ nên dùng khi cần crawl từ 2 loại trở lên** (ví dụ: cả tờ khai + thông báo)
- **Nếu chỉ cần 1 loại**, dùng API riêng sẽ đơn giản hơn:
  - 1 loại tờ khai → `/crawl/tokhai` hoặc `/crawl/tokhai/sync`
  - 1 loại thông báo → `/crawl/thongbao` hoặc `/crawl/thongbao/sync`
  - 1 loại giấy nộp → `/crawl/giaynoptien` hoặc `/crawl/giaynoptien/sync`

**Ưu điểm**: Khi crawl nhiều loại cùng lúc, chạy song song nên nhanh hơn rất nhiều so với gọi tuần tự.

**Response (khi `download_files: false` - chỉ lấy thông tin)**:
```json
{
    "status": "success",
    "data": {
        "tokhai": {
            "total": 10,
            "results": [
                {
                    "id": "11320250305601017",
                    "name": "01/GTGT (TT80/2021)",
                    "ky_tinh_thue": "Q1/2024",
                    "loai": "Chính thức",
                    "ngay_nop": "25/03/2025 15:22:00",
                    "trang_thai": "accepted",
                    "file_name": "01_GTGT (TT80_2021) -Q1_2024 -L1 -Chinh thuc -(11320250305601017) -[25-03-2025 15-22-00] [Chap nhan].xml"
                }
            ]
        },
        "thongbao": {
            "total": 5,
            "results": [...]
        }
    }
}
```

**Response (khi `download_files: true` - có file download)**:
```json
{
    "status": "success",
    "data": {
        "tokhai": {
            "total": 10,
            "results": [...],
            "zip_base64": "base64-encoded-zip-data",
            "zip_filename": "tokhai_842_01012023_31122023.zip",
            "files_count": 10,
            "total_size": 123456,
            "files": [
                {"name": "11320250305601017.xml", "size": 12345}
            ]
        },
        "thongbao": {
            "total": 5,
            "results": [...],
            "zip_base64": "base64-encoded-zip-data",
            "zip_filename": "thongbao_01012023_31122023.zip",
            "files_count": 5,
            "total_size": 67890,
            "files": [...]
        }
    }
}
```

**Lưu ý**: 
- Mặc định `download_files: false` → chỉ trả về `results` (thông tin)
- Nếu set `download_files: true` → trả về cả `zip_base64` cho từng loại (có thể download file ngay)

## 🎯 Tính năng nổi bật

### 1. Hỗ trợ "Tất cả" loại tờ khai
```json
{
    "tokhai_type": "00"  // hoặc null, hoặc không truyền
}
```
→ Crawl **TẤT CẢ** loại tờ khai trong khoảng thời gian

### 2. Phân trang tự động
- ✅ Tờ khai: Tự động crawl tất cả trang
- ✅ Thông báo: Tự động crawl tất cả trang  
- ✅ Giấy nộp tiền: Tự động crawl tất cả trang

### 3. Batch Download
- Download **5 file cùng lúc** (concurrent)
- Tốc độ nhanh hơn **3-5x** so với tuần tự

### 4. Đếm chính xác
- `total` = Số file thực tế trong ZIP (không lệch)
- `results_count` = Số items đã tìm thấy
- `files_count` = Số file đã download thành công

### Convert XML to Excel
```http
POST /api/go-soft/convert/xml2xlsx
Content-Type: application/json

{
    "zip_base64": "base64-zip-containing-xmls"
}
```

## 🔧 Cấu trúc Code

```
tool-go-soft/
├── api/
│   └── routes.py          # API routes (Quart async)
├── services/
│   ├── session_manager.py # Playwright session management
│   └── tax_crawler.py     # Hybrid crawler (Playwright + httpx)
├── requirements.txt
└── README.md
```

## 🔄 Flow Hoạt động (v2.0)

```
┌─────────────────────────────────────────────────────────────┐
│                        CLIENT                                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Quart API Server                         │
│                      (async routes)                          │
└─────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
┌───────────────────┐ ┌───────────────┐ ┌───────────────────┐
│   Session Manager │ │  Tax Crawler  │ │     Converter     │
│   (Playwright)    │ │   (Hybrid)    │ │     (openpyxl)    │
└───────────────────┘ └───────────────┘ └───────────────────┘
            │                 │
            ▼                 ▼
┌───────────────────┐ ┌───────────────────────────────────────┐
│    Playwright     │ │              httpx                     │
│   (Login only)    │ │   (Crawl data - 10-50x faster!)       │
└───────────────────┘ └───────────────────────────────────────┘
            │                 │
            └────────┬────────┘
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              thuedientu.gdt.gov.vn                           │
└─────────────────────────────────────────────────────────────┘
```

## ⚡ So sánh hiệu năng

| Tác vụ | Selenium (v1) | Playwright + httpx (v2.1) |
|--------|---------------|---------------------------|
| Login | ~5s | ~3s |
| Crawl 100 tờ khai | ~120s | ~15s |
| Download 100 XML | ~60s | ~12s (batch 5 concurrent) |
| Crawl với phân trang | Không hỗ trợ | ✅ Tự động crawl tất cả trang |
| RAM usage | 400MB | 150MB |
| Độ chính xác total | ❌ Lệch | ✅ Chính xác 100% |

## 🔒 Security

- API Key authentication (optional)
- Set `API_KEY` environment variable để bật
- Không set = không yêu cầu key (local deployment)

```bash
export API_KEY=your-secret-key
```

## 📝 Changelog

### v2.1 (Current)
- ✅ **Hỗ trợ "Tất cả" loại tờ khai** (`tokhai_type: "00"` hoặc `null`)
- ✅ **API lấy danh sách loại tờ khai** (`GET /tokhai/types`)
- ✅ **Sửa lỗi đếm total** - Total = số file thực tế trong ZIP (chính xác)
- ✅ **Hỗ trợ phân trang cho Thông báo & Giấy nộp tiền** (tự động crawl tất cả trang)
- ✅ **Batch download tối ưu** - Download 5 file cùng lúc (nhanh hơn 3-5x)
- ✅ **Phân loại thông báo** - Tự động phân loại: Tiếp nhận, Xác nhận, Chấp nhận, Không chấp nhận
- ✅ **Response format cải thiện**:
  - `total`: Số file thực tế trong ZIP
  - `results_count`: Số items đã tìm thấy
  - `total_rows_processed`: Số rows đã xử lý (debug)

### v2.0
- ✅ Migrate từ Selenium sang Playwright
- ✅ Thêm httpx cho HTTP requests nhanh
- ✅ Migrate từ Flask sang Quart (async)
- ✅ Parallel downloads
- ✅ Batch crawl API
- ✅ Giảm RAM usage 50%
- ✅ Tăng tốc độ 5-10x

### v1.0
- Selenium + Flask
- Sequential processing

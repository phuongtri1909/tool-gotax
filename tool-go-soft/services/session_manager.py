"""
Session Manager - Quản lý các session Playwright cho nhiều user đồng thời
Đã migrate từ Selenium sang Playwright + httpx để tối ưu tốc độ

Ưu điểm so với Selenium:
- Async native - chạy song song dễ dàng
- Nhanh hơn 2-3x
- Auto-wait thông minh (không cần time.sleep())
- RAM ít hơn
"""
import os
import uuid
import asyncio
import logging
import re
import base64
import tempfile
import shutil
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SessionData:
    """Dữ liệu của một session"""
    session_id: str
    browser: Browser
    context: BrowserContext
    page: Page
    created_at: datetime = field(default_factory=datetime.now)
    last_active: datetime = field(default_factory=datetime.now)
    username: Optional[str] = None
    is_logged_in: bool = False
    dse_session_id: Optional[str] = None
    cookies: Dict[str, str] = field(default_factory=dict)
    download_path: Optional[str] = None  # Folder để lưu file download
    
    def update_activity(self):
        self.last_active = datetime.now()


class SessionManager:
    """
    Quản lý các session Playwright (Async)
    - Tạo session mới với Chromium headless
    - Theo dõi và cleanup session hết hạn
    - Async-safe cho multi-user
    - Lưu cookies để dùng với httpx sau khi login
    """
    
    SESSION_TIMEOUT_MINUTES = 30
    CLEANUP_INTERVAL_SECONDS = 60
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        """Singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if getattr(self, '_initialized', False):
            return
        self._initialized = True
        
        self._sessions: Dict[str, SessionData] = {}
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
        logger.info("SessionManager initialized (Playwright async version)")
    
    async def _ensure_browser(self):
        """Đảm bảo browser đã được khởi tạo"""
        if self._playwright is None:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-gpu',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-animations',
                    '--disable-notifications',
                ]
            )
            logger.info("Playwright browser launched")
            
            # Start cleanup task
            if self._cleanup_task is None:
                self._cleanup_task = asyncio.create_task(self._cleanup_expired_sessions())
    
    async def _cleanup_expired_sessions(self):
        """Background task để cleanup sessions hết hạn"""
        while True:
            await asyncio.sleep(self.CLEANUP_INTERVAL_SECONDS)
            try:
                expired_sessions = []
                cutoff_time = datetime.now() - timedelta(minutes=self.SESSION_TIMEOUT_MINUTES)
                
                for session_id, session_data in list(self._sessions.items()):
                    if session_data.last_active < cutoff_time:
                        expired_sessions.append(session_id)
                
                for session_id in expired_sessions:
                    logger.info(f"Cleaning up expired session: {session_id}")
                    await self.close_session(session_id)
                    
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
    
    async def create_session(self) -> str:
        """
        Tạo session mới với Playwright context
        Returns: session_id
        """
        await self._ensure_browser()
        
        session_id = str(uuid.uuid4())
        
        # Tạo download folder cho session này
        import tempfile
        download_path = tempfile.mkdtemp(prefix=f"taxcrawl_{session_id[:8]}_")
        
        # Mỗi session có context riêng (isolated cookies, storage)
        context = await self._browser.new_context(
            ignore_https_errors=True,
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            accept_downloads=True
        )
        
        # Set download behavior - files sẽ được download vào folder này
        # Lưu download_path vào session để dùng sau
        
        # Enable request/response tracking
        await context.route("**/*", lambda route: route.continue_())
        
        page = await context.new_page()
        
        session_data = SessionData(
            session_id=session_id,
            browser=self._browser,
            context=context,
            page=page,
            download_path=download_path
        )
        
        self._sessions[session_id] = session_data
        
        logger.info(f"Created new session: {session_id}")
        return session_id
    
    def get_session(self, session_id: str) -> Optional[SessionData]:
        """Lấy session data theo ID"""
        session = self._sessions.get(session_id)
        if session:
            session.update_activity()
        return session
    
    async def close_session(self, session_id: str) -> bool:
        """Đóng và xóa session"""
        session = self._sessions.pop(session_id, None)
        
        if session:
            try:
                await session.page.close()
                await session.context.close()
                
                # Cleanup download folder
                if session.download_path and os.path.exists(session.download_path):
                    import shutil
                    shutil.rmtree(session.download_path, ignore_errors=True)
                    
                logger.info(f"Closed session: {session_id}")
                return True
            except Exception as e:
                logger.error(f"Error closing session {session_id}: {e}")
        return False
    
    def get_active_session_count(self) -> int:
        """Đếm số session đang hoạt động"""
        return len(self._sessions)
    
    async def init_login_page(self, session_id: str) -> Dict[str, Any]:
        """
        Lấy captcha: Navigate đến trang login (để có cookies) rồi fetch ImageServlet với timestamp
        Returns: {success, captcha_base64, error}
        """
        session = self.get_session(session_id)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        page = session.page
        
        try:
            # Check xem đã ở trang login chưa (có cookies chưa)
            is_login_page = False
            try:
                if await page.locator('input#_userName, input[name="_userName"]').count() > 0:
                    is_login_page = True
                    logger.info("Login page already loaded, using existing cookies")
            except:
                pass
            
            # Nếu chưa có trang login, navigate đến để có cookies
            if not is_login_page:
                # Navigate to main page
                await page.goto('https://thuedientu.gdt.gov.vn/', wait_until='domcontentloaded')
                
                # Click vào "DOANH NGHIỆP"
                retry_count = 0
                while retry_count < 3:
                    try:
                        dn_btn = page.locator('a:has(span:text("DOANH NGHIỆP"))')
                        await dn_btn.wait_for(timeout=5000, state='visible')
                        await dn_btn.click()
                        break
                    except:
                        await page.goto('https://thuedientu.gdt.gov.vn/', wait_until='domcontentloaded')
                        retry_count += 1
                        await asyncio.sleep(0.3)
                
                if retry_count >= 3:
                    return {"success": False, "error": "Cannot load login page"}
                
                # Handle alert nếu có
                async def handle_dialog(dialog):
                    try:
                        await dialog.accept()
                    except:
                        pass
                
                page.on("dialog", lambda dialog: asyncio.create_task(handle_dialog(dialog)))
                await asyncio.sleep(0.3)
                
                # Click login button
                login_div = page.locator('div.dangnhap')
                spans = login_div.locator('span')
                if await spans.count() >= 2:
                    await spans.nth(1).click()
                
                await asyncio.sleep(0.3)
                
                # Đóng popup thư ngõ nếu có
                try:
                    await page.evaluate("popupThungo();")
                except:
                    pass
                
                # Click đăng nhập bằng tài khoản thuế điện tử
                try:
                    element = page.locator("text='Đăng nhập bằng tài khoản Thuế điện tử'")
                    await element.click(timeout=5000)
                except:
                    pass
                
                # Đợi form login xuất hiện
                try:
                    await page.wait_for_selector('input#_userName, input[name="_userName"]', timeout=5000, state='visible')
                except:
                    pass
            
            # Bây giờ đã có cookies từ trang login, fetch ImageServlet với timestamp
            import time
            timenow = int(time.time() * 1000)  # milliseconds
            captcha_url = f"https://thuedientu.gdt.gov.vn/etaxnnt/servlet/ImageServlet?d={timenow}"
            
            # Fetch image với cookies từ browser context (QUAN TRỌNG: cần cookies)
            response = await page.request.get(captcha_url)
            
            if response.status == 200:
                image_bytes = await response.body()
                base64_data = base64.b64encode(image_bytes).decode('utf-8')
                logger.info(f"Captcha fetched from {captcha_url[:80]}...")
                return {"success": True, "captcha_base64": base64_data}
            else:
                return {"success": False, "error": f"Failed to fetch captcha: HTTP {response.status}"}
            
        except Exception as e:
            logger.error(f"Error in init_login_page: {e}")
            return {"success": False, "error": str(e)}
    
    async def reload_captcha(self, session_id: str) -> Dict[str, Any]:
        """
        Reload captcha (giống init, chỉ fetch ImageServlet với timestamp mới)
        Returns: {success, captcha_base64, error}
        """
        return await self.init_login_page(session_id)
    
    async def _get_captcha_base64(self, page: Page) -> Optional[str]:
        """
        Lấy captcha image dạng base64 từ URL trực tiếp (NHANH NHẤT)
        Fetch trực tiếp từ ImageServlet URL bằng Playwright request
        """
        try:
            # Tìm captcha image element để lấy URL
            captcha_img = page.locator('img#safecode, img[src*="ImageServlet"]')
            
            if await captcha_img.count() > 0:
                # Lấy src của image
                img_src = await captcha_img.first.get_attribute('src')
                
                if img_src:
                    # Chuyển thành absolute URL
                    if img_src.startswith('/'):
                        img_src = f"https://thuedientu.gdt.gov.vn{img_src}"
                    elif not img_src.startswith('http'):
                        img_src = f"https://thuedientu.gdt.gov.vn/etaxnnt{img_src}"
                    
                    # Tối ưu: Fetch image trực tiếp bằng Playwright request (NHANH HƠN JavaScript fetch)
                    try:
                        # Dùng page.request để fetch image với cookies tự động
                        response = await page.request.get(img_src)
                        
                        if response.status == 200:
                            # Lấy image bytes
                            image_bytes = await response.body()
                            # Convert sang base64
                            base64_data = base64.b64encode(image_bytes).decode('utf-8')
                            logger.info(f"Captcha fetched successfully from {img_src[:80]}...")
                            return base64_data
                        else:
                            logger.warning(f"Failed to fetch captcha: HTTP {response.status}")
                    except Exception as e:
                        logger.warning(f"Error fetching captcha via request: {e}, trying screenshot fallback")
            
            # Fallback: Screenshot nếu request không được
            captcha_img = page.locator('img#safecode, img[src*="ImageServlet"]')
            if await captcha_img.count() > 0:
                captcha_bytes = await captcha_img.first.screenshot()
                return base64.b64encode(captcha_bytes).decode('utf-8')
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting captcha: {e}")
            return None
    
    async def submit_login(self, session_id: str, username: str, password: str, captcha: str) -> Dict[str, Any]:
        """
        Submit login với username, password và captcha
        Returns: {success, error, cookies}
        """
        session = self.get_session(session_id)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        page = session.page
        
        try:
            # Nhập username (theo frm_login.html: input#_userName hoặc input[name="_userName"])
            user_input = page.locator('input#_userName, input[name="_userName"]').first
            await user_input.wait_for(timeout=5000)
            await user_input.fill(username)
            
            # Nhập password (theo frm_login.html: input#password hoặc input[name="_password"])
            pass_input = page.locator('input#password, input[name="_password"], input[type="password"]').first
            await pass_input.wait_for(timeout=5000)
            await pass_input.fill(password)
            
            # Nhập captcha (theo frm_login.html: input#vcode hoặc input[name="_verifyCode"])
            captcha_input = page.locator('input#vcode, input[name="_verifyCode"]').first
            await captcha_input.wait_for(timeout=5000)
            await captcha_input.fill(captcha)
            
            await asyncio.sleep(0.3)
            
            # Intercept network để lấy dse_sessionId
            dse_session_id = None
            
            async def capture_session_id(request):
                nonlocal dse_session_id
                url = request.url
                match = re.search(r"dse_sessionId=([^&]+)", url)
                if match:
                    dse_session_id = match.group(1)
            
            page.on("request", capture_session_id)
            
            # Click login button
            login_btn = page.locator('input#dangnhap, input[type="button"][value="Đăng nhập"], input[type="submit"]').first
            await login_btn.click()
            
            # Wait for navigation hoặc error message
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
            except:
                pass  # Có thể không navigate nếu sai thông tin
            
            await asyncio.sleep(1.5)
            
            # Kiểm tra lỗi từ HTML (giống frm_login.html)
            # Tìm <span style="color: #fcdf00; font-size: 12px;"> trong <tr>
            error_text = None
            try:
                # Tìm span có style color: #fcdf00 (màu vàng - màu lỗi)
                error_span = page.locator('span[style*="color: #fcdf00"], span[style*="color:#fcdf00"]')
                if await error_span.count() > 0:
                    error_text = await error_span.first.text_content()
                    error_text = error_text.strip() if error_text else None
                    logger.warning(f"Login error detected: {error_text}")
            except Exception as e:
                logger.debug(f"Error checking error message: {e}")
            
            # Kiểm tra login thành công
            current_url = page.url
            if error_text or ('login' in current_url.lower() or 'dang-nhap' in current_url.lower()):
                # Còn ở trang login hoặc có thông báo lỗi -> sai thông tin
                if not error_text:
                    # Thử tìm error message khác
                    try:
                        error_elem = page.locator('.error, .alert-danger, span[style*="color"]')
                        if await error_elem.count() > 0:
                            error_text = await error_elem.first.text_content()
                    except:
                        pass
                
                return {
                    "success": False, 
                    "error": error_text or "Sai tài khoản, mật khẩu hoặc captcha",
                    "dse_session_id": None
                }
            
            # Lưu cookies để dùng với httpx
            cookies = await session.context.cookies()
            session.cookies = {c['name']: c['value'] for c in cookies}
            
            # Cập nhật session data
            session.username = username
            session.is_logged_in = True
            session.dse_session_id = dse_session_id
            
            logger.info(f"Login successful for session {session_id}, user: {username}")
            
            return {
                "success": True, 
                "dse_session_id": dse_session_id,
                "cookies": session.cookies
            }
            
        except Exception as e:
            logger.error(f"Error in submit_login: {e}")
            return {"success": False, "error": str(e)}
    
    async def get_cookies_for_httpx(self, session_id: str) -> Optional[Dict[str, str]]:
        """
        Lấy cookies để dùng với httpx client
        Sau khi login, có thể dùng httpx để crawl nhanh hơn
        """
        session = self.get_session(session_id)
        if not session or not session.is_logged_in:
            return None
        
        # Refresh cookies
        cookies = await session.context.cookies()
        session.cookies = {c['name']: c['value'] for c in cookies}
        
        return session.cookies
    
    async def navigate_to_search(self, session_id: str, search_type: str = "tokhai") -> Dict[str, Any]:
        """
        Navigate đến trang tra cứu tương ứng
        search_type: "tokhai", "thongbao", "giaynopthue"
        """
        session = self.get_session(session_id)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        if not session.is_logged_in:
            return {"success": False, "error": "Not logged in"}
        
        page = session.page
        
        try:
            # Chờ menu load
            await page.wait_for_selector('//ul/li[3]', timeout=10000)
            
            # Click menu Kế toán thuế
            kthue = page.locator('//ul/li[3]').first
            await kthue.click()
            
            await asyncio.sleep(0.5)
            
            if search_type == "tokhai":
                tcuu = page.locator('//ul/li[8]').first
                await tcuu.click()
                
            elif search_type == "thongbao":
                tcuu = page.locator('//ul/li[9]').first
                await tcuu.click()
                
            elif search_type == "giaynopthue":
                await page.locator('//ul/li[4]').first.click()
                await page.locator('//ul/li[4]').first.click()
            
            await asyncio.sleep(1)
            
            # Switch to mainframe
            frame = page.frame('mainframe')
            if frame:
                # Lưu reference đến frame
                session.main_frame = frame
            
            return {"success": True}
            
        except Exception as e:
            logger.error(f"Error navigating to search: {e}")
            return {"success": False, "error": str(e)}
    
    async def shutdown(self):
        """Shutdown tất cả sessions và browser"""
        logger.info("Shutting down SessionManager...")
        
        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        # Close all sessions
        for session_id in list(self._sessions.keys()):
            await self.close_session(session_id)
        
        # Close browser
        if self._browser:
            await self._browser.close()
        
        if self._playwright:
            await self._playwright.stop()
        
        logger.info("SessionManager shutdown complete")


# Singleton instance
session_manager = SessionManager()

import os
import uuid
import asyncio
import logging
import re
import base64
import tempfile
import shutil
import json
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SessionData:
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
        
        # Lấy context ID để match chính xác (Playwright context có thể có ID)
        # Nếu không có ID, dùng index trong browser.contexts
        try:
            context_index = len(self._browser.contexts) - 1  # Index của context mới tạo
            context_id = str(context_index)  # Dùng index làm ID
        except:
            context_id = None
        
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
    
    async def get_context(self, session_id: str) -> Optional[BrowserContext]:
        """
        Lấy BrowserContext theo session_id
        Worker có thể gọi method này để lấy context và dùng trực tiếp
        """
        session = self.get_session(session_id)
        if session:
            return session.context
        return None
    async def init_login_page(self, session_id: str) -> Dict[str, Any]:
        """
        Lấy captcha: Navigate đến trang login dịch vụ công và fetch captcha
        Returns: {success, captcha_base64, error}
        """
        session = self.get_session(session_id)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        page = session.page
        
        try:
            # Navigate đến trang login dịch vụ công (chỉ để có cookies)
            current_url = page.url
            if '/tthc/login' not in current_url:
                logger.info("Navigating to dichvucong login page...")
                await page.goto('https://dichvucong.gdt.gov.vn/tthc/login', wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(1)  # Đợi page load xong để có cookies
            
            # Lấy captcha từ /tthc/homelogin/getCaptcha với timestamp
            # KHÔNG CẦN đợi form login xuất hiện, chỉ cần có cookies từ trang login
            import time
            timenow = int(time.time() * 1000)  # milliseconds
            captcha_url = f"https://dichvucong.gdt.gov.vn/tthc/homelogin/getCaptcha?{timenow}"
            
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
        session = self.get_session(session_id)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        page = session.page
        
        try:
            # Đảm bảo đang ở trang login
            current_url = page.url
            if '/tthc/login' not in current_url:
                await page.goto('https://dichvucong.gdt.gov.vn/tthc/login', wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(1)
            
            # Đợi page load xong
            try:
                await page.wait_for_load_state('domcontentloaded', timeout=10000)
            except:
                pass
            
            logger.info(f"Attempting login for user: {username}")
            
            # ===== LẤY JSESSIONID TRƯỚC KHI LOGIN =====
            jsessionid_before = None
            try:
                cookies = await session.context.cookies()
                for cookie in cookies:
                    if cookie.get('name') == 'JSESSIONID':
                        jsessionid_before = cookie.get('value')
                        logger.info(f"JSESSIONID BEFORE login: {jsessionid_before}")
                        break
                if not jsessionid_before:
                    logger.info("JSESSIONID BEFORE login: None (no session cookie found)")
            except Exception as e:
                logger.warning(f"Error getting JSESSIONID before login: {e}")
            
            # QUAN TRỌNG: Gọi AJAX bằng JS thay vì Python để đảm bảo encoding giống hệt
            # Vì JS có btoa(unescape(encodeURIComponent())) - khó replicate chính xác trong Python
            
            # Gọi AJAX trực tiếp bằng page.evaluate để lấy response
            # QUAN TRỌNG: Để JS tự encode password để đảm bảo 100% giống code gốc
            try:
                ajax_response = await page.evaluate("""
                    async (credentials) => {
                        const base_url = "/tthc/";
                        const tenDN = credentials.username;
                        const matKhau = credentials.password;
                        const doiTuong = 'DN';
                        
                        // Encode password CHÍNH XÁC như JS gốc
                        const matKhauEncoded = btoa(unescape(encodeURIComponent(matKhau)));
                        
                        return new Promise((resolve) => {
                            $.ajax({
                                type: 'POST',
                                url: base_url + 'loginLDAP',
                                data: {
                                    tenDN: tenDN,
                                    matKhau: matKhauEncoded,
                                    doiTuong: doiTuong
                                },
                                success: function (data) {
                                    resolve({
                                        success: true,
                                        data: data
                                    });
                                },
                                error: function (xhr, error) {
                                    resolve({
                                        success: false,
                                        error: error,
                                        status: xhr.status,
                                        responseText: xhr.responseText
                                    });
                                }
                            });
                        });
                    }
                """, {
                    'username': username,
                    'password': password  # Truyền password gốc, để JS tự encode
                })
                
                logger.info(f"AJAX response: {ajax_response}")
                
                # Check AJAX có thành công không
                if not ajax_response.get('success'):
                    error_msg = ajax_response.get('error', 'Unknown error')
                    logger.error(f"AJAX call failed: {error_msg}")
                    return {
                        "success": False,
                        "error": f"AJAX request failed: {error_msg}"
                    }
                
                # Parse response data
                response_data = ajax_response.get('data', {})
                response_status = response_data.get('status')
                
                logger.info(f"Response status: {response_status}")
                logger.info(f"Response data: {response_data}")
                
                # Xử lý theo response status
                if response_status == '200':
                    # Login thành công → redirect đến /tthc/home
                    logger.info("Login successful (status 200) - navigating to /tthc/home")
                    
                    # Đợi 1 giây để cookies được set
                    await asyncio.sleep(1)
                    
                    # Navigate đến trang home
                    try:
                        await page.goto('https://dichvucong.gdt.gov.vn/tthc/home', 
                                      wait_until='domcontentloaded', 
                                      timeout=30000)
                        logger.info("Successfully navigated to /tthc/home")
                    except Exception as e:
                        logger.warning(f"Error navigating to home: {e}, but login was successful")
                    
                    # Lưu cookies
                    cookies = await session.context.cookies()
                    session.cookies = {c['name']: c['value'] for c in cookies}
                    
                    # ===== LẤY JSESSIONID SAU KHI LOGIN =====
                    jsessionid_after = None
                    try:
                        for cookie in cookies:
                            if cookie.get('name') == 'JSESSIONID':
                                jsessionid_after = cookie.get('value')
                                logger.info(f"JSESSIONID AFTER login: {jsessionid_after}")
                                break
                        if not jsessionid_after:
                            logger.info("JSESSIONID AFTER login: None (no session cookie found)")
                    except Exception as e:
                        logger.warning(f"Error getting JSESSIONID after login: {e}")
                    
                    # So sánh JSESSIONID trước và sau
                    if jsessionid_before and jsessionid_after:
                        if jsessionid_before == jsessionid_after:
                            logger.info("JSESSIONID unchanged - same session maintained")
                        else:
                            logger.warning(f"JSESSIONID changed: {jsessionid_before[:30]}... → {jsessionid_after[:30]}...")
                            logger.warning("JSESSIONID changed indicates logout/session expired - login failed")
                            return {
                                "success": False,
                                "error": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.",
                                "error_code": "SESSION_EXPIRED"
                            }
                    elif jsessionid_after:
                        logger.info("JSESSIONID created after login (new session)")
                    elif jsessionid_before:
                        logger.warning("JSESSIONID lost after login (session expired?)")
                        return {
                            "success": False,
                            "error": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.",
                            "error_code": "SESSION_EXPIRED"
                        }
                    
                    # Cập nhật session data
                    session.username = username
                    session.is_logged_in = True
                    session.dse_session_id = None
                    
                    
                    return {
                        "success": True,
                        "dse_session_id": None,
                        "cookies": session.cookies
                    }
                    
                elif response_status == '201':
                    # Cần thêm bước (chọn MST)
                    logger.info(f"Login requires additional step (status 201): {response_data.get('value')}")
                    
                    # Navigate đến URL được chỉ định
                    redirect_url = response_data.get('value', '')
                    if redirect_url:
                        full_url = f"https://dichvucong.gdt.gov.vn/tthc/{redirect_url}"
                        try:
                            await page.goto(full_url, wait_until='domcontentloaded', timeout=30000)
                            logger.info(f"Navigated to: {full_url}")
                        except Exception as e:
                            logger.warning(f"Error navigating to {full_url}: {e}")
                    
                    # Lưu cookies
                    cookies = await session.context.cookies()
                    session.cookies = {c['name']: c['value'] for c in cookies}
                    
                    # ===== LẤY JSESSIONID SAU KHI LOGIN (status 201) =====
                    jsessionid_after = None
                    try:
                        for cookie in cookies:
                            if cookie.get('name') == 'JSESSIONID':
                                jsessionid_after = cookie.get('value')
                                logger.info(f"JSESSIONID AFTER login (status 201): {jsessionid_after}")
                                break
                        if not jsessionid_after:
                            logger.info("JSESSIONID AFTER login (status 201): None (no session cookie found)")
                    except Exception as e:
                        logger.warning(f"Error getting JSESSIONID after login (status 201): {e}")
                    
                    # So sánh JSESSIONID trước và sau
                    if jsessionid_before and jsessionid_after:
                        if jsessionid_before == jsessionid_after:
                            logger.info("JSESSIONID unchanged - same session maintained (status 201)")
                        else:
                            logger.warning(f"JSESSIONID changed (status 201): {jsessionid_before[:30]}... → {jsessionid_after[:30]}...")
                            logger.warning("JSESSIONID changed indicates logout/session expired - login failed")
                            return {
                                "success": False,
                                "error": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.",
                                "error_code": "SESSION_EXPIRED"
                            }
                    elif jsessionid_after:
                        logger.info("JSESSIONID created after login (new session, status 201)")
                    elif jsessionid_before:
                        logger.warning("JSESSIONID lost after login (session expired?, status 201)")
                        return {
                            "success": False,
                            "error": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại.",
                            "error_code": "SESSION_EXPIRED"
                        }
                    
                    session.username = username
                    session.is_logged_in = True
                    
                    
                    return {
                        "success": True,
                        "requires_mst_selection": True,
                        "redirect_url": redirect_url,
                        "dse_session_id": None,
                        "cookies": session.cookies
                    }
                    
                else:
                    # Login failed
                    error_desc = response_data.get('desc', 'Đăng nhập thất bại')
                    logger.warning(f"Login failed: {error_desc}")
                    
                    return {
                        "success": False,
                        "error": error_desc
                    }
                    
            except Exception as e:
                logger.error(f"Error during AJAX call: {e}")
                return {
                    "success": False,
                    "error": f"Error during login: {str(e)}"
                }
            
        except Exception as e:
            logger.error(f"Error in submit_login: {e}")
            return {"success": False, "error": str(e)}
  
    async def get_cookies_for_httpx(self, session_id: str) -> Optional[Dict[str, str]]:
        session = self.get_session(session_id)
        if not session or not session.is_logged_in:
            return None
        
        # Refresh cookies
        cookies = await session.context.cookies()
        session.cookies = {c['name']: c['value'] for c in cookies}
        
        return session.cookies
    
    async def navigate_to_search(self, session_id: str, search_type: str = "tokhai") -> Dict[str, Any]:
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

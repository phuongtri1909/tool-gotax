"""
Tax Crawler Service - Core logic để crawl dữ liệu từ thuedientu.gdt.gov.vn
Đã migrate sang Playwright + httpx hybrid để tối ưu tốc độ

Strategy:
- Dùng Playwright cho: Login (captcha), Navigation phức tạp
- Dùng httpx cho: Crawl data (nhanh hơn 10-50x so với browser)
"""
import os
import asyncio
import base64
import logging
import tempfile
import shutil
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, AsyncGenerator
from io import BytesIO
import zipfile

import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
from openpyxl import Workbook

# Suppress XMLParsedAsHTMLWarning khi parse XML với html.parser
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
from openpyxl.styles import Font, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.styles.numbers import FORMAT_NUMBER_COMMA_SEPARATED1

from .session_manager import SessionManager, SessionData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Mapping loại tờ khai
TOKHAI_TYPES = {
    "01/GTGT": "01/GTGT",
    "01/GTGT (TT80/2021)": "01/GTGT (TT80/2021)",
    "05/QTT-TNCN": "05/QTT-TNCN",
    "03/TNDN": "03/TNDN",
    "01A/TNDN": "01A/TNDN",
    "01B/TNDN": "01B/TNDN",
    "02/TNDN": "02/TNDN",
    "05/KK-TNCN": "05/KK-TNCN",
    "06/KK-TNCN": "06/KK-TNCN",
    "01/MBAI": "01/MBAI",
    "01/LPMB": "01/LPMB",
}

# Base URL
BASE_URL = "https://thuedientu.gdt.gov.vn"


class TaxCrawlerService:
    """
    Service xử lý việc crawl dữ liệu thuế (Async version)
    
    Hybrid approach:
    - Playwright: Xử lý login, navigation, JavaScript-heavy pages
    - httpx: Crawl data nhanh với HTTP requests thuần
    """
    
    def __init__(self, session_manager: SessionManager):
        self.session_manager = session_manager
        self._http_clients: Dict[str, httpx.AsyncClient] = {}
    
    async def _get_http_client(self, session_id: str) -> Optional[httpx.AsyncClient]:
        """
        Lấy hoặc tạo httpx client với cookies từ session
        Dùng để crawl nhanh sau khi login
        """
        session = self.session_manager.get_session(session_id)
        if not session or not session.is_logged_in:
            return None
        
        if session_id not in self._http_clients:
            # Lấy cookies từ session
            cookies = await self.session_manager.get_cookies_for_httpx(session_id)
            if not cookies:
                return None
            
            # Tạo httpx client với cookies
            self._http_clients[session_id] = httpx.AsyncClient(
                cookies=cookies,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
                },
                timeout=30.0,
                verify=False,  # Ignore SSL (same as browser)
                follow_redirects=True
            )
        
        return self._http_clients[session_id]
    
    async def close_http_client(self, session_id: str):
        """Đóng httpx client khi session kết thúc"""
        if session_id in self._http_clients:
            await self._http_clients[session_id].aclose()
            del self._http_clients[session_id]
    
    async def _check_session_timeout(self, page) -> bool:
        """
        Kiểm tra xem session có bị timeout không
        
        Returns:
            True nếu session timeout, False nếu không
        """
        try:
            current_url = page.url
            
            # Check URL timeout
            if 'timeout.jsp' in current_url:
                logger.warning("Session timeout detected from URL")
                return True
            
            # Check content timeout
            try:
                # Tìm text "Phiên giao dịch hết hạn"
                timeout_text = page.locator('text=Phiên giao dịch hết hạn')
                if await timeout_text.count() > 0:
                    logger.warning("Session timeout detected from content")
                    return True
                
                # Tìm nút "Trở lại" với onclick chứa corpIndexProc
                back_button = page.locator('input[type="button"][onclick*="corpIndexProc"]')
                if await back_button.count() > 0:
                    logger.warning("Session timeout detected from back button")
                    return True
            except Exception as e:
                logger.debug(f"Error checking timeout content: {e}")
            
            return False
        except Exception as e:
            logger.error(f"Error in _check_session_timeout: {e}")
            return False
    
    def _get_date_ranges(self, start_date: str, end_date: str, days_interval: int = 350) -> List[List[str]]:
        """
        Chia khoảng thời gian thành các đoạn nhỏ
        Format: dd/mm/yyyy
        """
        date_format = "%d/%m/%Y"
        date1 = datetime.strptime(start_date, date_format)
        date2 = datetime.strptime(end_date, date_format)
        interval = timedelta(days=days_interval)
        
        date_ranges = []
        while date1 <= date2:
            sub_array = [date1.strftime(date_format)]
            date1 += interval
            if date1 > date2:
                date1 = date2
            sub_array.append(date1.strftime(date_format))
            date_ranges.append(sub_array)
            date1 += timedelta(days=1)
        
        return date_ranges
    
    def _normalize_tokhai_name(self, name_tk: str) -> str:
        """Chuẩn hóa tên tờ khai"""
        if "TỜ KHAI QUYẾT TOÁN THUẾ THU NHẬP CÁ NHÂN" in name_tk:
            if "(TT92/2015)" in name_tk:
                return "05/QTT-TNCN (TT92/2015)"
            elif "TT80/2021" in name_tk:
                return "05/QTT-TNCN (TT80/2021)"
        elif "03/TNDN" in name_tk and "(TT80/2021)" in name_tk:
            return "03/TNDN (TT80/2021)"
        elif "01A/TNDN" in name_tk:
            return "01A/TNDN"
        elif "01B/TNDN" in name_tk:
            return "01B/TNDN"
        elif "02/TNDN" in name_tk:
            return "02/TNDN"
        elif "06/KK-TNCN" in name_tk and "(TT156/2013)" in name_tk:
            return "06/KK-TNCN (TT156/2013)"
        elif "05/KK-TNCN" in name_tk and "(TT92/2015)" in name_tk:
            return "05/KK-TNCN (TT92/2015)"
        elif "05/KK-TNCN" in name_tk and "(TT80)" in name_tk:
            return "05/KK-TNCN (TT80)"
        elif "01/GTGT" in name_tk and "(GTGT)" in name_tk:
            return "01/GTGT (GTGT)"
        elif "01/GTGT" in name_tk and "(TT80/2021)" in name_tk:
            return "01/GTGT (TT80/2021)"
        elif "01/MBAI" in name_tk and "(TT156/2013)" in name_tk:
            return "01/MBAI (TT156/2013)"
        elif "01/LPMB" in name_tk and "(TT80/2021)" in name_tk:
            return "01/LPMB (TT80/2021)"
        
        return name_tk
    
    async def _navigate_to_tokhai_page(self, page, dse_session_id: str) -> bool:
        """
        Navigate đến trang tra cứu tờ khai qua dichvucong.gdt.gov.vn
        
        Flow:
        1. Navigate đến /tthc/dich-vu-khac
        2. Click vào link có onclick="connectSSO('360103', '', '', '')"
        3. Đợi iframe load với src từ thuedientu.gdt.gov.vn
        4. Switch vào iframe và đợi #maTKhai xuất hiện
        
        Returns: True nếu thành công
        """
        success = False
        frame = None
        
        try:
            # Bước 1: Navigate đến trang dich-vu-khac
            current_url = page.url
            if '/tthc/dich-vu-khac' not in current_url:
                logger.info("Navigating to /tthc/dich-vu-khac...")
                await page.goto('https://dichvucong.gdt.gov.vn/tthc/dich-vu-khac', wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(2)
            else:
                logger.info("Already on /tthc/dich-vu-khac page")
            
            # Bước 2: Gọi trực tiếp hàm JavaScript connectSSO('360103', '', '', '')
            logger.info("Calling connectSSO('360103', '', '', '') via JavaScript...")
            
            try:
                # Gọi hàm connectSSO trực tiếp bằng JavaScript
                await page.evaluate("""
                    async () => {
                        // Kiểm tra xem hàm connectSSO có tồn tại không
                        if (typeof connectSSO === 'function') {
                            await connectSSO('360103', '', '', '');
                            return { success: true, message: 'connectSSO called' };
                        } else {
                            return { success: false, message: 'connectSSO function not found' };
                        }
                    }
                """)
                logger.info("connectSSO('360103', '', '', '') called successfully")
                # Đợi AJAX hoàn tất và iframe được set src
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Error calling connectSSO: {e}")
                return False
            
            # Bước 3: Đợi iframe load với src từ thuedientu.gdt.gov.vn
            logger.info("Waiting for iframe to load with thuedientu.gdt.gov.vn...")
            
            # Tìm iframe trong #iframeRenderSSO
            max_wait = 20  # Đợi tối đa 10 giây (20 * 0.5)
            for i in range(max_wait):
                try:
                    # Tìm iframe trong modal #iframeRenderSSO
                    iframe_elem = page.locator('#iframeRenderSSO iframe').first
                    if await iframe_elem.count() > 0:
                        # Lấy src của iframe
                        iframe_src = await iframe_elem.get_attribute('src')
                        if iframe_src and 'thuedientu.gdt.gov.vn' in iframe_src:
                            logger.info(f"Found iframe with src: {iframe_src[:100]}...")
                            
                            # Tìm frame từ page.frames
                            frames = page.frames
                            for f in frames:
                                if 'thuedientu.gdt.gov.vn' in f.url:
                                    frame = f
                                    logger.info(f"Found frame: {frame.url[:100]}...")
                                    break
                            
                            if frame:
                                break
                except Exception as e:
                    logger.debug(f"Waiting for iframe (attempt {i + 1}/{max_wait}): {e}")
                
                await asyncio.sleep(0.5)
            
            # Bước 4: Switch vào iframe và đợi #maTKhai xuất hiện
            if frame:
                try:
                    logger.info("Waiting for #maTKhai in iframe...")
                    await frame.wait_for_load_state('domcontentloaded', timeout=15000)
                    await asyncio.sleep(1)
                    await frame.wait_for_selector('#maTKhai', timeout=15000)
                    success = True
                    logger.info("Tra cuu tokhai page loaded successfully via SSO iframe")
                except Exception as e:
                    logger.warning(f"Frame found but #maTKhai not found: {e}")
                    # Thử đợi thêm một chút
                    try:
                        await asyncio.sleep(2)
                        await frame.wait_for_selector('#maTKhai', timeout=10000)
                        success = True
                        logger.info("Tra cuu tokhai page loaded after additional wait")
                    except:
                        logger.error("Still cannot find #maTKhai after additional wait")
            else:
                logger.error("Iframe not found after clicking connectSSO link")
            
            return success
            
        except Exception as e:
            logger.error(f"Error navigating to tokhai page: {e}")
            return False
    
    async def _navigate_to_tokhai_search(self, session: SessionData) -> bool:
        """
        Navigate đến trang tra cứu tờ khai (deprecated - dùng _navigate_to_tokhai_page)
        Returns: True nếu thành công
        """
        return await self._navigate_to_tokhai_page(session.page, session.dse_session_id)
    
    async def _navigate_to_thongbao_page(self, page, dse_session_id: str) -> bool:
        """
        Navigate đến trang tra cứu thông báo bằng JavaScript (nhanh hơn click menu)
        openPage('lookUpNotificationProc')
        
        Returns: True nếu thành công
        """
        success = False
        
        try:
            # Cách 1: Gọi JavaScript function openPage
            try:
                await page.evaluate("""
                    () => {
                        if (typeof openPage === 'function') {
                            openPage('lookUpNotificationProc');
                            return true;
                        }
                        return false;
                    }
                """)
                logger.info("Called openPage('lookUpNotificationProc') via JavaScript")
                await asyncio.sleep(1.5)
                
                frame = page.frame('mainframe')
                if frame:
                    try:
                        # Đợi form thông báo load - kiểm tra input qryFromDate
                        await frame.wait_for_selector('#qryFromDate', timeout=10000)
                        success = True
                        logger.info("Thong bao page loaded successfully via JS")
                    except:
                        logger.warning("Frame loaded but form not found")
            except Exception as e:
                logger.warning(f"JavaScript openPage failed: {e}")
            
            # Cách 2: Navigate trực tiếp iframe bằng URL
            if not success:
                try:
                    current_url = page.url
                    dse_match = re.search(r'dse_sessionId=([^&]+)', current_url)
                    dse_session = dse_match.group(1) if dse_match else dse_session_id
                    
                    if dse_session:
                        iframe_url = f"/etaxnnt/Request?dse_sessionId={dse_session}&dse_applicationId=-1&dse_pageId=10&dse_operationName=lookUpNotificationProc&dse_processorState=initial&dse_nextEventName=start"
                        
                        await page.evaluate(f"""
                            () => {{
                                const iframe = document.getElementById('tranFrame') || document.querySelector('iframe[name="mainframe"]');
                                if (iframe) {{
                                    iframe.src = '{iframe_url}';
                                    return true;
                                }}
                                return false;
                            }}
                        """)
                        logger.info("Set iframe src directly to thong bao page")
                        await asyncio.sleep(2)
                        
                        frame = page.frame('mainframe')
                        if frame:
                            try:
                                await frame.wait_for_selector('#qryFromDate', timeout=10000)
                                success = True
                                logger.info("Thong bao page loaded via direct iframe navigation")
                            except:
                                logger.warning("Direct iframe navigation failed")
                except Exception as e2:
                    logger.warning(f"Direct iframe navigation failed: {e2}")
            
            # Cách 3: Fallback - click menu như cũ
            if not success:
                logger.info("Falling back to menu click method for thongbao...")
                for retry in range(2):
                    try:
                        menu_ke_toan = page.locator('//html/body/div[1]/div[2]/ul/li[3]')
                        await menu_ke_toan.wait_for(state='visible', timeout=8000)
                        await menu_ke_toan.click(timeout=5000)
                        await asyncio.sleep(0.8)
                        
                        tra_cuu = page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[9]')
                        await tra_cuu.wait_for(state='visible', timeout=8000)
                        await tra_cuu.click(timeout=5000)
                        
                        await asyncio.sleep(1)
                        frame = page.frame('mainframe')
                        if frame:
                            try:
                                await frame.wait_for_selector('#qryFromDate', timeout=10000)
                                success = True
                                break
                            except:
                                pass
                    except Exception as e3:
                        logger.warning(f"Menu click attempt {retry + 1} failed: {e3}")
                        if retry == 0:
                            await page.reload(wait_until='domcontentloaded', timeout=15000)
                            await asyncio.sleep(2)
            
            return success
            
        except Exception as e:
            logger.error(f"Error navigating to thongbao page: {e}")
            return False
    
    async def _navigate_to_giaynoptien_page(self, page, dse_session_id: str) -> bool:
        """
        Navigate đến trang tra cứu giấy nộp tiền bằng JavaScript (nhanh hơn click menu)
        openPage('corpQueryTaxProc')
        
        Returns: True nếu thành công
        """
        success = False
        
        try:
            # Cách 1: Gọi JavaScript function openPage
            try:
                await page.evaluate("""
                    () => {
                        if (typeof openPage === 'function') {
                            openPage('corpQueryTaxProc');
                            return true;
                        }
                        return false;
                    }
                """)
                logger.info("Called openPage('corpQueryTaxProc') via JavaScript")
                await asyncio.sleep(1.5)
                
                frame = page.frame('mainframe')
                if frame:
                    try:
                        # Đợi form giấy nộp tiền load
                        await frame.wait_for_selector('input[name="ngay_lap_tu_ngay"], #ngay_lap_tu_ngay', timeout=10000)
                        success = True
                        logger.info("Giay nop tien page loaded successfully via JS")
                    except:
                        logger.warning("Frame loaded but form not found")
            except Exception as e:
                logger.warning(f"JavaScript openPage failed: {e}")
            
            # Cách 2: Navigate trực tiếp iframe bằng URL
            if not success:
                try:
                    current_url = page.url
                    dse_match = re.search(r'dse_sessionId=([^&]+)', current_url)
                    dse_session = dse_match.group(1) if dse_match else dse_session_id
                    
                    if dse_session:
                        iframe_url = f"/etaxnnt/Request?dse_sessionId={dse_session}&dse_applicationId=-1&dse_pageId=10&dse_operationName=corpQueryTaxProc&dse_processorState=initial&dse_nextEventName=start"
                        
                        await page.evaluate(f"""
                            () => {{
                                const iframe = document.getElementById('tranFrame') || document.querySelector('iframe[name="mainframe"]');
                                if (iframe) {{
                                    iframe.src = '{iframe_url}';
                                    return true;
                                }}
                                return false;
                            }}
                        """)
                        logger.info("Set iframe src directly to giay nop tien page")
                        await asyncio.sleep(2)
                        
                        frame = page.frame('mainframe')
                        if frame:
                            try:
                                await frame.wait_for_selector('input[name="ngay_lap_tu_ngay"], #ngay_lap_tu_ngay', timeout=10000)
                                success = True
                                logger.info("Giay nop tien page loaded via direct iframe navigation")
                            except:
                                logger.warning("Direct iframe navigation failed")
                except Exception as e2:
                    logger.warning(f"Direct iframe navigation failed: {e2}")
            
            # Cách 3: Fallback - click menu như cũ
            if not success:
                logger.info("Falling back to menu click method for giaynoptien...")
                for retry in range(2):
                    try:
                        # Click menu "Nộp thuế" (li thứ 4)
                        menu_nop_thue = page.locator('//html/body/div[1]/div[2]/ul/li[4]')
                        await menu_nop_thue.wait_for(state='visible', timeout=8000)
                        await menu_nop_thue.click(timeout=5000)
                        await asyncio.sleep(0.8)
                        
                        # Click "Tra cứu giấy nộp tiền" (li thứ 4 trong submenu)
                        tra_cuu = page.locator('//html/body/div[1]/div[3]/div/div[4]/ul/li[4]')
                        await tra_cuu.wait_for(state='visible', timeout=8000)
                        await tra_cuu.click(timeout=5000)
                        
                        await asyncio.sleep(1)
                        frame = page.frame('mainframe')
                        if frame:
                            try:
                                await frame.wait_for_selector('input[name="ngay_lap_tu_ngay"], #ngay_lap_tu_ngay', timeout=10000)
                                success = True
                                break
                            except:
                                pass
                    except Exception as e3:
                        logger.warning(f"Menu click attempt {retry + 1} failed: {e3}")
                        if retry == 0:
                            await page.reload(wait_until='domcontentloaded', timeout=15000)
                            await asyncio.sleep(2)
            
            return success
            
        except Exception as e:
            logger.error(f"Error navigating to giaynoptien page: {e}")
            return False
    
    async def crawl_tokhai_info(
        self,
        session_id: str,
        tokhai_type: str,
        start_date: str,
        end_date: str,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Chỉ lấy thông tin tờ khai (KHÔNG download file)
        Dùng để hiển thị danh sách trước, user chọn tải sau
        
        Yields:
            Dict với các key: type, data, progress, error
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            yield {"type": "error", "error": "Session not found"}
            return
        
        if not session.is_logged_in:
            yield {"type": "error", "error": "Not logged in"}
            return
        
        page = session.page
        
        try:
            yield {"type": "info", "message": "Đang navigate đến trang tra cứu..."}
            
            # Navigate đến trang tra cứu tờ khai bằng JavaScript (nhanh hơn click menu)
            success = await self._navigate_to_tokhai_page(page, session.dse_session_id)
            
            if not success:
                yield {"type": "error", "error": "Không thể navigate đến trang tra cứu. Vui lòng thử lại."}
                return
            
            # Switch to mainframe
            frame = page.frame('mainframe')
            if not frame:
                yield {"type": "error", "error": "Không tìm thấy mainframe"}
                return
            
            yield {"type": "info", "message": "Đang chọn loại tờ khai..."}
            
            # Chọn loại tờ khai
            try:
                select_element = frame.locator('#maTKhai')
                await select_element.wait_for(timeout=10000)
                
                if tokhai_type in ["00", "Tất cả", "tat_ca", None, ""]:
                    await select_element.select_option(value="00")
                    is_all_types = True
                else:
                    try:
                        await select_element.select_option(value=tokhai_type)
                        is_all_types = False
                    except:
                        option = frame.locator(f'#maTKhai option:has-text("{tokhai_type}")')
                        if await option.count() > 0:
                            option_value = await option.first.get_attribute('value')
                            await select_element.select_option(value=option_value)
                            is_all_types = (option_value == "00")
                        else:
                            raise Exception(f"Option not found: {tokhai_type}")
            except Exception as e:
                yield {"type": "error", "error": f"Không tìm thấy loại tờ khai: {tokhai_type}"}
                return
            
            await asyncio.sleep(0.5)
            
            # Chia khoảng thời gian
            date_ranges = self._get_date_ranges(start_date, end_date)
            
            total_count = 0
            results = []
            
            yield {"type": "info", "message": f"Bắt đầu crawl {len(date_ranges)} khoảng thời gian..."}
            
            for range_idx, date_range in enumerate(date_ranges):
                yield {
                    "type": "progress", 
                    "current": range_idx + 1, 
                    "total": len(date_ranges),
                    "message": f"Đang xử lý khoảng {date_range[0]} - {date_range[1]}..."
                }
                
                try:
                    # Nhập ngày
                    start_input = frame.locator('#qryFromDate')
                    await start_input.fill('')
                    await start_input.fill(date_range[0])
                    
                    end_input = frame.locator('#qryToDate')
                    await end_input.click()
                    await end_input.fill('')
                    await end_input.fill(date_range[1])
                    
                    # Click Tra cứu
                    search_btn = frame.locator('input[value="Tra cứu"]')
                    await search_btn.click()
                    
                    await asyncio.sleep(2)
                    
                    # Xử lý pagination
                    check_pages = True
                    while check_pages:
                        try:
                            table_body = frame.locator('#allResultTableBody, table.md_list2 tbody, table#data_content_onday tbody').first
                            await table_body.wait_for(timeout=5000)
                        except:
                            yield {"type": "info", "message": f"Không có dữ liệu trong khoảng {date_range[0]} - {date_range[1]}"}
                            break
                        
                        rows = table_body.locator('tr')
                        row_count = await rows.count()
                        
                        yield {"type": "progress", "current": total_count, "message": f"Đang parse {row_count} tờ khai (trang hiện tại)..."}
                        
                        for i in range(row_count):
                            try:
                                row = rows.nth(i)
                                cols = row.locator('td')
                                col_count = await cols.count()
                                
                                if col_count < 3:
                                    continue
                                
                                # Cột 1: Mã giao dịch (id_tk)
                                id_tk = await cols.nth(1).text_content()
                                id_tk = id_tk.strip() if id_tk else ""
                                
                                if len(id_tk) < 4:
                                    continue
                                
                                # Extract thông tin
                                name_tk = await cols.nth(2).text_content() if col_count > 2 else ""
                                ky_tinh_thue = await cols.nth(3).text_content() if col_count > 3 else ""
                                loai_tk = await cols.nth(4).text_content() if col_count > 4 else ""
                                lan_nop = await cols.nth(5).text_content() if col_count > 5 else ""
                                lan_bs = await cols.nth(6).text_content() if col_count > 6 else ""
                                ngay_nop = await cols.nth(7).text_content() if col_count > 7 else ""
                                noi_nop = await cols.nth(9).text_content() if col_count > 9 else ""
                                trang_thai = await cols.nth(10).text_content() if col_count > 10 else ""
                                
                                # Chuẩn hóa tên tờ khai
                                name_tk_normalized = self._normalize_tokhai_name(name_tk.strip() if name_tk else "")
                                
                                # Xác định trạng thái
                                status = "unknown"
                                status_text = ""
                                trang_thai_lower = trang_thai.lower() if trang_thai else ""
                                if "không chấp nhận" in trang_thai_lower:
                                    status = "rejected"
                                    status_text = "[Khong chap nhan]"
                                elif "chấp nhận" in trang_thai_lower:
                                    status = "accepted"
                                    status_text = "[Chap nhan]"
                                
                                # Tạo tên file (để user biết tên file sẽ được tải)
                                ngay_nop_clean = ngay_nop.strip().replace("/", "-").replace(":", "-") if ngay_nop else ""
                                file_name = f"{name_tk_normalized} -{ky_tinh_thue.strip()} -L{lan_nop.strip()} -{loai_tk.strip()} -({id_tk}) -[{ngay_nop_clean}] {status_text}"
                                file_name = self._remove_accents(file_name)
                                file_name = file_name.replace("/", "_").replace(":", "_").replace("\\", "_")
                                
                                # Check xem có link download không
                                has_link = False
                                try:
                                    col2 = cols.nth(2)
                                    download_link = col2.locator('a')
                                    link_count = await download_link.count()
                                    
                                    if link_count > 0:
                                        first_link = download_link.first
                                        onclick = await first_link.get_attribute('onclick')
                                        title = await first_link.get_attribute('title')
                                        
                                        if onclick and 'downloadTkhai' in onclick:
                                            has_link = True
                                        elif title and 'Tải tệp' in title:
                                            has_link = True
                                except:
                                    has_link = False
                                
                                result = {
                                    "id": id_tk,
                                    "name": name_tk_normalized,
                                    "ky_tinh_thue": ky_tinh_thue.strip() if ky_tinh_thue else "",
                                    "loai": loai_tk.strip() if loai_tk else "",
                                    "lan_nop": lan_nop.strip() if lan_nop else "",
                                    "lan_bo_sung": lan_bs.strip() if lan_bs else "",
                                    "ngay_nop": ngay_nop.strip() if ngay_nop else "",
                                    "noi_nop": noi_nop.strip() if noi_nop else "",
                                    "trang_thai": status,
                                    "trang_thai_text": status_text,
                                    "file_name": file_name + ".xml",
                                    "has_download_link": has_link  # Có link download sẵn hay không
                                }
                                
                                results.append(result)
                                total_count += 1
                                
                                yield {"type": "item", "data": result}
                                
                            except Exception as e:
                                logger.error(f"Error processing row: {e}")
                                continue
                        
                        # Check pagination
                        try:
                            next_btn = frame.locator('img[src="/etaxnnt/static/images/pagination_right.gif"]')
                            if await next_btn.count() > 0:
                                await next_btn.click()
                                await asyncio.sleep(1)
                            else:
                                check_pages = False
                        except:
                            check_pages = False
                
                except Exception as e:
                    logger.error(f"Error processing date range {date_range}: {e}")
                    yield {"type": "warning", "message": f"Lỗi xử lý khoảng {date_range}: {str(e)}"}
                    continue
            
            yield {
                "type": "complete",
                "total": total_count,
                "results": results
            }
            
        except Exception as e:
            logger.error(f"Error in crawl_tokhai_info: {e}")
            yield {"type": "error", "error": str(e)}
    
    async def crawl_tokhai(
        self,
        session_id: str,
        tokhai_type: str,
        start_date: str,
        end_date: str,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Crawl tờ khai theo loại và khoảng thời gian (Hybrid approach)
        
        Flow:
        1. Dùng Playwright để navigate đến trang tra cứu
        2. Dùng Playwright để điền form và lấy kết quả
        3. Dùng httpx để download files (nhanh hơn nhiều)
        
        Yields:
            Dict với các key: type, data, progress, error
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            yield {"type": "error", "error": "Session not found"}
            return
        
        if not session.is_logged_in:
            yield {"type": "error", "error": "Not logged in"}
            return
        
        page = session.page
        temp_dir = tempfile.mkdtemp()
        ssid = session.dse_session_id
        
        try:
            yield {"type": "info", "message": "Đang navigate đến trang tra cứu..."}
            
            # Bước 1: Gọi _navigate_to_tokhai_page để click vào link và lấy iframe src
            logger.info("Calling _navigate_to_tokhai_page to get iframe src...")
            success = await self._navigate_to_tokhai_page(page, ssid)
            
            if not success:
                yield {"type": "error", "error": "Không thể navigate đến trang tra cứu. Vui lòng thử lại."}
                return
            
            # Bước 2: Lấy src của iframe từ #iframeRenderSSO
            logger.info("Getting iframe src from #iframeRenderSSO...")
            iframe_src = None
            try:
                iframe_elem = page.locator('#iframeRenderSSO iframe').first
                if await iframe_elem.count() > 0:
                    iframe_src = await iframe_elem.get_attribute('src')
                    if iframe_src:
                        logger.info(f"Got iframe src: {iframe_src[:100]}...")
                    else:
                        logger.warning("Iframe src is empty")
                else:
                    logger.warning("Iframe element not found in #iframeRenderSSO")
            except Exception as e:
                logger.warning(f"Error getting iframe src: {e}")
            
            # Bước 3: Tìm frame từ page.frames với URL chứa thuedientu.gdt.gov.vn
            frame = None
            try:
                frames = page.frames
                for f in frames:
                    if 'thuedientu.gdt.gov.vn' in f.url:
                        frame = f
                        logger.info(f"Found frame: {frame.url[:100]}...")
                        break
            except Exception as e:
                logger.warning(f"Error finding frame: {e}")
            
            if not frame:
                yield {"type": "error", "error": "Không tìm thấy iframe sau khi navigate. Vui lòng thử lại."}
                return
            
            # Bước 4: Đợi frame load và kiểm tra #maTKhai
            try:
                await frame.wait_for_load_state('domcontentloaded', timeout=15000)
                await asyncio.sleep(1)
                await frame.wait_for_selector('#maTKhai', timeout=15000)
                logger.info("Tra cuu tokhai form loaded successfully")
            except Exception as e:
                logger.warning(f"Frame found but #maTKhai not found: {e}")
                yield {"type": "error", "error": "Không tìm thấy form tra cứu. Vui lòng thử lại."}
                return
            
            # Check session timeout
            if await self._check_session_timeout(page):
                yield {
                    "type": "error",
                    "error": "SESSION_EXPIRED",
                    "error_code": "SESSION_EXPIRED",
                    "message": "Phiên giao dịch hết hạn. Vui lòng đăng nhập lại."
                }
                return
            
            yield {"type": "info", "message": "Đang chọn loại tờ khai..."}
            
            # Chọn loại tờ khai bằng id="maTKhai"
            # Hỗ trợ "Tất cả" (value="00")
            try:
                select_element = frame.locator('#maTKhai')
                await select_element.wait_for(timeout=10000)
                
                # Xử lý trường hợp "Tất cả"
                if tokhai_type in ["00", "Tất cả", "tat_ca", None, ""]:
                    await select_element.select_option(value="00")
                    logger.info("Selected tokhai: Tất cả")
                    is_all_types = True
                else:
                    # Select bằng value (có thể là số như "842" hoặc text như "01/GTGT")
                    try:
                        await select_element.select_option(value=tokhai_type)
                        logger.info(f"Selected tokhai by value: {tokhai_type}")
                        is_all_types = False
                    except:
                        # Nếu không được, thử tìm option chứa text
                        option = frame.locator(f'#maTKhai option:has-text("{tokhai_type}")')
                        if await option.count() > 0:
                            option_value = await option.first.get_attribute('value')
                            await select_element.select_option(value=option_value)
                            logger.info(f"Selected tokhai by text, value: {option_value}")
                            is_all_types = (option_value == "00")
                        else:
                            raise Exception(f"Option not found: {tokhai_type}")
                        
            except Exception as e:
                logger.error(f"Error selecting tokhai type: {e}")
                yield {"type": "error", "error": f"Không tìm thấy loại tờ khai: {tokhai_type}. Hãy dùng value như '842', '00' (Tất cả), hoặc text như '01/GTGT'"}
                return
            
            await asyncio.sleep(0.5)
            
            # Chia khoảng thời gian
            date_ranges = self._get_date_ranges(start_date, end_date)
            
            total_count = 0
            results = []
            
            yield {"type": "info", "message": f"Bắt đầu crawl {len(date_ranges)} khoảng thời gian..."}
            
            # Lấy httpx client để download nhanh
            http_client = await self._get_http_client(session_id)
            
            for range_idx, date_range in enumerate(date_ranges):
                yield {
                    "type": "progress", 
                    "current": range_idx + 1, 
                    "total": len(date_ranges),
                    "message": f"Đang xử lý khoảng {date_range[0]} - {date_range[1]}..."
                }
                
                try:
                    # Nhập ngày bắt đầu (id="qryFromDate")
                    start_input = frame.locator('#qryFromDate')
                    await start_input.fill('')
                    await start_input.fill(date_range[0])
                    
                    # Nhập ngày kết thúc (id="qryToDate")
                    end_input = frame.locator('#qryFromDate')
                    await end_input.click()
                    await end_input.fill('')
                    await end_input.fill(date_range[1])
                    
                    # Click button Tra cứu
                    search_btn = frame.locator('input[value="Tra cứu"]')
                    await search_btn.click()
                    
                    await asyncio.sleep(2)
                    
                    # Xử lý pagination
                    check_pages = True
                    while check_pages:
                        # Tìm bảng kết quả - theo HTML thực tế
                        # Bảng: #data_content_onday hoặc table.md_list2
                        # Tbody: #allResultTableBody
                        try:
                            table_body = frame.locator('#allResultTableBody, table.md_list2 tbody, table#data_content_onday tbody').first
                            await table_body.wait_for(timeout=5000)
                        except:
                            yield {"type": "info", "message": f"Không có dữ liệu trong khoảng {date_range[0]} - {date_range[1]}"}
                            break
                        
                        rows = table_body.locator('tr')
                        row_count = await rows.count()
                        
                        yield {"type": "progress", "current": total_count, "message": f"Đang xử lý {row_count} tờ khai (trang hiện tại)..."}
                        
                        download_queue = []  # Queue để batch download
                        page_valid_count = 0  # Đếm số items hợp lệ trong trang này
                        
                        for i in range(row_count):
                            try:
                                row = rows.nth(i)
                                cols = row.locator('td')
                                col_count = await cols.count()
                                
                                if col_count < 3:
                                    continue
                                
                                # Cột 1: Mã giao dịch (id_tk)
                                id_tk = await cols.nth(1).text_content()
                                id_tk = id_tk.strip() if id_tk else ""
                                
                                if len(id_tk) < 4:
                                    continue
                                
                                # Chỉ đếm khi item hợp lệ
                                page_valid_count += 1
                                
                                # Extract thông tin theo đúng cấu trúc HTML
                                # Cột 2: Tờ khai/Phụ lục
                                name_tk = await cols.nth(2).text_content() if col_count > 2 else ""
                                # Cột 3: Kỳ tính thuế  
                                ky_tinh_thue = await cols.nth(3).text_content() if col_count > 3 else ""
                                # Cột 4: Loại tờ khai (Chính thức/Bổ sung)
                                loai_tk = await cols.nth(4).text_content() if col_count > 4 else ""
                                # Cột 5: Lần nộp
                                lan_nop = await cols.nth(5).text_content() if col_count > 5 else ""
                                # Cột 6: Lần bổ sung
                                lan_bs = await cols.nth(6).text_content() if col_count > 6 else ""
                                # Cột 7: Ngày nộp
                                ngay_nop = await cols.nth(7).text_content() if col_count > 7 else ""
                                # Cột 9: Nơi nộp
                                noi_nop = await cols.nth(9).text_content() if col_count > 9 else ""
                                # Cột 10: Trạng thái
                                trang_thai = await cols.nth(10).text_content() if col_count > 10 else ""
                                
                                # Chuẩn hóa tên tờ khai
                                name_tk_normalized = self._normalize_tokhai_name(name_tk.strip() if name_tk else "")
                                
                                # Xác định trạng thái
                                status = "unknown"
                                status_text = ""
                                trang_thai_lower = trang_thai.lower() if trang_thai else ""
                                if "không chấp nhận" in trang_thai_lower:
                                    status = "rejected"
                                    status_text = "[Khong chap nhan]"
                                elif "chấp nhận" in trang_thai_lower:
                                    status = "accepted"
                                    status_text = "[Chap nhan]"
                                
                                # Tạo tên file giống code cũ
                                # Format: {name_tk} -{ky} -L{lan} -{loai} -({id}) -[{ngay}] [{status}]
                                ngay_nop_clean = ngay_nop.strip().replace("/", "-").replace(":", "-") if ngay_nop else ""
                                file_name = f"{name_tk_normalized} -{ky_tinh_thue.strip()} -L{lan_nop.strip()} -{loai_tk.strip()} -({id_tk}) -[{ngay_nop_clean}] {status_text}"
                                file_name = self._remove_accents(file_name)
                                file_name = file_name.replace("/", "_").replace(":", "_").replace("\\", "_")
                                
                                # QUAN TRỌNG: Check xem có link download trong cột 2
                                # Link download phải có onclick="downloadTkhai(...)" hoặc title="Tải tệp tờ khai về"
                                has_link = False
                                try:
                                    col2 = cols.nth(2)
                                    download_link = col2.locator('a')
                                    link_count = await download_link.count()
                                    
                                    if link_count > 0:
                                        # Kiểm tra link có onclick="downloadTkhai" (link download thực sự)
                                        first_link = download_link.first
                                        onclick = await first_link.get_attribute('onclick')
                                        title = await first_link.get_attribute('title')
                                        
                                        # Link download phải có onclick chứa "downloadTkhai" hoặc title="Tải tệp tờ khai về"
                                        if onclick and 'downloadTkhai' in onclick:
                                            has_link = True
                                        elif title and 'Tải tệp' in title:
                                            has_link = True
                                        
                                        logger.info(f"Row {id_tk}: has_link={has_link}, onclick={onclick[:50] if onclick else None}, title={title}")
                                except Exception as e:
                                    logger.debug(f"Error checking link for {id_tk}: {e}")
                                    has_link = False
                                
                                download_info = {
                                    "id": id_tk,
                                    "name": name_tk_normalized,
                                    "ky_tinh_thue": ky_tinh_thue.strip() if ky_tinh_thue else "",
                                    "loai": loai_tk.strip() if loai_tk else "",
                                    "lan_nop": lan_nop.strip() if lan_nop else "",
                                    "lan_bo_sung": lan_bs.strip() if lan_bs else "",
                                    "ngay_nop": ngay_nop.strip() if ngay_nop else "",
                                    "noi_nop": noi_nop.strip() if noi_nop else "",
                                    "trang_thai": status,
                                    "file_name": file_name,
                                    "cols": cols,
                                    "row_index": i,
                                    "has_link": has_link  # Đánh dấu có link hay không
                                }
                                download_queue.append(download_info)
                                
                            except Exception as e:
                                logger.error(f"Error processing row: {e}")
                                continue
                        
                        # Batch download - tối ưu tốc độ (download 5 file cùng lúc)
                        if download_queue:
                            yield {"type": "info", "message": f"Đang download {len(download_queue)} file..."}
                            successful_downloads = await self._batch_download(session, download_queue, temp_dir, ssid, frame)
                            
                            for item in successful_downloads:
                                result = {
                                    "id": item["id"],
                                    "name": item["name"],
                                    "ky_tinh_thue": item["ky_tinh_thue"],
                                    "loai": item["loai"],
                                    "lan_nop": item["lan_nop"],
                                    "lan_bo_sung": item["lan_bo_sung"],
                                    "ngay_nop": item["ngay_nop"],
                                    "noi_nop": item["noi_nop"],
                                    "trang_thai": item["trang_thai"],
                                    "file_name": item["file_name"] + ".xml"
                                }
                                results.append(result)
                                yield {"type": "item", "data": result}
                            
                            # Chỉ đếm những file download thành công
                            total_count += len(successful_downloads)
                            
                            download_queue = []
                        else:
                            # Nếu không có gì để download, không đếm
                            pass
                        
                        # Check pagination - next page
                        try:
                            next_btn = frame.locator('img[src="/etaxnnt/static/images/pagination_right.gif"]')
                            if await next_btn.count() > 0:
                                await next_btn.click()
                                await asyncio.sleep(1)
                            else:
                                check_pages = False
                        except:
                            check_pages = False
                
                except Exception as e:
                    logger.error(f"Error processing date range {date_range}: {e}")
                    yield {"type": "warning", "message": f"Lỗi xử lý khoảng {date_range}: {str(e)}"}
                    continue
            
            # Tạo ZIP file từ các file đã download
            zip_base64 = None
            files_info = []
            total_size = 0
            
            if os.listdir(temp_dir):
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for file_name in os.listdir(temp_dir):
                        file_path = os.path.join(temp_dir, file_name)
                        if os.path.isfile(file_path):
                            file_size = os.path.getsize(file_path)
                            total_size += file_size
                            zf.write(file_path, file_name)
                            files_info.append({
                                "name": file_name,
                                "size": file_size
                            })
                
                zip_base64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')
            
            # Tạo tên file ZIP
            if is_all_types:
                zip_filename = f"tokhai_TAT_CA_{start_date.replace('/', '')}_{end_date.replace('/', '')}.zip"
                tokhai_type_label = "Tất cả"
            else:
                zip_filename = f"tokhai_{tokhai_type}_{start_date.replace('/', '')}_{end_date.replace('/', '')}.zip"
                tokhai_type_label = tokhai_type
            
            # Đếm lại số file thực tế đã download
            actual_files_count = len(files_info)
            # Đếm số results thực tế
            actual_results_count = len(results)
            
            # Total = số file thực tế đã download (vì user cần số file trong ZIP)
            # Nếu muốn biết số items đã tìm thấy, dùng actual_results_count
            yield {
                "type": "complete",
                "total": actual_files_count,  # Số file thực tế trong ZIP (chính xác nhất)
                "results_count": actual_results_count,  # Số items đã tìm thấy (có thể > files nếu download thất bại)
                "total_rows_processed": total_count,  # Số rows đã xử lý (để debug)
                "results": results,
                "files": files_info,
                "files_count": actual_files_count,
                "total_size": total_size,
                "zip_base64": zip_base64,
                "zip_filename": zip_filename,
                "tokhai_type": tokhai_type_label,
                "is_all_types": is_all_types
            }
            
        except Exception as e:
            logger.error(f"Error in crawl_tokhai: {e}")
            yield {"type": "error", "error": str(e)}
        
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def _remove_accents(self, text: str) -> str:
        """Remove Vietnamese accents"""
        try:
            import unidecode
            return unidecode.unidecode(text)
        except:
            return text
    
    async def _batch_download(self, session: SessionData, download_queue: List[Dict], temp_dir: str, ssid: str, frame):
        """
        Download nhiều file song song (tối ưu tốc độ)
        Limit concurrent downloads = 5 để không quá tải
        
        Returns: List các item download thành công
        """
        semaphore = asyncio.Semaphore(5)  # Max 5 downloads cùng lúc
        page = session.page
        successful_downloads = []  # Track những item download thành công
        
        async def download_one(item: Dict):
            async with semaphore:
                try:
                    id_tk = item["id"]
                    file_name = item["file_name"]
                    cols = item["cols"]
                    has_link = item.get("has_link", False)  # Lấy từ item (đã check trước)
                    
                    if has_link:
                        # Có link - click để download (bình thường)
                        download_link = cols.nth(2).locator('a')
                        # Bắt download event từ page (không phải frame)
                        async with page.expect_download(timeout=30000) as download_info:
                            await download_link.first.click()
                        
                        download = await download_info.value
                        save_path = os.path.join(temp_dir, file_name + ".xml" if not file_name.endswith(".xml") else file_name)
                        await download.save_as(save_path)
                        
                        # Kiểm tra file đã được lưu thành công
                        if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                            logger.info(f"Downloaded {id_tk} -> {file_name}")
                            successful_downloads.append(item)
                        else:
                            logger.warning(f"Download failed: File not saved or empty for {id_tk}")
                    else:
                        # Tờ khai đặc biệt - không có link <a> download (không có onclick="downloadTkhai" hoặc title="Tải tệp")
                        # Dùng URL trực tiếp với dse_pageId=14 và messageId={id_tk}
                        logger.info(f"Special tokhai (no download link) detected: {id_tk}, using manual download method")
                        current_ssid = ssid
                        
                        # Ưu tiên 1: Lấy từ input hidden trong form traCuuKhaiForm
                        if not current_ssid or current_ssid == "NotFound":
                            try:
                                dse_session_input = frame.locator('form[name="traCuuKhaiForm"] input[name="dse_sessionId"], form#traCuuKhaiForm input[name="dse_sessionId"], input[name="dse_sessionId"]').first
                                if await dse_session_input.count() > 0:
                                    current_ssid = await dse_session_input.get_attribute('value') or ""
                                    if current_ssid:
                                        logger.info(f"Retrieved dse_sessionId from form input: {current_ssid[:30]}...")
                            except Exception as e:
                                logger.warning(f"Error getting dse_sessionId from form input: {e}")
                        
                        # Ưu tiên 2: Lấy từ frame URL
                        if not current_ssid or current_ssid == "NotFound":
                            try:
                                # Lấy từ frame URL
                                frame_url = frame.url
                                match = re.search(r"[&?]dse_sessionId=([^&]+)", frame_url)
                                if match:
                                    current_ssid = match.group(1)
                                    logger.info(f"Retrieved dse_sessionId from frame URL: {current_ssid[:30]}...")
                            except Exception as e:
                                logger.warning(f"Error getting dse_sessionId from frame URL: {e}")
                        
                        # Fallback: Lấy từ performance logs (giống code cũ)
                        if not current_ssid or current_ssid == "NotFound":
                            try:
                                # Lấy từ performance logs
                                performance_logs = await page.evaluate("""
                                    () => {
                                        return window.performance.getEntriesByType('resource').map(entry => entry.name);
                                    }
                                """)
                                
                                for url in performance_logs:
                                    match = re.search(r"[&?]dse_sessionId=([^&]+)", url)
                                    if match:
                                        current_ssid = match.group(1)
                                        logger.info(f"Retrieved dse_sessionId from performance log: {current_ssid[:30]}...")
                                        break
                            except Exception as e:
                                logger.warning(f"Error getting dse_sessionId from performance logs: {e}")
                        
                        if current_ssid and current_ssid != "NotFound":
                            dse_processor_id = ""
                            try:
                                processor_id_input = frame.locator('form[name="traCuuKhaiForm"] input[name="dse_processorId"], form#traCuuKhaiForm input[name="dse_processorId"], input[name="dse_processorId"]').first
                                if await processor_id_input.count() > 0:
                                    dse_processor_id = await processor_id_input.first.get_attribute('value') or ""
                                    if dse_processor_id:
                                        logger.info(f"Retrieved dse_processorId from form: {dse_processor_id[:30]}...")
                            except:
                                pass
                            
                            if dse_processor_id:
                                download_url = f"{BASE_URL}/etaxnnt/Request?dse_sessionId={current_ssid}&dse_applicationId=-1&dse_operationName=traCuuToKhaiProc&dse_pageId=10&dse_processorState=viewTraCuuTkhai&dse_processorId={dse_processor_id}&dse_nextEventName=downTkhai&messageId={id_tk}"
                            else:
                                download_url = f"{BASE_URL}/etaxnnt/Request?dse_sessionId={current_ssid}&dse_applicationId=-1&dse_operationName=traCuuToKhaiProc&dse_pageId=14&dse_processorState=viewTraCuuTkhai&dse_nextEventName=downTkhai&messageId={id_tk}"
                            
                            logger.info(f"Downloading special (no link) {id_tk} via window.open: {download_url[:100]}...")
                            
                            try:
                                async with page.expect_download(timeout=30000) as download_info:
                                    await frame.evaluate(f"window.open('{download_url}', '_blank');")
                                
                                await asyncio.sleep(0.5)
                                
                                download = await download_info.value
                                save_path = os.path.join(temp_dir, file_name + ".xml" if not file_name.endswith(".xml") else file_name)
                                await download.save_as(save_path)
                                
                                if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                                    logger.info(f"Downloaded special (no link) {id_tk} -> {file_name}")
                                    successful_downloads.append(item)
                                else:
                                    logger.warning(f"Download failed: File not saved or empty for special {id_tk}")
                            except Exception as e2:
                                logger.warning(f"Error downloading special {id_tk} via window.open: {e2}, trying new_page fallback...")
                                try:
                                    new_page = await session.context.new_page()
                                    try:
                                        async with new_page.expect_download(timeout=30000) as download_info:
                                            await new_page.goto(download_url)
                                        
                                        download = await download_info.value
                                        save_path = os.path.join(temp_dir, file_name + ".xml" if not file_name.endswith(".xml") else file_name)
                                        await download.save_as(save_path)
                                        
                                        if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                                            logger.info(f"Downloaded special (no link) {id_tk} via fallback -> {file_name}")
                                            successful_downloads.append(item)
                                        else:
                                            logger.warning(f"Fallback download failed: File not saved or empty for {id_tk}")
                                    finally:
                                        await new_page.close()
                                except Exception as e3:
                                    logger.error(f"Fallback download also failed for {id_tk}: {e3}")
                        else:
                            logger.warning(f"No valid session ID for special download: {id_tk}. ssid={ssid}")
                except Exception as e:
                    logger.warning(f"Error downloading {item.get('id', 'unknown')}: {e}")
        
        # Download tất cả song song (max 5 cùng lúc)
        await asyncio.gather(*[download_one(item) for item in download_queue], return_exceptions=True)
        
        return successful_downloads
    
    async def _download_xml(self, client: httpx.AsyncClient, url: str, temp_dir: str, file_id: str):
        """Download XML file (async)"""
        try:
            response = await client.get(url)
            if response.status_code == 200:
                file_path = os.path.join(temp_dir, f"{file_id}.xml")
                with open(file_path, 'wb') as f:
                    f.write(response.content)
        except Exception as e:
            logger.error(f"Error downloading {file_id}: {e}")
    
    async def _download_xml_with_name(self, client: httpx.AsyncClient, url: str, temp_dir: str, file_id: str, file_name: str):
        """Download XML file với tên file custom (async)"""
        try:
            response = await client.get(url)
            if response.status_code == 200:
                # Đảm bảo tên file hợp lệ
                safe_name = file_name.replace("/", "_").replace("\\", "_").replace(":", "_")
                if not safe_name.endswith(".xml"):
                    safe_name += ".xml"
                file_path = os.path.join(temp_dir, safe_name)
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                return {"id": file_id, "file_name": safe_name, "size": len(response.content)}
        except Exception as e:
            logger.error(f"Error downloading {file_id}: {e}")
            return None
    
    async def crawl_thongbao(
        self,
        session_id: str,
        start_date: str,
        end_date: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Crawl thông báo thuế (Playwright version với form input)
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            yield {"type": "error", "error": "Session not found"}
            return
        
        if not session.is_logged_in:
            yield {"type": "error", "error": "Not logged in"}
            return
        
        page = session.page
        temp_dir = tempfile.mkdtemp()
        ssid = session.dse_session_id
        
        try:
            yield {"type": "info", "message": "Đang navigate đến trang tra cứu thông báo..."}
            
            # Navigate đến trang tra cứu thông báo bằng JavaScript (nhanh hơn)
            success = await self._navigate_to_thongbao_page(page, ssid)
            
            if not success:
                yield {"type": "error", "error": "Không thể navigate đến trang tra cứu thông báo. Vui lòng thử lại."}
                return
            
            # Switch to mainframe
            frame = page.frame('mainframe')
            if not frame:
                yield {"type": "error", "error": "Không tìm thấy mainframe"}
                return
            
            # Check session timeout
            if await self._check_session_timeout(page):
                yield {
                    "type": "error",
                    "error": "SESSION_EXPIRED",
                    "error_code": "SESSION_EXPIRED",
                    "message": "Phiên giao dịch hết hạn. Vui lòng đăng nhập lại."
                }
                return
            
            # Chia khoảng thời gian
            date_ranges = self._get_date_ranges(start_date, end_date)
            
            total_count = 0
            results = []
            files_info = []
            total_size = 0
            
            yield {"type": "info", "message": f"Bắt đầu crawl {len(date_ranges)} khoảng thời gian..."}
            
            for range_idx, date_range in enumerate(date_ranges):
                yield {
                    "type": "progress", 
                    "current": range_idx + 1, 
                    "total": len(date_ranges),
                    "message": f"Đang xử lý khoảng {date_range[0]} - {date_range[1]}..."
                }
                
                try:
                    # Nhập ngày bắt đầu - dùng id qryFromDate theo HTML form
                    start_input = frame.locator('#qryFromDate')
                    await start_input.fill('')
                    await start_input.fill(date_range[0])
                    
                    # Nhập ngày kết thúc - dùng id qryToDate theo HTML form
                    end_input = frame.locator('#qryToDate')
                    await end_input.click()
                    await end_input.fill('')
                    await end_input.fill(date_range[1])
                    
                    # Click tìm kiếm - button "Tra cứu"
                    search_btn = frame.locator('input[value="Tra cứu"]')
                    await search_btn.click()
                    
                    await asyncio.sleep(2)
                    
                    # Xử lý phân trang
                    check_pages = True
                    while check_pages:
                        # Tìm bảng kết quả - theo HTML: #allResultTableBody hoặc table.result_table tbody
                        try:
                            table_body = frame.locator('#allResultTableBody, table.result_table tbody, table#data_content_onday tbody').first
                            await table_body.wait_for(timeout=5000)
                        except:
                            if total_count == 0:
                                yield {"type": "info", "message": f"Không có thông báo trong khoảng {date_range[0]} - {date_range[1]}"}
                            break
                        
                        rows = table_body.locator('tr')
                        row_count = await rows.count()
                        
                        yield {"type": "progress", "current": total_count, "message": f"Đang xử lý {row_count} thông báo (trang hiện tại)..."}
                        
                        download_queue = []
                        page_valid_count = 0
                        
                        for i in range(row_count):
                            try:
                                row = rows.nth(i)
                                cols = row.locator('td')
                                col_count = await cols.count()
                                
                                if col_count < 6:
                                    continue
                                
                                # Theo HTML contentthongbao.html:
                                # Cột 0: STT
                                # Cột 1: CQ thông báo (Ngân hàng/Cơ quan thuế)
                                # Cột 2: Mã giao dịch
                                # Cột 3: Loại thông báo
                                # Cột 4: Số thông báo
                                # Cột 5: Ngày thông báo
                                # Cột 6-9: Số GNT, Mã hiệu chứng từ, Số chứng từ, Ngày nộp thuế
                                # Cột 10: Chi tiết | Tải về
                                
                                # Cột 2: Mã giao dịch
                                ma_giao_dich = await cols.nth(2).text_content()
                                ma_giao_dich = ma_giao_dich.strip() if ma_giao_dich else ""
                                
                                if not ma_giao_dich or len(ma_giao_dich) < 5:
                                    continue
                                
                                # Chỉ đếm khi item hợp lệ
                                page_valid_count += 1
                                
                                # Cột 1: CQ thông báo
                                cq_thong_bao = await cols.nth(1).text_content()
                                cq_thong_bao = cq_thong_bao.strip() if cq_thong_bao else ""
                                
                                # Cột 3: Loại thông báo
                                loai_thong_bao = await cols.nth(3).text_content()
                                loai_thong_bao = loai_thong_bao.strip() if loai_thong_bao else ""
                                
                                # Cột 4: Số thông báo
                                so_thong_bao = await cols.nth(4).text_content()
                                so_thong_bao = so_thong_bao.strip() if so_thong_bao else ""
                                
                                # Cột 5: Ngày thông báo
                                ngay_thong_bao = await cols.nth(5).text_content()
                                ngay_thong_bao = ngay_thong_bao.strip() if ngay_thong_bao else ""
                                
                                result = {
                                    "id": ma_giao_dich,
                                    "ma_giao_dich": ma_giao_dich,
                                    "cq_thong_bao": cq_thong_bao,
                                    "loai_thong_bao": loai_thong_bao,
                                    "so_thong_bao": so_thong_bao,
                                    "ngay_thong_bao": ngay_thong_bao,
                                    "type": "thongbao"
                                }
                                results.append(result)
                                
                                yield {"type": "item", "data": result}
                                
                                # Tìm link "Tải về" trong cột cuối
                                # Theo HTML: có 11 cột (index 0-10), cột cuối chứa "Chi tiết | Tải về"
                                last_col_index = col_count - 1  # Cột cuối cùng
                                if last_col_index >= 10:
                                    last_col = cols.nth(last_col_index)
                                    download_link = last_col.locator('a:has-text("Tải về")')
                                    
                                    if await download_link.count() > 0:
                                        # Tạo tên file từ thông tin thông báo
                                        ngay_clean = ngay_thong_bao.replace("/", "-").replace(":", "-").replace(" ", "_")
                                        file_name = f"{ma_giao_dich} - {loai_thong_bao[:40]} - {ngay_clean}"
                                        file_name = self._remove_accents(file_name)
                                        file_name = file_name.replace("/", "_").replace(":", "_").replace("\\", "_")
                                        
                                        download_queue.append({
                                            "id": ma_giao_dich,
                                            "loai_thong_bao": loai_thong_bao,
                                            "ngay_thong_bao": ngay_thong_bao,
                                            "file_name": file_name,
                                            "download_link": download_link,
                                            "cols": cols,
                                            "col_index": last_col_index
                                        })
                            
                            except Exception as e:
                                logger.error(f"Error processing row: {e}")
                                continue
                        
                        # Download từng file và yield progress
                        if download_queue:
                            queue_total = len(download_queue)
                            yield {
                                "type": "download_start",
                                "total_to_download": queue_total,
                                "message": f"Bắt đầu tải {queue_total} thông báo..."
                            }
                            
                            downloaded = 0
                            for item in download_queue:
                                success = await self._download_single_thongbao(session, item, temp_dir)
                                if success:
                                    downloaded += 1
                                
                                yield {
                                    "type": "download_progress",
                                    "downloaded": downloaded,
                                    "total": queue_total,
                                    "percent": round(downloaded / queue_total * 100, 1) if queue_total > 0 else 0,
                                    "current_item": item.get("id", ""),
                                    "message": f"Đã tải {downloaded}/{queue_total} ({round(downloaded / queue_total * 100, 1) if queue_total > 0 else 0}%)"
                                }
                            
                            yield {
                                "type": "download_complete",
                                "downloaded": downloaded,
                                "total": queue_total,
                                "message": f"Hoàn thành tải {downloaded}/{queue_total} thông báo"
                            }
                        
                        # Chỉ cộng số items hợp lệ vào total_count
                        total_count += page_valid_count
                        
                        # Check pagination - next page
                        try:
                            next_btn = frame.locator('img[src="/etaxnnt/static/images/pagination_right.gif"]')
                            if await next_btn.count() > 0:
                                await next_btn.click()
                                await asyncio.sleep(1)
                            else:
                                check_pages = False
                        except:
                            check_pages = False
                
                except Exception as e:
                    logger.error(f"Error processing date range {date_range}: {e}")
                    yield {"type": "warning", "message": f"Lỗi xử lý khoảng {date_range}: {str(e)}"}
                    continue
            
            # Parse downloaded files và rename
            parsed_results = []
            files_in_temp_dir = os.listdir(temp_dir) if os.path.exists(temp_dir) else []
            logger.info(f"crawl_thongbao: Found {len(files_in_temp_dir)} files in temp_dir")
            
            if files_in_temp_dir:
                for file_name in files_in_temp_dir:
                    file_path = os.path.join(temp_dir, file_name)
                    if not os.path.isfile(file_path):
                        continue
                    
                    try:
                        # Parse XML để lấy thông tin
                        with open(file_path, 'r', encoding='utf-8') as f:
                            soup = BeautifulSoup(f, 'html.parser')
                        
                        mgd = soup.find('magiaodichdtu')
                        mgd = mgd.text if mgd else ""
                        
                        ttb = soup.find('tentbao')
                        ttb = ttb.text if ttb else ""
                        
                        ma_tbao = soup.find('matbao')
                        ma_tbao = ma_tbao.text if ma_tbao else ""
                        
                        
                        if "Tiếp nhận" in ttb:
                            ttb = "Tiếp nhận"
                        if "Xác nhận" in ttb:
                            ttb = "Xác nhận"
                        if ma_tbao == "844":
                            ttb = "Không chấp nhận"
                        elif ma_tbao == "451":
                            ttb = "Chấp nhận"
                        
                        ttb_2 = "X"
                        try:
                            if ttb == "Tiếp nhận":
                                ngay_tbao_elem = soup.find('ngaytbao')
                                ttb_2 = ngay_tbao_elem.text if ngay_tbao_elem else "X"
                            else:
                                ngay_chap_nhan_elem = soup.find('ngaychapnhan')
                                if ngay_chap_nhan_elem:
                                    ttb_2 = ngay_chap_nhan_elem.text.split("T")[0] if "T" in ngay_chap_nhan_elem.text else ngay_chap_nhan_elem.text
                        except:
                            ttb_2 = "X"
                        
                        ttb_2_clean = ttb_2.replace("/", "-")
                        new_file_name = f"{mgd} - {ttb} - {ttb_2_clean}.xml"
                        new_file_name = self._remove_accents(new_file_name)
                        
                        # Rename file
                        new_file_path = os.path.join(temp_dir, new_file_name)
                        if os.path.exists(file_path):
                            try:
                                os.rename(file_path, new_file_path)
                                file_name = new_file_name
                                file_path = new_file_path
                            except Exception as rename_err:
                                logger.warning(f"Error renaming {file_name}: {rename_err}")
                        
                        parsed_results.append({
                            "ma_giao_dich": mgd,
                            "ten_thong_bao": ttb,
                            "ma_thong_bao": ma_tbao,
                            "ngay_thong_bao": ttb_2.replace("-", "/") if ttb_2 != "X" else ""  # Trả về format gốc
                        })
                        
                        file_size = os.path.getsize(file_path)
                        total_size += file_size
                        files_info.append({"name": file_name, "size": file_size})
                    except Exception as e:
                        logger.warning(f"Error parsing/renaming file {file_name}: {e}")
                        # Nếu parse lỗi, vẫn thêm vào files_info với tên cũ
                        try:
                            file_size = os.path.getsize(file_path)
                            total_size += file_size
                            files_info.append({"name": file_name, "size": file_size})
                        except:
                            pass
                        continue
                
                # Tạo ZIP từ tất cả file trong temp_dir (sau khi parse/rename)
                zip_buffer = BytesIO()
                final_files = os.listdir(temp_dir)
                logger.info(f"crawl_thongbao: Creating ZIP from {len(final_files)} files")
                
                if final_files:
                    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                        for file_name in final_files:
                            file_path = os.path.join(temp_dir, file_name)
                            if os.path.isfile(file_path):
                                zf.write(file_path, file_name)
                                logger.debug(f"Added to ZIP: {file_name}")
                    
                    zip_base64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')
                    logger.info(f"crawl_thongbao: ZIP created, base64 length: {len(zip_base64)}")
                else:
                    zip_base64 = None
                    logger.warning("crawl_thongbao: No files to add to ZIP")
            else:
                zip_base64 = None
                logger.warning("crawl_thongbao: No files in temp_dir")
            
            actual_files_count = len(files_info)
            actual_results_count = len(parsed_results)
            
            yield {
                "type": "complete",
                "total": actual_files_count,  # Số file thực tế trong ZIP
                "results_count": actual_results_count,  # Số items đã parse
                "total_rows_processed": total_count,  # Số rows đã xử lý (để debug)
                "results": parsed_results,
                "files": files_info,
                "files_count": actual_files_count,
                "total_size": total_size,
                "zip_base64": zip_base64,
                "zip_filename": f"thongbao_{start_date.replace('/', '')}_{end_date.replace('/', '')}.zip"
            }
            
        except Exception as e:
            logger.error(f"Error in crawl_thongbao: {e}")
            yield {"type": "error", "error": str(e)}
        
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    async def _download_single_thongbao(self, session: SessionData, item: Dict, temp_dir: str, max_retries: int = 2) -> bool:
        """
        Download 1 file thông báo với retry logic
        
        Returns:
            True nếu download thành công
        """
        page = session.page
        id_tb = item["id"]
        file_name = item.get("file_name", id_tb)
        
        for retry in range(max_retries + 1):
            try:
                # Ưu tiên dùng download_link đã tìm sẵn
                download_link = item.get("download_link")
                
                if not download_link:
                    # Fallback: tìm lại từ cols
                    cols = item.get("cols")
                    col_idx = item.get("col_index", 10)
                    if cols:
                        download_link = cols.nth(col_idx).locator('a:has-text("Tải về")')
                
                if download_link and await download_link.count() > 0:
                    async with page.expect_download(timeout=30000) as download_info:
                        await download_link.first.click()
                    
                    download = await download_info.value
                    save_path = os.path.join(temp_dir, file_name + ".xml" if not file_name.endswith(".xml") else file_name)
                    await download.save_as(save_path)
                    
                    # Verify file exists and has content
                    if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                        logger.info(f"Downloaded thongbao {id_tb} -> {file_name}")
                        return True
                    else:
                        raise Exception("File empty or not saved")
                else:
                    logger.warning(f"No download link for thongbao {id_tb}")
                    return False
                    
            except Exception as e:
                logger.warning(f"Error downloading thongbao {id_tb} (attempt {retry + 1}/{max_retries + 1}): {e}")
                if retry < max_retries:
                    await asyncio.sleep(1)  # Wait before retry
        
        return False
    
    async def crawl_giay_nop_tien(
        self,
        session_id: str,
        start_date: str,
        end_date: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Crawl giấy nộp tiền thuế (Playwright version với form input)
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            yield {"type": "error", "error": "Session not found"}
            return
        
        if not session.is_logged_in:
            yield {"type": "error", "error": "Not logged in"}
            return
        
        page = session.page
        temp_dir = tempfile.mkdtemp()
        ssid = session.dse_session_id
        
        try:
            yield {"type": "info", "message": "Đang navigate đến trang tra cứu giấy nộp thuế..."}
            
            # Navigate đến trang giấy nộp tiền bằng JavaScript (nhanh hơn)
            success = await self._navigate_to_giaynoptien_page(page, ssid)
            
            if not success:
                yield {"type": "error", "error": "Không thể navigate đến trang tra cứu giấy nộp thuế. Vui lòng thử lại."}
                return
            
            # Check session timeout
            if await self._check_session_timeout(page):
                yield {
                    "type": "error",
                    "error": "SESSION_EXPIRED",
                    "error_code": "SESSION_EXPIRED",
                    "message": "Phiên giao dịch hết hạn. Vui lòng đăng nhập lại."
                }
                return
            
            # Giấy nộp tiền không có mainframe, dùng page trực tiếp
            # Nhưng vẫn thử tìm mainframe trước, nếu không có thì dùng page
            frame = page.frame('mainframe')
            if not frame:
                # Không có mainframe, dùng page trực tiếp
                frame = page
            
            # Chia khoảng thời gian
            date_ranges = self._get_date_ranges(start_date, end_date, days_interval=360)
            
            total_count = 0
            results = []
            files_info = []
            total_size = 0
            
            yield {"type": "info", "message": f"Bắt đầu crawl {len(date_ranges)} khoảng thời gian..."}
            
            for range_idx, date_range in enumerate(date_ranges):
                yield {
                    "type": "progress", 
                    "current": range_idx + 1, 
                    "total": len(date_ranges),
                    "message": f"Đang xử lý khoảng {date_range[0]} - {date_range[1]}..."
                }
                
                try:
                    # Nhập ngày bắt đầu (dùng name attribute)
                    start_input = frame.locator('input[name="ngay_lap_tu_ngay"], input#ngay_lap_tu_ngay')
                    await start_input.fill('')
                    await start_input.fill(date_range[0])
                    
                    # Nhập ngày kết thúc (dùng name attribute)
                    end_input = frame.locator('input[name="ngay_lap_den_ngay"], input#ngay_lap_den_ngay')
                    await end_input.click()
                    from playwright.async_api import Keyboard
                    await end_input.press('Control+a')
                    await end_input.fill(date_range[1])
                    
                    # Click tìm kiếm (dùng value hoặc onclick)
                    search_btn = frame.locator('input[value="Tra cứu"], input[onclick*="traCuuChungTu"]')
                    await search_btn.click()
                    
                    await asyncio.sleep(2)
                    
                    # Kiểm tra kết quả tìm kiếm
                    # Nếu có data: có <div class="tab-content">
                    # Nếu không có: có <div align="center"><strong>Không có dữ liệu</strong></div>
                    try:
                        no_data_div = frame.locator('div[align="center"] strong:has-text("Không có dữ liệu")')
                        if await no_data_div.count() > 0:
                            yield {"type": "info", "message": f"Không có giấy nộp thuế trong khoảng {date_range[0]} - {date_range[1]}"}
                            continue
                    except:
                        pass
                    
                    # Kiểm tra xem có tab-content không
                    try:
                        tab_content = frame.locator('div.tab-content')
                        tab_content_count = await tab_content.count()
                        if tab_content_count == 0:
                            yield {"type": "info", "message": f"Không có giấy nộp thuế trong khoảng {date_range[0]} - {date_range[1]}"}
                            continue
                    except:
                        pass
                    
                    # Xử lý pagination
                    check_pages = True
                    while check_pages:
                        try:
                            # Tìm table body chứa kết quả
                            table_body = frame.locator('div.tab-content table#data_content_onday tbody#allResultTableBody, div.tab-content tbody#allResultTableBody, #allResultTableBody')
                            await table_body.wait_for(timeout=10000, state='attached')
                        except:
                            # Nếu không tìm thấy table, có thể không có data
                            yield {"type": "info", "message": f"Không có giấy nộp thuế trong khoảng {date_range[0]} - {date_range[1]}"}
                            break
                        
                        rows = table_body.locator('tr')
                        row_count = await rows.count()
                        
                        yield {"type": "progress", "current": total_count, "message": f"Đang xử lý {row_count} giấy nộp thuế..."}
                        
                        download_queue = []
                        page_valid_count = 0  # Đếm số items hợp lệ trong trang này
                        
                        # Xử lý từng row
                        i = 0
                        while i < row_count:
                            try:
                                row = rows.nth(i)
                                cols = row.locator('td')
                                col_count = await cols.count()
                                
                                if col_count < 5:
                                    i += 1
                                    continue
                                
                                # Lấy id_gnt từ link chiTietCT(id) trong cột 5 (index 4)
                                # Hoặc từ link downloadGNT(id) trong cột 19 (index 18)
                                id_gnt = None
                                
                                # Thử lấy từ cột 5 (chiTietCT)
                                try:
                                    if col_count > 4:
                                        col5_links = cols.nth(4).locator('a[href*="chiTietCT"]')
                                        if await col5_links.count() > 0:
                                            href = await col5_links.first.get_attribute('href')
                                            if href and 'chiTietCT(' in href:
                                                # Extract ID from chiTietCT(52263061)
                                                match = re.search(r'chiTietCT\((\d+)\)', href)
                                                if match:
                                                    id_gnt = match.group(1)
                                except:
                                    pass
                                
                                # Nếu không lấy được từ cột 5, thử lấy từ cột 19 (downloadGNT)
                                if not id_gnt:
                                    try:
                                        if col_count > 18:
                                            col19_links = cols.nth(18).locator('a[href*="downloadGNT"]')
                                            if await col19_links.count() > 0:
                                                href = await col19_links.first.get_attribute('href')
                                                if href and 'downloadGNT(' in href:
                                                    match = re.search(r'downloadGNT\((\d+)\)', href)
                                                    if match:
                                                        id_gnt = match.group(1)
                                    except:
                                        pass
                                
                                # Fallback: Lấy từ cột 2 (cho các row "even")
                                if not id_gnt:
                                    try:
                                        id_gnt = await cols.nth(2).text_content()
                                        id_gnt = id_gnt.strip() if id_gnt else ""
                                        # Chỉ dùng nếu có độ dài hợp lệ
                                        if not id_gnt or len(id_gnt) < 4:
                                            id_gnt = None
                                    except:
                                        pass
                                
                                # Nếu không có id, skip
                                if not id_gnt:
                                    i += 1
                                    continue
                                
                                # Chỉ đếm khi item hợp lệ
                                page_valid_count += 1
                                
                                result = {"id": id_gnt, "type": "giaynoptien"}
                                results.append(result)
                                
                                yield {"type": "item", "data": result}
                                
                                # Download từ các cột 17-20
                                download_link_found = False
                                for col_idx in [17, 18, 19, 20]:
                                    if col_count > col_idx and not download_link_found:
                                        try:
                                            # Check xem có link downloadGNT không
                                            links = cols.nth(col_idx).locator('a[href*="downloadGNT"]')
                                            link_count = await links.count()
                                            if link_count > 0:
                                                download_queue.append({
                                                    "id": id_gnt,
                                                    "row": row,
                                                    "col_index": col_idx,
                                                    "link_locator": links.first
                                                })
                                                download_link_found = True
                                                break  # Chỉ cần 1 link download
                                        except:
                                            pass
                                
                                i += 1
                            
                            except Exception as e:
                                logger.error(f"Error processing row {i}: {e}")
                                i += 1
                                continue
                        
                        # Download từng file và yield progress
                        if download_queue:
                            queue_total = len(download_queue)
                            yield {
                                "type": "download_start",
                                "total_to_download": queue_total,
                                "message": f"Bắt đầu tải {queue_total} giấy nộp tiền..."
                            }
                            
                            downloaded = 0
                            for item in download_queue:
                                success = await self._download_single_giaynoptien(session, item, temp_dir)
                                if success:
                                    downloaded += 1
                                
                                yield {
                                    "type": "download_progress",
                                    "downloaded": downloaded,
                                    "total": queue_total,
                                    "percent": round(downloaded / queue_total * 100, 1) if queue_total > 0 else 0,
                                    "current_item": item.get("id", ""),
                                    "message": f"Đã tải {downloaded}/{queue_total} ({round(downloaded / queue_total * 100, 1) if queue_total > 0 else 0}%)"
                                }
                            
                            yield {
                                "type": "download_complete",
                                "downloaded": downloaded,
                                "total": queue_total,
                                "message": f"Hoàn thành tải {downloaded}/{queue_total} giấy nộp tiền"
                            }
                        
                        # Chỉ cộng số items hợp lệ vào total_count
                        total_count += page_valid_count
                        
                        # Check pagination
                        try:
                            next_btn = frame.locator('img[src="/etaxnnt/static/images/pagination_right.gif"]')
                            if await next_btn.count() > 0:
                                await next_btn.click()
                                await asyncio.sleep(1)
                            else:
                                check_pages = False
                        except:
                            check_pages = False
                
                except Exception as e:
                    logger.error(f"Error processing date range {date_range}: {e}")
                    yield {"type": "warning", "message": f"Lỗi xử lý khoảng {date_range}: {str(e)}"}
                    continue
            
            # Parse downloaded files và rename
            parsed_results = []
            files_in_temp_dir = os.listdir(temp_dir) if os.path.exists(temp_dir) else []
            logger.info(f"crawl_giay_nop_tien: Found {len(files_in_temp_dir)} files in temp_dir")
            
            if files_in_temp_dir:
                nnn = 0
                
                for file_name in files_in_temp_dir:
                    file_path = os.path.join(temp_dir, file_name)
                    if not os.path.isfile(file_path):
                        continue
                    
                    try:
                        # Parse XML để lấy thông tin
                        with open(file_path, 'r', encoding='utf-8') as f:
                            soup = BeautifulSoup(f, 'html.parser')
                        
                        ma_ndkt = soup.find('ma_ndkt')
                        ma_ndkt = ma_ndkt.text if ma_ndkt else ""
                        
                        ngay_lap = soup.find('ngay_lap')
                        ngay_lap = ngay_lap.text if ngay_lap else ""
                        ngay_lap = ngay_lap.replace("/", "-")
                        
                        ma_chuong = soup.find('ma_chuong')
                        ma_chuong = ma_chuong.text if ma_chuong else ""
                        
                        ky_thue = soup.find('ky_thue')
                        ky_thue = ky_thue.text if ky_thue else ""
                        ky_thue = ky_thue.replace("/", "-")
                        
                        # Rename file theo format
                        nnn += 1
                        new_file_name = f"{ma_ndkt} - {ma_chuong} - Kynopthue - {ky_thue} - Ngaynopthue - {ngay_lap} [{nnn}].xml"
                        new_file_name = self._remove_accents(new_file_name)
                        
                        # Rename file
                        new_file_path = os.path.join(temp_dir, new_file_name)
                        if os.path.exists(file_path):
                            try:
                                os.rename(file_path, new_file_path)
                                file_name = new_file_name
                                file_path = new_file_path
                            except Exception as rename_err:
                                logger.warning(f"Error renaming {file_name}: {rename_err}")
                        
                        parsed_results.append({
                            "ma_noi_dung_kinh_te": ma_ndkt,
                            "ngay_lap": ngay_lap.replace("-", "/") if ngay_lap else "",  # Trả về format gốc
                            "ma_chuong": ma_chuong,
                            "ky_thue": ky_thue.replace("-", "/") if ky_thue else ""  # Trả về format gốc
                        })
                        
                        file_size = os.path.getsize(file_path)
                        total_size += file_size
                        files_info.append({"name": file_name, "size": file_size})
                    except Exception as e:
                        logger.warning(f"Error parsing/renaming file {file_name}: {e}")
                        # Nếu parse lỗi, vẫn thêm vào files_info với tên cũ
                        try:
                            file_size = os.path.getsize(file_path)
                            total_size += file_size
                            files_info.append({"name": file_name, "size": file_size})
                        except:
                            pass
                        continue
                
                # Tạo ZIP từ tất cả file trong temp_dir (sau khi parse/rename)
                zip_buffer = BytesIO()
                final_files = os.listdir(temp_dir)
                logger.info(f"crawl_giay_nop_tien: Creating ZIP from {len(final_files)} files")
                
                if final_files:
                    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                        for file_name in final_files:
                            file_path = os.path.join(temp_dir, file_name)
                            if os.path.isfile(file_path):
                                zf.write(file_path, file_name)
                                logger.debug(f"Added to ZIP: {file_name}")
                    
                    zip_base64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')
                    logger.info(f"crawl_giay_nop_tien: ZIP created, base64 length: {len(zip_base64)}")
                else:
                    zip_base64 = None
                    logger.warning("crawl_giay_nop_tien: No files to add to ZIP")
            else:
                zip_base64 = None
                logger.warning("crawl_giay_nop_tien: No files in temp_dir")
            
            actual_files_count = len(files_info)
            actual_results_count = len(parsed_results)
            
            yield {
                "type": "complete",
                "total": actual_files_count,  # Số file thực tế trong ZIP
                "results_count": actual_results_count,  # Số items đã parse
                "total_rows_processed": total_count,  # Số rows đã xử lý (để debug)
                "results": parsed_results,
                "files": files_info,
                "files_count": actual_files_count,
                "total_size": total_size,
                "zip_base64": zip_base64,
                "zip_filename": f"giaynoptien_{start_date.replace('/', '')}_{end_date.replace('/', '')}.zip"
            }
            
        except Exception as e:
            logger.error(f"Error in crawl_giay_nop_tien: {e}")
            yield {"type": "error", "error": str(e)}
        
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    _gnt_download_counter = 0  # Class-level counter for unique file names
    
    async def _download_single_giaynoptien(self, session: SessionData, item: Dict, temp_dir: str, max_retries: int = 2) -> bool:
        """
        Download 1 file giấy nộp tiền với retry logic
        
        Returns:
            True nếu download thành công
        """
        page = session.page
        id_gnt = item["id"]
        
        for retry in range(max_retries + 1):
            try:
                # Nếu đã có link_locator, dùng trực tiếp
                if "link_locator" in item:
                    download_link = item["link_locator"]
                else:
                    # Fallback: tìm lại link từ row và col_index
                    row = item.get("row")
                    col_idx = item.get("col_index")
                    if row and col_idx is not None:
                        cols = row.locator('td')
                        links = cols.nth(col_idx).locator('a[href*="downloadGNT"]')
                        link_count = await links.count()
                        
                        # Nếu có 2 links thì click link thứ 2, nếu không thì click link đầu
                        if link_count >= 2:
                            download_link = links.nth(1)
                        elif link_count >= 1:
                            download_link = links.first
                        else:
                            logger.warning(f"No download link found for {id_gnt}")
                            return False
                    else:
                        logger.warning(f"Missing link_locator or row/col_index for {id_gnt}")
                        return False
                
                # Download file
                async with page.expect_download(timeout=30000) as download_info:
                    await download_link.click()
                
                download = await download_info.value
                
                # Lưu file với tên tạm unique
                TaxCrawlerService._gnt_download_counter += 1
                temp_name = f"chungtu_{id_gnt}_{TaxCrawlerService._gnt_download_counter}.xml"
                save_path = os.path.join(temp_dir, temp_name)
                await download.save_as(save_path)
                
                # Verify file exists and has content
                await asyncio.sleep(0.3)
                if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                    logger.info(f"Downloaded giaynoptien {id_gnt} -> {temp_name}")
                    return True
                else:
                    raise Exception("File empty or not saved")
                    
            except Exception as e:
                logger.warning(f"Error downloading giaynoptien {id_gnt} (attempt {retry + 1}/{max_retries + 1}): {e}")
                if retry < max_retries:
                    await asyncio.sleep(1)  # Wait before retry
        
        return False
    
    async def convert_xml_to_xlsx(self, xml_files_base64: str) -> Dict[str, Any]:
        """
        Chuyển đổi các file XML sang Excel (async version)
        
        Args:
            xml_files_base64: ZIP file chứa các XML dạng base64
        
        Returns:
            Dict với xlsx_base64
        """
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Giải nén ZIP
            zip_bytes = base64.b64decode(xml_files_base64)
            zip_buffer = BytesIO(zip_bytes)
            
            with zipfile.ZipFile(zip_buffer, 'r') as zf:
                zf.extractall(temp_dir)
            
            # Tạo workbook
            workbook = Workbook()
            worksheet = workbook.active
            
            # Headers
            headers = [
                'Tên', 'Kỳ tính thuế Tháng/Quý', 'Lần', 'Năm',
                'VAT đầu kỳ', 'Giá trị HH mua vào', 'VAT mua vào',
                'VAT được khấu trừ kỳ này', 'Giá trị HH bán ra', 'VAT bán ra',
                'Điều chỉnh tăng', 'Điều chỉnh giảm', 'Thuế vãng lai ngoại tỉnh',
                'VAT còn phải nộp', 'VAT còn được khấu trừ chuyển kỳ sau'
            ]
            worksheet.append(headers)
            
            # Parse each XML file
            for filename in os.listdir(temp_dir):
                if not filename.endswith('.xml'):
                    continue
                
                file_path = os.path.join(temp_dir, filename)
                
                try:
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    
                    # Get namespace
                    namespace = {'ns0': root.tag.split('}')[0][1:]} if '}' in root.tag else {}
                    
                    # Extract data
                    def get_element_text(tag):
                        if namespace:
                            elem = root.find(f'.//ns0:{tag}', namespace)
                        else:
                            elem = root.find(f'.//{tag}')
                        return elem.text if elem is not None else ''
                    
                    ky_kkhai = get_element_text('kyKKhai')
                    ky = ky_kkhai.split("/")[0] if "/" in ky_kkhai else ''
                    nam = ky_kkhai.split("/")[1] if "/" in ky_kkhai else ''
                    
                    try:
                        so_lan = filename.split("-")[2] + " " + filename.split("-")[3]
                    except:
                        so_lan = ""
                    
                    row = [
                        filename,
                        ky,
                        so_lan,
                        nam,
                        get_element_text('ct22'),
                        get_element_text('ct23'),
                        get_element_text('ct24'),
                        get_element_text('ct25'),
                        get_element_text('ct34'),
                        get_element_text('ct35'),
                        get_element_text('ct38'),
                        get_element_text('ct37'),
                        get_element_text('ct39'),
                        get_element_text('ct40'),
                        get_element_text('ct43'),
                    ]
                    
                    worksheet.append(row)
                    
                except Exception as e:
                    logger.error(f"Error parsing XML {filename}: {e}")
                    continue
            
            # Format worksheet
            header_font = Font(bold=True)
            thin_border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
            
            for col in range(1, worksheet.max_column + 1):
                cell = worksheet.cell(row=1, column=col)
                cell.font = header_font
                cell.border = thin_border
            
            for row in range(2, worksheet.max_row + 1):
                for col in range(1, worksheet.max_column + 1):
                    cell = worksheet.cell(row=row, column=col)
                    cell.border = thin_border
                    
                    if col >= 5:
                        if cell.value:
                            try:
                                cell.value = float(cell.value)
                                cell.number_format = FORMAT_NUMBER_COMMA_SEPARATED1
                            except ValueError:
                                pass
            
            for col in range(1, worksheet.max_column + 1):
                header_text = worksheet.cell(row=1, column=col).value
                worksheet.column_dimensions[get_column_letter(col)].width = len(str(header_text)) + 5
            
            xlsx_buffer = BytesIO()
            workbook.save(xlsx_buffer)
            xlsx_base64 = base64.b64encode(xlsx_buffer.getvalue()).decode('utf-8')
            
            return {
                "success": True,
                "xlsx_base64": xlsx_base64,
                "row_count": worksheet.max_row - 1
            }
            
        except Exception as e:
            logger.error(f"Error in convert_xml_to_xlsx: {e}")
            return {"success": False, "error": str(e)}
        
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    async def get_tokhai_types(self, session_id: str) -> Dict[str, Any]:
        """
        Lấy danh sách các loại tờ khai có thể chọn
        Dùng Playwright vì cần render JavaScript
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            return {"success": False, "error": "Session not found"}
        
        if not session.is_logged_in:
            return {"success": False, "error": "Not logged in"}
        
        page = session.page
        
        try:
            # Navigate đến trang tra cứu tờ khai bằng JavaScript (nhanh hơn)
            success = await self._navigate_to_tokhai_page(page, session.dse_session_id)
            
            if not success:
                return {"success": False, "error": "Không thể navigate đến trang tra cứu. Vui lòng thử lại."}
            
            frame = page.frame('mainframe')
            if not frame:
                return {"success": False, "error": "Không tìm thấy mainframe"}
            
            # Tìm dropdown id="maTKhai"
            select = frame.locator('#maTKhai')
            await select.wait_for(timeout=10000)
            
            options = await select.locator('option').all()
            tokhai_types = []
            
            # Thêm option "Tất cả" vào đầu danh sách
            tokhai_types.append({
                "value": "00",
                "label": "--Tất cả--"
            })
            
            for option in options:
                value = await option.get_attribute('value')
                text = await option.text_content()
                # Bỏ qua header groups (value="--") và "Tất cả" (value="00") vì đã thêm ở trên
                if value and value not in ['--', '00'] and text:
                    tokhai_types.append({
                        "value": value,
                        "label": text.strip()
                    })
            
            return {
                "success": True,
                "tokhai_types": tokhai_types
            }
            
        except Exception as e:
            logger.error(f"Error getting tokhai types: {e}")
            return {"success": False, "error": str(e)}
    
    async def crawl_batch(
        self,
        session_id: str,
        start_date: str,
        end_date: str,
        crawl_types: List[str],
        tokhai_type: str = "00"
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Crawl nhiều loại dữ liệu đồng thời (tờ khai, thông báo, giấy nộp tiền)
        
        Args:
            session_id: Session ID đã đăng nhập
            start_date: Ngày bắt đầu (dd/mm/yyyy)
            end_date: Ngày kết thúc (dd/mm/yyyy)
            crawl_types: Danh sách loại cần crawl ["tokhai", "thongbao", "giaynoptien"]
            tokhai_type: Loại tờ khai (chỉ áp dụng nếu crawl tokhai)
        
        Yields:
            Dict với progress và kết quả từng loại
        """
        session = self.session_manager.get_session(session_id)
        if not session:
            yield {"type": "error", "error": "Session not found"}
            return
        
        if not session.is_logged_in:
            yield {"type": "error", "error": "Not logged in"}
            return
        
        # Validate crawl_types
        valid_types = ["tokhai", "thongbao", "giaynoptien"]
        crawl_types = [t for t in crawl_types if t in valid_types]
        
        if not crawl_types:
            yield {"type": "error", "error": "Không có loại crawl hợp lệ. Chọn từ: tokhai, thongbao, giaynoptien"}
            return
        
        total_types = len(crawl_types)
        yield {
            "type": "batch_start",
            "message": f"Bắt đầu crawl {total_types} loại dữ liệu: {', '.join(crawl_types)}",
            "crawl_types": crawl_types,
            "total_types": total_types
        }
        
        # Kết quả tổng hợp
        batch_results = {
            "tokhai": None,
            "thongbao": None,
            "giaynoptien": None
        }
        
        # Xử lý từng loại tuần tự (vì cùng dùng 1 session/page)
        for idx, crawl_type in enumerate(crawl_types):
            yield {
                "type": "batch_progress",
                "current_type": crawl_type,
                "type_index": idx + 1,
                "total_types": total_types,
                "message": f"Đang crawl {crawl_type} ({idx + 1}/{total_types})..."
            }
            
            try:
                if crawl_type == "tokhai":
                    # Crawl tờ khai - thứ tự: session_id, tokhai_type, start_date, end_date
                    async for result in self.crawl_tokhai(session_id, tokhai_type, start_date, end_date):
                        # Forward progress events với prefix
                        if result.get("type") == "complete":
                            batch_results["tokhai"] = result
                            yield {
                                "type": "type_complete",
                                "crawl_type": "tokhai",
                                "result": result
                            }
                        elif result.get("type") == "zip_data":
                            # Lưu zip_data vào batch_results
                            if batch_results.get("tokhai"):
                                batch_results["tokhai"]["zip_base64"] = result.get("zip_base64")
                            # Forward event
                            yield {
                                **result,
                                "crawl_type": "tokhai"
                            }
                        elif result.get("type") == "error":
                            yield {
                                "type": "type_error",
                                "crawl_type": "tokhai",
                                "error": result.get("error")
                            }
                        else:
                            # Forward info/progress events
                            yield {
                                **result,
                                "crawl_type": "tokhai"
                            }
                
                elif crawl_type == "thongbao":
                    # Crawl thông báo
                    async for result in self.crawl_thongbao(session_id, start_date, end_date):
                        if result.get("type") == "complete":
                            batch_results["thongbao"] = result
                            # Nếu có zip_base64 trong complete event, giữ lại
                            yield {
                                "type": "type_complete",
                                "crawl_type": "thongbao",
                                "result": result
                            }
                        elif result.get("type") == "zip_data":
                            if batch_results.get("thongbao"):
                                batch_results["thongbao"]["zip_base64"] = result.get("zip_base64")
                            yield {
                                **result,
                                "crawl_type": "thongbao"
                            }
                        elif result.get("type") == "error":
                            yield {
                                "type": "type_error",
                                "crawl_type": "thongbao",
                                "error": result.get("error")
                            }
                        else:
                            yield {
                                **result,
                                "crawl_type": "thongbao"
                            }
                
                elif crawl_type == "giaynoptien":
                    # Crawl giấy nộp tiền
                    async for result in self.crawl_giay_nop_tien(session_id, start_date, end_date):
                        if result.get("type") == "complete":
                            batch_results["giaynoptien"] = result
                            yield {
                                "type": "type_complete",
                                "crawl_type": "giaynoptien",
                                "result": result
                            }
                        elif result.get("type") == "zip_data":
                            if batch_results.get("giaynoptien"):
                                batch_results["giaynoptien"]["zip_base64"] = result.get("zip_base64")
                            yield {
                                **result,
                                "crawl_type": "giaynoptien"
                            }
                        elif result.get("type") == "error":
                            yield {
                                "type": "type_error",
                                "crawl_type": "giaynoptien",
                                "error": result.get("error")
                            }
                        else:
                            yield {
                                **result,
                                "crawl_type": "giaynoptien"
                            }
                
            except Exception as e:
                logger.error(f"Error crawling {crawl_type}: {e}")
                yield {
                    "type": "type_error",
                    "crawl_type": crawl_type,
                    "error": str(e)
                }
        
        # Tổng hợp kết quả cuối cùng
        # Merge tất cả ZIP files thành 1 ZIP duy nhất
        merged_zip_buffer = BytesIO()
        total_files = 0
        total_size = 0
        all_results = []
        
        with zipfile.ZipFile(merged_zip_buffer, 'w', zipfile.ZIP_DEFLATED) as merged_zip:
            for crawl_type, result in batch_results.items():
                if result and result.get("zip_base64"):
                    try:
                        # Decode ZIP của từng loại
                        type_zip_bytes = base64.b64decode(result["zip_base64"])
                        type_zip_buffer = BytesIO(type_zip_bytes)
                        
                        with zipfile.ZipFile(type_zip_buffer, 'r') as type_zip:
                            for file_info in type_zip.filelist:
                                # Thêm prefix folder theo loại
                                new_name = f"{crawl_type}/{file_info.filename}"
                                file_data = type_zip.read(file_info.filename)
                                merged_zip.writestr(new_name, file_data)
                                total_files += 1
                                total_size += len(file_data)
                        
                        # Collect results
                        if result.get("results"):
                            for r in result["results"]:
                                r["crawl_type"] = crawl_type
                                all_results.append(r)
                    except Exception as e:
                        logger.warning(f"Error merging ZIP for {crawl_type}: {e}")
        
        # Encode merged ZIP
        merged_zip_base64 = base64.b64encode(merged_zip_buffer.getvalue()).decode('utf-8') if total_files > 0 else None
        zip_filename = f"batch_crawl_{start_date.replace('/', '')}_{end_date.replace('/', '')}.zip"
        
        yield {
            "type": "batch_complete",
            "message": f"Hoàn thành crawl {total_types} loại dữ liệu",
            "total_files": total_files,
            "total_size": total_size,
            "results": all_results,
            "batch_results": {
                crawl_type: {
                    "total": result.get("total", 0) if result else 0,
                    "files_count": result.get("files_count", 0) if result else 0,
                    "total_size": result.get("total_size", 0) if result else 0,
                    "zip_base64": result.get("zip_base64") if result else None,
                    "zip_filename": result.get("zip_filename") if result else None,
                    "results": result.get("results", []) if result else []
                }
                for crawl_type, result in batch_results.items()
                if crawl_type in crawl_types
            },
            "zip_base64": merged_zip_base64,
            "zip_filename": zip_filename
        }


# Singleton instance - sẽ được khởi tạo với session_manager
_tax_crawler_instance = None

def get_tax_crawler() -> TaxCrawlerService:
    global _tax_crawler_instance
    if _tax_crawler_instance is None:
        from .session_manager import session_manager
        _tax_crawler_instance = TaxCrawlerService(session_manager)
    return _tax_crawler_instance

# Backwards compatibility
tax_crawler = None  # Will be lazy-initialized

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
from bs4 import BeautifulSoup
from openpyxl import Workbook
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
    
    async def _navigate_to_tokhai_search(self, session: SessionData) -> bool:
        """
        Navigate đến trang tra cứu tờ khai (giống flow Selenium cũ)
        Returns: True nếu thành công
        """
        page = session.page
        
        try:
            # Đảm bảo đang ở default content
            await page.goto(f'{BASE_URL}/etaxnnt/Request?dse_sessionId={session.dse_session_id}&dse_applicationId=-1', wait_until='networkidle')
            await asyncio.sleep(1)
            
            # Click vào menu "Kế toán thuế" (li thứ 3 trong menu)
            menu_items = page.locator('div.div-menu ul li')
            if await menu_items.count() >= 3:
                await menu_items.nth(2).click()  # Index 2 = li thứ 3
                await asyncio.sleep(0.5)
            
            # Click vào "Tra cứu tờ khai" (li thứ 8 trong submenu)
            submenu_items = page.locator('div.div-left-menu ul li')
            if await submenu_items.count() >= 8:
                await submenu_items.nth(7).click()  # Index 7 = li thứ 8
                await asyncio.sleep(1)
            
            return True
            
        except Exception as e:
            logger.error(f"Error navigating to tokhai search: {e}")
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
            
            # Navigate đến trang tra cứu tờ khai
            try:
                await page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[8]').click(timeout=10000)
                logger.info("Clicked directly on tra cuu tokhai menu")
            except Exception as e1:
                logger.info(f"Direct click failed: {e1}, trying menu first...")
                try:
                    menu_ke_toan = page.locator('//html/body/div[1]/div[2]/ul/li[3]')
                    await menu_ke_toan.click(timeout=5000)
                    await asyncio.sleep(1)
                    tra_cuu = page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[8]')
                    await tra_cuu.click(timeout=10000)
                except Exception as e2:
                    yield {"type": "error", "error": f"Không thể navigate đến trang tra cứu. Lỗi: {str(e2)}"}
                    return
            
            await asyncio.sleep(1)
            
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
            
            # Log current URL
            logger.info(f"Current URL: {page.url}")
            
            # Navigate đến trang tra cứu tờ khai
            # Đầu tiên click vào menu "Kê khai thuế" (menu chính)
            try:
                # Thử click trực tiếp vào tra cứu tờ khai
                await page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[8]').click(timeout=10000)
                logger.info("Clicked directly on tra cuu tokhai menu")
            except Exception as e1:
                logger.info(f"Direct click failed: {e1}, trying menu first...")
                # Thử click menu Kế toán thuế trước
                try:
                    menu_ke_toan = page.locator('//html/body/div[1]/div[2]/ul/li[3]')
                    await menu_ke_toan.click(timeout=5000)
                    logger.info("Clicked on Ke toan thue menu")
                    await asyncio.sleep(1)
                    
                    # Sau đó click vào tra cứu tờ khai
                    tra_cuu = page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[8]')
                    await tra_cuu.click(timeout=10000)
                    logger.info("Clicked on tra cuu tokhai submenu")
                except Exception as e2:
                    logger.error(f"Menu click failed: {e2}")
                    # Screenshot để debug
                    try:
                        screenshot = await page.screenshot()
                        logger.info(f"Page screenshot taken, size: {len(screenshot)} bytes")
                    except:
                        pass
                    yield {"type": "error", "error": f"Không thể navigate đến trang tra cứu. Lỗi: {str(e2)}"}
                    return
            
            await asyncio.sleep(1)
            
            # Switch to mainframe
            frame = page.frame('mainframe')
            if not frame:
                yield {"type": "error", "error": "Không tìm thấy mainframe"}
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
                    end_input = frame.locator('#qryToDate')
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
                        # Tờ khai đặc biệt - không có link <a> (giống code cũ)
                        # Dùng URL trực tiếp với dse_pageId=14 và messageId={id_tk}
                        current_ssid = ssid
                        
                        # Nếu ssid không có, thử lấy lại từ performance logs (giống code cũ)
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
                                        logger.info(f"Retrieved dse_sessionId from performance log: {current_ssid}")
                                        break
                            except Exception as e:
                                logger.warning(f"Error getting dse_sessionId from performance logs: {e}")
                        
                        if current_ssid and current_ssid != "NotFound":
                            dse_processor_id = ""
                            try:
                                processor_id_input = frame.locator('input[name="dse_processorId"]')
                                if await processor_id_input.count() > 0:
                                    dse_processor_id = await processor_id_input.first.get_attribute('value') or ""
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
            
            # Navigate đến trang tra cứu thông báo
            try:
                await page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[9]').click(timeout=10000)
            except:
                try:
                    await page.locator('//html/body/div[1]/div[2]/ul/li[3]').click(timeout=5000)
                    await asyncio.sleep(0.5)
                    await page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[9]').click(timeout=10000)
                except:
                    yield {"type": "error", "error": "Không thể navigate đến trang tra cứu thông báo"}
                    return
            
            await asyncio.sleep(1)
            
            # Switch to mainframe
            frame = page.frame('mainframe')
            if not frame:
                yield {"type": "error", "error": "Không tìm thấy mainframe"}
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
                    # Nhập ngày bắt đầu
                    start_input = frame.locator('//html/body/form/div[2]/div[1]/table[1]/tbody/tr[3]/td[2]/input')
                    await start_input.fill('')
                    await start_input.fill(date_range[0])
                    
                    # Nhập ngày kết thúc
                    end_input = frame.locator('//html/body/form/div[2]/div[1]/table[1]/tbody/tr[3]/td[3]/input')
                    await end_input.click()
                    await end_input.fill('')
                    await end_input.fill(date_range[1])
                    
                    # Click tìm kiếm
                    search_btn = frame.locator('//html/body/form/div[2]/div[1]/table[2]/tbody/tr/td/div/input')
                    await search_btn.click()
                    
                    await asyncio.sleep(2)
                    
                    # Xử lý phân trang (giống tờ khai)
                    check_pages = True
                    while check_pages:
                        # Tìm bảng kết quả
                        try:
                            table_body = frame.locator('//html/body/form/div[2]/div[3]/div[2]/div[2]/div/div/table/tbody')
                            await table_body.wait_for(timeout=5000)
                        except:
                            if total_count == 0:
                                yield {"type": "info", "message": f"Không có thông báo trong khoảng {date_range[0]} - {date_range[1]}"}
                            break
                        
                        rows = table_body.locator('tr')
                        row_count = await rows.count()
                        
                        yield {"type": "progress", "current": total_count, "message": f"Đang xử lý {row_count} thông báo (trang hiện tại)..."}
                        
                        download_queue = []
                        page_valid_count = 0  # Đếm số items hợp lệ trong trang này
                        
                        for i in range(row_count):
                            try:
                                row = rows.nth(i)
                                cols = row.locator('td')
                                col_count = await cols.count()
                                
                                if col_count < 3:
                                    continue
                                
                                # Cột 2: Mã giao dịch (theo HTML contentthongbao.html)
                                id_tb = await cols.nth(2).text_content()
                                id_tb = id_tb.strip() if id_tb else ""
                                
                                if not id_tb:
                                    continue
                                
                                # Chỉ đếm khi item hợp lệ
                                page_valid_count += 1
                                
                                # Cột 1: Số thông báo
                                so_tb = await cols.nth(1).text_content()
                                so_tb = so_tb.strip() if so_tb else ""
                                
                                # Cột 3: Tên thông báo
                                ten_tb = await cols.nth(3).text_content()
                                ten_tb = ten_tb.strip() if ten_tb else ""
                                
                                # Cột 4: Ngày gửi
                                ngay_gui = await cols.nth(4).text_content()
                                ngay_gui = ngay_gui.strip() if ngay_gui else ""
                                
                                if not id_tb:
                                    continue
                                
                                result = {
                                    "id": id_tb,
                                    "so_thong_bao": so_tb,
                                    "ten_thong_bao": ten_tb,
                                    "ngay_gui": ngay_gui,
                                    "type": "thongbao"
                                }
                                results.append(result)
                                
                                yield {"type": "item", "data": result}
                                
                                # Lưu vào queue để download batch (cột 5 là Tải về)
                                if col_count > 5:
                                    # Tạo tên file từ thông tin thông báo (giống code cũ)
                                    file_name = f"{id_tb} - {ten_tb[:50]} - {ngay_gui.replace('/', '-')}"
                                    file_name = self._remove_accents(file_name)
                                    file_name = file_name.replace("/", "_").replace(":", "_").replace("\\", "_")
                                    
                                    download_queue.append({
                                        "id": id_tb,
                                        "so_thong_bao": so_tb,
                                        "ten_thong_bao": ten_tb,
                                        "ngay_gui": ngay_gui,
                                        "file_name": file_name,
                                        "cols": cols,
                                        "col_index": 5
                                    })
                            
                            except Exception as e:
                                logger.error(f"Error processing row: {e}")
                                continue
                        
                        # Batch download
                        if download_queue:
                            await self._batch_download_thongbao(session, download_queue, temp_dir, frame)
                        
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
            if os.listdir(temp_dir):
                file_list = os.listdir(temp_dir)
                
                for file_name in file_list:
                    file_path = os.path.join(temp_dir, file_name)
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
                
                # Tạo ZIP
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for file_name in os.listdir(temp_dir):
                        file_path = os.path.join(temp_dir, file_name)
                        if os.path.isfile(file_path):
                            zf.write(file_path, file_name)
                
                zip_base64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')
            else:
                zip_base64 = None
            
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
    
    async def _batch_download_thongbao(self, session: SessionData, download_queue: List[Dict], temp_dir: str, frame):
        """Download thông báo song song"""
        semaphore = asyncio.Semaphore(5)
        page = session.page
        
        async def download_one(item: Dict):
            async with semaphore:
                try:
                    id_tb = item["id"]
                    file_name = item.get("file_name", id_tb)
                    cols = item["cols"]
                    col_idx = item["col_index"]
                    
                    download_link = cols.nth(col_idx).locator('a')
                    if await download_link.count() > 0:
                        async with page.expect_download(timeout=30000) as download_info:
                            await download_link.first.click()
                        
                        download = await download_info.value
                        save_path = os.path.join(temp_dir, file_name + ".xml" if not file_name.endswith(".xml") else file_name)
                        await download.save_as(save_path)
                        logger.info(f"Downloaded thongbao {id_tb} -> {file_name}")
                except Exception as e:
                    logger.warning(f"Error downloading {item['id']}: {e}")
        
        await asyncio.gather(*[download_one(item) for item in download_queue], return_exceptions=True)
    
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
            
            # Navigate đến trang giấy nộp thuế
            try:
                await page.locator('//html/body/div[1]/div[2]/ul/li[4]').click(timeout=10000)
                await asyncio.sleep(0.5)
                await page.locator('//html/body/div[1]/div[3]/div/div[4]/ul/li[4]').click(timeout=10000)
            except:
                yield {"type": "error", "error": "Không thể navigate đến trang tra cứu giấy nộp thuế"}
                return
            
            await asyncio.sleep(1)
            
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
                        
                        # Batch download
                        if download_queue:
                            await self._batch_download_giaynoptien(session, download_queue, temp_dir, frame)
                        
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
            if os.listdir(temp_dir):
                file_list = os.listdir(temp_dir)
                nnn = 0
                
                for file_name in file_list:
                    file_path = os.path.join(temp_dir, file_name)
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
                
                # Tạo ZIP
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                    for file_name in os.listdir(temp_dir):
                        file_path = os.path.join(temp_dir, file_name)
                        if os.path.isfile(file_path):
                            zf.write(file_path, file_name)
                
                zip_base64 = base64.b64encode(zip_buffer.getvalue()).decode('utf-8')
            else:
                zip_base64 = None
            
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
    
    async def _batch_download_giaynoptien(self, session: SessionData, download_queue: List[Dict], temp_dir: str, frame):
        """Download giấy nộp thuế song song (giống tool cũ)"""
        semaphore = asyncio.Semaphore(5)
        page = session.page
        name_r = 0
        
        async def download_one(item: Dict):
            nonlocal name_r
            async with semaphore:
                try:
                    id_gnt = item["id"]
                    
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
                                return
                        else:
                            logger.warning(f"Missing link_locator or row/col_index for {id_gnt}")
                            return
                    
                    # Download file (sẽ được lưu tạm với tên mặc định)
                    async with page.expect_download(timeout=30000) as download_info:
                        await download_link.click()
                    
                    download = await download_info.value
                    
                    # Lưu file với tên tạm
                    temp_name = f"chungtu_{id_gnt}_{name_r}.xml"
                    name_r += 1
                    save_path = os.path.join(temp_dir, temp_name)
                    await download.save_as(save_path)
                    
                    # Đợi file được lưu xong
                    await asyncio.sleep(0.3)
                    
                    logger.info(f"Downloaded giaynoptien {id_gnt} -> {temp_name}")
                except Exception as e:
                    logger.warning(f"Error downloading {item.get('id', 'unknown')}: {e}")
        
        await asyncio.gather(*[download_one(item) for item in download_queue], return_exceptions=True)
    
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
            # Navigate đến trang tra cứu tờ khai
            try:
                await page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[8]').click(timeout=10000)
            except:
                try:
                    await page.locator('//html/body/div[1]/div[2]/ul/li[3]').click(timeout=5000)
                    await asyncio.sleep(0.5)
                    await page.locator('//html/body/div[1]/div[3]/div/div[3]/ul/li[8]').click(timeout=10000)
                except:
                    return {"success": False, "error": "Không thể navigate đến trang tra cứu"}
            
            await asyncio.sleep(1)
            
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

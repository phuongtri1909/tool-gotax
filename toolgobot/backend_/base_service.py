from requests import get, adapters, Session
from urllib3 import poolmanager
import requests,time,random
from requests import adapters, Session
from urllib3 import poolmanager
from ssl import create_default_context, Purpose, CERT_NONE
import os
from PyQt5.QtGui import QPixmap
import uuid
import base64
import re
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
import logging

logger = logging.getLogger(__name__)

# Lazy-load captcha solvers (tránh import TensorFlow khi chỉ tra cứu DN; TensorFlow DLL dễ lỗi trên Windows)
_solver = None
_solver_cmt = None

def get_solver():
    """Get or create solver instance (singleton). Import TensorFlow chỉ khi gọi lần đầu."""
    global _solver
    try:
        if _solver is None:
            from toolgobot.backend_.captcha_solver import CaptchaSolver
            _solver = CaptchaSolver()
            logger.info("Loaded model captcha solver (MST)")
    except Exception as e:
        logger.error("Error loading model captcha solver (MST): %s", e)
    return _solver

def get_solver_cmt():
    """Get or create CMT solver instance (singleton). Import TensorFlow chỉ khi gọi lần đầu."""
    global _solver_cmt
    if _solver_cmt is None:
        from toolgobot.backend_.captchasolverCMT import CaptchaSolver as CaptchaSolverCMT
        _solver_cmt = CaptchaSolverCMT()
        logger.info("Loaded model captcha solver (CMT)")
    return _solver_cmt
class CustomHttpAdapter (adapters.HTTPAdapter):
    
    def __init__(self, ssl_context=None, **kwargs):
        
        self.ssl_context = ssl_context
        super().__init__(**kwargs)
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = poolmanager.PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, ssl_context=self.ssl_context)

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/122.0"
]

# Reason: Lazy-load onnxruntime DLL only when actually needed (Windows DLL safety)
class BaseService:
    def __init__(self, proxy_url=None):
        self._solver = None  # Lazy: chỉ load TensorFlow khi dùng captcha (tra CMT/CCCD)
        self.session = Session()
        # Path theo thu muc backend_ de chay dung khi subprocess cwd khac (gotax_root)
        _gobot_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.base_temp_dir = os.path.join(_gobot_root, "__pycache__")
        self.risklist_file = os.path.join(self.base_temp_dir, "risklist.txt")
        self.session_id = str(uuid.uuid4())[:8]
        self.csv_buffer = StringIO()      
        self.html_buffer = StringIO()   
        self.json_buffer = StringIO()
        
        # ✅ Setup proxy ONCE khi khởi tạo session
        if proxy_url:
            self.session.proxies = {
                'http': proxy_url
            }
            logger.info("Proxy configured: %s", proxy_url[:50] + "..." if len(proxy_url or "") > 50 else proxy_url)
        if not os.path.exists(self.base_temp_dir):
            os.makedirs(self.base_temp_dir)

    @property
    def solver(self):
        if self._solver is None:
            self._solver = get_solver()
        return self._solver

    def _create_ssl_suppressed_session(self):
        ctx = create_default_context(Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = CERT_NONE
        # accepting legacy connections
        ctx.options |= 0x4    
        session = Session()
        session.mount('https://', CustomHttpAdapter(ctx))
        return session
    def check_risk(self,taxcode = "0"):
        with open(self.risklist_file, 'r', encoding='utf-8') as file:
            self.lines = file.readlines()
            if taxcode+'\n' in self.lines:
                return True
        return False
    def check_id_type(self,id_number:str = ""):
        if len(id_number) == 9:
            return "CMND"
        elif len(id_number) == 12:
            return "CCCD"
        elif len(id_number) == 10 or len(id_number.split("-")[0]) == 10:
            return "MST"
        else:
            return "UNKNOWN"   
    def get_random_ua(self):
        return random.choice(user_agents) 
    def get_captcha(self,logger=None):
        url = "https://tracuunnt.gdt.gov.vn/tcnnt/captcha.png"

        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    cookies = response.cookies.get_dict()
                    text = self.solver.solve(response.content)
                    if logger:
                        logger.info("Captcha solved: %s", text)
                    return text, cookies
                else:
                    logger.warning("Get captcha status %s, retry...", response.status_code)
                    continue
            except Exception as e:
                print(f"⚠️ Get captcha error: {e}, retry...")
                continue

        return None, {} 
class BaseServiceCMT:
    def __init__(self, proxy_url=None):
        _gobot_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.base_temp_dir = os.path.join(_gobot_root, "__pycache__")
        self.risklist_file = os.path.join(self.base_temp_dir, "risklist.txt")
        self.session_id = str(uuid.uuid4())[:8]
        self.csv_buffer = StringIO()      
        self.html_buffer = StringIO()   
        self.json_buffer = StringIO()
        self.proxy_url = proxy_url  # ✅ Lưu để recreate session
        self._solver = None  # Lazy: chỉ load TensorFlow khi dùng captcha
        self.session = requests.Session()
        
        if proxy_url:
            self.session.proxies = {
                'http': proxy_url
            }
            logger.info("CMT Proxy configured")
        software_names = [SoftwareName.CHROME.value]
        os_list = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(software_names=software_names, operating_systems=os_list)
        self.ua = user_agent_rotator.get_random_user_agent()
    
    @property
    def solver(self):
        if self._solver is None:
            self._solver = get_solver_cmt()
        return self._solver

    def _recreate_session_with_new_proxy(self):
        """✅ Tạo session mới + rotate proxy IP"""
        logger.info("CMT: Recreating session + rotating proxy IP...")
        self.session = requests.Session()
        
        if self.proxy_url:
            self.session.proxies = {
                'http': self.proxy_url
            }
    
    def get_captcha(self,headers):
        url3 = "https://canhantmdt.gdt.gov.vn/ICanhan/servlet/ImageServlet"
        # ✅ Proxy đã setup trong session
        r3 = self.session.get(url3, headers=headers, timeout=30)
        solved_captcha_ = self.solver.solve_captcha(r3.content)
        if solved_captcha_["status"] == "success":
            captcha_text = solved_captcha_.get("text", "unknown")
            return captcha_text
        else:
            return None
    def get_dse(self):
        headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7,fr-FR;q=0.6,fr;q=0.5",
        "Referer": "https://canhantmdt.gdt.gov.vn/",
        "User-Agent": self.ua,
        "Origin": "https://canhantmdt.gdt.gov.vn"
        }
        # ✅ Proxy đã setup trong session
        r1 = self.session.get("https://canhantmdt.gdt.gov.vn", headers=headers, timeout=30)
        soup1 = BeautifulSoup(r1.text, "html.parser")
        
        script_content = str(soup1)
        match = re.search(r"dse_sessionId=([^&]+)", script_content)
        session_id = match.group(1)
        url2 = f"https://canhantmdt.gdt.gov.vn/ICanhan/Request?&dse_sessionId={session_id.strip()}&dse_applicationId=-1&dse_pageId=8&dse_operationName=retailTraCuuMSTCNTMDTProc&dse_processorState=initial&dse_nextEventName=start"

        r2 = self.session.get(url2, headers=headers, timeout=30)
        soup1 = BeautifulSoup(r2.text, "html.parser")
        '''with open("t.html", "w", encoding="utf-8") as f:
            f.write(r2.text)'''
        session_id = soup1.find("input", {"name": "dse_sessionId"})["value"]
        processor_id = soup1.find("input", {"name": "dse_processorId"})["value"]
        page_id = soup1.find("input", {"name": "dse_pageId"})["value"]
        captcha_text = self.get_captcha(headers)
        payload = {
            "session": self.session,
            "dse_sessionId": session_id,
            "dse_processorId": processor_id,
            "dse_pageId": page_id,
            "headers": headers,
            "captcha": captcha_text
        }
        return payload
import requests
import json
import time
import logging
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('testclf.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# List of User-Agents to try when 403 error occurs
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]   

def extract_company_data(html_content):
    """
    Extract company data from HTML based on unique tag identifiers
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    company_data = {}
    
    # Extract representative (Người đại diện): tr with itemprop='alumni' is unique
    ndd_tr = soup.find('tr', {'itemprop': 'alumni'})
    if ndd_tr:
        name_span = ndd_tr.find('span', {'itemprop': 'name'})
        if name_span:
            company_data['representative_name'] = name_span.get_text(strip=True)
    
    # Extract all tr rows to find CQTQL, LHDN, NNC by icon class
    all_rows = soup.find_all('tr')
    
    for row in all_rows:
        # Find CQTQL (Quản lý bởi) - icon fa-users
        i_tag = row.find('i', class_='fa-users')
        if i_tag:
            span_copy = row.find('span', class_='copy')
            if span_copy:
                company_data['tax_agency'] = span_copy.get_text(strip=True)
        
        # Find LHDN (Loại hình DN) - icon fa-building
        i_tag = row.find('i', class_='fa-building')
        if i_tag:
            link = row.find('a')
            if link and '/tra-cuu-ma-so-thue-theo-loai-hinh-doanh-nghiep/' in link.get('href', ''):
                company_data['company_type'] = link.get_text(strip=True)
        
        # Find NNC (Ngành nghề chính) - icon fa-briefcase
        i_tag = row.find('i', class_='fa-briefcase')
        if i_tag:
            link = row.select_one('td:has(a)')
            if link:
                company_data['main_industry'] = link.get_text(strip=True)
    # Extract industries list (LNN) - from "Ngành nghề kinh doanh" table
    # Find the table with h3 header "Ngành nghề kinh doanh"
    industries_list = []
    h3_tags = soup.find_all('h3')
    for h3 in h3_tags:
        if 'Ngành nghề kinh doanh' in h3.get_text():
            # Find the next table after this h3
            table = h3.find_next('table')
            if table:
                tbody = table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        tds = row.find_all('td')
                        if len(tds) >= 2:
                            code_a = tds[0].find('a')
                            job_a = tds[1].find('a')
                            if code_a and job_a:
                                code = code_a.get_text(strip=True)
                                job = job_a.get_text(strip=True)
                                industries_list.append({'code': code, 'job': job})
    
    if industries_list:
        company_data['industries_list'] = industries_list
    
    return company_data

def get_data_Company(tax_code,session=None,proxy_dict=None):
    """
    Get company data with automatic User-Agent rotation on 403 error
    """
    
    '''base_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
    }'''
    base_headers = {
        'accept-language': 'vi-VN,vi;q=0.9',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://masothue.com',
        'priority': 'u=1, i',
        'referer': 'https://masothue.com/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    
    url = f"https://masothue.com/Search/?q={tax_code}&type=auto&token=&force-search=0"
    headers = base_headers.copy()
    
    while 1:
        try:
            r = session.get(url, headers=headers, timeout=10)
            # If success (200), save and return
            if r.status_code == 200:
                with open("testclf.html", "w", encoding="utf-8") as f:
                    f.write(r.text)
                
                # Extract company data
                company_data = extract_company_data(r.text)
                
                return {
                    "status_code": r.status_code,
                    "content": r.text,
                    "company_data": company_data,
                }
            
            # If not 403, return immediately (don't retry)
            if r.status_code == 403 or r.status_code == 429:
                session = requests.Session()
                if proxy_dict:
                    session.proxies.update(proxy_dict) 
                else:
                    time.sleep(3)
                logger.info(f"Got {r.status_code} Forbidden, retrying...")
        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            continue
    
    # All attempts exhausted
    return {
        "status_code": 403,
        "content": None,
        "company_data": {},
        "ua_used": None,
        "attempts": len(USER_AGENTS),
        "error": "All User-Agents exhausted, still getting 403"
    }

def process_tax_codes(tax_codes_list, proxy_url=None):
    if proxy_url:
        proxy_dict = {
                'http': proxy_url,
                'https': proxy_url
            }
    else:
        proxy_dict = None
    results = []
    session = requests.Session()
    if proxy_dict:
        session.proxies.update(proxy_dict)
    
    for index, tax_code in enumerate(tax_codes_list, start=1):
        logger.info(f"\n{'='*60}")
        logger.info(f"INDEX: {index}")
        logger.info(f"Tax Code: {tax_code}")
        logger.info('='*60)
        
        result = get_data_Company(tax_code, session=session,proxy_dict=proxy_dict)
        
        if result.get('error'):
            logger.error(f"Error: {result['error']}")
        results.append(result)
    
    return results


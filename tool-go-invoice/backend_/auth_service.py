from .base_service import BaseService

class AuthService(BaseService):
    def __init__(self, proxy_url=None):
        super().__init__(proxy_url=proxy_url)
    def getckey_captcha(self):
        try:
            response = self.session.get('https://hoadondientu.gdt.gov.vn:30000/captcha',verify=False)
            getCaptcha = response.json()
            
            # ✅ Kiểm tra getCaptcha có phải là dict không
            if not isinstance(getCaptcha, dict):
                return {
                    'ckey': None,
                    'svg_content': None
                }
            
            self.ckey = getCaptcha.get('key')
            self.svg_content = getCaptcha.get('content')
            return {
                'ckey': self.ckey,
                'svg_content': self.svg_content
            }
        except Exception as e:
            return {
                'ckey': None,
                'svg_content': None
            }
    def login_web(self,ckey=None,captcha_inp=None,user=None,pass_=None):
        try:
            payload = { 
                'ckey': ckey,
                'cvalue': captcha_inp,
                'password': pass_,
                'username': user
            } 
            response = self.session.post('https://hoadondientu.gdt.gov.vn:30000/security-taxpayer/authenticate',verify=False,json=payload)
            login_to = response.json()
            
            # ✅ Kiểm tra login_to có phải là dict không
            if not isinstance(login_to, dict):
                return {
                    "status":"error",
                    "message":"Login failed - Invalid response"
                }
            
            if 'token' in login_to:
                self.token_ = login_to.get('token')
                self.headers = {
                    "status":"success",
                    "Authorization":'Bearer ' + self.token_,
                    "token": self.token_  # Thêm token trực tiếp
                }       
                return self.headers
            else:
                return {
                    "status":"error",
                    "message":"Login failed"
                }
        except Exception as e:
            return {
                "status":"error",
                "message":f"Login failed: {str(e)}"
            }
"""
Proxy Manager - Quản lý danh sách proxy với cơ chế round-robin
Dùng từ api_server.py để chia proxy cho các requests

Cách hoạt động:
- Request 1 → Proxy 1
- Request 2 → Proxy 2
- Request 3 → Proxy 3
- Request 4 → Proxy 1 (quay lại đầu)
"""

import os
import threading
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

class ProxyManager:
    """
    Quản lý proxy list với cơ chế round-robin
    
    Cách hoạt động:
    - Request 1 → Proxy 1
    - Request 2 → Proxy 2
    - Request 3 → Proxy 3
    - Request 4 → Proxy 1 (quay lại đầu)
    """
    
    def __init__(self, proxy_file: str = "proxylist.txt"):
        """
        Khởi tạo Proxy Manager
        
        Args:
            proxy_file: Đường dẫn tới file chứa danh sách proxy (mỗi dòng 1 proxy)
        """
        self.proxy_file = proxy_file
        self.proxies: List[str] = []
        self.current_index = 0
        self.lock = threading.Lock()  # ✅ Thread-safe cho multi-request
        self._warned_no_proxy = False  # ✅ Chỉ warning một lần
        
        # Load proxy từ file
        self._load_proxies()
    
    def _load_proxies(self) -> None:
        """Load danh sách proxy từ file"""
        try:
            if not os.path.exists(self.proxy_file):
                logger.warning(f"⚠️ Proxy file không tồn tại: {self.proxy_file}")
                return
            
            with open(self.proxy_file, 'r', encoding='utf-8') as f:
                # Đọc từng dòng, loại bỏ spaces + lines trống
                proxies = [line.strip() for line in f if line.strip()]
            
            if not proxies:
                logger.warning(f"⚠️ Proxy file trống: {self.proxy_file}")
                return
            
            self.proxies = proxies
            self._warned_no_proxy = False  # ✅ Reset flag khi có proxy
            logger.info(f"✅ Đã load {len(self.proxies)} proxy từ {self.proxy_file}")
            for i, proxy in enumerate(self.proxies, 1):
                logger.debug(f"   [{i}] {proxy}")
        
        except Exception as e:
            logger.error(f"❌ Lỗi load proxy file: {e}")
    
    def get_next_proxy(self) -> Optional[str]:
        """
        Lấy proxy tiếp theo theo cơ chế round-robin
        
        Returns:
            str: Proxy URL, hoặc None nếu không có proxy
        
        Example:
            proxy1 = manager.get_next_proxy()  # "proxy1"
            proxy2 = manager.get_next_proxy()  # "proxy2"
            proxy3 = manager.get_next_proxy()  # "proxy3"
            proxy4 = manager.get_next_proxy()  # "proxy1" (quay lại)
        """
        if not self.proxies:
            # ✅ Chỉ warning một lần để tránh spam log
            if not self._warned_no_proxy:
                logger.warning("❌ Không có proxy trong danh sách! (Chỉ hiển thị một lần)")
                self._warned_no_proxy = True
            return None
        
        with self.lock:
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            logger.info(f"📌 Phân phối proxy: {proxy}")
            return proxy
    
    def reload_proxies(self) -> None:
        """Tải lại danh sách proxy từ file (hữu ích khi update file)"""
        logger.info("🔄 Đang tải lại danh sách proxy...")
        with self.lock:
            self.proxies = []
            self.current_index = 0
            self._warned_no_proxy = False  # ✅ Reset warning flag khi reload
            self._load_proxies()
    
    def get_all_proxies(self) -> List[str]:
        """Lấy toàn bộ danh sách proxy"""
        return self.proxies.copy()
    
    def get_proxy_count(self) -> int:
        """Lấy số lượng proxy hiện có"""
        return len(self.proxies)
    
    def get_current_index(self) -> int:
        """Lấy index hiện tại (dùng cho debug/monitoring)"""
        return self.current_index
    
    def reset_index(self) -> None:
        """Reset index quay về 0 (restart round-robin)"""
        with self.lock:
            self.current_index = 0
            logger.info("✅ Đã reset proxy index về 0")


# ============================================================
# GLOBAL INSTANCE - Singleton Pattern
# ============================================================

_proxy_manager_instance: Optional[ProxyManager] = None
_proxy_manager_lock = threading.Lock()


def get_proxy_manager(proxy_file: str = "proxylist.txt") -> ProxyManager:
    """
    Lấy global instance của ProxyManager (Singleton)
    
    Args:
        proxy_file: Đường dẫn tới file proxy
    
    Returns:
        ProxyManager instance
    """
    global _proxy_manager_instance
    
    if _proxy_manager_instance is None:
        with _proxy_manager_lock:
            if _proxy_manager_instance is None:
                _proxy_manager_instance = ProxyManager(proxy_file)
    
    return _proxy_manager_instance


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_next_proxy() -> Optional[str]:
    """Hàm tiện lợi: Lấy proxy tiếp theo"""
    manager = get_proxy_manager()
    return manager.get_next_proxy()


if __name__ == "__main__":
    # Test ProxyManager
    print("\n" + "="*80)
    print("🧪 TEST PROXY MANAGER - ROUND ROBIN")
    print("="*80 + "\n")
    
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    manager = ProxyManager("proxylist.txt")
    
    # Test: Round-robin distribution
    print("📌 Round-robin Distribution:")
    print("-" * 80)
    for i in range(1, 11):
        proxy = manager.get_next_proxy()
        print(f"Request {i}: {proxy}")
    
    # Info
    print("\n📌 Info:")
    print("-" * 80)
    print(f"Tổng proxy: {manager.get_proxy_count()}")
    print(f"Index hiện tại: {manager.get_current_index()}")
    
    print("\n✅ Test hoàn tất!")

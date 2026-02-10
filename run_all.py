import subprocess
import time
import sys
import os
import signal
import socket

# Try to import psutil, fallback if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️ psutil chưa được cài đặt. Chạy: pip install psutil")
    print("⚠️ Sẽ bỏ qua việc kill processes cũ tự động")

processes = []
shutdown_requested = False

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global shutdown_requested
    if not shutdown_requested:
        shutdown_requested = True
        print("\n🛑 Đang shutdown...")
        shutdown_all()

def kill_old_processes():
    """Kill các processes cũ đang chạy (api_server.py, workers). Trả về số process đã kill."""
    if not PSUTIL_AVAILABLE:
        print("⚠️ Bỏ qua dọn process cũ (pip install psutil)")
        return 0
    
    scripts_to_kill = [
        'api_server.py',
        'go_soft_worker.py',
        'go_quick_worker.py',
        'go_invoice_worker.py',
        'go_bot_worker.py'
    ]
    
    killed_count = 0
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info.get('cmdline', [])
                if not cmdline:
                    continue
                cmdline_str = ' '.join(cmdline).lower()
                for script in scripts_to_kill:
                    if script.lower() in cmdline_str and 'python' in cmdline_str:
                        try:
                            proc.terminate()
                            killed_count += 1
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        if killed_count > 0:
            time.sleep(2)
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = proc.info.get('cmdline', [])
                    if not cmdline:
                        continue
                    cmdline_str = ' '.join(cmdline).lower()
                    for script in scripts_to_kill:
                        if script.lower() in cmdline_str and 'python' in cmdline_str:
                            try:
                                proc.kill()
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                            break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
    except Exception as e:
        print(f"⚠️ Lỗi dọn process cũ: {e}")
    return killed_count

def check_port(port, host='127.0.0.1'):
    """Kiểm tra port đã được sử dụng chưa"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex((host, port))
            return result == 0  # Port đang được sử dụng
    except Exception:
        return False

def shutdown_all():
    """Shutdown all processes gracefully"""
    global processes
    if not processes:
        return
    
    # First, try graceful shutdown (SIGTERM on Unix, terminate on Windows)
    print("⏹️ Đang dừng các processes...")
    for p in processes:
        try:
            if sys.platform == "win32":
                # Windows: terminate() sends CTRL_BREAK_EVENT which is more graceful than kill()
                p.terminate()
            else:
                # Unix: send SIGTERM for graceful shutdown
                p.send_signal(signal.SIGTERM)
        except Exception as e:
            print(f"⚠️ Lỗi khi terminate process {p.pid}: {e}")
    
    # Wait for processes to terminate (max 5 seconds)
    timeout = 5
    start_time = time.time()
    for p in processes:
        try:
            remaining_time = timeout - (time.time() - start_time)
            if remaining_time > 0:
                p.wait(timeout=remaining_time)
            else:
                break
        except subprocess.TimeoutExpired:
            print(f"⚠️ Process {p.pid} không dừng sau {timeout}s, force kill...")
            try:
                p.kill()
            except Exception as e:
                print(f"⚠️ Không thể kill process {p.pid}: {e}")
        except Exception as e:
            # Process may have already terminated
            pass
    
    # Force kill any remaining processes
    for p in processes:
        if p.poll() is None:  # Process still running
            try:
                print(f"🔪 Force killing process {p.pid}...")
                p.kill()
                try:
                    p.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            except Exception as e:
                print(f"⚠️ Không thể force kill process {p.pid}: {e}")
    
    print("✅ Tất cả processes đã dừng")

def run():
    global processes
    
    killed = kill_old_processes()
    if PSUTIL_AVAILABLE:
        print("✅ Process cũ: %s" % ("đã dọn %d" % killed if killed else "không có"))
    if check_port(5000) and PSUTIL_AVAILABLE:
        try:
            for proc in psutil.process_iter(['pid', 'name', 'connections']):
                try:
                    for conn in (proc.info.get('connections') or []):
                        if getattr(conn.laddr, 'port', None) == 5000:
                            proc.kill()
                            time.sleep(1)
                            break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception as e:
            print("⚠️ Port 5000: %s" % e)
    print("✅ Port 5000 sẵn sàng")
    
    python_cmd = "py" if sys.platform == "win32" else "python"
    num_go_quick_workers = 10
    cmds = [
        [python_cmd, "api_server.py"],
        [python_cmd, "workers/go_soft_worker.py"],
        [python_cmd, "workers/go_invoice_worker.py"],
        [python_cmd, "workers/go_bot_worker.py"],
    ]
    for _ in range(num_go_quick_workers):
        cmds.append([python_cmd, "workers/go_quick_worker.py"])
    try:
        signal.signal(signal.SIGINT, signal_handler)
        if sys.platform != "win32":
            signal.signal(signal.SIGTERM, signal_handler)
    except (ValueError, OSError):
        pass
    
    print("🚀 Khởi động: api_server, go_soft, go_invoice, go_bot, go_quick×%d" % num_go_quick_workers)
    for cmd in cmds:
        try:
            p = subprocess.Popen(
                cmd,
                shell=False,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            processes.append(p)
            time.sleep(0.5)
        except Exception as e:
            print("❌ Lỗi start %s: %s" % (" ".join(cmd), e))
    print("✅ %d processes đang chạy | Ctrl+C để dừng\n" % len(processes))

    try:
        # Vong lap sleep de Ctrl+C ngat duoc (tren Windows p.wait() co the khong nhan SIGINT)
        while True:
            alive = [p for p in processes if p.poll() is None]
            if not alive:
                print("Tat ca processes da thoat.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        if not shutdown_requested:
            shutdown_requested = True
            print("\n🛑 Ctrl+C - Đang shutdown...")
            shutdown_all()

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        if not shutdown_requested:
            shutdown_requested = True
            print("\n🛑 Ctrl+C - Đang shutdown...")
            shutdown_all()
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        shutdown_all()

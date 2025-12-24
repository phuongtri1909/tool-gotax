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
    """Kill các processes cũ đang chạy (api_server.py, workers)"""
    if not PSUTIL_AVAILABLE:
        print("⚠️ Bỏ qua kill processes cũ (psutil chưa được cài đặt)")
        return
    
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
                
                # Check if this process is running one of our scripts
                cmdline_str = ' '.join(cmdline).lower()
                for script in scripts_to_kill:
                    if script.lower() in cmdline_str and 'python' in cmdline_str:
                        print(f"🔪 Tìm thấy process cũ: PID {proc.info['pid']} - {script}")
                        try:
                            proc.terminate()
                            killed_count += 1
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                        break
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        if killed_count > 0:
            print(f"⏳ Đang đợi {killed_count} process cũ dừng...")
            time.sleep(2)
            
            # Force kill nếu vẫn còn chạy
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
            
            print(f"✅ Đã dọn dẹp {killed_count} process cũ")
        else:
            print("✅ Không có process cũ nào đang chạy")
    except Exception as e:
        print(f"⚠️ Lỗi khi kill processes cũ: {e}")

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
    
    # Bước 1: Kill các processes cũ
    print("🔍 Đang kiểm tra và dọn dẹp processes cũ...")
    kill_old_processes()
    
    # Bước 2: Kiểm tra port 5000 (api_server)
    print("🔍 Đang kiểm tra port 5000...")
    if check_port(5000):
        print("⚠️ Port 5000 đang được sử dụng. Đang kill process sử dụng port này...")
        if PSUTIL_AVAILABLE:
            try:
                for proc in psutil.process_iter(['pid', 'name', 'connections']):
                    try:
                        connections = proc.info.get('connections', [])
                        for conn in connections:
                            if conn.laddr.port == 5000:
                                print(f"🔪 Kill process {proc.info['pid']} đang dùng port 5000")
                                proc.kill()
                                time.sleep(1)
                                break
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
            except Exception as e:
                print(f"⚠️ Lỗi khi kill process dùng port 5000: {e}")
        else:
            print("⚠️ Không thể kill process dùng port 5000 (psutil chưa được cài đặt)")
            print("⚠️ Vui lòng kill process thủ công hoặc cài đặt psutil: pip install psutil")
    
    python_cmd = "py" if sys.platform == "win32" else "python"
    
    num_go_quick_workers = 10
    
    cmds = [
        [python_cmd, "api_server.py"],
        [python_cmd, "workers/go_soft_worker.py"],
    ]
    
    for i in range(num_go_quick_workers):
        cmds.append([python_cmd, "workers/go_quick_worker.py"])

    # Register signal handler for graceful shutdown
    try:
        signal.signal(signal.SIGINT, signal_handler)
        if sys.platform != "win32":
            signal.signal(signal.SIGTERM, signal_handler)
    except (ValueError, OSError):
        # Signal handler may not work in all contexts
        pass
    
    print("\n🚀 Đang khởi động các services...")
    for cmd in cmds:
        print(f"✅ Started: {' '.join(cmd)}")
        try:
            p = subprocess.Popen(cmd, shell=(sys.platform == "win32"))
            processes.append(p)
            time.sleep(0.2)  # Đợi một chút giữa các process để tránh conflict
        except Exception as e:
            print(f"❌ Lỗi khi start {' '.join(cmd)}: {e}")
    
    print(f"\n✅ Đã khởi động {len(processes)} processes")
    print("📋 Đang chạy... (Nhấn Ctrl+C để dừng)\n")

    try:
        # Wait for all processes
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        if not shutdown_requested:
            shutdown_all()

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        shutdown_all()
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        shutdown_all()

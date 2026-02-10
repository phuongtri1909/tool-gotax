import sys
import os

# Setup paths like routes.py
current_dir = os.path.dirname(os.path.abspath(__file__))
tool_root = os.path.dirname(current_dir)
if tool_root not in sys.path:
    sys.path.insert(0, tool_root)

print(f"Current dir: {current_dir}")
print(f"Tool root: {tool_root}")
print(f"Sys path[0]: {sys.path[0]}")

try:
    from shared.download_service import get_file_path, STORAGE_DIR
    print(f"Imported download_service from: {sys.modules['shared.download_service'].__file__}")
    print(f"STORAGE_DIR: {STORAGE_DIR}")
    
    download_id = "48fc64d3-e68c-47a9-a2ec-5a4a24bcd053"
    file_path = get_file_path(download_id, "xlsx")
    print(f"Resolved file_path: {file_path}")
    
    if file_path:
        exists = os.path.exists(file_path)
        print(f"File exists: {exists}")
        if exists:
            print(f"File size: {os.path.getsize(file_path)}")
    else:
        print("get_file_path returned None")

except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e:
    print(f"Error: {e}")

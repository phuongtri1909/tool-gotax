#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Test script để kiểm tra import hoạt động đúng
"""

import os
import sys

# Thêm tool-go-invoice vào path
tool_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, tool_path)

print("🔍 Testing imports...")
print(f"Tool path: {tool_path}")
print(f"sys.path[0]: {sys.path[0]}")

try:
    print("\n1️⃣ Importing BaseService...")
    from backend_.base_service import BaseService
    print("✅ BaseService imported successfully")
except Exception as e:
    print(f"❌ Error importing BaseService: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\n2️⃣ Importing AuthService...")
    from backend_.auth_service import AuthService
    print("✅ AuthService imported successfully")
except Exception as e:
    print(f"❌ Error importing AuthService: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\n3️⃣ Importing BackendService...")
    from backend_.backend_service import BackendService
    print("✅ BackendService imported successfully")
    
    # Check if tongquat_ method exists
    if hasattr(BackendService, 'tongquat_'):
        print("✅ tongquat_ method exists in BackendService")
    else:
        print("❌ tongquat_ method NOT found in BackendService")
        print(f"Available methods: {[m for m in dir(BackendService) if not m.startswith('_')]}")
except Exception as e:
    print(f"❌ Error importing BackendService: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\n4️⃣ Importing InvoiceBackend...")
    from InvoiceBackend import InvoiceBackend
    print("✅ InvoiceBackend imported successfully")
    
    print("\n5️⃣ Creating InvoiceBackend instance...")
    backend = InvoiceBackend()
    print("✅ InvoiceBackend instance created")
    
    print("\n6️⃣ Accessing backend_service...")
    bs = backend.backend_service
    print("✅ backend_service accessed successfully")
    
    print("\n7️⃣ Checking if tongquat_ method exists...")
    if hasattr(bs, 'tongquat_'):
        print("✅ tongquat_ method found!")
    else:
        print("❌ tongquat_ method NOT found!")
        print(f"Available methods: {[m for m in dir(bs) if not m.startswith('_') and callable(getattr(bs, m))]}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n✨ Test completed!")

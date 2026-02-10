#!/usr/bin/env python3
"""
Test script to verify PDF endpoint fix
"""
import requests
import json
from datetime import datetime, timedelta

BASE_URL = "http://localhost:5000"

def test_captcha():
    """Test 1: Get captcha"""
    print("\n=== TEST 1: Get Captcha ===")
    try:
        response = requests.get(f"{BASE_URL}/api/v1/invoice/get-captcha")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {data.keys()}")
            print("✓ Captcha endpoint works")
            return data.get('ckey'), data.get('captcha_base64')
        else:
            print(f"✗ Error: {response.text}")
            return None, None
    except Exception as e:
        print(f"✗ Exception: {e}")
        return None, None

def test_login(ckey, captcha_value):
    """Test 2: Login with captcha"""
    print("\n=== TEST 2: Login ===")
    try:
        payload = {
            "ckey": ckey,
            "captcha_value": captcha_value,
            "username": "test_user",  # Change to real username
            "password": "test_pass"   # Change to real password
        }
        response = requests.post(
            f"{BASE_URL}/api/v1/invoice/login",
            json=payload
        )
        print(f"Status: {response.status_code}")
        data = response.json()
        print(f"Response keys: {data.keys()}")
        if data.get('status') == 'success':
            token = data.get('token')
            print(f"✓ Login successful, token: {token[:20]}...")
            return token
        else:
            print(f"✗ Login failed: {data.get('message')}")
            return None
    except Exception as e:
        print(f"✗ Exception: {e}")
        return None

def test_tongquat(token):
    """Test 3: Tongquat"""
    print("\n=== TEST 3: Tongquat ===")
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        payload = {
            "Authorization": f"Bearer {token}",
            "type_invoice": 1,
            "start_date": start_date.strftime("%d/%m/%Y"),
            "end_date": end_date.strftime("%d/%m/%Y")
        }
        response = requests.post(
            f"{BASE_URL}/api/v1/invoice/tongquat",
            json=payload,
            timeout=120
        )
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {list(data.keys())}")
            if data.get('status') == 'success':
                print("✓ Tongquat endpoint works (JSON serializable)")
                return data
            else:
                print(f"✗ Error: {data.get('message')}")
                return None
        else:
            print(f"✗ Error: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"✗ Exception: {e}")
        return None

def test_pdf(token):
    """Test 4: PDF (this was the failing endpoint)"""
    print("\n=== TEST 4: PDF ===")
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        payload = {
            "Authorization": f"Bearer {token}",
            "type_invoice": 1,
            "start_date": start_date.strftime("%d/%m/%Y"),
            "end_date": end_date.strftime("%d/%m/%Y")
        }
        response = requests.post(
            f"{BASE_URL}/api/v1/invoice/pdf",
            json=payload,
            timeout=300
        )
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {list(data.keys())}")
            if 'token' in data:
                print(f"✓ Token in response: {data['token'][:20]}...")
            if 'data' in data and 'zip_bytes' in data['data']:
                zip_bytes_len = len(data['data']['zip_bytes'])
                print(f"✓ ZIP base64 encoded ({zip_bytes_len} chars)")
            print(f"✓ PDF endpoint works (JSON serializable)")
            return data
        else:
            print(f"✗ Error: {response.text[:500]}")
            return None
    except Exception as e:
        print(f"✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    print("Testing fixed API endpoints...")
    
    # Step 1: Get captcha
    ckey, captcha_b64 = test_captcha()
    if not ckey:
        print("Cannot proceed without captcha")
        return
    
    # For testing, we'll skip login and use a mock token
    # In real scenario, user would solve captcha and login
    print("\n=== SKIPPING LOGIN (requires manual captcha entry) ===")
    print("Using mock token for endpoint testing...")
    
    mock_token = "test-token-12345"
    
    # Test endpoints with mock token
    test_tongquat(mock_token)
    test_pdf(mock_token)
    
    print("\n=== Test Summary ===")
    print("If PDF and XMLhtml endpoints return JSON without serialization errors, the fix worked!")

if __name__ == "__main__":
    main()

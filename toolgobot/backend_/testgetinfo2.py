from getmst_info2 import process_tax_codes

# Chuẩn bị data
tax_codes = ["0110179092", "0316777988"]
'''proxy = {
    'http': 'http://user:pass@proxy:port',
    'https': 'http://user:pass@proxy:port',
}'''

# Gọi hàm
results = process_tax_codes(tax_codes)

# results là list chứa dict result của mỗi mst
for result in results:
    print(result['status_code'])
    print(result['company_data'])
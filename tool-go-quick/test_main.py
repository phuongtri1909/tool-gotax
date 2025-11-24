from main import CCCDExtractor
import json
import base64
import os

#====================================== 
zip_path = "./datatest/datatest.zip"
if os.path.exists(zip_path):
    with open(zip_path, "rb") as f:
        zip_bytes = f.read()
    
    go_quick = CCCDExtractor()
    task = {
        "func_type": 1,
        "inp_path": zip_bytes,
    }
    results = go_quick.handle_task(task)
    print(json.dumps(results, ensure_ascii=False, indent=2))
#====================================== 
# pdf_path = r"./datatest/cccd_list pdf.pdf"
# if os.path.exists(pdf_path):
#     with open(pdf_path, "rb") as f:
#         pdf_bytes = f.read()
    
#     go_quick = CCCDExtractor()
#     task = {
#         "func_type": 2,
#         "inp_path": pdf_bytes,
#     }
#     results = go_quick.handle_task(task)
    
#     if results["status"] == "success":
#         result_zip_bytes = base64.b64decode(results["zip_base64"])
#         with open("test_pdf_output.zip", "wb") as f:
#             f.write(result_zip_bytes)
    
#     print(json.dumps(results, ensure_ascii=False, indent=2))
# #======================================
# excel_path = r"./datatest/cccd_excel.xlsx"
# if os.path.exists(excel_path):
#     with open(excel_path, "rb") as f:
#         excel_bytes = f.read()
    
#     go_quick = CCCDExtractor()
#     task = {
#         "func_type": 3,
#         "inp_path": excel_bytes,
#     }
#     results = go_quick.handle_task(task)
    
#     if results["status"] == "success":
#         result_zip_bytes = base64.b64decode(results["zip_base64"])
#         with open("test_excel_output.zip", "wb") as f:
#             f.write(result_zip_bytes)
    
#     print(json.dumps(results, ensure_ascii=False, indent=2))

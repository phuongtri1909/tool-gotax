## GO QUICK 



## Cài đặt thư viện


```bash
pip install -r requirements.txt
```
## Cài đặt Python - WINDOWS 


```bash
https://www.python.org/ftp/python/3.11.0/python-3.11.0-amd64.exe
```

## CÁCH DÙNG 3 CHỨC NĂNG  - trong test_main.py
```bash
go_quick = CCCDExtractor()
task = {
    "func_type": 1, # 1:CCCD Extractor, 2:PDF2PNG, 3:XLSX2PNG
    "inp_path": r"C:\Users\PC\Desktop\tách source\ID Quick 2025\datatest\cccd_excel.xlsx",
}
results = go_quick.handle_task(task)
```
## CCCD Extractor
INP: PATH FOLDER CHỨA CÁC ẢNH CẦN TRÍCH XUẤT

OUT: JSON KẾT QUẢ
## PDF2PNG
INP: PATH FOLDER CHỨA CÁC FILE PDF CẦN CHUYỂN ĐỔI

OUT: JSON KẾT QUẢ TRẢ VỀ BYTE ZIP
## XLSX2PNG
INP: PATH FILE EXCEL CẦN CHUYỂN ĐỔI

OUT: JSON KẾT QUẢ TRẢ VỀ BYTE ZIP

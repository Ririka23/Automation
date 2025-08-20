# -*- coding: utf-8 -*-
import csv
from io import StringIO
from collections import OrderedDict

# ====== ตั้งค่า ======
# ไฟล์ต้นฉบับ (ที่ต้องแยก "ส่วนลดติดลบ")
src_file = r"C:\Users\kornkanok\Documents\Automation_api\Mizerp_api\2025-07-28_to_2025-07-31\sap1_2025-07-28_to_2025-07-31.txt"
# ไฟล์ปลายทางที่ต้องค้น DocNum ตรงกัน
lookup_file = r"C:\Users\kornkanok\Documents\Automation_api\Mizerp_api\2025-07-28_to_2025-07-31\sap2_2025-07-28_to_2025-07-31.txt"

keyword = "ส่วนลดติดลบ"
header_rows = 2  # ดึงหัว 2 แถวแรก

# เอาท์พุต
out_with_keyword   = r"C:\Users\kornkanok\Documents\Automation_api\minus_0\Edit_sap1(0)_with_keyword.txt"
out_without_keyword= r"C:\Users\kornkanok\Documents\Automation_api\minus_0\Edit_sap1.txt"
out_lookup_matches = r"C:\Users\kornkanok\Documents\Automation_api\minus_0\sap2_pull0.txt"
out_lookup_without = r"C:\Users\kornkanok\Documents\Automation_api\minus_0\Edit_sap2.txt"

# ====== ยูทิล ======
def read_text(path):
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return f.read()
    except UnicodeError:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

def detect_dialect(sample_text):
    sniffer = csv.Sniffer()
    try:
        return sniffer.sniff(sample_text, delimiters=[",", "\t", "|", ";"])
    except Exception:
        class Fallback(csv.Dialect):
            delimiter = "\t"
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return Fallback

def iter_rows(text, header_rows=1):
    lines = text.splitlines(keepends=True)
    return lines[:header_rows], lines[header_rows:]

def find_docnum_index(header_line, dialect):
    reader = csv.reader(StringIO(header_line), dialect=dialect)
    cols = next(reader)
    lowered = [c.strip().lower() for c in cols]
    for i, name in enumerate(lowered):
        if name == "docnum":
            return i
    return None

def get_field(line, idx, dialect):
    reader = csv.reader(StringIO(line), dialect=dialect)
    try:
        row = next(reader)
        if idx is None or idx >= len(row):
            return None
        return row[idx]
    except Exception:
        return None

# ====== ขั้นตอน 1: แยก "ส่วนลดติดลบ" จากไฟล์ต้นฉบับ ======
src_text = read_text(src_file)
src_header, src_body = iter_rows(src_text, header_rows=header_rows)

sample_for_sniff = "".join(src_header[-1:]) or "".join(src_body[:1])
src_dialect = detect_dialect(sample_for_sniff)

docnum_idx_src = find_docnum_index(src_header[-1], src_dialect)

lines_with_keyword = []
lines_without_keyword = []
docnums_found = OrderedDict()
count_keyword = 0

for lineno, line in enumerate(src_body, start=header_rows+1):
    occ = line.count(keyword)
    if occ > 0:
        lines_with_keyword.append(line)
        count_keyword += occ
        print(f"[ต้นฉบับ บรรทัด {lineno}] {line.strip()}")
        if docnum_idx_src is not None:
            dv = get_field(line, docnum_idx_src, src_dialect)
            if dv:
                docnums_found[dv] = True
    else:
        lines_without_keyword.append(line)

with open(out_with_keyword, "w", encoding="utf-8") as f:
    f.writelines(src_header)
    f.writelines(lines_with_keyword)

with open(out_without_keyword, "w", encoding="utf-8") as f:
    f.writelines(src_header)
    f.writelines(lines_without_keyword)

# ====== ขั้นตอน 2: ดึงข้อมูล DocNum ตรงกันจาก lookup และแยกที่ไม่ตรง ======
lookup_text = read_text(lookup_file)
lookup_header, lookup_body = iter_rows(lookup_text, header_rows=header_rows)

sample_for_sniff2 = "".join(lookup_header[-1:]) or "".join(lookup_body[:1])
lookup_dialect = detect_dialect(sample_for_sniff2)
docnum_idx_lookup = find_docnum_index(lookup_header[-1], lookup_dialect)

lookup_matches = []
lookup_without_matches = []

if docnum_idx_lookup is None:
    print("\n⚠️ ไม่พบคอลัมน์ชื่อ 'DocNum' ในไฟล์ lookup")
else:
    targets = set(docnums_found.keys())
    for lineno, line in enumerate(lookup_body, start=header_rows+1):
        dv = get_field(line, docnum_idx_lookup, lookup_dialect)
        if dv in targets:
            lookup_matches.append(line)
            print(f"[lookup บรรทัด {lineno}] match: {dv} | {line.strip()}")
        else:
            lookup_without_matches.append(line)

with open(out_lookup_matches, "w", encoding="utf-8") as f:
    f.writelines(lookup_header)
    f.writelines(lookup_matches)

with open(out_lookup_without, "w", encoding="utf-8") as f:
    f.writelines(lookup_header)
    f.writelines(lookup_without_matches)

# ====== สรุป ======
print("\n========== สรุป ==========")
print(f"พบคำว่า '{keyword}' ในไฟล์ต้นฉบับ {count_keyword} ครั้ง")
print(f"DocNum ที่พบ: {len(docnums_found)} รายการ")
print(f"บันทึกไฟล์ (มี keyword): {out_with_keyword}")
print(f"บันทึกไฟล์ (ไม่มี keyword): {out_without_keyword}")
print(f"บันทึกไฟล์ match จาก lookup: {out_lookup_matches}")
print(f"บันทึกไฟล์ที่เหลือจาก lookup: {out_lookup_without}")

# -*- coding: utf-8 -*-
import os, csv
from io import StringIO
from collections import Counter

# ====== ตั้งค่า ======
# เปลี่ยน path ให้ตรงกับไฟล์ Edit_sap1.txt ของคุณ
target_file = r"C:\Users\kornkanok\Documents\Automation_api\minus_0\Edit_sap1.txt"

# โฟลเดอร์ปลายทาง (ใช้โฟลเดอร์เดียวกับไฟล์ต้นฉบับ)
output_folder = os.path.dirname(target_file)
out_duplicates = os.path.join(output_folder, r"C:\Users\kornkanok\Documents\Automation_api\Check_Docnum\Double_DocNum.txt")

header_rows = 2  # จำนวนบรรทัดหัวตาราง

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

def find_col_index(header_line, col_name, dialect):
    reader = csv.reader(StringIO(header_line), dialect=dialect)
    cols = next(reader)
    lowered = [c.strip().lower() for c in cols]
    col_name = col_name.lower()
    for i, name in enumerate(lowered):
        if name == col_name:
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

# ====== อ่านไฟล์เป้าหมาย ======
text = read_text(target_file)
header_lines, body_lines = iter_rows(text, header_rows=header_rows)

# เดา delimiter จากหัวแถวสุดท้ายหรือบรรทัดข้อมูล
sample_for_sniff = "".join(header_lines[-1:]) or "".join(body_lines[:1])
dialect = detect_dialect(sample_for_sniff)

# หา index ของคอลัมน์ DocNum
docnum_idx = find_col_index(header_lines[-1], "DocNum", dialect)
if docnum_idx is None:
    raise RuntimeError("ไม่พบคอลัมน์ชื่อ 'DocNum' ในไฟล์ Edit_sap1.txt (ตรวจหัวตารางอีกครั้ง)")

# ====== นับ DocNum ======
cnt = Counter()
for line in body_lines:
    dv = get_field(line, docnum_idx, dialect)
    if dv is not None and dv != "":
        cnt[dv] += 1

# DocNum ที่ซ้ำ (จำนวน > 1)
duplicate_docnums = {k for k, v in cnt.items() if v > 1}

# ====== แยกบรรทัดที่ซ้ำ เขียนไฟล์ใหม่ ======
dup_lines = []
for lineno, line in enumerate(body_lines, start=header_rows+1):
    dv = get_field(line, docnum_idx, dialect)
    if dv in duplicate_docnums:
        dup_lines.append(line)
        # แสดงผลเพื่ออ้างอิง
        print(f"[dup line {lineno}] DocNum={dv} | {line.strip()}")

with open(out_duplicates, "w", encoding="utf-8") as f:
    f.writelines(header_lines)
    f.writelines(dup_lines)

# ====== สรุป ======
total_rows = len(body_lines)
total_docnums = sum(1 for k in cnt.keys())
total_dups = len(duplicate_docnums)
print("\n========== สรุป ==========")
print(f"แถวข้อมูลทั้งหมด (ไม่รวม header): {total_rows:,}")
print(f"DocNum ทั้งหมด (นับชนิด): {total_docnums:,}")
print(f"DocNum ที่ซ้ำ (ชนิด): {total_dups:,}")
print(f"จำนวนแถวที่ถูกดึงออก (ซ้ำ): {len(dup_lines):,}")
print(f"ไฟล์ที่บันทึกบรรทัด DocNum ซ้ำ: {out_duplicates}")

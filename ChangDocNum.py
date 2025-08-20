# -*- coding: utf-8 -*-
import csv
from io import StringIO
from collections import defaultdict

# ====== ตั้งค่า ======
target_file = r"C:\Users\kornkanok\Documents\Automation_api\minus_0\Edit_sap1.txt"
output_file = r"C:\Users\kornkanok\Documents\Automation_api\Check_Docnum\ChangDocNum.txt"

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

def parse_row(line, dialect):
    return next(csv.reader(StringIO(line), dialect=dialect))

def make_line(fields, dialect):
    output = StringIO()
    # กำหนด lineterminator="" ป้องกันการเว้นบรรทัด
    writer = csv.writer(output, dialect=dialect, lineterminator="\n")
    writer.writerow(fields)
    return output.getvalue()

# ====== โหลดไฟล์ ======
text = read_text(target_file)
header_lines, body_lines = iter_rows(text, header_rows=header_rows)

sample_for_sniff = "".join(header_lines[-1:]) or "".join(body_lines[:1])
dialect = detect_dialect(sample_for_sniff)

docnum_idx = find_col_index(header_lines[-1], "DocNum", dialect)
if docnum_idx is None:
    raise RuntimeError("ไม่พบคอลัมน์ชื่อ 'DocNum' ในไฟล์ Edit_sap1.txt")

# ====== แก้ DocNum ซ้ำ ======
seen = defaultdict(int)
fixed_lines = []

for line in body_lines:
    row = parse_row(line, dialect)
    docnum_val = row[docnum_idx].strip()

    # แปลงเป็นตัวเลขถ้าทำได้
    try:
        base_num = int(docnum_val)
    except ValueError:
        base_num = None

    seen[docnum_val] += 1
    if seen[docnum_val] > 1:
        if base_num is not None:
            new_num = base_num + (seen[docnum_val] - 1)
            print(f"แก้ DocNum ซ้ำ: {docnum_val} -> {new_num}")
            row[docnum_idx] = str(new_num)
        else:
            new_val = f"{docnum_val}_{seen[docnum_val]-1}"
            print(f"แก้ DocNum ซ้ำ (non-numeric): {docnum_val} -> {new_val}")
            row[docnum_idx] = new_val

    fixed_lines.append(make_line(row, dialect))

# ====== บันทึกไฟล์ (ไม่มีบรรทัดเว้น) ======
with open(output_file, "w", encoding="utf-8", newline="") as f:
    f.writelines(header_lines)
    f.writelines(fixed_lines)

print("\n✅ แก้ไข DocNum ซ้ำเรียบร้อย (ไม่มีบรรทัดว่าง)")
print(f"ไฟล์ที่บันทึก: {output_file}")
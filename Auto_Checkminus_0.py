# -*- coding: utf-8 -*-
import time
import csv
import re
from typing import Optional
from io import StringIO
from collections import OrderedDict
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ====== ตั้งค่า ======
WATCH_ROOT = r"C:\Users\kornkanok\Documents\Automation_api\Mizerp_api"  # โฟลเดอร์ที่เฝ้า
FOLDER_NAME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2}$")  # ชื่อโฟลเดอร์ใหม่ (ปรับได้)
SLEEP_AFTER_CREATE = 2  # หน่วงเวลา(วินาที)ให้ไฟล์เขียนจบ ก่อนเริ่มอ่าน
HEADER_ROWS = 2
KEYWORD = "ส่วนลดติดลบ"

SAP1_GLOB = "sap1_*.txt"
SAP2_GLOB = "sap2_*.txt"

# ====== ยูทิล ======
def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except UnicodeError:
        return path.read_text(encoding="utf-8")

def detect_dialect(sample_text: str):
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

def iter_rows(text: str, header_rows: int = 1):
    lines = text.splitlines(keepends=True)
    return lines[:header_rows], lines[header_rows:]

def find_docnum_index(header_line: str, dialect) -> Optional[int]:
    reader = csv.reader(StringIO(header_line), dialect=dialect)
    cols = next(reader)
    lowered = [c.strip().lower() for c in cols]
    for i, name in enumerate(lowered):
        if name == "docnum":
            return i
    return None

def get_field(line: str, idx: Optional[int], dialect) -> Optional[str]:
    reader = csv.reader(StringIO(line), dialect=dialect)
    try:
        row = next(reader)
        if idx is None or idx >= len(row):
            return None
        return row[idx]
    except Exception:
        return None

def process_folder(folder: Path):
    """ประมวลผลหนึ่งโฟลเดอร์: หา sap1/sap2 แล้วทำแยก keyword + match DocNum"""
    print(f"\n=== เริ่มประมวลผลโฟลเดอร์: {folder} ===")
    out_dir = folder / "minus_0"
    out_dir.mkdir(exist_ok=True)

    # หาไฟล์ sap1 และ sap2
    sap1_files = sorted(folder.glob(SAP1_GLOB))
    sap2_files = sorted(folder.glob(SAP2_GLOB))

    if not sap1_files:
        print("⚠️ ไม่พบไฟล์ sap1_*.txt ในโฟลเดอร์นี้ ข้าม")
        return
    if not sap2_files:
        print("⚠️ ไม่พบไฟล์ sap2_*.txt ในโฟลเดอร์นี้ ข้าม")
        return

    # เลือกไฟล์แรกที่พบ (จะวนทุกไฟล์ก็ปรับเพิ่มได้)
    src_file = sap1_files[0]
    lookup_file = sap2_files[0]

    # ====== ขั้นตอน 1: แยก "ส่วนลดติดลบ" จาก sap1 ======
    src_text = read_text(src_file)
    src_header, src_body = iter_rows(src_text, header_rows=HEADER_ROWS)

    sample_for_sniff = "".join(src_header[-1:]) or "".join(src_body[:1])
    src_dialect = detect_dialect(sample_for_sniff)
    docnum_idx_src = find_docnum_index(src_header[-1], src_dialect)

    lines_with_keyword = []
    lines_without_keyword = []
    docnums_found = OrderedDict()
    count_keyword = 0

    for lineno, line in enumerate(src_body, start=HEADER_ROWS + 1):
        # จุดทำงานเกี่ยวกับ "ส่วนลดติดลบ"
        occ = line.count(KEYWORD)
        if occ > 0:
            lines_with_keyword.append(line)
            count_keyword += occ
            dv = get_field(line, docnum_idx_src, src_dialect)
            dv = (dv or "").strip()
            if dv:
                docnums_found[dv] = True
        else:
            lines_without_keyword.append(line)

    # เขียนผลลัพธ์ sap1
    out_with_keyword   = out_dir / "sap1_only_negative.txt"
    out_without_keyword= out_dir / "sap1_without_negative.txt"

    (out_with_keyword).write_text("".join(src_header) + "".join(lines_with_keyword), encoding="utf-8")
    (out_without_keyword).write_text("".join(src_header) + "".join(lines_without_keyword), encoding="utf-8")

    # ====== ขั้นตอน 2: ดึง DocNum ตรงกันจาก sap2 ======
    lookup_text = read_text(lookup_file)
    lookup_header, lookup_body = iter_rows(lookup_text, header_rows=HEADER_ROWS)

    sample_for_sniff2 = "".join(lookup_header[-1:]) or "".join(lookup_body[:1])
    lookup_dialect = detect_dialect(sample_for_sniff2)
    docnum_idx_lookup = find_docnum_index(lookup_header[-1], lookup_dialect)

    lookup_matches = []
    lookup_without_matches = []

    if docnum_idx_lookup is None:
        print("⚠️ ไม่พบคอลัมน์ DocNum ในไฟล์ sap2")
    else:
        targets = set(docnums_found.keys())
        for line in lookup_body:
            dv = get_field(line, docnum_idx_lookup, lookup_dialect)
            dv = (dv or "").strip()
            if dv in targets:
                lookup_matches.append(line)
            else:
                lookup_without_matches.append(line)

    out_lookup_matches = out_dir / "sap2_match_negative_docnums.txt"
    out_lookup_without = out_dir / "sap2_without_negative_docnums.txt"

    (out_lookup_matches).write_text("".join(lookup_header) + "".join(lookup_matches), encoding="utf-8")
    (out_lookup_without).write_text("".join(lookup_header) + "".join(lookup_without_matches), encoding="utf-8")

    # ====== สรุป ======
    print("========== สรุป ==========")
    print(f"โฟลเดอร์: {folder.name}")
    print(f"พบคำว่า '{KEYWORD}' ใน {src_file.name} ทั้งหมด {count_keyword} ครั้ง")
    print(f"DocNum ที่พบ: {len(docnums_found)} รายการ")
    print(f"บันทึกไฟล์ sap1 (เฉพาะที่มีคำ): {out_with_keyword}")
    print(f"บันทึกไฟล์ sap1 (ที่เหลือ):    {out_without_keyword}")
    print(f"บันทึกไฟล์ sap2 (match):        {out_lookup_matches}")
    print(f"บันทึกไฟล์ sap2 (not match):    {out_lookup_without}")
    print("================================\n")

class NewFolderHandler(FileSystemEventHandler):
    """จับเหตุการณ์โฟลเดอร์ใหม่ถูกสร้างใน WATCH_ROOT"""
    def on_created(self, event):
        if not event.is_directory:
            return
        new_path = Path(event.src_path)
        if not FOLDER_NAME_PATTERN.match(new_path.name):
            print(f"พบโฟลเดอร์ใหม่ แต่ชื่อไม่ตรง pattern: {new_path.name} -> ข้าม")
            return

        print(f"📁 พบโฟลเดอร์ใหม่: {new_path} รอ {SLEEP_AFTER_CREATE}s ให้ไฟล์เซฟเสร็จ...")
        time.sleep(SLEEP_AFTER_CREATE)

        try:
            process_folder(new_path)
        except Exception as e:
            print(f"❌ เกิดข้อผิดพลาดขณะประมวลผล {new_path}: {e}")

def main():
    root = Path(WATCH_ROOT)
    if not root.exists():
        raise SystemExit(f"โฟลเดอร์ที่เฝ้าไม่มีอยู่จริง: {root}")

    print(f"เริ่มเฝ้าโฟลเดอร์: {root}")
    observer = Observer()
    event_handler = NewFolderHandler()
    observer.schedule(event_handler, str(root), recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("หยุดเฝ้าโฟลเดอร์")
    finally:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    main()

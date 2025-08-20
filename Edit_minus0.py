import os
import re
import csv
import argparse
from io import StringIO
from typing import Optional, Dict, List
from pathlib import Path

# -------------------- ค่าดีฟอลต์ --------------------
DEFAULT_KEYWORD = "ส่วนลดติดลบ"
DEFAULT_HEADER_ROWS = 2
CASE_INSENSITIVE = True  # ปรับได้

# คอลัมน์ใน master
MASTER_CODE_COL_DEFAULT = "Code"
MASTER_MSRP_COL_DEFAULT = "msrp"

# -------------------- CSV helpers --------------------
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

def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig")
    except UnicodeError:
        return path.read_text(encoding="utf-8")

# -------------------- ตัวช่วยตัวเลข --------------------
def parse_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1]
    for ch in ["฿", "$", "€", " ", "\u00a0"]:
        t = t.replace(ch, "")
    t = t.replace(",", "")
    try:
        v = float(t)
        return -v if neg else v
    except ValueError:
        return None

# -------------------- โหลด MasterData (Code -> msrp) --------------------
def load_msrp_map(path: Path,
                  code_col: str = MASTER_CODE_COL_DEFAULT,
                  msrp_col: str = MASTER_MSRP_COL_DEFAULT) -> Dict[str, float]:
    encodings = ["utf-8-sig", "cp874", "tis-620", "cp1252", "latin-1"]
    last_err = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc) as f:
                reader = csv.DictReader(f)
                # map ชื่อคอลัมน์แบบ case-insensitive
                cols = {c.lower(): c for c in (reader.fieldnames or [])}
                code_c = cols.get(code_col.lower())
                msrp_c = cols.get(msrp_col.lower())
                if not code_c or not msrp_c:
                    raise RuntimeError(f"ไม่พบคอลัมน์ '{code_col}' หรือ '{msrp_col}' ใน {path}")

                out: Dict[str, float] = {}
                for row in reader:
                    code = (row.get(code_c) or "").strip().upper()
                    msrp_v = parse_float(row.get(msrp_c))
                    if code and msrp_v is not None:
                        out[code] = msrp_v
                return out
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"เปิด/อ่าน MasterData ล้มเหลว: {last_err}")

# -------------------- เขียนทับค่าด้วย MSRP ใน sap1 --------------------
def rewrite_rows_with_msrp(header_line: str,
                           body_lines: List[str],
                           dialect,
                           msrp_map: Dict[str, float]) -> List[str]:
    """
    - หา index ของคอลัมน์: ProductCode, GPBefDisc, PriceAfVAT, GTotal, Quantity (รองรับ alias)
    - ถ้าหา msrp ของสินค้าพบนำไปแทน:
        GPBefDisc = msrp
        PriceAfVAT = msrp
        GTotal = msrp * Quantity
    """
    reader = csv.reader(StringIO(header_line), dialect=dialect)
    cols = next(reader)
    idx = {c.strip().lower(): i for i, c in enumerate(cols)}

    def find_col(*names) -> Optional[int]:
        for name in names:
            i = idx.get(name.lower())
            if i is not None:
                return i
        return None

    prod_idx = find_col("itemcode", "code", "productcode", "sku", "item_code")
    gpbef_idx = find_col("gpbefdisc", "gpbeforediscount", "gp_bef_disc")
    price_idx = find_col("priceafvat", "price_after_vat", "price")
    gtot_idx  = find_col("gtotal", "grandtotal", "total")
    qty_idx   = find_col("quantity", "qty", "qty1")

    # ถ้าคอลัมน์สำคัญหาไม่ครบ ให้คืนค่าตามเดิม
    if None in (prod_idx, gpbef_idx, price_idx, gtot_idx, qty_idx):
        return body_lines

    out: List[str] = []
    for line in body_lines:
        row = list(next(csv.reader(StringIO(line), dialect=dialect)))
        if prod_idx >= len(row):
            out.append(line); continue

        code = (row[prod_idx] or "").strip().upper()
        if not code:
            out.append(line); continue

        msrp = msrp_map.get(code)
        if msrp is None:
            out.append(line); continue

        q = parse_float(row[qty_idx] if qty_idx < len(row) else "")
        if q is None:
            q = 0.0

        def fmt(x: float) -> str:
            return f"{x:.2f}"

        if gpbef_idx < len(row): row[gpbef_idx] = fmt(msrp)
        if price_idx < len(row): row[price_idx] = fmt(msrp)
        if gtot_idx  < len(row): row[gtot_idx]  = fmt(msrp * q)

        sio = StringIO()
        w = csv.writer(sio, dialect=dialect)
        w.writerow(row)
        out.append(sio.getvalue())

    return out

# -------------------- งานหลัก --------------------
def process_folder(folder: Path,
                   master_csv: Path,
                   header_rows: int = DEFAULT_HEADER_ROWS,
                   keyword: str = DEFAULT_KEYWORD):
    print(f"\n=== เริ่มประมวลผลโฟลเดอร์: {folder} ===")

    # หาไฟล์ sap1/sap2
    def pick_first(patterns) -> Optional[Path]:
        for pat in patterns:
            for p in sorted(folder.glob(pat)):
                return p
        return None

    sap1 = pick_first(("sap1*.txt", "sap1.txt"))
    sap2 = pick_first(("sap2*.txt", "sap2.txt"))

    if not sap1 or not sap2:
        raise SystemExit("⚠️ ไม่พบไฟล์ sap1 หรือ sap2")

    # โหลด master msrp
    msrp_map = load_msrp_map(master_csv)
    print(f"[info] โหลด msrp ได้ {len(msrp_map)} รายการ")

    # อ่าน sap1
    src_text = read_text(sap1)
    src_header, src_body = iter_rows(src_text, header_rows=header_rows)

    sample_for_sniff = "".join(src_header[-1:]) or "".join(src_body[:1])
    src_dialect = detect_dialect(sample_for_sniff)
    docnum_idx_src = find_docnum_index(src_header[-1], src_dialect)

    # แยกบรรทัดที่มี/ไม่มี keyword
    lines_with_keyword: List[str] = []
    lines_without_keyword: List[str] = []
    docnums_found = {}
    count_keyword = 0

    for line in src_body:
        tgt_line = line.lower() if CASE_INSENSITIVE else line
        tgt_kw = keyword.lower() if CASE_INSENSITIVE else keyword
        occ = tgt_line.count(tgt_kw)
        if occ > 0:
            lines_with_keyword.append(line)
            count_keyword += occ
            dv = get_field(line, docnum_idx_src, src_dialect)
            dv = (dv or "").strip()
            if dv:
                docnums_found[dv] = True
        else:
            lines_without_keyword.append(line)

    # แทนค่าด้วย msrp ทั้งสองชุด
    lines_with_keyword    = rewrite_rows_with_msrp(src_header[-1], lines_with_keyword,    src_dialect, msrp_map)
    lines_without_keyword = rewrite_rows_with_msrp(src_header[-1], lines_without_keyword, src_dialect, msrp_map)

    # เขียน sap1 ออก
    out_dir = folder / "minus_0"
    out_dir.mkdir(exist_ok=True)
    out_sap1_onlyneg = out_dir / "sap1_only_negative.txt"
    out_sap1_wo      = out_dir / "sap1_without_negative.txt"
    out_sap1_onlyneg.write_text("".join(src_header) + "".join(lines_with_keyword), encoding="utf-8")
    out_sap1_wo.write_text("".join(src_header) + "".join(lines_without_keyword), encoding="utf-8")

    # อ่าน sap2 และทำไฟล์ focus: sap2_match_negative_docnums.txt
    lookup_text = read_text(sap2)
    lookup_header, lookup_body = iter_rows(lookup_text, header_rows=header_rows)
    sample_for_sniff2 = "".join(lookup_header[-1:]) or "".join(lookup_body[:1])
    lookup_dialect = detect_dialect(sample_for_sniff2)
    docnum_idx_lookup = find_docnum_index(lookup_header[-1], lookup_dialect)

    lookup_matches: List[str] = []
    lookup_without_matches: List[str] = []

    if docnum_idx_lookup is None:
        print("⚠️ ไม่พบคอลัมน์ DocNum ในไฟล์ sap2 (จะเขียนไฟล์ matches ว่าง)")
    else:
        targets = set(docnums_found.keys())
        for line in lookup_body:
            dv = get_field(line, docnum_idx_lookup, lookup_dialect)
            dv = (dv or "").strip()
            if dv in targets:
                lookup_matches.append(line)
            else:
                lookup_without_matches.append(line)

    out_lookup_matches = out_dir / "sap2_match_negative_docnums.txt"      # << โฟกัส
    out_lookup_without = out_dir / "sap2_without_negative_docnums.txt"

    out_lookup_matches.write_text("".join(lookup_header) + "".join(lookup_matches), encoding="utf-8")
    out_lookup_without.write_text("".join(lookup_header) + "".join(lookup_without_matches), encoding="utf-8")

    # สรุป
    print("\n========== สรุป ==========")
    print(f"พบคำว่า '{keyword}' ใน {sap1.name} ทั้งหมด {count_keyword} ครั้ง")
    print(f"DocNum ที่เกี่ยวข้อง: {len(docnums_found)} รายการ")
    print(f"เขียนไฟล์: {out_sap1_onlyneg.name}, {out_sap1_wo.name}")
    print(f"เขียนไฟล์โฟกัส: {out_lookup_matches.name}")
    print("==========================\n")

    return {
        "keyword_count": count_keyword,
        "docnum_count": len(docnums_found),
        "sap1_only_negative": str(out_sap1_onlyneg),
        "sap1_without_negative": str(out_sap1_wo),
        "sap2_match_negative_docnums": str(out_lookup_matches),
        "sap2_without_negative_docnums": str(out_lookup_without),
    }

# -------------------- CLI --------------------
def main():
    p = argparse.ArgumentParser(description="แยก 'ส่วนลดติดลบ' + แทน msrp + ทำ sap2_match_negative_docnums")
    p.add_argument("--folder", required=True, help="โฟลเดอร์ที่มี sap1_*.txt และ sap2_*.txt")
    p.add_argument("--master", required=True, help="ไฟล์ MasterData.csv")
    p.add_argument("--header-rows", type=int, default=DEFAULT_HEADER_ROWS, help="จำนวนบรรทัดหัวตาราง (ดีฟอลต์ 2)")
    p.add_argument("--keyword", default=DEFAULT_KEYWORD, help="คำที่ใช้ค้นหาใน sap1 (ดีฟอลต์ 'ส่วนลดติดลบ')")
    args = p.parse_args()

    folder = Path(args.folder)
    master = Path(args.master)
    if not folder.exists():
        raise SystemExit(f"โฟลเดอร์ไม่พบ: {folder}")
    if not master.exists():
        raise SystemExit(f"ไม่พบไฟล์ MasterData: {master}")

    process_folder(folder, master, header_rows=args.header_rows, keyword=args.keyword)

if __name__ == "__main__":
    main()

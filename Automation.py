import os, sys, csv, argparse, re, json, time, shutil
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin
import requests

# ---------- โหลด .env ให้ครอบคลุม ----------
# ---------- โหลด .env แบบครอบคลุม ----------
def _load_env_smart():
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    from pathlib import Path
    for p in [
        Path.cwd() / ".env",
        Path(__file__).resolve().with_name(".env"),
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]:
        if p.is_file():
            load_dotenv(p, override=False)
            break

_load_env_smart()
import os, requests

# ---------- โฟลเดอร์ปลายทาง (รองรับ DEFAULT_FOLDER ของคุณด้วย) ----------
DEFAULT_OUT_DIR = (
    os.getenv("FIXED_OUT_DIR")
    or os.getenv("DEFAULT_FOLDER")  # <- ของคุณใน .env
    or "/Users/pianoxyz/Documents/Automation-main/mizerp_api"
)

# ---------- LINE ENV (รองรับชื่อคีย์ทั้งสองแบบ) ----------
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

# เดิมสคริปต์ใช้ LINE_USER_ID / LINE_GROUP_ID
# รองรับคีย์ของคุณ: LINE_TO_ID, LINE_GROUP_IDS (คอมมาได้หลายกลุ่ม)
LINE_USER_ID = os.getenv("LINE_USER_ID") or os.getenv("LINE_TO_ID", "")
_line_groups_raw = (
    os.getenv("LINE_GROUP_IDS", "")  # "Cxxx,Cyyy"
    or os.getenv("LINE_GROUP_ID", "")  # เผื่อใครตั้งชื่อเดิม
)
LINE_GROUP_IDS = [g.strip() for g in _line_groups_raw.split(",") if g.strip()]

# ส่งสรุปเสมอจาก .env ได้ (เลือก)
LINE_NOTIFY_ALWAYS_DEFAULT = os.getenv("LINE_NOTIFY_ALWAYS", "false").lower() == "true"

def line_push(text: str) -> bool:
    tok = LINE_CHANNEL_ACCESS_TOKEN
    if not tok:
        print("⚠️ LINE token ไม่ถูกตั้งค่า (.env: LINE_CHANNEL_ACCESS_TOKEN)")
        return False
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    ok = True
    # ส่งไปทั้งผู้ใช้เดียว และหลายกลุ่มตามที่ตั้ง
    targets = []
    if LINE_USER_ID:
        targets.append(LINE_USER_ID)
    targets.extend(LINE_GROUP_IDS)
    if not targets:
        print("⚠️ ไม่พบ LINE target (ตั้ง LINE_TO_ID/LINE_USER_ID หรือ LINE_GROUP_IDS)")
        return False

    for to in targets:
        payload = {"to": to, "messages": [{"type": "text", "text": text}]}
        try:
            r = requests.post("https://api.line.me/v2/bot/message/push",
                              headers=headers, json=payload, timeout=20)
            print(f"[LINE] to={to[:1]}...{to[-4:]} status={r.status_code} body={r.text}")
            ok = ok and (r.status_code == 200)
        except Exception as e:
            print(f"[LINE] error sending to {to}: {e}")
            ok = False
    return ok

# ---------- ค่าพื้นฐาน ----------
DEFAULT_OUT_DIR     = os.getenv("FIXED_OUT_DIR", "/Users/pianoxyz/Documents/Automation-main/mizerp_api")
DEFAULT_HEADER_ROWS = int(os.getenv("HEADER_ROWS", "2"))
DEFAULT_KEYWORD     = os.getenv("KEYWORD", "ส่วนลดติดลบ")
CASE_INSENSITIVE    = os.getenv("CASE_INSENSITIVE", "true").lower() == "true"

MASTER_DATA_DEFAULT = os.getenv("MASTER_DATA_CSV", "/Users/pianoxyz/Documents/Automation-main/MasterData.xls")
MASTER_CODE_COL     = (os.getenv("MASTER_CODE_COL") or "code").strip()
MASTER_MSRP_COL     = (os.getenv("MASTER_MSRP_COL") or "msrp").strip()

BASE_URL        = (os.getenv("BASE_URL") or "https://mizerp.com").rstrip("/")
EXPORT_PATH     = os.getenv("EXPORT_PATH", "/wh/api/exportSap")
VERIFY_TLS      = os.getenv("VERIFY_TLS", "true").lower() == "true"
TIMEOUT         = int(os.getenv("TIMEOUT_SECONDS", "60"))
AUTH_HEADER_NAME  = os.getenv("AUTH_HEADER_NAME") or None
AUTH_HEADER_VALUE = os.getenv("AUTH_HEADER_VALUE") or None

SESSION = requests.Session()

# ---------- LINE ----------
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_ID  = os.getenv("LINE_USER_ID", "")
LINE_GROUP_ID = os.getenv("LINE_GROUP_ID", "")

def line_push(text: str) -> bool:
    """ส่งข้อความไป LINE (user/group ที่ตั้งใน .env)"""
    tok = LINE_CHANNEL_ACCESS_TOKEN
    if not tok:
        print("⚠️ LINE token ไม่ถูกตั้งค่า (.env: LINE_CHANNEL_ACCESS_TOKEN)")
        return False
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    ok = True
    for to in [LINE_USER_ID, LINE_GROUP_ID]:
        if not to:
            continue
        payload = {"to": to, "messages": [{"type": "text", "text": text}]}
        try:
            r = requests.post("https://api.line.me/v2/bot/message/push",
                              headers=headers, json=payload, timeout=20)
            print(f"[LINE] to={to[:1]}...{to[-4:]} status={r.status_code} body={r.text}")
            ok = ok and (r.status_code == 200)
        except Exception as e:
            print(f"[LINE] error sending to {to}: {e}")
            ok = False
    return ok

# ---------- โหลด .env แบบครอบคลุม ----------
def _load_env_smart():
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    from pathlib import Path
    for p in [
        Path.cwd() / ".env",
        Path(__file__).resolve().with_name(".env"),
        Path(__file__).resolve().parent / ".env",
        Path(__file__).resolve().parent.parent / ".env",
    ]:
        if p.is_file():
            load_dotenv(p, override=False)
            break

_load_env_smart()
import os, requests

# ---------- โฟลเดอร์ปลายทาง (รองรับ DEFAULT_FOLDER/FIXED_OUT_DIR) ----------
DEFAULT_OUT_DIR = (
    os.getenv("FIXED_OUT_DIR")
    or os.getenv("DEFAULT_FOLDER")
    or "/Users/pianoxyz/Documents/Automation-main/mizerp_api"
)

# ---------- LINE ENV (รองรับชื่อคีย์ทั้งสองแบบ) ----------
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_ID  = os.getenv("LINE_USER_ID") or os.getenv("LINE_TO_ID", "")
_line_groups_raw = os.getenv("LINE_GROUP_IDS", "") or os.getenv("LINE_GROUP_ID", "")
LINE_GROUP_IDS = [g.strip() for g in _line_groups_raw.split(",") if g.strip()]

LINE_NOTIFY_ALWAYS_DEFAULT = os.getenv("LINE_NOTIFY_ALWAYS", "false").lower() == "true"

def _obf(s: str) -> str:
    s = s or ""
    return f"{s[:3]}…{s[-4:]}" if len(s) > 8 else s

def line_push(text: str) -> bool:
    tok = LINE_CHANNEL_ACCESS_TOKEN
    if not tok:
        print("⚠️ LINE token ไม่ถูกตั้งค่า (.env: LINE_CHANNEL_ACCESS_TOKEN)")
        return False
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

    targets = []
    if LINE_USER_ID: targets.append(LINE_USER_ID)
    targets.extend(LINE_GROUP_IDS)
    if not targets:
        print("⚠️ ไม่พบ LINE target (ตั้ง LINE_USER_ID/LINE_TO_ID หรือ LINE_GROUP_IDS/LINE_GROUP_ID)")
        return False

    ok = True
    for to in targets:
        payload = {"to": to, "messages": [{"type": "text", "text": text}]}
        try:
            r = requests.post("https://api.line.me/v2/bot/message/push",
                              headers=headers, json=payload, timeout=20)
            print(f"[LINE] to={_obf(to)} status={r.status_code} body={r.text}")
            ok = ok and (r.status_code == 200)
        except Exception as e:
            print(f"[LINE] error sending to {to}: {e}")
            ok = False
    return ok

def line_diag():
    tok = LINE_CHANNEL_ACCESS_TOKEN
    print("=== LINE DIAG ===")
    print("token_len:", len(tok))
    print("user:", _obf(LINE_USER_ID))
    print("groups:", ", ".join(_obf(g) for g in LINE_GROUP_IDS) or "-")
    if not tok:
        print("❌ ไม่มี LINE_CHANNEL_ACCESS_TOKEN"); return
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    try:
        r = requests.get("https://api.line.me/v2/bot/info", headers=headers, timeout=10)
        print("bot/info:", r.status_code)
    except Exception as e:
        print("❌ bot/info error:", e)
    line_push("🔔 LINE DIAG: hello")
    print("=== END LINE DIAG ===")

# ---------- helpers ----------
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def iso_date(s):
    try:
        datetime.strptime(s, "%Y-%m-%d"); return s
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be yyyy-mm-dd")

def read_text(path: Path) -> str:
    try: return path.read_text(encoding="utf-8-sig")
    except UnicodeError: return path.read_text(encoding="utf-8")

def sanitize(name: str) -> str:
    for ch in '<>:"/\\|?*': name = name.replace(ch, "_")
    return name

# ---------- CSV helpers ----------
def detect_dialect(sample_text: str):
    sniffer = csv.Sniffer()
    try: return sniffer.sniff(sample_text, delimiters=[",","\t","|",";"])
    except Exception:
        class Fallback(csv.Dialect):
            delimiter="\t"; quotechar='"'; doublequote=True
            skipinitialspace=False; lineterminator="\n"; quoting=csv.QUOTE_MINIMAL
        return Fallback

def iter_rows(text: str, header_rows: int = 1):
    lines = text.splitlines(keepends=True)
    return lines[:header_rows], lines[header_rows:]

def find_index_ci(header_line: str, dialect, names):
    cols = next(csv.reader(StringIO(header_line), dialect=dialect))
    idx = {c.strip().lower(): i for i, c in enumerate(cols)}
    for name in names:
        i = idx.get(name.lower())
        if i is not None: return i
    return None

def get_field(line: str, idx, dialect):
    try:
        row = next(csv.reader(StringIO(line), dialect=dialect))
        if idx is None or idx >= len(row): return None
        return row[idx]
    except Exception:
        return None

# ---------- number helpers ----------
def parse_float(s):
    if s is None: return None
    t = str(s).strip()
    if not t: return None
    neg = t.startswith("(") and t.endswith(")")
    if neg: t = t[1:-1]
    for ch in ["฿","$","€"," ", "\u00a0"]: t = t.replace(ch,"")
    t = t.replace(",","")
    try:
        v = float(t); return -v if neg else v
    except ValueError:
        return None

def fmt2(x): return f"{x:.2f}"

# ---------- MasterData loader (csv/xlsx/xls) ----------
def load_msrp_map(master_path: Path, code_col="code", msrp_col="msrp"):
    suffix = master_path.suffix.lower()
    def _norm_cols(cols): return {str(c).strip().lower(): c for c in cols}

    if suffix in {".csv", ".txt"}:
        encs = ["utf-8-sig","cp874","tis-620","cp1252","latin-1"]
        last_err = None
        for enc in encs:
            try:
                with master_path.open("r", encoding=enc) as f:
                    rdr = csv.DictReader(f)
                    if not rdr.fieldnames: raise RuntimeError("no header in MasterData (csv)")
                    cols = _norm_cols(rdr.fieldnames)
                    ccol = cols.get(code_col.lower()); mcol = cols.get(msrp_col.lower())
                    if not ccol or not mcol: raise RuntimeError("missing columns")
                    out = {}
                    for row in rdr:
                        code = (row.get(ccol) or "").strip().upper()
                        msrp = parse_float(row.get(mcol))
                        if code and msrp is not None: out[code] = msrp
                    if not out: raise RuntimeError("empty msrp map (csv)")
                    return out
            except Exception as e:
                last_err = e; continue
        raise RuntimeError(f"read CSV failed: {last_err}")

    elif suffix in {".xlsx", ".xls"}:
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("ต้องติดตั้ง pandas / openpyxl / (xlrd==1.2.0 สำหรับ .xls)")
        engine = "openpyxl" if suffix == ".xlsx" else "xlrd"
        try:
            df = pd.read_excel(master_path, engine=engine)
        except Exception as e:
            raise RuntimeError(f"เปิด Excel ไม่ได้: {e}")
        if df.empty: raise RuntimeError("MasterData ว่าง")
        cols = _norm_cols(df.columns)
        ccol = cols.get(code_col.lower()); mcol = cols.get(msrp_col.lower())
        if not ccol or not mcol: raise RuntimeError("ไม่พบคอลัมน์ที่ต้องใช้ (code/msrp)")
        out = {}
        for _, row in df.iterrows():
            code = str(row.get(ccol) or "").strip().upper()
            msrp = parse_float(row.get(mcol))
            if code and msrp is not None: out[code] = msrp
        if not out: raise RuntimeError("อ่าน Excel ได้แต่ msrp_map ว่าง")
        return out

    else:
        raise RuntimeError(f"สกุลไฟล์ไม่รองรับ: {suffix}")

# ---------- API ----------
def build_post_headers():
    h = {"Accept": "application/json","Content-Type":"application/x-www-form-urlencoded"}
    if AUTH_HEADER_NAME and AUTH_HEADER_VALUE: h[AUTH_HEADER_NAME] = AUTH_HEADER_VALUE
    return h

def build_get_headers():
    h = {}
    if AUTH_HEADER_NAME and AUTH_HEADER_VALUE: h[AUTH_HEADER_NAME] = AUTH_HEADER_VALUE
    return h

def api_export(from_date, to_date):
    url = urljoin(BASE_URL + "/", EXPORT_PATH.lstrip("/"))
    payload = {"fromDate": from_date, "toDate": to_date}
    for attempt in range(6):
        try:
            r = SESSION.post(url, headers=build_post_headers(), data=payload,
                             timeout=TIMEOUT, verify=VERIFY_TLS, allow_redirects=True)
            if r.status_code in (429,502,503,504):
                time.sleep(min(30, 2**attempt)); continue
            if not (200 <= r.status_code < 300): r.raise_for_status()
            try: return r.json()
            except Exception: raise RuntimeError("API did not return JSON")
        except requests.RequestException:
            if attempt == 5: raise
            time.sleep(min(30, 2**attempt))

def download(url, dest):
    if not (url.startswith("http://") or url.startswith("https://")):
        url = urljoin(BASE_URL + "/", url.lstrip("/"))
    with SESSION.get(url, headers=build_get_headers(), stream=True,
                     timeout=TIMEOUT, verify=VERIFY_TLS, allow_redirects=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(262144):
                if chunk: f.write(chunk)

def have_required_files(folder: Path) -> bool:
    return any(folder.glob("sap1*.txt")) and any(folder.glob("sap2*.txt"))

# ---------- ขั้นตอนหลัก ----------
def split_sap1_and_collect_docnums(folder: Path, header_rows: int, keyword: str):
    def pick(patterns):
        for pat in patterns:
            for p in sorted(folder.glob(pat)): return p
        return None
    sap1 = pick(("sap1*.txt","sap1.txt"))
    if not sap1: raise RuntimeError("⚠️ ไม่พบไฟล์ sap1_*.txt")

    text = read_text(sap1)
    head, body = iter_rows(text, header_rows=header_rows)
    sample = "".join(head[-1:]) or "".join(body[:1])
    dialect = detect_dialect(sample)
    doc_idx = find_index_ci(head[-1], dialect, ["docnum"])

    with_kw, without_kw, docnums, cnt = [], [], {}, 0
    kw = keyword.lower() if CASE_INSENSITIVE else keyword
    for line in body:
        tgt = line.lower() if CASE_INSENSITIVE else line
        occ = tgt.count(kw)
        if occ > 0:
            with_kw.append(line); cnt += occ
            dv = get_field(line, doc_idx, dialect)
            dv = (dv or "").strip()
            if dv: docnums[dv] = True
        else:
            without_kw.append(line)

    out_dir = folder / "minus_0"; out_dir.mkdir(exist_ok=True)
    (out_dir/"sap1_only_negative.txt").write_text("".join(head)+"".join(with_kw), encoding="utf-8")
    (out_dir/"sap1_without_negative.txt").write_text("".join(head)+"".join(without_kw), encoding="utf-8")
    return {"docnums": set(docnums.keys()), "count_keyword": cnt}

def build_sap2_match(folder: Path, header_rows: int, docnums):
    def pick(patterns):
        for pat in patterns:
            for p in sorted(folder.glob(pat)): return p
        return None
    sap2 = pick(("sap2*.txt","sap2.txt"))
    if not sap2: raise RuntimeError("⚠️ ไม่พบไฟล์ sap2_*.txt")

    text = read_text(sap2)
    head, body = iter_rows(text, header_rows=header_rows)
    sample = "".join(head[-1:]) or "".join(body[:1])
    dialect = detect_dialect(sample)
    doc_idx = find_index_ci(head[-1], dialect, ["docnum"])

    matches, others = [], []
    tg = set(docnums)
    if doc_idx is None:
        print("⚠️ ไม่พบคอลัมน์ DocNum ใน sap2")
    for line in body:
        dv = get_field(line, doc_idx, dialect) if doc_idx is not None else None
        dv = (dv or "").strip()
        (matches if dv in tg else others).append(line)

    out_dir = folder / "minus_0"
    p_match = out_dir/"sap2_match_negative_docnums.txt"
    p_other = out_dir/"sap2_without_negative_docnums.txt"
    p_match.write_text("".join(head)+"".join(matches), encoding="utf-8")
    p_other.write_text("".join(head)+"".join(others), encoding="utf-8")
    return p_match

def apply_msrp_to_sap2_match(sap2_match_path: Path, master_csv: Path, header_rows: int):
    msrp_map = load_msrp_map(master_csv, code_col=MASTER_CODE_COL, msrp_col=MASTER_MSRP_COL)
    print(f"[info] msrp items loaded: {len(msrp_map)}")

    text = read_text(sap2_match_path)
    head, body = iter_rows(text, header_rows=header_rows)
    sample = "".join(head[-1:]) or "".join(body[:1])
    dialect = detect_dialect(sample)

    cols = next(csv.reader(StringIO(head[-1]), dialect=dialect))
    colmap = {c.strip().lower(): i for i, c in enumerate(cols)}
    def gi(nm): return colmap.get(nm.lower())

    idx_item  = gi("itemcode") or colmap.get("code")
    idx_gp    = gi("gpbefdisc")
    idx_price = gi("priceafvat") or colmap.get("price_after_vat")
    idx_total = gi("gtotal")
    idx_qty   = gi("quantity")
    idx_desc  = gi("itemdescription") or gi("item description") or gi("description") or gi("dscription") or gi("itemname")

    need = [("ItemCode", idx_item), ("GPBefDisc", idx_gp), ("PriceAfVAT", idx_price), ("GTotal", idx_total), ("Quantity", idx_qty)]
    miss = [n for n,i in need if i is None]
    if miss:
        raise RuntimeError("⚠️ หา column ไม่ครบใน sap2_match: %s\ncolumns: %s" % (miss, cols))

    updated, out_lines = 0, []
    for line in body:
        row = list(next(csv.reader(StringIO(line), dialect=dialect)))

        # ลบ "(แถม)" ในคำอธิบายสินค้า
        if idx_desc is not None and idx_desc < len(row):
            desc = row[idx_desc]
            if desc:
                desc = re.sub(r"\s*\(แถม\)\s*", " ", str(desc))
                desc = re.sub(r"\s{2,}", " ", desc).strip()
                row[idx_desc] = desc

        code = (row[idx_item] if idx_item < len(row) else "").strip().upper()
        if code:
            msrp = msrp_map.get(code)
            if msrp is not None:
                q = parse_float(row[idx_qty] if idx_qty < len(row) else "")
                if q is None: q = 0.0
                if idx_gp    < len(row): row[idx_gp]    = fmt2(msrp)
                if idx_price < len(row): row[idx_price] = fmt2(msrp)
                if idx_total < len(row): row[idx_total] = fmt2(msrp * q)
                updated += 1

        sio = StringIO(); csv.writer(sio, dialect=dialect).writerow(row)
        out_lines.append(sio.getvalue())

    sap2_match_path.write_text("".join(head) + "".join(out_lines), encoding="utf-8")
    print(f"[done] apply msrp + clean '(แถม)' -> updated {updated}/{len(body)} rows")
    return updated
def apply_msrp_to_sap2_main(folder: Path, master_csv: Path, header_rows: int, docnums: set, overwrite: bool = True):
    """
    อัปเดตไฟล์ sap2_* ตัวหลัก:
      - เฉพาะแถวที่ DocNum อยู่ใน 'docnums'
      - map MSRP ตาม ItemCode -> ใส่แทน GPBefDisc, PriceAfVAT และคำนวณ GTotal = msrp * Quantity
      - ลบคำว่า "(แถม)" ในคอลัมน์ ItemDescription (ถ้ามี)
    ถ้า overwrite=True: สำรองไฟล์เดิมเป็น .bak แล้วเขียนทับไฟล์เดิม
    return: (updated_count, output_path)
    """
    # หาไฟล์ sap2 ตัวหลัก
    def pick(patterns):
        for pat in patterns:
            for p in sorted((folder).glob(pat)):
                return p
        return None
    sap2_path = pick(("sap2*.txt", "sap2.txt"))
    if not sap2_path:
        raise RuntimeError("⚠️ ไม่พบไฟล์ sap2_*.txt สำหรับอัปเดตไฟล์หลัก")

    # โหลด msrp map
    msrp_map = load_msrp_map(master_csv, code_col=MASTER_CODE_COL, msrp_col=MASTER_MSRP_COL)

    text = read_text(sap2_path)
    head, body = iter_rows(text, header_rows=header_rows)
    sample = "".join(head[-1:]) or "".join(body[:1])
    dialect = detect_dialect(sample)

    cols = next(csv.reader(StringIO(head[-1]), dialect=dialect))
    colmap = {c.strip().lower(): i for i, c in enumerate(cols)}
    def gi(nm): return colmap.get(nm.lower())

    idx_doc  = gi("docnum")
    idx_item = gi("itemcode") or colmap.get("code")
    idx_gp   = gi("gpbefdisc")
    idx_price= gi("priceafvat") or colmap.get("price_after_vat")
    idx_total= gi("gtotal")
    idx_qty  = gi("quantity")
    idx_desc = gi("itemdescription") or gi("item description") or gi("description") or gi("dscription") or gi("itemname")

    need = [("DocNum", idx_doc), ("ItemCode", idx_item), ("GPBefDisc", idx_gp), ("PriceAfVAT", idx_price), ("GTotal", idx_total), ("Quantity", idx_qty)]
    miss = [n for n,i in need if i is None]
    if miss:
        raise RuntimeError(f"⚠️ หา column ไม่ครบใน sap2 หลัก: {miss}\ncolumns: {cols}")

    targets = set(docnums or [])
    updated, out_lines = 0, []

    for line in body:
        row = list(next(csv.reader(StringIO(line), dialect=dialect)))
        docv = (row[idx_doc] if idx_doc < len(row) else "").strip()
        if docv in targets:
            # ลบ "(แถม)" ถ้ามีคอลัมน์คำอธิบาย
            if idx_desc is not None and idx_desc < len(row):
                desc = row[idx_desc]
                if desc:
                    desc = re.sub(r"\s*\(แถม\)\s*", " ", str(desc))
                    desc = re.sub(r"\s{2,}", " ", desc).strip()
                    row[idx_desc] = desc

            code = (row[idx_item] if idx_item < len(row) else "").strip().upper()
            if code:
                msrp = msrp_map.get(code)
                if msrp is not None:
                    q = parse_float(row[idx_qty] if idx_qty < len(row) else "")
                    if q is None: q = 0.0
                    if idx_gp    < len(row): row[idx_gp]    = fmt2(msrp)
                    if idx_price < len(row): row[idx_price] = fmt2(msrp)
                    if idx_total < len(row): row[idx_total] = fmt2(msrp * q)
                    updated += 1

        # เขียนคืนทุกแถว
        sio = StringIO(); csv.writer(sio, dialect=dialect).writerow(row)
        out_lines.append(sio.getvalue())

    # เขียนผลลัพธ์
    new_text = "".join(head) + "".join(out_lines)
    out_path = sap2_path
    if overwrite:
        # สำรองไฟล์เก่า
        bak = sap2_path.with_suffix(sap2_path.suffix + ".bak")
        try:
            shutil.copyfile(sap2_path, bak)
        except Exception as e:
            print(f"⚠️ backup ไม่สำเร็จ: {e}")
        # เขียนทับ
        tmp = sap2_path.with_suffix(sap2_path.suffix + ".tmp")
        tmp.write_text(new_text, encoding="utf-8")
        os.replace(tmp, sap2_path)  # atomic replace
    else:
        # ไม่ overwrite: เขียนเป็นไฟล์ใหม่ใน minus_0
        out_dir = folder / "minus_0"
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / "sap2_main_updated.txt"
        out_path.write_text(new_text, encoding="utf-8")

    print(f"[done] update sap2 main -> updated {updated}/{len(body)} rows | out: {out_path}")
    return updated, out_path

# ---------- main ----------
def main():
    p = argparse.ArgumentParser(description="Split sap1 -> sap2 match -> apply MSRP; auto-download if missing; LINE notify")
    p.add_argument("--date", dest="one_date", type=iso_date, help="yyyy-mm-dd (from=to=ค่านี้)")
    p.add_argument("--from", dest="from_date", type=iso_date, help="yyyy-mm-dd เริ่มช่วง")
    p.add_argument("--to", dest="to_date", type=iso_date, help="yyyy-mm-dd สิ้นสุดช่วง")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="โฟลเดอร์รากเก็บงาน")
    p.add_argument("--folder", help="โฟลเดอร์ช่วงวันที่ที่มี sap1_*.txt / sap2_*.txt")
    p.add_argument("--master", default=MASTER_DATA_DEFAULT, help="ไฟล์ MasterData (.csv/.xlsx/.xls)")
    p.add_argument("--header-rows", type=int, default=DEFAULT_HEADER_ROWS)
    p.add_argument("--keyword", default=DEFAULT_KEYWORD)
    p.add_argument("--no-download", action="store_true", help="ปิด auto-download")
    # LINE options
    p.add_argument("--notify-test", action="store_true", help="ส่งข้อความทดสอบ LINE ทันที แล้วรันงานต่อ")
    p.add_argument("--notify-always", action="store_true", help="บังคับส่งสรุป LINE แม้ไม่พบบรรทัดคีย์เวิร์ด")
    p.add_argument("--notify-diag", action="store_true", help="รันโหมดวิเคราะห์ LINE (ตรวจ token/user/group และทดสอบ push)")

    args = p.parse_args()

    # โหมดวิเคราะห์ LINE แยกเดี่ยว
    if args.notify_diag:
        line_diag()
        return
    if args.notify_test:
        line_push("🔔 LINE test: script connected")

    # ตีความโฟลเดอร์
    folder = args.folder
    from_date = None; to_date = None
    if not folder:
        if args.one_date:
            from_date = to_date = args.one_date
        else:
            if not args.from_date or not args.to_date:
                p.print_help(); sys.exit("ต้องระบุ --folder หรือ (--date | --from --to)")
            from_date, to_date = args.from_date, args.to_date
        folder = os.path.join(args.out_dir, f"{from_date}_to_{to_date}")

    folder_p = Path(folder); ensure_dir(folder_p)

    # auto-download ถ้าไม่มีไฟล์
    def have_required_files(fp: Path) -> bool:
        return any(fp.glob("sap1*.txt")) and any(fp.glob("sap2*.txt"))

    if not have_required_files(folder_p):
        if args.no_download:
            sys.exit(f"โฟลเดอร์ยังไม่มี sap1/sap2 และปิด auto-download: {folder_p}")
        if not (from_date and to_date):
            sys.exit("ต้องมี --date หรือ --from/--to เพื่อ auto-download")
        print(f"[fetch] ไม่มีไฟล์ใน {folder_p} -> ดึงจาก API {from_date} → {to_date}")
        try:
            data = api_export(from_date, to_date)
            status = str(data.get("status","")).lower()
            files = data.get("files", {}) or {}
            if status != "success":
                print(json.dumps(data, indent=2, ensure_ascii=False))
                line_push(f"❌ ERROR: API status != success ({from_date}→{to_date})")
                sys.exit("API status != success")
            date_suffix = from_date if from_date == to_date else f"{from_date}_to_{to_date}"
            for key, link in files.items():
                if not link: continue
                dest = folder_p / sanitize(f"{key}_{date_suffix}.txt")
                print(f"[dl] {key} -> {dest}")
                try: download(link, str(dest))
                except Exception as e: print(f"[err] download {key}: {e}")
        except Exception as e:
            line_push(f"❌ ERROR: ดึงไฟล์ช่วง {from_date}→{to_date} ล้มเหลว\n{e}")
            sys.exit(f"ดึง API ล้มเหลว: {e}")

    # ตรวจ master
    master_p = Path(args.master) if args.master else None
    if not master_p or not master_p.exists():
        sys.exit("ไม่พบไฟล์ MasterData: %s" % master_p)

    # 1) sap1 split
    info = split_sap1_and_collect_docnums(folder_p, header_rows=args.header_rows, keyword=args.keyword)
    # 2) sap2 match
    p_match = build_sap2_match(folder_p, header_rows=args.header_rows, docnums=info["docnums"])
    # 3) apply msrp + clean "(แถม)"
    upd = apply_msrp_to_sap2_match(Path(p_match), master_p, header_rows=args.header_rows)
    # 4) apply msrp main
    upd_main, sap2_out = apply_msrp_to_sap2_main(folder_p, master_p, args.header_rows, info["docnums"], overwrite=True)

    # สรุป + แจ้ง LINE
    summary = []
    if from_date and to_date: summary.append(f"ช่วง: {from_date} → {to_date}")
    summary += [
        f"โฟลเดอร์: {folder_p.name}",
        f"พบ '{args.keyword}': {info['count_keyword']} ครั้ง",
        f"DocNum สำหรับ sap2: {len(info['docnums'])} รายการ",
        f"อัปเดต sap2_match_negative_docnums: {upd} แถว",
        "ไฟล์ผลลัพธ์:",
        " • sap1_only_negative.txt",
        " • sap2_match_negative_docnums.txt",
        " • sap2_without_negative_docnums.txt",
    ]
    summary.append(f"อัปเดต sap2 (ไฟล์หลัก): {upd_main} แถว -> {sap2_out.name}")

    text_summary = "🔔 สรุปงาน minus_0\n" + "\n".join(summary)
    print("\n" + text_summary + "\n")

    should_notify = (info['count_keyword'] > 0) or args.notify_always
    if should_notify:
        line_push(text_summary)

if __name__ == "__main__":
    main()
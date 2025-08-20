# -*- coding: utf-8 -*-
import argparse
import os
import sys
import json
import time
import re
import csv
from io import StringIO
from datetime import datetime, timedelta
from urllib.parse import urljoin
import requests
from dotenv import load_dotenv

# ---------- env & config ----------
load_dotenv()
BASE_URL = os.getenv("BASE_URL", "https://mizerp.com").rstrip("/")
EXPORT_PATH = os.getenv("EXPORT_PATH", "/wh/api/exportSap")
VERIFY_TLS = os.getenv("VERIFY_TLS", "true").lower() == "true"
TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", "60"))
AUTH_HEADER_NAME = os.getenv("AUTH_HEADER_NAME") or None
AUTH_HEADER_VALUE = os.getenv("AUTH_HEADER_VALUE") or None

# โฟลเดอร์ปลายทาง (แก้ได้ด้วย --out-dir)
FIXED_OUT_DIR = os.getenv("FIXED_OUT_DIR", r"C:\Users\kornkanok\Documents\Automation_api\Mizerp_api")

# ขั้นตอนประมวลผล
HEADER_ROWS = int(os.getenv("HEADER_ROWS", "2"))
KEYWORD = os.getenv("KEYWORD", "ส่วนลดติดลบ")
CASE_INSENSITIVE = os.getenv("CASE_INSENSITIVE", "true").lower() == "true"

# LINE
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_TO_ID = (os.getenv("LINE_TO_ID") or "").strip()

SESSION = requests.Session()

# ---------- helpers ----------
def iso_date(s: str) -> str:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be yyyy-mm-dd")

def default_dates():
    # ค่าเริ่มต้น = เมื่อวาน-เมื่อวาน (ตามเวลาท้องถิ่นเครื่องที่รัน)
    yday = datetime.now() - timedelta(days=1)
    d = yday.strftime("%Y-%m-%d")
    return d, d

def build_post_headers():
    h = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if AUTH_HEADER_NAME and AUTH_HEADER_VALUE:
        h[AUTH_HEADER_NAME] = AUTH_HEADER_VALUE
    return h

def build_get_headers():
    h = {}
    if AUTH_HEADER_NAME and AUTH_HEADER_VALUE:
        h[AUTH_HEADER_NAME] = AUTH_HEADER_VALUE
    return h

def normalize_url(u: str) -> str:
    u = u.replace("///", "/")
    if u.startswith(("http://", "https://")):
        return u
    return urljoin(BASE_URL + "/", u.lstrip("/"))

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def sanitize_filename(name: str) -> str:
    for ch in '<>:"/\\|?*':
        name = name.replace(ch, "_")
    return name

def backoff_sleep(attempt: int):
    time.sleep(min(30, 2 ** attempt))  # 1,2,4,8,16,30

def _dump_response(resp):
    os.makedirs("debug", exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    path = os.path.join("debug", f"exportSap_resp_{ts}.bin")
    with open(path, "wb") as f:
        f.write(resp.content or b"")
    return path

def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        raw = resp.content or b""
        if raw.startswith(b"\xef\xbb\xbf"):
            try:
                return json.loads(raw.lstrip(b"\xef\xbb\xbf").decode("utf-8"))
            except Exception:
                pass
        dump_path = _dump_response(resp)
        ct = (resp.headers.get("Content-Type") or "").lower()
        raise RuntimeError(
            f"API did not return JSON. status={resp.status_code}, "
            f"content-type={ct}, len={len(raw)}. Raw saved to: {dump_path}"
        )

def post_export(from_date: str, to_date: str) -> dict:
    url = urljoin(BASE_URL + "/", EXPORT_PATH.lstrip("/"))
    payload = {"fromDate": from_date, "toDate": to_date}
    headers = build_post_headers()

    for attempt in range(6):
        try:
            resp = SESSION.post(
                url, headers=headers, data=payload,
                timeout=TIMEOUT, verify=VERIFY_TLS, allow_redirects=True
            )
            print(f"[debug] POST {resp.status_code} ct={resp.headers.get('Content-Type')} len={len(resp.content)}", flush=True)

            if resp.status_code in (429, 502, 503, 504):
                backoff_sleep(attempt); continue

            if not (200 <= resp.status_code < 300):
                dump_path = _dump_response(resp)
                resp.raise_for_status()
                raise RuntimeError(f"HTTP {resp.status_code}; raw saved to {dump_path}")

            return safe_json(resp)

        except requests.RequestException as e:
            if attempt == 5:
                raise
            print(f"[warn] request failed: {e}; retry...", flush=True)
            backoff_sleep(attempt)

def download_file(file_url: str, dest_path: str):
    url = normalize_url(file_url)
    headers = build_get_headers()
    with SESSION.get(url, stream=True, timeout=TIMEOUT,
                     verify=VERIFY_TLS, headers=headers, allow_redirects=True) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

# ---------- date extraction from content / url ----------
DATE_PATTERNS = [
    # yyyy-mm-dd
    (re.compile(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b"), "%Y-%m-%d"),
    # dd/mm/yyyy
    (re.compile(r"\b(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/(20\d{2})\b"), "%d/%m/%Y"),
    # yyyymmdd
    (re.compile(r"\b(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\b"), "%Y%m%d"),
]

def normalize_date_str(s: str, fmt: str) -> str:
    try:
        dt = datetime.strptime(s, fmt)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""

def extract_date_from_text(text: str) -> str:
    for rx, fmt in DATE_PATTERNS:
        m = rx.search(text)
        if not m:
            continue
        token = m.group(0)
        norm = normalize_date_str(token, fmt)
        if norm:
            return norm
    return ""

def extract_date_from_url(url: str) -> str:
    base = os.path.basename(url)
    return extract_date_from_text(base)

def detect_file_date(path: str, url: str, max_bytes: int = 256 * 1024) -> str:
    d = extract_date_from_url(url)
    if d:
        return d
    try:
        with open(path, "rb") as f:
            blob = f.read(max_bytes)
        try:
            text = blob.decode("utf-8", errors="ignore")
        except Exception:
            text = blob.decode("latin-1", errors="ignore")
        d = extract_date_from_text(text)
        return d
    except Exception:
        return ""

# ---------- CSV helpers for processing ----------
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

def find_docnum_index(header_line: str, dialect):
    reader = csv.reader(StringIO(header_line), dialect=dialect)
    cols = next(reader)
    lowered = [c.strip().lower() for c in cols]
    for i, name in enumerate(lowered):
        if name == "docnum":
            return i
    return None

def get_field(line: str, idx, dialect):
    reader = csv.reader(StringIO(line), dialect=dialect)
    try:
        row = next(reader)
        if idx is None or idx >= len(row):
            return None
        return row[idx]
    except Exception:
        return None

# ---------- LINE ----------
def line_push(text: str) -> bool:
    if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_TO_ID:
        print("⚠️ LINE config ไม่ครบ (LINE_CHANNEL_ACCESS_TOKEN/LINE_TO_ID) : ข้ามการส่งแจ้งเตือน")
        return False
    try:
        resp = requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                     "Content-Type": "application/json"},
            json={"to": LINE_TO_ID, "messages": [{"type": "text", "text": text}]},
            timeout=20,
        )
        if resp.status_code == 200:
            return True
        print(f"⚠️ ส่ง LINE ไม่สำเร็จ: {resp.status_code} {resp.text}")
        return False
    except Exception as e:
        print(f"❌ ส่ง LINE error: {e}")
        return False

# ---------- processing ----------
def process_negative_and_match(folder: str):
    """
    - มองหา sap1*.txt (+ sap1.txt) และ sap2*.txt (+ sap2.txt) ในโฟลเดอร์ที่ดาวน์โหลด
    - แยกบรรทัดที่พบ KEYWORD ใน sap1 -> sap1_only_negative.txt / sap1_without_negative.txt (ในโฟลเดอร์ย่อย minus_0)
    - จับคู่ DocNum ที่พบใน sap1 กับ sap2 -> sap2_match_negative_docnums.txt / sap2_without_negative_docnums.txt
    - ส่งสรุป LINE ถ้าพบอย่างน้อย 1 รายการ
    """
    folder_p = os.path.abspath(folder)
    sap1_candidates = []
    sap2_candidates = []
    for pat in ("sap1*.txt", "sap1.txt"):
        sap1_candidates += [os.path.join(folder_p, x) for x in os.listdir(folder_p) if re.fullmatch(pat.replace("*", ".*"), x)]
    for pat in ("sap2*.txt", "sap2.txt"):
        sap2_candidates += [os.path.join(folder_p, x) for x in os.listdir(folder_p) if re.fullmatch(pat.replace("*", ".*"), x)]

    if not sap1_candidates or not sap2_candidates:
        print("⚠️ ไม่พบไฟล์ sap1 หรือ sap2 สำหรับประมวลผล")
        return {"keyword_count": 0, "docnum_count": 0, "notes": "missing sap1/sap2"}

    sap1_path = sorted(sap1_candidates)[0]
    sap2_path = sorted(sap2_candidates)[0]

    # อ่าน sap1
    with open(sap1_path, "r", encoding="utf-8-sig") as f:
        src_text = f.read()
    src_header, src_body = iter_rows(src_text, header_rows=HEADER_ROWS)

    sample_for_sniff = "".join(src_header[-1:]) or "".join(src_body[:1])
    src_dialect = detect_dialect(sample_for_sniff)
    docnum_idx_src = find_docnum_index(src_header[-1], src_dialect)

    lines_with_keyword = []
    lines_without_keyword = []
    docnums_found = {}
    count_keyword = 0

    for line in src_body:
        target_line = line.lower() if CASE_INSENSITIVE else line
        target_kw = KEYWORD.lower() if CASE_INSENSITIVE else KEYWORD
        occ = target_line.count(target_kw)
        if occ > 0:
            lines_with_keyword.append(line)
            count_keyword += occ
            dv = get_field(line, docnum_idx_src, src_dialect)
            dv = (dv or "").strip()
            if dv:
                docnums_found[dv] = True
        else:
            lines_without_keyword.append(line)

    minus_dir = os.path.join(folder_p, "minus_0")
    ensure_dir(minus_dir)
    out_with_keyword    = os.path.join(minus_dir, "sap1_only_negative.txt")
    out_without_keyword = os.path.join(minus_dir, "sap1_without_negative.txt")

    with open(out_with_keyword, "w", encoding="utf-8") as f:
        f.write("".join(src_header) + "".join(lines_with_keyword))
    with open(out_without_keyword, "w", encoding="utf-8") as f:
        f.write("".join(src_header) + "".join(lines_without_keyword))

    # อ่าน sap2 + match
    with open(sap2_path, "r", encoding="utf-8-sig") as f:
        lookup_text = f.read()
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

    out_lookup_matches = os.path.join(minus_dir, "sap2_match_negative_docnums.txt")
    out_lookup_without = os.path.join(minus_dir, "sap2_without_negative_docnums.txt")
    with open(out_lookup_matches, "w", encoding="utf-8") as f:
        f.write("".join(lookup_header) + "".join(lookup_matches))
    with open(out_lookup_without, "w", encoding="utf-8") as f:
        f.write("".join(lookup_header) + "".join(lookup_without_matches))

    # สรุป/แจ้ง LINE ถ้าพบ
    summary = (
        f"สรุปประมวลผล\n"
        f"- โฟลเดอร์: {os.path.basename(folder_p)}\n"
        f"- พบ \"{KEYWORD}\": {count_keyword} ครั้ง\n"
        f"- DocNum เกี่ยวข้อง: {len(docnums_found)} รายการ\n"
        f"- ผลลัพธ์: sap1_only_negative.txt, sap2_match_negative_docnums.txt"
    )
    print(summary)
    if count_keyword > 0:
        line_push(
            f"แจ้งเตือน: พบ \"{KEYWORD}\" {count_keyword} ครั้ง\n"
            f"DocNum: {len(docnums_found)}\n"
            f"โฟลเดอร์: {os.path.basename(folder_p)}"
        )

    return {"keyword_count": count_keyword, "docnum_count": len(docnums_found), "notes": ""}

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(
        description="Export via API -> Download -> Check 'ส่วนลดติดลบ' -> (LINE)"
    )
    fd, td = default_dates()
    parser.add_argument("--date", dest="one_date", type=iso_date,
                        help="yyyy-mm-dd (วันเดียว: from=to=ค่าวันนี้)")
    parser.add_argument("--from", dest="from_date", type=iso_date, default=None,
                        help="yyyy-mm-dd (เริ่มช่วง)")
    parser.add_argument("--to", dest="to_date", type=iso_date, default=None,
                        help="yyyy-mm-dd (สิ้นสุดช่วง)")
    parser.add_argument("--out-dir", dest="out_dir", default=FIXED_OUT_DIR,
                        help="โฟลเดอร์ปลายทางหลัก (default มาจาก FIXED_OUT_DIR)")
    parser.add_argument("--use-file-date", action="store_true",
                        help="ถ้าระบุ จะพยายามรีเนมไฟล์ตามวันที่ที่พบในไฟล์/URL")
    parser.add_argument("--skip-process", action="store_true",
                        help="ไม่ต้องทำขั้นตอนตรวจ 'ส่วนลดติดลบ'/จับคู่ DocNum/แจ้ง LINE")

    args = parser.parse_args()

    # เลือกช่วงเวลา
    if args.one_date:
        from_date = to_date = args.one_date
    else:
        from_date = args.from_date or fd
        to_date   = args.to_date or td

    # suffix วันที่ (สำหรับ fallback หรือ --no-rename-from-file-date)
    if from_date == to_date:
        date_suffix = from_date
    else:
        date_suffix = f"{from_date}_to_{to_date}"

    print(f"[info] Requesting export from {from_date} to {to_date} ...", flush=True)
    data = post_export(from_date, to_date)

    status = str(data.get("status", "")).lower()
    message = data.get("message", "")
    files = data.get("files", {}) or {}

    print(f"[info] status={status} message={message}", flush=True)
    if status != "success":
        print("[error] API did not return success status.", flush=True)
        print(json.dumps(data, indent=2, ensure_ascii=False), flush=True)
        sys.exit(1)

    # โฟลเดอร์ปลายทาง: แยก subfolder ตามช่วงวันที่ที่สั่ง
    folder_name = f"{from_date}_to_{to_date}"
    out_root = args.out_dir
    out_dir = os.path.join(out_root, folder_name)
    ensure_dir(out_dir)

    # ดาวน์โหลดไฟล์ แล้ว (ถ้าไม่ปิด) จะพยายามเปลี่ยนชื่อให้เป็นวันที่ที่อยู่ในไฟล์/URL
    downloaded = []
    for key, link in files.items():
        if not link:
            print(f"[warn] missing link for key={key}, skip", flush=True)
            continue

        fname = sanitize_filename(f"{key}_{date_suffix}.txt")
        dest  = os.path.join(out_dir, fname)

        print(f"[info] downloading {key} -> {dest}", flush=True)
        try:
            download_file(link, dest)
        except requests.RequestException as e:
            print(f"[error] download failed for {key}: {e}", flush=True)
            continue

        if args.use_file_date:
            detected = detect_file_date(dest, link)
            if detected:
                new_name = sanitize_filename(f"{key}_{detected}.txt")
                new_path = os.path.join(out_dir, new_name)
                if os.path.abspath(new_path) != os.path.abspath(dest):
                    if os.path.exists(new_path):
                        base, ext = os.path.splitext(new_name)
                        i = 2
                        while True:
                            cand = os.path.join(out_dir, f"{base}({i}){ext}")
                            if not os.path.exists(cand):
                                new_path = cand
                                break
                            i += 1
                    os.replace(dest, new_path)
                    print(f"[info] renamed -> {new_path}", flush=True)
                    dest = new_path

        downloaded.append(dest)

    print("[info] Download Done.", flush=True)
    if downloaded:
        print("[info] saved files:", flush=True)
        for p in downloaded:
            print(" -", p, flush=True)
    else:
        print("[warn] no files downloaded.", flush=True)

    # ขั้นตอนประมวลผล + LINE
    if not args.skip_process:
        result = process_negative_and_match(out_dir)
        print(f"[info] Process result: found={result['keyword_count']} docnums={result['docnum_count']} notes={result['notes']}")
    else:
        print("[info] skip_process enabled; ไม่ทำขั้นตอนตรวจ/แจ้ง LINE")

if __name__ == "__main__":
    main()

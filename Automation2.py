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
import paramiko

# ---------- env & config ----------
load_dotenv()

BASE_URL = os.getenv("BASE_URL", "https://mizerp.com").rstrip("/")
EXPORT_PATH = os.getenv("EXPORT_PATH", "/wh/api/exportSap")
VERIFY_TLS = os.getenv("VERIFY_TLS", "true").lower() == "true"
TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", "60"))
AUTH_HEADER_NAME = os.getenv("AUTH_HEADER_NAME") or None
AUTH_HEADER_VALUE = os.getenv("AUTH_HEADER_VALUE") or None

FIXED_OUT_DIR = os.getenv("FIXED_OUT_DIR", r"C:\Users\kornkanok\Documents\Automation_api\Mizerp_api")

HEADER_ROWS = int(os.getenv("HEADER_ROWS", "2"))
KEYWORD = os.getenv("KEYWORD", "ส่วนลดติดลบ")
CASE_INSENSITIVE = os.getenv("CASE_INSENSITIVE", "true").lower() == "true"

LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
LINE_TO_ID = (os.getenv("LINE_TO_ID") or "").strip()

ENABLE_SFTP = (os.getenv("ENABLE_SFTP", "true").lower() == "true")
SFTP_HOST = os.getenv("SFTP_HOST") or ""
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER") or ""
SFTP_PASS = os.getenv("SFTP_PASS") or ""
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH") or ""
SFTP_KEY_PASS = os.getenv("SFTP_KEY_PASS") or ""
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR") or ""
SFTP_SUBFOLDER_BY_DATE = (os.getenv("SFTP_SUBFOLDER_BY_DATE", "true").lower() == "true")

SESSION = requests.Session()

# ---------- helpers ----------
def iso_date(s: str) -> str:
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be yyyy-mm-dd")

def default_dates():
    yday = datetime.now() - timedelta(days=1)
    d = yday.strftime("%Y-%m-%d")
    return d, d

def build_post_headers():
    h = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    if AUTH_HEADER_NAME and AUTH_HEADER_VALUE:
        h[AUTH_HEADER_NAME] = AUTH_HEADER_VALUE
    return h

def build_get_headers():
    h = {}
    if AUTH_HEADER_NAME and AUTH_HEADER_VALUE:
        h[AUTH_HEADER_NAME] = AUTH_HEADER_VALUE
    return h

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

def normalize_url(u: str) -> str:
    u = u.replace("///", "/")
    if u.startswith(("http://", "https://")):
        return u
    return urljoin(BASE_URL + "/", u.lstrip("/"))

def post_export(from_date: str, to_date: str) -> dict:
    url = urljoin(BASE_URL + "/", EXPORT_PATH.lstrip("/"))
    payload = {"fromDate": from_date, "toDate": to_date}
    headers = build_post_headers()

    for attempt in range(6):
        try:
            resp = SESSION.post(url, headers=headers, data=payload,
                                timeout=TIMEOUT, verify=VERIFY_TLS, allow_redirects=True)
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
    (re.compile(r"\b(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\b"), "%Y-%m-%d"),  # yyyy-mm-dd
    (re.compile(r"\b(0[1-9]|[12]\d|3[01])/(0[1-9]|1[0-2])/(20\d{2})\b"), "%d/%m/%Y"),  # dd/mm/yyyy
    (re.compile(r"\b(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\b"), "%Y%m%d"),      # yyyymmdd
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

# ---------- CSV helpers ----------
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
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()

def _split_ids(val: str):
    return [x.strip() for x in (val or "").split(",") if x and x.strip()]

LINE_TO_ID     = (os.getenv("LINE_TO_ID") or "").strip()  # backwards-compat
LINE_USER_IDS  = _split_ids(os.getenv("LINE_USER_IDS") or "")
LINE_GROUP_IDS = _split_ids(os.getenv("LINE_GROUP_IDS") or "")

def line_notify(text: str) -> bool:
    """ส่งข้อความไปได้ทั้ง: LINE_TO_ID (เดี่ยว), หลาย userIds (multicast), หลาย groupIds (push ทีละกลุ่ม)"""
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("⚠️ ไม่มี LINE_CHANNEL_ACCESS_TOKEN"); return False

    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
               "Content-Type": "application/json"}
    messages = [{"type": "text", "text": text}]
    ok = True

    # 0) ส่งตาม LINE_TO_ID เดิม (ถ้ากำหนดไว้)
    if LINE_TO_ID:
        r = requests.post("https://api.line.me/v2/bot/message/push",
                          headers=headers, json={"to": LINE_TO_ID, "messages": messages}, timeout=20)
        ok &= (r.status_code == 200)
        if not ok: print("push LINE_TO_ID:", r.status_code, r.text)

    # 1) ส่งเข้ากลุ่มทั้งหมด (ต้องใช้ push ต่อกลุ่ม)
    for gid in LINE_GROUP_IDS:
        r = requests.post("https://api.line.me/v2/bot/message/push",
                          headers=headers, json={"to": gid, "messages": messages}, timeout=20)
        ok &= (r.status_code == 200)
        if r.status_code != 200:
            print(f"push group {gid}:", r.status_code, r.text)

    # 2) ส่งเข้าผู้ใช้ทั้งหมด
    if len(LINE_USER_IDS) == 1:
        uid = LINE_USER_IDS[0]
        r = requests.post("https://api.line.me/v2/bot/message/push",
                          headers=headers, json={"to": uid, "messages": messages}, timeout=20)
        ok &= (r.status_code == 200)
        if r.status_code != 200:
            print(f"push user {uid}:", r.status_code, r.text)

    elif len(LINE_USER_IDS) > 1:
        # multicast ส่งได้เฉพาะ "ผู้ใช้" ไม่รองรับ group/room
        # สูงสุด 500 user ต่อครั้ง (ถ้ามากกว่านั้นให้ chunk เป็นก้อน ๆ)
        users = LINE_USER_IDS
        CHUNK = 500
        for i in range(0, len(users), CHUNK):
            batch = users[i:i+CHUNK]
            r = requests.post("https://api.line.me/v2/bot/message/multicast",
                              headers=headers, json={"to": batch, "messages": messages}, timeout=30)
            ok &= (r.status_code == 200)
            if r.status_code != 200:
                print(f"multicast users {i}-{i+len(batch)-1}:", r.status_code, r.text)

    return ok

# ---------- SFTP ----------
def _sftp_connect():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = None
    if SFTP_KEY_PATH:
        try:
            try:
                pkey = paramiko.Ed25519Key.from_private_key_file(SFTP_KEY_PATH, password=(SFTP_KEY_PASS or None))
            except Exception:
                pkey = paramiko.RSAKey.from_private_key_file(SFTP_KEY_PATH, password=(SFTP_KEY_PASS or None))
        except Exception as e:
            raise RuntimeError(f"โหลดกุญแจ SFTP ไม่สำเร็จ: {e}")
    client.connect(
        hostname=SFTP_HOST, port=SFTP_PORT, username=SFTP_USER,
        password=SFTP_PASS if not pkey else None, pkey=pkey, timeout=30
    )
    return client

def _sftp_ensure_dir(sftp, remote_dir: str):
    parts = [p for p in remote_dir.strip("/").split("/") if p]
    cur = ""
    for seg in parts:
        cur = cur + "/" + seg
        try:
            sftp.stat(cur)
        except IOError:
            sftp.mkdir(cur)

def sftp_upload_files(local_paths, base_remote_dir: str, subdir: str = None):
    if not ENABLE_SFTP:
        print("SFTP disabled; ข้ามการอัปโหลด"); return
    if not SFTP_HOST or not SFTP_USER or not base_remote_dir:
        print("⚠️ SFTP config ไม่ครบ (HOST/USER/REMOTE_DIR)"); return
    client = None
    try:
        client = _sftp_connect()
        sftp = client.open_sftp()
        remote_dir = base_remote_dir
        if subdir:
            remote_dir = base_remote_dir.rstrip("/") + "/" + subdir
        _sftp_ensure_dir(sftp, remote_dir)

        for p in local_paths:
            if not p:
                continue
            if not os.path.exists(p):
                print(f"⚠️ ไฟล์ไม่พบ: {p} -> ข้าม"); continue
            remote_path = remote_dir.rstrip("/") + "/" + os.path.basename(p)
            print(f"put {p} -> {remote_path}")
            sftp.put(p, remote_path)

        sftp.close()
        client.close()
        print("อัปโหลด SFTP เสร็จ")
    except Exception as e:
        if client:
            try: client.close()
            except: pass
        print(f"❌ SFTP error: {e}")

# ---------- processing ----------
def process_negative_and_match(folder: str):
    folder_p = os.path.abspath(folder)
    names = os.listdir(folder_p)

    def pick_first(patterns):
        for pat in patterns:
            rx = re.compile("^" + pat.replace(".", r"\.").replace("*", ".*") + "$", re.IGNORECASE)
            for x in sorted(names):
                if rx.fullmatch(x):
                    return os.path.join(folder_p, x)
        return None

    sap1_path = pick_first(("sap1*.txt", "sap1.txt"))
    sap2_path = pick_first(("sap2*.txt", "sap2.txt"))

    if not sap1_path or not sap2_path:
        print("⚠️ ไม่พบไฟล์ sap1 หรือ sap2 สำหรับประมวลผล")
        return {"keyword_count": 0, "docnum_count": 0, "path_without_negative": None, "notes": "missing sap1/sap2"}

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
    out_without_negative = os.path.join(minus_dir, "sap1_without_negative.txt")

    with open(out_with_keyword, "w", encoding="utf-8") as f:
        f.write("".join(src_header) + "".join(lines_with_keyword))
    with open(out_without_negative, "w", encoding="utf-8") as f:
        f.write("".join(src_header) + "".join(lines_without_keyword))

    # sap2 matching
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

    # สรุป / LINE
    summary = (f"สรุปประมวลผล\n"
               f"- โฟลเดอร์: {os.path.basename(folder_p)}\n"
               f"- พบ \"{KEYWORD}\": {count_keyword} ครั้ง\n"
               f"- DocNum เกี่ยวข้อง: {len(docnums_found)} รายการ\n"
               f"- ผลลัพธ์: sap1_only_negative.txt, sap2_match_negative_docnums.txt")
    print(summary)
    if count_keyword > 0:
        line_notify(f"แจ้งเตือน: พบ \"{KEYWORD}\" {count_keyword} ครั้ง\n"
                  f"DocNum: {len(docnums_found)}\n"
                  f"โฟลเดอร์: {os.path.basename(folder_p)}")

    return {
        "keyword_count": count_keyword,
        "docnum_count": len(docnums_found),
        "path_sap1_without_negative": out_without_negative,     # << เดิม (sap1 ที่ตัดออกแล้ว)
        "path_sap2_without_negative": out_lookup_without,       # << เพิ่ม (sap2 ที่ตัด DocNum ติดลบออกแล้ว)
        "notes": ""
    }

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Export via API -> Download -> Check 'ส่วนลดติดลบ' -> Upload (sap1_without_negative, sap3, dl0) -> LINE")
    fd, td = default_dates()
    parser.add_argument("--date", dest="one_date", type=iso_date, help="yyyy-mm-dd (from=to=ค่านี้)")
    parser.add_argument("--from", dest="from_date", type=iso_date, default=None, help="yyyy-mm-dd (เริ่มช่วง)")
    parser.add_argument("--to", dest="to_date", type=iso_date, default=None, help="yyyy-mm-dd (สิ้นสุดช่วง)")
    parser.add_argument("--out-dir", dest="out_dir", default=FIXED_OUT_DIR, help="โฟลเดอร์ปลายทางหลัก")
    parser.add_argument("--use-file-date", action="store_true", help="รีเนมไฟล์ตามวันที่ที่พบในไฟล์/URL")
    parser.add_argument("--skip-process", action="store_true", help="ดึงไฟล์อย่างเดียว (ไม่ประมวลผล/ไม่อัปโหลด/ไม่ส่ง LINE)")
    args = parser.parse_args()

    # เลือกช่วงเวลา
    if args.one_date:
        from_date = to_date = args.one_date
    else:
        from_date = args.from_date or fd
        to_date   = args.to_date or td

    date_suffix = from_date if from_date == to_date else f"{from_date}_to_{to_date}"
    print(f"[info] Requesting export {from_date} → {to_date}", flush=True)
    data = post_export(from_date, to_date)

    status = str(data.get("status", "")).lower()
    message = data.get("message", "")
    files = data.get("files", {}) or {}

    print(f"[info] status={status} message={message}", flush=True)
    if status != "success":
        print("[error] API did not return success status.", flush=True)
        print(json.dumps(data, indent=2, ensure_ascii=False), flush=True)
        sys.exit(1)

    # โฟลเดอร์ปลายทางตามช่วงวันที่
    out_root = args.out_dir
    folder_name = f"{from_date}_to_{to_date}"
    out_dir = os.path.join(out_root, folder_name)
    ensure_dir(out_dir)

    # ดาวน์โหลด & เก็บ mapping key->path
    saved_paths = {}  # key (lower) -> path
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
                                new_path = cand; break
                            i += 1
                    os.replace(dest, new_path)
                    print(f"[info] renamed -> {new_path}", flush=True)
                    dest = new_path

        saved_paths[str(key).lower()] = dest

    if saved_paths:
        print("[info] saved files:")
        for k, p in saved_paths.items():
            print(f" - {k}: {p}")
    else:
        print("[warn] no files downloaded.")
        if not args.skip_process:
            print("[warn] ไม่มีไฟล์ให้ประมวลผล"); return

    # ประมวลผล & เตรียมอัปโหลด
    # ขั้นตอนประมวลผล & เตรียมอัปโหลด
    if not args.skip_process:
        result = process_negative_and_match(out_dir)
        print(f"[info] Process result: found={result['keyword_count']} docnums={result['docnum_count']}")

         #เลือกไฟล์ sap3 และ dl0 จาก saved_paths
        def pick_path(d, prefix):
            prefix = prefix.lower()
            for k, v in d.items():
                if k.startswith(prefix):
                    return v
            return None

        path_sap1_wo = result.get("path_sap1_without_negative")
        path_sap2_wo = result.get("path_sap2_without_negative")   # << เพิ่มบรรทัดนี้
        sap3_path    = pick_path(saved_paths, "sap3")
        dl0_path     = pick_path(saved_paths, "dl0")

        # อัปโหลด 4 ไฟล์: sap1_wo, sap2_wo, sap3, dl0
        files_to_upload = [path_sap1_wo, path_sap2_wo, sap3_path, dl0_path]
        iles_to_upload = [p for p in files_to_upload if p]

        subdir = f"{from_date}_to_{to_date}" if SFTP_SUBFOLDER_BY_DATE else None
        sftp_upload_files(files_to_upload, SFTP_REMOTE_DIR, subdir=subdir)
    else:
        print("[info] skip_process enabled; ไม่ประมวลผล/ไม่อัปโหลด/ไม่ส่ง LINE")


if __name__ == "__main__":
    main()

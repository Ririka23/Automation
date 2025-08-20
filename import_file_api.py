import argparse
import os
import sys
import json
import time
import re
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
    # ลองจับทุกแพทเทิร์นตามลำดับ
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
    # เผื่อไฟล์ใน URL มีวันที่ เช่น .../INV_2025-08-10.txt หรือ 20250810
    base = os.path.basename(url)
    return extract_date_from_text(base)

def detect_file_date(path: str, url: str, max_bytes: int = 256 * 1024) -> str:
    # ลองจาก URL ก่อน
    d = extract_date_from_url(url)
    if d:
        return d

    # แล้วค่อยอ่านจากเนื้อไฟล์ (อ่านแค่ต้นไฟล์เพื่อความเร็ว)
    try:
        with open(path, "rb") as f:
            blob = f.read(max_bytes)
        text = ""
        try:
            text = blob.decode("utf-8", errors="ignore")
        except Exception:
            text = blob.decode("latin-1", errors="ignore")
        d = extract_date_from_text(text)
        return d
    except Exception:
        return ""

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(
        description="Fetch and download exportSap .txt files from mizerp.com"
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
    parser.add_argument(
        "--use-file-date",
        action="store_true",
        help="ถ้าระบุ จะพยายามรีเนมไฟล์ตามวันที่ที่พบในไฟล์/URL"
    )

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

        # ตั้งชื่อจากช่วงวันที่ที่สั่ง (ค่ามาตรฐานให้เหมือนกันทุกไฟล์)
        fname = sanitize_filename(f"{key}_{date_suffix}.txt")
        dest  = os.path.join(out_dir, fname)

        print(f"[info] downloading {key} -> {dest}", flush=True)
        try:
            download_file(link, dest)
        except requests.RequestException as e:
            print(f"[error] download failed for {key}: {e}", flush=True)
            continue

        # ถ้าต้องการ “อนุญาต” ให้รีเนมตามวันที่ในไฟล์/URL ค่อยทำเมื่อใส่ --use-file-date เท่านั้น
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


    print("[info] Done.", flush=True)
    if downloaded:
        print("[info] saved files:", flush=True)
        for p in downloaded:
            print(" -", p, flush=True)
    else:
        print("[warn] no files downloaded.", flush=True)

if __name__ == "__main__":
    main()

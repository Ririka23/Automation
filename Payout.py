import os, re, time
from pathlib import Path
from typing import List

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ========================== CONFIG ==========================
SENDERS = [
    "priyapornp@mizuhada.com",    # <- ผู้ส่งคนที่ 1
    "nathapats@mizuhada.com",    # <- ผู้ส่งคนที่ 2 
]
SUBJECT_EQUALS = "Report Payout"  # <- หัวข้อเมล

DAYS_LOOKBACK = 30  # กี่วันย้อนหลัง

LOCAL_SAVE_DIR   = Path("/Users/pianoxyz/Documents/Automation-main/PayOut")
TXT_FILENAME_FMT = "sheet_{idx:02d}_{ts}.txt"
# ===========================================================

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

LOCAL_SAVE_DIR.mkdir(parents=True, exist_ok=True)

def build_gmail_query() -> str:
    from_clause = " OR ".join([f"from:{s}" for s in SENDERS])
    q = f"({from_clause}) subject:\"{SUBJECT_EQUALS}\" has:drive newer_than:{DAYS_LOOKBACK}d"
    return q

def get_creds():
    creds = None
    token = Path("token.json")
    if token.exists():
        creds = Credentials.from_authorized_user_file(str(token), SCOPES)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file("/Users/pianoxyz/Documents/Automation-main/PayOut/credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
        token.write_text(creds.to_json(), encoding="utf-8")
    return creds

def gmail_search_messages(service, query: str, max_results: int = 50) -> List[str]:
    msg_ids = []
    resp = service.users().messages().list(
        userId="me", q=query, maxResults=max_results
    ).execute()
    if "messages" in resp:
        msg_ids.extend(m["id"] for m in resp["messages"])
    return msg_ids

def gmail_get_body_text(service, msg_id: str) -> str:
    msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
    payload = msg.get("payload", {})
    parts = payload.get("parts", [])
    segments = []

    def add_part(part):
        body = part.get("body", {})
        data = body.get("data")
        if data:
            import base64
            decoded = base64.urlsafe_b64decode(data.encode()).decode("utf-8", errors="ignore")
            segments.append(decoded)
        for p in part.get("parts", []) or []:
            add_part(p)

    if parts:
        for p in parts:
            add_part(p)
    else:
        body = payload.get("body", {})
        if body.get("data"):
            import base64
            decoded = base64.urlsafe_b64decode(body["data"].encode()).decode("utf-8", errors="ignore")
            segments.append(decoded)

    return "\n".join(segments)

def extract_gsheet_ids(text: str) -> List[str]:
    pat = r'https?://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]+)'
    ids = re.findall(pat, text)
    seen, out = set(), []
    for fid in ids:
        if fid not in seen:
            out.append(fid); seen.add(fid)
    return out

def drive_export_csv_bytes(drive, file_id: str) -> bytes:
    return drive.files().export(fileId=file_id, mimeType="text/csv").execute()

def save_txt(content: str, filename: str) -> Path:
    p = LOCAL_SAVE_DIR / filename
    p.write_text(content, encoding="utf-8")
    return p

def main():
    creds = get_creds()
    gmail = build("gmail", "v1", credentials=creds)
    drive = build("drive", "v3", credentials=creds)

    query = build_gmail_query()
    print("Gmail query:", query)
    msg_ids = gmail_search_messages(gmail, query, max_results=20)
    if not msg_ids:
        print("ไม่พบอีเมลที่ตรงเงื่อนไข")
        return

    ts = time.strftime("%Y%m%d_%H%M%S")
    idx = 0

    for mid in msg_ids:
        body = gmail_get_body_text(gmail, mid)
        file_ids = extract_gsheet_ids(body)
        for fid in file_ids:
            try:
                csv_bytes = drive_export_csv_bytes(drive, fid)
                text = csv_bytes.decode("utf-8", errors="ignore")
                idx += 1
                fname = TXT_FILENAME_FMT.format(idx=idx, ts=ts)
                out = save_txt(text, fname)
                print("บันทึก:", out)
            except HttpError as e:
                print(f"[Drive] export ล้มเหลว file_id={fid}: {e}")
            except Exception as ex:
                print(f"[ERROR] file_id={fid}: {ex}")

if __name__ == "__main__":
    main()

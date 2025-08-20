import os, sys, time, threading, queue, posixpath, fnmatch
from pathlib import Path

# ---- .env optional ----
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import paramiko
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# ================== CONFIG (มีค่า default ใช้งานได้ทันที) ==================

LOCAL_WATCH_DIR = os.getenv("LOCAL_WATCH_DIR", r"C:\Users\kornkanok\Documents\Mizerp_api")
WATCH_PATTERNS = [p.strip() for p in os.getenv("WATCH_PATTERNS", "*.txt").split(",") if p.strip()]
PRESERVE_SUBDIR = (os.getenv("PRESERVE_SUBDIR", "true").lower() == "true")

# ------------- SFTP (Windows OpenSSH ใช้ POSIX path) -------------
SFTP_HOST = os.getenv("SFTP_HOST")
SFTP_PORT = int(os.getenv("SFTP_PORT", "22"))
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PASS = os.getenv("SFTP_PASS")
SFTP_KEY_PATH = os.getenv("SFTP_KEY_PATH")
SFTP_KEY_PASSPHRASE = os.getenv("SFTP_KEY_PASSPHRASE")
SFTP_REMOTE_DIR = os.getenv(
    "SFTP_REMOTE_DIR",
    "/C:/Users/Administrator.WIN-LRO1J4IE0N1/Desktop/AR/2025/Test_api"
)

STABLE_CHECKS = int(os.getenv("STABLE_CHECKS", "3"))
STABLE_INTERVAL = float(os.getenv("STABLE_INTERVAL", "1.0"))

# ============================================================================

def log(*a): print(*a, flush=True)
def to_posix(p: str) -> str: return p.replace("\\", "/")

def wait_file_stable(path: str, checks=3, interval=1.0) -> bool:
    """รอจนไฟล์นิ่ง (ขนาดไม่เปลี่ยน) ติดต่อกัน 'checks' ครั้ง"""
    prev = -1
    ok = 0
    for _ in range(checks * 10):
        if not os.path.exists(path):
            time.sleep(interval)
            continue
        try:
            size = os.path.getsize(path)
        except OSError:
            time.sleep(interval)
            continue
        if size == prev and size >= 0:
            ok += 1
            if ok >= checks:
                return True
        else:
            ok = 0
            prev = size
        time.sleep(interval)
    return False

class SFTPManager:
    def __init__(self):
        self.client = None
        self.sftp = None
        self.lock = threading.Lock()

    def _load_pkey(self):
        if not SFTP_KEY_PATH:
            return None
        for KeyCls in (paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key, paramiko.DSSKey):
            try:
                return KeyCls.from_private_key_file(SFTP_KEY_PATH, password=SFTP_KEY_PASSPHRASE or None)
            except Exception:
                continue
        raise RuntimeError("Unable to load private key from SFTP_KEY_PATH")

    def connect(self):
        with self.lock:
            if self.sftp:
                return
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            pkey = self._load_pkey()
            client.connect(
                hostname=SFTP_HOST,
                port=SFTP_PORT,
                username=SFTP_USER,
                password=SFTP_PASS if not pkey else None,
                pkey=pkey,
                timeout=30,
                look_for_keys=False,
                allow_agent=False
            )
            # กัน connection หลุดตอนคอยไฟล์
            try:
                client.get_transport().set_keepalive(30)
            except Exception:
                pass

            self.client = client
            self.sftp = client.open_sftp()

    def close(self):
        with self.lock:
            try:
                if self.sftp:
                    self.sftp.close()
                if self.client:
                    self.client.close()
            finally:
                self.sftp = None
                self.client = None

    def mkdirs(self, remote_dir: str):
        parts = [p for p in remote_dir.strip("/").split("/") if p]
        cur = ""
        for part in parts:
            cur = "/" + part if not cur else posixpath.join(cur, part)
            try:
                self.sftp.stat(cur)
                continue
            except IOError:
                # ข้าม drive root เช่น /C:
                if part.endswith(":") and (cur.count("/") <= 1 or cur in ("/C:", "/D:", "/E:")):
                    continue
                try:
                    self.sftp.mkdir(cur)
                except IOError:
                    # เช็คอีกครั้ง ถ้ายังไม่มีจริงค่อยยก error
                    try:
                        self.sftp.stat(cur)
                    except IOError as e:
                        raise

    def put_atomic(self, local_path: str, remote_path: str):
        """อัปโหลดแบบ atomic: put เป็น .part แล้ว rename เป็นชื่อจริง"""
        remote_dir = posixpath.dirname(remote_path)
        self.mkdirs(remote_dir)
        tmp_path = remote_path + ".part"
        self.sftp.put(local_path, tmp_path)
        try:
            self.sftp.remove(remote_path)
        except IOError:
            pass
        self.sftp.rename(tmp_path, remote_path)

class UploadWorker(threading.Thread):
    def __init__(self, q: queue.Queue, sftp_mgr: SFTPManager, base_local: str, base_remote: str, preserve: bool):
        super().__init__(daemon=True)
        self.q = q
        self.sftp = sftp_mgr
        self.base_local = os.path.abspath(base_local)
        self.base_remote = base_remote.rstrip("/")
        self.preserve = preserve

    def run(self):
        while True:
            path = self.q.get()
            try:
                self.process(path)
            except Exception as e:
                log(f"[error] upload failed for {path}: {e}")
            finally:
                self.q.task_done()

    def process(self, local_path: str):
        if not os.path.isfile(local_path):
            return
        log(f"[debug] event for: {local_path}")
        if not wait_file_stable(local_path, STABLE_CHECKS, STABLE_INTERVAL):
            log(f"[warn] file not stable, skip: {local_path}")
            return

        if self.preserve:
            rel = os.path.relpath(local_path, self.base_local)
            rel_posix = to_posix(rel)
            remote_path = posixpath.join(self.base_remote, rel_posix)
        else:
            fname = os.path.basename(local_path)
            remote_path = posixpath.join(self.base_remote, fname)

        log(f"[info] uploading -> {remote_path}")
        self.sftp.connect()
        self.sftp.put_atomic(local_path, remote_path)
        log(f"[info] done: {remote_path}")

class Handler(PatternMatchingEventHandler):
    def __init__(self, patterns, q):
        super().__init__(patterns=patterns, ignore_directories=True, case_sensitive=False)
        self.q = q
    def on_created(self, e): log(f"[evt] created : {e.src_path}"); self.q.put(os.path.abspath(e.src_path))
    def on_modified(self, e): log(f"[evt] modified: {e.src_path}"); self.q.put(os.path.abspath(e.src_path))
    def on_moved(self, e): log(f"[evt] moved   : {e.dest_path}"); self.q.put(os.path.abspath(e.dest_path))

def _matches_patterns(filename: str) -> bool:
    name = filename.lower()
    for pat in WATCH_PATTERNS:
        if fnmatch.fnmatch(name, pat.lower()):
            return True
    return False

def main():
    # ตรวจค่า SFTP ขั้นต่ำ
    if not (SFTP_HOST and SFTP_USER and SFTP_REMOTE_DIR):
        log("[error] ต้องตั้ง SFTP_HOST, SFTP_USER และ SFTP_REMOTE_DIR"); sys.exit(1)

    base_dir = os.path.abspath(LOCAL_WATCH_DIR)
    if not os.path.isdir(base_dir):
        log(f"[error] ไม่พบโฟลเดอร์ที่ต้องเฝ้า: {base_dir}"); sys.exit(1)

    log(f"[info] Watching : {base_dir}")
    log(f"[info] Patterns : {WATCH_PATTERNS}")
    log(f"[info] Preserve : {PRESERVE_SUBDIR}")
    log(f"[info] SFTP     : {SFTP_USER}@{SFTP_HOST}:{SFTP_PORT}")
    log(f"[info] Remote   : {SFTP_REMOTE_DIR}")

    # Self-check: เชื่อมต่อ + สร้างโฟลเดอร์ปลายทางฐาน
    try:
        _mgr = SFTPManager()
        _mgr.connect()
        _mgr.mkdirs(SFTP_REMOTE_DIR)
        _mgr.close()
        log("[info] SFTP self-check OK")
    except Exception as e:
        log(f"[error] SFTP self-check failed: {e}")
        sys.exit(1)

    q = queue.Queue()
    sftp_mgr = SFTPManager()
    worker = UploadWorker(q, sftp_mgr, base_dir, SFTP_REMOTE_DIR, PRESERVE_SUBDIR)
    worker.start()

    # คิวไฟล์ที่มีอยู่แล้วในโฟลเดอร์ (และย่อย) ให้ส่งทันที
    for root, _, files in os.walk(base_dir):
        for fn in files:
            if _matches_patterns(fn):
                q.put(os.path.abspath(os.path.join(root, fn)))
    log("[info] queued existing files (if any).")
    log("[info] --- ready; drop a file to test ---")

    observer = Observer()
    observer.schedule(Handler(WATCH_PATTERNS, q), base_dir, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("stopping...")
    finally:
        observer.stop()
        observer.join()
        sftp_mgr.close()

if __name__ == "__main__":
    main()

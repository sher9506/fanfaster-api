"""
FanFaster NotebookLM xizmati (brauzersiz, notebooklm-py CLI orqali).

Ishga tushirish (lokal):
    export FANFASTER_API_KEY="uzun-tasodifiy-kalit"
    python3.12 -m uvicorn main:app --port 8000

Ish tartibi:
    POST /api/jobs        -> darhol job_id qaytaradi (navbatga qo'yadi)
    GET  /api/jobs/{id}   -> holat: queued | running | done | error
"""
import base64
import json
import os
import queue
import re
import secrets
import shutil
import subprocess
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ───────────────────────── SOZLAMALAR ─────────────────────────
API_KEY = os.environ.get("FANFASTER_API_KEY", "")
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "https://fanfaster.uz").split(",") if o.strip()]

_DEFAULT_ACCOUNTS = [
    {
        "profile": "default",
        "email": "fafystudent@gmail.com",
        "umumiy": "88fca466-8162-466f-a64f-5f84fb6cde97",
        "workers": 2,  # shu akkauntda bir vaqtda nechta ish
    },
    {
        "profile": "profile2",
        "email": "newbolt090@gmail.com",
        "umumiy": "7b905115-d4b9-433f-aaac-4921543ae747",
        "workers": 1,
    },
]

# Hostingda (Render) NLM_ACCOUNTS_JSON env orqali akkauntlar ro'yxati beriladi, masalan:
# [{"profile":"render","email":"x@gmail.com","umumiy":"NOTEBOOK-ID","workers":1}]
ACCOUNTS = json.loads(os.environ["NLM_ACCOUNTS_JSON"]) if os.environ.get("NLM_ACCOUNTS_JSON") else _DEFAULT_ACCOUNTS

MIN_GAP_SEC = float(os.environ.get("MIN_GAP_SEC", "3"))      # bir akkauntning ketma-ket CLI chaqiruvlari orasidagi pauza
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "2"))        # vaqtinchalik xatoda qayta urinishlar soni
MAX_SOURCES = 10
JOB_TTL_SEC = 3600                                           # tugagan ishlar xotirada 1 soat turadi
AUTH_REFRESH_HOURS = float(os.environ.get("NLM_AUTH_REFRESH_HOURS", "6"))  # 0 = o'chirilgan

NLM = shutil.which("notebooklm") or "notebooklm"
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
UMUMIY_IDS = {a["umumiy"] for a in ACCOUNTS}

DEFAULT_PROMPT = (
    "Siz professional O'zbekiston huquqshunosisiz. "
    "Quyidagi kazusni IRAC (Issue, Rule, Application, Conclusion) usulida "
    "va berilgan manbalar (kodekslar, moddalar) asosida chuqur tahlil qilib yechib bering:\n\n"
    "KAZUS MATNI:\n{kazus}"
)

TRANSIENT_WORDS = ("429", "rate", "quota", "limit", "timeout", "timed out", "temporar", "503", "500", "unavailable")
AUTH_WORDS = ("auth", "login", "expired", "401", "403", "unauthorized", "sign in", "cookie")


class NlmError(Exception):
    pass


class AuthError(NlmError):
    pass


# ───────────────────────── HOLAT ─────────────────────────
JOBS = {}
JOBS_LOCK = threading.Lock()
QUEUE: "queue.Queue[str]" = queue.Queue()

UMUMIY_LOCKS = {a["profile"]: threading.Lock() for a in ACCOUNTS}   # umumiy chatda javoblar aralashmasin
GAP_LOCKS = {a["profile"]: threading.Lock() for a in ACCOUNTS}
LAST_CALL = {a["profile"]: 0.0 for a in ACCOUNTS}
STATS = {
    a["profile"]: {"email": a["email"], "ok": 0, "fail": 0, "auth_ok": True, "last_error": None, "last_refresh": None}
    for a in ACCOUNTS
}


# ───────────────────────── CLI YORDAMCHILARI ─────────────────────────
def nlm(profile: str, *args: str, timeout: int = 300, stdin: Optional[str] = None) -> str:
    """notebooklm CLI'ni bitta akkaunt (profil) nomidan chaqiradi. Chaqiruvlar orasida pauza qo'yadi."""
    with GAP_LOCKS[profile]:
        wait = MIN_GAP_SEC - (time.time() - LAST_CALL[profile])
        if wait > 0:
            time.sleep(wait)
        LAST_CALL[profile] = time.time()
    try:
        r = subprocess.run([NLM, "-p", profile, *args], capture_output=True, text=True, timeout=timeout, input=stdin)
    except subprocess.TimeoutExpired:
        raise NlmError("timeout: NotebookLM javob bermadi")
    if r.returncode != 0:
        msg = (r.stderr or r.stdout or "noma'lum xato").strip()[:500]
        low = msg.lower()
        if any(w in low for w in AUTH_WORDS):
            raise AuthError(msg)
        raise NlmError(msg)
    return r.stdout.strip()


def with_retry(fn):
    """Vaqtinchalik (limit/timeout) xatolarda kutib qayta urinadi."""
    delay = 10
    for attempt in range(MAX_RETRIES + 1):
        try:
            return fn()
        except AuthError:
            raise
        except NlmError as e:
            transient = any(w in str(e).lower() for w in TRANSIENT_WORDS)
            if not transient or attempt == MAX_RETRIES:
                raise
            time.sleep(delay)
            delay *= 3


def clean_answer(raw: str) -> str:
    lines = [
        ln for ln in raw.splitlines()
        if not re.match(r"^\s*(Continuing conversation|Resumed conversation|Conversation:)", ln)
    ]
    txt = "\n".join(lines).strip()
    return re.sub(r"^Answer:\s*", "", txt).strip()


def delete_notebook(profile: str, nid: str) -> bool:
    """Faqat vaqtinchalik daftarni o'chiradi. Umumiy daftarlarga hech qachon tegmaydi."""
    if not nid or nid in UMUMIY_IDS:
        return False
    for args in (("delete", nid), ("delete", "--notebook", nid)):
        try:
            nlm(profile, *args, timeout=60, stdin="y\n")
            return True
        except NlmError as e:
            print(f"[delete] {profile} {nid}: {e}")
    return False


# ───────────────────────── ISHNI BAJARISH ─────────────────────────
def build_prompt(req: "KazusRequest") -> str:
    tpl = req.instruction or DEFAULT_PROMPT
    if "{kazus}" not in tpl:
        tpl = tpl + "\n\nKAZUS MATNI:\n{kazus}"
    return tpl.replace("{kazus}", req.kazus_text)


def run_job(job: dict, acc: dict) -> dict:
    p = acc["profile"]
    req: KazusRequest = job["req"]
    nid = acc["umumiy"]
    custom = False
    try:
        if req.sources:
            out = with_retry(lambda: nlm(p, "create", f"Kazus: {req.title}"[:80], timeout=120))
            m = UUID_RE.search(out)
            if not m:
                raise NlmError(f"daftar ID topilmadi: {out[:200]}")
            nid, custom = m.group(0), True
            for src in req.sources[:MAX_SOURCES]:
                if src.content.startswith(("http://", "https://")):
                    with_retry(lambda s=src: nlm(p, "source", "add", "--notebook", nid, s.content, timeout=300))
                else:
                    with_retry(lambda s=src: nlm(p, "source", "add", "--notebook", nid, "--title", s.title or "Manba", s.content, timeout=300))

        prompt = build_prompt(req)
        ask = lambda: nlm(p, "ask", "--notebook", nid, prompt, timeout=420)
        if custom:
            raw = with_retry(ask)
        else:
            with UMUMIY_LOCKS[p]:   # umumiy chat: bir vaqtda bitta savol
                raw = with_retry(ask)
        return {"answer": clean_answer(raw), "notebook_type": "custom_new" if custom else "umumiy", "notebook_id": nid}
    finally:
        if custom:
            delete_notebook(p, nid)


def worker(acc: dict):
    p = acc["profile"]
    while True:
        job_id = QUEUE.get()
        job = JOBS.get(job_id)
        if not job:
            continue
        # Bu akkaunt allaqachon auth xatosi bergan ishni boshqa akkaunt olsin
        if p in job["tried"] and len(job["tried"]) < len(ACCOUNTS):
            QUEUE.put(job_id)
            time.sleep(1)
            continue
        job.update(status="running", account=acc["email"], started=time.time())
        try:
            res = run_job(job, acc)
            job.update(status="done", finished=time.time(), **res)
            STATS[p]["ok"] += 1
            STATS[p]["auth_ok"] = True
        except AuthError as e:
            STATS[p].update(auth_ok=False, last_error=str(e)[:200])
            STATS[p]["fail"] += 1
            job["tried"].add(p)
            if len(job["tried"]) < len(ACCOUNTS):
                job["status"] = "queued"
                QUEUE.put(job_id)
            else:
                job.update(status="error", finished=time.time(), error="Google sessiyasi eskirgan (cookie). Qayta login kerak.")
        except Exception as e:  # noqa: BLE001
            STATS[p]["fail"] += 1
            STATS[p]["last_error"] = str(e)[:200]
            job.update(status="error", finished=time.time(), error=str(e)[:500])


def janitor():
    while True:
        time.sleep(300)
        now = time.time()
        with JOBS_LOCK:
            for jid in [j for j, v in JOBS.items() if v.get("finished") and now - v["finished"] > JOB_TTL_SEC]:
                JOBS.pop(jid, None)


def auth_refresher():
    while True:
        for acc in ACCOUNTS:
            try:
                nlm(acc["profile"], "auth", "refresh", "--quiet", timeout=120)
                STATS[acc["profile"]]["last_refresh"] = time.strftime("%Y-%m-%d %H:%M:%S")
                STATS[acc["profile"]]["auth_ok"] = True
            except NlmError as e:
                print(f"[auth refresh] {acc['profile']}: {e}")
                STATS[acc["profile"]]["last_error"] = str(e)[:200]
        time.sleep(max(AUTH_REFRESH_HOURS, 0.1) * 3600)


def restore_cookies():
    """Hosting uchun: env'dagi base64 fayllardan profil fayllarini tiklaydi (fayl yo'q bo'lsagina).
    NLM_MASTER_<PROFIL>_B64  -> master_token.json  (asosiy, o'zi cookie yangilaydi)
    NLM_STORAGE_<PROFIL>_B64 -> storage_state.json (ixtiyoriy)"""
    for acc in ACCOUNTS:
        pname = acc["profile"]
        for env_prefix, fname in (("NLM_MASTER_", "master_token.json"), ("NLM_STORAGE_", "storage_state.json")):
            b64 = os.environ.get(f"{env_prefix}{pname.upper()}_B64")
            path = os.path.expanduser(f"~/.notebooklm/profiles/{pname}/{fname}")
            if b64 and not os.path.exists(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(base64.b64decode(b64))
                os.chmod(path, 0o600)


# ───────────────────────── API ─────────────────────────
class Source(BaseModel):
    title: str = "Manba"
    content: str


class KazusRequest(BaseModel):
    title: str = Field(default="Kazus", max_length=100)
    kazus_text: str = Field(min_length=10, max_length=12000)
    sources: List[Source] = []
    instruction: Optional[str] = Field(default=None, max_length=4000)  # {kazus} joy belgisi bilan
    external_id: Optional[str] = Field(default=None, max_length=100)   # FanFaster jadvalidagi ID


def require_key(x_api_key: str = Header(default="")):
    if not API_KEY:
        raise HTTPException(503, "FANFASTER_API_KEY sozlanmagan")
    if not secrets.compare_digest(x_api_key.encode(), API_KEY.encode()):
        raise HTTPException(401, "Kalit noto'g'ri")


@asynccontextmanager
async def lifespan(app: FastAPI):
    restore_cookies()
    for acc in ACCOUNTS:
        for _ in range(acc["workers"]):
            threading.Thread(target=worker, args=(acc,), daemon=True).start()
    threading.Thread(target=janitor, daemon=True).start()
    if AUTH_REFRESH_HOURS > 0:
        threading.Thread(target=auth_refresher, daemon=True).start()
    yield


app = FastAPI(title="FanFaster NotebookLM API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)


@app.post("/api/jobs", dependencies=[Depends(require_key)])
def create_job(req: KazusRequest):
    if len(req.sources) > MAX_SOURCES:
        raise HTTPException(422, f"Manbalar soni {MAX_SOURCES} tadan oshmasin")
    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {
            "req": req, "status": "queued", "created": time.time(), "tried": set(),
            "external_id": req.external_id,
        }
    QUEUE.put(job_id)
    return {"job_id": job_id, "status": "queued", "queue_size": QUEUE.qsize()}


@app.get("/api/jobs/{job_id}", dependencies=[Depends(require_key)])
def get_job(job_id: str):
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(404, "Ish topilmadi (muddati o'tgan yoki server qayta ishga tushgan)")
    return {
        "job_id": job_id,
        "status": job["status"],
        "external_id": job.get("external_id"),
        "answer": job.get("answer"),
        "error": job.get("error"),
        "notebook_type": job.get("notebook_type"),
        "used_account": job.get("account"),
        "elapsed_sec": round((job.get("finished") or time.time()) - job["created"], 1),
    }


@app.get("/api/stats", dependencies=[Depends(require_key)])
def stats():
    return {"queue_size": QUEUE.qsize(), "accounts": STATS}


@app.get("/")
@app.get("/ping")
def health_check():
    return {"status": "FanFaster AI backend faol ishlamoqda!", "active_profiles_count": len(ACCOUNTS)}

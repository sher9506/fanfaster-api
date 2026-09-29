"""
FanFaster NotebookLM xizmati (brauzersiz, notebooklm-py CLI orqali).

Ishga tushirish (lokal):
    export FANFASTER_API_KEY="uzun-tasodifiy-kalit"
    python3.12 -m uvicorn main:app --port 8000

Ish tartibi:
    POST /api/jobs        -> darhol job_id qaytaradi (navbatga qo'yadi)
    GET  /api/jobs/{id}   -> holat: queued | running | done | error
"""
import asyncio
import base64
import json
import os
import queue
import re
import secrets
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, model_validator

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
MAX_SOURCES = 18
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

# Har bir so'rovga qo'shiladigan qoida: javob oxirida savol/taklif bo'lmasin.
NO_QUESTION_RULE = (
    "\n\nMUHIM QOIDA: Javobni xulosa bilan tugating. Javob oxirida foydalanuvchiga hech qanday savol bermang, "
    "internetdan qidirishni yoki qo'shimcha yordam/ma'lumot taklif qilishni yozmang."
)
LATIN_RULE = (
    "\nJavobni to'liq o'zbek LOTIN yozuvida yozing. Manba kirill yozuvida bo'lsa ham, undagi iboralarni lotin alifbosiga o'tkazib yozing; "
    "javobda kirill harflari bo'lmasin."
)
OFFER_HINTS = ("xohlaysizmi", "istaysizmi", "internet", "qidirib", "batafsilroq", "yordam beraman", "yordam bera olaman")
BRAND_RE = re.compile(r"(?i)notebook\s?lm|gemini\s+notebook")
# Ustozga ko'rinmasligi kerak bo'lgan ichki so'zlar (daftar, baza, brend). Shunday so'z bor gap butunlay olib tashlanadi.
LEAK_RE = re.compile(r"(?i)notebook\s?lm|gemini\s+notebook|daftar|umumiy\s+baza")
# NotebookLM "manba yo'q, tahlil qila olmayman" degan holatni aniqlash iboralari
REFUSAL_HINTS = (
    "hech qanday manba", "manbalar qatorida bo'lmagani", "manbalar orasida yo'q",
    "imkoni yo'q", "amalga oshirish imkoni", "faqat daftar", "yuklangan manba",
    "manbalar mavjud emas", "manba yoki hujjat mavjud emas",
)
NO_BASIS_MSG_GENERAL = (
    "Bu kazus bo'yicha tegishli qonun matnlari topilmadi. "
    "Kazusga mos manba (qonun havolasi yoki matni) qo'shib, qayta urinib ko'ring."
)
NO_BASIS_MSG_SOURCES = (
    "Berilgan manbalarda bu kazusga tegishli norma topilmadi. "
    "Boshqa manba qo'shing yoki manbasiz qayta urinib ko'ring."
)
CLEAN_MARKDOWN = os.environ.get("CLEAN_MARKDOWN", "1") == "1"   # ###, **, --- belgilarini olib tashlash
URL_ONLY_RE = re.compile(r"^https?://\S+$", re.I)
MAX_ARG_BYTES = 100_000   # Linux'da bitta buyruq argumenti ~128 KB dan oshmasligi kerak (UTF-8 baytlarda)
PENDING_STATUSES = {"processing", "pending", "preparing", "indexing", "uploading", "importing"}
BAD_STATUSES = {"error", "failed"}
SOURCE_FAIL_URL_MSG = "Manbalardan ba'zilarini ochib bo'lmadi. Havola o'rniga matnini «+ Matn» orqali qo'shing."
SOURCE_FAIL_TEXT_MSG = "Manbalardan ba'zilarini qo'shib bo'lmadi. Qayta urinib ko'ring."

TRANSIENT_WORDS = ("429", "rate", "quota", "limit", "timeout", "timed out", "temporar", "503", "500", "unavailable")
AUTH_WORDS = ("auth", "login", "expired", "401", "403", "unauthorized", "sign in", "cookie")


class NlmError(Exception):
    pass


class AuthError(NlmError):
    pass


class SourceError(NlmError):
    """Manba bilan bog'liq xato. `user_msg` ustozga ko'rsatilishi mumkin (brendsiz, neytral)."""

    def __init__(self, user_msg: str, detail: str = ""):
        super().__init__(detail or user_msg)
        self.user_msg = user_msg


class NoBasisError(SourceError):
    """Model "manba yo'q, tahlil qila olmayman" deb rad etdi. Ustozga tayyor neytral matn ko'rsatiladi."""


# ───────────────────────── HOLAT ─────────────────────────
JOBS = {}
JOBS_LOCK = threading.Lock()
QUEUE: "queue.Queue[str]" = queue.Queue()

UMUMIY_LOCKS = {a["profile"]: threading.Lock() for a in ACCOUNTS}   # umumiy chatda javoblar aralashmasin
GAP_LOCKS = {a["profile"]: threading.Lock() for a in ACCOUNTS}
REFRESH_LOCKS = {a["profile"]: threading.Lock() for a in ACCOUNTS}
LAST_CALL = {a["profile"]: 0.0 for a in ACCOUNTS}
STATS = {
    a["profile"]: {"email": a["email"], "ok": 0, "fail": 0, "auth_ok": True, "last_error": None, "last_refresh": None}
    for a in ACCOUNTS
}


# ───────────────────────── CLI YORDAMCHILARI ─────────────────────────
def ensure_session(profile: str, force: bool = False) -> bool:
    """storage_state.json yo'q (yoki majburan) bo'lsa, master token'dan cookie'ni qayta yasaydi."""
    path = os.path.expanduser(f"~/.notebooklm/profiles/{profile}/storage_state.json")
    with REFRESH_LOCKS[profile]:
        if os.path.exists(path) and not force:
            return True
        try:
            r = subprocess.run([NLM, "-p", profile, "auth", "refresh", "--quiet"],
                               capture_output=True, text=True, timeout=180)
        except subprocess.TimeoutExpired:
            print(f"[ensure_session] {profile}: timeout")
            return False
        if r.returncode != 0:
            print(f"[ensure_session] {profile}: {(r.stderr or r.stdout).strip()[:300]}")
            return False
        STATS[profile]["last_refresh"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return os.path.exists(path)


def nlm(profile: str, *args: str, timeout: int = 300, stdin: Optional[str] = None, _retried: bool = False) -> str:
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
        if "not logged in" in low and not _retried and ensure_session(profile, force=True):
            return nlm(profile, *args, timeout=timeout, stdin=stdin, _retried=True)
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


def _norm(s: str) -> str:
    return re.sub(r"[‘’ʻʼ`´]", "'", s).lower()


def is_refusal(text: str) -> bool:
    """Qisqa javobda "manba yo'q / tahlil imkoni yo'q" iborasi bo'lsa va IRAC tuzilmasi bo'lmasa, rad deb hisoblanadi.
    Uzun yoki tuzilmali javobdagi "manbada yo'q" degan foydali gaplar rad hisoblanmaydi."""
    n = _norm(text)
    hit = any(h in n for h in REFUSAL_HINTS)
    structured = "issue" in n and "conclusion" in n
    return hit and len(text) < 1500 and not structured


def strip_markdown(txt: str) -> str:
    """###, **, --- belgilarini olib tashlaydi, ro'yxatni tirega aylantiradi, qattiq qator uzilishlarini birlashtiradi."""
    out_lines = []
    for ln in txt.splitlines():
        s = ln.rstrip()
        if re.match(r"^\s*([-*_])\1{2,}\s*$", s):          # --- yoki ***
            out_lines.append("")
            continue
        h = re.match(r"^\s*#{1,6}\s*(.*)$", s)
        if h:                                                # sarlavha: alohida abzats
            out_lines += ["", re.sub(r"\*\*|__", "", h.group(1)).strip(), ""]
            continue
        s = re.sub(r"^(\s*)[*•]\s+", r"\1- ", s)              # * bandi -> - bandi
        s = re.sub(r"\*\*|__", "", s)
        s = re.sub(r"(?<![*\w])\*(?!\s)([^*\n]+?)\*(?![*\w])", r"\1", s)   # *kursiv*
        out_lines.append(s)
    # qattiq qator uzilishlarini birlashtirish (ro'yxat bandi/raqamli band/sarlavha yangi qator boshlaydi)
    starts_new = re.compile(r"^\s*(\d+[.)]\s|[-•]\s)")
    head_kw = re.compile(r"(?i)^\s*(?:\d+[.)]\s*|[IVX]+[.)]\s*)?(issue|rule|application|conclusion)\b")
    merged = []
    for ln in out_lines:
        cur = ln.strip()
        if not cur:
            if merged and merged[-1] != "":
                merged.append("")
            continue
        prev_blank = (not merged) or merged[-1] == ""
        is_head = (head_kw.match(cur) and len(cur) <= 100) or \
                  (prev_blank and len(cur) <= 65 and not re.search(r"[.!?:;,]$", cur) and not starts_new.match(ln))
        if is_head:                                          # sarlavha: alohida abzats
            if merged and merged[-1] != "":
                merged.append("")
            merged += [cur, ""]
            continue
        if merged and merged[-1] != "" and not starts_new.match(ln):
            merged[-1] += " " + cur
        else:
            merged.append(ln.rstrip() if starts_new.match(ln) else cur)
    res = []
    for ln in merged:                                        # ko'p qatorga cho'zilgan *kursiv* ham endi bir qatorda
        ln = re.sub(r"(?<![*\w])\*(?!\s)([^*\n]+?)\*(?![*\w])", r"\1", ln)
        res.append(ln)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(res)).strip()


def strip_leaks(txt: str) -> str:
    """Ichki so'zlar (daftar, umumiy baza, NotebookLM) uchragan gaplarni olib tashlaydi."""
    res = []
    for ln in txt.splitlines():
        if not LEAK_RE.search(ln):
            res.append(ln)
            continue
        m = re.match(r"^(\s*(?:\d+[.)]|[-•])\s+)?(.*)$", ln)
        prefix, body = (m.group(1) or ""), m.group(2)
        kept = [x for x in re.split(r"(?<=[.!?])\s+", body) if x and not LEAK_RE.search(x)]
        if kept:
            res.append(prefix + " ".join(kept))
    return re.sub(r"\n{3,}", "\n\n", "\n".join(res)).strip()


def clean_answer(raw: str, custom: bool = False) -> str:
    lines = [
        ln for ln in raw.splitlines()
        if not re.match(r"^\s*(Continuing conversation|Resumed conversation|Conversation:)", ln)
    ]
    txt = "\n".join(lines).strip()
    txt = re.sub(r"^Answer:\s*", "", txt).strip()
    if is_refusal(txt):
        raise NoBasisError(NO_BASIS_MSG_SOURCES if custom else NO_BASIS_MSG_GENERAL,
                           f"model rad etdi: {txt[:300]!r}")
    txt = re.sub(r"(?<=\w)[ʻʼ‘’`´](?=\w)", "'", txt)       # oʻ, gʻ va o', g' bir xil ko'rinsin
    if CLEAN_MARKDOWN:
        txt = strip_markdown(txt)
    txt = strip_trailing_offer(txt)
    txt = strip_leaks(txt)
    return BRAND_RE.sub("", txt)


def strip_trailing_offer(txt: str) -> str:
    """Javob oxiridagi "internetdan qidiraymi?" kabi savol/taklifni olib tashlaydi (oxirgi gap yoki 💡 abzats)."""
    txt = txt.strip()
    for _ in range(3):
        paras = re.split(r"\n\s*\n", txt)
        last = paras[-1].strip()
        if len(paras) > 1 and last.startswith(("💡", "🔍")):
            txt = "\n\n".join(paras[:-1]).strip()
            continue
        sents = re.split(r"(?<=[.!?])\s+", last)
        tail = sents[-1].strip()
        if tail.rstrip("*_ ").endswith("?") and any(h in tail.lower() for h in OFFER_HINTS):
            rest = " ".join(sents[:-1]).strip()
            txt = "\n\n".join(paras[:-1] + ([rest] if rest else [])).strip()
            continue
        break
    return txt


def delete_notebook(profile: str, nid: str) -> bool:
    """Faqat vaqtinchalik daftarni o'chiradi. Umumiy daftarlarga hech qachon tegmaydi."""
    if not nid or nid in UMUMIY_IDS:
        return False
    for args in (("delete", "--notebook", nid, "--yes"), ("delete", "--notebook", nid), ("delete", nid)):
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
    return tpl.replace("{kazus}", req.kazus_text) + NO_QUESTION_RULE + LATIN_RULE


def source_target(src: "Source"):
    """Manba turini aniqlaydi: ("url", havola) yoki ("text", matn)."""
    if src.url:
        return "url", src.url.strip()
    c = (src.content or "").strip()
    if URL_ONLY_RE.match(c):     # eski format: content ichida faqat havola
        return "url", c
    return "text", c


def is_source_failure(msg: str) -> bool:
    low = msg.lower()
    return any(k in low for k in ("failed to add source", "add_source failed", "rpc_code=9", "could not be confirmed"))


def add_source(p: str, nid: str, src: "Source") -> None:
    kind, value = source_target(src)
    title = (src.title or "Manba")[:120]
    fail_msg = SOURCE_FAIL_URL_MSG if kind == "url" else SOURCE_FAIL_TEXT_MSG
    try:
        if kind == "url":
            with_retry(lambda: nlm(p, "source", "add", "--notebook", nid, "--type", "url",
                                   "--timeout", "180", "--", value, timeout=360))
        elif len(value.encode("utf-8")) <= MAX_ARG_BYTES:
            # --type text: matn hech qachon serverdagi fayl yo'li deb talqin qilinmaydi
            with_retry(lambda: nlm(p, "source", "add", "--notebook", nid, "--type", "text",
                                   "--title", title, "--", value, timeout=300))
        else:
            # katta matn: argument o'lchami cheklovi sababli vaqtinchalik fayl orqali
            fd, path = tempfile.mkstemp(suffix=".txt")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(value)
                with_retry(lambda: nlm(p, "source", "add", "--notebook", nid, "--type", "file",
                                       "--mime-type", "text/plain", "--title", title,
                                       "--timeout", "180", "--", path, timeout=420))
            finally:
                try:
                    os.remove(path)
                except OSError:
                    pass
    except AuthError as e:
        # "Failed to add source" / rpc_code=9 -- bu manbaning o'zi ochilmagani, akkaunt auth xatosi emas
        if is_source_failure(str(e)):
            raise SourceError(fail_msg, f"source add ({kind}, «{title}»): {e}")
        raise
    except SourceError:
        raise
    except NlmError as e:
        raise SourceError(fail_msg, f"source add ({kind}, «{title}»): {e}")


def source_statuses(p: str, nid: str) -> List[str]:
    """`source list` jadvalidan har bir manbaning holatini (oxirgi ustun) o'qiydi."""
    out = nlm(p, "source", "list", "--notebook", nid, timeout=60)
    res = []
    for line in out.splitlines():
        if "│" not in line:
            continue
        cells = [c.strip() for c in line.split("│") if c.strip()]
        if len(cells) >= 2:
            res.append(cells[-1].lower())
    return res


def wait_sources_ready(p: str, nid: str, max_wait: int = 180) -> None:
    """Manbalar tayyor bo'lguncha kutadi; xato holatdagi manba bo'lsa, ishni to'xtatadi.
    `source list` o'qib bo'lmasa, 3 marta qayta urinadi — xatolik bilan davom etmaydi."""
    deadline = time.time() + max_wait
    read_errors = 0
    while True:
        try:
            st = source_statuses(p, nid)
            read_errors = 0
        except AuthError:
            raise
        except NlmError as e:
            read_errors += 1
            print(f"[wait_sources] {p}: holatni o'qib bo'lmadi ({read_errors}/3): {e}")
            if read_errors >= 3:
                raise SourceError(SOURCE_FAIL_TEXT_MSG, f"source list 3 marta xato: {e}")
            time.sleep(5)
            continue
        if any(s in BAD_STATUSES for s in st):
            raise SourceError(SOURCE_FAIL_URL_MSG if any("url" in s for s in st) else SOURCE_FAIL_TEXT_MSG,
                              f"manba holati xato: {st}")
        if not any(s in PENDING_STATUSES for s in st) or time.time() > deadline:
            return
        time.sleep(5)


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
                add_source(p, nid, src)
            wait_sources_ready(p, nid)

        prompt = build_prompt(req)
        ask = lambda: nlm(p, "ask", "--notebook", nid, prompt, timeout=420)
        if custom:
            raw = with_retry(ask)
        else:
            with UMUMIY_LOCKS[p]:   # umumiy chat: bir vaqtda bitta savol
                raw = with_retry(ask)
        return {"answer": clean_answer(raw, custom), "notebook_type": "custom_new" if custom else "umumiy", "notebook_id": nid}
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
                job.update(status="error", finished=time.time(), error="AI xizmatiga ulanib bo'lmadi. Birozdan keyin qayta urinib ko'ring.")
        except Exception as e:  # noqa: BLE001
            STATS[p]["fail"] += 1
            STATS[p]["last_error"] = str(e)[:200]
            print(f"[job {job_id}] {p}: {e}")   # xom xato faqat serverda (ustozga ko'rsatilmaydi)
            user_msg = getattr(e, "user_msg", None) or "Javob tayyorlanmadi. Qayta urinib ko'ring."
            job.update(status="error", finished=time.time(), error=user_msg)


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
    """Manba: yoki `url` (havola), yoki `content` (matn). Eski format ham ishlaydi:
    content ichida faqat http(s) havola bo'lsa, havola sifatida olinadi."""
    title: str = Field(default="Manba", max_length=200)
    content: Optional[str] = Field(default=None, max_length=300000)
    url: Optional[str] = Field(default=None, max_length=2000)

    @model_validator(mode="after")
    def _check(self):
        if not (self.url or (self.content and self.content.strip())):
            raise ValueError("Manba uchun url yoki content kerak")
        if self.url and not self.url.strip().lower().startswith(("http://", "https://")):
            raise ValueError("url faqat http:// yoki https:// bo'lishi kerak")
        return self


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
        await asyncio.to_thread(ensure_session, acc["profile"])
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
    # diagnostika: Bolt manbalarni yuboryaptimi? (Render Logs'da ko'rinadi, matn chiqarilmaydi)
    kinds = [("url" if (x.url or URL_ONLY_RE.match((x.content or "").strip())) else f"text:{len(x.content or '')}") for x in req.sources]
    print(f"[create] job={job_id} sources={len(req.sources)} {kinds}")
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

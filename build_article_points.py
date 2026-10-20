"""Build article_points/<doc>.json = {"161": [[qism_frag|null, [[band_no, band_frag|null], ...]], ...]}
from cached lex.uz pages (lexpages/<doc>.html). Fragments are the shortest (4..12 words) paragraph prefix whose
first whole-word occurrence in the lowercased page text is that paragraph (i.e. a browser #:~:text= lands there)."""
import bisect, html, json, re, sys, time
from pathlib import Path

LIB = Path(__file__).resolve().parent
OUT = LIB / "article_points"
DIV = re.compile(r'<div name="([^"]+)" id="\1"[^>]*>(.*?)</div>', re.S)
WRAP = re.compile(r'<div class="([A-Z][A-Z0-9_]*)[^"]*"([^>]*)>')
BLOCK = re.compile(r"<(?:br|p|div|li|tr|td)\b[^>]*>|</(?:p|div|li|tr|td)>", re.I)
TAG = re.compile(r"<[^>]+>")  # inline tags vanish without a space, like innerText: 47<sup>4</sup> -> 474
ARTICLE = re.compile(r"^(\d+)(?:<sup>\s*(\d+)\s*</sup>)?\s*-\s*(?:модда|modda)\.", re.I)
CHAPTER = re.compile(r"^\d+[\s\-–]*(?:боб|bob)\b|бўлим|bo[ʻʼ‘’'`]?lim", re.I)
BAND = re.compile(r"^(\d{1,2})\)\s")
NOTE = re.compile(r"таҳрир|tahrir|кучга|kuchga|асосан|asosan|чиқарилган|chiqarilgan|ўз кучини|o[ʻʼ‘’'`]z kuchini", re.I)
PREV = re.compile(r"олдинги таҳрирга қаранг|oldingi tahrirga qarang", re.I)
UI = {"Ҳужжатга таклиф юбориш", "Аудиони тинглаш", "Ҳужжат элементидан ҳавола олиш",
      "Hujjatga taklif yuborish", "Audioni tinglash", "Hujjat elementidan havola olish"}
NOTE_CLASSES = {"COMMENT", "COMMENTLEXUZ", "CHANGES_ORIGINS", "BY_DEFAULT", "INDEXES_ON_REF", "FOOTNOTE"}
KMIN, KMAX = 4, 16


def clean(raw):
    return " ".join(html.unescape(TAG.sub("", BLOCK.sub(" ", raw))).split())


def blocks(page):
    """Yield (wrapper_class, text, raw) for every visible lex.uz text div after divCont."""
    i = page.find('id="divCont"')
    if i >= 0:
        page = page[i:]
    wraps = [(m.start(), m.group(1), "display:none" in m.group(2)) for m in WRAP.finditer(page)]
    pos = [w[0] for w in wraps]
    for m in DIV.finditer(page):
        k = bisect.bisect(pos, m.start()) - 1
        cls, hidden = (wraps[k][1], wraps[k][2]) if k >= 0 else ("ACT_TEXT", False)
        if not hidden:
            yield cls, clean(m.group(2)), m.group(2)


def is_skip(cls, t):
    return (not any(c.isalnum() for c in t) or cls in NOTE_CLASSES or t in UI or t.startswith("[") or PREV.search(t)
            or (t.startswith("(") and NOTE.search(t)))


def article_key(raw):
    m = ARTICLE.match(re.sub(r"<(?!/?sup\b)[^>]*>", "", raw).strip())
    return (m.group(1) + ("_" + m.group(2) if m.group(2) else "")) if m else None


def starts_lower(t):
    c = next((ch for ch in t if ch.isalpha()), "")
    return c.islower()


class Locator:
    """Finds the shortest unique-at-position word prefix in the lowercased flat page text."""

    def __init__(self, texts):
        self.offsets, parts, off = [], [], 0
        for t in texts:
            self.offsets.append(off)
            parts.append(t)
            off += len(t) + 1
        self.flat = " ".join(parts).lower()

    def first_hit(self, needle, limit):
        """True iff the first whole-word occurrence of needle starts at `limit`."""
        f, start, n = self.flat, 0, len(needle)
        while True:
            j = f.find(needle, start, limit + n)
            if j < 0:
                return False
            if (j == 0 or not f[j - 1].isalnum()) and (j + n == len(f) or not f[j + n].isalnum()):
                return j == limit
            start = j + 1

    def frag(self, idx, text):
        words = text.split()
        lo = min(KMIN, len(words))
        for k in range(lo, min(KMAX, len(words)) + 1):
            cand = " ".join(words[:k])
            if self.first_hit(cand.lower(), self.offsets[idx]):
                return cand
        return None


def build(page):
    items = list(blocks(page))
    loc = Locator([t for _, t, _ in items])
    res, cur = {}, None
    for idx, (cls, t, raw) in enumerate(items):
        key = article_key(raw) if cls != "ACT_TEXT" or ARTICLE.match(t) else None
        if key:
            cur = None if key in res else res.setdefault(key, [])  # repeated heading (quoted text): ignore
            continue
        if cur is None:
            continue
        if cls in ("CLAUSE_DEFAULT", "TEXT_HEADER_DEFAULT", "SIGNATURE", "ACT_TITLE_APPL") or (
                len(t) < 200 and CHAPTER.search(t) and not t.endswith((";", ".", ":"))):
            cur = None
            continue
        if is_skip(cls, t):
            continue
        m = BAND.match(t)
        if m:
            if not cur:
                cur.append([None, []])
            cur[-1][1].append([int(m.group(1)), loc.frag(idx, t)])
        elif re.match(r"^[a-zа-яёўқғҳ]\)", t, re.I) or starts_lower(t):
            continue  # letter sub-point / unnumbered paragraph (хатбоши) continuing the current part
        else:
            cur.append([loc.frag(idx, t), []])
    return res


def main():
    t0 = time.time()
    OUT.mkdir(exist_ok=True)
    docs = (LIB / "artdocs.txt").read_text().split()
    stats = dict(docs=0, arts=0, qisms=0, bands=0, noart=0, missing=0)
    for d in docs:
        p = LIB / "lexpages" / f"{d}.html"
        if not p.exists() or p.stat().st_size <= 5000:
            stats["missing"] += 1
            continue
        res = build(p.read_text(encoding="utf-8", errors="replace"))
        if not res:
            stats["noart"] += 1
            continue
        (OUT / f"{d}.json").write_text(json.dumps(res, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
        stats["docs"] += 1
        stats["arts"] += len(res)
        stats["qisms"] += sum(len(v) for v in res.values())
        stats["bands"] += sum(len(q[1]) for v in res.values() for q in v)
    files = list(OUT.glob("*.json"))
    sizes = [f.stat().st_size for f in files]
    print(stats, f"total={sum(sizes)/1e6:.2f}MB largest={max(sizes)/1e6:.3f}MB",
          max(files, key=lambda f: f.stat().st_size).name, f"time={time.time()-t0:.1f}s")
    lab = json.loads((OUT / "6257288.json").read_text())
    assert lab["161"][0][0].startswith("Номуайян муддатга"), lab["161"][0]
    assert lab["161"][1][1][1][0] == 2 and lab["161"][1][1][1][1].startswith("2) технологиянинг"), lab["161"][1]
    assert "20" in json.loads((OUT / "-6445145.json").read_text())


if __name__ == "__main__":
    sys.exit(main())

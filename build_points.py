"""Build plenum_points.json = {doc: [[n, "first 8 words"], ...]} from cached lex.uz pages."""
import html, json, re, sys
from pathlib import Path

LIB = Path(__file__).resolve().parent
NWORDS = 8
DIV = re.compile(r'<div name="-?\d+" id="-?\d+"[^>]*>(.*?)</div>', re.S)
TAG = re.compile(r"<[^>]+>")  # tags -> space, except </sup> ('17<sup>4</sup>-bandi' -> '17 4-bandi', as in fulltext)
POINT = re.compile(r"^(\d{1,3})\.\s+(.*)", re.S)


def div_texts(page):
    i = page.find('id="divCont"')
    if i >= 0:
        page = page[i:]
    for m in DIV.finditer(page):
        yield re.sub(r"\s+", " ", html.unescape(TAG.sub(" ", m.group(1).replace("</sup>", "")))).strip()


def points(page):
    out, seen = [], set()
    for t in div_texts(page):
        m = POINT.match(t)
        if not m:
            continue
        n = int(m.group(1))
        if n in seen:
            continue
        seen.add(n)
        out.append([n, " ".join(m.group(2).split()[:NWORDS])])
    return out


def main():
    docs = [d.strip() for d in (LIB / "plenums.txt").read_text().split() if d.strip()]
    res, missing = {}, []
    for d in docs:
        p = LIB / "lexpages" / f"{d}.html"
        if not p.exists():
            missing.append(d)
            continue
        res[d] = points(p.read_text(encoding="utf-8", errors="replace"))
    (LIB / "plenum_points.json").write_text(
        json.dumps(res, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    pts = sum(map(len, res.values()))
    print(f"docs={len(res)} points={pts} zero={sum(not v for v in res.values())} missing_pages={missing}")
    s = dict(res.get("-6684613", []))  # sanity check from spec
    assert s.get(44, "").startswith("MKning 156-moddasiga koʻra"), s.get(44)
    assert s.get(62, "").startswith("Mehnat shartnomasi qonunga xilof"), s.get(62)


if __name__ == "__main__":
    sys.exit(main())

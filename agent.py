#!/usr/bin/env python3
"""
WTN AutoPost v2 -- World Trend Network autonomous news publisher.

Runs on GitHub Actions every 30 minutes. Target: 2-3 live Blogger posts per run.

Pipeline
  1. HUNT      Google News RSS (7 categories). Window = HUNT_HOURS, widened to
               HUNT_FALLBACK_HOURS automatically if too few stories qualify.
  2. VERIFY    A story needs 3+ independent outlets incl. 1 reputable, or 2
               reputable outlets. Harm-category + duplicate filters.
  3. PHOTO     Real article image first (Google News links are decoded to the
               publisher URL), Wikimedia Commons as fallback. Checked BEFORE the
               AI call so no AI quota is wasted. Never posts without a photo.
  4. REWRITE   Gemini (round-robin keys, model fallback, per-key cooldown),
               Groq as backup. Output is validated (length / structure).
  5. PUBLISH   Blogger API v3, live (isDraft=false). One OAuth token per run,
               retries with backoff, duplicate-guard, clear fatal-error messages.
  6. REPORT    seen.json + status.json written atomically after every post.

The loop keeps going through candidates until MAX_POSTS are published (or the
candidates / time budget run out) -- one bad story never costs a whole slot.
"""

import os
import re
import json
import time
import random
import base64
import hashlib
import html as htmlmod
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, quote_plus, quote, urljoin
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

import requests

# ------------------------------------------------------------------ config
UA = {"User-Agent": "Mozilla/5.0 (Linux; Android 10) WTN-AutoPost/2.0"}
UA_PAGE = {   # article pages: many publishers block non-browser agents
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _split_keys(raw):
    return [k.strip() for k in re.split(r"[,\s;]+", raw or "") if k.strip()]


GEMINI_KEYS = _split_keys(os.environ.get("GEMINI_API_KEYS")
                          or os.environ.get("GEMINI_API_KEY", ""))
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()

BLOG_ID = os.environ.get("BLOGGER_BLOG_ID", "").strip()
BG_CLIENT_ID = os.environ.get("BLOGGER_CLIENT_ID", "").strip()
BG_CLIENT_SECRET = os.environ.get("BLOGGER_CLIENT_SECRET", "").strip()
BG_REFRESH_TOKEN = os.environ.get("BLOGGER_REFRESH_TOKEN", "").strip()


def _int(name, default):
    try:
        return int(float(os.environ.get(name) or default))
    except ValueError:
        return int(default)


MAX_POSTS = _int("MAX_POSTS_PER_RUN", 3)
MIN_POSTS = _int("MIN_POSTS_PER_RUN", 2)       # below this -> widen the window
MIN_SOURCES = _int("MIN_SOURCES", 3)
HUNT_HOURS = _int("HUNT_HOURS", 2)
HUNT_FALLBACK_HOURS = _int("HUNT_FALLBACK_HOURS", 6)
RUN_BUDGET_SEC = _int("RUN_BUDGET_SEC", 11 * 60)   # workflow timeout is 15 min
MAX_CANDIDATES = _int("MAX_CANDIDATES", 40)

# Preferred order (Google's docs, Oct 2026). gemini-2.5-* and 2.0-* were shut
# down on/before 16 Oct 2026, so they are NOT in the chain. At run start the
# real list is fetched from the API (discover_models) and this order is
# filtered to what is actually live; unknown IDs here are harmless (404 -> skip).
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash").strip()
GEMINI_FALLBACK_MODELS = ["gemini-3.5-flash", "gemini-3.5-flash-lite",
                          "gemini-3.1-flash-lite", "gemini-3-flash"]
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
GROQ_FALLBACK_MODELS = ["llama-3.1-8b-instant", "openai/gpt-oss-20b"]

CATEGORIES = {
    "World": "world news",
    "Technology": "technology news",
    "Sports": "sports news",
    "Entertainment": "entertainment news",
    "Business": "business news",
    "Health": "health news",
    "Science": "science news",
}

STATE_FILE = os.environ.get("STATE_FILE", "seen.json")
STATUS_FILE = os.environ.get("STATUS_FILE", "status.json")
DRY_RUN = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")


class FatalError(Exception):
    """Misconfiguration / dead credentials: retrying in this run is pointless."""


class BloggerLimit(Exception):
    """Blogger quota / rate limit hit: stop publishing for this run."""


def log(msg):
    print(f"[wtn] {datetime.now(timezone.utc):%H:%M:%S} {msg}", flush=True)


def http_get(url, timeout=20, headers=None, retries=2):
    last = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=headers or UA, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2 * (attempt + 1))
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
    raise last


# --------------------------------------------------------- domains / sources
REPUTABLE_DOMAINS = {
    "reuters.com", "apnews.com", "bbc.com", "bbc.co.uk", "bbci.co.uk", "cnn.com",
    "nbcnews.com", "abcnews.go.com", "cbsnews.com", "npr.org", "pbs.org",
    "theguardian.com", "nytimes.com", "washingtonpost.com", "wsj.com",
    "usatoday.com", "latimes.com", "chicagotribune.com", "nypost.com",
    "thehill.com", "politico.com", "axios.com", "time.com", "newsweek.com",
    "independent.co.uk", "telegraph.co.uk", "thetimes.co.uk", "foxnews.com",
    "usnews.com", "businessinsider.com", "theatlantic.com", "globalnews.ca",
    "rte.ie", "irishtimes.com", "smh.com.au", "theage.com.au", "nzherald.co.nz",
    "dw.com", "france24.com", "aljazeera.com", "euronews.com", "sky.com",
    "skynews.com.au", "abc.net.au", "cbc.ca", "ctvnews.ca", "theglobeandmail.com",
    "straitstimes.com", "channelnewsasia.com", "scmp.com", "japantimes.co.jp",
    "nhk.or.jp", "koreaherald.com", "dawn.com", "tribune.com.pk", "thehindu.com",
    "indianexpress.com", "indiatimes.com", "hindustantimes.com", "ndtv.com",
    "al-monitor.com", "middleeasteye.net",
    "bloomberg.com", "cnbc.com", "forbes.com", "fortune.com", "economist.com",
    "ft.com", "marketwatch.com", "barrons.com", "investing.com", "fool.com",
    "techcrunch.com", "theverge.com", "wired.com", "arstechnica.com",
    "engadget.com", "gizmodo.com", "zdnet.com", "cnet.com", "mashable.com",
    "venturebeat.com", "thenextweb.com", "9to5mac.com", "9to5google.com",
    "techradar.com", "tomsguide.com", "pcmag.com", "androidauthority.com",
    "espn.com", "skysports.com", "foxsports.com", "cbssports.com", "nbcsports.com",
    "si.com", "theathletic.com", "espncricinfo.com", "cricbuzz.com", "goal.com",
    "mlb.com", "nba.com", "nfl.com", "ign.com", "polygon.com",
    "variety.com", "hollywoodreporter.com", "deadline.com", "billboard.com",
    "rollingstone.com", "ew.com", "people.com", "etonline.com", "vulture.com",
    "indiewire.com", "screenrant.com", "collider.com",
    "who.int", "nih.gov", "cdc.gov", "nasa.gov", "esa.int", "webmd.com",
    "medicalxpress.com", "sciencedaily.com", "newscientist.com", "nature.com",
    "science.org", "space.com", "livescience.com", "healthline.com",
    "mayoclinic.org", "statnews.com", "scientificamerican.com",
}

# Re-publishers: they copy other outlets, so they never count as "independent".
AGGREGATORS = {"msn.com", "yahoo.com", "flipboard.com", "newsbreak.com",
               "smartnews.com", "ground.news", "aol.com", "dailyhunt.in",
               "news.google.com", "google.com", "newsnow.co.uk", "ca.news.yahoo.com"}

_SECOND_LEVEL = {"co", "com", "org", "net", "gov", "ac", "edu", "go", "or"}


def domain_of(url):
    try:
        d = urlparse(url).netloc.lower().split(":")[0]
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return ""


def root_domain(d):
    """edition.cnn.com -> cnn.com ; feeds.bbci.co.uk -> bbci.co.uk"""
    parts = [p for p in (d or "").lower().split(".") if p]
    if len(parts) <= 2:
        return ".".join(parts)
    if len(parts[-1]) == 2 and parts[-2] in _SECOND_LEVEL:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def canonical_outlet(d):
    """Map any host to its canonical outlet key (reputable list wins)."""
    d = (d or "").lower()
    for r in REPUTABLE_DOMAINS:
        if d == r or d.endswith("." + r):
            return "bbc.com" if r in ("bbc.co.uk", "bbci.co.uk") else r
    return root_domain(d)


def is_reputable(d):
    return canonical_outlet(d) in REPUTABLE_DOMAINS


def is_aggregator(d):
    rd = root_domain(d)
    return rd in AGGREGATORS or (d or "") in AGGREGATORS


# ------------------------------------------------------------ STAGE 1: HUNT
def fetch_rss(category, query, hours):
    url = ("https://news.google.com/rss/search?q=" + quote_plus(query) +
           f"+when%3A{int(hours)}h&hl=en-US&gl=US&ceid=US%3Aen")
    items = []
    try:
        r = http_get(url)
        root = ET.fromstring(r.content)
        now = datetime.now(timezone.utc)
        for it in root.iter("item"):
            try:
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                pub = it.findtext("pubDate") or ""
                try:
                    dt = parsedate_to_datetime(pub)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                except Exception:
                    dt = now
                if not title or not link:
                    continue
                if (now - dt) > timedelta(hours=hours + 0.5):
                    continue
                src_el = it.find("source")
                src_url = src_el.get("url") if src_el is not None else ""
                domain = domain_of(src_url) or domain_of(link)
                if not domain or "news.google.com" in domain:
                    continue
                items.append({"title": htmlmod.unescape(title), "link": link,
                              "domain": domain, "published": dt,
                              "category": category})
            except Exception:
                continue
    except Exception as e:
        log(f"HUNT rss failed [{category}]: {e}")
    return items


def hunt(hours):
    all_items = []
    for cat, q in CATEGORIES.items():
        got = fetch_rss(cat, q, hours)
        log(f"HUNT {cat} ({hours}h): {len(got)} items")
        all_items.extend(got)
        time.sleep(1)
    return all_items


# ---------------------------------------------------------- STAGE 2: VERIFY
HARM_KEYWORDS = {
    "murder", "killed", "killing", "kills", "dead", "death", "dies", "died",
    "fatal", "rape", "sexual assault", "molest", "accuse", "accused",
    "accusation", "allegation", "alleged", "scandal", "suicide", "bombing",
    "bomb blast", "shooting", "shot dead", "stabbing", "stabbed",
    "kidnap", "abduct", "terrorist", "terror attack", "massacre",
    "plane crash", "train crash", "earthquake", "tsunami",
}

_STOP = {"the", "a", "an", "of", "in", "on", "for", "to", "and", "with",
         "from", "after", "over", "says", "say", "new", "amid", "its", "his",
         "her", "was", "are", "has", "have", "will", "that", "this", "than",
         "into", "out", "not", "but", "how", "why", "what", "who"}


def normalize_title(t):
    t = htmlmod.unescape(t).lower()
    t = re.sub(r"\s+[-|–—]\s+[^-|–—]+$", "", t)     # drop trailing " - Source"
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _stem(w):
    return w[:-1] if len(w) > 4 and w.endswith("s") and not w.endswith("ss") else w


def sig_words(t):
    return {_stem(w) for w in normalize_title(t).split()
            if w not in _STOP and len(w) > 2}


def same_story(a, b):
    """True if two significant-word sets describe the same story."""
    if not a or not b:
        return False
    inter = len(a & b)
    if inter < 3 and min(len(a), len(b)) >= 3:
        return False
    return inter / min(len(a), len(b)) >= 0.6 or inter / max(len(a), len(b)) >= 0.45


def cluster_items(items):
    clusters, used = [], [False] * len(items)
    sigs = [sig_words(i["title"]) for i in items]
    for i, a in enumerate(items):
        if used[i]:
            continue
        group, used[i] = [a], True
        for j in range(i + 1, len(items)):
            if not used[j] and same_story(sigs[i], sigs[j]):
                group.append(items[j])
                used[j] = True
        clusters.append(group)
    return clusters


def fingerprint_title(title):
    return hashlib.sha1(normalize_title(title).encode()).hexdigest()[:16]


_HARM_PREFIX = ("kidnap", "abduct", "molest", "massacre", "terroris", "suicid",
                "murder", "stabb", "bombing")


def has_harm(text):
    low = re.sub(r"[^a-z ]", " ", text.lower())
    low = " " + re.sub(r"\s+", " ", low) + " "
    for k in HARM_KEYWORDS:
        if " " in k:
            if f" {k} " in low:
                return k
        elif re.search(rf"\b{k}(s|es|ed|ing)?\b", low):
            return k
    m = re.search(r"\b(" + "|".join(_HARM_PREFIX) + r")\w*", low)
    return m.group(0) if m else None


def is_near_dup(title, known_titles):
    sw = sig_words(title)
    return bool(sw) and any(same_story(sw, sig_words(t)) for t in known_titles if t)


def fetch_verify(query):
    url = ("https://news.google.com/rss/search?q=" + quote_plus(query) +
           "+when%3A24h&hl=en-US&gl=US&ceid=US%3Aen")
    items = []
    try:
        r = http_get(url)
        root = ET.fromstring(r.content)
        for it in root.iter("item"):
            try:
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                if not title or not link:
                    continue
                src_el = it.find("source")
                src_url = src_el.get("url") if src_el is not None else ""
                domain = domain_of(src_url) or domain_of(link)
                if not domain or "news.google.com" in domain:
                    continue
                items.append({"title": htmlmod.unescape(title),
                              "link": link, "domain": domain})
            except Exception:
                continue
    except Exception as e:
        log(f"VERIFY search failed for {query[:40]}: {e}")
    return items


def _keyword_query(title):
    filler = {"news", "today", "report", "reports", "breaking", "live",
              "update", "updates"}
    words = [w for w in sig_words(title) if len(w) > 3 and w not in filler]
    return " ".join(sorted(set(words), key=len, reverse=True)[:5])


def _passes(doms, rep):
    return (len(doms) >= MIN_SOURCES and bool(rep)) or len(rep) >= 2


def verify_candidate(cand, seen_fps, known_titles):
    """cand = {"item": rep item, "cluster": [items]}. Returns (story|None, reason)."""
    item = cand["item"]
    title = item["title"]
    if has_harm(title):
        return None, "harm"
    fp = fingerprint_title(title)
    if fp in seen_fps or is_near_dup(title, known_titles):
        return None, "duplicate"

    csig = sig_words(title)
    outlets, links = {}, {}      # canonical outlet -> link

    def add(domain, link):
        if not domain or is_aggregator(domain):
            return
        o = canonical_outlet(domain)
        outlets.setdefault(o, domain)
        links.setdefault(o, link)

    # (a) outlets already seen in the hunt cluster
    for c in cand["cluster"]:
        if same_story(csig, sig_words(c["title"])):
            add(c["domain"], c["link"])
    add(item["domain"], item["link"])

    def ok():
        return _passes(set(outlets), {o for o in outlets if o in REPUTABLE_DOMAINS})

    # (b) active corroboration: full-headline search, then keyword search
    if not ok():
        for q in (normalize_title(title)[:110], _keyword_query(title)):
            if not q:
                continue
            for r in fetch_verify(q):
                if same_story(csig, sig_words(r["title"])):
                    add(r["domain"], r["link"])
            time.sleep(1)
            if ok():
                break

    if not ok():
        return None, "single_source"
    return {
        "fingerprint": fp,
        "title": title,
        "link": item["link"],
        "category": item["category"],
        "sources": sorted(outlets),
        "source_links": sorted((o, links[o]) for o in outlets)[:6],
        "published": item["published"],
    }, "ok"


def select_candidates(clusters, limit=MAX_CANDIDATES):
    """Best story per cluster, interleaved across categories for variety."""
    ranked = []
    for cl in clusters:
        outlets = {canonical_outlet(c["domain"]) for c in cl
                   if not is_aggregator(c["domain"])}
        if not outlets:
            continue
        reps = [c for c in cl if is_reputable(c["domain"])] or cl
        rep = min(reps, key=lambda c: c["published"])
        n_rep = sum(1 for o in outlets if o in REPUTABLE_DOMAINS)
        ranked.append((min(len(outlets), 6), n_rep, rep["published"],
                       {"item": rep, "cluster": cl}))
    ranked.sort(key=lambda t: (t[0], t[1], t[2]), reverse=True)
    buckets = {}
    for _, _, _, cand in ranked:
        buckets.setdefault(cand["item"]["category"], []).append(cand)
    out = []
    while len(out) < limit and any(buckets.values()):
        for cat in list(buckets):
            if buckets[cat]:
                out.append(buckets[cat].pop(0))
                if len(out) >= limit:
                    break
    return out


# ------------------------------------------- Google News link -> real article
_gn_cache = {}


def decode_gnews_url(url):
    """news.google.com/rss/articles/<id> -> publisher URL (best effort)."""
    if "news.google.com" not in domain_of(url):
        return url
    if url in _gn_cache:
        return _gn_cache[url]
    result = url
    try:
        parts = urlparse(url).path.split("/")
        if len(parts) >= 2 and parts[-2] in ("articles", "read"):
            gid = parts[-1]
            raw = base64.urlsafe_b64decode(gid + "=" * (-len(gid) % 4))
            if raw.startswith(b"\x08\x13\x22"):
                raw = raw[3:]
            if raw.endswith(b"\xd2\x01\x00"):
                raw = raw[:-3]
            if raw:
                ln, off = raw[0], 1
                if ln >= 0x80 and len(raw) > 1:       # 2-byte protobuf varint
                    ln, off = (raw[0] & 0x7F) | (raw[1] << 7), 2
                s = raw[off:off + ln].decode("latin1")
                if s.startswith("http"):
                    result = s
            if result == url:
                result = _decode_gnews_online(gid) or url
    except Exception as e:
        log(f"gnews decode failed: {e}")
    _gn_cache[url] = result
    return result


def _decode_gnews_online(gid):
    sg = ts = None
    for base in (f"https://news.google.com/rss/articles/{gid}",
                 f"https://news.google.com/articles/{gid}"):
        try:
            r = http_get(base, headers=UA_PAGE, timeout=15, retries=1)
        except Exception:
            continue
        m1 = re.search(r'data-n-a-sg="([^"]+)"', r.text)
        m2 = re.search(r'data-n-a-ts="([^"]+)"', r.text)
        if m1 and m2:
            sg, ts = m1.group(1), m2.group(1)
            break
    if not sg:
        return None
    inner = ('["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,'
             'null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],'
             f'"{gid}",{ts},"{sg}"]')
    body = "f.req=" + quote(json.dumps([[["Fbv4je", inner]]]))
    r = requests.post(
        "https://news.google.com/_/DotsSplashUi/data/batchexecute",
        headers={"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                 "User-Agent": UA_PAGE["User-Agent"]},
        data=body, timeout=20)
    r.raise_for_status()
    parsed = json.loads(r.text.split("\n\n")[1])[:-2]
    real = json.loads(parsed[0][2])[1]
    return real if isinstance(real, str) and real.startswith("http") else None


# -------------------------------------------------- article text + images
def fetch_page(url):
    """Returns (final_url, html) or (None, "")."""
    real = decode_gnews_url(url)
    if "news.google.com" in domain_of(real):
        return None, ""
    try:
        r = http_get(real, timeout=20, headers=UA_PAGE, retries=1)
        if "news.google.com" in domain_of(r.url):
            return None, ""
        return r.url, r.text
    except Exception as e:
        log(f"PAGE fetch failed {domain_of(real)}: {e}")
        return None, ""


def _clean_text(s):
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", htmlmod.unescape(s)).strip()


def article_text_from(html, limit=6000):
    if not html:
        return ""
    body = re.sub(r"<(script|style|noscript|nav|footer|header|aside|form|svg)\b.*?</\1>",
                  " ", html, flags=re.S | re.I)
    paras = [_clean_text(p) for p in re.findall(r"<p\b[^>]*>(.*?)</p>", body,
                                                 flags=re.S | re.I)]
    paras = [p for p in paras if len(p) >= 60]
    text = " ".join(paras)
    m = re.search(r'<meta[^>]+(?:property|name)=["\'](?:og:description|description)["\']'
                  r'[^>]+content=["\']([^"\']+)', html, re.I)
    if m:
        text = htmlmod.unescape(m.group(1)).strip() + " " + text
    return text[:limit]


def gather_context(story):
    """Article text + image candidates from the main link, then other sources."""
    text, images = "", []
    urls = [story["link"]] + [u for _, u in story["source_links"]
                              if u != story["link"]]
    for i, u in enumerate(urls[:4]):
        final, html = fetch_page(u)
        if not html:
            continue
        if len(text) < 800:
            text = (text + " " + article_text_from(html)).strip()
        for im in images_from(html, final):
            if im not in images:
                images.append(im)
        if len(text) >= 800 and images:
            break
    return text, images


def _bad_image(u):
    d = urlparse(u).netloc.lower()
    p = u.lower()
    if p.endswith(".svg") or p.endswith(".gif") or "news.google.com" in d:
        return True
    if "google" in d or "gstatic" in d:
        return "logo" in p
    name = p.rsplit("/", 1)[-1]
    return any(k in name for k in ("logo", "sprite", "placeholder", "avatar",
                                   "blank", "pixel", "favicon", "no-image",
                                   "default", "icon", "badge", "banner-ad"))


def images_from(html, base):
    found = []

    def add(u):
        u = (u or "").strip()
        if u and not u.startswith("data:"):
            u = urljoin(base, htmlmod.unescape(u))
            if u.startswith("http") and not _bad_image(u) and u not in found:
                found.append(u)

    for pat in (r'<meta[^>]+property=["\']og:image(?::secure_url)?["\'][^>]+content=["\']([^"\']+)',
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                r'<meta[^>]+name=["\']twitter:image(?::src)?["\'][^>]+content=["\']([^"\']+)'):
        m = re.search(pat, html, re.I)
        if m:
            add(m.group(1))
    for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', html, re.I):
        add(m.group(1))
        if len(found) >= 6:
            break
    return found


def image_ok(url):
    """Real, reachable, non-tiny image? (avoids broken/hot-link-blocked files)"""
    try:
        r = requests.get(url, headers=UA_PAGE, timeout=15, stream=True)
        try:
            if r.status_code != 200:
                return False
            ct = r.headers.get("Content-Type", "").lower()
            if not ct.startswith("image/") or "svg" in ct or "gif" in ct:
                return False
            ln = r.headers.get("Content-Length")
            if ln and ln.isdigit() and int(ln) < 15000:
                return False
            head = next(r.iter_content(2048), b"")
            return len(head) > 0
        finally:
            r.close()
    except Exception:
        return False


def image_queries_for(story):
    filler = {"news", "today", "report", "reports", "after", "amid", "over",
              "under", "with", "from", "will"}
    words = [w for w in sig_words(story["title"]) if len(w) > 3 and w not in filler]
    by_len = sorted(set(words), key=len, reverse=True)
    queries = [f"intitle:{a} intitle:{b}" for a, b in zip(by_len[:3], by_len[1:4])]
    queries.append(story["category"])
    return queries


def search_image(query):
    """Wikimedia Commons (free, no key). Watermark-categorised files skipped."""
    try:
        url = ("https://commons.wikimedia.org/w/api.php?action=query"
               "&format=json&generator=search&gsrsearch=" + quote_plus(query) +
               "&gsrlimit=12&gsrnamespace=6&prop=imageinfo|categories"
               "&iiprop=url|size&iiurlwidth=1280&cllimit=50")
        pages = http_get(url, timeout=15).json().get("query", {}).get("pages", {})
        cands = []
        for p in pages.values():
            cats = [c.get("title", "") for c in p.get("categories", [])]
            if any("watermark" in c.lower() for c in cats):
                continue
            info = (p.get("imageinfo") or [{}])[0]
            u = info.get("thumburl") or info.get("url") or ""
            w, h = info.get("width") or 0, info.get("height") or 0
            if not u or _bad_image(u) or w < 600:
                continue
            cands.append((w + (500 if h and w / h >= 1.2 else 0), u))
        cands.sort(reverse=True)
        return [u for _, u in cands]
    except Exception as e:
        log(f"IMAGE search failed: {e}")
        return []


def pick_image(story, article_images, seen_images):
    for u in article_images:
        if u not in seen_images and image_ok(u):
            return u
    for q in image_queries_for(story):
        log(f"PHOTO searching: {q}")
        for u in search_image(q):
            if u not in seen_images and image_ok(u):
                return u
    return None


# ---------------------------------------------------------- STAGE 4: REWRITE
REWRITE_PROMPT = """Rewrite this news story as a FULL, detailed news article in English -- NOT a short summary.

STORY TITLE: {title}
REPORTED BY: {sources}
ARTICLE TEXT:
{article}

RULES:
- Use ONLY facts found in the article text above. Never invent facts, numbers, names or quotes.
- Write in your own words; do not copy sentences from the source.
- Headline: engaging and hook-y, under 90 characters, main keywords up front, accurate. Do NOT copy the original headline.
- Opening paragraph must HOOK the reader AND work as an SEO snippet: most important angle first, key names/places, ~150 characters of substance before expanding.
- Write 6 to 10 paragraphs. Cover who, what, when, where, why, and what happens next (only if the text supports it).
- Format with HTML: <strong> for key facts/names/numbers/dates, <h2> subheadings, <ul><li> key-point lists, <blockquote> only for real quotes from the text.
- Do NOT include <h1>, <script>, <style>, <iframe> or images.
- Reply ONLY as JSON: {{"headline": "...", "body": "<p>...</p><h2>...</h2>..."}}
"""


def _find_json_object(text):
    start = text.find("{")
    if start < 0:
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        elif ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start:i + 1]
    return None


def _headline_ok(h):
    return bool(h) and len(h) >= 8 and h[0] not in '{"' \
        and not re.match(r'^headline\s*["\':]', h, re.I)


def _harvest_headline(text):
    for line in text.splitlines():
        s = line.strip().strip("#").strip()
        if not s or s.startswith("```") or s.startswith("{") \
                or re.match(r'^"(headline|body)"\s*:', s):
            continue
        s = re.sub(r"<[^>]+>", "", s).strip()
        if len(s) >= 8:
            return s[:90]
    return ""


def _harvest_body(text, json_obj):
    rest = text.replace(json_obj, " ", 1) if json_obj else text
    lines = [l for l in rest.splitlines()
             if l.strip() and not l.strip().startswith("```")
             and not re.match(r'^\s*"(headline|body)"\s*:', l)]
    rest = "\n".join(lines).strip()
    if re.search(r"<(p|h2|h3|ul|ol|blockquote|strong)[ >]", rest, re.I):
        return rest
    out = []
    for line in lines:
        s = line.strip()
        if not s:
            continue
        s = re.sub(r"^#{1,3}\s*", "", s)
        s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
        if len(s) < 90 and s.endswith(":"):
            out.append(f"<h2>{htmlmod.escape(s.rstrip(':'))}</h2>")
        else:
            out.append(f"<p>{s if '<' in s else htmlmod.escape(s)}</p>")
    return "\n".join(out[:14])


def sanitize_html(body):
    body = re.sub(r"<(script|style|iframe|object|embed)\b.*?</\1>", "", body,
                  flags=re.S | re.I)
    body = re.sub(r"<(script|style|iframe|object|embed)\b[^>]*/?>", "", body, flags=re.I)
    body = re.sub(r"\son\w+\s*=\s*(\"[^\"]*\"|'[^']*')", "", body, flags=re.I)
    body = re.sub(r"javascript:", "", body, flags=re.I)
    body = re.sub(r"</?h1\b[^>]*>", lambda m: "<h2>" if not m.group(0).startswith("</")
                  else "</h2>", body, flags=re.I)
    body = re.sub(r"<img\b[^>]*>", "", body, flags=re.I)   # our photo goes on top
    return body.strip()


def clean_headline(h, fallback):
    h = re.sub(r"<[^>]+>", "", htmlmod.unescape(h or "")).strip().strip('"').strip()
    if len(h) > 100:
        h = h[:100].rsplit(" ", 1)[0]
    return h if len(h) >= 8 else fallback[:90]


def _parse_rewrite(text, fallback_title):
    headline, body = "", ""
    obj = _find_json_object(text)
    if obj:
        try:
            data = json.loads(obj, strict=False)
        except Exception:
            data = None
        if isinstance(data, dict):
            headline = str(data.get("headline") or "").strip()
            body = str(data.get("body") or "").strip()
    if not body:
        body = _harvest_body(text, obj)
    if not _headline_ok(headline):
        headline = _harvest_headline(text) or fallback_title[:90]
    return clean_headline(headline, fallback_title), sanitize_html(body)


def body_ok(body):
    """Reject truncated / empty / one-liner AI output before it can be posted."""
    txt = _clean_text(body)
    return len(txt) >= 700 and body.lower().count("<p") >= 3


class AIError(Exception):
    def __init__(self, kind, msg=""):
        super().__init__(f"{kind}: {msg}")
        self.kind = kind     # quota | auth | model | server | blocked | other


def _prompt(story):
    return REWRITE_PROMPT.format(title=story["title"],
                                 sources=", ".join(story["sources"]),
                                 article=story["article_text"])


def gemini_rewrite(key, model, story):
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model}:generateContent")
    payload = {"contents": [{"parts": [{"text": _prompt(story)}]}],
               "generationConfig": {"temperature": 0.7, "maxOutputTokens": 8192,
                                    "responseMimeType": "application/json"}}
    try:
        r = requests.post(url, json=payload, timeout=90,
                          headers={"x-goog-api-key": key})
    except requests.RequestException as e:
        raise AIError("server", str(e)[:120])
    if r.status_code == 429:
        raise AIError("quota", "429")
    if r.status_code == 404:
        raise AIError("model", f"{model} not found")
    if r.status_code in (400, 401, 403):
        msg = r.text[:200]
        if "API key" in msg or r.status_code in (401, 403):
            raise AIError("auth", msg)
        raise AIError("other", msg)
    if r.status_code >= 500:
        raise AIError("server", str(r.status_code))
    data = r.json()
    cands = data.get("candidates") or []
    if not cands:
        raise AIError("blocked", str(data.get("promptFeedback", ""))[:150])
    parts = (cands[0].get("content") or {}).get("parts") or []
    text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
    if not text.strip():
        raise AIError("other", f"empty ({cands[0].get('finishReason')})")
    return _parse_rewrite(text, story["title"])


def groq_rewrite(story):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}",
               "Content-Type": "application/json"}
    last = None
    for model in [GROQ_MODEL] + [m for m in GROQ_FALLBACK_MODELS if m != GROQ_MODEL]:
        payload = {"model": model, "temperature": 0.7, "max_tokens": 4096,
                   "response_format": {"type": "json_object"},
                   "messages": [
                       {"role": "system",
                        "content": "You are a news rewrite assistant. Reply only JSON."},
                       {"role": "user", "content": _prompt(story)}]}
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=90)
            if r.status_code == 400 and "model" not in r.text.lower():
                payload.pop("response_format")         # json mode unsupported
                r = requests.post(url, headers=headers, json=payload, timeout=90)
            if r.status_code in (400, 404):            # decommissioned / unknown model
                last = f"{model}: {r.text[:100]}"
                log(f"GROQ model {model} unavailable -> next")
                continue
            if r.status_code == 429:
                last = f"{model}: 429"
                continue
            r.raise_for_status()
            h, b = _parse_rewrite(r.json()["choices"][0]["message"]["content"],
                                  story["title"])
            if not body_ok(b):
                last = f"{model}: output too short"
                continue
            return h, b, model
        except requests.RequestException as e:
            last = f"{model}: {e}"
    raise AIError("other", f"groq failed ({last})")


_bad_keys, _dead_models = set(), set()
_key_cursor = {"i": None}
_live_models = {"list": None}        # filled by discover_models()


def discover_models():
    """Ask the API which Gemini text models are live right now."""
    for key in GEMINI_KEYS:
        try:
            r = requests.get(
                "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1000",
                headers={"x-goog-api-key": key}, timeout=20)
            if r.status_code != 200:
                continue
            live = []
            for m in r.json().get("models", []):
                name = m.get("name", "").replace("models/", "")
                if (name.startswith("gemini-") and "flash" in name
                        and "generateContent" in m.get("supportedGenerationMethods", [])
                        and not any(x in name for x in ("tts", "live", "image", "audio",
                                                         "embed", "robotics", "omni",
                                                         "computer", "preview"))):
                    live.append(name)
            if live:
                _live_models["list"] = live
                log(f"MODELS live on API: {', '.join(sorted(live))}")
                return
        except Exception as e:
            log(f"model discovery failed: {e}")
    log("MODELS: discovery unavailable -- using built-in order")


def _models():
    pref = [GEMINI_MODEL] + [m for m in GEMINI_FALLBACK_MODELS if m != GEMINI_MODEL]
    live = _live_models["list"]
    if live:
        ordered = [m for m in pref if m in live]
        # any other live flash model (newest names sort last) as extra backup
        ordered += sorted((m for m in live if m not in ordered), reverse=True)
    else:
        ordered = pref
    return [m for m in ordered if m not in _dead_models]


def rewrite_story(story, key_status):
    """Gemini (round-robin keys + model fallback) -> Groq. Returns (headline, body, via)."""
    n = len(GEMINI_KEYS)
    if _key_cursor["i"] is None:
        _key_cursor["i"] = random.randrange(n) if n else 0
    deadline = time.time() + 150
    last_err = None
    for step in range(n):
        if time.time() > deadline:
            break
        i = (_key_cursor["i"] + step) % n
        key = GEMINI_KEYS[i]
        if key in _bad_keys:
            continue
        for model in _models():
            try:
                h, b = gemini_rewrite(key, model, story)
                if not body_ok(b):
                    raise AIError("other", "output too short/truncated")
                key_status["ok"][i] = True
                _key_cursor["i"] = (i + 1) % n          # next story -> next key
                return h, b, f"gemini_key{i+1}:{model}"
            except AIError as e:
                last_err = e
                if e.kind == "model":
                    _dead_models.add(model)
                    log(f"REWRITE model {model} unavailable -> trying fallback")
                    continue                              # same key, next model
                log(f"REWRITE gemini key{i+1} {model}: {e}")
                if e.kind in ("quota", "auth"):
                    _bad_keys.add(key)
                    key_status["ok"][i] = False
                break                                     # next key
            except Exception as e:
                last_err = e
                log(f"REWRITE gemini key{i+1} error: {e}")
                break
    if GROQ_API_KEY:
        try:
            h, b, gm = groq_rewrite(story)
            key_status["groq"] = "ok"
            return h, b, f"groq:{gm}"
        except Exception as e:
            last_err = e
            key_status["groq"] = "fail"
            log(f"REWRITE groq failed: {e}")
    raise RuntimeError(f"all rewrite backends failed: {last_err}")


# ----------------------------------------------------------- STAGE 5: PUBLISH
_token = {"value": None, "exp": 0}


def blogger_token(force=False):
    if not force and _token["value"] and time.time() < _token["exp"] - 60:
        return _token["value"]
    last = None
    for attempt in range(3):
        try:
            r = requests.post("https://oauth2.googleapis.com/token", data={
                "client_id": BG_CLIENT_ID, "client_secret": BG_CLIENT_SECRET,
                "refresh_token": BG_REFRESH_TOKEN, "grant_type": "refresh_token"},
                timeout=30)
        except requests.RequestException as e:
            last = e
            time.sleep(3 * (attempt + 1))
            continue
        if r.status_code == 200:
            j = r.json()
            _token["value"] = j["access_token"]
            _token["exp"] = time.time() + int(j.get("expires_in", 3000))
            return _token["value"]
        try:
            err = r.json()
        except Exception:
            err = {}
        code = err.get("error", "")
        if code == "invalid_grant":
            raise FatalError(
                "Blogger refresh token is expired or revoked (invalid_grant). "
                "If your Google OAuth consent screen is in 'Testing' mode, tokens "
                "die after 7 days: publish the app to 'In production' and create "
                "a NEW refresh token, then update BLOGGER_REFRESH_TOKEN.")
        if code in ("invalid_client", "unauthorized_client", "invalid_request"):
            raise FatalError(f"Blogger OAuth config error ({code}): "
                             f"{err.get('error_description', '')[:150]} -- check "
                             "BLOGGER_CLIENT_ID / BLOGGER_CLIENT_SECRET.")
        last = RuntimeError(f"token HTTP {r.status_code}: {r.text[:150]}")
        time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"could not get Blogger token: {last}")


def build_content(headline, body_html, image_url, source_links):
    parts = []
    if image_url:
        parts.append(
            '<div class="separator" style="clear:both;text-align:center;">'
            f'<img src="{htmlmod.escape(image_url, quote=True)}" '
            f'alt="{htmlmod.escape(headline, quote=True)}" referrerpolicy="no-referrer" '
            'style="width:100%;max-width:100%;height:auto;border-radius:10px;'
            'margin-bottom:12px;" /></div>')
    parts.append(body_html)
    if source_links:
        links = ", ".join(
            f'<a href="{htmlmod.escape(u, quote=True)}" rel="nofollow noopener" '
            f'target="_blank">{htmlmod.escape(d)}</a>' for d, u in source_links)
        parts.append(f"<hr/><p><em>Sources: {links}</em></p>")
    return "\n".join(parts)


def _blogger_reason(r):
    try:
        e = r.json().get("error", {})
        reasons = [x.get("reason", "") for x in e.get("errors", [])]
        return e.get("message", "")[:200], reasons
    except Exception:
        return r.text[:200], []


def _recent_blogger_titles():
    try:
        r = requests.get(
            f"https://www.googleapis.com/blogger/v3/blogs/{BLOG_ID}/posts"
            "?maxResults=10&fields=items(title,url)&status=live",
            headers={"Authorization": f"Bearer {blogger_token()}"}, timeout=20)
        if r.status_code == 200:
            return {i["title"]: i["url"] for i in r.json().get("items", [])}
    except Exception:
        pass
    return {}


def publish_post(headline, body_html, image_url, source_links, label):
    payload = {"title": headline,
               "content": build_content(headline, body_html, image_url, source_links),
               "labels": [label, "WTN"]}
    if DRY_RUN:
        log(f"DRY-RUN would publish: {headline}")
        return "https://example.invalid/dry-run"
    endpoint = (f"https://www.googleapis.com/blogger/v3/blogs/{BLOG_ID}/posts"
                "?isDraft=false&fetchImages=false")
    refreshed, ambiguous = False, False
    last = None
    for attempt in range(4):
        if ambiguous:                      # timeout/5xx may still have created it
            existing = _recent_blogger_titles().get(headline)
            if existing:
                log("PUBLISH: post already exists (earlier attempt succeeded)")
                return existing
        try:
            r = requests.post(endpoint, json=payload, timeout=60, headers={
                "Authorization": f"Bearer {blogger_token()}",
                "Content-Type": "application/json"})
        except requests.RequestException as e:
            last, ambiguous = f"network: {e}", True
            time.sleep(4 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json().get("url", "")
        msg, reasons = _blogger_reason(r)
        if r.status_code == 401 and not refreshed:
            refreshed = True
            blogger_token(force=True)
            continue
        if r.status_code == 404:
            raise FatalError(f"Blogger blog not found (404): check BLOGGER_BLOG_ID. {msg}")
        if r.status_code == 403:
            if any(x in ("rateLimitExceeded", "userRateLimitExceeded",
                         "quotaExceeded", "dailyLimitExceeded") for x in reasons):
                raise BloggerLimit(f"Blogger quota/rate limit: {msg}")
            raise FatalError(f"Blogger refused (403): {msg} -- is the Google "
                             "account the blog's author/admin, and is the Blogger "
                             "API enabled for the OAuth project?")
        if r.status_code in (429, 500, 502, 503, 504):
            last, ambiguous = f"HTTP {r.status_code}: {msg}", True
            time.sleep(5 * (attempt + 1))
            continue
        raise RuntimeError(f"Blogger HTTP {r.status_code}: {msg}")
    if last and "429" in last:
        raise BloggerLimit(last)
    raise RuntimeError(f"Blogger publish failed after retries: {last}")


# ------------------------------------------------------------------- state
def _blank_state():
    return {"seen_fps": [], "seen_images": [], "counters": {}, "total_posts": 0,
            "recent_posts": [], "orig_titles": [], "skips": {}}


def load_state():
    if not os.path.exists(STATE_FILE):
        return _blank_state()
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        for k, v in _blank_state().items():
            s.setdefault(k, v)
        return s
    except Exception as e:
        log(f"WARNING: {STATE_FILE} unreadable ({e}); backing up and starting fresh")
        try:
            os.replace(STATE_FILE, STATE_FILE + ".bak")
        except Exception:
            pass
        return _blank_state()


def _atomic_write(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def save_state(state, seen, seen_images):
    state["seen_fps"] = sorted(set(state["seen_fps"]) | seen)[-2000:]
    state["seen_images"] = sorted(set(state["seen_images"]) | seen_images)[-500:]
    state["recent_posts"] = state["recent_posts"][:20]
    state["orig_titles"] = state["orig_titles"][-300:]
    _atomic_write(STATE_FILE, state)


def build_status(started, state, vstats, key_status, errors, today):
    hist = []
    for i in range(6, -1, -1):
        d = (started - timedelta(days=i)).strftime("%Y-%m-%d")
        hist.append({"date": d, "posts": state["counters"].get(d, {}).get("posts", 0)})
    rr = vstats.get("rejected_reasons", {})
    okflags = key_status["ok"]
    return {
        "last_run": started.strftime("%Y-%m-%d %H:%M UTC"),
        "next_run": (started + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M UTC"),
        "status": "error" if errors else "ok",
        "runs_today": state["counters"].get(today, {}).get("runs", 0),
        "posts_today": state["counters"].get(today, {}).get("posts", 0),
        "posts_total": state.get("total_posts", 0),
        "posts_this_run": vstats.get("published", 0),
        "last_posts": state.get("recent_posts", [])[:8],
        "verifier": {
            "checked": vstats.get("checked", 0),
            "passed": vstats.get("passed", 0),
            "rejected": vstats.get("rejected", 0),
            "rejected_reasons": {"single_source": rr.get("single_source", 0),
                                 "harm": rr.get("harm", 0),
                                 "duplicate": rr.get("duplicate", 0)},
        },
        "keys": {
            "gemini_keys_total": len(okflags),
            # untested keys count as healthy; only keys that FAILED count as bad
            "gemini_keys_ok": sum(1 for v in okflags if v is not False),
            "groq": key_status.get("groq", "unused"),
        },
        "errors": errors[-5:],
        "history": hist,
    }


# ---------------------------------------------------------- one story, end to end
def process_story(story, state, seen, seen_images, key_status, vstats):
    """Returns 'published' | 'skipped' | 'failed'. May raise FatalError/BloggerLimit."""
    fp = story["fingerprint"]
    log(f"STORY: {story['title'][:70]}")

    # PHOTO + TEXT first: cheap checks before spending AI quota
    text, article_images = gather_context(story)
    if len(text) < 400:
        return _skip(state, seen, fp, "article text unavailable")
    story["article_text"] = text
    image = pick_image(story, article_images, seen_images)
    if not image:
        return _skip(state, seen, fp, "no usable photo")

    headline, body, via = rewrite_story(story, key_status)

    url = publish_post(headline, body, image, story["source_links"], story["category"])
    log(f"PUBLISHED [{via}] {headline} -> {url}")
    seen.add(fp)
    seen_images.add(image)
    state["orig_titles"].append(story["title"])
    state["recent_posts"].insert(0, {
        "title": headline, "url": url,
        "time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "label": story["category"], "image": image})
    state["total_posts"] += 1
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    c = state["counters"].setdefault(today, {"runs": 0, "posts": 0})
    c["posts"] += 1
    vstats["published"] = vstats.get("published", 0) + 1
    save_state(state, seen, seen_images)        # persisted after EVERY post
    return "published"


def _skip(state, seen, fp, why):
    n = state["skips"].get(fp, 0) + 1
    state["skips"][fp] = n
    log(f"SKIP ({why}) attempt {n}")
    if n >= 3:                      # give up on it for good
        seen.add(fp)
    if len(state["skips"]) > 500:
        state["skips"] = dict(list(state["skips"].items())[-250:])
    return "skipped"


# -------------------------------------------------------------------- main
def run_pipeline(state, seen, seen_images, key_status, vstats, errors):
    t0 = time.time()
    tried = set()
    windows = [HUNT_HOURS]
    if HUNT_FALLBACK_HOURS > HUNT_HOURS:
        windows.append(HUNT_FALLBACK_HOURS)

    for wi, hours in enumerate(windows):
        if vstats["published"] >= MAX_POSTS or time.time() - t0 > RUN_BUDGET_SEC:
            break
        if wi > 0:
            if vstats["published"] >= MIN_POSTS:
                break
            log(f"Only {vstats['published']} published -- widening hunt to {hours}h")
        log(f"STAGE 1 HUNT ({hours}h)")
        items = hunt(hours)
        known_titles = state["orig_titles"] + [p.get("title", "") for p in state["recent_posts"]]
        cands = select_candidates(cluster_items(items))
        log(f"HUNT: {len(items)} items -> {len(cands)} candidates")

        for cand in cands:
            if vstats["published"] >= MAX_POSTS:
                break
            if time.time() - t0 > RUN_BUDGET_SEC:
                log("time budget reached -- stopping")
                return
            fp = fingerprint_title(cand["item"]["title"])
            if fp in tried:
                continue
            tried.add(fp)
            vstats["checked"] += 1
            story, reason = verify_candidate(cand, seen, known_titles)
            if not story:
                vstats["rejected"] += 1
                rr = vstats["rejected_reasons"]
                rr[reason] = rr.get(reason, 0) + 1
                continue
            vstats["passed"] += 1
            log(f"VERIFIED [{len(story['sources'])} outlets] {story['title'][:60]}")
            try:
                process_story(story, state, seen, seen_images, key_status, vstats)
            except (FatalError, BloggerLimit):
                raise
            except Exception as e:
                errors.append(f"story failed: {story['title'][:50]}: {str(e)[:160]}")
                log(f"ERROR story: {e}")
            time.sleep(3)


def main():
    started = datetime.now(timezone.utc)
    today = started.strftime("%Y-%m-%d")
    log("WTN AutoPost v2 run started")
    errors, fatal = [], False
    key_status = {"ok": [None] * len(GEMINI_KEYS), "groq": "unused"}
    vstats = {"checked": 0, "passed": 0, "rejected": 0, "published": 0,
              "rejected_reasons": {}}

    for name in ["BLOGGER_BLOG_ID", "BLOGGER_CLIENT_ID",
                 "BLOGGER_CLIENT_SECRET", "BLOGGER_REFRESH_TOKEN"]:
        if not os.environ.get(name, "").strip():
            errors.append(f"Missing secret: {name}")
    if not GEMINI_KEYS and not GROQ_API_KEY:
        errors.append("No AI key: set GEMINI_API_KEYS and/or GROQ_API_KEY")
    fatal = bool(errors)

    state = load_state()
    seen = set(state["seen_fps"])
    seen_images = set(state["seen_images"])

    if not fatal:
        try:
            if not DRY_RUN:
                blogger_token()          # fail fast, with a clear message
                log("Blogger auth OK")
            if GEMINI_KEYS:
                discover_models()
            run_pipeline(state, seen, seen_images, key_status, vstats, errors)
        except FatalError as e:
            errors.append(f"FATAL: {e}")
            fatal = True
            log(f"FATAL: {e}")
        except BloggerLimit as e:
            errors.append(str(e))
            log(f"STOP: {e}")
        except Exception as e:
            errors.append(f"pipeline: {str(e)[:200]}")
            log(f"ERROR pipeline: {e}")

    c = state["counters"].setdefault(today, {"runs": 0, "posts": 0})
    c["runs"] += 1
    save_state(state, seen, seen_images)
    _atomic_write(STATUS_FILE, build_status(started, state, vstats, key_status,
                                            errors, today))
    log(f"done: {vstats['published']} published, {len(errors)} errors, "
        f"verified {vstats['passed']}/{vstats['checked']}")
    # Non-zero ONLY when retrying can't help (bad secrets / dead token).
    return 1 if fatal else 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
WTN AutoPost -- World Trend Network autonomous news publisher.

Runs on GitHub Actions (free tier) every 30 minutes. Pipeline:

  1. HUNT    - Google News RSS across categories (World, Technology, Sports,
               Entertainment, Business, Health, Science). Free, no key.
  2. VERIFY  - truth check. A story is published ONLY if >= MIN_SOURCES
               distinct reputable domains report it with consistent facts,
               it has no harm-category keywords, and it was not posted before.
               Rejected stories are skipped (no drafts -- direct post or skip).
  3. REWRITE - Google Gemini with MULTI-KEY rotation: keys are tried in order,
               on 429/5xx the next key is used. If every Gemini key fails,
               Groq (llama-3.3-70b-versatile) is the backup. Never copy-paste.
  4. IMAGE   - collect the article's own high-quality image (og:image).
               No logo/watermark is ever added. No image found -> post as text.
  5. PUBLISH - Blogger API v3, DIRECT live post (isDraft=false).

All secrets come from environment variables (GitHub Secrets). All knobs have
safe defaults so the workflow runs even if optional repo Variables are unset.
State (seen.json) and dashboard data (status.json) are committed back by the
workflow after every run.
"""

import os
import re
import json
import time
import hashlib
import html as htmlmod
from collections import Counter
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, quote_plus, urljoin
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

import requests

UA = {"User-Agent": "Mozilla/5.0 (Linux; Android 10) WTN-AutoPost/1.0"}

# ------------------------------------------------------------------ config
GEMINI_KEYS = [k.strip() for k in
               os.environ.get("GEMINI_API_KEYS",
                              os.environ.get("GEMINI_API_KEY", "")).split(",")
               if k.strip()]
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")

BLOG_ID = os.environ.get("BLOGGER_BLOG_ID", "")
BG_CLIENT_ID = os.environ.get("BLOGGER_CLIENT_ID", "")
BG_CLIENT_SECRET = os.environ.get("BLOGGER_CLIENT_SECRET", "")
BG_REFRESH_TOKEN = os.environ.get("BLOGGER_REFRESH_TOKEN", "")

MAX_POSTS = int(os.environ.get("MAX_POSTS_PER_RUN", "3"))
MIN_SOURCES = int(os.environ.get("MIN_SOURCES", "3"))
HUNT_HOURS = float(os.environ.get("HUNT_HOURS", "2"))
BLOG_LANGUAGE = os.environ.get("BLOG_LANGUAGE", "en")

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

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


def log(msg):
    print(f"[wtn] {datetime.now(timezone.utc):%H:%M:%S} {msg}", flush=True)


def http_get(url, timeout=20):
    r = requests.get(url, headers=UA, timeout=timeout)
    r.raise_for_status()
    return r


# ------------------------------------------------------------ STAGE 1: HUNT
REPUTABLE_DOMAINS = {
    # wire / international
    "reuters.com", "apnews.com", "bbc.com", "bbc.co.uk", "cnn.com", "nbcnews.com",
    "abcnews.go.com", "cbsnews.com", "npr.org", "pbs.org", "theguardian.com",
    "nytimes.com", "washingtonpost.com", "wsj.com", "usatoday.com", "latimes.com",
    "chicagotribune.com", "nypost.com", "thehill.com", "politico.com",
    "dw.com", "france24.com", "aljazeera.com", "euronews.com", "sky.com",
    "skynews.com.au", "abc.net.au", "cbc.ca", "ctvnews.ca", "theglobeandmail.com",
    "straitstimes.com", "channelnewsasia.com", "scmp.com", "japantimes.co.jp",
    "nhk.or.jp", "koreaherald.com", "dawn.com", "tribune.com.pk", "thehindu.com",
    "indianexpress.com", "timesofindia.indiatimes.com", "hindustantimes.com",
    "ndtv.com", "al-monitor.com", "middleeasteye.net",
    # business
    "bloomberg.com", "cnbc.com", "forbes.com", "fortune.com", "economist.com",
    "ft.com", "marketwatch.com", "barrons.com", "investing.com", "fool.com",
    # tech
    "techcrunch.com", "theverge.com", "wired.com", "arstechnica.com",
    "engadget.com", "gizmodo.com", "zdnet.com", "cnet.com", "mashable.com",
    "venturebeat.com", "thenextweb.com", "9to5mac.com", "9to5google.com",
    # sports
    "espn.com", "skysports.com", "bbc.com", "foxsports.com", "cbssports.com",
    "nbcsports.com", "si.com", "theathletic.com", "espncricinfo.com",
    "cricbuzz.com", "goal.com", "mlb.com", "nba.com", "nfl.com",
    # entertainment
    "variety.com", "hollywoodreporter.com", "deadline.com", "billboard.com",
    "rollingstone.com", "ew.com", "people.com", "etonline.com", "vulture.com",
    "indiewire.com", "screenrant.com", "collider.com",
    # health / science
    "who.int", "nih.gov", "cdc.gov", "webmd.com", "medicalxpress.com",
    "sciencedaily.com", "newscientist.com", "nature.com", "science.org",
    "space.com", "livescience.com", "healthline.com", "mayoclinic.org",
}


def domain_of(url):
    try:
        d = urlparse(url).netloc.lower()
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return ""


def fetch_rss(category, query):
    """Google News RSS for a category query, limited to the hunt window."""
    url = ("https://news.google.com/rss/search?q=" + quote_plus(query) +
           f"+when%3A{int(HUNT_HOURS)}h&hl=en-US&gl=US&ceid=US%3Aen")
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
                if (now - dt) > timedelta(hours=HUNT_HOURS + 0.5):
                    continue
                # Google News wraps links in its own redirect URL; the real
                # publisher domain lives in the <source url="..."> element.
                src_el = it.find("source")
                src_url = src_el.get("url") if src_el is not None else ""
                domain = domain_of(src_url) or domain_of(link)
                items.append({
                    "title": htmlmod.unescape(title),
                    "link": link,
                    "domain": domain,
                    "published": dt,
                    "category": category,
                })
            except Exception:
                continue
    except Exception as e:
        log(f"HUNT rss failed [{category}]: {e}")
    return items


def hunt():
    all_items = []
    for cat, q in CATEGORIES.items():
        got = fetch_rss(cat, q)
        log(f"HUNT {cat}: {len(got)} items")
        all_items.extend(got)
        time.sleep(1)  # be gentle with Google News
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
         "from", "after", "over", "says", "say", "new", "amid"}


def normalize_title(t):
    t = htmlmod.unescape(t).lower()
    t = re.sub(r"\s*[-|–—]\s*[^-|–—]+$", "", t)   # drop trailing source name
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def sig_words(t):
    return {w for w in normalize_title(t).split() if w not in _STOP and len(w) > 2}


def sig_overlap(a, b):
    """0..1 title similarity between two sig-word sets."""
    if not a or not b:
        return 0.0
    return len(a & b) / max(len(a), len(b))


def cluster_items(items):
    """Group items that describe the same story."""
    clusters, used = [], [False] * len(items)
    for i, a in enumerate(items):
        if used[i]:
            continue
        group, used[i] = [a], True
        sa = sig_words(a["title"])
        for j in range(i + 1, len(items)):
            if used[j]:
                continue
            sb = sig_words(items[j]["title"])
            if not sa or not sb:
                continue
            overlap = len(sa & sb) / max(len(sa), len(sb))
            if overlap >= 0.55:
                group.append(items[j])
                used[j] = True
        clusters.append(group)
    return clusters


def fingerprint(cluster):
    key = normalize_title(sorted((c["title"] for c in cluster),
                                 key=len)[0])
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def has_harm(text):
    low = text.lower()
    return next((k for k in HARM_KEYWORDS if k in low), None)


def _is_near_dup(title, recent_titles, thresh=0.5):
    """Same story, reworded headline -> treat as duplicate."""
    sw = sig_words(title)
    if not sw:
        return False
    for t in recent_titles:
        pw = sig_words(t)
        if not pw:
            continue
        if len(sw & pw) / max(len(sw), len(pw)) >= thresh:
            return True
    return False


def fetch_verify(query):
    """Corroboration search: is this story reported by other outlets?

    Uses a wider 24h window -- the story itself is already known fresh;
    here we only need independent outlets reporting the SAME story.
    """
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


def _verify_query(title):
    """Focused keyword query for corroboration search."""
    filler = {"news", "today", "report", "reports", "says", "after", "amid",
              "over", "under", "with", "from", "will", "breaking", "live",
              "update", "updates"}
    words = [w for w in sig_words(title)
             if len(w) > 3 and w not in filler]
    by_len = sorted(set(words), key=len, reverse=True)
    return " ".join(by_len[:5])


def verify_candidate(item, seen_fps, recent_titles):
    """STAGE 2 -- VERIFIER. Actively corroborate ONE story.

    Searches Google News for the story's key terms, keeps only results
    really about the SAME story (title overlap), and counts independent
    outlets. Pass rule: 3+ outlets incl. 1 reputable, OR 2 reputable.
    Returns (story_dict | None, reason).
    """
    title = item["title"]
    if has_harm(title):
        return None, "harm"
    fp = fingerprint([item])
    if fp in seen_fps or _is_near_dup(title, recent_titles):
        return None, "duplicate"

    query = _verify_query(title)
    if not query:
        return None, "no_query"
    results = fetch_verify(query)
    time.sleep(1)  # politeness between verification searches

    csig = sig_words(title)
    doms, rep_doms, src_links, seen_d = set(), set(), set(), set()
    for r in results:
        # strict: same keywords but different story must NOT corroborate
        if sig_overlap(csig, sig_words(r["title"])) < 0.45:
            continue
        d = r["domain"]
        if d not in seen_d:
            seen_d.add(d)
            src_links.add((d, r["link"]))
        doms.add(d)
        if d in REPUTABLE_DOMAINS:
            rep_doms.add(d)
    # the candidate's own outlet counts too
    doms.add(item["domain"])
    if item["domain"] in REPUTABLE_DOMAINS:
        rep_doms.add(item["domain"])
    src_links.add((item["domain"], item["link"]))

    if not ((len(doms) >= MIN_SOURCES and rep_doms) or len(rep_doms) >= 2):
        return None, "single_source"
    return {
        "fingerprint": fp,
        "title": title,
        "link": item["link"],
        "category": item["category"],
        "sources": sorted(doms),
        "source_links": sorted(src_links)[:6],
        "published": item["published"],
    }, "ok"


def select_candidates(clusters, limit=25):
    """Rank story clusters, most newsworthy first.

    Reputable-outlet stories first (easiest to corroborate), then newest.
    """
    cands = []
    for cl in clusters:
        rep_item = min(cl, key=lambda c: c["published"])
        rep = 1 if rep_item["domain"] in REPUTABLE_DOMAINS else 0
        cands.append((rep, rep_item["published"], rep_item))
    cands.sort(key=lambda t: (t[0], t[1]), reverse=True)
    return [c for _, _, c in cands[:limit]]


# ---------------------------------------------------------- STAGE 3: REWRITE
REWRITE_PROMPT = """Rewrite this news story as a FULL, detailed news article in English -- NOT a short summary.

STORY TITLE: {title}
REPORTED BY: {sources}
ARTICLE TEXT:
{article}

RULES:
- Headline: engaging and hook-y, under 90 characters, with the main keywords up front. Must stay accurate -- never invent facts. Do NOT copy the original headline.
- Opening paragraph must HOOK the reader AND work as an SEO snippet: lead with the most surprising or important angle, include the key names/places, keep it tight (~150 characters of substance before expanding).
- Write 6 to 10 paragraphs with rich detail drawn from the article text above. Cover who, what, when, where, why, and what happens next.
- Format the article beautifully with HTML:
  * <strong> around key facts, names, numbers, and dates
  * <h2> subheadings (keyword-rich) to break the article into sections
  * <ul><li> bullet lists for key points or takeaways
  * <blockquote> for direct quotes (only real quotes from the text above)
- Do not invent quotes, numbers, or facts not present above.
- Reply ONLY as JSON: {{"headline": "...", "body": "<p>...</p><h2>...</h2>..."}}
"""


def _find_json_object(text):
    """Return the first {...} block, matching braces string-aware."""
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
    return bool(h) and len(h) >= 2 and h[0] not in '{"' \
        and "headline" not in h[:24].lower()


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
    if re.search(r"<(p|h3|ul|ol|blockquote|strong)[ >]", rest, re.I):
        return rest  # model already returned formatted HTML
    out = []
    for line in lines:
        s = line.strip()
        if not s:
            continue
        s = re.sub(r"^#{1,3}\s*", "", s)
        s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
        if re.match(r"^<h3>", s, re.I) or len(s) < 90 and s.endswith(":"):
            out.append(f"<h3>{htmlmod.escape(s.rstrip(':'))}</h3>")
        else:
            out.append(f"<p>{s if '<' in s else htmlmod.escape(s)}</p>")
    return "\n".join(out[:12])


def _parse_rewrite(text, fallback_title):
    """Extract headline/body from model output, robustly."""
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
    if not body:
        body = f"<p>{htmlmod.escape(fallback_title)}</p>"
    # safety: never let scripts through into the blog post
    body = re.sub(r"<script.*?</script>", "", body, flags=re.S | re.I)
    return headline, body


def gemini_rewrite(key, story):
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent?key={key}")
    payload = {"contents": [{"parts": [{"text": REWRITE_PROMPT.format(
        title=story["title"], sources=", ".join(story["sources"]),
        article=story.get("article_text", story["title"]))}]}],
        "generationConfig": {"temperature": 0.7, "maxOutputTokens": 2500}}
    r = requests.post(url, json=payload, timeout=60)
    if r.status_code == 429:
        raise RuntimeError("gemini_429")
    r.raise_for_status()
    data = r.json()
    text = data["candidates"][0]["content"]["parts"][0]["text"]
    return _parse_rewrite(text, story["title"])


def groq_rewrite(story):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}",
               "Content-Type": "application/json"}
    payload = {"model": GROQ_MODEL, "temperature": 0.7, "max_tokens": 2500,
               "messages": [
                   {"role": "system",
                    "content": "You are a news rewrite assistant. Reply only JSON."},
                   {"role": "user", "content": REWRITE_PROMPT.format(
                       title=story["title"],
                       sources=", ".join(story["sources"]),
                       article=story.get("article_text", story["title"]))}]}
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if r.status_code == 429:
        raise RuntimeError("groq_429")
    r.raise_for_status()
    text = r.json()["choices"][0]["message"]["content"]
    return _parse_rewrite(text, story["title"])


def fetch_article_text(url, limit=4000):
    """Best-effort article body for the rewriter (context, not published)."""
    try:
        r = http_get(url, timeout=15)
        text = re.sub(r"<script.*?</script>|<style.*?</style>",
                      " ", r.text, flags=re.S | re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = htmlmod.unescape(re.sub(r"\s+", " ", text)).strip()
        return text[:limit]
    except Exception:
        return ""


def rewrite_story(story, key_status):
    """Gemini multi-key rotation -> Groq backup. Returns (headline, body, via)."""
    story["article_text"] = fetch_article_text(story["link"])
    last_err = None
    for i, key in enumerate(GEMINI_KEYS):
        try:
            h, b = gemini_rewrite(key, story)
            key_status["ok"][i] = True
            return h, b, f"gemini_key{i+1}"
        except Exception as e:
            last_err = e
            key_status["ok"][i] = False
            log(f"REWRITE gemini key{i+1} failed: {e}; trying next")
            time.sleep(2)
    if GROQ_API_KEY:
        try:
            h, b = groq_rewrite(story)
            key_status["groq"] = "ok"
            return h, b, "groq"
        except Exception as e:
            last_err = e
            key_status["groq"] = "fail"
            log(f"REWRITE groq failed: {e}")
    raise RuntimeError(f"all rewrite backends failed: {last_err}")


# ------------------------------------------------------------ STAGE 4: IMAGE
def _bad_image(u):
    d = urlparse(u).netloc.lower()
    p = u.lower()
    if p.endswith(".svg") or "news.google.com" in d:
        return True
    if "google" in d or "gstatic" in d:
        return "logo" in p  # Google's own logo/CDN art, never article photos
    name = p.rsplit("/", 1)[-1]  # filename only ("/thumb/" path is fine)
    return any(k in name for k in ("logo", "sprite", "placeholder", "avatar",
                                   "blank", "pixel", "favicon", "no-image",
                                   "thumbnail", "default"))


def image_queries_for(story):
    """Ordered image-search queries, most specific first, broadest last."""
    filler = {"news", "today", "report", "reports", "says", "after", "amid",
              "over", "under", "with", "from", "will", "says"}
    words = [w for w in sig_words(story["title"])
             if len(w) > 3 and w not in filler]
    by_len = sorted(set(words), key=len, reverse=True)
    queries = []
    # 1. intitle: pairs of the most specific words -> best relevance
    for a, b in zip(by_len[:3], by_len[1:4]):
        queries.append(f"intitle:{a} intitle:{b}")
    # 2. plain keyword queries -> broader net when intitle: finds nothing
    if len(by_len) >= 3:
        queries.append(" ".join(by_len[:3]))
    if len(by_len) >= 2:
        queries.append(" ".join(by_len[:2]))
    if by_len:
        queries.append(by_len[0])
    # 3. safe generic fallback: the news category itself
    queries.append(story["category"])
    return queries


def search_image(query):
    """Wikimedia Commons image search (free, no key). Best wide photo first.

    Skips watermarked/logo-stamped files via their Commons categories --
    Commons discourages watermarks, and tagged files are filtered out here.
    """
    try:
        url = ("https://commons.wikimedia.org/w/api.php?action=query"
               "&format=json&generator=search&gsrsearch=" +
               quote_plus(query) +
               "&gsrlimit=12&gsrnamespace=6&prop=imageinfo|categories"
               "&iiprop=url|size&iiurlwidth=1280&cllimit=50")
        r = http_get(url, timeout=15)
        pages = r.json().get("query", {}).get("pages", {})
        cands = []
        for p in pages.values():
            cats = [c.get("title", "") for c in p.get("categories", [])]
            if any("watermark" in c.lower() for c in cats):
                continue  # stamped/logo overlay -- never use
            info = (p.get("imageinfo") or [{}])[0]
            u = info.get("thumburl") or info.get("url") or ""
            w = info.get("width") or 0
            h = info.get("height") or 0
            if not u or _bad_image(u) or w < 600:
                continue
            # prefer wide landscape photos (clickbait-friendly)
            score = w + (500 if h and w / h >= 1.2 else 0)
            cands.append((score, u))
        cands.sort(reverse=True)
        return [u for _, u in cands]
    except Exception as e:
        log(f"IMAGE search failed: {e}")
        return []


def extract_images(article_url):
    """Collect candidate article images, best first. Never adds logos."""
    found = []

    def add(u):
        if u and u.startswith("http") and not _bad_image(u) \
                and u not in found:
            found.append(u)

    try:
        r = http_get(article_url, timeout=15)
        final = r.url  # after redirects -- resolve relative URLs against this
        if "news.google.com" in urlparse(final).netloc.lower():
            return []  # redirect never resolved; skip Google's own logo
        html = r.text
        for pat in (r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)',
                    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                    r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)'):
            m = re.search(pat, html, re.I)
            if m:
                add(urljoin(final, htmlmod.unescape(m.group(1)).strip()))
        for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', html, re.I):
            add(urljoin(final, htmlmod.unescape(m.group(1)).strip()))
            if len(found) >= 6:
                break
    except Exception as e:
        log(f"IMAGE collect failed: {e}")
    return found

# ----------------------------------------------------------- STAGE 5: PUBLISH
def blogger_access_token():
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": BG_CLIENT_ID, "client_secret": BG_CLIENT_SECRET,
        "refresh_token": BG_REFRESH_TOKEN, "grant_type": "refresh_token"},
        timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def build_content(headline, body_html, image_url, source_links):
    parts = []
    if image_url:
        parts.append(
            f'<img src="{htmlmod.escape(image_url, quote=True)}" '
            f'alt="{htmlmod.escape(headline, quote=True)}" '
            'style="width:100%;height:auto;border-radius:10px;margin-bottom:12px;" />')
    parts.append(body_html)
    if source_links:
        links = ", ".join(
            f'<a href="{htmlmod.escape(u, quote=True)}" rel="nofollow">'
            f'{htmlmod.escape(d)}</a>' for d, u in source_links)
        parts.append(f"<hr/><p><em>Sources: {links}</em></p>")
    return "\n".join(parts)


def publish_post(headline, body_html, image_url, source_links, label):
    token = blogger_access_token()
    payload = {
        "title": headline,
        "content": build_content(headline, body_html, image_url, source_links),
        "labels": [label, "WTN"],
    }
    if DRY_RUN:
        log(f"DRY-RUN would publish: {headline}")
        return "https://wtnlive.blogspot.com/dry-run"
    r = requests.post(
        f"https://www.googleapis.com/blogger/v3/blogs/{BLOG_ID}/posts"
        f"?isDraft=false&fetchImages=false",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        json=payload, timeout=30)
    r.raise_for_status()
    return r.json().get("url", "")


# ------------------------------------------------------------------- state
def load_state():
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
            s.setdefault("seen_fps", [])
            s.setdefault("seen_images", [])
            s.setdefault("counters", {})
            s.setdefault("total_posts", 0)
            s.setdefault("recent_posts", [])
            return s
    except Exception:
        return {"seen_fps": [], "seen_images": [], "counters": {},
                "total_posts": 0, "recent_posts": []}


def save_state(state):
    state["seen_fps"] = state["seen_fps"][-2000:]
    state["seen_images"] = state.get("seen_images", [])[-500:]
    state["recent_posts"] = state["recent_posts"][:20]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False)


def build_status(started, state, vstats, key_status, posts, errors, today):
    hist = []
    for i in range(6, -1, -1):
        d = (started - timedelta(days=i)).strftime("%Y-%m-%d")
        hist.append({"date": d,
                     "posts": state["counters"].get(d, {}).get("posts", 0)})
    rr = vstats.get("rejected_reasons", {})
    checked = vstats.get("checked", 0)
    return {
        "last_run": started.strftime("%Y-%m-%d %H:%M UTC"),
        "next_run": (started + timedelta(minutes=30)).strftime(
            "%Y-%m-%d %H:%M UTC"),
        "status": "error" if errors else "ok",
        "runs_today": state["counters"].get(today, {}).get("runs", 0),
        "posts_today": state["counters"].get(today, {}).get("posts", 0),
        "posts_total": state.get("total_posts", 0),
        "last_posts": state.get("recent_posts", [])[:8],
        "verifier": {
            "checked": checked,
            "passed": vstats.get("passed", 0),
            "rejected": vstats.get("rejected", 0),
            "rejected_reasons": {
                "single_source": rr.get("single_source", 0),
                "harm": rr.get("harm", 0),
                "duplicate": rr.get("duplicate", 0),
            },
        },
        "keys": {
            "gemini_keys_total": len(key_status["ok"]),
            "gemini_keys_ok": sum(1 for v in key_status["ok"] if v is True),
            "gemini_keys_failed": sum(1 for v in key_status["ok"]
                                      if v is False),
            "gemini_keys_untested": sum(1 for v in key_status["ok"]
                                        if v is None),
            "groq": key_status.get("groq", "unused"),
        },
        "errors": errors[-5:],
        "history": hist,
    }


# -------------------------------------------------------------------- main
def main():
    started = datetime.now(timezone.utc)
    today = started.strftime("%Y-%m-%d")
    log("WTN AutoPost run started")
    errors, posts = [], []
    key_status = {"ok": [None] * len(GEMINI_KEYS), "groq": "unused"}

    for name in ["BLOGGER_BLOG_ID", "BLOGGER_CLIENT_ID",
                 "BLOGGER_CLIENT_SECRET", "BLOGGER_REFRESH_TOKEN"]:
        if not os.environ.get(name):
            errors.append(f"Missing secret: {name}")
    if not GEMINI_KEYS and not GROQ_API_KEY:
        errors.append("No AI key: set GEMINI_API_KEYS and/or GROQ_API_KEY")

    state = load_state()
    seen = set(state["seen_fps"])
    seen_images = set(state.get("seen_images", []))
    recent_titles = [p.get("title", "") for p in state.get("recent_posts", [])]
    vstats = {"checked": 0, "passed": 0, "rejected": 0, "rejected_reasons": {}}

    if not errors:
        try:
            # ---- STAGE 1/6: HUNTER -- collect fresh headlines ------------

            log("STAGE 1/6 HUNTER: collecting headlines")
            items = hunt()
            log(f"HUNTER done: {len(items)} items")

            # ---- STAGE 2/6: VERIFIER -- actively corroborate each story --
            log("STAGE 2/6 VERIFIER: corroborating stories")
            clusters = cluster_items(items)
            candidates = select_candidates(clusters, limit=25)
            stories = []
            vstats = {"checked": 0, "passed": 0, "rejected": 0,
                      "rejected_reasons": {}}
            for item in candidates:
                if len(stories) >= MAX_POSTS:
                    break
                vstats["checked"] += 1
                story, reason = verify_candidate(item, seen, recent_titles)
                if story:
                    stories.append(story)
                    vstats["passed"] += 1
                    log(f"VERIFIED [{len(story['sources'])} outlets] "
                        f"{story['title'][:60]}")
                else:
                    vstats["rejected"] += 1
                    rr = vstats["rejected_reasons"]
                    rr[reason] = rr.get(reason, 0) + 1
            log(f"VERIFIER done: passed={vstats['passed']} "
                f"rejected={vstats['rejected']}")

            # ---- STAGES 3-5 per story: REWRITE -> PHOTO -> PUBLISH -------
            for story in stories:
                try:
                    # STAGE 3/6: REWRITER -- full SEO article
                    log(f"STAGE 3/6 REWRITER: {story['title'][:50]}")
                    headline, body, via = rewrite_story(story, key_status)

                    # STAGE 4/6: PHOTOGRAPHER -- fresh photo, never repeated
                    log("STAGE 4/6 PHOTOGRAPHER: finding photo")
                    candidates = extract_images(story["link"])
                    image = next((u for u in candidates
                                  if u not in seen_images), None)
                    if not image:
                        # HARD RULE: never post without a photo --
                        # search the web for one instead.
                        for q in image_queries_for(story):
                            log(f"PHOTO searching: {q}")
                            for u in search_image(q):
                                if u not in seen_images:
                                    image = u
                                    break
                            if image:
                                break
                    if not image:
                        # Visible skip (never silent): the user must see WHY
                        # a verified story did not become a post.
                        msg = f"no photo found, skipped: {story['title'][:60]}"
                        log(f"SKIP ({msg})")
                        errors.append(msg)
                        seen.add(story["fingerprint"])
                        save_state(state)
                        continue

                    # STAGE 5/6: PUBLISHER -- live to Blogger
                    log("STAGE 5/6 PUBLISHER: publishing live")
                    url = publish_post(headline, body, image,
                                       story["source_links"], story["category"])
                    log(f"PUBLISHED [{via}] {headline} -> {url}")
                    seen.add(story["fingerprint"])
                    if image:
                        seen_images.add(image)
                    posts.append(headline)
                    state["recent_posts"].insert(0, {
                        "title": headline, "url": url,
                        "time": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M UTC"),
                        "label": story["category"],
                        "image": image or ""})
                    state["total_posts"] += 1
                    # save after EVERY post: if the run crashes mid-batch,
                    # the next run still knows what was published (no dupes)
                    save_state(state)
                except Exception as e:
                    errors.append(f"story failed: {story['title'][:60]}: {e}")
                    log(f"ERROR story: {e}")
        except Exception as e:
            errors.append(f"pipeline: {e}")
            log(f"ERROR pipeline: {e}")

    # ---- STAGE 6/6: REPORTER -- save state + dashboard -------------------
    log("STAGE 6/6 REPORTER: saving state")
    c = state["counters"].setdefault(today, {"runs": 0, "posts": 0})
    c["runs"] += 1
    c["posts"] += len(posts)
    # CRITICAL: write the in-memory sets back -- otherwise dedup resets
    # every run and the same stories get posted again and again.
    state["seen_fps"] = sorted(seen)[-2000:]
    state["seen_images"] = sorted(seen_images)[-500:]
    save_state(state)
    with open(STATUS_FILE, "w") as f:
        json.dump(build_status(started, state, vstats, key_status,
                               posts, errors, today), f, ensure_ascii=False)

    log(f"done: {len(posts)} published, {len(errors)} errors")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())

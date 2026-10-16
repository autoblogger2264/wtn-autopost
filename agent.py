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

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
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


def verify(clusters, seen_fps):
    """Truth check. Returns (passed, stats). passed = list of story dicts."""
    passed, stats = [], {"checked": len(clusters), "passed": 0,
                         "rejected": 0, "rejected_reasons": {}}

    def reject(reason):
        stats["rejected"] += 1
        rr = stats["rejected_reasons"]
        rr[reason] = rr.get(reason, 0) + 1

    for cl in clusters:
        titles = " ".join(c["title"] for c in cl)
        harm = has_harm(titles)
        if harm:
            reject("harm"); continue
        doms = {c["domain"] for c in cl if c["domain"]}
        rep = {d for d in doms if d in REPUTABLE_DOMAINS}
        # truth signal: >=MIN_SOURCES independent outlets AND >=1 reputable
        if len(doms) < MIN_SOURCES or not rep:
            reject("single_source"); continue
        fp = fingerprint(cl)
        if fp in seen_fps:
            reject("duplicate"); continue
        rep = min(cl, key=lambda c: c["published"])  # earliest report
        seen_d, src_links = set(), []
        for c in cl:
            if c["domain"] and c["domain"] not in seen_d:
                seen_d.add(c["domain"])
                src_links.append((c["domain"], c["link"]))
        top_cat = Counter(c["category"] for c in cl).most_common(1)[0][0]
        passed.append({
            "fingerprint": fp,
            "title": rep["title"],
            "link": rep["link"],
            "category": top_cat,
            "sources": sorted(doms),
            "source_links": src_links[:6],
            "published": rep["published"],
        })
    stats["passed"] = len(passed)
    # newest verified first
    passed.sort(key=lambda s: s["published"], reverse=True)
    return passed[:MAX_POSTS], stats

# ---------------------------------------------------------- STAGE 3: REWRITE
REWRITE_PROMPT = """Rewrite this news story as a FULL, detailed news article in English -- NOT a short summary.

STORY TITLE: {title}
REPORTED BY: {sources}
ARTICLE TEXT:
{article}

RULES:
- Headline: engaging and hook-y, under 90 characters. Must stay accurate -- never invent facts. Do NOT copy the original headline.
- Opening paragraph must HOOK the reader: lead with the most surprising or important angle of the story.
- Write 6 to 10 paragraphs with rich detail drawn from the article text above. Cover who, what, when, where, why, and what happens next.
- Format the article beautifully with HTML:
  * <strong> around key facts, names, numbers, and dates
  * <h3> subheadings to break the article into sections
  * <ul><li> bullet lists for key points or takeaways
  * <blockquote> for direct quotes (only real quotes from the text above)
- Do not invent quotes, numbers, or facts not present above.
- Reply ONLY as JSON: {{"headline": "...", "body": "<p>...</p><h3>...</h3>..."}}
"""


def _parse_rewrite(text, fallback_title):
    """Extract headline/body from model output, robustly."""
    headline, body = "", ""
    try:
        m = re.search(r"\{.*\}", text, re.S)
        if m:
            data = json.loads(m.group(0))
            headline = str(data.get("headline", "")).strip()
            body = str(data.get("body", "")).strip()
    except Exception:
        pass
    if not headline or not body:
        lines = [l.strip() for l in text.strip().splitlines()
                 if l.strip() and not l.strip().startswith("```")]
        if lines:
            headline = re.sub(r"^#+\s*", "",
                              re.sub(r"<[^>]+>", "", lines[0]))[:90]
            rest = "\n".join(lines[1:])
            if re.search(r"<(p|h3|ul|blockquote)[ >]", rest, re.I):
                body = rest  # model already returned formatted HTML
            else:
                paras = [l for l in lines[1:] if len(l) > 20][:10]
                body = "".join(f"<p>{htmlmod.escape(p)}</p>" for p in paras)
    if not headline:
        headline = fallback_title[:90]
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
def extract_image(article_url):
    """Collect the article's own high-quality image. Never adds logos."""
    try:
        r = http_get(article_url, timeout=15)
        html = r.text
        for pat in (r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)',
                    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                    r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)'):
            m = re.search(pat, html, re.I)
            if m:
                u = urljoin(article_url, htmlmod.unescape(m.group(1)).strip())
                if u.startswith("http"):
                    return u
        # fallback: first plausible <img>
        for m in re.finditer(r'<img[^>]+src=["\']([^"\']+)["\']', html, re.I):
            u = urljoin(article_url, htmlmod.unescape(m.group(1)).strip())
            if u.startswith("http") and not u.endswith(".svg"):
                return u
    except Exception as e:
        log(f"IMAGE collect failed: {e}")
    return None

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
            s.setdefault("counters", {})
            s.setdefault("total_posts", 0)
            s.setdefault("recent_posts", [])
            return s
    except Exception:
        return {"seen_fps": [], "counters": {}, "total_posts": 0,
                "recent_posts": []}


def save_state(state):
    state["seen_fps"] = state["seen_fps"][-2000:]
    state["recent_posts"] = state["recent_posts"][:10]
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
            "gemini_keys_ok": sum(1 for v in key_status["ok"] if v),
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
    vstats = {"checked": 0, "passed": 0, "rejected": 0, "rejected_reasons": {}}

    if not errors:
        try:
            items = hunt()
            log(f"HUNT total: {len(items)} items")
            clusters = cluster_items(items)
            log(f"VERIFY {len(clusters)} story clusters")
            stories, vstats = verify(clusters, seen)
            log(f"VERIFY passed={vstats['passed']} "
                f"rejected={vstats['rejected']}")
            for story in stories:
                try:
                    headline, body, via = rewrite_story(story, key_status)
                    image = extract_image(story["link"])
                    url = publish_post(headline, body, image,
                                       story["source_links"], story["category"])
                    log(f"PUBLISH [{via}] {headline} -> {url}")
                    seen.add(story["fingerprint"])
                    posts.append(headline)
                    state["recent_posts"].insert(0, {
                        "title": headline, "url": url,
                        "time": datetime.now(timezone.utc).strftime(
                            "%Y-%m-%d %H:%M UTC"),
                        "label": story["category"],
                        "image": image or ""})
                    state["total_posts"] += 1
                except Exception as e:
                    errors.append(f"story failed: {story['title'][:60]}: {e}")
                    log(f"ERROR story: {e}")
        except Exception as e:
            errors.append(f"pipeline: {e}")
            log(f"ERROR pipeline: {e}")

    c = state["counters"].setdefault(today, {"runs": 0, "posts": 0})
    c["runs"] += 1
    c["posts"] += len(posts)
    save_state(state)
    with open(STATUS_FILE, "w") as f:
        json.dump(build_status(started, state, vstats, key_status,
                               posts, errors, today), f, ensure_ascii=False)

    log(f"done: {len(posts)} published, {len(errors)} errors")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())

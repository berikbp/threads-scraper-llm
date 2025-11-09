import json, time, os, re
from parsel import Selector
from nested_lookup import nested_lookup
import jmespath
from playwright.sync_api import sync_playwright
import pandas as pd
from urllib.parse import quote
from glob import glob


ROOT_SAVE_DIR = "data/english/"
LIMIT_PER_KEYWORD = 250
SLEEP = 2
LOCALE = ("en-US", "en-US,en;q=0.9")
os.makedirs(ROOT_SAVE_DIR, exist_ok=True)


SAD = [
    "sad", "sadness", "lonely", "depression", "anxiety",
    "crying", "broken heart", "miss you", "tired",
    "pain", "hopeless", "heartbreak", "hurt", "bad day"
]
NEUTRAL = [
    "life", "work", "school", "friends", "weather", "family",
    "travel", "morning", "routine", "food", "study",
    "weekend", "day", "evening", "city"
]
HAPPY = [
    "happy", "happiness", "joy", "smile", "love", "success",
    "motivation", "good vibes", "grateful", "blessed",
    "proud", "amazing day", "sunshine"
]

ALL_KEYWORDS = [("sad", k) for k in SAD] + [("neutral", k) for k in NEUTRAL] + [("happy", k) for k in HAPPY]



def load_existing_ids(keyword):
    """Avoid re-scraping posts that already exist."""
    existing = set()
    for file in glob(f"{ROOT_SAVE_DIR}/threads_{keyword}_*.csv"):
        try:
            df = pd.read_csv(file, usecols=["id"])
            existing.update(df["id"].dropna().astype(str))
        except Exception:
            continue
    return existing

def parse_thread(data):
    """Extract main post data from JSON."""
    result = jmespath.search(
        """{
            text: post.caption.text,
            published_on: post.taken_at,
            id: post.id,
            code: post.code,
            username: post.user.username,
            like_count: post.like_count,
            reply_count: view_replies_cta_string,
            image_count: post.carousel_media_count,
            videos: post.video_versions[].url
        }""",
        data,
    )
    if not result:
        return None
    if result.get("reply_count") and not isinstance(result["reply_count"], int):
        try:
            first = str(result["reply_count"]).split(" ")[0]
            result["reply_count"] = int(first) if first.isdigit() else 0
        except Exception:
            result["reply_count"] = 0
    result["url"] = f"https://www.threads.net/@{result['username']}/post/{result['code']}"
    result["repost_count"] = 0
    return result

def scrape_thread_page(page_source):
    """Extract all posts from the Threads page source."""
    selector = Selector(text=page_source)
    datasets = selector.css('script[type="application/json"][data-sjs]::text').getall()
    posts, seen_ids = [], set()

    for raw in datasets:
        if '"ScheduledServerJS"' not in raw or "thread_items" not in raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        thread_items = nested_lookup("thread_items", data)
        for group in thread_items:
            for t in group:
                parsed = parse_thread(t)
                if not parsed:
                    continue
                pid = str(parsed.get("id"))
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                posts.append(parsed)
    return posts

def save_results(keyword, posts):
    """Save results as CSV."""
    df = pd.DataFrame(posts)
    fname = f"{ROOT_SAVE_DIR}/threads_{keyword}_en_{int(time.time())}.csv"
    df.to_csv(fname, index=False)
    print(f"💾 Saved {len(df)} posts → {fname}")


def scrape_english_data():
    locale, accept = LOCALE
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            locale=locale,
            extra_http_headers={"Accept-Language": accept},
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )

        for emotion, keyword in ALL_KEYWORDS:
            results, seen_ids = [], set()
            existing_ids = load_existing_ids(keyword)
            encoded_keyword = quote(keyword)
            search_url = f"https://www.threads.net/tag/{encoded_keyword}"
            print(f"🌍 [{emotion}] → #{keyword}")

            page = context.new_page()
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                print("⚠️ Failed to open:", e)
                continue

            last_height = 0
            while len(results) < LIMIT_PER_KEYWORD:
                links = page.locator('a[href*="/post/"]').all()
                for link in links:
                    href = link.get_attribute("href")
                    if not href or "post" not in href:
                        continue
                    if href.startswith("/"):
                        href = "https://www.threads.net" + href
                    post_code = href.split("/")[-1]
                    if post_code in seen_ids or post_code in existing_ids:
                        continue
                    seen_ids.add(post_code)

                    try:
                        p2 = context.new_page()
                        p2.goto(href, wait_until="domcontentloaded", timeout=20000)
                        p2.wait_for_selector("[data-pressable-container=true]", timeout=8000)
                        posts = scrape_thread_page(p2.content())
                        p2.close()
                        for p in posts:
                            pid = str(p.get("id"))
                            if pid in existing_ids:
                                continue
                            p["keyword"] = keyword
                            p["emotion"] = emotion
                            p["language_context"] = "english"
                            results.append(p)
                    except Exception as e:
                        print("⚠️ Post error:", e)
                    if len(results) >= LIMIT_PER_KEYWORD:
                        break

                page.mouse.wheel(0, 4000)
                time.sleep(SLEEP)
                new_height = page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            save_results(keyword, results)
            page.close()

        browser.close()
    print("✅ All English keywords scraped.")



if __name__ == "__main__":
    scrape_english_data()

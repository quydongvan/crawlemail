# crawler_logic.py
import time
import json
import re
import os
import base64
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# ==========================
# 0) CẤU HÌNH
# ==========================
class ConfigManager:
    FILE = "settings_maps_email.json"
    DEFAULT = {
        "sub_keywords": [], "contact_hints": [], "blocklist": [],
        "headless": True, "max_scroll": 60, "delay": 2.2, "request_workers": 4,
        "selenium_workers": 1, "selenium_contact_limit": 4, "selenium_wait_body": 3,
        "selenium_wait_click": 1, "selenium_sleep_per_page": 0.8
    }
    @classmethod
    def load(cls):
        if os.path.exists(cls.FILE):
            try:
                with open(cls.FILE, "r", encoding="utf-8") as f: data = json.load(f)
                cfg = cls.DEFAULT.copy(); cfg.update(data or {}); return cfg
            except Exception: pass
        return cls.DEFAULT.copy()
    @classmethod
    def save(cls, data: dict):
        try:
            with open(cls.FILE, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception: return False

# ==================================================
# 2) DỌN DẸP RESPONSE GOOGLE MAPS
# ==================================================
def clean_google_maps_body(raw: str):
    if raw is None: return None, ""
    s = raw.strip()
    if s.endswith('/*""*/'): s = s[:-6].rstrip()
    def try_parse_json(txt: str):
        try: return json.loads(txt)
        except Exception: return None
    if s.startswith(")]}'\n"):
        s2 = s[5:].lstrip(); obj = try_parse_json(s2)
        return (obj, s2) if obj is not None else (None, s2)
    if s.startswith(")]}'"):
        nl = s.find("\n"); s2 = s[nl + 1:] if nl != -1 else s[4:]
        obj = try_parse_json(s2)
        return (obj, s2) if obj is not None else (None, s2)
    obj = try_parse_json(s)
    if obj is not None:
        if isinstance(obj, dict) and isinstance(obj.get("d"), str):
            inner = obj["d"].lstrip()
            if inner.startswith(")]}'\n"): inner = inner[5:].lstrip()
            elif inner.startswith(")]}'"):
                nl = inner.find("\n"); inner = inner[nl + 1:] if nl != -1 else inner[4:]
            obj2 = try_parse_json(inner)
            return (obj2, inner) if obj2 is not None else (None, inner)
        return obj, s
    return None, s

# ===========================================================
# 3) TRÍCH XUẤT THÔNG TIN (VIẾT LẠI CHÍNH XÁC NHƯ BẢN GỐC)
# ===========================================================
def extract_address_from_text(text: str, name: str) -> str:
    m = re.search(r'"' + re.escape(name) + r',\s*[^"]*"', text)
    return m.group(0).strip('"') if m else ""

def find_businesses_from_text(text: str):
    pat = re.compile(r'\[\s*null\s*,\s*null\s*,\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\s*\]\s*,\s*"(?:[^"]+)"\s*,\s*"([^"]+)"')
    for m in pat.finditer(text):
        yield m.group(3), m.start(3)

def extract_website_near(text: str, name: str, pos: int, window: int = 12000) -> str:
    start = max(0, pos - window)
    end = min(len(text), pos + window)
    chunk = text[start:end]
    urls = [m.group(0).strip('"') for m in re.finditer(r'"https?://[^"]+"', chunk)]
    if not urls: return ""
    
    def is_business_url(u: str) -> bool:
        u2 = u.lower()
        bad = ("google.com", "gstatic.com", "ggpht.com", "googleusercontent.com", "/maps")
        return not any(b in u2 for b in bad)
    
    cands = [u for u in urls if is_business_url(u)]
    if not cands: return ""
    
    tokens = [t for t in re.findall(r"[a-zA-Z0-9]+", name.lower()) if len(t) >= 3]
    best_url, best_score = "", float("-inf")
    for u in cands:
        u_lower = u.lower()
        url_pos = chunk.find(u)
        dist = abs((start + url_pos) - pos) if url_pos != -1 else 10**9
        score = -dist
        if any(t in u_lower for t in tokens): score += 2000
        if u.count("?") == 0: score += 100
        if score > best_score: best_score, best_url = score, u
    return best_url

def extract_rows_from_text(big_text: str, kw_label: str):
    rows, seen = [], set()
    for name, pos in find_businesses_from_text(big_text):
        if name in seen: continue
        seen.add(name)
        rows.append({
            "Từ khóa": kw_label,
            "Tên": name,
            "Địa chỉ": extract_address_from_text(big_text, name) or "",
            "Trang web": extract_website_near(big_text, name, pos) or "",
            "Email": "", "Trạng thái": "Chưa lấy"
        })
    return rows

# ==================================================
# 4) SELENIUM & LOGIC CHÍNH
# ==================================================
def build_driver(headless: bool = True):
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service as ChromeService
    chrome_options = Options()
    if headless: chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--lang=vi-VN"); chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage"); chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled"); chrome_options.add_argument("--window-size=1200,900")
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    try: driver.execute_cdp_cmd("Network.enable", {})
    except Exception: pass
    return driver

def scroll_until_end(driver, log_callback, max_rounds: int, delay: float):
    try:
        container = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div[@role='feed']")))
    except TimeoutException:
        log_callback("❌ Không tìm thấy panel kết quả."); return False
    for i in range(1, max_rounds + 1):
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)
        log_callback(f"Đang cuộn lần {i}/{max_rounds}…")
        time.sleep(delay)
        if driver.find_elements(By.XPATH, "//span[contains(text(), 'Bạn đã xem hết danh sách này.')]"):
            log_callback("✔️ Đã cuộn tới cuối."); return True
    log_callback("⚠️ Dừng vì đạt số lần cuộn tối đa."); return False

def collect_search_bodies_via_perflog(driver, log_callback):
    logs = driver.get_log("performance")
    search_responses = []
    for entry in logs:
        try:
            msg = json.loads(entry["message"]).get("message", {})
            if msg.get("method") == "Network.responseReceived":
                if "search?" in msg.get("params", {}).get("response", {}).get("url", ""):
                    search_responses.append(msg["params"].get("requestId"))
        except Exception: continue
    results = []
    seen_ids = set()
    for req_id in filter(None, search_responses):
        if req_id in seen_ids: continue
        seen_ids.add(req_id)
        try:
            body_obj = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
            body = base64.b64decode(body_obj["body"]).decode("utf-8", "ignore") if body_obj.get("base64Encoded") else body_obj.get("body", "")
            results.append({"body_text": body})
        except Exception as e: log_callback(f"⚠️ Lỗi đọc body response: {e}")
    log_callback(f"Đã lấy được {len(results)} response 'search?'.")
    return results

def harvest_one_query(main_kw, sub_kw, headless, max_rounds, delay, log_callback):
    kw_label = f"{main_kw} | {sub_kw}" if sub_kw else main_kw
    q = f"{main_kw} {sub_kw}".strip() if sub_kw else main_kw
    driver = None
    try:
        driver = build_driver(headless=headless)
        driver.get(f"https://www.google.com/maps/search/{q.replace(' ', '+')}")
        log_callback(f"[{kw_label}] Đã mở Maps, bắt đầu cuộn...")
        scroll_until_end(driver, log_callback, max_rounds, delay)
        bodies = collect_search_bodies_via_perflog(driver, log_callback)
        big_text_parts = []
        for item in bodies:
            body_text = item.get("body_text", "")
            obj, cleaned_text = clean_google_maps_body(body_text)
            if obj is not None:
                big_text_parts.append(json.dumps(obj, ensure_ascii=False))
            else:
                big_text_parts.append(cleaned_text)
        big_text = "\n".join(big_text_parts)
        rows = extract_rows_from_text(big_text, kw_label)
        log_callback(f"[{kw_label}] ✅ Trích xuất được {len(rows)} mục.")
        return rows
    except Exception as e:
        log_callback(f"[{kw_label}] ❌ Lỗi: {e}"); return []
    finally:
        if driver: driver.quit()

# ======================================================
# 5) TRÍCH XUẤT EMAIL & LINK LIÊN HỆ
# ======================================================
EMAIL_RE = re.compile(r'(?i)(?<![\w.\-])([A-Z0-9._%+\-]+@[A-Z0-9\-]+(?:\.[A-Z0-9\-]+)+)(?![\w.\-])')

def normalize_url(u: str) -> str:
    u = (u or "").strip().strip('"')
    return "http://" + u if u and not u.startswith(("http:", "https:")) else u

def canonical_domain(u: str) -> str:
    try: host = urlparse(normalize_url(u)).hostname or ""
    except Exception: host = ""
    return host[4:] if host.startswith("www.") else host.lower()

def fetch_html(url: str, timeout: int = 15) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_emails_from_html(html: str) -> list[str]:
    html = re.sub(r"\s*\[\s*(at|AT)\s*\]\s*", "@", html)
    html = re.sub(r"\s*\(\s*(at|AT)\s*\)\s*", "@", html)
    html = re.sub(r"\s*\[\s*(dot|DOT)\s*\]\s*", ".", html)
    html = re.sub(r"\s*\(\s*(dot|DOT)\s*\)\s*", ".", html)
    emails = set(e.strip('.,') for e in EMAIL_RE.findall(html))
    return sorted(list(emails), key=str.lower)

def _short(u: str) -> str:
    try: return urlparse(u).netloc
    except: return u[:30]

def pick_contact_links_from_html(base_url: str, html: str, hints: list[str], limit=6):
    soup = BeautifulSoup(html, "html.parser")
    seen, out = set(), []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.lower().startswith(("mailto:", "tel:")): continue
        abs_u = requests.compat.urljoin(base_url, href)
        if abs_u in seen: continue
        text = (a.get_text(" ", strip=True) or "").lower()
        if any(h in abs_u.lower() or h in text for h in hints):
            out.append(abs_u); seen.add(abs_u)
            if len(out) >= limit: return out
    return out

def selenium_emails_from_current_page(driver) -> list:
    return extract_emails_from_html(driver.page_source or "")

def request_phase_contact_only(base_url, hints, blocklist, on_status):
    base = normalize_url(base_url)
    try:
        on_status(f"REQ: tải HOME {_short(base)}...")
        html0 = fetch_html(base)
        links = pick_contact_links_from_html(base, html0, hints)
        if not links:
            on_status("REQ: không thấy link liên hệ."); return [], None
        for u2 in links:
            on_status(f"REQ CONTACT: {_short(u2)}...")
            html = fetch_html(u2)
            emails = [e for e in extract_emails_from_html(html) if not any(b in e.lower() for b in blocklist)]
            if emails: return emails, f"Request CONTACT {_short(u2)}"
        return [], None
    except Exception as e:
        on_status(f"REQ: lỗi - {e}"); return [], None

def selenium_phase_contact_then_home(driver, base_url, hints, blocklist, on_status, limit=4):
    base = normalize_url(base_url)
    try:
        on_status(f"SEL: mở HOME {_short(base)}...")
        driver.get(base)
        time.sleep(1)
        links = pick_contact_links_from_html(base, driver.page_source, hints, limit=limit)
        for u2 in links:
            on_status(f"SEL CONTACT: {_short(u2)}...")
            driver.get(u2)
            time.sleep(1)
            emails = [e for e in selenium_emails_from_current_page(driver) if not any(b in e.lower() for b in blocklist)]
            if emails: return emails, f"Selenium CONTACT {_short(u2)}"
        on_status("SEL: không thấy @, quay lại HOME...")
        driver.get(base)
        time.sleep(1)
        emails = [e for e in selenium_emails_from_current_page(driver) if not any(b in e.lower() for b in blocklist)]
        if emails: return emails, "Selenium HOME"
        return [], None
    except Exception as e:
        on_status(f"SEL: lỗi - {e}"); return [], None
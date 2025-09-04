import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog, simpledialog
import tkinter.font as tkfont
import threading
import time
import json
import re
import csv
import os
import base64
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import webbrowser
import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


# ==========================
# 0) CẤU HÌNH / LƯU SETTINGS
# ==========================
class ConfigManager:
    FILE = "settings_maps_email.json"
    DEFAULT = {
        "sub_keywords": ["Hà Đông", "Bắc Ninh"],
        "contact_hints": [
            "liên hệ", "lien-he", "contact", "kontakt", "impressum", "imprint",
            "about", "gioi-thieu", "support", "privacy", "datenschutz", "legal"
        ],
        "blocklist": [".png", ".jpg", ".jpeg", ".svg"],
        "headless": True,
        "max_scroll": 60,
        "delay": 2.2,
        # Giới hạn luồng nhẹ máy
        "request_workers": 4,   # tối đa luồng requests
        "selenium_workers": 1,  # tối đa luồng selenium
        "selenium_contact_limit": 4,  # tối đa số link liên hệ sẽ mở bằng selenium
        "selenium_wait_body": 3,      # WebDriverWait body
        "selenium_wait_click": 1,     # WebDriverWait click
        "selenium_sleep_per_page": 0.8
    }

    @classmethod
    def load(cls):
        if os.path.exists(cls.FILE):
            try:
                with open(cls.FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                cfg = cls.DEFAULT.copy()
                cfg.update(data or {})
                return cfg
            except Exception:
                pass
        return cls.DEFAULT.copy()

    @classmethod
    def save(cls, data: dict):
        try:
            with open(cls.FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            messagebox.showerror("Lỗi lưu", f"Không thể lưu cấu hình:\n{e}")
            return False


# ==========================
# 1) TIỆN ÍCH CHUNG (LOG/UI)
# ==========================
class UILogger:
    def __init__(self, text_widget: scrolledtext.ScrolledText, status_var: tk.StringVar = None):
        self.text_widget = text_widget
        self.status_var = status_var

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.text_widget.configure(state="normal")
        self.text_widget.insert(tk.END, f"[{ts}] {msg}\n")
        self.text_widget.configure(state="disabled")
        self.text_widget.see(tk.END)
        self.text_widget.update_idletasks()

    def status(self, msg: str):
        if self.status_var is not None:
            self.status_var.set(msg)


# ==================================================
# 2) DỌN DẸP RESPONSE GOOGLE MAPS (XSSI…)
# ==================================================
def clean_google_maps_body(raw: str):
    if raw is None:
        return None, ""
    s = raw.strip()
    if s.endswith('/*""*/'):
        s = s[:-6].rstrip()

    def try_parse_json(txt: str):
        try:
            return json.loads(txt)
        except Exception:
            return None

    if s.startswith(")]}'\n"):
        s2 = s[5:].lstrip()
        obj = try_parse_json(s2)
        return (obj, s2) if obj is not None else (None, s2)

    if s.startswith(")]}'"):
        nl = s.find("\n")
        s2 = s[nl + 1:] if nl != -1 else s[4:]
        obj = try_parse_json(s2)
        return (obj, s2) if obj is not None else (None, s2)

    obj = try_parse_json(s)
    if obj is not None:
        if isinstance(obj, dict) and isinstance(obj.get("d"), str):
            inner = obj["d"].lstrip()
            if inner.startswith(")]}'\n"):
                inner = inner[5:].lstrip()
            elif inner.startswith(")]}'"):
                nl = inner.find("\n")
                inner = inner[nl + 1:] if nl != -1 else inner[4:]
            obj2 = try_parse_json(inner)
            return (obj2, inner) if obj2 is not None else (None, inner)
        return obj, s
    return None, s


# ===========================================================
# 3) TRÍCH XUẤT THÔNG TIN TỪ VĂN BẢN (HEURISTIC)
# ===========================================================
def extract_address_from_text(text: str, name: str) -> str:
    m = re.search(r'"' + re.escape(name) + r',\s*[^"]*"', text)
    return m.group(0).strip('"') if m else ""


def find_businesses_from_text(text: str):
    pat = re.compile(
        r'\[\s*null\s*,\s*null\s*,\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\s*\]\s*,\s*"(?:[^"]+)"\s*,\s*"([^"]+)"'
    )
    for m in pat.finditer(text):
        name = m.group(3)
        yield name, m.start(3)


def extract_website_near(text: str, name: str, pos: int, window: int = 12000) -> str:
    start = max(0, pos - window)
    end = min(len(text), pos + window)
    chunk = text[start:end]

    urls = [m.group(0).strip('"') for m in re.finditer(r'"https?://[^"]+"', chunk)]
    if not urls:
        return ""

    def is_business_url(u: str) -> bool:
        u2 = u.lower()
        bad = ("google.com", "gstatic.com", "ggpht.com", "googleusercontent.com", "/maps")
        return not any(b in u2 for b in bad)

    cands = [u for u in urls if is_business_url(u)]
    if not cands:
        return ""

    tokens = [t for t in re.findall(r"[a-zA-Z0-9]+", name.lower()) if len(t) >= 3]

    best_url, best_score = "", float("-inf")
    for u in cands:
        u_lower = u.lower()
        url_pos = chunk.find(u)
        if url_pos == -1:
            url_pos = 10**9
        dist = abs((start + url_pos) - pos)
        score = -dist
        if any(t in u_lower for t in tokens):
            score += 2000
        if u.count("?") == 0:
            score += 100
        if score > best_score:
            best_score, best_url = score, u
    return best_url


def extract_rows_from_text(big_text: str, kw_label: str):
    rows = []
    seen = set()
    for name, pos in find_businesses_from_text(big_text):
        if name in seen:
            continue
        seen.add(name)
        addr = extract_address_from_text(big_text, name) or ""
        site = extract_website_near(big_text, name, pos) or ""
        rows.append({
            "Chọn": False,
            "Từ khóa": kw_label,
            "Tên": name,
            "Địa chỉ": addr,
            "Trang web": site,
            "Email": "",
            "Trạng thái": "Chưa lấy"
        })
    return rows


# ==================================================
# 4) SELENIUM + DEVTOOLS
# ==================================================
def build_driver(headless: bool = True):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--lang=vi-VN")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--window-size=1200,900")
    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
    except Exception:
        pass
    return driver


def scroll_until_end(driver, logger: UILogger, max_rounds: int, delay: float):
    try:
        logger.log("Đang chờ panel kết quả (div[role='feed'])...")
        container = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='feed']"))
        )
        logger.log("✔️ Đã có panel kết quả.")
    except TimeoutException:
        logger.log("❌ Không tìm thấy panel kết quả.")
        return False

    end_xpath = "//span[contains(text(), 'Bạn đã xem hết danh sách này.')]"
    end_seen = False

    for i in range(1, max_rounds + 1):
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", container)
        logger.status(f"Đang cuộn lần {i}/{max_rounds} …")
        time.sleep(delay)
        try:
            end_el = driver.find_element(By.XPATH, end_xpath)
            if end_el.is_displayed():
                logger.log("✔️ Thấy thông báo 'Bạn đã xem hết danh sách này.' — dừng cuộn.")
                end_seen = True
                break
        except NoSuchElementException:
            pass
    if not end_seen:
        logger.log("⚠️ Dừng vì đạt số lần cuộn tối đa (có thể vẫn còn kết quả).")
    return end_seen


def collect_search_bodies_via_perflog(driver, logger: UILogger):
    logs = driver.get_log("performance")
    logger.log(f"Thu được {len(logs)} bản ghi performance log.")

    search_responses = []
    for entry in logs:
        try:
            msg = json.loads(entry["message"]).get("message", {})
            method = msg.get("method")
            params = msg.get("params", {})
            if method == "Network.responseReceived":
                resp = params.get("response", {})
                url = resp.get("url", "")
                type_ = params.get("type", "")
                if "search?" in url and type_ in ("XHR", "Fetch"):
                    search_responses.append((params.get("requestId"), url))
        except Exception:
            continue

    results = []
    seen_ids = set()
    for req_id, url in search_responses:
        if not req_id or req_id in seen_ids:
            continue
        seen_ids.add(req_id)
        try:
            body_obj = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": req_id})
            body = body_obj.get("body", "")
            if body_obj.get("base64Encoded"):
                body = base64.b64decode(body).decode("utf-8", errors="ignore")
            results.append({"url": url, "body_text": body})
        except Exception as e:
            logger.log(f"⚠️ Không đọc được body cho {url[:100]}… | {e}")

    logger.log(f"Đã lấy body cho {len(results)}/{len(search_responses)} phản hồi 'search?'.")
    return results


# ======================================================
# 5) TRÍCH XUẤT EMAIL & LINK LIÊN HỆ
# ======================================================
EMAIL_RE = re.compile(
    r'(?i)(?<![\w.\-])([A-Z0-9._%+\-]+@[A-Z0-9\-]+(?:\.[A-Z0-9\-]+)+)(?![\w.\-])'
)

def normalize_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip().strip('"')
    try:
        p = urlparse(u)
        if not p.scheme:
            return "http://" + u
        return u
    except Exception:
        return u

def canonical_domain(u: str) -> str:
    try:
        host = urlparse(normalize_url(u)).hostname or ""
    except Exception:
        host = ""
    if host.startswith("www."):
        host = host[4:]
    return host.lower()

def fetch_html(url: str, timeout: int = 15) -> str:
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept-Language": "vi,vi-VN;q=0.9,en;q=0.8",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def sanitize_email(s: str) -> str:
    return s.strip().strip('.,;:!?)]}\'"')

def extract_emails_from_html(html: str) -> list[str]:
    emails = set()
    emails.update(EMAIL_RE.findall(html))
    soup = BeautifulSoup(html, "html.parser")
    emails.update(EMAIL_RE.findall(soup.get_text("\n", strip=True)))
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().startswith("mailto:"):
            addr = href.split(":", 1)[1].split("?", 1)[0]
            emails.update(EMAIL_RE.findall(addr))
    emails = {sanitize_email(e) for e in emails}
    return sorted(emails, key=str.lower)

def deobfuscate_email_text(text: str) -> str:
    t = text
    t = re.sub(r"\s*\[\s*at\s*\]\s*", "@", t, flags=re.I)
    t = re.sub(r"\s*\(\s*at\s*\)\s*", "@", t, flags=re.I)
    t = re.sub(r"\s*\[\s*dot\s*\]\s*", ".", t, flags=re.I)
    t = re.sub(r"\s*\(\s*dot\s*\)\s*", ".", t, flags=re.I)
    t = t.replace(" at ", "@").replace(" dot ", ".")
    return t

def _short(u: str) -> str:
    try:
        p = urlparse(u)
        path = (p.path or "/").rstrip("/")
        if len(path) > 24:
            path = path[:24] + "…"
        return f"{p.netloc}{path}"
    except Exception:
        return u

def pick_contact_links_from_html(base_url: str, html: str, hints: list[str], limit=6) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    for unwanted_section in soup.select('div.data-protect-box'):
        unwanted_section.decompose()
    seen, out = set(), []
    hints_l = [h.lower() for h in hints if h.strip()]
    all_links = soup.find_all("a", href=True)
    for hint in hints_l:
        for a in all_links:
            href = a["href"].strip()
            abs_u = requests.compat.urljoin(base_url, href)
            if abs_u in seen: 
                continue
            if href.lower().startswith(("mailto:", "tel:")):
                continue
            low_href = abs_u.lower()
            low_text = (a.get_text(" ", strip=True) or "").lower()
            if hint in low_href or hint in low_text:
                out.append(abs_u)
                seen.add(abs_u)
                if len(out) >= limit:
                    return out
    return out


# ================== Selenium helpers (tối ưu tốc độ) ==================
def try_accept_cookies(driver, wait_click=4):
    candidates = [
        "//button[contains(translate(., 'ACEPTAROKIJN', 'aceptarokijn'), 'ok')]",
        "//button[contains(translate(., 'ACCEPTALLOWALLEINVERSTANDEN', 'acceptallowalleinverstanden'), 'accept')]",
        "//button[contains(translate(., 'ACCEPTALLOWALLEINVERSTANDEN', 'acceptallowalleinverstanden'), 'allow')]",
        "//button[contains(translate(., 'AKZEPTIERENZUSTIMMEN', 'akzeptierenzustimmen'), 'akzeptieren')]",
        "//button[contains(translate(., 'AKZEPTIERENZUSTIMMEN', 'akzeptierenzustimmen'), 'zustimmen')]",
        "//button[contains(., 'Chấp nhận')]",
        "//button[contains(., 'Đồng ý')]",
        "//button[contains(., 'Tôi đồng ý')]",
        "//button[contains(., 'Accept')]",
        "//button[contains(., 'I agree')]",
        "//button[contains(., 'Alle akzeptieren')]",
        "//button[contains(., 'Allow all')]",
    ]
    try:
        for xp in candidates:
            try:
                el = WebDriverWait(driver, wait_click).until(EC.element_to_be_clickable((By.XPATH, xp)))
                el.click()
                time.sleep(0.2)
                break
            except Exception:
                continue
    except Exception:
        pass

def selenium_emails_from_current_page(driver) -> list:
    html = driver.page_source or ""
    html = deobfuscate_email_text(html)
    emails = set(extract_emails_from_html(html))
    try:
        anchors = driver.find_elements(By.XPATH, "//a[starts-with(translate(@href, 'MAILTO', 'mailto'), 'mailto')]")
        for a in anchors:
            href = (a.get_attribute("href") or "")
            if href.lower().startswith("mailto:"):
                addr = href.split(":",1)[1].split("?",1)[0]
                emails.update(EMAIL_RE.findall(addr))
    except Exception:
        pass
    return sorted({sanitize_email(e) for e in emails}, key=str.lower)

def selenium_pick_contact_links_on_home(driver, base_url: str, hints: list[str], limit=5) -> list[str]:
    # đọc từ DOM để nhanh, tránh parse lại bằng BS4
    seen, out = set(), []
    hints_l = [h.lower() for h in hints if h.strip()]

    def abs_url(href: str) -> str:
        try:
            return requests.compat.urljoin(base_url, href)
        except Exception:
            return href

    try:
        anchors = driver.find_elements(By.TAG_NAME, "a")
    except Exception:
        return out

    for hint in hints_l:
        for a in anchors:
            try:
                href = (a.get_attribute("href") or "").strip()
                text = (a.text or "").strip()
            except Exception:
                continue
            if not href or href.lower().startswith(("mailto:", "tel:")):
                continue
            u_abs = abs_url(href)
            key = u_abs.lower()
            if key in seen:
                continue
            low_href = key
            low_text = text.lower()
            if hint in low_href or hint in low_text:
                out.append(u_abs)
                seen.add(key)
                if len(out) >= limit:
                    return out
    return out


# ================== Pha 1 (Requests) – CHỈ LIÊN HỆ, KHÔNG HOME ==================
def request_phase_contact_only(base_url: str, contact_hints: list[str], blocklist: list[str], on_status,
                               timeout=12, per_site_contact_limit=6) -> tuple[list[str], str]:
    """
    Trả về (emails, source_tag). 
    Chỉ thử các link 'liên hệ' bằng requests (không lấy Home).
    """
    base = normalize_url(base_url)
    host = canonical_domain(base)
    try:
        on_status(f"REQ: tải HOME {_short(base)} (chỉ để tìm link)…")
        html0 = fetch_html(base, timeout=timeout)
    except Exception:
        on_status("REQ: lỗi tải HOME – bỏ qua")
        return [], None

    # tìm link liên hệ từ HTML (theo thứ tự hints người dùng), sau đó requests từng trang
    links = pick_contact_links_from_html(base, html0, contact_hints, limit=per_site_contact_limit)
    if not links:
        on_status("REQ: không tìm thấy link liên hệ")
        return [], None

    on_status(f"REQ: thử {len(links)} trang liên hệ …")
    for u2 in links:
        on_status(f"REQ CONTACT {_short(u2)} – tải…")
        try:
            html = fetch_html(u2, timeout=timeout)
        except Exception:
            on_status("REQ CONTACT – lỗi mạng")
            continue
        txt = deobfuscate_email_text(html)
        emails = extract_emails_from_html(txt)
        # lọc blocklist theo chuỗi chứa trong email (ví dụ chặn png/jpg không cần thiết nhưng giữ logic cũ)
        if blocklist:
            low_bl = [b.lower() for b in blocklist if b]
            emails = [e for e in emails if not any(b in e.lower() for b in low_bl)]
        if emails:
            on_status(f"REQ CONTACT – có @ ({len(emails)}): {_short(u2)}")
            return sorted(set(emails), key=str.lower), f"Request CONTACT {_short(u2)}"
        else:
            on_status(f"REQ CONTACT {_short(u2)} – không @")

    on_status("REQ: không tìm thấy @ từ các trang liên hệ")
    return [], None


# ================== Pha 2 (Selenium) – LIÊN HỆ TRƯỚC, HOME CUỐI ==================
def selenium_phase_contact_then_home(driver, base_url: str, contact_hints: list[str], blocklist: list[str], on_status,
                                     wait_body=8, wait_click=5, sleep_per_page=0.8, contact_limit=4) -> tuple[list[str], str]:
    """
    Thứ tự:
      1) Mở HOME (để lấy link liên hệ) – KHÔNG trích email ngay
      2) Mở lần lượt các CONTACT link -> trích email
      3) Nếu vẫn chưa có, cuối cùng mới trích email từ HOME
    """
    base = normalize_url(base_url)
    on_status(f"SEL: mở HOME {_short(base)}")
    try:
        driver.get(base)
        try:
            WebDriverWait(driver, wait_body).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            pass
        try_accept_cookies(driver, wait_click=wait_click)
        time.sleep(min(1.2, sleep_per_page))
    except Exception:
        on_status("SEL: lỗi mở HOME")
        return [], None

    # 1) Tìm link liên hệ từ Home
    links = selenium_pick_contact_links_on_home(driver, base, contact_hints, limit=max(1, contact_limit))
    on_status(f"SEL: phát hiện {len(links)} link liên hệ")

    # 2) Mở từng link liên hệ để trích email
    for u2 in links:
        on_status(f"SEL CONTACT {_short(u2)} – mở…")
        try:
            driver.get(u2)
            try:
                WebDriverWait(driver, wait_body).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except TimeoutException:
                pass
            try_accept_cookies(driver, wait_click=wait_click)
            time.sleep(sleep_per_page)
            ems = selenium_emails_from_current_page(driver)
            # lọc blocklist
            if blocklist:
                low_bl = [b.lower() for b in blocklist if b]
                ems = [e for e in ems if not any(b in e.lower() for b in low_bl)]
            if ems:
                on_status(f"SEL CONTACT – có @ ({len(ems)})")
                return sorted(set(ems), key=str.lower), f"Selenium CONTACT {_short(u2)}"
            else:
                on_status("SEL CONTACT – không @")
        except Exception:
            on_status("SEL CONTACT – lỗi tải/trích")

    # 3) Cuối cùng mới trích Home
    on_status("SEL: cuối cùng thử trích HOME …")
    try:
        driver.get(base)
        try:
            WebDriverWait(driver, wait_body).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            pass
        try_accept_cookies(driver, wait_click=wait_click)
        time.sleep(sleep_per_page)
        ems = selenium_emails_from_current_page(driver)
        if blocklist:
            low_bl = [b.lower() for b in blocklist if b]
            ems = [e for e in ems if not any(b in e.lower() for b in low_bl)]
        if ems:
            on_status(f"SEL HOME – có @ ({len(ems)})")
            return sorted(set(ems), key=str.lower), "Selenium HOME"
        else:
            on_status("SEL HOME – không @")
    except Exception:
        on_status("SEL HOME – lỗi")

    return [], None


# ==========================================
# 6) HỘP THOẠI NHẬP NHIỀU DÒNG
# ==========================================
class MultiLineDialog(tk.Toplevel):
    def __init__(self, parent, title, prompt, initial_text=""):
        super().__init__(parent)
        self.title(title)
        self.result = None
        self.transient(parent)
        self.grab_set()

        ttk.Label(self, text=prompt).pack(anchor="w", padx=10, pady=(10, 2))
        self.txt = scrolledtext.ScrolledText(self, width=48, height=10, wrap="word")
        self.txt.pack(fill="both", expand=True, padx=10, pady=4)
        if initial_text:
            self.txt.insert("1.0", initial_text)

        btns = ttk.Frame(self); btns.pack(fill="x", padx=10, pady=10)
        ttk.Button(btns, text="OK", command=self.on_ok).pack(side="right")
        ttk.Button(btns, text="Hủy", command=self.on_cancel).pack(side="right", padx=6)

        self.bind("<Escape>", lambda e: self.on_cancel())
        self.txt.focus_set()
        self.geometry("+%d+%d" % (parent.winfo_rootx()+60, parent.winfo_rooty()+80))

    def on_ok(self):
        lines = [ln.strip() for ln in self.txt.get("1.0", "end").splitlines()]
        self.result = [ln for ln in lines if ln]
        self.destroy()

    def on_cancel(self):
        self.result = None
        self.destroy()


# ==========================================
# 7) ỨNG DỤNG TKINTER – BỐ CỤC 3 HÀNG + NOTEBOOK
# ==========================================
class MapsSearchHarvesterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Google Maps – Harvester (Multi-keyword + Email Smart Crawl)")
        self.root.geometry("1180x760")
        self.root.minsize(1024, 640)

        # ---- State ----
        self.cfg = ConfigManager.load()
        self.rows_data = []
        self._iid_to_index = {}
        self._index_to_iid = {}
        self.rows_lock = threading.Lock()
        self.harvest_thread = None
        self.cancel_event = threading.Event()
        self.cell_edit = None
        self.tooltip_window = None
        self.tooltip_job = None

        # Cache theo domain để chống trùng lặp (áp kết quả cho nhiều dòng)
        # domain_cache: { 'example.com': {'emails': [...], 'source': '...'} }
        self.domain_cache = {}

        container = ttk.Frame(root)
        container.pack(fill="both", expand=True)
        container.grid_columnconfigure(0, weight=1)
        container.grid_rowconfigure(0, weight=0, minsize=170)  # Thiết lập
        container.grid_rowconfigure(1, weight=1)              # Trung tâm (Notebook)
        container.grid_rowconfigure(1, weight=0, minsize=50)  # Nhật ký

        self.f_settings = ttk.Frame(container)
        self.f_center   = ttk.Frame(container)
        self.f_log      = ttk.Frame(container)
        self.f_settings.grid(row=0, column=0, sticky="nsew")
        self.f_center.grid(row=1, column=0, sticky="nsew")
        self.f_log.grid(row=2, column=0, sticky="nsew")

        self._build_settings(self.f_settings)
        self._build_center_notebook(self.f_center)
        self._build_log(self.f_log)

        self.logger.log("Đã tải cấu hình.")
        self._update_sel_header()

    # ---------- UI builders ----------
    def _build_settings(self, parent):
        cfg = ttk.LabelFrame(parent, text="⚙️ Thiết lập Tìm kiếm (Từ khóa chính + danh sách từ khóa phụ)", padding=8)
        cfg.pack(fill=tk.BOTH, expand=True)

        ttk.Label(cfg, text="Từ khóa chính:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=4)
        self.main_kw_var = tk.StringVar(value="")
        ttk.Entry(cfg, textvariable=self.main_kw_var, width=36).grid(row=0, column=1, sticky=tk.W, padx=5, pady=4)

        subs_frame = ttk.Frame(cfg); subs_frame.grid(row=0, column=2, rowspan=3, sticky=tk.NSEW, padx=(20, 5), pady=4)
        ttk.Label(subs_frame, text="Từ khóa phụ").pack(anchor=tk.W)
        self.sub_list = tk.Listbox(subs_frame, height=5, exportselection=False)
        self.sub_list.pack(fill=tk.BOTH, expand=True)
        btns = ttk.Frame(subs_frame); btns.pack(fill=tk.X, pady=(4,0))
        ttk.Button(btns, text="Thêm", command=self.add_sub_kw_multi, width=9).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="Sửa", command=self.edit_sub_kw, width=9).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="Xóa", command=self.del_sub_kw, width=9).pack(side=tk.LEFT, padx=2)
        ttk.Button(btns, text="Lưu", command=self.save_settings, width=9).pack(side=tk.RIGHT, padx=2)
        for kw in self.cfg.get("sub_keywords", []):
            self.sub_list.insert(tk.END, kw)

        ttk.Label(cfg, text="Số lần cuộn tối đa:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=4)
        self.max_scroll_var = tk.IntVar(value=int(self.cfg.get("max_scroll", 500)))
        ttk.Spinbox(cfg, from_=5, to=500, textvariable=self.max_scroll_var, width=8).grid(row=1, column=1, sticky=tk.W, padx=5)

        ttk.Label(cfg, text="Độ trễ mỗi lần cuộn (s):").grid(row=2, column=0, sticky=tk.W, padx=5, pady=4)
        self.delay_var = tk.DoubleVar(value=float(self.cfg.get("delay", 1)))
        ttk.Spinbox(cfg, from_=0.5, to=10.0, increment=0.1, textvariable=self.delay_var, width=8).grid(row=2, column=1, sticky=tk.W, padx=5)

        self.headless_var = tk.BooleanVar(value=bool(self.cfg.get("headless", True)))
        ttk.Checkbutton(cfg, text="Chạy ẩn trình duyệt (headless)", variable=self.headless_var).grid(row=1, column=3, sticky=tk.W, padx=10, pady=4)

        # tối ưu tải
        ttk.Label(cfg, text="Luồng requests tối đa:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=4)
        self.req_workers_var = tk.IntVar(value=int(self.cfg.get("request_workers", 4)))
        ttk.Spinbox(cfg, from_=1, to=8, textvariable=self.req_workers_var, width=8).grid(row=3, column=1, sticky=tk.W, padx=5)
        
        ttk.Label(cfg, text="Luồng Selenium tối đa:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=4)
        self.sel_workers_var = tk.IntVar(value=int(self.cfg.get("selenium_workers", 1)))
        ttk.Spinbox(cfg, from_=1, to=4, textvariable=self.sel_workers_var, width=8).grid(row=4, column=1, sticky=tk.W, padx=5)

        actions = ttk.Frame(parent); actions.pack(fill=tk.X, pady=(6, 4))
        self.btn_start = ttk.Button(actions, text="🚀 Bắt đầu thu thập (chạy song song theo từ khóa)", command=self.on_start)
        self.btn_start.pack(side=tk.LEFT)
        ttk.Button(actions, text="⏹ Dừng", command=self.on_cancel).pack(side=tk.LEFT, padx=6)

    def _build_center_notebook(self, parent):
        nb = ttk.Notebook(parent)
        nb.pack(fill="both", expand=True)

        # Tab 1: Danh sách & Lấy email
        tab_list = ttk.Frame(nb)
        nb.add(tab_list, text="Danh sách & Lấy email")

        toolbar = ttk.Frame(tab_list); toolbar.pack(fill="x", padx=8, pady=(8,6))
        toolbar.columnconfigure(4, weight=1)
        ttk.Button(toolbar, text="✔ Chọn tất cả", command=self.select_all).grid(row=0, column=0, padx=4, sticky="w")
        ttk.Button(toolbar, text="✖ Bỏ chọn", command=self.deselect_all).grid(row=0, column=1, padx=4, sticky="w")
        ttk.Button(toolbar, text="⛭ Chọn có Trang web", command=self.select_has_site).grid(row=0, column=2, padx=6, sticky="w")
        ttk.Button(toolbar, text="⛭ Chọn chưa có Email", command=self.select_no_email).grid(row=0, column=3, padx=6, sticky="w")
        self.btn_get_sel = ttk.Button(toolbar, text="📥 Lấy email (mục đã chọn)", command=self.get_emails_selected)
        self.btn_get_sel.grid(row=0, column=4, padx=8, sticky="e")
        self.btn_get_all = ttk.Button(toolbar, text="📥 Lấy tất cả", command=self.get_emails_all)
        self.btn_get_all.grid(row=0, column=5, padx=6, sticky="e")
        self.btn_export = ttk.Button(toolbar, text="💾 Xuất Excel (.xlsx)", command=self.export_excel)
        self.btn_export.grid(row=0, column=6, padx=6, sticky="e")

        treewrap = ttk.Frame(tab_list); treewrap.pack(fill="both", expand=True, padx=8, pady=(0,6))
        columns = ("sel", "kw", "name", "addr", "site", "email", "stat", "act")
        self.tree = ttk.Treeview(treewrap, columns=columns, show="headings", selectmode="browse")
        sy = ttk.Scrollbar(treewrap, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=sy.set)
        self.tree.heading("sel", text="Chọn ☐", command=self._toggle_all_from_header)
        self.tree.heading("kw", text="Từ khóa")
        self.tree.heading("name", text="Tên")
        self.tree.heading("addr", text="Địa chỉ")
        self.tree.heading("site", text="Trang web")
        self.tree.heading("email", text="Email")
        self.tree.heading("stat", text="Trạng thái")
        self.tree.heading("act", text="Hành động")
        self.tree.column("sel", width=68, anchor=tk.CENTER, stretch=False)
        self.tree.column("act", width=100, anchor=tk.CENTER, stretch=False)
        # Kích thước cột theo yêu cầu cũ
        self.tree.column("kw", anchor=tk.W, width=120)
        self.tree.column("name", anchor=tk.W, width=100)
        self.tree.column("addr", anchor=tk.W, width=100)
        self.tree.column("site", anchor=tk.W, width=200)
        self.tree.column("email", anchor=tk.W, width=100)
        self.tree.column("stat", anchor=tk.W, width=250)
        self.tree.grid(row=0, column=0, sticky="nsew")
        sy.grid(row=0, column=1, sticky="ns")
        treewrap.rowconfigure(0, weight=1)
        treewrap.columnconfigure(0, weight=1)

        self.col_ids = columns
        self.col_index_map = {cid: i+1 for i, cid in enumerate(columns)}  # 1-based
        self.tree.bind("<Button-1>", self.on_tree_click)
        self.tree.bind("<Button-3>", self.on_right_click)
        self.tree.bind("<space>", self.on_space_toggle)
        self.tree.bind("<Motion>", self.on_motion)
        self.tree.bind("<Leave>", self.on_leave_tree)
        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<MouseWheel>", lambda e: self.hide_cell_editor())

        prowf = ttk.Frame(tab_list); prowf.pack(fill="x", padx=8, pady=(0,8))
        self.mail_progress = tk.DoubleVar(value=0)
        ttk.Progressbar(prowf, variable=self.mail_progress, maximum=100).pack(fill=tk.X, expand=True, side=tk.LEFT, padx=(0, 10))
        self.mail_status = tk.StringVar(value="Chưa có dữ liệu.")
        ttk.Label(prowf, textvariable=self.mail_status).pack(side=tk.RIGHT)

        # Context menu bảng
        self.ctx = tk.Menu(self.root, tearoff=0)
        self.ctx.add_command(label="Copy ô", command=self.copy_cell)
        self.ctx.add_command(label="Copy dòng", command=self.copy_row)
        self.ctx.add_separator()
        self.ctx.add_command(label="Mở website", command=self.ctx_open_site)

        # Tab 2: Cấu hình Email (liên hệ + blocklist)
        tab_cfg = ttk.Frame(nb)
        nb.add(tab_cfg, text="Cấu hình Email")

        cfg2 = ttk.Frame(tab_cfg); cfg2.pack(fill="both", expand=True, padx=8, pady=8)
        cfg2.columnconfigure(0, weight=1)
        cfg2.columnconfigure(1, weight=1)
        cfg2.rowconfigure(0, weight=1)

        # ------ Từ khóa liên hệ ------
        left = ttk.Frame(cfg2)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left.rowconfigure(1, weight=1)
        left.columnconfigure(0, weight=1)

        ttk.Label(left, text="Từ khóa 'liên hệ' (chỉ theo thứ tự trên xuống dưới)").grid(row=0, column=0, sticky="w")
        hints_frame = ttk.Frame(left)
        hints_frame.grid(row=1, column=0, sticky="nsew", pady=(4, 0))
        hints_frame.rowconfigure(0, weight=1)
        hints_frame.columnconfigure(0, weight=1)

        self.hints_list = tk.Listbox(hints_frame, height=12, exportselection=False)
        self.hints_list.grid(row=0, column=0, sticky="nsew")
        hints_scrollbar = ttk.Scrollbar(hints_frame, orient="vertical", command=self.hints_list.yview)
        hints_scrollbar.grid(row=0, column=1, sticky="ns")
        self.hints_list['yscrollcommand'] = hints_scrollbar.set
        for h in self.cfg.get("contact_hints", []):
            self.hints_list.insert(tk.END, h)

        hb = ttk.Frame(left)
        hb.grid(row=2, column=0, sticky="ew", pady=(6,0))
        ttk.Button(hb, text="Thêm", width=10, command=self.add_hint_multi).pack(side=tk.LEFT, padx=2)
        ttk.Button(hb, text="Sửa",  width=10, command=self.edit_hint).pack(side=tk.LEFT, padx=2)
        ttk.Button(hb, text="Xóa",  width=10, command=self.del_hint).pack(side=tk.LEFT, padx=2)
        ttk.Button(hb, text="Lưu",  width=10, command=self.save_settings).pack(side=tk.RIGHT, padx=2)

        self.hints_ctx = tk.Menu(self.root, tearoff=0)
        self.hints_ctx.add_command(label="Thêm…", command=self.add_hint_multi)
        self.hints_ctx.add_command(label="Sửa…", command=self.edit_hint)
        self.hints_ctx.add_command(label="Xóa", command=self.del_hint)
        self.hints_ctx.add_separator()
        self.hints_ctx.add_command(label="Lưu", command=self.save_settings)
        self.hints_list.bind("<Button-3>", lambda e: (self.hints_list.focus_set(), self.hints_ctx.post(e.x_root, e.y_root)))
        self.hints_list.bind("<Double-1>", lambda e: self.edit_hint())
        self.hints_list.bind("<F2>",       lambda e: self.edit_hint())
        self.hints_list.bind("<Insert>",   lambda e: self.add_hint_multi())
        self.hints_list.bind("<Delete>",   lambda e: self.del_hint())
        self.hints_list.bind("<Control-s>",lambda e: self.save_settings())

        # ------ Danh sách loại bỏ ------
        right = ttk.Frame(cfg2)
        right.grid(row=0, column=1, sticky="nsew")
        right.rowconfigure(1, weight=1)
        right.columnconfigure(0, weight=1)

        ttk.Label(right, text="Danh sách loại bỏ (email chứa chuỗi sẽ bị loại)").grid(row=0, column=0, sticky="w")
        block_frame = ttk.Frame(right)
        block_frame.grid(row=1, column=0, sticky="nsew", pady=(4, 0))
        block_frame.rowconfigure(0, weight=1)
        block_frame.columnconfigure(0, weight=1)

        self.block_list = tk.Listbox(block_frame, height=12, exportselection=False)
        self.block_list.grid(row=0, column=0, sticky="nsew")
        block_scrollbar = ttk.Scrollbar(block_frame, orient="vertical", command=self.block_list.yview)
        block_scrollbar.grid(row=0, column=1, sticky="ns")
        self.block_list['yscrollcommand'] = block_scrollbar.set
        for b in self.cfg.get("blocklist", []):
            self.block_list.insert(tk.END, b)

        rb = ttk.Frame(right)
        rb.grid(row=2, column=0, sticky="ew", pady=(6,0))
        ttk.Button(rb, text="Thêm", width=10, command=self.add_block_multi).pack(side=tk.LEFT, padx=2)
        ttk.Button(rb, text="Sửa",  width=10, command=self.edit_block).pack(side=tk.LEFT, padx=2)
        ttk.Button(rb, text="Xóa",  width=10, command=self.del_block).pack(side=tk.LEFT, padx=2)
        ttk.Button(rb, text="Lưu",  width=10, command=self.save_settings).pack(side=tk.RIGHT, padx=2)

    def _build_log(self, parent):
        logf = ttk.LabelFrame(parent, text="📜 Nhật ký", padding=6)
        logf.pack(fill=tk.BOTH, expand=True)
        self.log_text = scrolledtext.ScrolledText(logf, wrap=tk.WORD, state="disabled", font=("Consolas", 10))
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.status_var = tk.StringVar(value="Sẵn sàng.")
        self.logger = UILogger(self.log_text, self.status_var)
        ttk.Label(logf, textvariable=self.status_var).pack(anchor=tk.W, pady=(4,0))

    # ---------- Tooltip & helpers ----------
    def on_leave_tree(self, event=None):
        if self.tooltip_job:
            self.root.after_cancel(self.tooltip_job)
            self.tooltip_job = None
        if self.tooltip_window:
            self.tooltip_window.destroy()
            self.tooltip_window = None

    def show_tooltip(self, event, row_id, col_id):
        self.on_leave_tree()
        col_index = int(col_id.replace('#', ''))
        vals = self.tree.item(row_id, "values")
        if not vals or col_index > len(vals):
            return
        full_text = vals[col_index - 1]
        if not full_text:
            return

        col_width = self.tree.column(col_id, "width")
        font = tkfont.Font(font=self.tree.cget("font"))
        text_width = font.measure(full_text)
        if text_width < col_width - 10:
            return

        x = event.x_root + 15
        y = event.y_root + 10

        self.tooltip_window = tw = tk.Toplevel(self.root)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")

        label = ttk.Label(tw, text=full_text, justify=tk.LEFT,
                          background="#ffffe0", relief=tk.SOLID, borderwidth=1,
                          wraplength=400)
        label.pack(ipadx=4, ipady=2)

    def _open_url(self, url):
        if url and url.strip() and url.strip().lower().startswith(("http://", "https://")):
            webbrowser.open(url.strip())

    def _find_iid_by_index(self, idx: int):
        iid = self._index_to_iid.get(idx)
        if iid:
            return iid
        for k, v in self._iid_to_index.items():
            if v == idx:
                self._index_to_iid[idx] = k
                return k
        return None

    def set_row_status(self, idx: int, text: str):
        self.rows_data[idx]["Trạng thái"] = text
        iid = self._find_iid_by_index(idx)
        if iid and self.tree.exists(iid):
            vals = list(self.tree.item(iid, "values"))
            vals[6] = text
            self.tree.item(iid, values=vals)

    def set_row_action_text(self, idx: int, text: str):
        iid = self._find_iid_by_index(idx)
        if iid and self.tree.exists(iid):
            vals = list(self.tree.item(iid, "values"))
            vals[7] = text
            self.tree.item(iid, values=vals)

    def set_row_email(self, idx: int, email_str: str):
        self.rows_data[idx]["Email"] = email_str
        iid = self._find_iid_by_index(idx)
        if iid and self.tree.exists(iid):
            vals = list(self.tree.item(iid, "values"))
            vals[5] = email_str
            self.tree.item(iid, values=vals)

    # ---------- Tự canh chiều rộng cột ----------
    def _autosize_columns(self, event=None):
        try:
            tree_w = self.tree.winfo_width()
            w_sel = int(self.tree.column("sel", option="width"))
            w_act = int(self.tree.column("act", option="width"))
            fixed = w_sel + w_act
            padding = 18
            avail = max(300, tree_w - fixed - padding)
            weights = {"kw":1.0, "name":1.3, "addr":2.1, "site":1.6, "email":1.5, "stat":0.5}
            total = sum(weights.values())
            for cid, wt in weights.items():
                w = int(avail * wt / total)
                self.tree.column(cid, width=max(80, w))
        except Exception:
            pass

    # ---------- Listbox helpers ----------
    def _ask_lines(self, title, prompt, initial=""):
        dlg = MultiLineDialog(self.root, title, prompt, initial_text=initial)
        self.root.wait_window(dlg)
        return dlg.result

    def _lb_selected_or_all(self, lb: tk.Listbox):
        sel = list(lb.curselection())
        if sel:
            return sel
        return list(range(lb.size()))

    def _lb_edit_selected_multi(self, lb: tk.Listbox, title: str):
        sel = list(lb.curselection())
        if not sel:
            messagebox.showinfo("Chưa chọn", "Hãy chọn ít nhất 1 mục để sửa.")
            return
        initial = "\n".join(lb.get(i) for i in sel)
        lines = self._ask_lines(title, "Nhập nội dung mới (mỗi dòng 1 mục):", initial_text=initial)
        if lines is None:
            return
        start = sel[0]
        for i in reversed(sel):
            lb.delete(i)
        for idx, ln in enumerate(lines):
            lb.insert(start + idx, ln)

    def _lb_delete_selected(self, lb: tk.Listbox):
        sel = list(lb.curselection())
        if not sel:
            messagebox.showinfo("Chưa chọn", "Hãy chọn ít nhất 1 mục để xóa.")
            return
        for i in reversed(sel):
            lb.delete(i)

    # ---------- List editors ----------
    def add_sub_kw_multi(self):
        lines = self._ask_lines("Thêm từ khóa phụ", "Nhập mỗi dòng 1 từ khóa:")
        if lines:
            for s in lines:
                self.sub_list.insert(tk.END, s)

    def edit_sub_kw(self):
        self._lb_edit_selected_multi(self.sub_list, "Sửa từ khóa phụ")

    def del_sub_kw(self):
        self._lb_delete_selected(self.sub_list)

    def add_hint_multi(self):
        lines = self._ask_lines("Thêm từ khóa liên hệ", "Nhập mỗi dòng 1 từ khóa:")
        if lines:
            for s in lines:
                self.hints_list.insert(tk.END, s)

    def edit_hint(self):
        self._lb_edit_selected_multi(self.hints_list, "Sửa từ khóa liên hệ")

    def del_hint(self):
        self._lb_delete_selected(self.hints_list)

    def add_block_multi(self):
        lines = self._ask_lines("Thêm chuỗi loại bỏ", "Nhập mỗi dòng 1 chuỗi:")
        if lines:
            for s in lines:
                self.block_list.insert(tk.END, s)

    def edit_block(self):
        self._lb_edit_selected_multi(self.block_list, "Sửa chuỗi loại bỏ")

    def del_block(self):
        self._lb_delete_selected(self.block_list)

    def save_settings(self):
        self.cfg["sub_keywords"] = [self.sub_list.get(i) for i in range(self.sub_list.size())]
        self.cfg["contact_hints"] = [self.hints_list.get(i) for i in range(self.hints_list.size())]
        self.cfg["blocklist"] = [self.block_list.get(i) for i in range(self.block_list.size())]
        self.cfg["headless"] = bool(self.headless_var.get())
        self.cfg["max_scroll"] = int(self.max_scroll_var.get())
        self.cfg["delay"] = float(self.delay_var.get())
        self.cfg["request_workers"] = int(self.req_workers_var.get())
        self.cfg["selenium_workers"] = int(self.sel_workers_var.get())
        if ConfigManager.save(self.cfg):
            self.logger.log("💾 Đã lưu cấu hình.")

    # ---------- Harvest ----------
    def on_cancel(self):
        self.cancel_event.set()
        self.logger.log("⏹ Đã yêu cầu dừng quá trình.")

    def on_start(self):
        main_kw = self.main_kw_var.get().strip()
        if not main_kw:
            messagebox.showerror("Thiếu từ khóa chính", "Vui lòng nhập từ khóa chính.")
            return
        if self.harvest_thread and self.harvest_thread.is_alive():
            messagebox.showwarning("Đang chạy", "Quá trình trước chưa hoàn tất.")
            return

        self.cancel_event.clear()
        self.btn_start.config(state="disabled", text="⏳ Đang chạy…")
        self.log_text.configure(state="normal"); self.log_text.delete("1.0", tk.END); self.log_text.configure(state="disabled")
        with self.rows_lock:
            self.rows_data = []
            self.domain_cache.clear()
        self.clear_tree()

        sub_kws = [self.sub_list.get(i).strip() for i in range(self.sub_list.size()) if self.sub_list.get(i).strip()]
        queries = [(main_kw, sk) for sk in sub_kws] if sub_kws else [(main_kw, None)]

        self.harvest_thread = threading.Thread(target=self.run_harvest_multi, args=(queries,), daemon=True)
        self.harvest_thread.start()

    def run_harvest_multi(self, queries):
        headless = bool(self.headless_var.get())
        max_rounds = int(self.max_scroll_var.get())
        delay = float(self.delay_var.get())

        self.logger.log(f"Bắt đầu thu thập {len(queries)} luồng tìm kiếm…")
        done = 0
        max_workers = min(3, len(queries))
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futs = [ex.submit(self.harvest_one_query, mk, sk, headless, max_rounds, delay) for (mk, sk) in queries]
                for fut in as_completed(futs):
                    if self.cancel_event.is_set():
                        break
                    try:
                        rows = fut.result()
                    except Exception as e:
                        self.logger.log(f"⚠️ Lỗi luồng: {e}")
                        rows = []
                    if rows:
                        self.append_rows(rows)
                    done += 1
                    self.logger.status(f"Đã xong {done}/{len(queries)} nhóm từ khóa.")
        finally:
            self.root.after(0, lambda: self.btn_start.config(state="normal", text="🚀 Bắt đầu thu thập (chạy song song theo từ khóa)"))
            if self.cancel_event.is_set():
                self.logger.log("⏹ Đã dừng theo yêu cầu.")
            else:
                self.logger.log("✅ Hoàn tất giai đoạn Maps.")
            self.root.after(0, lambda: self.mail_status.set("Đã nạp danh sách. Chọn/Lấy email."))

    def harvest_one_query(self, main_kw, sub_kw, headless, max_rounds, delay):
        if self.cancel_event.is_set():
            return []
        kw_label = f"{main_kw} | {sub_kw}" if sub_kw else main_kw
        q = f"{main_kw} {sub_kw}".strip() if sub_kw else main_kw

        driver = None
        rows = []
        try:
            driver = build_driver(headless=headless)
            maps_url = f"https://www.google.com/maps/search/{q.replace(' ', '+')}"
            self.logger.log(f"[{kw_label}] Mở Maps: {maps_url}")
            driver.get(maps_url)

            try:
                WebDriverWait(driver, 12).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']")))
            except TimeoutException:
                self.logger.log(f"[{kw_label}] ⚠️ Trang không hiển thị feed đúng hạn, vẫn tiếp tục cuộn…")

            if self.cancel_event.is_set():
                return []

            ended = scroll_until_end(driver, self.logger, max_rounds=max_rounds, delay=delay)

            if self.cancel_event.is_set():
                return []

            self.logger.log(f"[{kw_label}] Đang thu thập các phản hồi 'search?' …")
            bodies = collect_search_bodies_via_perflog(driver, self.logger)
            if not bodies:
                self.logger.log(f"[{kw_label}] ⚠️ Không tìm thấy response nào chứa 'search?'.")
            else:
                self.logger.log(f"[{kw_label}] Đã thu {len(bodies)} response 'search?'.")

            big_text_parts = []
            cleaned_count = 0
            for item in bodies:
                obj, cleaned_text = clean_google_maps_body(item.get("body_text", ""))
                if obj is not None:
                    big_text_parts.append(json.dumps(obj, ensure_ascii=False))
                    cleaned_count += 1
                else:
                    big_text_parts.append(cleaned_text)
            self.logger.log(f"[{kw_label}] Đã 'làm đẹp' được {cleaned_count}/{len(bodies)} response.")

            big_text = "\n".join(big_text_parts)
            rows = extract_rows_from_text(big_text, kw_label)
            self.logger.log(f"[{kw_label}] ✅ Trích xuất được {len(rows)} mục (Tên/Địa chỉ/Trang web).")

            if ended:
                self.logger.log(f"[{kw_label}] Hoàn tất Maps (đã cuộn tới cuối danh sách).")
            else:
                self.logger.log(f"[{kw_label}] Hoàn tất Maps (dừng do đạt số lần cuộn tối đa).")

        except Exception as e:
            import traceback
            self.logger.log(f"[{kw_label}] ❌ Lỗi: {e}")
            self.logger.log(traceback.format_exc())
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
        return rows

    # ---------- Bảng + chọn ----------
    def clear_tree(self):
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        self._iid_to_index.clear()
        self._index_to_iid.clear()
        self._update_sel_header()

    def append_rows(self, new_rows):
        with self.rows_lock:
            existing = {(r.get("Tên",""), r.get("Trang web","")) for r in self.rows_data}
            to_add = [r for r in new_rows if (r.get("Tên",""), r.get("Trang web","")) not in existing]
            base_index = len(self.rows_data)
            self.rows_data.extend(to_add)

        def ui():
            for off, row in enumerate(to_add):
                sel_txt = "☑" if row.get("Chọn") else "☐"
                act = "Lấy ▶"
                iid = self.tree.insert("", tk.END, values=(
                    sel_txt, row.get("Từ khóa",""), row["Tên"], row["Địa chỉ"],
                    row["Trang web"], row.get("Email",""), row.get("Trạng thái","Chưa lấy"), act
                ))
                idx = base_index + off
                self._iid_to_index[iid] = idx
                self._index_to_iid[idx] = iid
            self._update_sel_header()
            self._autosize_columns()
        self.root.after(0, ui)

    def _update_sel_header(self):
        total = len(self.rows_data)
        marked = sum(1 for r in self.rows_data if r.get("Chọn"))
        if total == 0 or marked == 0:
            txt = "Chọn ☐"
        elif marked == total:
            txt = "Chọn ☑"
        else:
            txt = "Chọn ◩"
        self.tree.heading("sel", text=txt)

    def _toggle_all_from_header(self):
        total = len(self.rows_data)
        marked = sum(1 for r in self.rows_data if r.get("Chọn"))
        target = (marked != total)
        for iid in self.tree.get_children():
            idx = self._iid_to_index[iid]
            self.rows_data[idx]["Chọn"] = target
            vals = list(self.tree.item(iid, "values"))
            vals[0] = "☑" if target else "☐"
            self.tree.item(iid, values=vals)
        self._update_sel_header()

    # ---- Overlay editor cho copy theo ô
    def show_cell_editor(self, row_id, col_index):
        self.hide_cell_editor()
        if col_index in (self.col_index_map["sel"], self.col_index_map["act"]):
            return
        bbox = self.tree.bbox(row_id, f"#{col_index}")
        if not bbox:
            return
        x, y, w, h = bbox
        vals = list(self.tree.item(row_id, "values"))
        if col_index-1 >= len(vals):
            return
        text = str(vals[col_index-1])

        self.cell_edit = tk.Entry(self.tree, relief="solid", borderwidth=1, readonlybackground="white")
        self.cell_edit.place(x=x+1, y=y+1, width=w-2, height=h-2)
        self.cell_edit.insert(0, text)
        self.cell_edit.select_range(0, "end")
        self.cell_edit.focus_set()
        self.cell_edit.configure(state="readonly")

        def copy_all(event=None):
            try:
                sel = self.cell_edit.selection_get()
            except Exception:
                sel = self.cell_edit.get()
            self.root.clipboard_clear()
            self.root.clipboard_append(sel)
            return "break"

        self.cell_edit.bind("<Control-c>", copy_all)
        self.cell_edit.bind("<Escape>", lambda e: self.hide_cell_editor())
        self.cell_edit.bind("<Return>", lambda e: self.hide_cell_editor())
        self.cell_edit.bind("<FocusOut>", lambda e: self.hide_cell_editor())

    def hide_cell_editor(self):
        if self.cell_edit is not None:
            try:
                self.cell_edit.destroy()
            except Exception:
                pass
            self.cell_edit = None

    # ---- Events
    def on_tree_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)
        if region != "cell" or not row_id or not col_id:
            self.hide_cell_editor()
            return

        col_index = int(col_id[1:])
        col_sel = self.col_index_map["sel"]
        col_act = self.col_index_map["act"]

        if col_index == col_sel:
            idx = self._iid_to_index.get(row_id)
            if idx is not None:
                self.rows_data[idx]["Chọn"] = not self.rows_data[idx].get("Chọn", False)
                vals = list(self.tree.item(row_id, "values"))
                vals[0] = "☑" if self.rows_data[idx]["Chọn"] else "☐"
                self.tree.item(row_id, values=vals)
                self._update_sel_header()
            self.hide_cell_editor()
            return "break"

        if col_index == col_act:
            idx = self._iid_to_index.get(row_id)
            if idx is not None:
                self.get_email_for_one(idx, row_id)
            self.hide_cell_editor()
            return "break"

        self.show_cell_editor(row_id, col_index)

    def on_double_click(self, event):
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)
        if not row_id or not col_id:
            return
        col_index = int(col_id[1:])
        if col_index == self.col_index_map["site"]:
            idx = self._iid_to_index.get(row_id)
            if idx is not None:
                url = self.rows_data[idx].get("Trang web", "")
                self._open_url(url)

    def on_right_click(self, event):
        row_id = self.tree.identify_row(event.y)
        if row_id:
            self.tree.selection_set(row_id)
            self.ctx_row = self._iid_to_index.get(row_id)
            self.ctx.post(event.x_root, event.y_root)

    def on_space_toggle(self, event):
        focus = self.tree.focus()
        if focus:
            idx = self._iid_to_index.get(focus)
            if idx is not None:
                self.rows_data[idx]["Chọn"] = not self.rows_data[idx].get("Chọn", False)
                vals = list(self.tree.item(focus, "values"))
                vals[0] = "☑" if self.rows_data[idx]["Chọn"] else "☐"
                self.tree.item(focus, values=vals)
                self._update_sel_header()
                return "break"

    def on_motion(self, event):
        region = self.tree.identify("region", event.x, event.y)
        col_id = self.tree.identify_column(event.x)
        col_index = int(col_id[1:]) if col_id else -1
        if region == "cell" and col_index in (self.col_index_map.get("act"), self.col_index_map.get("site")):
            self.tree.configure(cursor="hand2")
        else:
            self.tree.configure(cursor="")
        if self.tooltip_job:
            self.root.after_cancel(self.tooltip_job)
            self.tooltip_job = None
        self.on_leave_tree()
        row_id = self.tree.identify_row(event.y)
        col_id_str = self.tree.identify_column(event.x)
        if row_id and col_id_str:
            self.tooltip_job = self.root.after(500, lambda: self.show_tooltip(event, row_id, col_id_str))

    def copy_cell(self):
        if self.cell_edit is not None:
            try:
                txt = self.cell_edit.selection_get()
            except Exception:
                txt = self.cell_edit.get()
            self.root.clipboard_clear()
            self.root.clipboard_append(txt)
            return
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]
        vals = self.tree.item(iid, "values")
        text = vals[5] if len(vals) > 5 else ""
        self.root.clipboard_clear()
        self.root.clipboard_append(text)

    def copy_row(self):
        sel = self.tree.selection()
        if not sel: return
        iid = sel[0]; vals = self.tree.item(iid, "values")
        rowtxt = "\t".join(str(v) for v in vals)
        self.root.clipboard_clear()
        self.root.clipboard_append(rowtxt)

    def ctx_open_site(self):
        if getattr(self, "ctx_row", None) is None: return
        url = self.rows_data[self.ctx_row].get("Trang web","")
        self._open_url(url)

    # ---------- Chọn nhanh ----------
    def select_all(self):
        for iid in self.tree.get_children():
            idx = self._iid_to_index[iid]
            self.rows_data[idx]["Chọn"] = True
            vals = list(self.tree.item(iid, "values"))
            vals[0] = "☑"
            self.tree.item(iid, values=vals)
        self._update_sel_header()

    def deselect_all(self):
        for iid in self.tree.get_children():
            idx = self._iid_to_index[iid]
            self.rows_data[idx]["Chọn"] = False
            vals = list(self.tree.item(iid, "values"))
            vals[0] = "☐"
            self.tree.item(iid, values=vals)
        self._update_sel_header()

    def select_has_site(self):
        for iid in self.tree.get_children():
            idx = self._iid_to_index[iid]
            has = bool(self.rows_data[idx].get("Trang web"))
            self.rows_data[idx]["Chọn"] = has
            vals = list(self.tree.item(iid, "values"))
            vals[0] = "☑" if has else "☐"
            self.tree.item(iid, values=vals)
        self._update_sel_header()

    def select_no_email(self):
        for iid in self.tree.get_children():
            idx = self._iid_to_index[iid]
            no_email = not bool(self.rows_data[idx].get("Email"))
            self.rows_data[idx]["Chọn"] = no_email
            vals = list(self.tree.item(iid, "values"))
            vals[0] = "☑" if no_email else "☐"
            self.tree.item(iid, values=vals)
        self._update_sel_header()

    # ---------- Lấy email (2-pha với chống trùng domain) ----------
    def get_email_for_one(self, idx: int, row_id: str = None):
        # chạy 2 pha cho 1 dòng (vẫn dùng cache domain để tránh trùng)
        row = self.rows_data[idx]
        url = normalize_url(row.get("Trang web", ""))
        if not url:
            messagebox.showwarning("Thiếu URL", f"Dòng '{row.get('Tên','')}' không có trang web.")
            return

        domain = canonical_domain(url)
        if domain in self.domain_cache:
            # đã có kết quả từ domain khác → áp lại
            cached = self.domain_cache[domain]
            emails = cached.get("emails", [])
            source = cached.get("source", None)
            email_str = "; ".join(emails) if emails else ""
            self.set_row_email(idx, email_str)
            self.set_row_status(idx, f"Từ cache domain ({source or 'NA'})")
            self.set_row_action_text(idx, "Lấy lại ▶")
            return

        hints = [self.hints_list.get(i) for i in range(self.hints_list.size())]
        blocklist = [self.block_list.get(i) for i in range(self.block_list.size())]
        headless = bool(self.headless_var.get())

        def set_status(text):
            self.root.after(0, lambda: self.set_row_status(idx, text))

        def task():
            self.root.after(0, lambda: self.set_row_action_text(idx, "Đang lấy…"))
            self.root.after(0, lambda: self.mail_status.set(f"Đang lấy email: {row.get('Tên','')} …"))

            # PHA 1: REQUESTS – CHỈ LIÊN HỆ
            emails, source = request_phase_contact_only(
                url, hints, blocklist, on_status=set_status,
                timeout=12, per_site_contact_limit=6
            )

            # PHA 2: SELENIUM – LIÊN HỆ TRƯỚC, HOME CUỐI (nếu cần)
            if not emails:
                set_status("🔁 Chuyển Selenium (liên hệ trước, home cuối)…")
                driver = None
                try:
                    driver = build_driver(headless=headless)
                    sel_emails, sel_source = selenium_phase_contact_then_home(
                        driver, url, hints, blocklist, on_status=set_status,
                        wait_body=self.cfg.get("selenium_wait_body", 8),
                        wait_click=self.cfg.get("selenium_wait_click", 5),
                        sleep_per_page=self.cfg.get("selenium_sleep_per_page", 0.8),
                        contact_limit=self.cfg.get("selenium_contact_limit", 4)
                    )
                    if sel_emails:
                        emails, source = sel_emails, sel_source
                finally:
                    if driver:
                        try: driver.quit()
                        except Exception: pass

            # Áp kết quả + lưu cache domain
            emails = sorted(set(emails), key=str.lower)
            email_str = "; ".join(emails) if emails else ""
            self.root.after(0, lambda: self.set_row_email(idx, email_str))
            if emails:
                self.domain_cache[domain] = {"emails": emails, "source": source}
                set_status(f"Xong ({len(emails)}) – nguồn: {source}")
            else:
                set_status("Không thấy @")
            self.root.after(0, lambda: self.set_row_action_text(idx, "Lấy lại ▶"))
            self.root.after(0, lambda: self.mail_status.set("Xong."))

        threading.Thread(target=task, daemon=True).start()

    def selected_indices(self):
        idxs = []
        for iid in self.tree.get_children():
            idx = self._iid_to_index[iid]
            if self.rows_data[idx].get("Chọn"):
                idxs.append(idx)
        return idxs

    def _apply_email_result(self, idx, emails, source, is_cached=False):
        """Helper to apply results to the UI to avoid code duplication."""
        email_str = "; ".join(emails) if emails else ""
        self.set_row_email(idx, email_str)

        status = ""
        if emails:
            if is_cached:
                status = f"Từ cache ({source or 'NA'})"
            else:
                status = f"Xong ({len(emails)}) - {source or 'NA'}"
        else:
            if not is_cached:
                 status = "Không thấy @"
        
        if status:
            self.set_row_status(idx, status)
        self.set_row_action_text(idx, "Lấy lại ▶")

    def _run_jobs(self, idxs):
        """
        Runs email fetching in two concurrent phases, controlled by thread pools.
        """
        if not idxs:
            return

        jobs = []
        for idx in idxs:
            url = normalize_url(self.rows_data[idx].get("Trang web", ""))
            if url:
                jobs.append((idx, url))

        if not jobs:
            messagebox.showinfo("Không có URL", "Các dòng đã chọn không có trang web hợp lệ.")
            return

        # Get current settings
        hints = [self.hints_list.get(i) for i in range(self.hints_list.size())]
        blocklist = [self.block_list.get(i) for i in range(self.block_list.size())]
        headless = bool(self.headless_var.get())
        req_workers = max(1, self.req_workers_var.get())
        sel_workers = max(1, self.sel_workers_var.get())

        # This entire task runs in a background thread
        def task_wrapper():
            # Initial UI setup
            self.root.after(0, lambda: self.btn_get_sel.config(state="disabled"))
            self.root.after(0, lambda: self.btn_get_all.config(state="disabled"))
            self.cancel_event.clear()

            # --- Map unique domains to representative jobs to avoid duplicate work ---
            domain_to_rep_job = {}
            for idx, url in jobs:
                dom = canonical_domain(url)
                if dom not in domain_to_rep_job:
                    domain_to_rep_job[dom] = (idx, url)
            
            # Jobs that need processing (one per domain)
            rep_jobs = list(domain_to_rep_job.values())
            
            # Set initial status for all selected rows
            for idx, _ in jobs:
                self.root.after(0, lambda i=idx: self.set_row_action_text(i, "Đang lấy…"))

            # ====== PHASE 1: CONCURRENT REQUESTS ======
            total_rep_jobs = len(rep_jobs)
            done_count = 0
            self.root.after(0, lambda: self.mail_status.set(f"Pha 1 (Requests): 0/{total_rep_jobs}"))
            self.root.after(0, lambda: self.mail_progress.set(0))

            def req_task(idx, url):
                if self.cancel_event.is_set(): return None, None
                dom = canonical_domain(url)
                if dom in self.domain_cache and self.domain_cache[dom].get("emails"):
                    cached = self.domain_cache[dom]
                    return cached.get("emails", []), cached.get("source", "Cache")
                
                def on_status_update(text):
                    self.root.after(0, lambda: self.set_row_status(idx, text))

                return request_phase_contact_only(url, hints, blocklist, on_status=on_status_update)

            with ThreadPoolExecutor(max_workers=req_workers) as executor:
                future_to_job = {executor.submit(req_task, idx, url): (idx, url) for idx, url in rep_jobs}
                
                for future in as_completed(future_to_job):
                    if self.cancel_event.is_set(): break
                    rep_idx, url = future_to_job[future]
                    try:
                        emails, source = future.result()
                        if emails is not None:
                            dom = canonical_domain(url)
                            if emails and dom:
                                self.domain_cache[dom] = {"emails": emails, "source": source}
                    except Exception as e:
                        self.logger.log(f"Lỗi luồng request cho {url}: {e}")
                    
                    done_count += 1
                    prog = (done_count / total_rep_jobs) * 50
                    self.root.after(0, lambda p=prog, d=done_count, t=total_rep_jobs: (
                        self.mail_progress.set(p), self.mail_status.set(f"Pha 1 (Requests): {d}/{t}")
                    ))

            if self.cancel_event.is_set(): return

            # Propagate results from cache to all rows
            for idx, url in jobs:
                dom = canonical_domain(url)
                if dom in self.domain_cache:
                    cached = self.domain_cache[dom]
                    self.root.after(0, lambda i=idx, e=cached.get("emails", []), s=cached.get("source"): self._apply_email_result(i, e, s, is_cached=True))

            # ====== PHASE 2: CONCURRENT SELENIUM ======
            pending_domains = {canonical_domain(url) for idx, url in jobs if not self.rows_data[idx].get("Email")}
            if not pending_domains:
                self.root.after(0, lambda: self.mail_status.set("Hoàn tất! Không cần Pha 2."))
                self.root.after(0, lambda: self.mail_progress.set(100))
                return

            pending_rep_jobs = [(domain_to_rep_job[dom][0], domain_to_rep_job[dom][1]) for dom in pending_domains]
            total_sel = len(pending_rep_jobs)
            done_count = 0
            self.root.after(0, lambda: self.mail_status.set(f"Pha 2 (Selenium): 0/{total_sel}"))

            def sel_task(idx, url):
                if self.cancel_event.is_set(): return None, None
                driver = None
                try:
                    driver = build_driver(headless=headless)
                    def on_status_update(text):
                        self.root.after(0, lambda: self.set_row_status(idx, text))

                    return selenium_phase_contact_then_home(
                        driver, url, hints, blocklist, on_status=on_status_update,
                        wait_body=self.cfg.get("selenium_wait_body", 8),
                        wait_click=self.cfg.get("selenium_wait_click", 5),
                        sleep_per_page=self.cfg.get("selenium_sleep_per_page", 0.8),
                        contact_limit=self.cfg.get("selenium_contact_limit", 4)
                    )
                finally:
                    if driver:
                        try: driver.quit()
                        except: pass

            with ThreadPoolExecutor(max_workers=sel_workers) as executor:
                future_to_job = {executor.submit(sel_task, idx, url): (idx, url) for idx, url in pending_rep_jobs}

                for future in as_completed(future_to_job):
                    if self.cancel_event.is_set(): break
                    rep_idx, url = future_to_job[future]
                    try:
                        emails, source = future.result()
                        if emails is not None:
                            dom = canonical_domain(url)
                            if emails and dom:
                                self.domain_cache[dom] = {"emails": emails, "source": source}
                    except Exception as e:
                        self.logger.log(f"Lỗi luồng Selenium cho {url}: {e}")
                    
                    done_count += 1
                    prog = 50 + (done_count / total_sel) * 50
                    self.root.after(0, lambda p=prog, d=done_count, t=total_sel: (
                        self.mail_progress.set(p), self.mail_status.set(f"Pha 2 (Selenium): {d}/{t}")
                    ))
            
            # Final propagation for Selenium results
            for idx, url in jobs:
                 dom = canonical_domain(url)
                 if dom in self.domain_cache and not self.rows_data[idx].get("Email"):
                    cached = self.domain_cache[dom]
                    self.root.after(0, lambda i=idx, e=cached.get("emails", []), s=cached.get("source"): self._apply_email_result(i, e, s, is_cached=True))


        def final_cleanup():
            # Final check to reset status for any remaining "Đang lấy..." jobs
            for idx, _ in jobs:
                if self.tree.exists(self._find_iid_by_index(idx)):
                     vals = list(self.tree.item(self._find_iid_by_index(idx), "values"))
                     if vals[7] == "Đang lấy…":
                        self.set_row_action_text(idx, "Lấy lại ▶")

            self.btn_get_sel.config(state="normal")
            self.btn_get_all.config(state="normal")
            if self.cancel_event.is_set():
                self.mail_status.set("Đã dừng.")
                self.logger.log("Quá trình lấy email đã dừng.")
            else:
                self.mail_status.set("Hoàn tất trích xuất email.")
                self.mail_progress.set(100)

        def threaded_task_with_cleanup():
            try:
                task_wrapper()
            finally:
                self.root.after(0, final_cleanup)

        threading.Thread(target=threaded_task_with_cleanup, daemon=True).start()


    def get_emails_selected(self):
        idxs = self.selected_indices()
        if not idxs:
            messagebox.showinfo("Chưa chọn mục", "Hãy tick chọn ít nhất 1 dòng.")
            return
        self._run_jobs(idxs)

    def get_emails_all(self):
        if not self.rows_data:
            messagebox.showinfo("Chưa có dữ liệu", "Chưa có dữ liệu để lấy email.")
            return
        idxs = [i for i, r in enumerate(self.rows_data) if r.get("Trang web")]
        if not idxs:
            messagebox.showinfo("Không có URL", "Không có dòng nào có trang web hợp lệ.")
            return
        self._run_jobs(idxs)

    # ---------- Xuất Excel ----------
    def export_excel(self):
        if not self.rows_data:
            messagebox.showinfo("Chưa có dữ liệu", "Chưa có dữ liệu để xuất.")
            return
        default_name = f"maps_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        out_path = filedialog.asksaveasfilename(
            title="Lưu Excel",
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel Workbook", "*.xlsx"), ("All Files", "*.*")]
        )
        if not out_path:
            return
        try:
            import pandas as pd
            df = pd.DataFrame(self.rows_data)
            wanted = ["Từ khóa", "Tên", "Địa chỉ", "Trang web", "Email", "Trạng thái"]
            cols = [c for c in wanted if c in df.columns]
            df = df[cols]
            df.to_excel(out_path, index=False)
            self.logger.log(f"💾 Đã lưu Excel: {out_path}")
            messagebox.showinfo("Xong!", f"Đã lưu file Excel:\n{out_path}\n(Định dạng .xlsx chuẩn Unicode)")
        except ImportError:
            try:
                csv_path = out_path.replace(".xlsx", ".csv")
                with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=["Từ khóa","Tên","Địa chỉ","Trang web","Email","Trạng thái"])
                    writer.writeheader()
                    for r in self.rows_data:
                        writer.writerow({
                            "Từ khóa": r.get("Từ khóa",""),
                            "Tên": r.get("Tên",""),
                            "Địa chỉ": r.get("Địa chỉ",""),
                            "Trang web": r.get("Trang web",""),
                            "Email": r.get("Email",""),
                            "Trạng thái": r.get("Trạng thái","")
                        })
                self.logger.log(f"💾 Đã lưu CSV UTF-8 (BOM): {csv_path}")
                messagebox.showinfo("Xong!", "Máy bạn chưa cài pandas.\nĐã lưu CSV UTF-8 (BOM) để Excel mở đúng dấu.")
            except Exception as e:
                messagebox.showerror("Lỗi", f"Không thể lưu file: {e}")
        except Exception as e:
            messagebox.showerror("Lỗi", f"Không thể lưu Excel: {e}")


# ============
# 8) CHẠY APP
# ============
if __name__ == "__main__":
    root = tk.Tk()
    app = MapsSearchHarvesterApp(root)
    root.mainloop()



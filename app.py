import io
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, Response
import crawler_logic as crawler
import threading
from queue import Queue
import time

# --- CẤU HÌNH FLASK ---
app = Flask(__name__)
app.secret_key = 'a_very_secret_key_for_flash_messages'

# --- QUẢN LÝ TRẠNG THÁI NÂNG CAO ---
# Sử dụng Queue để giao tiếp an toàn giữa các luồng (thread-safe)
# Luồng worker sẽ đặt (put) log vào queue, luồng SSE sẽ lấy (get) log ra
LOG_QUEUE = Queue()
# Dict để lưu kết quả cuối cùng
APP_STATE = {"rows_data": []}

# --- CÁC ROUTE CỦA WEB APP ---

@app.route('/')
def index():
    return render_template('index.html')

# --- CÁC ROUTE CẤU HÌNH (GIỮ NGUYÊN) ---
@app.route('/config')
def config_page():
    cfg = crawler.ConfigManager.load()
    return render_template('config.html', cfg=cfg)

@app.route('/save-config', methods=['POST'])
def save_config():
    cfg = crawler.ConfigManager.load()
    cfg['contact_hints'] = [ln.strip() for ln in request.form['contact_hints'].splitlines() if ln.strip()]
    cfg['blocklist'] = [ln.strip() for ln in request.form['blocklist'].splitlines() if ln.strip()]
    cfg['headless'] = 'headless' in request.form
    cfg['max_scroll'] = int(request.form.get('max_scroll', 60))
    cfg['delay'] = float(request.form.get('delay', 2.2))
    cfg['request_workers'] = int(request.form.get('request_workers', 4))
    cfg['selenium_workers'] = int(request.form.get('selenium_workers', 2))
    cfg['selenium_contact_limit'] = int(request.form.get('selenium_contact_limit', 4))
    cfg['selenium_wait_body'] = int(request.form.get('selenium_wait_body', 5))
    cfg['selenium_wait_click'] = int(request.form.get('selenium_wait_click', 2))
    cfg['selenium_sleep_per_page'] = float(request.form.get('selenium_sleep_per_page', 0.5))
    crawler.ConfigManager.save(cfg)
    flash("Đã lưu cấu hình thành công!")
    return redirect(url_for('config_page'))

# --- CÁC HÀM WORKER CHẠY NỀN ---

def harvest_worker(main_kw, sub_kws):
    """Hàm này sẽ chạy trong một luồng riêng để không làm treo web."""
    cfg = crawler.ConfigManager.load()
    queries = [(main_kw, sk) for sk in sub_kws] if sub_kws else [(main_kw, None)]
    all_rows = []
    
    # Hàm callback để đặt log vào Queue
    def log_callback(msg):
        LOG_QUEUE.put(msg)

    with ThreadPoolExecutor(max_workers=min(3, len(queries))) as executor:
        futs = {executor.submit(crawler.harvest_one_query, q[0], q[1], cfg['headless'], cfg['max_scroll'], cfg['delay'], log_callback): q for q in queries}
        for future in as_completed(futs):
            try: all_rows.extend(future.result())
            except Exception as e: log_callback(f"Lỗi thread harvest {futs[future]}: {e}")

    seen = set()
    APP_STATE["rows_data"] = [row for row in all_rows if (row.get("Tên"), row.get("Trang web")) not in seen and not seen.add((row.get("Tên"), row.get("Trang web")))]
    # Gửi tín hiệu đặc biệt để báo cho client biết tác vụ đã hoàn thành
    LOG_QUEUE.put("---TASK_COMPLETE---")

def get_emails_worker(indices_to_process):
    """Hàm chạy nền để lấy email."""
    cfg = crawler.ConfigManager.load()
    rows_data = APP_STATE["rows_data"]
    domain_cache = {}

    def log_callback(msg):
        LOG_QUEUE.put(msg)

    def process_one_site(original_index):
        # ... (Toàn bộ logic xử lý một site được giữ nguyên)
        row_data = rows_data[original_index].copy()
        url = crawler.normalize_url(row_data.get("Trang web", ""))
        if not url:
            row_data['Trạng thái'] = "Không có trang web"
            return original_index, row_data
        domain = crawler.canonical_domain(url)
        if domain in domain_cache:
            cached = domain_cache[domain]
            row_data['Email'] = "; ".join(cached.get("emails", []))
            row_data['Trạng thái'] = f"Từ cache ({cached.get('source', 'NA')})"
            return original_index, row_data

        emails, source = crawler.request_phase_contact_only(url, cfg['contact_hints'], cfg['blocklist'], log_callback)
        if not emails:
            driver = None
            try:
                driver = crawler.build_driver(headless=cfg['headless'])
                emails, source = crawler.selenium_phase_contact_then_home(driver, url, cfg['contact_hints'], cfg['blocklist'], log_callback)
            finally:
                if driver: driver.quit()
        if emails:
            row_data['Email'] = "; ".join(emails)
            row_data['Trạng thái'] = f"OK - {source}"
            domain_cache[domain] = {"emails": emails, "source": source}
        else:
            row_data['Trạng thái'] = "Không tìm thấy email"
        return original_index, row_data

    with ThreadPoolExecutor(max_workers=cfg['selenium_workers']) as executor:
        f_to_idx = {executor.submit(process_one_site, i): i for i in indices_to_process}
        for future in as_completed(f_to_idx):
            try:
                idx, updated_row = future.result()
                rows_data[idx] = updated_row
            except Exception as e:
                idx = f_to_idx[future]
                rows_data[idx]['Trạng thái'] = f"Lỗi: {e}"
    
    APP_STATE["rows_data"] = rows_data
    LOG_QUEUE.put("---TASK_COMPLETE---")


# --- CÁC ROUTE KÍCH HOẠT TÁC VỤ ---

@app.route('/harvest', methods=['POST'])
def start_harvest_task():
    """Kích hoạt tác vụ harvest và chuyển hướng đến trang xem log."""
    main_kw = request.form['main_kw']
    sub_kws = [kw.strip() for kw in request.form['sub_kws'].splitlines() if kw.strip()]
    
    # Chạy hàm worker trong một luồng nền
    thread = threading.Thread(target=harvest_worker, args=(main_kw, sub_kws))
    thread.daemon = True
    thread.start()
    
    return redirect(url_for('log_viewer'))

@app.route('/get-emails', methods=['POST'])
def start_get_emails_task():
    """Kích hoạt tác vụ lấy email và chuyển hướng đến trang xem log."""
    selected_indices = [int(i) for i in request.form.getlist('selected_indices')]
    if not selected_indices:
        flash("Bạn chưa chọn mục nào để lấy email!")
        return redirect(url_for('show_results'))

    thread = threading.Thread(target=get_emails_worker, args=(selected_indices,))
    thread.daemon = True
    thread.start()
    
    return redirect(url_for('log_viewer'))


# --- CÁC ROUTE ĐẶC BIỆT CHO REAL-TIME ---

@app.route('/log-viewer')
def log_viewer():
    """Hiển thị trang xem log."""
    return render_template('log_viewer.html')

@app.route('/stream-logs')
def stream_logs():
    """Đây là trái tim của SSE. Nó sẽ gửi log từ Queue ra trình duyệt."""
    def generate():
        while True:
            # Lấy một item từ queue. Nếu queue rỗng, nó sẽ đợi.
            msg = LOG_QUEUE.get()
            # Dữ liệu SSE phải có định dạng "data: ...\n\n"
            yield f"data: {msg}\n\n"
            if msg == "---TASK_COMPLETE---":
                break
            time.sleep(0.1) # Thêm một chút delay để tránh quá tải
    return Response(generate(), mimetype='text/event-stream')


# --- CÁC ROUTE HIỂN THỊ KẾT QUẢ VÀ EXPORT ---

@app.route('/results')
def show_results():
    """Hiển thị trang kết quả cuối cùng."""
    return render_template('results.html', businesses=APP_STATE.get("rows_data", []))

@app.route('/export')
def export_results():
    # ... (Hàm này giữ nguyên như cũ)
    if not APP_STATE.get("rows_data"): return "Không có dữ liệu để xuất!", 404
    try:
        df = pd.DataFrame(APP_STATE["rows_data"])
        cols = ["Từ khóa", "Tên", "Địa chỉ", "Trang web", "Email", "Trạng thái"]
        df = df[[c for c in cols if c in df.columns]]
        output = io.BytesIO()
        df.to_excel(output, index=False, sheet_name='Sheet1')
        output.seek(0)
        filename = f"ketqua_email_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)
    except Exception as e:
        return f"Lỗi khi xuất file: {e}", 500

if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 5000))
    print(f"Khởi động ứng dụng web trên port {port} ...")
    app.run(host='0.0.0.0', port=port, debug=False)
# =============================================================================
# TEXT STREAM SERVER CHO SPARK DSTREAMS DEMO
# Mục đích: Gửi từng dòng văn bản qua socket TCP để Spark DStreams xử lý
# Cách hoạt động: Đọc file sample_text.txt → Gửi tuần tự → Lặp lại liên tục
# Cách chạy: python send_text_socket.py
# =============================================================================

import socket
import time
import threading
from pathlib import Path

# ================== CẤU HÌNH ==================
HOST = "127.0.0.1"        # Địa chỉ localhost
PORT = 9999               # Cổng để Spark kết nối
INTERVAL_SEC = 5        # Gửi mỗi 0.5 giây
TEXT_FILE = "sample_text.txt"  # File chứa dữ liệu text (mỗi dòng = 1 message)
LOOP_FOREVER = True       # True = lặp file liên tục
# ==============================================

# Đọc dữ liệu từ file
lines = Path(TEXT_FILE).read_text(encoding="utf-8", errors="ignore").splitlines()
print(f"Đã đọc {len(lines)} dòng văn bản từ {TEXT_FILE}")

def handle_client(conn, addr):
    """Gửi tuần tự từng dòng cho Spark"""
    print(f"Spark DStreams connected from {addr}")
    print(f"Bắt đầu streaming text mỗi {INTERVAL_SEC}s...")
    
    try:
        count = 0
        while True:
            for line in lines:
                if not line.strip():
                    continue
                # Gửi message (kết thúc bằng newline)
                conn.sendall((line + "\n").encode("utf-8"))
                count += 1
                print(f"[{count}] Sent: {line}")
                time.sleep(INTERVAL_SEC)
            if not LOOP_FOREVER:
                break
    except (BrokenPipeError, ConnectionResetError):
        print(f"Spark client {addr} disconnected.")
    finally:
        conn.close()
        print(f"Connection to {addr} closed.")

def main():
    """Khởi tạo server và chờ Spark kết nối"""
    print("TEXT STREAM SERVER FOR SPARK DSTREAMS DEMO")
    print("=" * 50)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(1)
        
        print(f"Server listening on {HOST}:{PORT}")
        print("Waiting for Spark DStreams connection...")
        print("Run your Spark app (e.g., stateful_wordcount.py) to connect")
        
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()

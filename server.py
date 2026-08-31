import json
import sys
import time
import random
import threading
import requests
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

EXTERNAL_SERVERS = [
    "http://celeste-cn31-privates.up.railway.app",
    "http://solar-solver-production.up.railway.app",
    "http://217.216.35.81:8080",
    "http://217.216.35.129:8082",
    "http://62.146.237.138:8080",
    "https://janicolesolbar.up.railway.app",
]
EXTERNAL_SERVERS = list(dict.fromkeys(EXTERNAL_SERVERS))

TOKEN_TTL_SECONDS = 14 * 60
POOL_MAX_SIZE = 3000
FETCH_TIMEOUT = 5
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8080
PROXY_LIST = []

MIN_WORKERS = 30
MAX_WORKERS = 35
active_workers = MAX_WORKERS
workers_lock = threading.Lock()

class TokenPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.tokens = []
        self.total_generated = 0
        self.total_served = 0
        self.total_expired = 0
        self.peak_pool = 0
        self.start_time = time.time()
        self.last_rate_check = time.time()
        self.generated_at_last_check = 0
        self.current_rate = 0.0

    def add(self, token):
        now = time.time()
        with self.lock:
            if any(t == token for t, _ in self.tokens):
                return False
            self.tokens.append((token, now))
            self.total_generated += 1
            if len(self.tokens) > self.peak_pool:
                self.peak_pool = len(self.tokens)
            self._prune_locked()
            return True

    def get(self):
        with self.lock:
            self._prune_locked()
            if not self.tokens:
                return None
            token, _ = self.tokens.pop(0)
            self.total_served += 1
            return token

    def _prune_locked(self):
        now = time.time()
        fresh = []
        expired = 0
        for t, ts in self.tokens:
            if now - ts < TOKEN_TTL_SECONDS:
                fresh.append((t, ts))
            else:
                expired += 1
        self.total_expired += expired
        self.tokens = fresh
        if len(self.tokens) > POOL_MAX_SIZE:
            drop = len(self.tokens) - POOL_MAX_SIZE
            self.tokens = self.tokens[drop:]
            self.total_expired += drop

    def stats(self):
        with self.lock:
            self._prune_locked()
            return {
                "pool_size": len(self.tokens),
                "peak_pool": self.peak_pool,
                "total_generated": self.total_generated,
                "total_served": self.total_served,
                "total_expired": self.total_expired,
                "ttl_seconds": TOKEN_TTL_SECONDS,
                "workers": active_workers,
                "servers": EXTERNAL_SERVERS,
                "uptime_seconds": int(time.time() - self.start_time),
                "rate_per_min": self.current_rate
            }

    def update_rate(self):
        now = time.time()
        if now - self.last_rate_check >= 60:
            delta = self.total_generated - self.generated_at_last_check
            self.current_rate = delta / ((now - self.last_rate_check) / 60.0)
            self.generated_at_last_check = self.total_generated
            self.last_rate_check = now

pool = TokenPool()

def fetch_from_server(server_url, proxy=None):
    try:
        url = f"{server_url}/get-token"
        proxies = {"http": proxy, "https": proxy} if proxy else None
        resp = requests.get(url, timeout=FETCH_TIMEOUT, proxies=proxies)
        if resp.status_code == 200:
            data = resp.json()
            token = data.get("token")
            if token:
                if not token.startswith("CN31_"):
                    token = f"CN31_{token}"
                return token
    except Exception:
        pass
    return None

def worker(worker_id):
    while True:
        with workers_lock:
            if worker_id >= active_workers:
                time.sleep(1)
                continue
        try:
            stats = pool.stats()
            if stats["pool_size"] >= POOL_MAX_SIZE * 0.95:
                time.sleep(2)
                continue
            server = random.choice(EXTERNAL_SERVERS)
            proxy = random.choice(PROXY_LIST) if PROXY_LIST else None
            token = fetch_from_server(server, proxy)
            if token:
                pool.add(token)
            time.sleep(random.uniform(0.02, 0.1))
        except Exception:
            time.sleep(0.5)

def manager():
    global active_workers
    while True:
        time.sleep(30)
        pool.update_rate()
        rate = pool.current_rate
        pool_size = pool.stats()["pool_size"]
        with workers_lock:
            if pool_size > 2500:
                active_workers = max(MIN_WORKERS, active_workers - 1)
            elif pool_size < 500:
                active_workers = min(MAX_WORKERS, active_workers + 1)
            elif rate > 50:
                active_workers = max(MIN_WORKERS, active_workers - 1)
            elif rate < 20:
                active_workers = min(MAX_WORKERS, active_workers + 1)
            else:
                if active_workers > MIN_WORKERS and active_workers < MAX_WORKERS:
                    pass

class CombinedHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/get-token":
            token = pool.get()
            if token:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET")
                self.end_headers()
                self.wfile.write(json.dumps({"token": token}).encode())
            else:
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET")
                self.end_headers()
                self.wfile.write(b'{"error":"No tokens available"}')
            return

        elif path == "/stats":
            stats = pool.stats()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET")
            self.end_headers()
            self.wfile.write(json.dumps(stats).encode())
            return

        elif path == "/health":
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return

        super().do_GET()

def main():
    sys.stdout.reconfigure(line_buffering=True)
    print("Starting CN31 Token Server...")
    threading.Thread(target=manager, daemon=True).start()
    for i in range(MAX_WORKERS):
        threading.Thread(target=worker, args=(i,), daemon=True).start()
    print(f"Workers started (dynamic: {MIN_WORKERS}-{MAX_WORKERS})")

    try:
        server = HTTPServer((SERVER_HOST, SERVER_PORT), CombinedHandler)
        print(f"\n  Server running at http://{SERVER_HOST}:{SERVER_PORT}")
        print("  Press Ctrl+C to stop.\n")
        server.serve_forever()
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"ERROR: Port {SERVER_PORT} is already in use.")
        else:
            print(f"Binding error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()

if __name__ == "__main__":
    main()
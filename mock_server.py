import http.server
import socketserver
import json
import urllib.parse
import threading
import time
import random

DEFAULT_PORT = 5000

class MockOddsState:
    def __init__(self):
        self.lock = threading.Lock()
        self.match_name = "India vs Australia - 3rd T20I"
        self.odds = {
            "India": 0.35,
            "Australia": 2.85
        }
        self.auto_fluctuate = True
        self.running = False

    def update_odd(self, team, new_odd):
        with self.lock:
            val = round(float(new_odd), 3)
            # Case-insensitive update match
            matched = False
            for k in list(self.odds.keys()):
                if k.lower() == team.lower().strip():
                    self.odds[k] = val
                    matched = True
                    break
            if not matched:
                self.odds[team] = val
            self.auto_fluctuate = False

    def get_data(self):
        with self.lock:
            if self.auto_fluctuate:
                # Random subtle fluctuation (-0.02 to +0.02)
                change = random.choice([-0.02, -0.01, 0.0, 0.01, 0.02])
                new_india = max(0.05, round(self.odds.get("India", 0.35) + change, 3))
                self.odds["India"] = new_india
                self.odds["Australia"] = round(1.0 / new_india if new_india > 0 else 3.0, 2)

            teams_arr = []
            for k, v in self.odds.items():
                teams_arr.append({
                    "name": k,
                    "odd": v,
                    "back": round(v, 2),
                    "lay": round(v + 0.01, 2)
                })

            return {
                "status": "success",
                "match": self.match_name,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "teams": teams_arr
            }

global_mock_state = MockOddsState()

class MockOddsHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress standard HTTP request logging in terminal
        return

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        path = parsed_url.path
        query = urllib.parse.parse_qs(parsed_url.query)

        if path == "/api/odds" or path == "/":
            data = global_mock_state.get_data()
            self._send_json(200, data)
        elif path == "/api/set_odds":
            team = query.get("team", ["India"])[0]
            odd_val = query.get("odd", ["0.25"])[0]
            try:
                global_mock_state.update_odd(team, float(odd_val))
                self._send_json(200, {"status": "ok", "updated": {team: float(odd_val)}})
            except ValueError:
                self._send_json(400, {"error": "Invalid odd value"})
        elif path == "/api/toggle_auto":
            global_mock_state.auto_fluctuate = not global_mock_state.auto_fluctuate
            self._send_json(200, {"auto_fluctuate": global_mock_state.auto_fluctuate})
        else:
            self._send_json(404, {"error": "Endpoint not found"})

    def _send_json(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

class MockOddsServer:
    def __init__(self, port=DEFAULT_PORT):
        self.port = port
        self.httpd = None
        self.thread = None

    def start(self):
        if global_mock_state.running:
            return True
        try:
            self.httpd = socketserver.TCPServer(("127.0.0.1", self.port), MockOddsHandler)
            self.httpd.allow_reuse_address = True
            global_mock_state.running = True
            self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
            self.thread.start()
            return True
        except Exception as e:
            print(f"Failed to start mock server: {e}")
            return False

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
            global_mock_state.running = False

    def set_team_odd(self, team, odd):
        global_mock_state.update_odd(team, odd)

    def set_auto_fluctuate(self, enabled):
        global_mock_state.auto_fluctuate = enabled

if __name__ == "__main__":
    server = MockOddsServer()
    if server.start():
        print(f"Mock Odds API running at http://127.0.0.1:{DEFAULT_PORT}/api/odds")
        print("Control endpoints:")
        print(f"  Set Odd: http://127.0.0.1:{DEFAULT_PORT}/api/set_odds?team=India&odd=0.25")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            server.stop()
            print("\nServer stopped.")

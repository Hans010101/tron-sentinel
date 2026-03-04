import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass

def run_scheduler():
    from scheduler import main
    main()

t = threading.Thread(target=run_scheduler, daemon=True)
t.start()

port = int(os.environ.get("PORT", 8080))
HTTPServer(("", port), HealthHandler).serve_forever()

#!/usr/bin/env python3
import http.server
import socketserver
import os
import sys

PORT = 8000
WEB_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        if self.path == '/':
            self.path = '/tools/dashboard.html'
        return super().do_GET()

def main():
    print(f"Serving dashboard at http://localhost:{PORT}")
    print(f"Root directory: {WEB_DIR}")
    
    # Ensure data directory exists
    os.makedirs(os.path.join(WEB_DIR, "data"), exist_ok=True)
    
    try:
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server")
        sys.exit(0)

if __name__ == "__main__":
    main()


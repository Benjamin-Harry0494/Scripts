#!/usr/bin/env python3

import os
import urllib3
import signal
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

class AllocateApiProxy:
    def start_server(self, port, forward_address, jwt):
        class ProxyHTTPRequestHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/2.0"

            def do_POST(self):
                self._handle_request()

            def _handle_request(self):
                url = self._resolve_url()
                body = self.rfile.read(int(self.headers["content-length"]))
                headers = dict(self.headers)

                headers['X-Authorization'] = jwt
                headers['Content-Type'] = 'application/json'
                print("Sending message to ", forward_address, " body: ", body, " headers: ", headers)
                http = urllib3.PoolManager()
                resp = http.request('POST', url, headers=headers, body=body)
                print(resp.status)
                print(resp.data)

                res=b'{"accepted":true}'
                self.send_response(202)
                self.send_header('Content-Length', str(len(res)))
                self.send_header('Content-Type', 'application/json')
                self.end_headers()

                self.wfile.write(res)
                return

            def _resolve_url(self):
                return forward_address

        server_address = ('', port)
        self.httpd = HTTPServer(server_address, ProxyHTTPRequestHandler)
        print('server is running on port: ', port)
        self.httpd.serve_forever()

def exit_now(signum, frame):
    sys.exit(0)

if __name__ == '__main__':
    port = int(os.environ['PROXY_PORT'])
    forward_address = os.environ['PATCHWORK_URL']
    jwt = os.environ['PATCHWORK_JWT']

    proxy = AllocateApiProxy()
    signal.signal(signal.SIGTERM, exit_now)
    proxy.start_server(port, forward_address, jwt)
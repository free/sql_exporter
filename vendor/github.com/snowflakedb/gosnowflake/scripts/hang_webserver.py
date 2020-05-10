#!/usr/bin/env python3
import sys
from http.server import BaseHTTPRequestHandler,HTTPServer
from socketserver import ThreadingMixIn
import threading
import time

class HTTPRequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.startswith('/403'):
            self.send_response(403)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
        elif self.path.startswith('/404'):
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
        elif self.path.startswith('/hang'):
            time.sleep(300)
            self.send_response(200, 'OK')
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
        else:
            self.send_response(200, 'OK')
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
    do_GET = do_POST

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
  allow_reuse_address = True

  def shutdown(self):
    self.socket.close()
    HTTPServer.shutdown(self)

class SimpleHttpServer():
  def __init__(self, ip, port):
    self.server = ThreadedHTTPServer((ip,port), HTTPRequestHandler)

  def start(self):
    self.server_thread = threading.Thread(target=self.server.serve_forever)
    self.server_thread.daemon = True
    self.server_thread.start()

  def waitForThread(self):
    self.server_thread.join()

  def stop(self):
    self.server.shutdown()
    self.waitForThread()

if __name__=='__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 {} PORT".format(sys.argv[0]))
        sys.exit(2)

    PORT = int(sys.argv[1])

    server = SimpleHttpServer('localhost', PORT)
    print('HTTP Server Running on PORT {}..........'.format(PORT))
    server.start()
    server.waitForThread()


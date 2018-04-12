# -*- coding: utf-8 -*-
import SimpleHTTPServer
import SocketServer

def init_server(ip,port):
	
	Handler = SimpleHTTPServer.SimpleHTTPRequestHandler

	httpd = SocketServer.TCPServer((ip, port), Handler)

	print "SimpleHTTPServer serving at port", port

	httpd.serve_forever()
	

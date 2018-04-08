# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, serve_forever
import socket, errno
import SocketServer

if __name__ == "__main__":
	set_context()
	#Creamos el host con diferente puerto
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	PORT=6000
	#Comporobamos el puerto para crear diferentes mappers, reducer y el server
	#con el mismo arhivo
	while True:
		try:
			s = host = create_host('http://127.0.0.1:'+str(PORT))
		except socket.error as e:
			print("Port " +str(PORT) +" is already in use")
			PORT+=1
		else:
			print("Start at port "+ str(PORT))
			break	
	

	#Conseguimos la referencia a registry de manera remota
	registry = host.lookup_url('http://192.168.0.15:8020/regis', 'Registry',
				               'p1_registry')

	#Registramos nuestro host en el registry
	'''remote_host = registry.lookup('reducer')
	if remote_host is None:
		registry.bind('reducer', host)
	else:
		registry.bind('host'+str(PORT), host)
	'''
	num_actors=registry.num_actors()
	if num_actors == 0:
		registry.bind('reducer', host)
	#elif num_actors == 1:
	#	registry.bind('server', host)
	else:
		registry.bind('host'+str(PORT), host)
	serve_forever()

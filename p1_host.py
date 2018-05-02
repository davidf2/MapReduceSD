# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, serve_forever
from p1_network import get_free_port, set_ip

if __name__ == "__main__":
	set_context()
	
	#Obtenim un port lliure
	port=get_free_port()
	
	#Introduim una ip
	ip=set_ip()
	
	#Creamos el host con diferente puerto
	host = create_host('http://'+ip+':'+str(port))
	
	ip_registry=str(raw_input("Introdueix la ip del registry: "))
    
	#Conseguimos la referencia a registry de manera remota
	registry = host.lookup_url('http://'+ip_registry+':6000/regis', 'Registry',
                               'p1_registry')

	if len(registry.get_all()) == 0:
		registry.bind('reducer', host)
	else:
		#Registramos nuestro host en el registry
		registry.bind('mapper'+str(port), host)
    

	serve_forever()

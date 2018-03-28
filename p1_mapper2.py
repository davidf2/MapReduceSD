# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, serve_forever

if __name__ == "__main__":
    set_context()
    #Creamos el host con diferente puerto
    host = create_host('http://127.0.0.1:6002')
	
	#Conseguimos la referencia a registry de manera remota
    registry = host.lookup_url('http://127.0.0.1:6000/regis', 'Registry',
                               'p1_registry')

	#Registramos nuestro host en el registry
    registry.bind('mapper2', host)
    

    serve_forever()

# -*- coding: utf-8 -*-
from pyactor.context import set_context, create_host, serve_forever
import os

if __name__ == "__main__":
	set_context()
	
	port=6000
	
	#Comprovamos que el puerto no este en uso en caso contrario lo incrementamos
	while os.popen('lsof -i :'+str(port)).read() != "" :
		port+=1
	
	#Pedimos al usuario que introduzca una interficie para obtener su ip de manera automatica
	#	mostrando las disponibles
	interface=str(raw_input("Introdueix una interficie de xarxa [ "+os.popen('ls /sys/class/net').read().replace('\n',' ')+"]\n(Per a localhost lo): "))
	
	#Obtenemos una ip, segun la interfaz de red
	ip=os.popen("ip addr show "+interface+" | grep inet | head -n 1 | sed -e 's/ \+/ /g' | cut -d ' ' -f 3| cut -d '/' -f 1").read().replace('\n','')
	
	#Creamos el host con diferente puerto
	host = create_host('http://'+ip+':'+str(port))
	
	#Conseguimos la referencia a registry de manera remota
	registry = host.lookup_url('http://192.168.0.15:6000/regis', 'Registry',
                               'p1_registry')

	if len(registry.get_all()) == 0:
		registry.bind('reducer', host)
	else:
		#Registramos nuestro host en el registry
		registry.bind('mapper'+str(port), host)
    

	serve_forever()

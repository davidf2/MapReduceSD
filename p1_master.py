# -*- coding: utf-8 -*-

from p1_file import split_file, rm_files
from server import init_server
from pyactor.context import set_context, create_host, sleep, shutdown, serve_forever
from multiprocessing import Process
from time import time
from p1_network import get_free_port, set_ip



class Master(object):
	
	_tell = ['echo', 'delete_files', 'start_time', 'elapsed_time','off']
	
	time=None
	def __init__(self,file_name):
		self.file_name=file_name
	
	def echo(self,msg):
		print msg
	
	def start_time(self):
		self.time=None
		self.time=time()
	
	def elapsed_time(self):
		print("Temps transcorregut: "+str(time() - self.time)+ " segons")
	
	def delete_files(self,num):
		rm_files(self.file_name,num)
	
	def off(self):
		shutdown()


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
	
	#Pedimos al usuario el nombre del fichero a modificar
	file_name=str(raw_input("Introdueix el nom del fitxer: "))
	
	#Creamos un actor master para mostrar la infromacion al final del programa y borrar los archivos
	master=host.spawn('master', 'p1_master/Master',file_name)
	
	#Obtenemos un puerto libre para el servidor
	port_servidor=get_free_port()
	
	#Creamos el servidor web a traves de un thread para que se pueda ejecutar concurrentemente
	p = Process(target=init_server, args=(ip,port_servidor,))
	p.start()
	
	#Guardamos todos los actores almacenados en el registry
	actors=registry.get_keys()
	
	option=str(raw_input("Per executar WordCount --> 1\nPer executar CountWords --> 2\n"))
	while option!='1' and option!='2':
		print '!Opció incorrecta!'
		option=str(raw_input("Per executar WordCount --> 1\nPer executar CountWords --> 2\n"))
	if option == '1':
		mappers_type='MapWordCount'
		reducer_type='ReduceWordCount'
	elif option == '2':
		mappers_type='MapCountWords'
		reducer_type='ReduceCountWords'
		
	#Lista para los mappers que controlaremos remotamente
	mappers=[]
	num_mappers=0
	for i in actors:
		if "mapper" in i:
			print i
			if not registry.lookup(i).has_actor(i):
				mappers.append(registry.lookup(i).spawn(i,'p1_mapper/'+mappers_type, file_name.split('.')[0]+str(num_mappers)+'.'+file_name.split('.')[-1],registry))
			else:
				mappers.append(registry.lookup(i))
			num_mappers+=1
	if( num_mappers >0):
		#Creamos el reducer en el host remoto
		registry.lookup('reducer').spawn('reducer', 'p1_reducer/'+reducer_type,num_mappers,master)
		
		#Invocamos al metodo split_file para dividir el fichero por lineas segun el numero de mappers encontrados
		split_file(file_name,num_mappers)
		
		master.start_time()
		#Ponemos a todos los mappers a trabajar
		for i in mappers:
			i.work(ip+':'+str(port_servidor))
			
		print("Podràs consultar els resultats a través del terminal del reducer")
		
		serve_forever()
	else:
		print "No s'ha trobat cap mapper"
		shutdown()

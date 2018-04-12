# -*- coding: utf-8 -*-

from p1_word_count import wordCount
from p1_counting_words import countingWords
from p1_file import split_file, rm_files
from server import init_server
from pyactor.context import set_context, create_host, sleep, shutdown, serve_forever
import os
import urllib2
from multiprocessing import Process
from collections import Counter
from time import time

#LAS CLASES MAP Y REDUCE PODRIAN IR EN OTRO FICHERO A PARTE
class Map(object):
	
	_tell = ['work']
	
	finish = False
	num_words=0
	w_dictionary={}
	
	def __init__(self,fitxer, registry):
		self.fitxer=fitxer
		self.registry=registry
		
	def work(self,ip_port):
		
		#SE TENDIRAN QUE MODIFICAR LAS FUNCIONES WORDCOUNT Y COUNTINGWORDS PARA QUE TRABAJEN SOBRE
		#	EL STRING Y NO TENER QUE ESCRIBIR EL FCIHERO
		#Descargamos el fichero, para que puedan acceder los mappers remotos
		text=urllib2.urlopen('http://'+ip_port+'/'+self.fitxer).read()
		file_aux = open(self.fitxer, 'w')
		file_aux.write(text)
		file_aux.close()
		
		#SE TENDRIAA QUE MODIFICAR PARA QUE SE PUEDA ELEGIR ENTRE COUNTINGOWORDS Y WORDCOUNT
		#Ejecutamos las funciones countingWords y wordCount
		#self.num_words=countingWords(self.fitxer)
		self.w_dictionary=wordCount(self.fitxer)
		#Buscamos al reducer en el registry
		remote_host = self.registry.lookup('reducer')
		reducer=remote_host.lookup('reducer')
		#Le pasamos el diccionario y numero de palabras resultantes al reducer
		reducer.add_dictionary(self.w_dictionary)
		#reducer.add_words(self.num_words)

class Reduce(object):
	
	_tell = ['join_dictionaries', 'add_words', 'add_dictionary']
	
	g_dictionary={}
	num_words=0
	counter=0
	def __init__(self, num_mappers):
		self.num_mappers=num_mappers
		self.list_dictionary=[]
		# Start counting
        start_time = time()
	
	def get_counter(self):
		return self.counter
	
	def get_dictionary(self):
		return self.g_dictionary
	
	def get_num_words(self):
		return self.num_words
	
	def add_words(self,num_words):
		self.num_words+=num_words
		
	def add_dictionary(self,dictionary):
		self.list_dictionary.append(dictionary)
		if(len(self.list_dictionary)==self.num_mappers):
			for i in self.list_dictionary:
				self.g_dictionary=dict(Counter(self.g_dictionary)+Counter(i))
			for key, value in self.g_dictionary.iteritems(): 
				print("La paraula "+key+" apareix "+str(value)+" cop/s.\n")
			#print self.num_words
			print( "Temps transcorregut: "+str(time() - self.start_time)+ " segons")
			#FALTARIA QUE BORRE LOS FICHEROS CREADOS ANTES DE TERMINAR
			shutdown()
		
	
if __name__ == "__main__":
	
	set_context()
	
	port =8000
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
	
	#Pedimos al usuario el nombre del fichero a modificar
	file_name=str(raw_input("Input a filename: "))
    
	port_servidor=9000
	#Creamos el servidor web a traves de un thread para que se pueda ejecutar concurrentemente
	p = Process(target=init_server, args=(ip,port_servidor,))
	p.start()
    
	#Conseguimos la referencia a registry de manera remota
	registry = host.lookup_url('http://192.168.20.1:6000/regis', 'Registry',
                               'p1_registry')
	
	#Buscamos al actor reducer en el registry i lo guardamos como remote_host
	remote_host= registry.lookup('reducer')
	
	#Guardamos todos los actores almacenados en el registry
	actors=registry.get_keys()
	
	#Lista para los mappers que controlaremos remotamente
	mappers=[]
	num_mappers=0
	for i in actors:
		if "mapper" in i:
			print i
			mappers.append(registry.lookup(i).spawn(i,'p1_master/Map', file_name.split('.')[0]+str(num_mappers)+'.'+file_name.split('.')[-1],registry))
			num_mappers+=1
	if( num_mappers >0):
		#Creamos el reducer en el host remoto
		reducer=remote_host.spawn('reducer', 'p1_master/Reduce',num_mappers)
		
		#Invocamos al metodo split_file para dividir el fichero por lineas segun el numero de mappers encontrados
		split_file(file_name,num_mappers)
		
		#Ponemos a todos los mappers a trabajar
		for i in mappers:
			i.work(ip+':'+str(port_servidor))
			
		print("Podràs consultar els resultats a través del terminal del reducer")
		
		serve_forever()
	else:
		print "No s'ha trobat cap mapper"
		shutdown()

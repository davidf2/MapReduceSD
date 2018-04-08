# -*- coding: utf-8 -*-

from p1_word_count import wordCount
from p1_counting_words import countingWords
from p1_file import split_file, rm_files
from pyactor.context import set_context, create_host, sleep, shutdown
import SimpleHTTPServer
import SocketServer
import urllib,random
import os
	
class Map(object):
	
	_tell = ['work']
	
	finish = False
	num_words=0
	w_dictionary={}
	noufit=' '
	
	def __init__(self,fitxer, registry):
		self.fitxer=fitxer
		self.registry=registry
		print("Soc Maper")
		
	def work(self):
		print(self.fitxer)
		random.seed(a=None)
		rando=random.randint(1,101)
		self.noufit=str(rando)+self.fitxer
		urllib.urlretrieve ("http://192.168.0.15:6100/"+self.fitxer, self.noufit)
		print(self.noufit)
		self.num_words=countingWords(self.noufit)

		self.w_dictionary=wordCount(self.noufit)
		remote_host = self.registry.lookup('reducer')
		
		reducer=remote_host.lookup('reducer')
		reducer.join_dictionaries(self.w_dictionary)
		reducer.add_words(self.num_words)
		os.remove(self.noufit)
	

class Reduce(object):
	
	_ask = ['get_counter','get_dictionary','join_dictionaries', 'add_words', 'get_num_words']
	g_dictionary={}
	num_words=0
	counter=0
	
	def __init__(self, num_mappers):
		self.num_mappers=num_mappers
		print("Soc Reducer")
		
	def get_counter(self):
		return self.counter
	
	def get_dictionary(self):
		return self.g_dictionary
	
	def get_num_words(self):
		return self.num_words
	
	def join_dictionaries(self, dictionary2):
		for word in dictionary2:
			if self.g_dictionary.has_key(word) == True:
				self.g_dictionary[word]=self.g_dictionary[word]+dictionary2[word]
			else:
				self.g_dictionary[word]=dictionary2[word]
		self.counter+=1
		if(self.counter==self.num_mappers):
			self.mostrar_resultat()
	
	def mostrar_resultat(self):
		print("entra fins aqui")
		for item,value in self.g_dictionary.iteritems():
			print("La paraula "+item+" apareix "+str(value)+" cop/s.")
	
	def add_words(self,num_words):
		self.num_words+=num_words
	
if __name__ == "__main__":
	
	set_context()
	#Creamos el host con un puerto diferente
	PORT=7800
	while True:
		try:
			host = create_host('http://127.0.0.1:'+str(PORT))
		except socket.error as e:
			print("Port " +str(PORT) +" is already in use")
			PORT+=1
		else:
			print("Start at port "+ str(PORT))
			break

	#Conseguimos la referencia a registry de manera remota
	registry = host.lookup_url('http://192.168.0.15:8020/regis', 'Registry',
                               'p1_registry')
    
	
	#Guardamos todos los actores almacenados en el registry
	actors=registry.get_keys()
	#print actors
	#Comporvacions opcionals	
	#if len(actors) < 3:
	#	print("Necesitamos minimo 3 actores")
	#else:
	#print("OK")
	#Creamos el servidor en el host remoto

	#server=registry.lookup(actors[0]).spawn(actors[0],'p1_master/Server')
	
	#Buscamos al actor reducer en el registry i lo guardamos como remote_host
	remote_host = registry.lookup('reducer')
	if remote_host is not None:
		if not remote_host.has_actor('reducer'):
			reducer=remote_host.spawn('reducer', 'p1_master/Reduce',len(actors)-1)
		else:
			reducer = remote_host.lookup('reducer')
	
	#Pedimos al usuario el nombre del fichero a modificar

	file_name=str(raw_input("Input a filename : "))
	#Lista para los mappers que controlaremos remotamente
	mappers=[]
	num_mappers=0
	'''for i in xrange(1,len(actors)-1):
		#CAMBIAR
		mappers.append(registry.lookup(actors[i]).spawn(actors[i],'p1_master/Map', file_name.split('.')[0]+str(num_mappers)+'.'+file_name.split('.')[-1],registry))
		num_mappers+=1
		print('spawnejo mapper sobre ' + actors[i])
	#print(mappers)
	'''
	for i in actors:
		if i != 'reducer':
			print i
			mappers.append(registry.lookup(i).spawn(i,'p1_master/Map', file_name.split('.')[0]+str(num_mappers)+'.'+file_name.split('.')[-1],registry))
			num_mappers+=1

	#Invocamos al metodo split_file para dividir el fichero por lineas segun el numero de mappers encontrados
	split_file(file_name,num_mappers)
	
	#Ponemos a todos los mappers a trabajar
	for i in mappers:
		i.work()
    
   	#Esperamos a que todos los mappers hayan terminado su trabajo
		
	#while( reducer.get_counter() < num_mappers):
	#	print "AAAAAAAAAAAAAAAAAA\n"
	
	#Obtenemos el diccionario del reducer y mostramos los resultados
	#dictionary=reducer.get_dictionary()
	
	#for key, value in dictionary.iteritems(): 
	#	print("La paraula "+key+" apareix "+str(value)+" cop/s.\n")
	
	#print reducer.get_num_words()
	
	#Eliminamos los ficheros creados
	rm_files(file_name,num_mappers)
	
	#Terminamos...
	shutdown()
	

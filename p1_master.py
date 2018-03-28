# -*- coding: utf-8 -*-

from p1_word_count import wordCount
from p1_counting_words import countingWords
from p1_file import split_file, rm_files
from pyactor.context import set_context, create_host, sleep, shutdown

	
class Map(object):
	
	_ask = ['work']
	
	finish = False
	num_words=0
	w_dictionary={}
	
	def __init__(self,fitxer, registry):
		self.fitxer=fitxer
		self.registry=registry
		
	def work(self):
		
		self.num_words=countingWords(self.fitxer)
		self.w_dictionary=wordCount(self.fitxer)
		remote_host = self.registry.lookup('reducer')
		reducer=remote_host.lookup('reducer')
		reducer.join_dictionaries(self.w_dictionary)
		reducer.add_words(self.num_words)

class Reduce(object):
	
	_ask = ['get_counter','get_dictionary','join_dictionaries', 'add_words', 'get_num_words']
	g_dictionary={}
	num_words=0
	counter=0
	
	def __init__(self, num_mappers):
		self.num_mappers=num_mappers
	
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
	
	def add_words(self,num_words):
		self.num_words+=num_words
	
if __name__ == "__main__":
	
	set_context()
	#Creamos el host con un puerto diferente
	host = create_host('http://127.0.0.1:6004')
    
	#Conseguimos la referencia a registry de manera remota
	registry = host.lookup_url('http://127.0.0.1:6000/regis', 'Registry',
                               'p1_registry')
    
	#Pedimos al usuario el nombre del fichero a modificar
	file_name=str(raw_input("Input a filename : "))
	
	
	#Buscamos al actor reducer en el registry i lo guardamos como remote_host
	remote_host= registry.lookup('reducer')
	
	#Guardamos todos los actores almacenados en el registry
	actors=registry.get_keys()
	
	#Lista para los mappers que controlaremos remotamente
	mappers=[]
	num_mappers=0
	for i in actors:
		if i != 'reducer':
			print i
			mappers.append(registry.lookup(i).spawn(i,'p1_master/Map', file_name.split('.')[0]+str(num_mappers)+'.'+file_name.split('.')[-1],registry))
			num_mappers+=1
	
	#Creamos el reducer en el host remoto
	reducer=remote_host.spawn('reducer', 'p1_master/Reduce',num_mappers)
	
	#Invocamos al metodo split_file para dividir el fichero por lineas segun el numero de mappers encontrados
	split_file(file_name,num_mappers)
	
	#Ponemos a todos los mappers a trabajar
	for i in mappers:
		i.work()
    
    #Esperamos a que todos los mappers hayan terminado su trabajo
	while( reducer.get_counter() < num_mappers):
		print "AAAAAAAAAAAAAAAAAA\n"
	
	#Obtenemos el diccionario del reducer y mostramos los resultados
	dictionary=reducer.get_dictionary()
	
	for key, value in dictionary.iteritems(): 
		print("La paraula "+key+" apareix "+str(value)+" cop/s.\n")
	print reducer.get_num_words()
	#Eliminamos los ficheros creados
	rm_files(file_name,num_mappers)
	#Terminamos...
	shutdown()
	

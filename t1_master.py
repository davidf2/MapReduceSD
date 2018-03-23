# -*- coding: utf-8 -*-

from p1_word_count import wordCount
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
		
		#self.num_words=CountingWords(self.fitxer)
		self.w_dictionary=wordCount(self.fitxer)
		remote_host = self.registry.lookup('reducer')
		reducer=remote_host.lookup('reducer')
		reducer.unir_diccionario(self.w_dictionary)

class Reduce(object):
	
	_ask = ['get_counter','get_dictionary','unir_diccionario']
	g_dictionary={}
	counter=0
	
	def __init__(self, num_mappers):
		self.num_mappers=num_mappers
	
	def get_counter(self):
		return self.counter
	
	def get_dictionary(self):
		return self.g_dictionary
			
	def unir_diccionario(self, dictionary2):
		for word in dictionary2:
			if self.g_dictionary.has_key(word) == True:
				self.g_dictionary[word]=self.g_dictionary[word]+dictionary2[word]
			else:
				self.g_dictionary[word]=dictionary2[word]
		self.counter+=1
	
if __name__ == "__main__":
	
	set_context()
	#Creamos el host con un puerto diferente
	host = create_host('http://127.0.0.1:6004')
    
	#Conseguimos la referencia a registry de manera remota
	registry = host.lookup_url('http://127.0.0.1:6000/regis', 'Registry',
                               's4_registry')
	
	#Buscamos al actor mapper1 en el registry i lo guardamos como remote_host
	
	
	remote_host1 = registry.lookup('mapper1')
	remote_host2 = registry.lookup('mapper2')
	remote_host3 = registry.lookup('reducer')
	reducer = remote_host3.spawn('reducer', 't1_master/Reduce',2)
	mapper1 = remote_host1.spawn('mapper1', 't1_master/Map',"file1",registry)
	mapper2 = remote_host2.spawn('mapper2', 't1_master/Map',"file2",registry)
	mapper1.work()
	mapper2.work()
     
	while( reducer.get_counter() < 2):
		print reducer.get_counter() 
	dictionary=reducer.get_dictionary()
	for key, value in dictionary.iteritems(): 
		print("La paraula "+key+" apareix "+str(value)+" cop/s.\n")
	'''
	llista_map=list()
	fitxer='apuntes.txt'
	f = open(fitxer, 'r')
	num_lines=len(f.readlines())
	
	for i in range(0, num_lines/NUM_MAP):
		i_linia=i*num_lines/NUM_MAP
		f_linia=(i_linia)+num_lines/NUM_MAP
		if i+1==num_lines/NUM_MAP:
			f_linia+=num_lines%NUM_MAP
		print("Primer Map de la linia "+str(i_linia)+" a la linia "+str(f_linia)+"\n")
		llista_map.append(Map(i_linia,f_linia,fitxer))
	'''
	
	#for i in range(0,len(llista_map)):
		#print llista_map[i].getILine()
		
		
	#print CountingWords("apuntes.txt")
	#dictionary= WordCount("apuntes.txt")
	#for key, value in dictionary.iteritems(): 
		#print("La paraula "+key+" apareix "+str(value)+" cop/s.\n")
	

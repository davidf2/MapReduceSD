import urllib2
from p1_word_count import wordCount
from p1_counting_words import countingWords

class MapAbstract(object):
	
	_tell = ['work']
	
	
	def __init__(self,fitxer, registry):
		self.fitxer=fitxer
		self.registry=registry
	
	def work(self,ip_port):
		pass
	
	
class MapWordCount(MapAbstract):
	
	_tell = ['work']
	
	element={}
	def work(self,ip_port):
		text=urllib2.urlopen('http://'+ip_port+'/'+self.fitxer).read()
		self.element=wordCount(text)
		#Buscamos al reducer en el registry
		remote_host = self.registry.lookup('reducer')
		reducer=remote_host.lookup('reducer')
		#Le pasamos el diccionario y numero de palabras resultantes al reducer
		reducer.add_element(self.element)

class MapCountWords(MapAbstract):
	_tell = ['work']
	
	element=0
	
	def work(self,ip_port):
		text=urllib2.urlopen('http://'+ip_port+'/'+self.fitxer).read()
		self.element=countingWords(text)
		#Buscamos al reducer en el registry
		remote_host = self.registry.lookup('reducer')
		reducer=remote_host.lookup('reducer')
		#Le pasamos el diccionario y numero de palabras resultantes al reducer
		reducer.add_element(self.element)
		#reducer.add_words(self.num_words)

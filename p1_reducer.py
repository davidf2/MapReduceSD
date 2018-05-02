from collections import Counter
from pyactor.context import shutdown

class ReduceAbstract(object):
	
	_tell = ['add_element']
	
	counter=0
	
	def __init__(self, num_mappers, master):
		self.num_mappers=num_mappers
		self.list_elements=[]
		self.master=master

	def add_element(self,element):
			pass
	
	

class ReduceWordCount(ReduceAbstract):
	
	g_dictionary={}
		
	def add_element(self,element):
		ReduceAbstract.add_element(self,element)
		self.list_elements.append(element)
		if(len(self.list_elements)==self.num_mappers):
			for i in self.list_elements:
				self.g_dictionary=dict(Counter(self.g_dictionary)+Counter(i))
			self.master.elapsed_time()
			self.master.delete_files(self.num_mappers)
			self.master.echo(self.g_dictionary)
			self.master.off()

class ReduceCountWords(ReduceAbstract):
	
	
	
	def add_element(self,element):
		ReduceAbstract.add_element(self,element)
		self.list_elements.append(element)
		if(len(self.list_elements)==self.num_mappers):
			num_words=0
			for i in self.list_elements:
				num_words+=i
			self.master.elapsed_time()
			self.master.delete_files(self.num_mappers)
			self.master.echo(num_words)
			self.master.off()


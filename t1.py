# -*- coding: utf-8 -*-
import re

def CountingWords(file):
	counter=0
	f = open(file, 'r')
	lines=f.readlines()
	for line in lines:
		for word in line.split(' '):
			counter+=1
	f.close()
	return counter

def WordCount(file):
	dictionary={}
	f = open(file, 'r')
	lines=f.readlines()
	
	for line in lines:
		line= re.sub(r'[-,;:?!.¿¡\'\(\)\[\]\n\t]',"",line) #Eliminamos caracteres especiales con Regular Expression
		for word in line.split(' '):	
			word=word.lower()					 #Pasamos a minusculas
			if dictionary.has_key(word) == True: #Si la clave ya este en el diccionario
				dictionary[word]=dictionary[word]+1
			else:								 #Si no esta en el diccionario
				dictionary[word]=1
	f.close()
	return dictionary
	
class Map(object):
	
	def __init__(self,i_line,f_line,fitxer):
		self.i_line=i_line
		self.f_line=f_line
		self.fitxer=fitxer
		
		
		
	
if __name__ == "__main__":
	NUM_MAP=4
	llista=list()
	fitxer='apuntes.txt'
	f = open(fitxer, 'r')
	num_lines=len(f.readlines())
	for i in range(0, num_lines/NUM_MAP):
		i_linia=i*num_lines/NUM_MAP
		f_linia=(i_linia)+num_lines/NUM_MAP
		if i+1==num_lines/NUM_MAP:
			f_linia+=num_lines%NUM_MAP
		print("Primer Map de la linia "+str(i_linia)+" a la linia "+str(f_linia)+"\n")
		llista.append(Map(i_linia,f_linia,fitxer))
	
	for i in llista:
		print i
	#print CountingWords("apuntes.txt")
	#dictionary= WordCount("apuntes.txt")
	#for key, value in dictionary.iteritems(): 
		#print("La paraula "+key+" apareix "+str(value)+" cop/s.\n")
	

# -*- coding: utf-8 -*-
import re

def wordCount(file):
	
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

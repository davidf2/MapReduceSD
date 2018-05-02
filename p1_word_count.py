# -*- coding: utf-8 -*-
import re

def wordCount(text):
	
	dictionary={}
	for line in text.splitlines():
		line= re.sub(r'[-,;:?!.¿¡\'\(\)\[\]\"|*+-_<>#@&^%$]'," ",line) #Eliminamos caracteres especiales con Regular Expression
		for word in line.split():
			word=word.lower()					 #Pasamos a minusculas
			if dictionary.has_key(word) == True: #Si la clave ya este en el diccionario
				dictionary[word]=dictionary[word]+1
			else:								 #Si no esta en el diccionario
				dictionary[word]=1
	return dictionary



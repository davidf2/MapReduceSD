# -*- coding: utf-8 -*-

from time import time
import re

def wordCount(file):
	
	dictionary={}
	f = open(file, 'r')
	lines=f.readlines()
	
	for line in lines:
		line= re.sub(r'[-,;:?!.¿¡\'\(\)\[\]\"*+-_<>#@&^%$]'," ",line) #Eliminamos caracteres especiales con Regular Expression
		for word in line.split():
			word=word.lower()					 #Pasamos a minusculas
			if dictionary.has_key(word) == True: #Si la clave ya este en el diccionario
				dictionary[word]=dictionary[word]+1
			else:								 #Si no esta en el diccionario
				dictionary[word]=1
	f.close()
	return dictionary


def countingWords(file):
	counter=0
	f = open(file, 'r')
	lines=f.readlines()
	for line in lines:
		line= re.sub(r'[-,;:?!.¿¡\'\(\)\[\]\"|*+-_<>#@&^%$]'," ",line)
		for word in line.split():
			counter+=1
	f.close()
	return counter

file_name=str(raw_input("Input a filename : "))

start_time = time()
num_words=countingWords(file_name)
time_countWord=time()-start_time

start_time = time()
dictionary=wordCount(file_name)
time_count=time()-start_time

print dictionary
print "Num total paraules: " + str(num_words)
print( "Temps transcorregut per Count Word: "+str(time_countWord)+ " segons")
print( "Temps transcorregut per Word Count: "+str(time_count)+ " segons")

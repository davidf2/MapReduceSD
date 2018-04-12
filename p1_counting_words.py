# -*- coding: utf-8 -*-

def countingWords(file):
		counter=0
		f = open(file, 'r')
		lines=f.readlines()
		for line in lines:
			for word in line.split():
				counter+=1
		f.close()
		return counter

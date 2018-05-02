# -*- coding: utf-8 -*-
import re

def countingWords(text):
		counter=0
		for line in text.splitlines():
			line= re.sub(r'[-,;:?!.¿¡\'\(\)\[\]\"|*+-_<>#@&^%$]'," ",line)
			for word in line.split():
				counter+=1
		return counter

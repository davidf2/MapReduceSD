# -*- coding: utf-8 -*-

import os
	
def split_file(file_name, num_mappers):
	f = open(file_name, 'r')
	o_file=f.readlines()
	num_lines=len(o_file)
	
	for i in range(0, num_mappers):
		i_line=i*num_lines/num_mappers
		f_line=(i_line)+num_lines/num_mappers
		print "Fitxer "+str(i)+" de linia "+str(i_line)+" a linia "+str(f_line)
		if i+1==num_lines/num_mappers:
			f_line+=num_lines%num_mappers
		f_aux=open(file_name.split('.')[0]+str(i)+'.'+file_name.split('.')[-1],'w')
		for j in range(i_line,f_line):
			f_aux.write(o_file[j])
		f_aux.close()
	f.close()

def rm_files(file_name,num_mappers):
	for i in range(0, num_mappers):
		os.remove(file_name.split('.')[0]+str(i)+'.'+file_name.split('.')[-1])

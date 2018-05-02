import os
import socket
import platform

def get_free_port():
	#Obtenemos un puerto libre mediante socket y despues lo cerramos
	sock = socket.socket()
	sock.bind(('', 0))
	port=sock.getsockname()[1]
	sock.close()
	return port

def set_ip():
	ip=''
	system= platform.system()
	
	if system == 'Linux':
		#Pedimos al usuario que introduzca una interficie para obtener su ip de manera automatica
		#	mostrando las disponibles
		interface=str(raw_input("Introdueix una interficie de xarxa [ "+os.popen('ls /sys/class/net').read().replace('\n',' ')+"]\n(Per a localhost lo): "))
		
		#Obtenemos una ip, segun la interfaz de red
		ip=os.popen("ip addr show "+interface+" | grep inet | head -n 1 | sed -e 's/ \+/ /g' | cut -d ' ' -f 3| cut -d '/' -f 1").read().replace('\n','')
	elif system == 'Windows':
		ip=str(raw_input("Introdueix la IP de la xarxa"))
	
	return ip

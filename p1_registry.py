'''
Remote example with a registry. SERVER
@author: Daniel Barcelona Pons
'''

from pyactor.context import set_context, create_host, serve_forever


class NotFound(Exception):
    pass


class Registry(object):
    _ask = ['get_all', 'bind', 'lookup', 'unbind', 'get_keys']
    _async = []
    _ref = ['get_all', 'bind', 'lookup', 'get_keys']

	#Guardamos los actores en un diccionario
    def __init__(self):
        self.actors = {}

	#Registramos los actores en el diccionario con key=nombre y value el actor
    def bind(self, name, actor):
        print "server registred", name
        self.actors[name] = actor

	#Eliminamos al actor del registro a traves de su nombre, si no mostramos un mensaje de no encontrado
    def unbind(self, name):
        if name in self.actors.keys():
            del self.actors[name]
        else:
            raise NotFound()
	#Miramos si el actor esta en el diccionario, si esta devolvemos el actor, si no, nulo
    def lookup(self, name):
        if name in self.actors:
            return self.actors[name]
        else:
            return None
	#Devolvemos todos los actores del diccionario
    def get_all(self):
        return self.actors.values()
    
    def get_keys(self):
        return self.actors.keys()


if __name__ == "__main__":
    set_context()
    #Creamos un host per crear actors
    host = create_host('http://127.0.0.1:6000/')
	#Metodo spawn(Nombre,Tipo)
    registry = host.spawn('regis', Registry)

    print 'host listening at port 6000'
	#Metodo para que el host no muera y pueda escuchar las llamadas de otros actores...
    serve_forever()

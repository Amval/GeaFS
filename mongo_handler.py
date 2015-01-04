import pymongo
import gridfs
import os
import sys
# Escribir arquitectura del sistema en papel. Primitivas, clases, etc
# connect to database

class MongoHandler:

    def __init__(self, username):
        """ Inicializa la clase, conecta a la base de datos, 
        crea una instancia del sistema de ficheros, obtiene id del usuario
        y crea alias para algunas operaciones """

        # conectar a la base de datos
        self.db = pymongo.MongoClient().geafs
        print('succesful connection to database')

        # crear instancia del sistema de ficheros
        self.fs = gridfs.GridFS(self.db)

        self.user_id = self.get_user_id(username)

        # Alias para algunas funciones no presentes en la API del sistema de ficheros
        self.find_one = self.fs._GridFS__files.find_one
        self.update = self.fs._GridFS__files.update

    def _open_file(self, path):
        # abrir ficheros en modo binario
        return open(path,'r+b')

    def write_file(self, path, status):
        try:
            file = self._open_file(path)
            self.fs.put(file, filename=path, status=status, user_id=self.user_id)
            print("File written: "+path)
        except:
            print("Unsupported type of file")

    
    def eliminate(self,path):
        """ Elimina fisicamente el fichero de la base de datos.
        Funcion auxiliar no necesaria """

        try:
            for file in (self.fs.find({'filename': path}).limit(1)):
                self.fs.delete(file._id)
        except:
            print("File not found")

    def file_deleted(self, path):
        """ Marca un fichero como eliminado. No se borra realmente para 
        facilitar la implementacion de un sistema de versiones en el futuro"""
        # Se vuelve a fijar el nombre del fichero para facilitar la observacion
        # de las operaciones del oplog
        try:
            self.update({"filename":path}, {"$set":{"status":"deleted","user_id":self.user_id, "filename":path}})
            print("File deleted: "+path)
        except:
            print("File not found")

    def folder_deleted(self, path):
        try:
            results = self.fs.find({"filename":{"$regex":"^"+path+"/.*"}})
            for file in results:
                self.file_deleted(file.filename)
        except:
            print("Folder deleted")

    def change_name(self, original_name, new_name):
        try:
            file = self.fs.get_last_version(original_name)
            # Marca fichero antiguo como cambiado. Mejor como "borrado"?
            self.update({"filename":original_name}, {"$set":{"status":"changed","user_id":self.user_id, "filename":original_name}})
            # Crea fichero con el nuevo nombre
            self.fs.put(file, filename=new_name,old_name=original_name, status="active")
            print("Name changed from "+original_name+" to "+new_name)
        except:
            print("Name change not possible")

    def get_file(self, file):
        try:
            return self.fs.get(file)
        except:
            print("File not available")

    # Encuentra un fichero
    def get_one(self, query):
        try:
            results = self.fs.find(query).limit(1)
            for file in results:
                return file
        except:
            print("File not Found")

    def _eliminate_all(self):
        """ Borra fisicamente todos los ficheros de la base de datos.
        Funcion auxiliar para pruebas """
        try:
            for file in (self.fs.find()):
                self.fs.delete(file._id)
        except:
            print("File not found")

    def get_user_id(self, username):
        """ Dado un nombre de usuario, obtiene un id.
        Ya sea de un usuario ya existente en la base de datos
        o creandolo por primera vez. """

        user = {"username":username}

        try:
            user_id = self.db.users.find_one(user)["_id"]
            return user_id
        except:
            return self.db.users.insert(user)

    # Podria ir en MongoHandler. Refactoriza, hijo de puta
    def download_file(self,filename):
        path = filename.split("/")
        if len(path) > 2:
            path.pop()
            path = "/".join(path)
            try:
                os.makedirs(path)
            except OSError:
                print("Ignorando la creacion de {0} porque ya existe".format(path))

        db_file = self.get_one({"filename":filename})
        with open(filename,"wb") as f_out:
            f_out.write(db_file.read())

    def remove_file(self,filename):
        # Posible bug. No se si lo he arreglado o no he conseguido reproducirlo
        # A veces borraba la carpeta raiz, 'geafiles', al borrar subcarpetas
        try:
            os.remove(filename)
            path = filename.split("/")
            if len(path) > 2:
                path.pop()
                path = "/".join(path)
                try:
                    os.removedirs(path)
                except OSError:
                    print("No es posible borrar el directorio {0} porque no esta vacio.".format(path))
        except: 
            print("File or path invalid")
        
        print("Eliminado: {0}".format(filename))

    # Encuentra un fichero
    def get_one(self, query):
        try:
            results = self.fs.find(query).limit(1)
            for file in results:
                return file
        except:
            print("File not Found")





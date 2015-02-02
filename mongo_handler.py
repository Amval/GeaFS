import pymongo
import gridfs
import os
import sys
import shutil
from config import *

# Escribir arquitectura del sistema en papel. Primitivas, clases, etc
# connect to database

class MongoHandler:

    def __init__(self, username):
        """ Inicializa la clase, conecta a la base de datos, 
        crea una instancia del sistema de ficheros, obtiene id del usuario
        y crea alias para algunas operaciones """

        # conectar a la base de datos
        self.db = pymongo.MongoReplicaSetClient(HOSTS,replicaSet="rs0").geafs
        self.db.read_preference = pymongo.ReadPreference.SECONDARY

        print('succesful connection to database')

        # crear instancia del sistema de ficheros
        self.fs = gridfs.GridFS(self.db)

        self.user_id = self.get_user_id(username)

        # Alias para algunas funciones no presentes en la API del sistema de ficheros
        self.find_one = self.fs._GridFS__files.find_one
        self.update = self.fs._GridFS__files.update

    

    def write_file(self, path,status):

        try:
            file_object = self.fs.new_file(filename=path, status=status)
            with open(path, "r+b") as f:
                byte = f.read(255)
                file_object.write(byte)
                try:
                    while byte:
                        byte = f.read(255)
                        file_object.write(byte)
                finally:
                    file_object.close()
        except:
            print("Unable to read file {0}".format(path))

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
        # necesaria?
        """ Devuelve el fichero especificado """
        try:
            return self.fs.get(file)
        except:
            print("File not available")

    # Encuentra un fichero
    def get_one(self, query):
        """ Busca y devuelve un fichero a partir de una consulta """
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

    def download_file(self,filename):
        """ Descarga un fichero  y lo crea en el cliente.
         Tambien crea el arbol de directorios que lo contiene,
        en caso de ser necesario """

        path = filename.split("/")
        if len(path) > 2:
            path.pop()
            path = "/".join(path)
            try:
                os.makedirs(path)
            except OSError:
                print("The directory {0} already exists".format(path))

        db_file = self.get_one({"filename":filename})
        with open(filename,"wb") as f_out:
            f_out.write(db_file.read())

    def remove_file(self,filename):
        # Posible bug. No se si lo he arreglado o no he conseguido reproducirlo
        # A veces borraba la carpeta raiz, 'geafiles', al borrar subcarpetas
        """ Borra un fichero del cliente y el arbol de directorios
        que lo contiene, en caso de ser necesario. """
        try:
            os.remove(filename)
            path = filename.split("/")
            if len(path) > 2:
                path.pop()
                path = "/".join(path)
                try:
                    print(path)
                    os.removedirs(path)
                except OSError:
                    print("It's not posible to delete {0} directory because it's not empty.".format(path))
        except: 
            print("Removing {0}. File doesn't exist already".format(filename))



    # deprecated
    def _write_file(self, path, status):
        """ Escribe un fichero a la base de datos """
        try:
            file = self._open_file(path)
            self.fs.put(file, filename=path, status=status, user_id=self.user_id)
            print("File written: "+path)
        except:
            print("Unsupported type of file")

    def _open_file(self, path):
        """ Abre el fichero especificado. Funcion de uso interno"""
        return open(path,'r+b')
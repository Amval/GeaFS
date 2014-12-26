import pymongo
import gridfs

# Escribir arquitectura del sistema en papel. Primitivas, clases, etc
# connect to database

class MongoHandler:

    def __init__(self, db):
        # conectar a la base de datos
        self.db = pymongo.MongoClient().geafs
        # crear instancia del sistema de ficheros
        self.fs = gridfs.GridFS(self.db)
        print('succesful connection to database')
        self.find_one = self.fs._GridFS__files.find_one
        self.update = self.fs._GridFS__files.update

    def _open_file(self, path):
        # abrir ficheros en modo binario
        return open(path,'r+b')

    def write_file(self, path, status):
        try:
            file = self._open_file(path)
            self.fs.put(file, filename=path, status=status)
            print("File written: "+path)
        except:
            print("Unsupported type of file")

    # Elimina fisicamente el fichero del sistema.
    # Normalmente no necesario
    def eliminate(self,path):
        try:
            for file in (self.fs.find({'filename': path}).limit(1)):
                self.fs.delete(file._id)
        except:
            print("File not found")

    # Actualiza fichero marcandolo como eliminado
    def file_deleted(self, path):
        try:
            self.fs._GridFS__files.update({"filename":path}, {"$set":{"status":"deleted"}})
            print("File deleted: "+path)
        except:
            print("File not found")

    def folder_deleted(self, path):
        try:
            results = self.fs.find({"filename":{"$regex":"^"+path+"/.*"}})
            for file in results:
                print(file.filename)
                self.file_deleted(file.filename)
        except:
            print("Folder deleted")

    def change_name(self, original_name, new_name):
        try:
            file = self.fs.get_last_version(original_name)
            # Marca fichero antiguo como cambiado. Mejor como "borrado"?
            self.fs._GridFS__files.update({"filename":original_name}, {"$set":{"status":"changed"}})
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
        try:
            for file in (self.fs.find()):
                self.fs.delete(file._id)
        except:
            print("File not found")


#handler = MongoHandler('geafs')

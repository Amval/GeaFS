import hashlib

def return_hash(filename):
	"""
	Esta funcion abre un fichero a partir de su ruta y devuelve un hash.

	Args:
		filename (str): La ruta del fichero a abrir.

	Returns:
		str. Cadena MD5.

	La implementacion actual no es muy eficiente a la hora de usar la memoria, ya que requiere procesar el fichero entero.
	"""
	return hashlib.md5(open(filename, "rb").read()).hexdigest()
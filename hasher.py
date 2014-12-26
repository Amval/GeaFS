import hashlib

def return_hash(filename):
	return hashlib.md5(open(filename, "rb").read()).hexdigest()
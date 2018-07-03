import hashlib


# https://stackoverflow.com/questions/3431825/generating-an-md5-checksum-of-a-file
def digest_file(file):
    hash_md5 = hashlib.md5()
    with open(file, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

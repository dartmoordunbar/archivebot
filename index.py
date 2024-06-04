import os
import hashlib
import mimetypes
import psycopg2

def get_file_info(file_path):
    sha1 = hashlib.sha1()
    size = os.path.getsize(file_path)

    with open(file_path, 'rb') as f:
        while True:
            data = f.read(65536)  # read in 64kb chunks
            if not data:
                break
            sha1.update(data)

    mime_type, _ = mimetypes.guess_type(file_path)
    return sha1.hexdigest(), size, mime_type

def save_to_db(filename, sha1, size, mime_type):
    conn = psycopg2.connect(dbname='your_database_name', user='your_username', password='your_password', host='your_host')
    cur = conn.cursor()
    cur.execute("INSERT INTO files (filename, sha1, size, mime_type) VALUES (%s, %s, %s, %s)", (filename, sha1, size, mime_type))
    conn.commit()
    cur.close()
    conn.close()

def process_directory(dir_path):
    for root, _, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            sha1, size, mime_type = get_file_info(file_path)
            save_to_db(file_path, sha1, size, mime_type)

# Start processing from the root directory
process_directory('/')

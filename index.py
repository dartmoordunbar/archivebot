import os
import shutil
import hashlib
import mimetypes
import psycopg2
import logging

# Set up logging
logging.basicConfig(filename='file_processing.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def get_file_info(file_path):
    try:
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
    except Exception as e:
        logging.error(f'Error getting file info for {file_path}: {e}')

def save_to_db(filename, sha1, size, mime_type):
    try:
        conn = psycopg2.connect(dbname='your_database_name', user='your_username', password='your_password', host='your_host')
        cur = conn.cursor()
        cur.execute("INSERT INTO files (filename, sha1, size, mime_type) VALUES (%s, %s, %s, %s)", (filename, sha1, size, mime_type))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f'Error saving to database for {filename}: {e}')

def copy_and_rename(file_path, sha1):
    try:
        destination_dir = '/path/to/destination/directory'
        destination_path = os.path.join(destination_dir, sha1)
        shutil.copyfile(file_path, destination_path)
    except Exception as e:
        logging.error(f'Error copying and renaming file {file_path} to {sha1}: {e}')

def process_directory(dir_path):
    for root, _, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                sha1, size, mime_type = get_file_info(file_path)
                save_to_db(file_path, sha1, size, mime_type)
                copy_and_rename(file_path, sha1)
            except Exception as e:
                logging.error(f'Error processing file {file_path}: {e}')

# Start processing from the root directory
process_directory('/')

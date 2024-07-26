import os
import shutil
import hashlib
import mimetypes
import psycopg2
import logging
import botocore
import boto3
from PIL import Image
import magic
from pdf2image import convert_from_path
import pandas as pd

# Set up logging
logging.basicConfig(filename='file_processing.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

# Functions


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
        conn = psycopg2.connect(
            dbname='trust',
            user='jamie',
            password='Yltmyya2008!',
            host='ls-f6392d893ff90bf640e6e836dceb89afa9d18912.cspwb35jp0as.eu-west-2.rds.amazonaws.com'
        )
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO recs(file_path, file_id, file_size, file_mime) VALUES (%s, %s, %s, %s) ON CONFLICT (file_id) DO NOTHING",
            (filename, sha1, size, mime_type)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f'Error saving to database for {filename}: {e}')


def s3_upload(source_path, bucket, object_name, mime=None):
    # Upload the file
    file_exists = check_s3(bucket, object_name)
    if file_exists:
        logging.info(f'File {object_name} exists')
        return
    else:
        try:
            logging.info(f'Uploading to {bucket} - {source_path}')
            response = s3.upload_file(
                source_path, bucket, object_name,
                ExtraArgs={'ContentType': mime}
            )
            return
        except Exception as e:
            logging.error(e)
            return False


def make_web(file_path, mime, sha1):
    try:
        if mime == 'application/pdf':
            print("converting PDF")
            pages = convert_from_path(
                file_path,
                output_folder='web/',
                output_file=f'w-{sha1}',
                first_page=1,
                last_page=1,
                dpi=72,
                single_file=True,
                fmt="jpg"
            )
            shutil.move(f'web/w-{sha1}.jpg', f'web/w-{sha1}')
        if mime.startswith('image/'):
            print("converting Image")
            im = Image.open(file_path)
            im.thumbnail([1000, 1000], Image.Resampling.LANCZOS)
            im.save(f'web/w-{sha1}', "webp")
    except Exception as e:
        print(e)


def copy_and_rename(file_path, sha1):
    try:
        destination_dir = './master'
        destination_path = os.path.join(destination_dir, sha1)
        shutil.copyfile(file_path, destination_path)
    except Exception as e:
        logging.error(f'Error copying and renaming file {
            file_path} to {sha1}: {e}'
        )


def check_s3(bucket, key):
    try:
        response = s3.head_object(
            Bucket=bucket,
            Key=key
        )
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True
        else:
            return False
    except Exception as e:
        logging.error(f'Error checking s3 file: {key}: {e}')


def main():

    input_path = "input/"

    # df = pd.DataFrame(index = ["sha1", "path", "size"])

    for root, _, files in os.walk(input_path):
        # if 'data' in root.split(os.sep):
        for file in files:
            try:
                file_path = os.path.join(root, file)
                sha1, size, mime_type = get_file_info(file_path)
                web_name = f"w-{sha1}"
                web_path = f"web/{web_name}"
                master_path = f"master/{sha1}"
                save_to_db(file_path, sha1, size, mime_type)
                make_web(file_path, mime_type, sha1)
                s3_upload(web_path, 'dartmoorweb', web_name)
                copy_and_rename(file_path, sha1)
                s3_upload(master_path, 'dartmoormaster', sha1, mime_type)
            except Exception as e:
                logging.error(f'Error processing file {file_path}: {e}')


if __name__ == "__main__":
    main()

import os
import boto3
import csv
import logging
from dotenv import load_dotenv, find_dotenv
import mysql.connector


def file_list(work_folder):
    fileset = set('')
    files = (file for file in os.listdir(work_folder)
             if os.path.isfile(os.path.join(work_folder, file)))
    for file in files:
        fileset.add(file)
    return fileset


def db_file_list(results):
    fileset = set('')
    logger.info('Connecting to the DB...')
    mydb = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASS"),
        database=os.getenv("MYSQL_DB")
    )

    mycursor = mydb.cursor()

    mycursor.execute(f"SELECT v3_track_variations.file_name \
                    FROM v3_track_variations \
                    INNER JOIN v3_tracks ON v3_tracks.id=v3_track_variations.track_id \
                    WHERE v3_tracks.out_of_rotation=0 LIMIT {results}")

    myresult = mycursor.fetchall()

    for x in myresult:
        fileset.add(x[0])

    print("Database records returned: ", len(fileset))

    return fileset


def s3_file_list(results):
    fileset = set('')
    logger.info('Connecting to S3...')
    response = client.Bucket(os.getenv('SPACES_BUCKET'))
    filelist = list(response.objects.all())
    for f in filelist:
        fileset.add(f.key)

    print("S3 records returned: ", len(fileset))

    return fileset


def write_csv(filename, filelist):
    with open(filename, 'w', newline='') as myfile:
        wr = csv.writer(myfile)
        for row in filelist:
            wr.writerow([row])


load_dotenv(find_dotenv())

logging.basicConfig(
        level=logging.INFO,
        format=f'%(asctime)s %(levelname)s %(message)s'
    )
logger = logging.getLogger()
logger.debug('Starting...')


folder = os.getenv("LOCAL_FOLDER")

session = boto3.session.Session()

client = session.resource('s3',
                        region_name='nyc3',
                        endpoint_url='https://nyc3.digitaloceanspaces.com',
                        aws_access_key_id=os.getenv('SPACES_KEY'),
                        aws_secret_access_key=os.getenv('SPACES_SECRET'))

max_results = 99999
folder_local = (file_list(folder))
folder_db = (db_file_list(max_results))
folder_s3 = (s3_file_list(max_results))

diff_db = folder_db.difference(folder_s3)
diff_s3 = folder_s3.difference(folder_db)


write_csv("errors.csv", diff_db)
write_csv("not_in_db.csv", diff_s3)

print(f"{len(diff_db)} files are enabled in the database, but not found in the file system.")
for f in diff_db:
    print(f)
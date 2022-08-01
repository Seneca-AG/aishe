from datetime import datetime
from lib.db import Database
import csv
import xlrd
import os
import requests
from dotenv import load_dotenv


def download(dest_folder: str, filename:str):
    url = os.getenv('CLOUD_FILE_PATH') + filename
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)  # create folder if it does not exist
    filename = filename  # be careful with file names
    file_path = os.path.join(dest_folder, filename)

    r = requests.get(url, stream=True)
    if r.ok:
        print("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 10000):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
                    return True
    else:  # HTTP status code 4XX/5XX
        print("Download failed: status code {}\n{}".format(r.status_code, r.text))
        return False


def storeResult(filename, productInfo, db):
    with open(filename, 'r') as file:
        csvreader = csv.reader(file, delimiter=";")
        header = next(csvreader)
        print(productInfo)
        for row in csvreader:
            if(row[1] != '' and row[1] !='Time'):
                index = productInfo[0]
                date_time_obj = datetime.strptime(row[1], '%d.%m.%Y %H:%M')
                time = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')
                # print(time, "   low=",row[index].strip().replace(".", ""), "  bid=", row[index + 1].strip().replace(".", ""), "  ask=", row[index + 2].strip().replace(".", ""), " high=", row[index + 3].strip().replace(".", ""), " valuel=", row[index + 4].strip().replace(".", ""), " valueR=", row[index + 5].strip().replace(".", ""), " resilt=", row[index + 6].strip().replace(".", ""))
                # exit()
                db.insert(
                    time=time,
                    low=row[index].strip().replace(".", ""),
                    bid=row[index+1].strip().replace(".", ""),
                    ask=row[index+2].strip().replace(".", ""),
                    high=row[index+3].strip().replace(".", ""),
                    valuel=row[index+4].strip().replace(".", ""),
                    valuer=row[index+5].strip().replace(".", ""),
                    result='',
                )
        db.commit();

load_dotenv()
curr_date = datetime.today()
curr_day = curr_date.strftime('%A')
week = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
dayofweek = week.index(curr_day)
week_day = dayofweek + 1
sheetname = '.'.join([str(week_day), curr_day])
filename = sheetname + '.csv'
destdir = 'csv/'
productInfo = {'EURUSD': [2, 100000, 5], 'USDDKK': [9, 10000, 4], 'USDCHF': [16, 100000, 5], 'EURCAD': [23, 100000, 5], 'USDCAD': [30, 100000, 5], 'EURGBP': [37, 100000, 5], 'GBPUSD': [44, 100000, 5],
             'AUDUSD': [51, 100000, 5], 'EURCHF': [58, 100000, 5], 'AUDJPY': [65, 1, 1], 'XAUUSD': [72, 100, 2]}
result = download(destdir, filename)
db = Database()
db.connect()
if(result):
    database_filename = destdir+filename
    for info in productInfo:
        product = info
        tablename = info + '_' + curr_day;
        tablename = tablename.lower()
        db.settable(tablename)
        results = storeResult(database_filename, productInfo[product], db)
    os.remove(database_filename)
db.close()

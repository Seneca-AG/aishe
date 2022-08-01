# from lib.database import Database as MyDatabase
# my_connection = MyDatabase.connect_dbs()
# exit()
from datetime import datetime
from lib.db import Database
import csv
import xlrd

db = Database()
db.connect()
productInfo = {'EURUSD': [2, 100000, 5], 'USDDKK': [9, 10000, 4], 'USDCHF': [16, 100000, 5], 'EURCAD': [23, 100000, 5], 'USDCAD': [30, 100000, 5], 'EURGBP': [37, 100000, 5], 'GBPUSD': [44, 100000, 5],
             'AUDUSD': [51, 100000, 5], 'EURCHF': [58, 100000, 5], 'AUDJPY': [65, 1, 1], 'XAUUSD': [72, 100, 2]}

database_filename = 'history/Monday.csv'
database_filename = 'history/Tuesday.csv'
database_filename = 'history/Wednesday.csv'
database_filename = 'history/Thursday.csv'
database_filename = 'history/Friday.csv'
def storeResult(filename, productInfo, product, db):
    with open(filename, 'r') as file:
        csvreader = csv.reader(file, delimiter=";")
        header = next(csvreader)
        for row in csvreader:
            if(row[1] != ''):
                index = productInfo[0];
                row[1] = row[1].replace(",", ".")
                row[1] = xlrd.xldate_as_datetime(float(row[1]), 0).strftime('%Y-%m-%d %H:%M:%S')

                db.insert(
                    time=row[1],
                    low=row[index].strip(),
                    bid=row[index+1].strip(),
                    ask=row[index+2].strip(),
                    high=row[index+3].strip(),
                    valuel=row[index+4].strip(),
                    valuer=row[index+5].strip(),
                    result='',
                )
        db.commit();
for info in productInfo:
    product = info
    tablename = info.lower()+'_monday';
    tablename = info.lower()+'_tuesday';
    tablename = info.lower()+'_wednesday';
    tablename = info.lower()+'_thursday';
    tablename = info.lower()+'_friday';
    db.settable(tablename)
    results = storeResult(database_filename, productInfo[product], product, db)
    # exit()
exit();

now = datetime.now()
dt_string = now.strftime("%Y-%m-%d %H:%M:%S")

print("now =", dt_string)
cursor = db.select_all()
#
#
#     cursor.execute("SELECT * FROM climbing_area_info ;")
#
db.insert(
    time=dt_string,
    low='1,19579',
    bid='1,19711',
    ask='1,19724',
    high='1,19729',
    valuel='466430459',
    valuer='493490497',
    result='',
)
db.insert_many(
    columns = ('time', 'low', 'bid', 'ask', 'high', 'valuel', 'valuer', 'result'),
    rows = [
        [dt_string, '1,19579', '1,19711', '1,19724', '1,19729', '466430459', '493490497', ''],
        [dt_string, '1,19579', '1,19711', '1,19724', '1,19729', '466430459', '493490497', '' ]
    ]
)
db.commit();
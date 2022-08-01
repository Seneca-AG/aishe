import os
import requests
import numpy as np
from datetime import datetime, timedelta
import csv
from os.path import exists
import xlrd
from dotenv import load_dotenv
import datetime as dt


# import gspread
from oauth2client.service_account import ServiceAccountCredentials
#from google.oauth2 import service_account
SERVICE_ACCOUNT_FILE = 'keys.json'

def download(url: str, dest_folder: str, filename:str):
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

def listToString(s):
    # initialize an empty string
    str1 = ""
    # traverse in the string
    for ele in s:
        str1 += ele
    return str1

def minmax1 (x):
    # this function fails if the list length is 0
    minimum = maximum = x[0]
    minIndex = maxIndex = 0
    for index, i in enumerate(x):
        if i < minimum:
            minimum = i
            minIndex = index
        else:
            if i > maximum:
                maximum = i
                maxIndex = index
    return (minimum,maximum, minIndex, maxIndex)

# Function to count occurrences
def countOccurrences(arr, x):
    count = 0
    n = len(arr)
    for i in range(n):
        if (arr[i] == x):
            count += 1
    return count

def csvwrite(writer, results):
    writer.writerow(results)
    # for result in results:
        # data = [result['Product'], result['Start'], result['Duration'], result['Event'], result['Value']]
        # writer.writerow(data)

def getCurrentVal(filename, productInfo):
    print((filename))
    with open(filename, 'r') as file:
        lastRow = file.readlines()[-1].split(";")
    currentVal = {'time':lastRow[1]}
    for info in productInfo:
        index = productInfo[info][0]
        currentVal[info] = lastRow[index+4] + ',' + lastRow[index+5]
    file.close()
    return {'download_status': True, 'currentVal': currentVal}

def getCurrentValFromCloud(fileInfo, destdir, filename, productInfo):
    url = 'https://docs.google.com/uc?export=download&id=' + fileInfo
    download_status = download(url, destdir, filename)
    if(download_status):
        filename = destdir+filename
        with open(filename, 'r') as file:
            lastRow = file.readlines()[-1].split(";")
        currentVal = {'time':lastRow[1]}
        for info in productInfo:
            index = productInfo[info][0]
            currentVal[info] = lastRow[index+4] + ',' + lastRow[index+5]
        return {'download_status': download_status, 'currentVal': currentVal}
    else:
        return {'download_status': False}

def matching(filename, productInfo, matchingPositionLength, product, currentVal, duration):
    rows = []
    time = []
    time = []
    low = []
    bid = []
    ask = []
    high = []
    valueLR = []

    # currentVal = result['currentVal']
    currentTime = currentVal['time']
    time_change = dt.timedelta(minutes=duration)
    date_time_obj = dt.datetime.strptime(currentTime, '%d.%m.%Y %H:%M')
    new_time = date_time_obj + time_change
    max_time = new_time.strftime("%d.%m.%Y %H:%M")
    with open(filename, 'r') as file:
        csvreader = csv.reader(file, delimiter=";")
        header = next(csvreader)
        for row in csvreader:
            if(row[1] != ''):
                row[1] = row[1].replace(",", ".")
                row[1] = xlrd.xldate_as_datetime(float(row[1]), 0).strftime('%d.%m.%Y %H:%M:%S')
                row[0] = row[1].replace(row[1][0:10], currentVal['time'][0:10])
                if ((currentVal['time'] <= row[0])):
                    rows.append(row)
                    index = productInfo[0];
                    time.append(row[1])
                    low.append(row[index].strip())
                    value = row[index + 1].replace(",", "")
                    value = value.replace(".", "")
                    bid.append(value)
                    value = row[index + 2].replace(",", "")
                    value = value.replace(".", "")
                    ask.append(value)
                    high.append(row[index+3].strip())
                    valueLR.append(row[index+4].strip() + ',' + row[index+5].strip())
    arr = np.array(valueLR)
    currentVal = currentVal[product]
    currentVal = '530570549,481450481'
    x = np.where(arr == currentVal)
    datalength = len(rows)
    result = []
    if(len(x[0])):
        for StartMatchPoint in x[0]:
            endMatchPoint = StartMatchPoint + matchingPositionLength
            if(datalength < endMatchPoint):
                matchingPositionLength = datalength - StartMatchPoint
            bidCompare = np.full(matchingPositionLength, bid[x[0][0]])
            askCompare = np.full(matchingPositionLength, ask[x[0][0]])
            bidMatch = np.split(bid, [StartMatchPoint, endMatchPoint])
            bidMatch = bidMatch[1]
            askMatch = np.split(ask, [StartMatchPoint, endMatchPoint])
            askMatch = askMatch[1]
            bidResult1 = bidMatch < bidCompare
            askResult1 = askMatch < askCompare
            bidResult = minmax1(bidMatch)
            askResult = minmax1(askMatch)
            bidCount = countOccurrences(bidResult1, True)
            dataCount = round(len(bidResult1)/2)
            bidMinPosition = StartMatchPoint + bidResult[2]
            bidMaxPosition = StartMatchPoint + bidResult[3]
            askMinPosition = StartMatchPoint + askResult[2]
            askMaxPosition = StartMatchPoint + askResult[3]
            start = rows[StartMatchPoint][0]
            if(dataCount <= bidCount):
                event = 'SELL'
                end = rows[bidMinPosition][0]
                value = round(float(bidResult[1]) - float(bidResult[0]), productInfo[2])
            else:
                event = 'BUY'
                end = rows[askMaxPosition][0]
                value = round(float(askResult[1]) - float(askResult[0]), productInfo[2])
            value = value
            time_format = "%d.%m.%Y %H:%M:%S"
            dt1 = datetime.strptime(start, time_format)
            dt2 = datetime.strptime(end, time_format)
            diff = ((dt2 - dt1) // timedelta(minutes=1))  # minutes
            if ((start <= max_time)):
                print('Product=',  product, ', CurrentValue=', currentVal, ',Start=', start, ', Duration=', diff, ', Event=', event, ', Value=', int(value))
                result.append([product, start, diff, event, int(value)])
        return result
    else:
        print('No matching')
        return False

productInfo = {'EURUSD': [2, 100000, 5], 'USDDKK': [9, 10000, 4], 'USDCHF': [16, 100000, 5], 'EURCAD': [23, 100000, 5], 'USDCAD': [30, 100000, 5], 'EURGBP': [37, 100000, 5], 'GBPUSD': [44, 100000, 5],
             'AUDUSD': [51, 100000, 5], 'EURCHF': [58, 100000, 5], 'AUDJPY': [65, 1, 1], 'XAUUSD': [72, 100, 2]}
fileInfo = {'Sunday': '1e8jhfnN8vxWjkuo71xzh-1Z8Tic5rodr', 'Monday': '1rVqzlJ2u6YEEGpfgpvUNbj3-42c5wOWJ',
            'Tuesday': '1HOn_9lJD0D_uXzdQ79T_plF7KCA4rNYF', 'Wednesday': '1QaXf7L1xy-jgr50tJ09MbvyVWVbXGUdK',
            'Thursday': '1WPGLOp9cH8yrJ9lz6ymi9L7C5s9SyXUY', 'Friday': '1FLtVOfwkJJmXIzzq6KfsKEZ7UKC0Oili',
            'Saturday': '1jgOS3a4wvIYKeDOLjTLd9BfYqGA6dekO' }

databaseInfo = {'Sunday': '', 'Monday': 'Monday.csv', 'Tuesday': 'Tuesday.csv', 'Wednesday': 'Wednesday.csv',
                'Thursday': 'Thursday.csv', 'Friday': 'Friday.csv', 'Saturday': '' }
# Identify the date
load_dotenv()
CLOUD_FILE_PATH = os.getenv('CLOUD_FILE_PATH')
curr_date = datetime.today()
curr_day = curr_date.strftime('%A')
week = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
dayofweek = week.index(curr_day)
week_day = dayofweek + 1
sheetname = '.'.join([str(week_day), curr_day])
filename = sheetname + '.csv'
database_filename = 'history/'+curr_day+'.csv'
destdir = 'csv/'
matchingPositionLength = 10
resultfilename = sheetname + '_results.csv'
resultfile = destdir + resultfilename;
result = getCurrentVal(CLOUD_FILE_PATH+filename, productInfo)
# result = getCurrentValFromCloud(fileInfo[curr_day], destdir, filename, productInfo)
currentVal = result['currentVal']
download_status = result['download_status']
duration = int(os.getenv('DURATION'))
if(download_status):
    filename = destdir+filename
    resultdata = {}
    # if ( exists(resultfile)):
    #     with open(resultfile, 'r') as file:
    #         csvreader = csv.reader(file, delimiter=";")
    #         header = next(csvreader)
    #         for row in csvreader:
    #             key = row[0]+row[1]
    #             resultdata[key] = row
    # else:
    #     with open(resultfile, 'w', encoding='UTF8', newline='') as f:
    #         header = ['Product', 'Start', 'Duration', 'Event', 'Value']
    #         writer = csv.writer(f, delimiter =";", quoting=csv.QUOTE_MINIMAL)
    #         writer.writerow(header)
    for info in productInfo:
        product = info
        results = matching(database_filename, productInfo[product], matchingPositionLength, product, currentVal, duration)
        if(results):
            with open(resultfile, 'w', encoding='UTF8', newline='') as f:
                header = ['Product', 'Start', 'Duration', 'Event', 'Value']
                writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
                writer.writerow(header)
                for result in results:
                    csvwrite(writer, result)
            # if(not exists(resultfile)):
            #     with open(resultfile, 'w', encoding='UTF8', newline='') as f:
            #         writer = csv.writer(f, delimiter =";", quoting=csv.QUOTE_MINIMAL)
            #         for result in results:
            #             csvwrite(writer, result)
            # else:
            #     with open(resultfile, 'a', encoding='UTF8', newline='') as f:
            #         writer = csv.writer(f, delimiter =";", quoting=csv.QUOTE_MINIMAL)
            #         for result in results:
            #             key = result[0]+result[1]
            #             if key not in resultdata:
            #                 csvwrite(writer, result)

    # if (exists(resultfile)):
    #     scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
    #              "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    #     credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    #     gc = gspread.authorize(credentials)
    #     # # Read CSV file contents
    #     content = open(resultfile, 'r').read()
    #     gc.import_csv('196QyQuGmkYkUPKzp4xV3PDcveTEVjXsST0oXkajSRdY', content)
else:
    print("Download failed: status code")
# files = os.listdir(destdir)
# for file in files:
#     if(resultfilename != file):
#         os.remove(os.path.join(destdir, file))

#os.remove(resultfilename)

import os
import requests
import numpy as np
from datetime import datetime, timedelta
import csv
from os.path import exists

import gspread
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

def matching(filename, productInfo, matchingPositionLength, product):
    rows = []
    time = []
    low = []
    bid = []
    ask = []
    high = []
    valueL = []
    valueR = []
    valueLR = []
    with open(filename, 'r') as file:
        csvreader = csv.reader(file, delimiter=";")
        header = next(csvreader)
        for row in csvreader:
            #row = value
            rows.append(row)
            index = productInfo[0];
            time.append(row[1].strip())
            low.append(row[index].strip())
            #bid.append(row[index+1].strip())
            value = row[index + 1].replace(",", "")
            value = value.replace(".", "")
            bid.append(value)
            #ask.append(row[index + 2].strip())
            value = row[index + 2].replace(",", "")
            value = value.replace(".", "")
            ask.append(value)
            high.append(row[index+3].strip())
            valueL.append(row[index+4].strip())
            valueR.append(row[index+5].strip())
            valueLR.append(row[index+4].strip() + ',' + row[index+5].strip())
    arr = np.array(valueLR)
    currentVal = valueLR[-1]
    # if(product=='EURCHF'):
    #     currentVal = '480430479,487450489'
    # elif(product=='USDCHF'):
    #     currentVal = '486430487,494490499'
    # elif (product == 'EURGBP'):
    #     currentVal = '499530505,501570509'
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
            start = time[StartMatchPoint]
            if(dataCount <= bidCount):
                event = 'SELL'
                end = time[bidMinPosition]
                value = round(float(bidResult[1]) - float(bidResult[0]), productInfo[2])
            else:
                event = 'BUY'
                end = time[askMaxPosition]
                value = round(float(askResult[1]) - float(askResult[0]), productInfo[2])
            value = value
            time_format = "%d.%m.%Y %H:%M:%S"
            dt1 = datetime.strptime(start, time_format)
            dt2 = datetime.strptime(end, time_format)
            diff = ((dt2 - dt1) // timedelta(minutes=1))  # minutes
            print('Product=',  product, ', CurrentValue=', currentVal, ',Start=', start, ', Duration=', diff, ', Event=', event, ', Value=', int(value))
            if(diff > 0):
                #result.append({'Product': product, 'Start': start, 'Duration': diff, 'Event': event, 'Value': int(value)})
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
# Identify the date
curr_date = datetime.today()
curr_day = curr_date.strftime('%A')
week = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
dayofweek = week.index(curr_day)
week_day = dayofweek + 1
sheetname = '.'.join([str(week_day), curr_day])
filename = sheetname + '.csv'
destdir = 'csv/'
matchingPositionLength = 10
resultfilename = sheetname + '_results.csv'
resultfile = destdir + resultfilename;
url = 'https://docs.google.com/uc?export=download&id='+fileInfo[curr_day]
download_status = download(url, destdir, filename)
if(download_status):
    filename = destdir+filename
    resultdata = {}
    if ( exists(resultfile)):
        with open(resultfile, 'r') as file:
            csvreader = csv.reader(file, delimiter=",")
            header = next(csvreader)
            for row in csvreader:
                key = row[0]+row[1]
                resultdata[key] = row
    else:
        with open(resultfile, 'w', encoding='UTF8', newline='') as f:
            header = ['Product', 'Start', 'Duration', 'Event', 'Value']
            writer = csv.writer(f)
            writer.writerow(header)
    for info in productInfo:
        product = info
        results = matching(filename, productInfo[product], matchingPositionLength, product)
        if(results):
            if(not exists(resultfile)):
                with open(resultfile, 'w', encoding='UTF8', newline='') as f:
                    writer = csv.writer(f)
                    for result in results:
                        csvwrite(writer, result)

            else:
                with open(resultfile, 'a', encoding='UTF8', newline='') as f:
                    writer = csv.writer(f)
                    for result in results:
                        key = result[0]+result[1]
                        if key not in resultdata:
                            csvwrite(writer, result)

    if (exists(resultfile)):
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(credentials)
        # # Read CSV file contents
        content = open(resultfile, 'r').read()
        gc.import_csv('196QyQuGmkYkUPKzp4xV3PDcveTEVjXsST0oXkajSRdY', content)
else:
    print("Download failed: status code")
files = os.listdir(destdir)
for file in files:
    if(resultfilename != file):
        os.remove(os.path.join(destdir, file))

#os.remove(resultfilename)

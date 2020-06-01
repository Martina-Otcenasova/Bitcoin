import statistics as stat
from datetime import datetime
import csv
import numpy as np

file_path = "/Users/mata/Digitalni_Akademie/00_Projekt_Kryptomeny/Data/Data-Raw/XRP-USD.csv" # change path for other curency
currency = "XRP" # for other currencies we must change file_path and currency

temporary_list = []
last_day = ""

header = [
        "currency", "day", 
        "sell_count", "buy_count", "market_count", "limit_count", "sl_count", "sm_count", "bl_count", "bm_count", 
        "price_open", "price_max", "price_min", "price_mean",
        "volume_sum",
        "trans_price_sell_sum", "trans_price_sell_mean", 
        "trans_price_buy_sum",  "trans_price_buy_mean"
        ]

# Initialization block: reader and writers:
out_file = open('daily-XRP-new.csv', 'w')
writer = csv.writer(out_file)

in_file = open(file_path)
reader = csv.reader(in_file)

writer.writerow(header)

# Here where the small function's definition happens

def getSellVolumes(list):
    volumes = []
    for line in list:
        if line[3] == "s":
            volumes.append(float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getSellTransPrice(list): # Volume * Price = > price per transaction
    volumes = [] 
    for line in list:
        if line[3] == "s":
            volumes.append(float(line[0])*float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getBuyTransPrice(list):
    volumes = [] 
    for line in list:
        if line[3] == "b":
            volumes.append(float(line[0])*float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes


# counts
def getSellLimitCount(list):
    count = []
    for line in list:
        if line[3] == "s" and line[4] == "l":
            count.append(1)
    return count

def getSellMarketCount(list):
    count = []
    for line in list:
        if line[3] == "s" and line[4] == "m":
            count.append(1)
    return count 

def getBuyLimitCount(list):
    count = []
    for line in list:
        if line[3] == "b" and line[4] == "l":
            count.append(1)
    return count

def getBuyMarketCount(list):
    count = []
    for line in list:
        if line[3] == "b" and line[4] == "m":
            count.append(1)
    return count 


# Aggregation:
def aggregate(list, day, i):  
    print("processing", day, "of total",i,"lines") # i => we can check number of lines while processing
    # Prices
    price_open = round(float(list[0][0]), 2)
    price_max = round(max(float(_[0]) for _ in list), 2)
    price_min = round(min(float(_[0]) for _ in list), 2)
    price_mean = round(stat.mean(float(_[0]) for _ in list), 2)


    # Count of Transactions
    sell_count = [_[3] for _ in list].count('s')
    buy_count = [_[3] for _ in list].count('b')
    market_count = [_[4] for _ in list].count('m')
    limit_count = [_[4] for _ in list].count('l')
    sl_count = sum(getSellLimitCount(list))
    sm_count = sum(getSellMarketCount(list))
    bl_count = sum(getBuyLimitCount(list))
    bm_count = sum(getBuyMarketCount(list))
   

    # Volumes:
    volume_sum = round(sum(float(_[1]) for _ in list), 2)

    # prices per transaction:
    trans_price_sell = getSellTransPrice(list)
    trans_price_sell_sum = round(sum(trans_price_sell), 2)
    trans_price_sell_mean = round(stat.mean(trans_price_sell), 2)

    trans_price_buy = getBuyTransPrice(list)
    trans_price_buy_sum = round(sum(trans_price_buy), 2)
    trans_price_buy_mean = round(stat.mean(trans_price_buy), 2)
    

    result = [
        currency, day, 
        sell_count, buy_count, market_count, limit_count, sl_count, sm_count, bl_count, bm_count, 
        price_open, price_max, price_min, price_mean,
        volume_sum,
        trans_price_sell_sum, trans_price_sell_mean, 
        trans_price_buy_sum,  trans_price_buy_mean
        ]
    #print(result)
    writer.writerow(result)



#------------------------------------------------------------
# Main program begins:
start_time = datetime.now()

for i, line in enumerate(reader):
    #print(line)
    day = datetime.fromtimestamp(float(line[2])).strftime('%Y-%m-%d')
    #print(day)
    if day != last_day and i > 0:
        aggregate(temporary_list, last_day, i)
        temporary_list = []
    last_day = day
    temporary_list.append(line + [day])


aggregate(temporary_list, last_day, i)

in_file.close()
out_file.close()

# Main progam ends
#--------------------------------------------------------------

# Stopwatch - duration of aggregation
print("Start:", start_time)
print("End:", datetime.now())
print("Duration:", datetime.now() - start_time)
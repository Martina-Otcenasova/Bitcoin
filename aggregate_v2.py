import statistics as stat
from datetime import datetime
import csv
import numpy as np

file_path = "/Users/mata/Digitalni_Akademie/00_Projekt_Kryptomeny/Data/Data-Raw/XBT-USD-till-2020-05-20.csv" # change path for other curency
currency = "XBT" # for other currencies we must change file_path and currency

temporary_list = []
last_day = ""

header = [
        "currency", "day", 
        "sell_count", "buy_count", "market_count", "limit_count", "sl_count", "sm_count", "bl_count", "bm_count", 
        "price_open", "price_max", "price_min", "price_mean", "price_median", "price_x10", "price_x25", "price_x75", "price_x90",
        "price_sell_max", "price_sell_min", "price_sell_mean", "price_sell_median", "price_sell_x10", "price_sell_x25", "price_sell_x75", "price_sell_x90",
        "price_buy_max", "price_buy_min", "price_buy_mean", "price_buy_median", "price_buy_x10", "price_buy_x25", "price_buy_x75", "price_buy_x90",
        "price_sm_max", "price_sm_min", "price_sm_mean", "price_sm_median", "price_sm_x10", "price_sm_x25", "price_sm_x75", "price_sm_x90",
        "price_sl_max", "price_sl_min", "price_sl_mean", "price_sl_median", "price_sl_x10", "price_sl_x25", "price_sl_x75", "price_sl_x90",
        "price_bm_max", "price_bm_min", "price_bm_mean", "price_bm_median", "price_bm_x10", "price_bm_x25", "price_bm_x75", "price_bm_x90",
        "price_bl_max", "price_bl_min", "price_bl_mean", "price_bl_median", "price_bl_x10", "price_bl_x25", "price_bl_x75", "price_bl_x90",
        "volume_sum", "volume_max", "volume_min", "volume_mean", "volume_median", "volume_x10", "volume_x25", "volume_x75", "volume_x90",
        "volume_buy_sum", "volume_buy_max", "volume_buy_min", "volume_buy_mean", "volume_buy_median", "volume_buy_x10", "volume_buy_x25", "volume_buy_x75", "volume_buy_x90",
        "volume_sell_sum", "volume_sell_max", "volume_sell_min", "volume_sell_mean", "volume_sell_median", "volume_sell_x10", "volume_sell_x25", "volume_sell_x75", "volume_sell_x90",
        "volume_sm_sum", "volume_sm_max", "volume_sm_min", "volume_sm_mean", "volume_sm_median", "volume_sm_x10", "volume_sm_x25", "volume_sm_x75", "volume_sm_x90", 
        "volume_sl_sum", "volume_sl_max", "volume_sl_min", "volume_sl_mean", "volume_sl_median", "volume_sl_x10", "volume_sl_x25", "volume_sl_x75", "volume_sl_x90",
        "volume_bm_sum", "volume_bm_max", "volume_bm_min", "volume_bm_mean", "volume_bm_median", "volume_bm_x10", "volume_bm_x25", "volume_bm_x75", "volume_bm_x90", 
        "volume_bl_sum", "volume_bl_max", "volume_bl_min", "volume_bl_mean", "volume_bl_median", "volume_bl_x10", "volume_bl_x25", "volume_bl_x75", "volume_bl_x90",
        "trans_price_sell_sum", "trans_price_sell_max", "trans_price_sell_min",  "trans_price_sell_mean", "trans_price_sell_median", "trans_price_sell_x10", "trans_price_sell_x25", "trans_price_sell_x75", "trans_price_sell_x90",
        "trans_price_buy_sum", "trans_price_buy_max", "trans_price_buy_min",  "trans_price_buy_mean", "trans_price_buy_median", "trans_price_buy_x10", "trans_price_buy_x25", "trans_price_buy_x75", "trans_price_buy_x90",
        "trans_price_sm_sum", "trans_price_sm_max", "trans_price_sm_min",  "trans_price_sm_mean", "trans_price_sm_median", "trans_price_sm_x10", "trans_price_sm_x25", "trans_price_sm_x75", "trans_price_sm_x90", 
        "trans_price_sl_sum", "trans_price_sl_max", "trans_price_sl_min",  "trans_price_sl_mean", "trans_price_sl_median", "trans_price_sl_x10", "trans_price_sl_x25", "trans_price_sl_x75", "trans_price_sl_x90", 
        "trans_price_bm_sum", "trans_price_bm_max", "trans_price_bm_min",  "trans_price_bm_mean", "trans_price_bm_median", "trans_price_bm_x10", "trans_price_bm_x25", "trans_price_bm_x75", "trans_price_bm_x90", 
        "trans_price_bl_sum", "trans_price_bl_max", "trans_price_bl_min",  "trans_price_bl_mean", "trans_price_bl_median", "trans_price_bl_x10", "trans_price_bl_x25", "trans_price_bl_x75", "trans_price_bl_x90"
        ]

# Initialization block: reader and writers:
out_file = open('daily-XBT_till_2020-05-20.csv', 'w')
writer = csv.writer(out_file)

in_file = open(file_path)
reader = csv.reader(in_file)

writer.writerow(header)

# Here where the small function's definition happens
def getSellPrices(list):
    prices = []
    for line in list:
        if line[3] == "s" :
            prices.append(float(line[0]))
    if len(prices) == 0:
        prices.append(0)
    return prices

def getBuyPrices(list):
    prices = []
    for line in list:
        if line[3] == "b" :
            prices.append(float(line[0]))
    if len(prices) == 0:
        prices.append(0)
    return prices

def getSellMarketPrices(list):
    prices = []
    for line in list:
        if line[3] == "s" and line[4] == "m":
            prices.append(float(line[1]))
    if len(prices) == 0:
        prices.append(0)
    return prices

def getSellLimitPrices(list):
    prices = []
    for line in list:
        if line[3] == "s" and line[4] == "l":
            prices.append(float(line[1]))
    if len(prices) == 0:
        prices.append(0)
    return prices

def getBuyMarketPrices(list):
    prices = []
    for line in list:
        if line[3] == "b" and line[4] == "m":
            prices.append(float(line[1]))
    if len(prices) == 0:
        prices.append(0)
    return prices

def getBuyLimitPrices(list):
    prices = []
    for line in list:
        if line[3] == "b" and line[4] == "l":
            prices.append(float(line[1]))
    if len(prices) == 0:
        prices.append(0)
    return prices


def getSellVolumes(list):
    volumes = []
    for line in list:
        if line[3] == "s":
            volumes.append(float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getBuyVolumes(list):
    volumes = []
    for line in list:
        if line[3] == "b":
            volumes.append(float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getSellMarketVolumes(list):
    volumes = []
    for line in list:
        if line[3] == "s" and line[4] == "m":
            volumes.append(float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getSellLimitVolumes(list):
    volumes = [] 
    for line in list:
        if line[3] == "s" and line[4] == "l":
            volumes.append(float(line[1])) 
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getBuylMarketVolumes(list):
    volumes = []
    for line in list:
        if line[3] == "b" and line[4] == "m":
            volumes.append(float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getBuyLimitVolumes(list):
    volumes = [] 
    for line in list:
        if line[3] == "b" and line[4] == "l":
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

def getSellMarketTransPrice(list):
    volumes = [] 
    for line in list:
        if line[3] == "s" and line[4] == "m":
            volumes.append(float(line[0])*float(line[1]))
    if len(volumes) == 0:
        volumes.append(0) 
    return volumes

def getSellLimitTransPrice(list): 
    volumes = [] 
    for line in list:
        if line[3] == "s" and line[4] == "l":
            volumes.append(float(line[0])*float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getBuyMarketTransPrice(list): 
    volumes = [] 
    for line in list:
        if line[3] == "b" and line[4] == "m":
            volumes.append(float(line[0])*float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

def getBuyLimitTransPrice(list): 
    volumes = [] 
    for line in list:
        if line[3] == "b" and line[4] == "l":
            volumes.append(float(line[0])*float(line[1]))
    if len(volumes) == 0:
        volumes.append(0)
    return volumes

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
    price_median = round(stat.median(float(_[0]) for _ in list), 2)
    price_x10 = round(np.percentile([float(_[0]) for _ in list], 10), 2)
    price_x25 = round(np.percentile([float(_[0]) for _ in list], 25), 2)
    price_x75 = round(np.percentile([float(_[0]) for _ in list], 75), 2)
    price_x90 = round(np.percentile([float(_[0]) for _ in list], 90), 2)

    price_buy = getBuyPrices(list)
    price_buy_max = round(max(price_buy), 2)
    price_buy_min = round(min(price_buy), 2)
    price_buy_mean = round(stat.mean(price_buy), 2)
    price_buy_median = round(stat.median(price_buy), 2)
    price_buy_x10 = round(np.percentile(price_buy, 10), 2)
    price_buy_x25 = round(np.percentile(price_buy, 25), 2)
    price_buy_x75 = round(np.percentile(price_buy, 75), 2)
    price_buy_x90 = round(np.percentile(price_buy, 90), 2)

    price_sell = getSellPrices(list)
    price_sell_max = round(max(price_sell), 2)
    price_sell_min = round(min(price_sell), 2)
    price_sell_mean = round(stat.mean(price_sell), 2)
    price_sell_median = round(stat.median(price_sell), 2)
    price_sell_x10 = round(np.percentile(price_sell, 10), 2)
    price_sell_x25 = round(np.percentile(price_sell, 25), 2)
    price_sell_x75 = round(np.percentile(price_sell, 75), 2)
    price_sell_x90 = round(np.percentile(price_sell, 90), 2)

    price_sm = getSellMarketPrices(list)
    price_sm_max = round(max(price_sm), 2)
    price_sm_min = round(min(price_sm), 2)
    price_sm_mean = round(stat.mean(price_sm), 2)
    price_sm_median = round(stat.median(price_sm), 2)
    price_sm_x10 = round(np.percentile(price_sm, 10), 2)
    price_sm_x25 = round(np.percentile(price_sm, 25), 2)
    price_sm_x75 = round(np.percentile(price_sm, 75), 2)
    price_sm_x90 = round(np.percentile(price_sm, 90), 2)
    
    price_sl = getSellLimitPrices(list) # oprava v2, preklep ve volané funkci
    price_sl_max = round(max(price_sl), 2)
    price_sl_min = round(min(price_sl), 2)
    price_sl_mean = round(stat.mean(price_sl), 2)
    price_sl_median = round(stat.median(price_sl), 2)
    price_sl_x10 = round(np.percentile(price_sl, 10), 2)
    price_sl_x25 = round(np.percentile(price_sl, 25), 2)
    price_sl_x75 = round(np.percentile(price_sl, 75), 2)
    price_sl_x90 = round(np.percentile(price_sl, 90), 2)

    price_bm = getBuyMarketPrices(list)
    price_bm_max = round(max(price_bm), 2)
    price_bm_min = round(min(price_bm), 2)
    price_bm_mean = round(stat.mean(price_bm), 2)
    price_bm_median = round(stat.median(price_bm), 2)
    price_bm_x10 = round(np.percentile(price_bm, 10), 2)
    price_bm_x25 = round(np.percentile(price_bm, 25), 2)
    price_bm_x75 = round(np.percentile(price_bm, 75), 2)
    price_bm_x90 = round(np.percentile(price_bm, 90), 2)

    price_bl = getBuyLimitPrices(list)
    price_bl_max = round(max(price_bl), 2)
    price_bl_min = round(min(price_bl), 2)
    price_bl_mean = round(stat.mean(price_bl), 2)
    price_bl_median = round(stat.median(price_bl), 2)
    price_bl_x10 = round(np.percentile(price_bl, 10), 2)
    price_bl_x25 = round(np.percentile(price_bl, 25), 2)
    price_bl_x75 = round(np.percentile(price_bl, 75), 2)
    price_bl_x90 = round(np.percentile(price_bl, 90), 2)


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
    volume_max = round(max(float(_[1]) for _ in list), 2)
    volume_min = round(min(float(_[1]) for _ in list), 2)
    volume_mean = round(stat.mean(float(_[1]) for _ in list), 2)
    volume_median = round(stat.median(float(_[1]) for _ in list), 2)
    volume_x10 = round(np.percentile([float(_[1]) for _ in list], 10), 2)
    volume_x25 = round(np.percentile([float(_[1]) for _ in list], 25), 2)
    volume_x75 = round(np.percentile([float(_[1]) for _ in list], 75), 2)
    volume_x90 = round(np.percentile([float(_[1]) for _ in list], 90), 2)
    
    volume_sell = getSellVolumes(list)
    volume_sell_sum = round(sum(volume_sell), 2)
    volume_sell_max = round(max(volume_sell), 2)
    volume_sell_min = round(min(volume_sell), 2)
    volume_sell_mean = round(stat.mean(volume_sell),2)
    volume_sell_median = round(stat.median(volume_sell), 2)
    volume_sell_x10 = round(np.percentile(volume_sell, 10), 2)
    volume_sell_x25 = round(np.percentile(volume_sell, 25), 2)
    volume_sell_x75 = round(np.percentile(volume_sell, 75), 2)
    volume_sell_x90 = round(np.percentile(volume_sell, 90), 2)

    volume_buy = getBuyVolumes(list)
    volume_buy_sum = round(sum(volume_buy), 2)
    volume_buy_max = round(max(volume_buy), 2)
    volume_buy_min = round(min(volume_buy), 2)
    volume_buy_mean = round(stat.mean(volume_buy),2)
    volume_buy_median = round(stat.median(volume_buy), 2)
    volume_buy_x10 = round(np.percentile(volume_buy, 10), 2)
    volume_buy_x25 = round(np.percentile(volume_buy, 25), 2)
    volume_buy_x75 = round(np.percentile(volume_buy, 75), 2)
    volume_buy_x90 = round(np.percentile(volume_buy, 90), 2)


    volume_sm = getSellMarketVolumes(list)
    volume_sm_sum = round(sum(volume_sm), 2)
    volume_sm_max = round(max(volume_sm), 2)
    volume_sm_min = round(min(volume_sm), 2)
    volume_sm_mean = round(stat.mean(volume_sm),2)
    volume_sm_median = round(stat.median(volume_sm), 2)
    volume_sm_x10 = round(np.percentile(volume_sm, 10), 2)
    volume_sm_x25 = round(np.percentile(volume_sm, 25), 2)
    volume_sm_x75 = round(np.percentile(volume_sm, 75), 2)
    volume_sm_x90 = round(np.percentile(volume_sm, 90), 2)

    volume_sl = getSellLimitVolumes(list)
    volume_sl_sum = round(sum(volume_sl), 2)
    volume_sl_max = round(max(volume_sl), 2)
    volume_sl_min = round(min(volume_sl), 2)
    volume_sl_mean = round(stat.mean(volume_sl), 2)
    volume_sl_median = round(stat.median(volume_sl), 2)
    volume_sl_x10 = round(np.percentile(volume_sl, 10), 2)
    volume_sl_x25 = round(np.percentile(volume_sl, 25), 2)
    volume_sl_x75 = round(np.percentile(volume_sl, 75), 2)
    volume_sl_x90 = round(np.percentile(volume_sl, 90), 2)

    volume_bm = getBuylMarketVolumes(list)
    volume_bm_sum = sum(volume_bm)
    volume_bm_max = max(volume_bm)
    volume_bm_min = min(volume_bm)
    volume_bm_mean = round(stat.mean(volume_bm), 2)
    volume_bm_median = round(stat.median(volume_bm), 2)
    volume_bm_x10 = round(np.percentile(volume_bm, 10), 2)
    volume_bm_x25 = round(np.percentile(volume_bm, 25), 2)
    volume_bm_x75 = round(np.percentile(volume_bm, 75), 2)
    volume_bm_x90 = round(np.percentile(volume_bm, 90), 2)


    volume_bl = getBuyLimitVolumes(list)
    volume_bl_sum = round(sum(volume_bl), 2)
    volume_bl_max = round(max(volume_bl), 2)
    volume_bl_min = round(min(volume_bl), 2)
    volume_bl_mean = round(stat.mean(volume_bl), 2)
    volume_bl_median = round(stat.median(volume_bl), 2)
    volume_bl_x10 = round(np.percentile(volume_bl, 10), 2)
    volume_bl_x25 = round(np.percentile(volume_bl, 25), 2)
    volume_bl_x75 = round(np.percentile(volume_bl, 75), 2)
    volume_bl_x90 = round(np.percentile(volume_bl, 90), 2)

    # prices per transaction:
    trans_price_sell = getSellTransPrice(list)
    trans_price_sell_sum = round(sum(trans_price_sell), 2)
    trans_price_sell_max = round(max(trans_price_sell), 2)
    trans_price_sell_min = round(min(trans_price_sell), 2)
    trans_price_sell_mean = round(stat.mean(trans_price_sell), 2)
    trans_price_sell_median = round(stat.median(trans_price_sell), 2)
    trans_price_sell_x10 = round(np.percentile(trans_price_sell, 10), 2)
    trans_price_sell_x25 = round(np.percentile(trans_price_sell, 25), 2)
    trans_price_sell_x75 = round(np.percentile(trans_price_sell, 75), 2)
    trans_price_sell_x90 = round(np.percentile(trans_price_sell, 90), 2)

    trans_price_buy = getBuyTransPrice(list)
    trans_price_buy_sum = round(sum(trans_price_buy), 2)
    trans_price_buy_max = round(max(trans_price_buy), 2)
    trans_price_buy_min = round(min(trans_price_buy), 2)
    trans_price_buy_mean = round(stat.mean(trans_price_buy), 2)
    trans_price_buy_median = round(stat.median(trans_price_buy), 2)
    trans_price_buy_x10 = round(np.percentile(trans_price_buy, 10), 2)
    trans_price_buy_x25 = round(np.percentile(trans_price_buy, 25), 2)
    trans_price_buy_x75 = round(np.percentile(trans_price_buy, 75), 2)
    trans_price_buy_x90 = round(np.percentile(trans_price_buy, 90), 2)

    trans_price_sl = getSellLimitTransPrice(list)
    trans_price_sl_sum = round(sum(trans_price_sl), 2)
    trans_price_sl_max = round(max(trans_price_sl), 2)
    trans_price_sl_min = round(min(trans_price_sl), 2)
    trans_price_sl_mean = round(stat.mean(trans_price_sl), 2)
    trans_price_sl_median = round(stat.median(trans_price_sl), 2)
    trans_price_sl_x10 = round(np.percentile(trans_price_sl, 10), 2)
    trans_price_sl_x25 = round(np.percentile(trans_price_sl, 25), 2)
    trans_price_sl_x75 = round(np.percentile(trans_price_sl, 75), 2)
    trans_price_sl_x90 = round(np.percentile(trans_price_sl, 90), 2)

    trans_price_sm = getSellMarketTransPrice(list)
    trans_price_sm_sum = round(sum(trans_price_sm), 2)
    trans_price_sm_max = round(max(trans_price_sm), 2)
    trans_price_sm_min = round(min(trans_price_sm), 2)
    trans_price_sm_mean = round(stat.mean(trans_price_sm), 2)
    trans_price_sm_median = round(stat.median(trans_price_sm), 2)
    trans_price_sm_x10 = round(np.percentile(trans_price_sm, 10), 2)
    trans_price_sm_x25 = round(np.percentile(trans_price_sm, 25), 2)
    trans_price_sm_x75 = round(np.percentile(trans_price_sm, 75), 2)
    trans_price_sm_x90 = round(np.percentile(trans_price_sm, 90), 2)

    trans_price_bm = getBuyMarketTransPrice(list)
    trans_price_bm_sum = round(sum(trans_price_bm), 2)
    trans_price_bm_max = round(max(trans_price_bm), 2)
    trans_price_bm_min = round(min(trans_price_bm), 2)
    trans_price_bm_mean = round(stat.mean(trans_price_bm), 2)
    trans_price_bm_median = round(stat.median(trans_price_bm), 2)
    trans_price_bm_x10 = round(np.percentile(trans_price_bm, 10), 2)
    trans_price_bm_x25 = round(np.percentile(trans_price_bm, 25), 2)
    trans_price_bm_x75 = round(np.percentile(trans_price_bm, 75), 2)
    trans_price_bm_x90 = round(np.percentile(trans_price_bm, 90), 2)
    
    trans_price_bl = getBuyLimitTransPrice(list)
    trans_price_bl_sum = round(sum(trans_price_bl), 2)
    trans_price_bl_max = round(max(trans_price_bl), 2)
    trans_price_bl_min = round(min(trans_price_bl), 2)
    trans_price_bl_mean = round(stat.mean(trans_price_bl), 2)
    trans_price_bl_median = round(stat.median(trans_price_bl), 2)
    trans_price_bl_x10 = round(np.percentile(trans_price_bl, 10), 2)
    trans_price_bl_x25 = round(np.percentile(trans_price_bl, 25), 2)
    trans_price_bl_x75 = round(np.percentile(trans_price_bl, 75), 2)
    trans_price_bl_x90 = round(np.percentile(trans_price_bl, 90), 2)

    result = [
        currency, day, 
        sell_count, buy_count, market_count, limit_count, sl_count, sm_count, bl_count, bm_count, 
        price_open, price_max, price_min, price_mean, price_median, price_x10, price_x25, price_x75, price_x90,
        price_sell_max, price_sell_min, price_sell_mean, price_sell_median, price_sell_x10, price_sell_x25, price_sell_x75, price_sell_x90,
        price_buy_max, price_buy_min, price_buy_mean, price_buy_median, price_buy_x10, price_buy_x25, price_buy_x75, price_buy_x90,
        price_sm_max, price_sm_min, price_sm_mean, price_sm_median, price_sm_x10, price_sm_x25, price_sm_x75, price_sm_x90,
        price_sl_max, price_sl_min, price_sl_mean, price_sl_median, price_sl_x10, price_sl_x25, price_sl_x75, price_sl_x90,
        price_bm_max, price_bm_min, price_bm_mean, price_bm_median, price_bm_x10, price_bm_x25, price_bm_x75, price_bm_x90,
        price_bl_max, price_bl_min, price_bl_mean, price_bl_median, price_bl_x10, price_bl_x25, price_bl_x75, price_bl_x90,
        volume_sum, volume_max, volume_min, volume_mean, volume_median, volume_x10, volume_x25, volume_x75, volume_x90,
        volume_buy_sum, volume_buy_max, volume_buy_min, volume_buy_mean, volume_buy_median, volume_buy_x10, volume_buy_x25, volume_buy_x75, volume_buy_x90,
        volume_sell_sum, volume_sell_max, volume_sell_min, volume_sell_mean, volume_sell_median, volume_sell_x10, volume_sell_x25, volume_sell_x75, volume_sell_x90,
        volume_sm_sum, volume_sm_max, volume_sm_min, volume_sm_mean, volume_sm_median, volume_sm_x10, volume_sm_x25, volume_sm_x75, volume_sm_x90, 
        volume_sl_sum, volume_sl_max, volume_sl_min, volume_sl_mean, volume_sl_median, volume_sl_x10, volume_sl_x25, volume_sl_x75, volume_sl_x90,
        volume_bm_sum, volume_bm_max, volume_bm_min, volume_bm_mean, volume_bm_median, volume_bm_x10, volume_bm_x25, volume_bm_x75, volume_bm_x90, 
        volume_bl_sum, volume_bl_max, volume_bl_min, volume_bl_mean, volume_bl_median, volume_bl_x10, volume_bl_x25, volume_bl_x75, volume_bl_x90,
        trans_price_sell_sum, trans_price_sell_max, trans_price_sell_min,  trans_price_sell_mean, trans_price_sell_median, trans_price_sell_x10, trans_price_sell_x25, trans_price_sell_x75, trans_price_sell_x90,
        trans_price_buy_sum, trans_price_buy_max, trans_price_buy_min,  trans_price_buy_mean, trans_price_buy_median, trans_price_buy_x10, trans_price_buy_x25, trans_price_buy_x75, trans_price_buy_x90,
        trans_price_sm_sum, trans_price_sm_max, trans_price_sm_min,  trans_price_sm_mean, trans_price_sm_median, trans_price_sm_x10, trans_price_sm_x25, trans_price_sm_x75, trans_price_sm_x90, 
        trans_price_sl_sum, trans_price_sl_max, trans_price_sl_min,  trans_price_sl_mean, trans_price_sl_median, trans_price_sl_x10, trans_price_sl_x25, trans_price_sl_x75, trans_price_sl_x90, 
        trans_price_bm_sum, trans_price_bm_max, trans_price_bm_min,  trans_price_bm_mean, trans_price_bm_median, trans_price_bm_x10, trans_price_bm_x25, trans_price_bm_x75, trans_price_bm_x90, 
        trans_price_bl_sum, trans_price_bl_max, trans_price_bl_min,  trans_price_bl_mean, trans_price_bl_median, trans_price_bl_x10, trans_price_bl_x25, trans_price_bl_x75, trans_price_bl_x90
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
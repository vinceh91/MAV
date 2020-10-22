# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 17:06:58 2020

@author: J0545269
"""

import datetime
import numpy as np



def week_day(date_time):
    return date_time.isoweekday()

def get_month(date_time):
    return date_time.month

def get_year(date_time):
    return date_time.year

def get_day(date_time):
    return date_time.day

def create_datetime(y,m,d):
    return datetime.datetime(year = y, month = m, day = d)

def int2datetime(myintdate):
    aaaa = int(np.floor(myintdate/10000))
    mm = int(np.floor(myintdate/100) - aaaa*100)
    dd = int(myintdate - aaaa*10000 - mm*100)
    return create_datetime(aaaa,mm,dd)

def str2datetime(AAAAMMDD):
    return  datetime.datetime.strptime(AAAAMMDD, "%Y%m%d")

def datetime2str(date_time):
    return  str(date_time)[:10]

def datetime2singlestr(date_time):
    return str(date_time)[:4] + str(date_time)[5:7] + str(date_time)[8:10]

def str2timestamp(string):
    return (datetime.datetime(year = int(string[0:4]), month = int(string[4:6]), day = int(string[6:8]), hour = int(string[8:10]), minute = int(string[10:12]) ) ) 

def my_f_date(date_time):
    return str(date_time)[:2] + str(date_time)[3:5] + str(date_time[6:10])


def timestamp2str(timestamp):
    y = str(timestamp.year)
    if timestamp.month < 10:
        m = "0" + str(timestamp.month)
    else:
        m = str(timestamp.month)
    if timestamp.day < 10:
        d = "0" + str(timestamp.day)
    else:
        d = str(timestamp.day)
    if timestamp.hour < 10:
        h = "0" + str(timestamp.hour)
    else:
        h = str(timestamp.hour)
    if timestamp.minute < 10:
        minute = "0" + str(timestamp.minute)
    else:
        minute = str(timestamp.minute)
        
    return y + m + d + h + minute

def date_bison(date_time):
    return str(date_time)[4:8] + str(date_time)[2:4] + str(date_time)[:2]
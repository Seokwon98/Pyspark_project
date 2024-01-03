import os,re
import pyspark
import pandas as pd
from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
import asyncio
import multiprocessing 
import chardet
import pandas as pd
      
def ReSplit(str):
    temp_list = [str]
    temp = str.split(".")
    temp[0] = re.sub(r'[^0-9]', '',temp[0])

    temp_list.append(temp[0])
    temp_list.append(temp[1])
    return temp_list

location = ['data/traffic/cor','data/traffic/speed','data/traffic/temp/speed']
문제파일리스트 = ['2017년11월 통행속도.csv','2017년 12월 통행속도.csv']
file_name = {}

file_name['좌표'] = os.listdir(location[0])
file_name['차량속도'] = os.listdir(location[2])

file_name['좌표'] = list(map(ReSplit,file_name['좌표']))
file_name['차량속도'] = list(map(ReSplit,file_name['차량속도']))


def qq():
    filename = location[2]+"/"+"2017년 12월 통행속도.csv"
    with open(filename, 'rb') as f:
        result = chardet.detect(f.readline())  # or read() if the file is small.
        print(result['encoding'])
def test():
    df = pd.read_csv(location[2]+"/"+"2017년11월 통행속도.csv",encoding="UTF-8-SIG")
    df.to_csv(location[1]+"/"+"2017년 12월 통행속도.csv",mode="w",encoding="cp949")
    print(df)
def excel_to_csv():
    pool = multiprocessing.Pool()
    li = [i for i in file_name['차량속도']]
    print(li)
    pool.map(_sub_2_excel_to_csv,li)
def _sub_2_excel_to_csv(i):
    if i[2] == "xlsx":
        print("xlsx")
        print(location[2]+"/"+i[0])
        df = pd.read_excel(location[2]+"/"+i[0])
        temp = i[0].split(".")
        df.to_csv(location[1]+"/"+temp[0]+".csv",mode="w",encoding="cp949")
    else:
        print("csv22")
        print(location[2]+"/"+i[0])
        if i[0] in 문제파일리스트:    
            df = pd.read_csv(location[2]+"/"+i[0])
        else:
            df = pd.read_csv(location[2]+"/"+i[0],encoding="cp949")
        temp = i[0].split(".")
        df.to_csv(location[1]+"/"+temp[0]+".csv",mode="w",encoding="cp949")


if __name__ =='__main__':
    test()


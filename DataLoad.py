import os,re
from pyspark.sql import *
from pyspark.sql.functions import *

class DataLoad:

    def __init__(self,spark):
        self.spark = spark

        self.location = ['data/traffic/speed','data/weather']

        self.문제파일리스트_Column순서 = ['2017년11월 통행속도.csv','2017년 12월 통행속도.csv'] # 다른파일들과 인코딩이 달라 따로 해줘야함
        self.문제파일리스트_속도Column = ['2017년 8월 통행속도.csv']        # -> 시간에 01시 가 아닌 ~01로 표시
        self.file_name = {}
        
        self.file_name['차량속도'] = os.listdir(self.location[0])  # ->경로에 있는 파일 불러오기
        self.file_name['날씨'] = os.listdir(self.location[1])  # ->경로에 있는 파일 불러오기

        self.file_name['차량속도'] = list(map(self.ReSplit,self.file_name['차량속도']))   # -> 문자열 입력받으면 [[파일 전체명,파일명(한글X 숫자만,확장자X),확장자],...............]
        self.file_name['날씨'] = list(map(self.ReSplit,self.file_name['날씨']))   # -> 문자열 입력받으면 [[파일 전체명,파일명(한글X 숫자만,확장자X),확장자],...............]

  
    def getSpeedDataFrame(self):   #Speed 데이터 불러오기 {날짜 : DataFrame,......} 2017년 12월 통행속도.csv 일경우 날짜 ->201712  2017년 1월 통행속도.csv 일경우 -> 20171
        print("교통 데이터 로드중......")
        temp = {}
        for i in self.file_name['차량속도']:
            temp_df = self.LoadFile("교통",self.location[0],i)
            if temp_df is not None:
                temp[i[1]] = temp_df  # ex 2017년 12월 통행속도.csv 파일의 경우 {201712:데이터프레임,........}
        print("교통 데이터 로드 완료")
        return temp
    def getWeatherDataFrame(self):
        print("기상 데이터 로드중......")
        temp = []
        for i in self.file_name['날씨']:
            temp_df = self.LoadFile("날씨",self.location[1],i)
            temp.append(temp_df) 
        print("기상 데이터 로드 완료")
        return temp


    # 
    def LoadFile(self,Type,location,filename):            # 파일 Load  파라미터(타입,경로,[파일전체이름,확장자제외파일이름,확장자])  # 교통데이터일 경우 "교통", 날씨 데이터일경우 "날씨"
        temp = self.spark.read.option("inferSchema", "true").csv(location+"/"+filename[0],header=True,encoding='cp949') #파일불러오기
        if Type== "교통":
            if filename[0] in self.문제파일리스트_Column순서:  # '2017년11월 통행속도.csv','2017년 12월 통행속도.csv' 파일은 컬럼이 하나씩 뒤로 밀려있음 그 부분 처리
                temp_column1 = temp.columns[1:]
                print("문제파일 :",temp_column1)
                temp = temp.drop('24시')
                temp_column0 = temp.columns
                temp = temp.select([col(temp_column0[x]).alias(temp_column1[x]) for x in range(len(temp_column0))])
            else:
                temp = temp.drop("_c0")

            if filename[0] in self.문제파일리스트_속도Column:   # 2017년 8월 통행속도.csv 파일은 속도 컬럼에 10시 이런식이 아닌 ~10시 이런식으로 되있음 이부분 처리
                temp_column_0_7 = temp.columns[0:8]
                temp_column_8 = temp.columns[8:]
                temp_column_mod = [x.replace("~","") for x in temp_column_8]
                column_complete =  [col(temp_column_8[x]).alias(temp_column_mod[x]) for x in range(len(temp_column_8))]
                column_complete = temp_column_0_7+column_complete
                temp = temp.select(column_complete)
        return temp

    def ReSplit(self,str):          # -> 문자열 입력받으면 [파일 전체명,파일명(한글X 숫자만,확장자X),확장자]
        temp_list = [str]
        temp = str.split(".")
        temp[0] = re.sub(r'[^0-9]', '',temp[0])     # 파일명 숫자를 제외하고 다 지운다

        temp_list.append(temp[0])
        temp_list.append(temp[1])
        return temp_list

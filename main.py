from pyspark import SparkConf,SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark
from DataLoad import DataLoad
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from graphframes import *
from graphframes.lib import AggregateMessages as AM
import os
import shutil
class main:
    def __init__(self,시점,종점):
        self.시점 = 시점            # 여기서도 시점 종점을 수정 할 수 있지만 맨아래 아래에서 
        self.종점 = 종점

        self.Partition = 16         # 파티션을 얼마나 나눌지 설정       
        self.traffic_concat = ['data/traffic/concat',]
        self.savefolder_file_path = ["data/save","data/save/weather_traffic","data/save/traffic_speed","data/save/weather","data/save/clusterinf","data/save/cuterinf_sub.txt"]
        self.필요한행 = ['일자','링크아이디','시점명','종점명','거리','10시','15시']
        self.spark = (SparkSession.builder.master("local[*]").config("spark.driver.memory", "10g")
                                        .config("spark.sql.shuffle.partitions",4)
                                        .config("spark.sql.autoBroadcastJoinThreshold",-1)
                                        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12")
                                        .config("spark.driver.extraJavaOptions","-Xss64M")
                                        .config("spark.sql.autoBroadcastJoinThreshold",-1)
                                        .getOrCreate())
        self.sc = self.spark.sparkContext
        
        self.DL = DataLoad(self.spark)

        ###################### 시작 #####################################
        TempDF = self.시작()
    #####################시작(판단 부분)######################################
    def 시작(self):

        if os.path.exists(self.savefolder_file_path[1]):
            while True:
                char = input("날씨_계절 전처리(군집화까지)가 완료된 파일이 존재 합니다. \n 이 파일로 진행하려면 '1', 다시 만드시려면 '2'를(시간많이 소요) 입력해주세요 :")
                print("입력 : ",char)
                if char=='1':
                    TempDF = self.spark.read.json(self.savefolder_file_path[1])
                    break
                elif char=='2':
                    TempDF = self.시작_Sub_1번다시()  
                    break
                else:
                    print("잘못된 입력입니다. 다시 입력해주세요")
        else:
            print("날씨_계절 전처리가 완료된 파일이 존재하지 않습니다. 다시 생성... 상단시간 소요....")
            TempDF = self.시작_Sub_1번다시()
        print('\n')
        print("###################로딩 완료#####################")
        print('\n')
        self.군집정보()
        while True:
            pre = input("군집 분류 번호를 입력해주세요(0~{0}) : ".format(self.군집수))
            num = input("시간을 선택해주세요 10시: 1 , 15시 : 2 : ")
            if((int(pre) >= 0 and int(pre) <= self.군집수) and (num == '1' or num =='2')):
                break
            else:
                print("잘못입력 하셨습니다.")
                
        time = "10" if num=="1" else "15"
        print("{0}시 시점 :{1} 부터 종점 : {2} 까지 군집 분류번호 {3}의 탐색을 시작합니다.".format(time,self.시점,self.종점,pre))
        self.탐색(TempDF,self.시점,self.종점,time,pre)

        return TempDF
    def 시작_Sub_1번다시(self):
        w_char = None
        t_char = None
        if os.path.exists(self.savefolder_file_path[2]):
            while True:
                t_char = input("교통속도 전처리가 완료된 파일이 존재 합니다. \n 이 파일로 진행하려면 '1', 다시 만드시려면 '2'를(시간많이 걸림) 입력해주세요 :")
                print("입력 : ",t_char)
                if t_char=='1' or t_char=='2':
                    break
                else:
                    print("잘못입력 하셨습니다.")
        else:
            print("교통속도 전처리가 완료된 파일이 존재하지 않습니다. 다시 생성... 상단시간 소요")
        if os.path.exists(self.savefolder_file_path[3]):
            while True:
                w_char = input("날씨 전처리가 완료된 파일이 존재 합니다. \n 이 파일로 진행하려면 '1', 다시 만드시려면 '2'를(시간많이 걸림) 입력해주세요 :")
                print("입력 : ",w_char)
                if w_char=='1' or w_char=='2':
                    break
                else:
                    print("잘못입력 하셨습니다.")
        else:
            print("날씨 전처리가 완료된 파일이 존재하지 않습니다. 다시 생성... 상단시간 소요")
        
        ## 교통속도
        if t_char=='1':
            Temp_Traffic_Speed = self.spark.read.json(self.savefolder_file_path[2])
        elif t_char=='2' or t_char==None:
            Temp_Traffic_Speed = self.교통_전처리()
        ## 날씨
        if w_char=='1':
            Temp_Weather = self.spark.read.json(self.savefolder_file_path[3])
        elif w_char=='2' or w_char==None:
            while True:
                print("#"*30)
                print("#"*30)
                print("기상 전처리 타입 '1' , '2' 와 군집 수 1 ~ 25(이하권장)입력해주세요")
                Type = input("타입 : ")
                clusters_num = int(input("클러스터 수 :"))
                if (Type=='1' or Type=='2') and (clusters_num >=1):
                    Temp_Weather = self.기상_전처리(Type,clusters_num)
                    break
                else:
                    print("잘못된 입력입니다 다시 입력해주세요!")



        return self.기상_교통데이터연결(Temp_Weather,Temp_Traffic_Speed)
    def 군집정보(self):         # 클러스팅한후 저장한 관련 정보 파일 불러옴(실루엣,컬럼명,타입 등등)
        temp = self.spark.read.json(self.savefolder_file_path[4])
        #######
        cl_sub_inf = open(self.savefolder_file_path[5],"r")
        self.실루엣 = cl_sub_inf.readline()
        self.컬럼명 = cl_sub_inf.readline()
        self.Type = cl_sub_inf.readline()
        cl_sub_inf.close()
        self.컬럼명 = self.컬럼명.split(",")
        self.컬럼명[-1] = self.컬럼명[-1].replace("\n","")
        print("이 날씨전처리파일은 Type : {0} 입니다.".format(self.Type))
        temp = temp.select(self.컬럼명)

        ###########
        self.군집수 = temp.sort(desc("prediction")).first().prediction
        while True:
            char = input("탐색을 진행하기전 군집정보를 확인 하려면 '1' 아니면 '2' :")
            print("입력 :",char)
            
            if char=='1':
                print("실루엣 : {0} 군집 수 : {1}입니다\n".format(self.실루엣,self.군집수))
                if self.Type=="2":
                    temp.groupBy(["prediction"]).agg(mean('기온(°C)'), mean('강수량(mm)'),mean("적설(cm)"),count('prediction')).sort(asc("prediction")).show(int(self.군집수)+1)
                elif self.Type=="1":
                    temp.groupBy(["prediction"]).mean().drop("avg(prediction)").show(int(self.군집수)+1)
                return
            elif char=="2":
                return
            else:
                print("잘못된 입력입니다.")

    def 탐색(self,Data : DataFrame,시점,종점,time,pre): # 최단 경로 검색을 위한 작업
        Temp = Data.filter((col("예측")==pre) & (col("시간")==time)).groupBy(["링크아이디","시점명","종점명"]).agg(mean("걸리는시간").alias("걸리는시간")).cache()
        # ↑ 입력받은 pre(군집 분류 수치)와 타임(10 or 15)을 기반으로 필터링한뒤 ["링크아이디","시점명","종점명"] 로 묶어 걸리는 시간의 평균을 구한다
        Temp.show()
        print("다익스트라 시작")
        ############# 노드 엣지 분류 작업
        node = Temp.select([col("시점명").alias("id")]).dropDuplicates(["id"]).repartition(self.Partition).cache()
        edge = Temp.select([col("시점명").alias("src"),col("종점명").alias("dst"),col("걸리는시간").alias("cost")]).repartition(self.Partition).cache()
        ############

        node.count()
        edge.count()
        Data.unpersist()

        g = GraphFrame(node,edge)
        self.다익스트라(g, 시점,종점)
    #####################시작(판단 부분)END######################################

    ################################################################################
    ##############################전처리 파트########################################
    ################################################################################

    ################기상###############################################################
    def 기상_전처리(self,Type,cluster_num):

        self.data_weather_list =self.DL.getWeatherDataFrame() # 초기 기상 데이터를 불러옴
        self.기상합친데이터 = self.데이터프레임_합치기(self.data_weather_list)  # self.data_weather_list 은 데이터프레임들의 리스트 이므로 합치기
        self.기상합친데이터 = self.기상합친데이터.withColumn("일시",col("일시").cast("string")).cache() #일시 컬럼 타입을 string으로 바꿈
        self.기상합친데이터.count()
        temp = self.군집화(self.기상합친데이터,Type,cluster_num)

        self.데이터저장(self.savefolder_file_path[3],temp)      # 전처리(군집화포함) 완료된 파일을 저장

        self.data_weather_list = None
        return temp

    def 군집화(self,weather_df : DataFrame,Type:str,cluster_num):
        if Type=='1':
            columns_to_drop = ["지점", "지점명", "기온 QC플래그", "강수량 QC플래그", "풍속 QC플래그", "풍향 QC플래그", "습도 QC플래그", "현지기압(hPa)","현지기압 QC플래그", "해면기압 QC플래그", "일조 QC플래그", "일사 QC플래그", "3시간신적설(cm)",
                    "중하층운량(10분위)", "운형(운형약어)", "최저운고(100m )", "지면상태(지면상태코드)", "현상번호(국내식)", "지면온도 QC플래그", "10cm 지중온도(°C)", "20cm 지중온도(°C)", "30cm 지중온도(°C)", "일조(hr)"]
            new_weather_df = weather_df.drop(*columns_to_drop)
            new_weather_df.select([count(when(isnull(c), c)).alias(c) for c in new_weather_df.columns]).show() # 결측치 개수 확인
            assembler = VectorAssembler(inputCols=['기온(°C)',
                                        '강수량(mm)',
                                        '풍속(m/s)',
                                        '풍향(16방위)',
                                        '습도(%)',
                                        '증기압(hPa)',
                                        '이슬점온도(°C)',
                                        '해면기압(hPa)',
                                        '일사(MJ/m2)',
                                        '적설(cm)',
                                        '전운량(10분위)',
                                        '시정(10m)',
                                        '지면온도(°C)',
                                        '5cm 지중온도(°C)',
                                        'new전운량'
                                        ], outputCol = 'features')
            df = new_weather_df
            df = df.na.fill(-1, subset=["강수량(mm)"])  #강수량
            df = df.na.fill(-1, subset=["적설(cm)"])  #적설
            df = df.na.fill(0, subset=["일사(MJ/m2)"])  #일사

            ################# 결측치 존재시 행 삭제################################
            df = df.na.drop(subset=["기온(°C)"])
            df = df.na.drop(subset=["풍속(m/s)"])
            df = df.na.drop(subset=["풍향(16방위)"])
            df = df.na.drop(subset=["습도(%)"])
            df = df.na.drop(subset=["증기압(hPa)"])
            df = df.na.drop(subset=["이슬점온도(°C)"])
            df = df.na.drop(subset=["시정(10m)"])
            df = df.na.drop(subset=["지면온도(°C)"])
            df = df.na.drop(subset=["5cm 지중온도(°C)"])
            df = df.na.drop(subset=["해면기압(hPa)"])
            ########################################################################       

            
            window_last = Window.orderBy("일시")                        # 바로 전에 있는 행으로 대체하기 위해
            df2 = df.withColumn("new전운량", last("전운량(10분위)", ignorenulls=True).over(window_last))                
            df2.select([count(when(isnull(c), c)).alias(c) for c in df2.columns]).show() 
            df2 = df2.na.drop(subset = ["전운량(10분위)"])
            df2 = assembler.transform(df2)

            km = KMeans().setK(cluster_num)
            model = km.fit(df2)
            predictions = model.transform(df2)
            ev = ClusteringEvaluator()
            실루엣 = ev.evaluate(predictions)
            self.데이터저장(self.savefolder_file_path[4],predictions)
            self.날씨군집_Sub데이터저장(실루엣,predictions.columns,Type)

            return predictions
        elif Type=='2':
            선택할_컬럼 = ["일시","기온(°C)","강수량(mm)","적설(cm)"]
            df = weather_df.select(선택할_컬럼).filter(col(선택할_컬럼[1]).isNotNull()).fillna(0)       ## 기온에서  결측치가 나올경우 그행을 제외한다.
            ab = VectorAssembler(inputCols=선택할_컬럼[1:],outputCol='features')
            ab_df = ab.transform(df)

            ################KMeans 학습및 pre 출력#########################################
            km = KMeans().setK(cluster_num)
            model = km.fit(ab_df)
            predictions = model.transform(ab_df)

            ############실루엣 평가######################
            ev = ClusteringEvaluator()
            cl_inf_df = predictions.drop("features")
            실루엣 = ev.evaluate(predictions)


            self.데이터저장(self.savefolder_file_path[4],cl_inf_df)             #다음 실행때 빠르게 불러오기 위한 저장
            self.날씨군집_Sub데이터저장(실루엣,cl_inf_df.columns,Type)           #다음 실행때 빠르게 불러오기 위한 저장(실루엣,컬럼명,타입등)
            return predictions
    ######################기상 전처리 종료##########################

    #####################교통 전처리 시작#########################
    def 교통_전처리(self):

        self.data_speed_dic = self.DL.getSpeedDataFrame()  # <---- 전체 교통 속도데이터 불러오기  
        data_speed_latest = self.data_speed_dic['202112']   # 가장 최근 파일
        data_speed_latest_linkid_start_end = data_speed_latest.select(data_speed_latest.columns[3:6]) # 가장 최근 파일 202112월자에서 링크아이디,시점,종점 column 추출한 후 선택한 데이터 프레임
        data_speed_latest_linkid_start_end = data_speed_latest_linkid_start_end.dropDuplicates(["링크아이디"]) # 링크아이디 기준으로 중복되는 row 제거 
        temp_column_value = data_speed_latest_linkid_start_end.select("링크아이디","시점명","종점명").collect()
        self.temp_column_value_total_dic = {x.링크아이디 : [x.시점명,x.종점명] for x in temp_column_value}      # data_speed_latest_linkid_start_end에서  {링크아이디:[시점명,종점명],....}으로
        temp_column_value = [x.링크아이디 for x in temp_column_value]                                          # 202112월자 데이터 프레임에서 링크아이디만 리스트로 만듬
        data_speed_list_수정 = list(map(self.A_Map,[[x,y] for x,y in self.data_speed_dic.items()]))           # data_speed_dic 전체 교통 속도 데이터(사전형) 에서 각 데이터프레임 
                                                                                                            # ['일자','링크아이디','시점명','종점명','거리','10시','15시'] 만 선택한후 리스트로 반환[수정된데이터프레임1,수정된데이터프레임1,.....]                                                                                  
        temp = self.데이터프레임_합치기(data_speed_list_수정).cache().coalesce(self.Partition)
        temp.count()
        ###########################################################
        temp_dataframe_linkid없는거제거 = temp.filter(col("링크아이디").isin(temp_column_value))  # <- 202112파일 기준으로 없는 링크아이디의 row를 제거

        ################## 시점명, 종정명 202112기준으로 수정
        li = self.temp_column_value_total_dic
        시점명_udf = udf(lambda x : li[x][0],StringType())
        종점명_udf = udf(lambda x : li[x][1],StringType())
        temp_dataframe_202112기준으로_시점종점  = temp_dataframe_linkid없는거제거.withColumn("시점명",시점명_udf(col("링크아이디"))).withColumn("종점명",종점명_udf(col("링크아이디"))).cache()
        temp_dataframe_202112기준으로_시점종점.count()
        ####################################################


        ############################### 10시 15시 데이터프레임 나누고 속도 계산 및 기상 데이터와 연결을 위해 일시 추가 ex 2021-01-01 10:00
        일자시간_udf = udf(lambda x,y: str(x)+"_"+y,StringType())
        _udf = udf(lambda x: str(x),StringType())
        #10시 데이터프레임
        tmep_dataframe_10시 = (temp_dataframe_202112기준으로_시점종점.select(["일자","링크아이디","시점명","종점명","거리",col("10시").alias("속도")])
                            .withColumn("시간",lit("10"))
                            .withColumn("걸리는시간",(col("거리")/(col("속도")/3.6)))
                            .withColumn("temp",일자시간_udf(col("일자"),col("시간")))
                            .withColumn("일시",to_timestamp("temp","yyyyMMdd_HH").cast("string")).drop("temp"))
                 
        #print("tmep_dataframe_10시파티션 수 :",tmep_dataframe_10시.rdd.getNumPartitions())
        #15시 데이터 프레임
        tmep_dataframe_15시 = (temp_dataframe_202112기준으로_시점종점.select(["일자","링크아이디","시점명","종점명","거리",col("15시").alias("속도")])
                            .withColumn("시간",lit("15"))
                            .withColumn("걸리는시간",(col("거리")/(col("속도")/3.6)))
                            .withColumn("temp",일자시간_udf(col("일자"),col("시간")))
                            .withColumn("일시",to_timestamp("temp","yyyyMMdd_HH").cast("string")).drop("temp"))
                            
        #print("tmep_dataframe_15시파티션 수 :",tmep_dataframe_15시.rdd.getNumPartitions())

        temp_dataframe_10_15시 =  self.데이터프레임_합치기([tmep_dataframe_10시,tmep_dataframe_15시]).filter(col("걸리는시간").isNotNull()).coalesce(self.Partition).cache()
        temp_dataframe_10_15시.count()

        self.데이터저장(self.savefolder_file_path[2],temp_dataframe_10_15시)        #전처리가 완료된 교통데이터 파일을 저장

        temp_dataframe_202112기준으로_시점종점.unpersist()
        temp.unpersist()

        self.data_speed_dic = None
        return temp_dataframe_10_15시

    #####################교통 전처리 종료   #########################

    ########################날씨_교통 전처리 ###################################
    def 기상_교통데이터연결(self,weather_pre :DataFrame,traffic_speed : DataFrame):
        DF_교통속도데이터_일시_Column = traffic_speed.select("일시").dropDuplicates(["일시"]).collect()
        교통속도데이터_일시_Column = [x.일시 for x in DF_교통속도데이터_일시_Column]
        기상데이터_일자_예측 =  weather_pre.select(["일시","prediction"]).filter(col("일시").isin(교통속도데이터_일시_Column)).collect()
        일자시간별_날씨예측값 = {x.일시 : x.prediction  for x in 기상데이터_일자_예측}
        Temp_UDF = udf(lambda x : 일자시간별_날씨예측값.get(x,-1),IntegerType())

        날씨예측_교통속도합치기 = traffic_speed.withColumn("예측", Temp_UDF(col("일시"))).cache() # 일자시간별_날씨예측값에 값이 없으면 예측값 -1로 변경
                                                                                                # 즉 해당 날자시간에 예측된 날씨 값이 없는 경우
        날씨예측_교통속도합치기.count()
        traffic_speed.unpersist()
        self.데이터저장(self.savefolder_file_path[1],날씨예측_교통속도합치기)   # 기상+교통 전처리가 완료된 파일을 저장
        return 날씨예측_교통속도합치기

########################################기타 데이터 저장 및 합치기 관련 함수################################################
    def 데이터저장(self,location,data:DataFrame):                   #파라미터로 받은 데이터프레임 JSON형식으로 저장
        print("데이터저장중 :",location)
        if os.path.exists(location):                                #만약 폴더가 있으면 그 폴더 지워버리고 저장
            shutil.rmtree(location)
        data.coalesce(self.Partition).write.json(location)
        print("데이터 저장완료 :",location)
    def 날씨군집_Sub데이터저장(self,sil,columnarray : List,i):      ## 실루엣과 리스트열(순서 맞추기위해),타입 텍스트로 저장
        file = open(self.savefolder_file_path[5], "w")
        file.write(str(sil)+"\n")
        text = ",".join(columnarray)+"\n"     
        file.write(text)
        file.write(i)                                           #군집화타입
        file.close()

    def 데이터프레임_합치기(self,DataframeList):
        temp = None
        ############  리스트에서 각 데이터프레임을 하나의 데이터 프레임으로 합침
        for i in range(len(DataframeList)):
            if i==0:
                temp = DataframeList[0]
            else:
                temp = temp.union(DataframeList[i])
        return temp

########################################################################################
    def A_Map(self,df): # data_speed_list_수정 변수를 만들때 Map 함수에 이용하려 만든 함수 
        temp = df[1].select(self.필요한행)      # self.필요한행 에만 있는 column만 선택한다
        return temp
    ############## 최단시간 구하기#########################
    def 다익스트라(self,g,src,dst):
        # 사용자 지점함수 설정
        path_udf = udf(lambda path, id : path + [id], ArrayType(StringType()))
        if g.vertices.filter(g.vertices.id == dst).count() == 0:        #### 목적지가 존재 하는지 확인
            print("!"*30)
            print("도착 지점이 존재하지 않습니다")
            print("!"*30)
            return 
        
        graph = (g.vertices.withColumn("visited", lit(False))
                .withColumn("TotalTime", when(g.vertices["id"] == src, 0).otherwise(float("inf")))      #초기 셋팅 출받질의 TotalTime은 0으로 하고 나머지는 무한대
                .withColumn("path", array()))                                                           # 경로 삽입을 위한 작업
        cached_vertices = AM.getCachedDataFrame(graph)
        graph = GraphFrame(cached_vertices, g.edges)
        graph_v = graph.vertices.cache()
        graph_v.count()
        cached_vertices.unpersist()
        Nav_node_list = []
        while graph_v.filter(col("visited")==False).first():                        # 전부 방문 했는지 확인
            te = graph_v.select("id","visited","TotalTime").filter(col("visited") == False).sort("TotalTime")           #아이디,방문,토탈시간 컬럼 선택후 방문하지 않은 행을                                                                                                      #TotalTime으로 내림차순
            node_id = te.first().id                                                                                     # 아이디 선택
            Nav_node_list.append(node_id)
            print("현재 노드리스트 :",Nav_node_list)
            m_TotalTime = AM.src['TotalTime']+ AM.edge["cost"]                                                          # vertext의 총걸린 시간과 간선의 비용(시간)
            m_path = path_udf(AM.src["path"], AM.src["id"])
            m_for_dst = when(AM.src['id'] == node_id, struct(m_TotalTime, m_path))                                      #도착지에게 메세지
            agm = graph.aggregateMessages(min(AM.msg).alias("msg"),sendToDst=m_for_dst)
            check_time = (agm["msg"].isNotNull() & (agm.msg['col1']< graph_v.TotalTime))                                #기존 시간와 업데이트된 시간 어떤게 작은지 확인
            visited_column = when(graph_v.visited | (graph_v.id == node_id), True).otherwise(False)                     #새로 업데이트된 시간이 작으면 그걸로교체 아니면 원래대로
            TotalTime_column = when(check_time,agm.msg["col1"]).otherwise(graph_v.TotalTime)                            #새로업데이트된 시간이 작으면 업데이트된 경로 넣고 아니면 그대로
            path_column = when(check_time,agm.msg["col2"].cast("array<string>")).otherwise(graph_v.path)
            re_df = (graph_v.join(agm, on="id", how="left_outer")                                                       #agg 에서 받은 메세지를 graph_v dataframe과 합함
                            .drop(agm["id"])
                            .withColumn("n_TotalTime", TotalTime_column)
                            .withColumn("n_Path", path_column)
                            .withColumn("visited", visited_column)
                            .drop("msg", "TotalTime", "path")
                            .withColumnRenamed('n_TotalTime', 'TotalTime')
                            .withColumnRenamed('n_Path', 'path'))

            if graph_v.filter(col("id") == dst).first().visited:                                           # 목적지에 도착했을 경우
                temp = re_df.select(['id',"path","TotalTime"]).filter(col("id") == dst)                    # 'id',"path","TotalTime" 선택후 id==dst 데이터프레임 반환
                li = temp.collect()[0][1]     # 기존에 왔던 경로들을 추출한다.
                Total = temp.collect()[0][2] 
                li.append(dst)
                print("-"*50)
                print("-"*50)
                print("시작 : {0} | 도착 : {1}".format(src,dst))
                print("경로 :",li)
                print("총 걸리 시간 : ",Total)
                print("-"*50)
                print("-"*50)
                return      #함수빠져나감
            re_graph = AM.getCachedDataFrame(re_df)
            graph = GraphFrame(re_graph, graph.edges)
            graph_v = graph.vertices.cache()
            graph_v.count()
            #new_vertices.unpersist(True)
            #cached_new_vertices.unpersist(True)
        print("!"*50)
        print("!"*50)
        print("경로가 존재하지 않습니다!!")
        print("!"*50)
        print("!"*50)

if __name__ == '__main__':


    main("금호사거리","동작역")       ########### main 시작 부분 순서대로 시점,종점 입력



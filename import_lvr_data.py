import zipfile
import pandas as pd
import pymysql
import os
import re
import shutil
from datetime import datetime
import sys

sys.getdefaultencoding() 

#判斷null
def replaceNan(word):
    if pd.isnull(word):
        return None
    else:
        return word
    
#民國年轉西元年   
def dateProcess(dateObj):
    if replaceNan(dateObj):
        try:
            #避免資料為float
            dateObj = int(dateObj)
            date_str = str(replaceNan(dateObj)+19110000)
            date_object = datetime.strptime(date_str, '%Y%m%d').date()
            return date_object
        except Exception as e:
            print(e)
            return None
        

def get_conn():
    sql_conn =  pymysql.connect(host='localhost',
                                 port=3306,
                                 user='root',
                                 password='1qaz2wsx',
                                 db='crawler',
                                 charset='utf8')
    return sql_conn
    
#股票代號名稱對應
def get_ouid_data():
    try :
        sql_conn = get_conn()
        sql_ouid_data = " SELECT 代碼, 縣市別 FROM 身分證統一編號英文對應表 "
        ouid_data = pd.read_sql(sql_ouid_data, sql_conn)    
    finally:
        sql_conn.close() 
    return ouid_data

#寫入實價登錄資料
def insert_lvr_date(data, city_name):
    sql_conn = get_conn()
    try:
        for index,row in data.iterrows():
            try :
                cursor = sql_conn.cursor()
                cursor.execute('INSERT INTO crawler.實價登陸主檔 (`編號`,`縣市別`,`鄉鎮市區`,`交易標的`,`土地區段位置建物區段門牌`'\
                               ',`土地移轉總面積平方公尺`,`都市土地使用分區`,`非都市土地使用分區`,`非都市土地使用編定`,`交易年月日`'\
                               ',`交易筆棟數`,`移轉層次`,`總樓層數`,`建物型態`,`主要用途`,`主要建材`,`建築完成年月`'\
                               ',`建物移轉總面積平方公尺`,`建物現況格局-房`,`建物現況格局-廳`,`建物現況格局-衛`,`建物現況格局-隔間`'\
                               ',`有無管理組織`,`總價元`,`單價元平方公尺`,`車位類別`,`車位移轉總面積平方公尺`,`車位總價元`,`備註`)'\
                               'VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                                ,( replaceNan(row['編號'][0]), city_name,replaceNan(row['鄉鎮市區'][0]),replaceNan(row['交易標的'][0])
                                ,replaceNan(row['土地區段位置建物區段門牌'][0]),replaceNan(row['土地移轉總面積平方公尺'][0])
                                ,replaceNan(row['都市土地使用分區'][0]),replaceNan(row['非都市土地使用分區'][0])
                                ,replaceNan(row['非都市土地使用編定'][0])
                                ,dateProcess(row['交易年月日'][0]),replaceNan(row['交易筆棟數'][0]),replaceNan(row['移轉層次'][0])
                                ,replaceNan(row['總樓層數'][0]),replaceNan(row['建物型態'][0]),replaceNan(row['主要用途'][0])
                                ,replaceNan(row['主要建材'][0]),dateProcess(row['建築完成年月'][0])
                                ,replaceNan(row['建物移轉總面積平方公尺'][0]),replaceNan(row['建物現況格局-房'][0])
                                ,replaceNan(row['建物現況格局-廳'][0]),replaceNan(row['建物現況格局-衛'][0])
                                ,replaceNan(row['建物現況格局-隔間'][0]),replaceNan(row['有無管理組織'][0])
                                ,replaceNan(row['總價元'][0]),replaceNan(row['單價元平方公尺'][0])
                                ,replaceNan(row['車位類別'][0]),replaceNan(row['車位移轉總面積平方公尺'][0])
                                ,replaceNan(row['車位總價元'][0]),replaceNan(row['備註'][0])
                                 )
                              )
                sql_conn.commit()
            except Exception as e:
                try:
                    param = str(replaceNan(row['編號'][0]))+ ' | ' +city_name+ ' | ' +str(replaceNan(row['鄉鎮市區'][0]))+ ' | ' +str(replaceNan(row['交易標的'][0])) \
                			+ ' | ' +str(replaceNan(row['土地區段位置建物區段門牌'][0]))+ ' | ' +str(replaceNan(row['土地移轉總面積平方公尺'][0])) \
                			+ ' | ' +str(replaceNan(row['都市土地使用分區'][0]))+ ' | ' +str(replaceNan(row['非都市土地使用分區'][0])) \
                			+ ' | ' +str(replaceNan(row['非都市土地使用編定'][0])) + ' | ' +str(dateProcess(row['交易年月日'][0])) \
                            + ' | ' +str(replaceNan(row['交易筆棟數'][0]))+ ' | ' +str(replaceNan(row['移轉層次'][0])) \
                            + ' | ' +str(replaceNan(row['總樓層數'][0]))+ ' | ' +str(replaceNan(row['建物型態'][0]))+ ' | ' +str(replaceNan(row['主要用途'][0])) \
                			+ ' | ' +str(replaceNan(row['主要建材'][0]))+ ' | ' +str(dateProcess(row['建築完成年月'][0])) \
                			+ ' | ' +str(replaceNan(row['建物移轉總面積平方公尺'][0]))+ ' | ' +str(replaceNan(row['建物現況格局-房'][0])) \
                			+ ' | ' +str(replaceNan(row['建物現況格局-廳'][0]))+ ' | ' +str(replaceNan(row['建物現況格局-衛'][0])) \
                			+ ' | ' +str(replaceNan(row['建物現況格局-隔間'][0]))+ ' | ' +str(replaceNan(row['有無管理組織'][0])) \
                			+ ' | ' +str(replaceNan(row['總價元'][0]))+ ' | ' +str(replaceNan(row['單價元平方公尺'][0])) \
                			+ ' | ' +str(replaceNan(row['車位類別'][0]))+ ' | ' +str(replaceNan(row['車位移轉總面積平方公尺'][0])) \
                			+ ' | ' +str(replaceNan(row['車位總價元'][0]))+ ' | ' +str(replaceNan(row['備註'][0]))
                except Exception as e:
                    print(e)
                    print('param error')
                    
                print(e)   
                print(' param : ' + param)
                print('insert error')   
    finally:
        sql_conn.close()
    
#解壓縮檔案
def unzip(obj_path):
    with zipfile.ZipFile(obj_path, 'r') as zip_file:
        for file in zip_file.namelist():
            match = re.search(pattern, file)
            #符合規格檔案，才解壓縮
            if match:
                zip_file.extract(file, tempPath)

#建立temp檔案                
def mkdir_Temp(tempPath):
    #若資料夾存在，刪除資料夾
    if(os.path.isdir(tempPath)):
        shutil.rmtree(tempPath)
    #建立資料夾
    os.mkdir(tempPath)
    
#逐筆檔案寫入
def insert_db(csvDir):
    for file in csvDir:
        try:
            data = pd.read_csv(tempPath+file,header = [0,1])
            city_name = ouid_data.loc[ouid_data['代碼'] == file[:1].upper(), '縣市別'].values[0]
            #寫入資料
            insert_lvr_date(data, city_name)
        except Exception as e:
            print(e)
            #讀取有問題檔案，移動至對應目錄
            shutil.move(tempPath+file,problemPath+file)

#start----------------------------------------------------------------------------------------------------------------------------
#設定參數
path = 'D://test//lvr_data//zip//'
tempPath = 'D://test//lvr_data//zip//temp//'
problemPath = 'D://test//lvr_data//problem//'
#正規表示法
pattern = r"^\D{1}_lvr_land_a.csv$"
#身分證第一碼資料
ouid_data = get_ouid_data()

#資料路徑
fileDir = os.listdir(path) 
for obj in fileDir:
    print('壓縮檔 : ' + obj)
    #檔案是否存在
    if(os.path.isfile(path+obj)):
        #1.建立temp檔案                
        mkdir_Temp(tempPath)
        #2.解壓縮檔案
        unzip(path+obj)
        #3.所有解壓縮檔案
        csvDir = os.listdir(tempPath) 
        #4.逐筆檔案寫入
        insert_db(csvDir)
        #5.刪除檔案
        os.remove(path+obj)
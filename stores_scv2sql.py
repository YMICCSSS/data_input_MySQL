from sqlalchemy import create_engine
import pandas as pd
import pymysql

#初始化資料庫連線，使用pymysql模組
#MySQL的使用者：tibame, 密碼:tibame2020, 埠：3306,資料庫：Store_db
engine = create_engine('mysql+pymysql://tibame:tibame2020@34.66.10.69:3306/Store_db')

def translate(obj):
    trans_obj  =obj.replace("[斜槓]","/").replace("[星號]","*").replace("[問號]","?").replace("[雙引號]",'"').replace("[左角括號]",">"). \
        replace("[右角括號]","<").replace("[豎線]","|")
    return trans_obj


#店家清單路徑
df = pd.read_csv("台北市\stores.csv")
df["name"] = df["name"].apply(translate)
df = df.drop(["id"],axis=1)
# #刪除重複的欄位、删除重複項並保留第一次出现的項
df.drop_duplicates(subset=['addr', 'name'],keep="first",inplace=True)
pd.options.display.max_rows = 1000000

db = pymysql.connect("34.66.10.69","tibame","tibame2020","Store_db" )
cursor = db.cursor()
#建立一個店家名稱加上區域的名稱來判斷是否是唯一的店家，用在之後評論家上sotre_id
# 創建店家清單字典

number = "select `addr`,`name` from `stores`"
cursor.execute(number)
data = cursor.fetchall()

data_addr= [data[i][0] for i in range(len(data))]
data_name_raw= [data[i][1] for i in range(len(data))]
data_name = []
for obj in data_name_raw:
    x =obj.replace("[斜槓]", "/").replace("[星號]", "*").replace("[問號]", "?").replace("[雙引號]", '"').replace("[左角括號]", ">"). \
        replace("[右角括號]", "<").replace("[豎線]", "|")
    data_name.append(x)

series_tf =  (df["addr"].isin(data_addr)) & (df["name"].isin(data_name))

# ~ = 將True False 反轉
df = df[~series_tf]
df.to_sql('stores', engine, if_exists='append', index=False)

print("寫入成功")



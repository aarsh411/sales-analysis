import mysql.connector as msql
from mysql.connector import Error
import pandas as pd
empdata = pd.read_csv('/home/adt-project/clean-products-final.csv', index_col=False, delimiter = ',')
empdata.head()
try:
    conn = msql.connect(host='localhost', database='bigbasket', user='root', password='1234')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS product_data;')
        print('Creating table....')
# in the below line please pass the create table statement which you want #to create
        cursor.execute("CREATE TABLE product_data(Unnamed_0 varchar(30),product VARCHAR(150),category varchar(255),sub_category varchar(255),brand varchar(255),sale_price VARCHAR(30),market_price VARCHAR(30),total_sales varchar(255), total_sales_perCities varchar(255), cities varchar(50), image_url varchar(255), p_url varchar(200), typr varchar(50), rating VARCHAR(30))")
        print("Table is created....")
        #loop through the data frame
        for i,row in empdata.iterrows():
            #here %S means string values 
            sql = "INSERT INTO bigbasket.product_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            # the connection is not auto committed by default, so we must commit to save our changes
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)

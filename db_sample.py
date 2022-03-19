# coding=utf8

import mysql.connector
from mysql.connector import errorcode

try:
    cnx = mysql.connector.connect(user='james',
                                database='douban',
                                password='tiger',
                                host='127.0.0.1')

    mycursor=cnx.cursor()#cursor() method create a cursor object  
    #SELECT * FROM books_info where ISBN like '9787560575285'
    #sql = f"SELECT * FROM books_info where ISBN like '9787560575285'"
    tablename = 'books_info'

    placeholders = ", ".join(['%s'] * (18))
    columns = 'ID, Name, url, ReadStatus, Author, Release_Date, ISBN, Pages, Price, Publisher, Short_Description, Average_Rating, No_of_Ratings, Ratio_of_1star, Ratio_of_2star, Ratio_of_3star, Ratio_of_4star, Ratio_of_5star'
    sql = f"INSERT INTO {tablename} ( {columns} ) VALUES ( {placeholders} )"    

    #sql = f"SELECT * FROM {tablename} where {keyname} = '{keyval}'"
    #sql = "INSERT INTO books_info ( ID, Name, url, ReadStatus, Author, Release_Date, ISBN, Pages, Price, Publisher, Short_Description, Average_Rating, No_of_Ratings, Ratio_of_1star, Ratio_of_2star, Ratio_of_3star, Ratio_of_4star, Ratio_of_5star ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
    #sql = "INSERT INTO books_info ( ID, Name, url, ReadStatus, Author, Release_Date, ISBN, Pages, Price, Publisher, Short_Description, Average_Rating, No_of_Ratings, Ratio_of_1star, Ratio_of_2star, Ratio_of_3star, Ratio_of_4star, Ratio_of_5star ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
    val_list = [
        1, 
    '卡拉马佐夫兄弟', 
    'https://book.douban.com/subject/30428749/', 
    'reading', 
    '[俄] 费奥多尔·陀思妥耶夫斯基', 
    '2016-11', 
    '9787560575285', 
    '', 
    '82.00元', 
    '西安交通大学出版社', 
    '陀思妥耶夫斯基（Ф.М.Достоевкий，1821～1881），俄国19世纪文坛上享有世界声誉的一位小说家，他的创作具有极其 复杂、矛盾的性质。 \r 陀思妥耶夫斯基生于医生家庭，',
    '9.4', '357', '0.0%', '0.3%', '2.0%', '17.4%', '80.4%']

    #print(sql)
    mycursor.execute(sql, val_list)
    #result = mycursor.fetchall()
    #print(len(result))
    cnx.commit()
    print('Record inserted successfully...')  

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
else:
    cnx.close()
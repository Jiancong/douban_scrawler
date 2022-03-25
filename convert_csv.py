#coding=utf8
# by Jiancong Xie
# date: 2022/3/18

import pandas as pd 
import mysql.connector
from mysql.connector import errorcode

TASK_SEL='2'

if __name__ == "__main__":
    try:
        connection = mysql.connector.connect(user='james',
                                    database='douban_small',
                                    password='tiger',
                                    host='127.0.0.1')

        connection.set_charset_collation('utf8mb4', 'utf8mb4_general_ci')

        if TASK_SEL == '1':
            books_info_df = pd.read_sql("select * from books_info", connection)
            movies_info_df = pd.read_sql("select * from movies_info", connection)
            users_info_df = pd.read_sql("select * from users", connection)
            social_info_df = pd.read_sql("select * from social_network_info", connection)
            u_books_behaviours_df = pd.read_sql("select * from user_books_behaviours", connection)
            u_movies_behaviours_df = pd.read_sql("select * from user_movies_behaviours", connection)
            u_joined_df = pd.read_sql("select * from user_joined_groups", connection)

            users_info_df.to_excel('users.xlsx', index=False)
            books_info_df.to_excel('books_info.xlsx', index=False)
            movies_info_df.to_excel('movies_info.xlsx', index=False)        
            social_info_df.to_excel('social_info.xlsx', index=False)        
            u_books_behaviours_df.to_excel("user_books_behaviours.xlsx", index=False)
            u_movies_behaviours_df.to_excel("user_movies_behaviours.xlsx", index=False)
            u_joined_df.to_excel("user_joined_groups.xlsx", index=False)
        elif TASK_SEL == '2':
            topic_info_df = pd.read_sql("select * from topic_infos", connection)
            diss_df = pd.read_sql("select * from discussion_threads_statistics", connection)

            topic_info_df.to_excel('topic_info.xlsx', index=False)
            diss_df.to_excel('discussion_threads_statistics.xlsx', index=False)


    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        connection.close()
    
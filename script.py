#coding=utf8
# by Jiancong Xie
# date: 2022/3/18

import requests, re, pickle, threading
import pandas as pd 

from bs4 import BeautifulSoup as bs
from pprint import pprint
from urllib.request import Request, urlopen

import mysql.connector
from mysql.connector import errorcode

from random import randint
from time import sleep

SLEEP_MIN=10
SLEEP_MAX=30

SCRAPE_URL = "https://www.douban.com/group/694601/"

headers = {"User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}
 
cookies = pickle.load(open("assets/cookies.pkl", "rb"))
req = requests.Session() 
for cookie in cookies:
    req.cookies.set(cookie['name'], cookie['value'])

output_dir="./outputs/"

PER_PAGE_MEMBER_CNT=36
SHORT_DESC_LENGTH=1023

with open("assets/page_range.cnf", "r") as f:
    start_page= int(f.readline().strip())
    end_page= int(f.readline().strip())

print("*" * 20)
print("scrape user page from {} to {}".format(str(start_page), str(end_page)))
print("*" * 20)



def get_metadata():
    """Scrape the metadata of the group """
    
    #url = 'https://www.douban.com/group/707650/'
    url = SCRAPE_URL
    r = req.get(url, headers=headers)
    soup = bs(r.content, 'lxml')

    df = pd.DataFrame(columns=['name', 'admin', 'admin_url', 'created_time', 'group_labels', 'member_count', 'recently_joined'])

    data = {}
    print(soup)
    # -------------
    info = soup.find('div', class_='group-info-item group-loc')
    if info is not None:
        admin = info.find('a')
        admin_name = admin.text.strip()
        print("admin_name:" + admin_name)

        admin_url = admin['href']
        print("admin_url:" + admin_url)

        # to find date
        date = re.search('[\d][\d][\d][\d]-[\d][\d]-[\d][\d]', info.text.strip()).group()
        print("date:" + date)

        data['admin'] = admin_name
        data['admin_url'] = admin_url
        data['created_time'] = date

    # --------------
    label_tag = soup.find('div', class_='group-info-item group-tags')
    labels = []
    for label in label_tag.find_all('a'): # to get the labels of group
        labels.append(label.text.strip())

    print("labels:" + "/".join(labels))
    #---------------------

    member_count = soup.find('div', class_='mod side-nav').text.strip()
    member_count = re.search('(\d+)', member_count).group()  # Number of members
    print("member_count:" + member_count)
    #-----------------------

    recently_joined = []
    ul = soup.find('div', class_='member-list').ul
    for li in ul.find_all('li'):
        recently_joined.append(li.text.strip())

    data['name'] = soup.find('title').text.strip()    
    data['group_labels'] = ', '.join(labels)
    data['member_count'] = member_count
    data['recently_joined'] =  ', '.join(recently_joined)

    df = df.append(data, ignore_index=True)

    df.to_excel(output_dir+'group_metadata.xlsx', index=False)

def task1A(connection):
    # Headers for the excels file
    user_columns = ['ID', 'Name', 'Location', 'Self_Description', 'Date_Time_Joined', 'No_of_Books_Reading', 'No_of_Books_Planned_to_Read', 'No_of_Books_Read', 'No_of_Booklist', 'No_of_Movies_Plan_to_Watch', 'No_of_Movies_Watched', 'No_of_Movielist', 'No_of_Followers', 'No_of_People_following', 'No_of_Movies_Reviewed_or_Rated']
    book_columns = ['ID', 'Name', 'Author', 'Release_Date', 'ISBN', 'Short_Description', 'Pages', 'ReadStatus', 'Price', 'Publisher', 'Average_Rating', 'No_of_Ratings', 'Ratio_of_1star', 'Ratio_of_2star', 'Ratio_of_3star', 'Ratio_of_4star', 'Ratio_of_5star', 'url']
    movie_columns = ['ID', 'Name', 'Director', 'Actor', 'IMDB', 'Release_Date', 'Type', 'Average_Rating', 'No_of_Ratings', 'Short_Description', 'Ratio_of_1star', 'Ratio_of_2star', 'Ratio_of_3star', 'Ratio_of_4star', 'Ratio_of_5star']
    network_columns = ['user_id', 'user_name', 'userid_follower(seperate by :)', 'userid_following(seperate by :)']
    
    user_df = pd.DataFrame(columns=user_columns) # Containg all users
    books_info = pd.DataFrame(columns=book_columns) # Books_info dataframe for all users
    movies_info = pd.DataFrame(columns=movie_columns) # movies info dataframe for all users
    social_network_info= pd.DataFrame(columns=network_columns) # dataframe containing followers and following for all users
    
    member_id = (start_page - 1) * PER_PAGE_MEMBER_CNT + 1

    for page in range(start_page, end_page):
        print("page:", page)

        #url = f'https://www.douban.com/group/707650/members?start={page}'
        url = SCRAPE_URL + f"/members?start={page}"
        r = req.get(url, headers=headers)
        soup = bs(r.content, 'lxml')
        
        members = soup.find_all('li', class_='member-item')

        for member in members:
            print("member_id: ", member_id)
            print("member:", member)
            div = member.find('div', class_='name')
            
            name = div.a.text.strip()
            location = div.span.text.strip('()')

            link = member.find('a')['href']

            print("link:", link)

            data = {}
            data['url'] = link
            length, _ = get_data_from_db("url", link, "users", connection) 
            if length > 0:
                if 1:
                    task1_joins(link, name, connection) # joined groups info for each user
                    task1C_books(books_info, link, connection) # books_info
                    task1C_movies(movies_info, link, connection) # get movie info

                    # get followers and following
                    social_network_info = task1D(social_network_info, link, name, connection)

                    #user_df = user_df.append(data, ignore_index=True)
                    member_id += 1
                    sleep(randint(SLEEP_MIN, SLEEP_MAX))

                print("user already in db, pass....")  

                continue

            p = req.get(link, headers=headers)
            page_soup = bs(p.content, 'lxml')     

            try:
                find_desc = page_soup.find('span', {'id':'intro_display'})
                if find_desc:
                    desc = find_desc.text.strip()
                else:
                    print("Cant find intro_display, print page_soup")
                    print(page_soup)
                    desc = None
            except AttributeError:
                desc = None

            try:
                date_joined = page_soup.find('div', class_='user-info')
                if date_joined:
                    date_joined = re.search('[\d][\d][\d][\d]-[\d][\d]-[\d][\d]', date_joined.text.strip()).group()
                else:
                    print("Can't find user-info, print page_soup")
                    print(page_soup)
                    date_joined = "用户已注销"
            except AttributeError:
                date_joined = None


            data['ID'] = member_id
            data['Name'] = name
            data['Location'] = location
            data['Self_Description'] = desc
            data['Date_Time_Joined'] = date_joined

            books = page_soup.select('#book > h2 > span > a')
            books_info_col = ['No_of_Books_Reading', 'No_of_Books_Planned_to_Read',	'No_of_Books_Read',	'No_of_Booklist']

            for book, info in zip(books, books_info_col):
                book_count = re.search('(\d+)', book.text.strip()).group()
                data[info] = book_count

            movies = page_soup.select('#movie > h2 > span > a')
            movies_info_col = ['No_of_Movies_Plan_to_Watch', 'No_of_Movies_Watched', 'No_of_Movielist']

            for movie, info in zip(movies, movies_info_col):
                movie_count = re.search('(\d+)', movie.text.strip()).group()
                data[info] = movie_count
        
            # No.of.Followers	No.of.People following	No.of.Movies reviewed/rated

            try:
                find_followers = page_soup.find('p', class_='rev-link')
                if find_followers:
                    followers = find_followers.text.strip()
                    followers = re.search('(\d+)', followers).group()
                    data['No_of_Followers'] = followers
                else:
                    print("No followers found, print page_soup")
                    print(page_soup)
                    data['No_of_Followers'] = ""
            except AttributeError:
                data['No_of_Followers'] = ""
            
            try:

                following = page_soup.select_one('#friend > h2 > span').text.strip()
                following = re.search('(\d+)', following).group()
                data['No_of_People_Following'] = following  
            except AttributeError:
                data['No_of_People_Following'] = ""
                print("No people following, print page_soup")
                print(page_soup)

            try:
                reviews = page_soup.select_one('#review > h2 > span > a').text.strip()
                reviews = re.search('(\d+)', reviews).group()
                data['No_of_Movies_Reviewed_or_Rated'] = reviews 
            except AttributeError:
                data['No_of_Movies_Reviewed_or_Rated'] = None 
                print("No movies reviewd/rated, print page_soup")
                print(page_soup)

            # update user info first.
            print("data:", data)
            insert_data_to_db(data, "users", connection)
            
            task1_joins(link, data['Name'], connection) # joined groups info for each user
            task1C_books(books_info, link, connection) # books_info
            task1C_movies(movies_info, link, connection) # get movie info

            # get followers and following
            social_network_info = task1D(social_network_info, link, name, connection)

            #user_df = user_df.append(data, ignore_index=True)
            member_id += 1

            sleep(randint(SLEEP_MIN, SLEEP_MAX))
            
    
    #books_info_df = pd.read_sql("select * from books_info", connection)
    #movies_info_df = pd.read_sql("select * from movies_info", connection)
    ## Create the excel files
    #books_info_df.to_excel(output_dir + 'books_info.xlsx', index=False)
    #movies_info_df.to_excel(output_dir + 'movies_info.xlsx', index=False)
    #social_network_info.to_excel(output_dir + 'task1D.xlsx', index=False)
            
    #user_df.to_excel(output_dir+'task1A.xlsx', index=False)
    

def task1C_books(books_info, link, connection): 
    prefix_books = "https://book.douban.com/"
    # reading, plan_to_read and read are gotten through the below url respectively
    categories = ['do', 'wish', 'collect']
    status = {
            'do' : 'reading',
            'wish' : 'plan_to_read',
            'collect' : 'read'}
    
    counter = 1
    for category in categories:
        # Modify the url for the category whether do, wish or collect
        reading = link + category
        reading = reading.replace('www', 'book')

        print("task1C_books, reading link:" + reading)        
        
        try:
            r = req.get(reading, headers=headers)
        except:
            r = req.get(reading, headers=headers)            
        soup = bs(r.content, 'lxml')

        page_navigate_links = []
        # please aware this list can be empty
        paginator_obj=soup.find('div', class_='paginator')
        if paginator_obj is not None:
            a_objs = paginator_obj.find_all('a')
            for a_obj in a_objs:
                page_navigate_links.append(prefix_books + a_obj['href'])
            #page_navigate_links = [ div.find_all('a')['href'] for div in soup.find_all('div', class_='paginator')]

        # make current link at the front 
        page_navigate_links.insert(0, reading)

        # make two pass to process these.
        print("Book First Pass....")

        book_info_map = {}

        for page_navigate_link in page_navigate_links:
            try:
                r = req.get(page_navigate_link, headers=headers)
            except:
                r = req.get(page_navigate_link, headers=headers)            
            soup = bs(r.content, 'lxml')

            li_subject_items = soup.find_all('li', class_='subject-item')
        
            book_links = [ li.find('a')['href'] for li in li_subject_items ]

            #print(f"book_links:{book_links}")
            for book in book_links:
                print("book #{}: {}".format(str(counter), book))

                length, row = get_data_from_db("url", book, "books_info", connection) 
                if length > 0:
                    
                    book_info_map[book]  = (row['ISBN'], row['Name'])
                    print("this book have beed in database, pass.")
                    continue

                b = req.get(book, headers=headers)
                bsoup = bs(b.content, 'lxml')
    
                data = {}

                try:
                    name = bsoup.select_one('#wrapper > h1 > span').text.strip()    
                except AttributeError:
                    name = "name_random_" + str(randint(1,1000000))

                # Validates if the book is already in the dataframe, if yes continue
                if len(books_info.loc[books_info['url'] == book]) != 0:
                    continue
                
                data['ID'] = (books_info['ID'].max() + 1) if not books_info.empty else 1
                data['Name'] = name
                data['url'] = book

                data['ReadStatus'] = status[category]

                info = bsoup.find('div', {'id':'info'})

                try:
                    find_author = info.find('a')
                    if find_author:
                        author = find_author.text.strip()
                        data['Author'] = author
                    else:
                        data['Author'] = None
                        print("Author not found, print info.")
                        print(info)
                except AttributeError:
                    pass
                
                try:
                    info = info.text.strip()
                except AttributeError:
                    info = ''

                print("*" * 40)
                print("name:", name)
                print("info: ", info)

                # Find the date
                try: date = re.search('[\d][\d][\d][\d]-\d*', info).group() 
                except AttributeError: date = ""

                try: 
                    isbn = re.search(r'ISBN: (\d*)', info).group(1)
                except AttributeError: 
                    isbn = "isbn_random_" + str(randint(1,1000000))

                try: pages = re.search(r'页数: (\d*)', info).group(1)
                except AttributeError: pages = ""

                #try: price = re.search(r'定价: (.* \d*)', info).group(1)
                try: price = re.search(r'定价: (\d*.?\d*元)', info).group(1)
                except AttributeError: price = ""

                try: publisher = re.search(r'(.+出版社)', info).group(1)
                except AttributeError: publisher = None
    
                data['Release_Date'] = date
                data['ISBN'] = isbn
                data['Pages'] = pages
                data['Price'] = price
                data['Publisher'] = publisher

                book_info_map[book]  = (isbn, name)

                try:
                    desc = bsoup.find('div', class_='related_info').find('span', class_='all hidden').text.strip()
                    if desc and len(desc) > SHORT_DESC_LENGTH:
                        data['Short_Description'] = desc[:SHORT_DESC_LENGTH]
                    else:
                        data['Short_Description'] = desc

                except AttributeError:
                    pass
                
                ratings = bsoup.find('div', class_='rating_wrap clearbox')

                try:
                    average = ratings.find('strong', class_='ll rating_num')
                    data['Average_Rating'] = average.text.strip()

                    rating_count = ratings.find('a').text.strip()
                    rating_count = re.search('(\d*)', rating_count).group()

                    data['No_of_Ratings'] = rating_count

                    start_ratings = ratings.find_all('span', class_='rating_per')
                    start_ratings.reverse()

                    for star, percent in enumerate(start_ratings, 1):
                        data[f'Ratio_of_{star}star'] = percent.text.strip()
                except AttributeError:
                    pass
        
                #books_info = books_info.append(data, ignore_index=True)

                print("update database.")
                length, _ = get_data_from_db("ISBN", isbn, "books_info", connection) 
                if length == 0:
                    insert_data_to_db(data, "books_info", connection)
                else:
                    print("books already in db, pass....")

                print("books_link counter:" + str(counter))
                counter+=1
                sleep(randint(SLEEP_MIN,SLEEP_MAX))

        # Second Pass
        print("Book Second Pass....")
        for page_navigate_link in page_navigate_links:
            try:
                r = req.get(page_navigate_link, headers=headers)
            except:
                r = req.get(page_navigate_link, headers=headers)            
            soup = bs(r.content, 'lxml')

            li_subject_items = soup.find_all('li', class_='subject-item')

            for li_subject_item in li_subject_items:

                book_link = li_subject_item.find('a')['href']

                print(f"user_link:{link}, book_link:{book_link}")
                if book_link in book_info_map and get_data_from_db2("UserUrl", link, "BookISBN", book_info_map[book_link][0] , "user_books_behaviours", connection) > 0:
                    print("this book behaviour have beed in database, pass.")
                    continue

                data = {}
                
                data['BookName'] = book_info_map[book_link][1]
                if data['BookName'] is None:
                    data['BookName'] = 'Unknown'
                data['BookISBN'] = book_info_map[book_link][0]
                data['ReadStatus'] = category

                #div_info_item = li_subject_item.find('div', class_='info')
                ##print(f"div_info_item:{div_info_item}")
                #short_note = div_info_item.find('div', class_='short-note')
                ##print(f"short_note_item:{short_note}")

                #span_objs = short_note.find_all('span')

                #rating = ""
                #comment = ""
                #for span_obj in span_objs:
                #    class_value = span_obj['class']
                #    if 'rating' in class_value:
                #        rating = class_value

                #comment = short_note.find('p', class_="comment").text.strip()
                rating_obj = li_subject_item.select_one('span[class^=rating]')
                if rating_obj is not None:
                    rating = rating_obj['class'][0]
                else:
                    rating = ""

                comment_obj = li_subject_item.find('p', class_='comment')
                if comment_obj is not None:
                    comment = comment_obj.text.strip()
                else:
                    comment = ""

                if rating == "" and comment == "" :
                    print("The book behaviour is invalid behaviours, pass.")
                    continue

                data['Review'] = comment
                data['Rating'] = rating
                data['UserUrl'] = link

                print(f"UserUrl:{link}")

                insert_data_to_db(data, "user_books_behaviours", connection)
                print("Insert record successfully.")
                sleep(randint(SLEEP_MIN, SLEEP_MAX))

    return books_info

def get_data_from_db(keyname, keyvalue, tablename, conn):
    mycursor=conn.cursor() #cursor() method create a cursor object  
    sql = f"SELECT * FROM {tablename} where {keyname} = '{keyvalue}'"
    print("sql:", sql)
    mycursor.execute(sql)
    result = mycursor.fetchall()
    print("len of result:", len(result))
    if len(result) > 0:
        row = dict(zip(mycursor.column_names, result[0]))
        return len(result), row
    else:
        return 0, None

def get_data_from_db2(keyname1, keyvalue1, keyname2, keyvalue2, tablename, conn):
    mycursor=conn.cursor() #cursor() method create a cursor object  
    sql = f"SELECT * FROM {tablename} where {keyname1} = '{keyvalue1}' and {keyname2} = '{keyvalue2}'"
    print("sql:", sql)
    mycursor.execute(sql)
    result = mycursor.fetchall()
    print("len of result:", len(result))
    return len(result)

def insert_data_to_db(data, tablename, conn):
    mycursor=conn.cursor() #cursor() method create a cursor object  
    placeholders = ", ".join(['%s'] * (len(data)))
    columns = ', '.join(data.keys())
    #columns = 'ID, Name, url, ReadStatus, Author, Release_Date, ISBN, Pages, Price, Publisher, Short_Description, Average_Rating, No_of_Ratings, Ratio_of_1star, Ratio_of_2star, Ratio_of_3star, Ratio_of_4star, Ratio_of_5star'
    #placeholders = '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s'
    sql = f"INSERT INTO {tablename} ( {columns} ) VALUES ( {placeholders} )" 

    #print("sql:", sql)
    #print("values:", list(data.values()))

    mycursor.execute(sql, list(data.values()))
    conn.commit()
    print('Record inserted successfully...')  
    
def task1C_movies(movies_info, link, connection):
    prefix_movies = "http://movie.douban.com/"
    categories = ['do', 'wish', 'collect']
    status = {
        'do' : 'watching',
        'wish' : 'plan_to_watch',
        'collect': 'watched'
    }
    
    for category in categories:
        reading = link + category
        reading = reading.replace('www', 'movie')

        print("task1C_movies, reading link:" + reading)        
        try:
            r = req.get(reading, headers=headers)
        except:
            r = req.get(reading, headers=headers)            
        soup = bs(r.content, 'lxml')

        page_navigate_links = []
        # please aware this list can be empty
        paginator_obj=soup.find('div', class_='paginator')
        if paginator_obj is not None:
            a_objs = paginator_obj.find_all('a')
            for a_obj in a_objs:
                page_navigate_links.append(prefix_movies + a_obj['href'])

        # make current link at the front 
        page_navigate_links.insert(0, reading)

        # make two pass to process these.
        print("Movie First Pass....")

        movie_info_map = {}

        for page_navigate_link in page_navigate_links:
            #page_navigate_link = "https://movie.douban.com/people/BlaBla1416/collect"
            try:
                r = req.get(page_navigate_link, headers=headers)
            except:
                r = req.get(page_navigate_link, headers=headers)            
            soup = bs(r.content, 'lxml')
        
            movie_links = [div.find('a')['href'] for div in soup.find_all('div', class_='item')]

            counter = 1
            for movie in movie_links:
                print("movie #{}: {}".format(str(counter), movie))

                length, row = get_data_from_db("url", movie, "movies_info", connection) 
                if length > 0:
                    print("this movies have been stored. Pass......")
                    counter+=1
                    print(f"{counter}:length of movie_links:{len(movie_links)}")

                    movie_info_map[movie] = (row['IMDB'], row['Name'])
                    continue
                else:
                    pass

                m = req.get(movie, headers=headers)
                msoup = bs(m.content, 'lxml')

                try:
                    name = msoup.select_one('#content > h1').text.strip()    
                    print(f"name:{name}")

                except AttributeError:
                    print("name is not found, null")
                    continue
                
                info = msoup.find('div', {'id':'info'})

                try: 
                    director = re.search('导演: (.*)', info.text.strip()).group(1)
                    director = director[:255]
                except AttributeError: 
                    director = None

                try: 
                    actors = info.find('span', class_='actor').find('span', class_='attrs').text.strip()
                    actors = actors[:SHORT_DESC_LENGTH]
                except AttributeError: 
                    actors = None

                try: imdb = re.search('IMDb: (.*)', info.text.strip()).group(1)
                except AttributeError: 
                    imdb = "tt_random_" + str(randint(1,1000000))

                try: date = info.select_one('span[property="v:initialReleaseDate"]').text.strip()
                except AttributeError: date = None

                try: genre = info.select_one('span[property="v:genre"]').text.strip()
                except AttributeError: genre = None

                try: 
                    desc = msoup.find('div', {'id':'link-report'}).text.strip()
                    if desc and len(desc) > SHORT_DESC_LENGTH:
                        desc = desc[:SHORT_DESC_LENGTH]
                except AttributeError: 
                    desc = None

                data = {
                        'Director' : director,
                        'Actor' : actors,
                        'IMDB' : imdb,
                        'Release_Date' : date,
                        'Type': genre,
                        'Short_Description' : desc,
                        'ID' : (movies_info['ID'].max() + 1) if not movies_info.empty else 1,
                        'Name' : name,
                        'url' : movie
                        }
#                data['ID'] = (movies_info['ID'].max() + 1) if not movies_info.empty else 1
#                data['Name'] = name

                movie_info_map[movie] = (imdb, name)

                print("data:", data)

                ratings = msoup.find('div', class_='rating_wrap clearbox')    
                try: 
                    average = ratings.find('strong', class_='ll rating_num')
                    data['Average_Rating'] = average.text.strip()


                    rating_count = ratings.select_one('span[property="v:votes"]').text.strip()

                    data['No_of_Ratings'] = rating_count

                    start_ratings = ratings.find_all('span', class_='rating_per')
                    start_ratings.reverse()

                    for star, percent in enumerate(start_ratings, 1):
                        data[f'Ratio_of_{star}star'] = percent.text.strip()
                except AttributeError:
                    pass
                
                insert_data_to_db(data, "movies_info", connection)
                #movies_info = movies_info.append(data, ignore_index=True)
                #length, _ = get_data_from_db("url", movie, "movies_info", connection) 
                #if length == 0:
                #else:
                #    print("movies already in db, pass....")            

                print("movies link counter:" + str(counter))
                counter += 1
                sleep(randint(SLEEP_MIN, SLEEP_MAX))
        
        # Second Pass
        print("Movie Second Pass....")
        for page_navigate_link in page_navigate_links:
            try:
                r = req.get(page_navigate_link, headers=headers)
            except:
                r = req.get(page_navigate_link, headers=headers)            
            soup = bs(r.content, 'lxml')

            div_subject_items = soup.find_all('div', class_='item')

            for div_subject_item in div_subject_items:
                movie_link = div_subject_item.find('a')['href']
                print("movie_link:", movie_link)
                if movie_link in movie_info_map and get_data_from_db2("UserUrl", link, "MovieIMDB", movie_info_map[movie_link][0], "user_movies_behaviours", connection) > 0:
                    print("this movie behaviour have been in database, pass.")
                    continue

                if movie_link not in movie_info_map:
                    print("error happened, pass...")
                    continue
                
                data = {}
                data['MovieName'] = movie_info_map[movie_link][1]
                data['MovieIMDB'] = movie_info_map[movie_link][0]
                data['WatchStatus'] = category

                rating_obj = div_subject_item.select_one('span[class^=rating]')
                if rating_obj is not None:
                    rating = rating_obj['class'][0]
                else:
                    rating = ""

                comment_obj = div_subject_item.find('span', class_='comment')
                if comment_obj is not None:
                    comment = comment_obj.text.strip()
                else:
                    comment = ""

                if rating == "" and comment == "":
                    print("The movie behaviours is invalid, pass...")
                    continue

                data["Review"] = comment
                data["Rating"] = rating
                #span_objs = div_subject_item.find('div', class_='info').find_all('span')
                #for span_obj in span_objs:
                #    print(f"span_obj:{span_obj}")
                #    class_value = span_obj['class']
                #    if "rating" in class_value:
                #        print(f"Rating:{class_value}")
                #        data["Rating"] = class_value
                #    elif "comment" in class_value:
                #        review = span_obj.text.strip()
                #        data["Review"] = review
                #        print(f"comment: {review}")

                data['UserUrl'] = link

                insert_data_to_db(data, "user_movies_behaviours", connection)
                sleep(randint(SLEEP_MIN,SLEEP_MAX))
        
    return movies_info

def task1C_books_behaviour(books_info, link, connection):

    r = req.get(link, headers=headers)
    soup = bs(r.content, 'lxml')

    books_info['url'] = books_info['url'].apply(lambda x: x.strip())
    
    columns = ['ID', 'BookName', 'ReadStatus', 'Rating', 'Review', 'UserUrl', 'BookISBN']
    #behave_df = pd.DataFrame(columns=columns)
    
    books = soup.find_all(id='book')

    print("*"*30)
    print(f"reviews:{books}") 
    print("*"*30)

    for review in books:
        data = {}
        data['UserUrl'] = link
        #print(f"review:{review}")

        a = review.find('a', class_='subject-img')['href']
        book = books_info.loc[books_info['url'] == a.strip()]
        print(f"books_info['url']:{books_info['url']}")
        print(f"book:{book}")
        # Check if the book exists in the dataset if yes get the name and status
        if not book.empty:
            book = book.iloc[0]
            data['BookISBN'] = book['ISBN']

            if get_data_from_db2('UserUrl', link, 'BookISBN', book['ISBN'], "user_books_behaviours", connection) > 0:
                print("this book behaviours of user is recorded, pass.")
                continue

            print(f"book:{book}")

            if 'Name' not in book:
                print("Name is not present, print book.")
                print(book)
                data['Name'] = ''
            else:
                data['Name'] = book['Name']
            data['ReadStatus'] = book['ReadStatus']

            
        rating = review.select_one('span[title]')['class'][0]
        rating = re.search('[\d]', rating).group()
        
        
        content = review.find('div', class_='short-content')
        try:
            content.find('a').decompose()
            content.find('p').decompose()
        except: pass
    
        data['Rating'] = rating
        data['Review'] = content.text.strip()

        insert_data_to_db(data, "user_behaviours", connection)

        #behave_df = behave_df.append(data, ignore_index=True)
        print("behave_df data:", list(data.values()))

    #behave_df.to_excel(output_dir + f'u{member_id}_behaviours_info.xlsx', index=False)


def task1D(network_info, link, name, connection):
    """ Task 1d"""
    followers_link = link + 'contacts' # Link to followers
    following_link = link + 'rev_contacts' #Link to following

    length, _ = get_data_from_db("UserUrl", link, "social_network_info", connection) 
    if length > 0:
        print("this social network info have beed in database, pass.")
        return
    
    r = req.get(followers_link, headers=headers)
    fwsoup = bs(r.content, 'lxml')
    # Scrape followers
    followers = [dl.dd.text.strip() for dl in fwsoup.find_all('dl', class_='obu')]
    
    r = req.get(following_link, headers=headers)
    ffsoup = bs(r.content, 'lxml')
    # Scrape following
    following = [dl.dd.text.strip() for dl in ffsoup.find_all('dl', class_='obu')]

    data = {
            'UserUrl' : link,
            'userName' : name,
            'UserFollowers' : ':'.join(followers), # Adds the followers to the row
            'UserFollowing' : ':'.join(following)
            }
    
    # Appends it to the network_info dataframer for all members
    #network_info = network_info.append(data, ignore_index=True)
    insert_data_to_db(data, "social_network_info", connection)
    
    return network_info

def task1_joins(link, member_id, connection):

    len, _ = get_data_from_db("UserUrl", link, "user_joined_groups", connection)

    if len > 0:
        print("The user's grouped joined info has been processed. Pass...")
        return

    data = {}
    data["UserUrl"] = link

    link += 'joins'
    link = link.replace('/people', '/group/people')
    print("task1_joins link:", link)
    
    r = req.get(link, headers=headers)
    soup = bs(r.content, 'lxml')
    
    groups = soup.select('.group-list.group-cards li div.title')
    names_list = [group.text.strip() for group in groups]

    names = ":".join(names_list)

    data["GroupName"] = names[:4086]

    insert_data_to_db(data, "user_joined_groups", connection)
    print("User_Joined_Groups: Insert record successfully.")
    
    #df = pd.Series(names, name='group_name')
    
    #df.to_excel(output_dir + f'u{member_id}_joined_groups_info.xlsx', index=False)
    
def task2A():
    df = pd.DataFrame(columns=['TopicID', 'TopicName', 'Author', 'Comments', 'LastCommentTime'])
    
    page = 0
    while page <= 35000:
        print(page)
        #url = f'https://www.douban.com/group/707650/discussion?start={page}&type=new'
        url = SCRAPE_URL + f"/discussion?start={page}&type=new"

        print("url:", url)

        r = req.get(url, headers=headers)

        if r.status_code != 200:
            print("the request is abnormal, stop...")
            break

        print('gotten')
        soup = bs(r.content, features='lxml')
        try:
            div = soup.select_one('#content > div > div.article')
            table = div.find('table')        
            if not table :
                print("Nothing found, return.")
                return

            for row in table.find_all('tr', class_=''):
                data = {}
                
                link = row.find('a')['href']
                print("task2Alink:", link)
                topic_id = re.search('/(\d+)/', link).group(1)
                discussion_obj = re.search('/(\d+)/.+/(\d+)/', link)

                is_discussion = False
                if discussion_obj is None or discussion_obj == "":
                    data['TopicID'] = topic_id
                else:
                    topic_id =  discussion_obj.group(2)
                    data['TopicID'] = topic_id
                    is_discussion = True
                    
                data['TopicName'], data['Author'], data['Comments'], data['LastCommentTime'] = [td.text.strip() for td in row.find_all('td')]

                print(f"TopicID:{topic_id}, TopicName:{data['TopicName']}")

                length, _ = get_data_from_db("TopicID", topic_id, "discussion_threads_statistics", connection) 
                if length > 0:
                    print("this discuss thread info have been in database, pass.")
                    if is_discussion:
                        task2B_discussion(link, connection)
                    else:
                        task2B(link, connection)
                    sleep(randint(SLEEP_MIN,SLEEP_MAX))
                    continue
                else:
                    print("Task2A data:", data)
                    insert_data_to_db(data, "discussion_threads_statistics", connection)
                
                if is_discussion:
                    task2B_discussion(link, connection)
                else:
                    task2B(link, connection)
                sleep(randint(SLEEP_MIN,SLEEP_MAX))
            
            page += 25
        except AttributeError as e:
            print("error:", e)
            print('Retrying: ', page)
            continue
    
    #df.to_excel(output_dir + 'task2A.xlsx', index=False)

def task2B_discussion(link, connection):
    topic_id = re.search('/(\d+)/.+/(\d+)/', link).group(2)
    topic_columns = ['OriginalPostText', 'Replies', 'UserNameWhoReplies', 'No_of_Likes', 'DateTime', 'url']
    data = {}
    print("link2B_discussion:", link)
    r = req.get(link, headers=headers)
    if r.status_code != 200:
        print("request error: return.")
        return

    soup = bs(r.content, 'lxml')

    for replyC in soup.find_all('div', class_='comment-item'):
        try:
            data={}
            id = replyC['id']
            data['ID'] = id

            content = soup.find(id='link-report').find('p').text.strip()

            if content is None or content == "":
                content = "None"
            else:
                print("content:", content)

            length, _ = get_data_from_db('ID', id, "topic_infos", connection)
            if length > 0:
                print("discussion info has been proceed, pass.")
                continue

            reply = replyC.find("div", class_="content report-comment")
            if reply is None:
                print("reply is not found, error., return")
                return 

            data['OriginalPostText'] = content

            replies = reply.find('p').text.strip()#  Reply text
            if replies is None:
                print("replies error, return.")
                return
            else:
                data['Replies'] = replies
                print("Replies:", data['Replies'])

            authorText = reply.find("div", class_="author").find('a').text.strip()
            print("author:", authorText)
            pubTime = reply.find("div", class_="author").find('span').text.strip()
            print("pubTime:", pubTime)

            likesText = reply.find("div", class_='op-lnks').find('a', class_='comment-vote cls_abnormal').text.strip()

            print("likesText:", likesText)
            if likesText is None or likesText == "" :
                likesText = "None"

            data['UserNameWhoReplies'] = authorText
            data['No_of_Likes'] = likesText
            data['url'] = link
            data['PubTime'] = pubTime
            data['TopicID'] = topic_id

            insert_data_to_db(data, "topic_infos", connection)
            print("data:", data)
            print("insert record into topic_infos")
        except AttributeError as e:
            print("error:", e)
        
        

def task2B(link, connection):
    
    topic_id = re.search('/(\d+)/', link).group(1)

    topic_columns = ['OriginalPostText', 'Replies', 'UserNameWhoReplies', 'No_of_Likes', 'DateTime', 'url']
    df = pd.DataFrame(columns=topic_columns)
    
    data = {}
    
    print("link2B:", link)
    r = req.get(link, headers=headers)
    if r.status_code != 200:
        print("request error: return.")
        return

    soup = bs(r.content, 'lxml')
    
    # Original post text
    content = soup.find('div', class_='rich-content topic-richtext').text.strip()
    if content is None or content == "":
        content = "None"
    
    for reply in soup.find_all('li', class_='clearfix comment-item reply-item'):

        id = reply['id']
        data['ID'] = id
        print("reply.id:", id)
        if id is None or id == "":
            print("reply id is None.... return")
            return

        length, _ = get_data_from_db('ID', id, "topic_infos", connection)
        if length > 0:
            print("discussion info has been proceed, pass.")
            continue


        data['OriginalPostText'] = content

        data['Replies'] = reply.find('p', 'reply-content').text.strip()#  Reply text
        
        head = reply.find('h4')
        data['UserNameWhoReplies'] = head.a.text.strip()

        
        data['PubTime'] = head.find('span', class_='pubtime').text.strip()
        data['url'] = link
        data['TopicID'] = topic_id
        
        try:
            # Likes doenst show every time
            likes = reply.find('div', class_='operation-div').text.strip()
            likes = re.search('(\d+)', likes).group()

            print("likes: ", likes)

            if likes is None or likes == "":
                likes = "None"

            data['No_of_Likes'] = likes

        except AttributeError:
            print("Likes parse error...")
            data['No_of_Likes'] = "None"
            pass
        

        insert_data_to_db(data, "topic_infos", connection)
        print("data:", data)
        print("insert record into topic_infos")
        #df = df.append(data, ignore_index=True)
        data = {}
        # So it wont repeat post_text for other rows
        #data['OriginalPostText'] = None
        
        
    #df.to_excel(output_dir + f'task2B_{topic_id}.xlsx', index=False)
    
if __name__ == '__main__':
    try:
        connection = mysql.connector.connect(user='james',
                                    database='douban_small',
                                    password='tiger',
                                    host='127.0.0.1')

        connection.set_charset_collation('utf8mb4', 'utf8mb4_general_ci')

        #get_metadata()
        task1A(connection) 
        #task2A()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        connection.close()
    
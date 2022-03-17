import requests, re, pickle, threading
import pandas as pd 

from bs4 import BeautifulSoup as bs
from pprint import pprint
from urllib.request import Request, urlopen

headers = {"User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}
 
cookies = pickle.load(open("assets/cookies.pkl", "rb"))
req = requests.Session() 
for cookie in cookies:
    req.cookies.set(cookie['name'], cookie['value'])

output_dirs="../output/"

def get_metadata():
    """Scrape the metadata of the group """
    
    url = 'https://www.douban.com/group/707650/'
    r = req.get(url, headers=headers)
    soup = bs(r.content, 'lxml')

    df = pd.DataFrame(columns=['name', 'admin', 'admin_url', 'created_time', 'group_labels', 'members_count', 'recently_joined'])

    data = {}
    # -------------
    info = soup.find('div', class_='group-info-item group-loc')
    admin = info.find('a')

    admin_name = admin.text.strip()
    admin_url = admin['href']
    # to find date
    date = re.search('[\d][\d][\d][\d]-[\d][\d]-[\d][\d]', info.text.strip()).group()
    # --------------

    label_tag = soup.find('div', class_='group-info-item group-tags')
    labels = []
    for label in label_tag.find_all('a'): # to get the labels of group
        labels.append(label.text.strip())
    #---------------------

    member_count = soup.find('div', class_='mod side-nav').text.strip()
    member_count = re.search('(\d+)', member_count).group()  # Number of members
    #-----------------------

    recently_joined = []
    ul = soup.find('div', class_='member-list').ul
    for li in ul.find_all('li'):
        recently_joined.append(li.text.strip())

    
    data['name'] = soup.find('title').text.strip()    
    data['admin'] = admin_name
    data['admin_url'] = admin_url
    data['created_time'] = date
    data['group_labels'] = ', '.join(labels)
    data['member_count'] = member_count
    data['recently_joined'] =  ', '.join(recently_joined)

    df = df.append(data, ignore_index=True)

    df.to_excel('metadata.xlsx', index=False)


def task1A():
    # Headers for the excels file
    columns = ['user_id', 'user_name', 'location', 'self_description', 'date_time_joined', 'No.of.Books Reading', 'No.of.Books planned to read', 'No.of.Books Read', 'No.of.Book list', 'No.of.Movies planning to watch', 'No.of.Movies Watched', 'No.of.Movies list', 'No.of.Followers', 'No.of.People following', 'No.of.Movies reviewed/rated']
    book_columns = ['ID', 'Name', 'Author', 'Release Date', 'ISBN', 'Short Description', 'Pages', 'ReadStatus', 'Price', 'Publisher', 'Average Rating', 'No.of Ratings', 'ratio of 1star', 'ratio of 2star', 'ratio of 3star', 'ratio of 4star', 'ratio of 5star', 'url']
    movie_columns = ['ID', 'Name', 'Director', 'Actor', 'IMDB', 'release date', 'Type', 'Average Rating', 'No.of.Ratings', 'Short Description', 'ratio of 1star', 'ratio of 2star', 'ratio of 3star', 'ratio of 4star', 'ratio of 5star']
    network_columns = ['user_id', 'user_name', 'userid_follower(seperate by :)', 'userid_following(seperate by :)']
    
    df = pd.DataFrame(columns=columns) # Containg all users
    books_info = pd.DataFrame(columns=book_columns) # Books_info dataframe for all users
    movies_info = pd.DataFrame(columns=movie_columns) # movies info dataframe for all users
    network_info= pd.DataFrame(columns=network_columns) # dataframe containing followers and following for all users
    
    member_id = 1
    for page in range(0, 45000, 36):
        url = f'https://www.douban.com/group/707650/members?start={page}'
        r = req.get(url, headers=headers)
        soup = bs(r.content, 'lxml')
        
            
        members = soup.find_all('li', class_='member-item')
        for member in members:
            div = member.find('div', class_='name')
            
            name = div.a.text.strip()
            location = div.span.text.strip('()')

            link = member.find('a')['href']

            p = req.get(link, headers=headers)
            page_soup = bs(p.content, 'lxml')     
            

            try:
                desc = page_soup.find('span', {'id':'intro_display'}).text.strip()
            except AttributeError:
                desc = None

            try:
                date_joined = page_soup.find('div', class_='user-info')
                date_joined = re.search('[\d][\d][\d][\d]-[\d][\d]-[\d][\d]', date_joined.text.strip()).group()
            except AttributeError:
                date_joined = None

            data = {}

            data['user_id'] = member_id
            data['user_name'] = name
            data['location'] = location
            data['self_description'] = desc
            data['date_time_joined'] = date_joined

            books = page_soup.select('#book > h2 > span > a')
            books_info_col = ['No.of.Books Reading', 'No.of.Books planned to read',	'No.of.Books Read',	'No.of.Book list']

            for book, info in zip(books, books_info_col):
                book_count = re.search('(\d+)', book.text.strip()).group()
                data[info] = book_count

            movies = page_soup.select('#movie > h2 > span > a')
            movies_info_col = ['No.of.Movies planning to watch', 'No.of.Movies Watched', 'No.of.Movies list']

            for movie, info in zip(movies, movies_info_col):
                movie_count = re.search('(\d+)', movie.text.strip()).group()
                data[info] = movie_count
        
            # No.of.Followers	No.of.People following	No.of.Movies reviewed/rated

            try:
                followers = page_soup.find('p', class_='rev-link').text.strip()
                followers = re.search('(\d+)', followers).group()
                data['No.of.Followers'] = followers
            except AttributeError:
                data['No.of.Followers'] = None
            
            try:
                following = page_soup.select_one('#friend > h2 > span').text.strip()
                following = re.search('(\d+)', following).group()
                data['No.of.People following'] = following  
            except AttributeError:
                data['No.of.People following'] = None 


            try:
                reviews = page_soup.select_one('#review > h2 > span > a').text.strip()
                reviews = re.search('(\d+)', reviews).group()
                data['No.of.Movies reviewed/rated'] = reviews 
            except AttributeError:
                data['No.of.Movies reviewed/rated'] = None 

            
            
            task1_joins(link, member_id) # joined groups info for each user
            books_info = task1C_books(books_info, link) # books_info
            movies_info = task1C_movies(movies_info, link) # get movie info
            task1C_behaviour(books_info, member_id, link) # get behaviour
            
            # get followers and following
            network_info = task1D(network_info, link, member_id, name)
           
            df = df.append(data, ignore_index=True)
            member_id += 1
            
    
    # Create the excel files
    books_info.to_excel('books_info.xlsx', index=False)
    movies_info.to_excel('movies_info.xlsx', index=False)
    network_info.to_excel('task1D.xlsx', index=False)
            
       
    df.to_excel('task1A.xlsx', index=False)
    

def task1C_books(books_info, link): 
    # reading, plan_to_read and read are gotten through the below url respectively
    categories = ['do', 'wish', 'collect']
    status = {
            'do' : 'reading',
            'wish' : 'plan_to_read',
            'collect' : 'read'}
    
    for category in categories:
        # Modify the url for the category whether do, wish or collect
        reading = link + category
        reading = reading.replace('www', 'book')
        
        
        try:
            r = req.get(reading, headers=headers)
        except:
            r = req.get(reading, headers=headers)            
        soup = bs(r.content, 'lxml')
        
        
        book_links = [li.find('a')['href'] for li in soup.find_all('li', class_='subject-item')]
        for book in book_links:
            b = req.get(book, headers=headers)
            bsoup = bs(b.content, 'lxml')
    
            data = {}
            
            try:
                name = bsoup.select_one('#wrapper > h1 > span').text.strip()    
            except AttributeError:
                name = None
            
            # Validates if the book is already in the dataframe, if yes continue
            if len(books_info.loc[books_info['url'] == book]) != 0:
                continue
            
            data['ID'] = (books_info['ID'].max() + 1) if not books_info.empty else 1
            data['Name'] = name
            data['url'] = book
            
            data['ReadStatus'] = status[category]
                
            info = bsoup.find('div', {'id':'info'})
            
            try:
                author = info.find('a').text.strip()
                data['Author'] = author
            except AttributeError:
                pass
            
            try:
                info = info.text.strip()
            except AttributeError:
                info = ''
                                    
                # Find the date
            try: date = re.search('[\d][\d][\d][\d]-\d*', info).group() 
            except AttributeError: date = None
            
            try: isbn = re.search(r'ISBN: (\d*)', info).group(1)
            except AttributeError: isbn = None
            
            try: pages = re.search('页数: (\d*)', info).group(1)
            except AttributeError: pages = None
            
            try: price = re.search('定价: (.* \d*)', info).group(1)
            except AttributeError: price = None
            
            try: publisher = re.search('出版社: (.*)', info).group(1)
            except AttributeError: publisher = None
    
            
            data['Release Date'] = date
            data['ISBN'] = isbn
            data['Pages'] = pages
            data['Price'] = price
            data['Publisher'] = publisher
            
            
            try:
                desc = bsoup.find('div', class_='related_info').find('span', class_='all hidden').text.strip()
                data['Short Description'] = desc
            except AttributeError:
                pass
            
            ratings = bsoup.find('div', class_='rating_wrap clearbox')
            
            try:
                average = ratings.find('strong', class_='ll rating_num')
                data['Average Rating'] = average.text.strip()
                
                rating_count = ratings.find('a').text.strip()
                rating_count = re.search('(\d*)', rating_count).group()
                
                data['No.of Ratings'] = rating_count
                
                start_ratings = ratings.find_all('span', class_='rating_per')
                start_ratings.reverse()
                
                for star, percent in enumerate(start_ratings, 1):
                    data[f'ratio of {star}star'] = percent.text.strip()
            except AttributeError:
                pass
        
            books_info = books_info.append(data, ignore_index=True)
            
        
    
    return books_info
   
    
def task1C_movies(movies_info, link):
    categories = ['wish', 'collect']
    
    for category in categories:
        reading = link + category
        reading = reading.replace('www', 'movie')
        
        r = req.get(reading, headers=headers)
        soup = bs(r.content, 'lxml')
        
        movie_links = [div.find('a')['href'] for div in soup.find_all('div', class_='item')]

        for movie in movie_links:
            m = req.get(movie, headers=headers)
            msoup = bs(m.content, 'lxml')

            name = msoup.select_one('#content > h1').text.strip()    
            # Validate if the movie already exists
            if len(movies_info.loc[movies_info['Name'] == name]) != 0:
                continue
            
          
            info = msoup.find('div', {'id':'info'})

            try: director = re.search('导演: (.*)', info.text.strip()).group(1)
            except AttributeError: director = None
            
            try: actors = info.find('span', class_='actor').find('span', class_='attrs').text.strip()
            except AttributeError: actors = None
            
            try: imdb = re.search('IMDb: (.*)', info.text.strip()).group(1)
            except AttributeError: imdb = None
            
            try: date = info.select_one('span[property="v:initialReleaseDate"]').text.strip()
            except AttributeError: date = None
            
            try: genre = info.select_one('span[property="v:genre"]').text.strip()
            except AttributeError: genre = None
            
            try: desc = msoup.find('div', {'id':'link-report'}).text.strip()
            except AttributeError: desc = None
            
            data = {
                    'Director' : director,
                    'Actor' : actors,
                    'IMDB' : imdb,
                    'release date' : date,
                    'Type': genre,
                    'Short Description' : desc,
                    'ID' : (movies_info['ID'].max() + 1) if not movies_info.empty else 1,
                    'Name' : name
                    }
#            data['ID'] = (movies_info['ID'].max() + 1) if not movies_info.empty else 1
#            data['Name'] = name

            
            
            ratings = msoup.find('div', class_='rating_wrap clearbox')    
            try: 
                average = ratings.find('strong', class_='ll rating_num')
                data['Average Rating'] = average.text.strip()
                
                
                rating_count = ratings.select_one('span[property="v:votes"]').text.strip()
                
                data['No.of.Ratings'] = rating_count
                
                start_ratings = ratings.find_all('span', class_='rating_per')
                start_ratings.reverse()
                
                for star, percent in enumerate(start_ratings, 1):
                    data[f'ratio of {star}star'] = percent.text.strip()
            except AttributeError:
                pass
            
            movies_info = movies_info.append(data, ignore_index=True)
            

        
    return movies_info

def task1C_behaviour(books_info, member_id, link):
    reviews = link + 'reviews'
    r = req.get(reviews, headers=headers)
    soup = bs(r.content, 'lxml')
    

    books_info['url'] = books_info['url'].apply(lambda x: x.strip())
    
    columns = ['id', 'name', 'ReadStatus(read/reading/plan_to_read)', 'rating', 'review']
    behave_df = pd.DataFrame(columns=columns)
    
    reviews = soup.find_all('div', class_='main review-item')
    
    for review in reviews:

        data = {}
        a = review.find('a', class_='subject-img')['href']
        book = books_info.loc[books_info['url'] == a.strip()]
        # Check if the book exists in the dataset if yes get the name and status
        if not book.empty:
            book = book.iloc[0]
            data['id'] = book['ID']
            data['name'] = book['Name']
            data['ReadStatus(read/reading/plan_to_read)'] = book['ReadStatus']

            
        rating = review.select_one('span[title]')['class'][0]
        rating = re.search('[\d]', rating).group()
        
        
        content = review.find('div', class_='short-content')
        try:
            content.find('a').decompose()
            content.find('p').decompose()
        except: pass
    
        data['rating'] = rating
        data['review'] = content.text.strip()
        

        behave_df = behave_df.append(data, ignore_index=True)

    behave_df.to_excel(f'u{member_id}_behaviours_info.xlsx', index=False)


def task1D(network_info, link, member_id, name):
    """ Task 1d"""
    followers_link = link + 'contacts' # Link to followers
    following_link = link + 'rev_contacts' #Link to following
    
    r = req.get(followers_link, headers=headers)
    fwsoup = bs(r.content, 'lxml')
    # Scrape followers
    followers = [dl.dd.text.strip() for dl in fwsoup.find_all('dl', class_='obu')]

    
    r = req.get(following_link, headers=headers)
    ffsoup = bs(r.content, 'lxml')
    # Scrape following
    following = [dl.dd.text.strip() for dl in ffsoup.find_all('dl', class_='obu')]

    data = {
            'user_id' : member_id,
            'user_name' : name,
            'userid_follower(seperate by :)' : ' : '.join(followers), # Adds the followers to the row
            'userid_following(seperate by :)' : ' : '.join(following)
            }
    
    # Appends it to the network_info dataframer for all members
    network_info = network_info.append(data, ignore_index=True)
    
    return network_info

def task1_joins(link, member_id):
    link += 'joins'
    link = link.replace('/people', '/group/people')
    
    r = req.get(link, headers=headers)
    soup = bs(r.content, 'lxml')
    
    groups = soup.select('.group-list.group-cards li div.title')
    names = [group.text.strip() for group in groups]
    
    
    df = pd.Series(names, name='group_name')
    
    df.to_excel(f'u{member_id}_joined_groups_info.xlsx', index=False)
    

    
def task2A():
    df = pd.DataFrame(columns=['topic_id', 'topic', 'author', 'comments', 'last_comment_time'])
    
    page = 0
    while page <= 35000:
        print(page)
        url = f'https://www.douban.com/group/707650/discussion?start={page}&type=new'
        r = req.get(url, headers=headers)
        print('gotten')
        soup = bs(r.content, features='lxml')
        try:
            div = soup.select_one('#content > div > div.article')
            table = div.find('table')        
            for row in table.find_all('tr', class_=''):
                data = {}
                
                link = row.find('a')['href']
                topic_id = re.search('/(\d+)/', link).group(1)
                data['topic_id'] = topic_id
                data['topic'], data['author'], data['comments'], data['last_comment_time'] = [td.text.strip() for td in row.find_all('td')]
                
                df = df.append(data, ignore_index=True)
                task2B(link)
                break
            
            page += 25
        except AttributeError:
            print('Retrying: ', page)
            continue
        
        
    
    df.to_excel('task2A.xlsx', index=False)

def task2B(link):
    
    topic_id = re.search('/(\d+)/', link).group(1)
    topic_columns = ['Original_post_text', 'Replies', 'User_name_who_replies', 'No.of.Likes', 'Datetime']
    df = pd.DataFrame(columns=topic_columns)
    
    data = {}
    
    r = req.get(link, headers=headers)
    soup = bs(r.content, 'lxml')
    
    # Original post text
    content = soup.find('div', class_='rich-content topic-richtext').text.strip()
    data['Original_post_text'] = content
    
    
    for reply in soup.find_all('li', class_='clearfix comment-item reply-item'):
        data['Replies'] = reply.find('p', 'reply-content').text.strip()#  Reply text
        
        head = reply.find('h4')
        data['User_name_who_replies'] = head.a.text.strip()
        
        data['Datetime'] = head.find('span', class_='pubtime').text.strip()
        
        try:
            # Likes doenst show every time
            likes = reply.find('div', class_='operation-div').text.strip()
            likes = re.search('(\d+)', likes).group()
            data['No.of.Likes'] = likes
        except AttributeError:
            pass
        
        df = df.append(data, ignore_index=True)
        data = {}
        # So it wont repeat post_text for other rows
        data['Original_post_text'] = None
        
        
    df.to_excel(f'task2B_{topic_id}.xlsx', index=False)
    

get_metadata()
task1A() 
task2A()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
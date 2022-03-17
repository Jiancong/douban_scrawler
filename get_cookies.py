from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import *
import pickle

PATH = 'assets/chromedriver.exe'
options = Options()
options.add_experimental_option('excludeSwitches', ['enable-logging'])

s = Service(PATH)
driver = webdriver.Chrome(service=s, options=options)
driver.maximize_window()


driver.get('https://www.douban.com/people/49407893/')
driver.minimize_window()

print('Please login to douban and press <enter> here when you are done!')
input('')

pickle.dump(driver.get_cookies() , open("assets/cookies.pkl","wb"))
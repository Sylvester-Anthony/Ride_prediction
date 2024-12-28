from bs4 import BeautifulSoup
import requests

url = 'https://s3.amazonaws.com/tripdata/index.html'

html_content = requests.get(url).text

print(html_content)
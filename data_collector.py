from bs4 import BeautifulSoup
import requests

url = 'https://s3.amazonaws.com/tripdata/index.html'

html_content = requests.get(url).text

soup = BeautifulSoup(html_content, "html.parser")
print(soup.find_all('tr'))
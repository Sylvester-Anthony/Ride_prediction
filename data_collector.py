# from bs4 import BeautifulSoup
# import requests

# url = 'https://s3.amazonaws.com/tripdata/index.html'

# html_content = requests.get(url)

# print(html_content.text)

# # # soup = BeautifulSoup(html_content, 'html.parser')

# # # td_tags = soup.find('tbody').find_all('tr')

# # # print(td_tags)

# # # td_texts = [td.get_text(strip=True) for td in td_tags]

# # # print(td_texts)

from requests_html import HTMLSession

# Create an HTML Session
session = HTMLSession()

# URL of the webpage
url = "https://s3.amazonaws.com/tripdata/index.html"

# Get the webpage content
response = session.get(url)

# Execute JavaScript
response.html.render(timeout=20)  # Render the page, allowing JavaScript to execute

# Extract the dynamic content from <tbody>
tbody_content = response.html.find('#tbody-content', first=True).html
print(tbody_content)
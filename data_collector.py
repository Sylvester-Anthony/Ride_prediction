import asyncio
from pyppeteer import launch
from bs4 import BeautifulSoup

async def scrape_tbody():
    # Launch a browser (non-headless for debugging)
    browser = await launch(headless=False, args=['--no-sandbox'])
    page = await browser.newPage()

    # Navigate to the URL
    url = "https://s3.amazonaws.com/tripdata/index.html"  # Replace with your actual URL
    await page.goto(url)

    # Wait for the page to load completely
    await page.waitForSelector('#tbody-content')
    await asyncio.sleep(5)  # Wait for JavaScript execution (5 seconds)

    # Capture the tbody content
    try:
        tbody_html = await page.querySelectorEval('#tbody-content', '(element) => element.outerHTML')
        print("Extracted HTML:", tbody_html)
    except Exception as e:
        print("Error extracting tbody content:", e)
        tbody_html = None

    # Close the browser
    await browser.close()

    if tbody_html:
        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(tbody_html, 'html.parser')
        rows = soup.find_all('tr')

        if not rows:
            print("No rows found! Check data population logic.")
        else:
            for row in rows:
                columns = [col.get_text(strip=True) for col in row.find_all('td')]
                print(columns)

# Run the coroutine
asyncio.get_event_loop().run_until_complete(scrape_tbody())

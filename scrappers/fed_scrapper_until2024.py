import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlparse
import pandas as pd

async def fetch_page(session, url):
    try:
        async with session.get(url) as response:
            if response.status:
                return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None

async def fetch_and_parse_speech(session, url):
    html = await fetch_page(session, url)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        article_div = soup.find('div', id='article')
        if not article_div:
            print('Error: no article div found')
            return

        speaker_tag = article_div.find('p', class_='speaker')
        speaker = speaker_tag.text if speaker_tag else 'Speaker not found'

        content_paragraphs = article_div.find_all('p', class_=False, id=False)
        content = ' '.join(paragraph.text for paragraph in content_paragraphs)

        parsed_url = urlparse(url)
        path_segments = parsed_url.path.split('/')
        date_segments = path_segments[-1].split('.')[0]
        date = ''.join(filter(str.isdigit, date_segments))

        speech_data = {
            'date': date,
            'speaker': speaker,
            'content': content
        }
        return speech_data

async def fetch_speeches_for_year(session, year):
    date_url = f'https://www.federalreserve.gov/newsevents/speech/{year}-speeches.htm'
    html = await fetch_page(session, date_url)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        eventList = soup.find('div', class_='row eventlist')

        hrefs = []
        if eventList:
            for p_tag in eventList.find_all('p'):
                a_tag = p_tag.find('a', class_=False)
                if a_tag and not a_tag.has_attr('class'):
                    full_url = urljoin(date_url, a_tag['href'])
                    hrefs.append(full_url)

        tasks = [fetch_and_parse_speech(session, url) for url in hrefs]
        await asyncio.gather(*tasks)

async def main():
    years = range(2011, 2024)
    async with aiohttp.ClientSession() as session:
        for year in years:
            speeches = await fetch_speeches_for_year(session, year)
            # Filter out empty results
            speeches = [speech for speech in speeches if speech]
            df = pd.DataFrame(speeches)
            csv_filename = f'speeches_{year}.csv'
            df.to_csv(csv_filename, index=False)
            print(f"Data for {year} saved to {csv_filename}")

if __name__ == "__main__":
    asyncio.run(main())

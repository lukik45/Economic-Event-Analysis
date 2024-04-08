import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import re
from urllib.parse import urljoin
from urllib.parse import urlparse

async def fetch_page(session, url):
    try:
        async with session.get(url) as response:
            return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

async def fetch_year_links(session, year):
    year_url = f'https://www.federalreserve.gov/newsevents/speech/{year}speech.htm'
    html = await fetch_page(session, year_url)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        speech_index = soup.find('ul', id='speechIndex')
        if speech_index:
            base_url = 'https://www.federalreserve.gov'
            return [urljoin(base_url, a_tag['href']) for a_tag in speech_index.find_all('a', href=True)]
    return []

async def scrape_speech_data(session, speech_url):
    html = await fetch_page(session, speech_url)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        title_text = soup.find('title').text
        # Regex to extract the speaker's name (assumed to be the first word before "--")
        speaker_match = re.search(r'Speech,\s(.*?)\s--', title_text)
        parsed_url = urlparse(speech_url)
        path_segments = parsed_url.path.split('/')
        date_segments = path_segments[-1].split('.')[0]


        speaker_name = speaker_match.group(1) if speaker_match else 'Speaker name not found'

        # Initialize an empty string to store concatenated text
        concatenated_text = ''

        # Find all <table> tags with specified width
        tables = soup.find_all('table', width="600")

        visited_elements = set()  # To track already processed elements

        for table in tables:
            for child in table.descendants:
                if child.name in ['p', 'ul', 'li'] and child not in visited_elements:
                    # Mark this element as visited
                    visited_elements.add(child)
                    if child.name == 'li':
                        concatenated_text += '• ' + child.get_text(strip=True) + '\n'
                    else:
                        concatenated_text += child.get_text(strip=True) + '\n'
        return {
            'date': date_segments,
            'Speaker': speaker_name,
            'content': concatenated_text,
        }
    return {}

async def fetch_speeches_for_year(session, year):
    speech_links = await fetch_year_links(session, year)
    tasks = [scrape_speech_data(session, link) for link in speech_links]
    return await asyncio.gather(*tasks)

async def main():
    years = range(1996, 2010)  
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

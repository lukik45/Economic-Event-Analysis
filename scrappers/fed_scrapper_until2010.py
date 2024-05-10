# import asyncio
# import aiohttp
# from bs4 import BeautifulSoup
# import pandas as pd
# import re
# from urllib.parse import urljoin
# from urllib.parse import urlparse

# async def fetch_page(session, url):
#     try:
#         async with session.get(url) as response:
#             return await response.text()
#     except Exception as e:
#         print(f"Error fetching {url}: {e}")
#         return None

# async def fetch_year_links(session, year):
#     year_url = f'https://www.federalreserve.gov/newsevents/speech/{year}speech.htm'
#     html = await fetch_page(session, year_url)
#     if html:
#         soup = BeautifulSoup(html, 'html.parser')
#         speech_index = soup.find('ul', id='speechIndex')
#         if speech_index:
#             base_url = 'https://www.federalreserve.gov'
#             return [urljoin(base_url, a_tag['href']) for a_tag in speech_index.find_all('a', href=True)]
#     return []

# async def scrape_speech_data(session, speech_url):
#     html = await fetch_page(session, speech_url)
#     if html:
#         soup = BeautifulSoup(html, 'html.parser')
#         title_text = soup.find('title').text
#         # Regex to extract the speaker's name (assumed to be the first word before "--")
#         speaker_match = re.search(r'Speech,\s(.*?)\s--', title_text)
#         parsed_url = urlparse(speech_url)
#         path_segments = parsed_url.path.split('/')
#         date_segments = path_segments[-1].split('.')[0]


#         speaker_name = speaker_match.group(1) if speaker_match else 'Speaker name not found'

#         # Initialize an empty string to store concatenated text
#         concatenated_text = ''

#         # Find all <table> tags with specified width
#         tables = soup.find_all('table', width="600")

#         visited_elements = set()  # To track already processed elements

#         for table in tables:
#             for child in table.descendants:
#                 if child.name in ['p', 'ul', 'li'] and child not in visited_elements:
#                     # Mark this element as visited
#                     visited_elements.add(child)
#                     if child.name == 'li':
#                         concatenated_text += '• ' + child.get_text(strip=True) + '\n'
#                     else:
#                         concatenated_text += child.get_text(strip=True) + '\n'
#         return {
#             'date': date_segments,
#             'Speaker': speaker_name,
#             'content': concatenated_text,
#         }
#     return {}

# async def fetch_speeches_for_year(session, year):
#     speech_links = await fetch_year_links(session, year)
#     tasks = [scrape_speech_data(session, link) for link in speech_links]
#     return await asyncio.gather(*tasks)

# async def main():
#     years = range(1996, 2010)  
#     async with aiohttp.ClientSession() as session:
#         for year in years:
#             speeches = await fetch_speeches_for_year(session, year)
#             # Filter out empty results
#             speeches = [speech for speech in speeches if speech]
#             df = pd.DataFrame(speeches)
#             csv_filename = f'speeches_{year}.csv'
#             df.to_csv(csv_filename, index=False)
#             print(f"Data for {year} saved to {csv_filename}")

# if __name__ == "__main__":
#     asyncio.run(main())


import requests
from bs4 import BeautifulSoup
import re
import csv
from datetime import datetime
from urllib.parse import urlparse

def getYearLinks(yearlink):
    try:
        response = requests.get(yearlink)
        response.raise_for_status()  # Raises an exception for 4XX/5XX errors
        soup = BeautifulSoup(response.content, 'html.parser')  # Use content for byte content to handle UTF-8
        speech_index = soup.find('ul', id='speechIndex')
        links = []
        if speech_index:
            parsed_base_url = urlparse(yearlink)
            base_url = f'{parsed_base_url.scheme}://{parsed_base_url.netloc}'
            for a_tag in speech_index.find_all('a', href=True):
                full_url = base_url + a_tag['href']
                links.append(full_url)
        return links
    except requests.HTTPError as e:
        print(f"Failed to fetch data from {yearlink}, status code: {e.response.status_code}")
        return []

def extract_date_from_url(url):
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split('/')
    date_segment = path_segments[-1].split('.')[0]
    return date_segment

def scrape_speech_data(year):
    complete_speech_data = []
    dateurl = f'https://www.federalreserve.gov/newsevents/speech/{year}speech.htm'
    yearlinks = getYearLinks(dateurl)
    for monthslinks in yearlinks:
        date = extract_date_from_url(monthslinks)
        try:
            response = requests.get(monthslinks)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding=response.encoding)
            title_text = soup.find('title').text
            speaker_match = re.search(r'Speech,\s(.*?)\s--', title_text)
            speaker_name = speaker_match.group(1) if speaker_match else 'Speaker name not found'
            concatenated_text = ''
            tables = soup.find_all('table', width="600")
            visited_elements = set()
            for table in tables:
                for child in table.descendants:
                    if child.name in ['p', 'ul', 'li'] and child not in visited_elements:
                        visited_elements.add(child)
                        text = child.get_text(" ", strip=True)  # Use space as a separator for <br/> tags
                        if child.name == 'li':
                            concatenated_text += '• ' + text + '\n'
                        else:
                            concatenated_text += text + '\n'
            speech_data = {
                'date': date,
                'speaker': speaker_name,
                'content': concatenated_text
            }
            complete_speech_data.append(speech_data)
        except requests.HTTPError as e:
            print(f"Failed to fetch data from {monthslinks}, status code: {e.response.status_code}")

    save_to_csv(complete_speech_data, year)

def save_to_csv(data, year):
    filename = f"speeches_{year}.csv"
    keys = data[0].keys() if data else ["date", "speaker", "content"]
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        for speech_data in data:
            writer.writerow(speech_data)
    print(f"Data for {year} saved to {filename}")

if __name__ == "__main__":
    pre2010dates = [str(date) for date in range(1996, 2011)]
    for date in pre2010dates:
        scrape_speech_data(date)

import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Thread
from tqdm import tqdm
async def fetch_page(session, url, headers):
    try:
        async with session.get(url, headers=headers) as response:
            return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def extract_text(url):
    pattern = r"with(.+?)(\d+)$"
    match = re.search(pattern, url)
    if match:
        return match.group(1).strip('-')
    else:
        return "No match found"

async def fetch_main_links(session, url, headers):
    html = await fetch_page(session, url, headers)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        terms_list = soup.find('ul', id='terms-bar__list_1-0')
        return [a_tag['href'] for a_tag in terms_list.find_all('a', href=True) if terms_list] if terms_list else []
    return []

async def fetch_detail_links(session, url, headers):
    html = await fetch_page(session, url, headers)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.find('div', id='dictionary-top300-list__content_1-0')
        return [a_tag['href'] for a_tag in content.find_all('a', href=True) if content] if content else []
    return []

async def scrape_title_and_summary(session, url, headers):
    html = await fetch_page(session, url, headers)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title').text.strip() if soup.find('title') else 'No Title Found'
        div = soup.find('div', id='mntl-sc-block-callout-body_1-0')
        summary = ''.join(item.get_text() for item in div.find_all('li')) if div else 'No Summary Found'
        return title, summary
    return 'No Title Found', 'No Summary Found'

def process_chunk(detail_links_chunk, headers):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def get_data():
        async with aiohttp.ClientSession() as session:
            tasks = [scrape_title_and_summary(session, url, headers) for url in detail_links_chunk]
            return await asyncio.gather(*tasks)
    
    results = loop.run_until_complete(get_data())
    loop.close()
    return results


async def handle_main_link(session, link, headers, progress_bar=None):
    detail_links = await fetch_detail_links(session, link, headers)
    data = []

    # Process in chunks instead of all at once
    chunk_size = 50  # Adjust based on your system's capability and target server's tolerance
    chunks = [detail_links[i:i + chunk_size] for i in range(0, len(detail_links), chunk_size)]
    
    with ProcessPoolExecutor() as executor:
        # Ensure process_chunk can accept sessions or make it capable of creating its own session
        futures = [executor.submit(process_chunk, chunk, headers) for chunk in chunks]
        for future in as_completed(futures):
            data.extend(future.result())

    if data:
        df = pd.DataFrame(data, columns=['Title', 'Summary'])
        csv_filename = extract_text(link) + '.csv'
        df.to_csv(csv_filename, index=False)
        print(f"Data saved to {csv_filename}")

def handle_main_link_threaded(links_chunk, headers, progress_bar):
    async def inner(session):
        tasks = [handle_main_link(session, link, headers, progress_bar) for link in links_chunk]
        await asyncio.gather(*tasks)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = aiohttp.ClientSession(loop=loop)
    loop.run_until_complete(inner(session))
    loop.run_until_complete(session.close())
    loop.close()

async def main():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    main_url = "https://www.investopedia.com/financial-term-dictionary-4769738"

    async with aiohttp.ClientSession() as session:
        main_links = await fetch_main_links(session, main_url, headers)
        
        # Determine the number of links per thread
        num_threads = 6
        links_per_thread = len(main_links) // num_threads + (1 if len(main_links) % num_threads else 0)
        
        threads = []
        progress_bars = [tqdm(total=links_per_thread, position=i, desc=f'Thread {i+1}') for i in range(num_threads)]

        for i in range(0, len(main_links), links_per_thread):
            chunk = main_links[i:i + links_per_thread]
            thread = Thread(target=handle_main_link_threaded, args=(chunk, headers, progress_bars[i//links_per_thread]))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Close all progress bars after completion
        for pb in progress_bars:
            pb.close()


if __name__ == "__main__":
    asyncio.run(main())
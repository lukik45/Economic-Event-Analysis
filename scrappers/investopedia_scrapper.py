"""
Investopedia Scraper
====================

This script scrapes financial terms and their summaries from Investopedia.
The script uses asynchronous programming for network requests and concurrent
programming to process data efficiently. It saves the scraped data into CSV
files.

Modules
-------
- asyncio: Supports asynchronous programming.
- aiohttp: Handles asynchronous HTTP requests.
- BeautifulSoup: Parses HTML content.
- pandas: Manages and manipulates data.
- re: Provides regular expression matching operations.
- concurrent.futures: Provides a high-level interface for asynchronously
    executing callables.
- threading: Constructs higher-level threading interfaces.
- tqdm: Displays progress bars.
- os: Provides a way of using operating system-dependent functionality.


Usage
-----
Run the script using the command:
    python investopedia_scraper.py

Ensure you have the required modules installed:
    pip install aiohttp beautifulsoup4 pandas tqdm
"""

import asyncio
import aiohttp
from aiohttp.client import ClientSession
from bs4 import BeautifulSoup, Tag
import pandas as pd
import re
from typing import List, Optional, Tuple, Dict
from concurrent.futures import ProcessPoolExecutor, as_completed
from threading import Thread
from tqdm import tqdm
import os


async def fetch_page(
    session: ClientSession, url: str, headers: Dict[str, str]
) -> Optional[str]:
    """
    Fetches the HTML content of the given URL asynchronously.

    Parameters
    ----------
    session : aiohttp.ClientSession
        The aiohttp session for making the HTTP request.
    url : str
        The URL to fetch the HTML content from.
    headers : dict
        The headers to include in the HTTP request.

    Returns
    -------
    str or None
        The HTML content of the page, or None if an error occurs.
    """
    try:
        async with session.get(url, headers=headers) as response:
            return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def extract_text(url: str) -> str:
    """
    Extracts a specific text pattern from the URL.

    Parameters
    ----------
    url : str
        The URL to extract the text from.

    Returns
    -------
    str
        The extracted text or 'No match found' if no match is found.
    """
    pattern = r"with(.+?)(\d+)$"
    match = re.search(pattern, url)
    if match:
        return match.group(1).strip('-')
    else:
        return "No match found"


async def fetch_main_links(
    session: ClientSession, url: str, headers: Dict[str, str]
) -> List[str]:
    """
    Fetches the main links from the Investopedia financial
    term dictionary page.

    Parameters
    ----------
    session : aiohttp.ClientSession
        The aiohttp session for making the HTTP request.
    url : str
        The URL to fetch the main links from.
    headers : dict
        The headers to include in the HTTP request.

    Returns
    -------
    list
        A list of main link URLs.
    """
    html = await fetch_page(session, url, headers)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        terms_list = soup.find('ul', id='terms-bar__list_1-0')
        if isinstance(terms_list, Tag):
            return [
                a_tag['href']
                for a_tag in terms_list.find_all('a', href=True)
            ]
    return []


async def fetch_detail_links(
    session: ClientSession, url: str, headers: Dict[str, str]
) -> List[str]:
    """
    Fetches the detailed links from each main link page.

    Parameters
    ----------
    session : aiohttp.ClientSession
        The aiohttp session for making the HTTP request.
    url : str
        The main link URL to fetch the detail links from.
    headers : dict
        The headers to include in the HTTP request.

    Returns
    -------
    list
        A list of detail link URLs.
    """
    html = await fetch_page(session, url, headers)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.find('div', id='dictionary-top300-list__content_1-0')
        if isinstance(content, Tag):
            return [
                a_tag['href']
                for a_tag in content.find_all('a', href=True)
            ]
    return []


async def scrape_title_and_summary(
    session: ClientSession, url: str, headers: Dict[str, str]
) -> Tuple[str, str]:
    """
    Scrapes the title and summary from the detail link page.

    Parameters
    ----------
    session : aiohttp.ClientSession
        The aiohttp session for making the HTTP request.
    url : str
        The detail link URL to scrape the title and summary from.
    headers : dict
        The headers to include in the HTTP request.

    Returns
    -------
    tuple
        A tuple containing the title and summary of the page.
    """
    html = await fetch_page(session, url, headers)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        title_tag = soup.find('title')
        title = title_tag.text.strip() if title_tag else 'No Title Found'
        div = soup.find('div', id='mntl-sc-block-callout-body_1-0')
        if isinstance(div, Tag):
            summary = (
                ''.join(item.get_text() for item in div.find_all('li'))
                if div else 'No Summary Found'
            )
            return title, summary
    return 'No Title Found', 'No Summary Found'


def process_chunk(
    detail_links_chunk: List[str], headers: Dict[str, str]
) -> List[Tuple[str, str]]:
    """
    Processes a chunk of detail links concurrently.

    Parameters
    ----------
    detail_links_chunk : list
        The list of detail link URLs to process.
    headers : dict
        The headers to include in the HTTP request.

    Returns
    -------
    list
        A list of tuples containing the title and summary for each detail link.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def get_data() -> List[Tuple[str, str]]:
        async with aiohttp.ClientSession() as session:
            tasks = [
                scrape_title_and_summary(session, url, headers)
                for url in detail_links_chunk
            ]
            return await asyncio.gather(*tasks)

    results: List[Tuple[str, str]] = loop.run_until_complete(get_data())
    loop.close()
    return results


async def handle_main_link(
    session: ClientSession, link: str, headers: Dict[str, str],
    progress_bar: Optional[tqdm] = None
) -> None:
    """
    Handles fetching and processing of detail links for a given main link.

    Parameters
    ----------
    session : aiohttp.ClientSession
        The aiohttp session for making the HTTP request.
    link : str
        The main link URL to handle.
    headers : dict
        The headers to include in the HTTP request.
    progress_bar : tqdm.tqdm, optional
        The progress bar to update during processing (default is None).

    Returns
    -------
    None
    """
    detail_links = await fetch_detail_links(session, link, headers)
    data: List[Tuple[str, str]] = []

    chunk_size = 50
    chunks = [
        detail_links[i:i + chunk_size]
        for i in range(0, len(detail_links), chunk_size)
    ]

    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_chunk, chunk, headers)
            for chunk in chunks
        ]
        for future in as_completed(futures):
            data.extend(future.result())

    if data:
        df = pd.DataFrame(data, columns=['Title', 'Summary'])
        csv_filename = 'data/investopedia/' + extract_text(link) + '.csv'
        df.to_csv(csv_filename, index=False)
        print(f"Data saved to {csv_filename}")


def handle_main_link_threaded(
    links_chunk: List[str], headers: Dict[str, str], progress_bar: tqdm
) -> None:
    """
    Manages threading for handling main link chunks.

    Parameters
    ----------
    links_chunk : list
        The list of main link URLs to handle.
    headers : dict
        The headers to include in the HTTP request.
    progress_bar : tqdm.tqdm
        The progress bar to update during processing.

    Returns
    -------
    None
    """
    async def inner(session: ClientSession) -> None:
        tasks = [
            handle_main_link(session, link, headers, progress_bar)
            for link in links_chunk
        ]
        await asyncio.gather(*tasks)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    session = aiohttp.ClientSession(loop=loop)
    loop.run_until_complete(inner(session))
    loop.run_until_complete(session.close())
    loop.close()


async def main() -> None:
    """
    The main function to orchestrate the entire scraping process.

    Creates necessary directories, initializes headers and session,
    and starts the scraping process using multiple threads.

    Returns
    -------
    None
    """
    if not os.path.exists('data'):
        os.makedirs('data')

    if not os.path.exists('data/investopedia'):
        os.makedirs('data/investopedia')

    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        )
    }
    main_url = "https://www.investopedia.com/financial-term-dictionary-4769738"

    async with aiohttp.ClientSession() as session:
        main_links = await fetch_main_links(session, main_url, headers)

        num_threads = 6
        links_per_thread = len(main_links) // num_threads + (
            1 if len(main_links) % num_threads else 0
        )

        threads: List[Thread] = []
        progress_bars = [
            tqdm(total=links_per_thread, position=i, desc=f'Thread {i+1}')
            for i in range(num_threads)
        ]

        for i in range(0, len(main_links), links_per_thread):
            chunk = main_links[i:i + links_per_thread]
            thread = Thread(
                target=handle_main_link_threaded,
                args=(chunk, headers, progress_bars[i // links_per_thread])
            )
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for pb in progress_bars:
            pb.close()


if __name__ == "__main__":
    asyncio.run(main())

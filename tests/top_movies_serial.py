import sys
import re
import time
import itertools
import logging
import requests
import pandas as pd
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup

df = pd.DataFrame(columns=['url', 'rank', 'title'])

urls = set(['https://www.imdb.com/chart/top',
            'https://www.ranker.com/crowdranked-list/the-best-movies-of-all-time',
            'https://www.empireonline.com/movies/features/best-movies/'])

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("sreq")
logging.getLogger("chardet.charsetprober").disabled = True

TRAIL_YR_RE = re.compile(r'^(.*)\s*\(\d{4}\)\s*$')


def fetch_html(url: str, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.
    """

    resp = requests.get(url)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status_code, url)
    html = resp.content
    return html


def parse(url: str, **kwargs) -> list:
    """Grab movie names and rankings from different sites
    They are set up differently, so different code is used for each site

    Returns a list of the url, ranking, and title
    """
    found = []
    try:
        html = fetch_html(url)
    except HTTPError as e:
        logger.error(
            "Exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return found
    except Exception as e:
        logger.exception(
            "Exception occurred:  %s", getattr(e, "__dict__", {})
        )
        return found
    else:
        soup = BeautifulSoup(html, 'html.parser')

        if 'www.empireonline.com' in url:
            # Empireonline
            for link in soup.find_all('h2'):
                rank, title = link.contents[0].split('.', 1)
                title = title.strip()[:-7]  # Strips off the year
                if title == 'Seven':
                    title = 'Se7en'
                found.append([url, int(rank), title])
            return found
        elif 'www.ranker.com' in url:
            # Ranker
            # It was easier to grab the rank and title in two different loops, then zip them together
            r = []
            t = []
            for link in soup.find_all('meta', itemprop='position'):
                r.append(int(link['content']))
            for link in soup.find_all('meta', itemprop='name'):
                if TRAIL_YR_RE.search(link['content']):
                    t.append(TRAIL_YR_RE.search(link['content']).group(1).strip())
                else:
                    if link['content'] == 'Star Wars':
                        t.append('Star Wars: Episode IV — A New Hope')
                    elif link['content'] == 'Indiana Jones and the Raiders of the Lost Ark':
                        t.append('Raiders of the Lost Ark')
                    else:
                        t.append(link['content'])
            t = t[2:]  # The first two are not rankings
            return list(zip(list(itertools.repeat(url, len(r))), r, t))
        elif 'www.imdb.com' in url:
            # IMDB
            for link in soup.find_all('td', 'titleColumn'):
                rank = link.next_element.strip()[:-1]
                title = link.contents[1].next_element.rstrip()
                found.append([url, int(rank), title])
            return found


def add_to_df(url: str, **kwargs) -> None:
    """Append the results of each url to a dataframe"""
    global df
    res = parse(url=url, **kwargs)
    if not res:
        return None
    df = df.append(pd.DataFrame(res, columns=['url', 'rank', 'title']), ignore_index=True)


def crawl_and_parse(urls: set, **kwargs) -> None:
    """Crawl & parse multiple `urls`."""
    for url in urls:
        add_to_df(url=url, **kwargs)


def print_lists() -> None:
    global df
    # Operations dealing with numbers need to have the rank converted to int
    df['rank'] = pd.to_numeric(df['rank'], errors='coerce').fillna(0).astype(int)
    df['title_comp'] = df['title']
    df = df.replace({'title_comp': r'[^a-zA-Z0-9 ]'}, {'title_comp': ''}, regex=True)
    df = df.replace({'title_comp': r'\s{2,}'}, {'title_comp': ' '}, regex=True)
    df['title_comp'] = df['title_comp'].str.lower()
    # Remove records where title was not in all lists - yes, this is controversial
    df = df.groupby('title_comp').filter(lambda x: len(x) == len(urls))

    print('Aggregated movie rankings by sum:')
    print(df.groupby(['title_comp'])['rank'].sum().sort_values().head(10))
    print()
    print('Aggregated movie rankings by average rank:')
    print(df.groupby(['title_comp'])['rank'].mean().sort_values().head(10))
    return


if __name__ == "__main__":
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent
    start = time.perf_counter()
    crawl_and_parse(urls=urls)
    elapsed = time.perf_counter() - start
    print(f"Serial portion completed in {elapsed:0.5f} seconds.")
    print_lists()

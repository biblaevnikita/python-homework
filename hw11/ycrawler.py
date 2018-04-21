import argparse
import asyncio
import logging
import os
import shutil
import time

import aiohttp
import requests
from lxml import html
from concurrent.futures import ThreadPoolExecutor

BASE_URL = 'https://news.ycombinator.com/'
RESTRICTED_CHARS = '<>:"/\\|?*'
THREAD_PAGE_HREF = 'item?id='

tp_executor = ThreadPoolExecutor(30)


def restore_state(output_dir):
    finished = set()

    for name in os.listdir(output_dir):
        path = os.path.join(output_dir, name)
        if not os.path.isdir(path):
            continue

        if name.isdigit():
            finished.add(name)

    return finished


def get_trending_news():
    response = requests.get(BASE_URL)
    tree = html.fromstring(response.text)
    post_id = tree.xpath('//tr[@class="athing"]/@id')
    news_urls = tree.xpath('//a[@class="storylink"]/@href')

    news = dict(zip(post_id, news_urls))
    return news


async def wait_cancel_pending(tasks, timeout):
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    for p in pending:
        p.cancel()

    return done


async def fetch_url(url, session):
    async with session.get(url) as response:
        data = await response.read()

    return data


async def download_url(url, session, to):
    logging.info('Downloading {}'.format(url))
    base_dir = os.path.dirname(to)

    if not os.path.isdir(base_dir):
        os.makedirs(base_dir)

    data = await fetch_url(url, session)
    loop = asyncio.get_event_loop()
    loop.run_in_executor(tp_executor, save_data, to, data)


def save_data(file_path, data):
    with open(file_path, 'wb') as fp:
        fp.write(data)


async def collect_news(news, output_dir, timeout):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for thread_id, news_url in news.items():
            # internal link support
            if news_url.startswith(THREAD_PAGE_HREF):
                news_url = BASE_URL + news_url

            path = os.path.join(output_dir, thread_id, 'news.html')
            tasks.append(download_url(news_url, session, path))

        await wait_cancel_pending(tasks, timeout)


async def collect_news_urls(news, output_dir, timeout):
    urls = []
    async with aiohttp.ClientSession() as session:
        fetch_urls_tasks = [fetch_thread_urls(thread_id, session) for thread_id in news]
        done = await wait_cancel_pending(fetch_urls_tasks, timeout)
        for done_task in done:
            urls.extend(done_task.result())

        if not urls:
            logging.info('No comments in threads')
            return

        download_urls_tasks = []
        for thread_id, comment_id, comment_urls in urls:
            for comment_url in comment_urls:
                if not comment_url.startswith('http'):
                    continue

                path = os.path.join(output_dir, thread_id, 'comments', comment_id, create_name_from_url(comment_url))
                download_urls_tasks.append(download_url(comment_url, session, path))

        if download_urls_tasks:
            await wait_cancel_pending(download_urls_tasks, timeout)


async def fetch_thread_urls(thread_id, session):
    thread_url = BASE_URL + THREAD_PAGE_HREF + thread_id
    page_data = await fetch_url(thread_url, session)

    urls = []
    for comment_id, comment_urls in extract_thread_urls(page_data):
        urls.append((thread_id, comment_id, comment_urls))

    return urls


def extract_thread_urls(thread_page):
    urls = []
    tree = html.fromstring(thread_page)
    comments = tree.xpath('//tr[@class="athing comtr "]')

    for comment in comments:
        comment_id = comment.attrib['id']
        comment_div = comment.xpath('.//div[@class="comment"]')
        if comment_div:
            comment_div = comment_div[0]
        else:
            continue

        comment_urls = comment_div.xpath('.//a[@rel="nofollow"]/@href')

        if comment_urls:
            urls.append((comment_id, comment_urls))

    return urls


def run(finished, output_dir, timeout):
    trending_news = get_trending_news()

    for post_id in list(trending_news):
        if post_id in finished:
            del trending_news[post_id]

    finished.update(trending_news.keys())

    if not trending_news:
        logging.info('Trending news are up to date')
        return

    logging.info('{} new trending news'.format(len(trending_news)))

    loop = asyncio.get_event_loop()
    tasks = [collect_news(trending_news, output_dir, timeout),
             collect_news_urls(trending_news.keys(), output_dir, timeout)]
    futures = asyncio.gather(*tasks)
    loop.run_until_complete(futures)


def main(args):
    output_dir = args.out
    if os.path.isdir(output_dir):
        if args.clear:
            shutil.rmtree(output_dir)
            os.makedirs(output_dir)
    else:
        os.makedirs(output_dir)

    finished = restore_state(output_dir)

    while True:
        logging.info('Collecting news...')
        duration = -time.time()

        run(finished, output_dir, args.timeout)

        duration += time.time()
        logging.info('Collected in {:.2f} seconds'.format(duration))
        logging.info('Zzz...')

        sleep_time = args.interval - duration
        if sleep_time > 0:
            time.sleep(args.interval)


def create_name_from_url(url):
    for c in RESTRICTED_CHARS:
        url = url.replace(c, '_')

    return url


def parse_args():
    parser = argparse.ArgumentParser('ycombinator news crawler')
    parser.add_argument('-i', '--interval', help='Poll interval (seconds)', type=int, default=60)
    parser.add_argument('-o', '--out', help='Output directory', required=True)
    parser.add_argument('-l', '--logfile', help='Logfile path')
    parser.add_argument('-t', '--timeout', help='Tasks timeout', type=int, default=30)
    parser.add_argument('-c', '--clear', help='Clear output directory before run', action='store_true')
    return parser.parse_args()


if __name__ == '__main__':
    parsed_args = parse_args()
    logging.basicConfig(filename=parsed_args.logfile, level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')

    try:
        main(parsed_args)
    except KeyboardInterrupt:
        logging.info('Canceled by user')
    except:
        logging.exception('Error: ')

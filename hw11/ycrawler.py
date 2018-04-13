import shutil
import asyncio
import aiohttp
import argparse
import logging
import os
import re
import requests
from lxml import html
import time

BASE_URL = 'https://news.ycombinator.com/'
FINISHED_NEWS_RE = re.compile('\d+')
RESTRICTED_CHARS = '<>:"/\\|?*'
COMMENTS_PAGE = 'item?id='


def create_name_from_url(url):
    for c in RESTRICTED_CHARS:
        url = url.replace(c, '_')

    return url


def restore_state(output_dir):
    finished = set()

    for name in os.listdir(output_dir):
        path = os.path.join(output_dir, name)
        if not os.path.isdir(path):
            continue

        if FINISHED_NEWS_RE.match(name):
            finished.add(name)

    return finished


def get_news():
    response = requests.get(BASE_URL)
    tree = html.fromstring(response.text)
    news_ids = tree.xpath('//tr[@class="athing"]/@id')
    news_urls = tree.xpath('//a[@class="storylink"]/@href')

    news = dict(zip(news_ids, news_urls))
    return news


async def download_file(url, session, save_dir, name=None):
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)

    file_name = name or create_name_from_url(url)
    file_path = os.path.join(save_dir, file_name)
    async with session.get(url) as response:
        with open(file_path, 'wb') as fp:
            fp.write(await response.read())


async def download_news(news, output_dir, timeout):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for news_id, news_url in news.items():
            download_path = os.path.join(output_dir, news_id)
            tasks.append(download_file(news_url, session, download_path, 'news.html'))

        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for future in pending:
            future.cancel()


async def download_comment_urls(news, output_dir, timeout):
    comments_info = await get_comments(news, timeout)
    await download_urls(comments_info, output_dir, timeout)


async def download_urls(comments_info, output_dir, timeout):
    if not comments_info:
        return

    async with aiohttp.ClientSession() as session:
        tasks = []
        for news_id, comment_id, urls in comments_info:
            path = os.path.join(os.path.join(output_dir, news_id, comment_id))
            for url in urls:
                tasks.append(download_file(url, session, path))

        done, pending = await asyncio.wait(tasks, timeout=timeout)

        for f in pending:
            f.cancel()


async def get_comments(news, timeout):
    async with aiohttp.ClientSession() as session:
        tasks = [get_comments_info(session, news_id) for news_id in news]
        done, pending = await asyncio.wait(tasks, timeout=timeout)

        for f in pending:
            f.cancel()

        comments = []
        for f in done:
            comments.extend(f.result())

        return comments


async def get_comments_info(session, news_id):
    comments_url = BASE_URL + COMMENTS_PAGE + news_id
    async with session.get(comments_url) as response:
        comments_page_text = await response.text()

    urls = parse_comments_info(comments_page_text)
    return [(news_id, comment_id, comment_urls) for comment_id, comment_urls in urls]


def parse_comments_info(comments_page_text):
    info = []
    tree = html.fromstring(comments_page_text)
    comments = tree.xpath('//tr[@class="athing comtr "]')

    for comment in comments:
        comment_id = comment.attrib['id']
        comment_div = comment.xpath('.//div[@class="comment"]')
        if comment_div:
            comment_div = comment_div[0]
        else:
            continue

        urls = comment_div.xpath('.//a[@rel="nofollow"]/@href')

        if urls:
            info.append((comment_id, urls))

    return info


def start_crawler(finished, output_dir, timeout, interval):
    while True:
        logging.info('Getting news...')
        news = get_news()

        for news_id in list(news):
            if news_id in finished:
                del news[news_id]

        finished.update(news)
        logging.info('{} new trending news'.format(len(news)))

        if news:
            loop = asyncio.get_event_loop()
            tasks = [
                download_news(news, output_dir, timeout),
                download_comment_urls(news, output_dir, timeout)
            ]
            wait_tasks = asyncio.wait(tasks)
            loop.run_until_complete(wait_tasks)

        logging.info('Zzz...')
        time.sleep(interval)


def main(args):
    output_dir = args.out
    if os.path.isdir(output_dir):
        if args.clear:
            shutil.rmtree(output_dir)
            os.makedirs(output_dir)
    else:
        os.makedirs(output_dir)

    finished = restore_state(output_dir)
    start_crawler(finished, output_dir, args.timeout, args.interval)


def parse_args():
    parser = argparse.ArgumentParser('ycombinator news crawler')
    parser.add_argument('-i', '--interval', help='Poll interval (seconds)', type=int, default=60)
    parser.add_argument('-o', '--out', help='Output directory')
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

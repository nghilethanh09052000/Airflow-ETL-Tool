import pandas as pd
import logging
import requests
from airflow.decorators import task
from airflow.models import Variable
from typing import List, Dict
from bs4 import BeautifulSoup
from utils.common import set_env_value
from tenacity import retry, stop_after_attempt, wait_fixed
from utils.data_upload.gcs_upload import GCSDataUpload, SupportedGcsFileFormat
import time
import uuid


class ArticlePage:

    def __init__(self, html_content):
        self.soup = BeautifulSoup(html_content, 'html.parser')

    def get_title(self):
        return self.soup.find('h1', class_='article-title').get('title')

    def get_article_info(self):
        article_element = self.soup.find('section', class_='article-info')
        article_text = article_element.get_text()
        for a_tag in article_element.find_all('a'):

            a_text = a_tag.get_text()
            a_href = a_tag.get('href')

            if not a_text or not a_href: continue

            if 'http://' in a_text or 'https://' in a_text: continue

            if a_href and a_href.startswith('/hc'):
                a_href = f"https://support.playsipher.com{a_href}"

            article_text = article_text.replace(a_text, f"{a_text} ({a_href})")

        try:
            table = article_element.find('table')
            if table:
                table_content = self.scrape_table_content(table)
                start_index = article_text.find(table.get_text())
                end_index = start_index + len(table.get_text())
                article_text = article_text[:start_index] + table_content + article_text[end_index:]
        except AttributeError:
            pass
        return article_text.strip()

    def scrape_table_content(self, table):
        rows = table.find_all('tr')
        table_content = ''
        for row in rows:
            cells = row.find_all('td')
            row_content = '\t'.join([' '.join(cell.stripped_strings) for cell in cells])
            table_content += row_content + '\n'
        return table_content.strip()

    def get_article_author(self):
        return self.soup.find('div', class_='article-author').find('a').get_text().strip()

    def get_updated_date(self):
        return self.soup.find('ul', class_='meta-group').find('time').get('datetime').strip()
        

class ArticleCrawler:
    def __init__(self, article_links: List[Dict[str,str]]):
        self.article_links = article_links
        self.data = []

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def crawl_article(self, url, article_id):
        post_body = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": 60000
        }
        response = requests.post(
            set_env_value(
                production=f"{Variable.get('flaresolverr_hosting')}/v1",
                dev='http://flaresolverr:8191/v1'
            ),
            json=post_body,
            headers={'Content-Type': 'application/json'}
        )
        data = response.json()

        if not data.get('status') == 'ok':
            raise Exception(f"Failed to get response from {url}")

        solution = data.get('solution')

        html_response = solution.get('response')

        article_page = ArticlePage(html_response)
        title = article_page.get_title()
        article_info = article_page.get_article_info()
        article_author = article_page.get_article_author()
        updated = article_page.get_updated_date()

        print('DATA------------------------------------------------------',
            {
                  'url': str(url),
                  'title': str(title),
                  'article_info': str(article_info),
                  'article_author': str(article_author),
                  'updated': str(updated)
              }
        )
        return {
            'url'           : str(url),
            'article_id'    : str(article_id),
            'title'         : str(title),
            'article_info'  : str(article_info),
            'article_author': str(article_author),
            'updated'       : str(updated)
        }

    def crawl_articles(self):
        for article_link in self.article_links:
            url = article_link.get('url')
            article_id = article_link.get('article_id')

            try:
                article_data = self.crawl_article(url, article_id)
                self.data.append(article_data)
            except Exception as e:
                logging.error(f"Failed to crawl article {url}: {e}")

        return self.data


@task(task_id='get_article_infos')
def get_article_infos(
    gcs_bucket: str,
    gcs_prefix: str,
    **kwargs
):

    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    timestamp=int(execution_date.timestamp())

    article_links = ti.xcom_pull(
        key='article_links',
        task_ids='get_article_links'
    )
        
    crawler = ArticleCrawler(article_links)
    data = crawler.crawl_articles()

    partition_prefix = f"snapshot_timestamp={str(timestamp)}"
    collected_ts = round(time.time() * 1000)

    gcs_upload = GCSDataUpload(
        gcs_bucket=gcs_bucket,
        gcs_prefix=gcs_prefix
    )
    gcs_upload.upload(
        object_name=f"/{partition_prefix}/{uuid.uuid4()}",
        data=data,
        gcs_file_format=SupportedGcsFileFormat.PARQUET,
        pre_upload_callable=gcs_upload._prepare_before_upload(collected_ts=collected_ts)
    )


    logging.info(f'Crawl Info Articles Data From Article Success {data}')



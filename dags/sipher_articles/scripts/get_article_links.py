import requests
import logging
import re
from bs4 import BeautifulSoup
from airflow.decorators import task



@task(task_id = 'get_article_links')
def get_article_links(**kwargs):
    ti = kwargs['ti']
    response = requests.get('https://support.playsipher.com/hc/sitemap.xml')

    # Parse the XML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'xml')

    # Find all 'loc' elements under 'url' elements containing 'articles' in their text
    locs = soup.find_all('loc', text=re.compile("articles"))

    # Extract URLs and IDs
    data = []
    for loc in locs:
        url = loc.text
        article_id = re.search(r'/(\d+)-', url).group(1)
        data.append({
            'url': url, 
            'article_id': article_id
        })

    logging.info(f'Crawl Data Successfully: {data}')

    ti.xcom_push(
        key='article_links',
        value=data
    )
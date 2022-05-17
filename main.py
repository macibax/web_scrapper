# argparse es un modulo que permite
# utilizar argumentos desde la liena de comandos
import argparse 
from common import config

import csv
import datetime

from news_page_objects import ArticlePage, HomePage

# configuracin de logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _news_scraper(news_site_uid):
    host = config()['news_sites'][news_site_uid]['url']
    logging.info(f'Beginning scrapper for {host}')

    page = HomePage(news_site_uid, host)

    articles = []
    for link in page.article_links:
        # print("link::: ",link)
        article = get_article(news_site_uid, link)
        if article:
            articles.append(article)
            # print(article.article_title)
    save_csv_dataset(news_site_uid, articles)


def get_article(news_site_uid, link):
    # logging.info(f'Accessing "{link}"')
    try:
        article = ArticlePage(news_site_uid, link)
        if not article.article_body:
            return None
        return article
    except:
        logging.info(f'Could not access "{link}"')
        return None
    
def save_csv_dataset(name, data):
    now = datetime.datetime.now().strftime('%Y-%m-%d')
    file_name = f"{name}_{now}.csv"
    logging.info(f'Saving info in "{file_name}"')

    # obtener propiedades no privadas de objeto data
    # s usaran como nombres de columnas y para obtener valores de las mismas
    csv_headers = list( filter( lambda property: not property.startswith('_'), dir(data[0]) ) )
    print("csv_headers::: ",csv_headers)

    with open(file_name, mode="w+", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)

        for e in data:
            row_value = [ str(getattr(e, prop)) for prop in csv_headers ]
            print("row_value::: ",row_value)
            row = row_value
            writer.writerow(row)

if __name__ == "__main__":
    logging.info('START')
    parser = argparse.ArgumentParser()

    news_site_choices = list(config()['news_sites'].keys())
    logging.info(f'news_site_choices::: {news_site_choices}')

    # agrega el argumento de news_sites para que se
    # pueda ejecutar desde linea de comandos, ej:
    # python main.py <nombre de sitio>
    parser.add_argument(
        'news_sites',
        help='The news site that you want to scrape',
        type=str,
        choices=news_site_choices,
    )
    args = parser.parse_args()
    _news_scraper(args.news_sites)
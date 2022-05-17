import requests
import bs4
import re

from common import config

is_well_formed_url = re.compile(r'^https?://.+/.+$')
is_root_path = re.compile(r'^/.+$')

class NewsSite:
    def __init__(self, news_site_uid: str, url: str) -> None:
        self._config = config()['news_sites'][news_site_uid]
        self._queries = self._config['queries']
        self._url = url
        self._html = None
        self._visit()

    def _select(self, query_string):
        return self._html.select(query_string)

    def _visit(self, url: str = ''):
        if not url:
            url = self._url
        response = requests.get(url)

        # lanza error si solicitud no se concreta correctamente
        response.raise_for_status()

        # guardar text del html
        self._html = bs4.BeautifulSoup(response.text, 'html.parser')

    def _get_well_formed_link(self, link):
        # print("_get_well_formed_link::: ",link)
        if is_well_formed_url.findall(link):
            return link
        elif is_root_path.findall(link):
            return f"{self._url}{link}"
        else:
            return f"{self._url}/{link}"


class HomePage(NewsSite):
    def __init__(self, news_site_uid: str, url: str) -> None:
        super().__init__(news_site_uid, url)

    @property
    def article_links(self):
        link_list = []
        for link in self._select(self._queries['homepage_article_links']):
            if link and link.has_attr('href'):
                link_list.append(self._get_well_formed_link(link["href"]))
        return set(link_list)

class ArticlePage(NewsSite):
    def __init__(self, news_site_uid: str, url: str) -> None:
        super().__init__(news_site_uid, url)

    @property
    def article_title(self):
        res = self._select(self._queries['article_title'])
        return res[0].text if len(res) else ''

    @property
    def article_body(self):
        res = self._select(self._queries['article_body'])
        return res[0] if len(res) else ''
    

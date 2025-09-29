"""
Crawler implementation.
"""

# pylint: disable=too-many-arguments, too-many-instance-attributes, unused-import, undefined-variable, unused-argument

import json
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core_utils.article import io
from core_utils.article.article import Article
from core_utils.config_dto import ConfigDTO
from core_utils.constants import (
    ASSETS_PATH,
    CRAWLER_CONFIG_PATH,
    NUM_ARTICLES_UPPER_LIMIT,
    TIMEOUT_LOWER_LIMIT,
    TIMEOUT_UPPER_LIMIT,
)


class IncorrectSeedURLError(Exception):
    """
    Raised when the seed URL is not written correctly in the configuration file.
    """


class NumberOfArticlesOutOfRangeError(Exception):
    """
    Raised when the number of articles is too large in the configuration file.
    """


class IncorrectNumberOfArticlesError(Exception):
    """
    Raised when the number of articles is too small or not an integer in the configuration file.
    """


class IncorrectHeadersError(Exception):
    """
    Raised when the headers are not in a form of dictionary in the configuration file.
    """


class IncorrectEncodingError(Exception):
    """
    Raised when the encoding is not specified as a string in the configuration file.
    """


class IncorrectTimeoutError(Exception):
    """
    Raised when the timeout is too large or not a positive integer in the configuration file.
    """


class IncorrectVerifyError(Exception):
    """
    Raised when the verify certificate value is neither True nor False in the configuration file.
    """


class Config:
    """
    Class for unpacking and validating configurations.
    """

    path_to_config: Path
    _seed_urls: list[str]
    _num_articles: int
    _headers: dict[str, str]
    _encoding: str
    _timeout: int
    _should_verify_certificate: bool
    _headless_mode: bool

    def __init__(self, path_to_config: Path) -> None:
        """
        Initialize an instance of the Config class.

        Args:
            path_to_config (pathlib.Path): Path to configuration.
        """
        self.path_to_config = path_to_config
        self.config_dto = self._extract_config_content()
        self._validate_config_content()

        self._seed_urls = self.config_dto.seed_urls
        self._num_articles = self.config_dto.total_articles
        self._headers = self.config_dto.headers
        self._encoding = self.config_dto.encoding
        self._timeout = self.config_dto.timeout
        self._should_verify_certificate = self.config_dto.should_verify_certificate
        self._headless_mode = self.config_dto.headless_mode

    def _extract_config_content(self) -> ConfigDTO:
        """
        Get config values.

        Returns:
            ConfigDTO: Config values
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as f:
            data = json.load(f)

        return ConfigDTO(**data)

    def _validate_config_content(self) -> None:
        """
        Ensure configuration parameters are not corrupt.
        """

        seed_urls = self.config_dto.seed_urls
        if not isinstance(seed_urls, list):
            raise IncorrectSeedURLError('Seed URLs must be a list of strings.')
        for url in seed_urls:
            if not isinstance(url, str) or not re.match(
                    r'https?://(www\.)?[\w\.-]+\.\w+', url):
                raise IncorrectSeedURLError('Each seed URL must be valid.')

        total = self.config_dto.total_articles
        if not isinstance(total, int) or total <= 0:
            raise IncorrectNumberOfArticlesError('Num articles must be a positive integer.')
        max_limit = NUM_ARTICLES_UPPER_LIMIT
        if total > max_limit:
            raise NumberOfArticlesOutOfRangeError('Num articles must not be too large.')

        headers = self.config_dto.headers
        if not isinstance(headers, dict):
            raise IncorrectHeadersError('Headers must be a dictionary with string keys and values.')

        encoding = self.config_dto.encoding
        if not isinstance(encoding, str):
            raise IncorrectEncodingError('Encoding must be a string.')

        timeout = self.config_dto.timeout
        if not isinstance(timeout, int) or timeout < TIMEOUT_LOWER_LIMIT \
           or timeout > TIMEOUT_UPPER_LIMIT:
            raise IncorrectTimeoutError('Timeout must be integer between 0 and 60.')

        verify = self.config_dto.should_verify_certificate
        if not isinstance(verify, bool):
            raise IncorrectVerifyError('Verify certificate must be either True or False.')

        headless = self.config_dto.headless_mode
        if not isinstance(headless, bool):
            raise IncorrectVerifyError('Headless mode must be either True or False.')

    def get_seed_urls(self) -> list[str]:
        """
        Retrieve seed urls.

        Returns:
            list[str]: Seed urls
        """
        return self._seed_urls

    def get_num_articles(self) -> int:
        """
        Retrieve total number of articles to scrape.

        Returns:
            int: Total number of articles to scrape
        """
        return self._num_articles

    def get_headers(self) -> dict[str, str]:
        """
        Retrieve headers to use during requesting.

        Returns:
            dict[str, str]: Headers
        """
        return self._headers

    def get_encoding(self) -> str:
        """
        Retrieve encoding to use during parsing.

        Returns:
            str: Encoding
        """
        return self._encoding

    def get_timeout(self) -> int:
        """
        Retrieve number of seconds to wait for response.

        Returns:
            int: Number of seconds to wait for response
        """
        return self._timeout

    def get_verify_certificate(self) -> bool:
        """
        Retrieve whether to verify certificate.

        Returns:
            bool: Whether to verify certificate or not
        """
        return self._should_verify_certificate

    def get_headless_mode(self) -> bool:
        """
        Retrieve whether to use headless mode.

        Returns:
            bool: Whether to use headless mode or not
        """
        return self._headless_mode


def make_request(url: str, config: Config) -> requests.Response:
    """
    Deliver a response from a request with given configuration.

    Args:
        url (str): Site url
        config (Config): Configuration

    Returns:
        requests.models.Response: A response from a request
    """
    try:
        resp = requests.get(
            url,
            headers=config.get_headers(),
            timeout=config.get_timeout(),
            verify=config.get_verify_certificate()
        )
    except ValueError as e:
        raise ValueError(f"Failed to fetch {url}: {e}") from e

    resp.encoding = config.get_encoding()
    time.sleep(1)
    return resp


class Crawler:
    """
    Crawler implementation.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize an instance of the Crawler class.

        Args:
            config (Config): Configuration
        """
        self.config = config
        self.urls: list[str] = []
        self.url_pattern = re.compile(r'/news-\d+-\d+\.html')

    def _extract_url(self, article_bs: BeautifulSoup) -> str:
        """
        Find and retrieve url from HTML.

        Args:
            article_bs (bs4.BeautifulSoup): BeautifulSoup instance

        Returns:
            str: Url from HTML
        """
        if article_bs.a is None:
            raise ValueError("Failed to reach the tag containing a link")
        link_text = article_bs.a["href"]
        if not isinstance(link_text, str):
            raise ValueError("The link is not a string")
        return link_text

    def find_articles(self) -> None:
        """
        Find articles.
        """
        required = self.config.get_num_articles()

        for seed_url in self.config.get_seed_urls():
            try:
                response = make_request(seed_url, self.config)
            except requests.RequestException:
                continue

            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            found = soup.find_all('a', href=self.url_pattern)
            for a_tag in found:
                href = a_tag.get('href', '').strip()
                if not href :
                    continue
                a_soup = BeautifulSoup(str(a_tag), 'html.parser')
                full_url = urljoin(seed_url, self._extract_url(a_soup))
                if full_url not in self.urls:
                    self.urls.append(full_url)
                if len(self.urls) >= required:
                    return

        # Duplicate last one
        if self.urls and len(self.urls) < required:
            last = self.urls[-1]
            while len(self.urls) < required:
                self.urls.append(last)


    def get_search_urls(self) -> list[str]:
        """
        Get seed_urls param.

        Returns:
            list: seed_urls param
        """
        return self.config.get_seed_urls()


class HTMLParser:
    """
    HTMLParser implementation.
    """

    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        """
        Initialize an instance of the HTMLParser class.

        Args:
            full_url (str): Site url
            article_id (int): Article id
            config (Config): Configuration
        """
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article = Article(url=self.full_url, article_id=self.article_id)

    def _fill_article_with_text(self, article_soup: BeautifulSoup) -> None:
        """
        Find text of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """

        all_p_blocks = article_soup.find_all('p')
        self.article.text = ' '.join(p_block.text for p_block in all_p_blocks)

    def unify_date_format(self, date_str: str) -> datetime:
        """
        Unify date format.

        Args:
            date_str (str): Date in text format

        Returns:
            datetime.datetime: Datetime object
        """
        return datetime.strptime(date_str, '%d.%m.%Y')

    def parse(self) -> Union[Article, bool]:
        """
        Parse each article.

        Returns:
            Union[Article, bool, list]: Article instance
        """
        if not isinstance(self.article.url, str):
            raise ValueError("The URL must be a string")
        try:
            response = make_request(self.article.url, self.config)
        except requests.RequestException:
            return False

        if response.status_code != 200:
            return False

        soup = BeautifulSoup(response.text, 'html.parser')

        self._fill_article_with_text(soup)
        self._fill_article_with_meta_information(soup)

        return self.article

    def _fill_article_with_meta_information(self, article_soup: BeautifulSoup) -> None:
        """
        Find meta information of article.

        Args:
            article_soup (bs4.BeautifulSoup): BeautifulSoup instance
        """
        title_tag = article_soup.find('h2')
        if title_tag:
            self.article.title = title_tag.text.strip()
        else:
            self.article.title = ''

        date_tag = article_soup.find('div', class_='mndata')
        if date_tag:
            self.article.date = self.unify_date_format(date_tag.text.strip())
        else:
            self.article.date = datetime.now()

        author_tags = article_soup.find_all('p', align='right')
        found_author = False
        for author_tag in author_tags:
            strong_tag = author_tag.find('strong')
            if not strong_tag:
                continue
            self.article.author.append(strong_tag.text.strip())
            found_author = True
        if not found_author:
            self.article.author.append('NOT FOUND')


def prepare_environment(base_path: Union[Path, str]) -> None:
    """
    Create ASSETS_PATH folder if no created and remove existing folder.

    Args:
        base_path (Union[pathlib.Path, str]): Path where articles stores
    """
    path = Path(base_path)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """
    Entrypoint for scrapper module.
    """
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()

    for idx, link in enumerate(crawler.urls[:configuration.get_num_articles()], start=1):
        parser = HTMLParser(full_url=link, article_id=idx, config=configuration)
        article = parser.parse()
        if not isinstance(article, Article):
            raise ValueError("Failed to create an article with the given data")
        io.to_raw(article)
        io.to_meta(article)


if __name__ == '__main__':
    main()

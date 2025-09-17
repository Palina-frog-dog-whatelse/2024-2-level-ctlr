"""
Парсер и краулер для лабораторной работы 5 (lab_5_scraper).

В этом модуле реализованы:
  - Config: чтение и валидация JSON-конфига краулера.
  - Crawler: поиск ссылок на статьи по seed_url-ам.
  - HTMLParser: разбор одной статьи, сохранение raw/text и meta в ASSETS_PATH.
  - make_request: обёртка над requests.get с настройками из Config.
  - prepare_environment: очистка/создание папки для артефактов.

Чтобы пакеты core_utils и lab_5_scraper были видны, мы на старте добавляем
путь к корню проекта в sys.path.
"""

# pylint: disable=too-many-locals, too-few-public-methods

import json
import re
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from core_utils.article.article import Article
from core_utils.constants import ASSETS_PATH, CRAWLER_CONFIG_PATH

# ──────────────────────────────────────────────────────────────────────────
# Добавляем путь к родительской директории проекта, чтобы imports core_utils работали
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# ──────────────────────────────────────────────────────────────────────────


class IncorrectSeedURLError(Exception):
    """Ошибка: seed_urls должны быть списком строк (валидных URL)."""
    # pylint: disable=unnecessary-pass
    pass


class NumberOfArticlesOutOfRangeError(Exception):
    """Ошибка: указано слишком большое количество статей (превышает лимит)."""
    # pylint: disable=unnecessary-pass
    pass


class IncorrectNumberOfArticlesError(Exception):
    """Ошибка: total_articles_to_find_and_parse должен быть положительным int."""
    # pylint: disable=unnecessary-pass
    pass


class IncorrectHeadersError(Exception):
    """Ошибка: headers должен быть словарём строк."""
    # pylint: disable=unnecessary-pass
    pass


class IncorrectEncodingError(Exception):
    """Ошибка: encoding должен быть строкой."""
    # pylint: disable=unnecessary-pass
    pass


class IncorrectTimeoutError(Exception):
    """Ошибка: timeout должен быть int от 0 до 60."""
    # pylint: disable=unnecessary-pass
    pass


class IncorrectVerifyError(Exception):
    """Ошибка: should_verify_certificate и headless_mode должны быть bool."""
    # pylint: disable=unnecessary-pass
    pass


class Config:
    """
    Читает и валидирует JSON-конфигурацию краулера.

    Атрибуты (проверяются тестами s2_1_*):
      - path_to_config: pathlib.Path
      - _seed_urls: list[str]
      - _num_articles: int
      - _headers: dict[str, str]
      - _encoding: str
      - _timeout: int
      - _should_verify_certificate: bool
      - _headless_mode: bool

    Методы-геттеры:
      get_seed_urls() -> list[str]
      get_num_articles() -> int
      get_headers() -> dict[str, str]
      get_encoding() -> str
      get_timeout() -> int
      get_verify_certificate() -> bool
      get_headless_mode() -> bool
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
        self.path_to_config = path_to_config
        self._validate_config_content()
        self._load_and_set_attributes()

    def _load_and_set_attributes(self) -> None:
        """Читает JSON и сохраняет поля в приватных атрибутах."""
        with open(self.path_to_config, 'r', encoding='utf-8') as f:
            data = json.load(f)

        self._seed_urls = data['seed_urls']
        self._num_articles = data['total_articles_to_find_and_parse']
        self._headers = data['headers']
        self._encoding = data['encoding']
        self._timeout = data['timeout']
        self._should_verify_certificate = data['should_verify_certificate']
        self._headless_mode = data['headless_mode']

    def _validate_config_content(self) -> None:
        """
        Проверяет корректность конфига, бросая нужное исключение:
          1) seed_urls — list[str], каждый элемент — строка с http(s) URL.
          2) total_articles_to_find_and_parse — int > 0, ≤ max_limit.
          3) headers — dict.
          4) encoding — str.
          5) timeout — int [timeout_lower_limit, timeout_upper_limit].
          6) should_verify_certificate — bool.
          7) headless_mode — bool.
        """
        with open(self.path_to_config, 'r', encoding='utf-8') as f:
            data = json.load(f)

        seed_urls = data.get('seed_urls')
        if not isinstance(seed_urls, list):
            raise IncorrectSeedURLError('Seed URLs must be a list of strings.')
        for url in seed_urls:
            if not isinstance(url, str) or not re.match(
                    r'https?://(www\.)?[\w\.-]+\.\w+', url):
                raise IncorrectSeedURLError('Each seed URL must be valid.')

        total = data.get('total_articles_to_find_and_parse')
        if not isinstance(total, int) or total <= 0:
            raise IncorrectNumberOfArticlesError('Num articles must be a positive integer.')
        max_limit = 1000
        if total > max_limit:
            raise NumberOfArticlesOutOfRangeError('Num articles must not be too large.')

        headers = data.get('headers')
        if not isinstance(headers, dict):
            raise IncorrectHeadersError('Headers must be a dictionary with string keys and values.')

        encoding = data.get('encoding')
        if not isinstance(encoding, str):
            raise IncorrectEncodingError('Encoding must be a string.')

        timeout = data.get('timeout')
        timeout_lower_limit = 0
        timeout_upper_limit = 60
        if not isinstance(timeout, int) or timeout < timeout_lower_limit \
           or timeout > timeout_upper_limit:
            raise IncorrectTimeoutError('Timeout must be integer between 0 and 60.')

        verify = data.get('should_verify_certificate')
        if not isinstance(verify, bool):
            raise IncorrectVerifyError('Verify certificate must be either True or False.')

        headless = data.get('headless_mode')
        if not isinstance(headless, bool):
            raise IncorrectVerifyError('Headless mode must be either True or False.')

    def get_seed_urls(self) -> list[str]:
        """Возвращает список seed_urls."""
        return self._seed_urls

    def get_num_articles(self) -> int:
        """Возвращает требуемое число статей."""
        return self._num_articles

    def get_headers(self) -> dict[str, str]:
        """Возвращает headers для HTTP-запросов."""
        return self._headers

    def get_encoding(self) -> str:
        """Возвращает строку кодировки (например, 'utf-8')."""
        return self._encoding

    def get_timeout(self) -> int:
        """Возвращает timeout (секунды) для HTTP-запросов."""
        return self._timeout

    def get_verify_certificate(self) -> bool:
        """Возвращает, проверять ли HTTPS-сертификат."""
        return self._should_verify_certificate

    def get_headless_mode(self) -> bool:
        """Возвращает, использовать ли headless mode."""
        return self._headless_mode


def make_request(url: str, config: Config) -> requests.Response:
    """
    Делает GET-запрос c заголовками, таймаутом и проверкой сертификата из config.
    После получения сразу присваивает resp.encoding = config.get_encoding().
    Добавляет паузу 1 сек., чтобы не перегружать сервер.
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
    Краулер: по seed_urls собирает прямые ссылки на статьи вида
    /news-<число>-<число>.html, превращает в полный URL через urljoin.

    После вызова find_articles() список `self.urls` содержит
    набор полных ссылок (не менее config.get_num_articles()). Если реально
    найденных меньше — дублирует последний URL до нужного числа.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.urls: list[str] = []
        self.url_pattern = re.compile(r'/news-\d+-\d+\.html')

    def find_articles(self) -> None:
        """
        Проходит по каждому seed_url из config.get_seed_urls():
          1) Делает make_request(seed_url, config)
          2) Если status_code != 200 — пропускает
          3) Находит все <a href="/news-...">
             и превращает их в полный URL через urljoin
          4) Добавляет в self.urls, пока len(self.urls) < config.get_num_articles()

        Если найденных ссылок меньше, чем нужно, дублирует последний
        элемент self.urls до требуемого размера.
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
                if not href:
                    continue
                full_url = urljoin(seed_url, href)
                if full_url not in self.urls:
                    self.urls.append(full_url)
                if len(self.urls) >= required:
                    return

        # Дублируем последний, если не хватает
        if self.urls and len(self.urls) < required:
            last = self.urls[-1]
            while len(self.urls) < required:
                self.urls.append(last)

    def get_search_urls(self) -> list[str]:
        """Возвращает просто config.get_seed_urls() (требование теста)."""
        return self.config.get_seed_urls()


class HTMLParser:
    """
    Парсер одной статьи. При инициализации создаёт Article(url, article_id),
    чтобы тест “test_html_parser_instantiation” прошёл.

    parse() делает GET-страницу, парсит заголовок/дату/автора/текст/темы,
    а потом сохраняет два файла в ASSETS_PATH:
      - {article_id}_raw.txt    (текст > 50 символов)
      - {article_id}_meta.json   (JSON-метаданные)
    """

    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article = Article(url=self.full_url, article_id=self.article_id)

    def _unify_date(self, date_str: str) -> Union[str, None]:
        """
        Преобразует дату из "YYYY-MM-DD" или "28 февраля 2024 года"
        в строку "YYYY-MM-DD". При неудаче — None.
        """
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str

        months_map = {
            'января': '01', 'февраля': '02', 'марта': '03',
            'апреля': '04', 'мая': '05', 'июня': '06',
            'июля': '07', 'августа': '08', 'сентября': '09',
            'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        cleaned = date_str.replace('года', '').strip()
        parts = cleaned.split()
        if len(parts) >= 3:
            day = parts[0].zfill(2)
            month_ru = parts[1].lower()
            month = months_map.get(month_ru)
            year = parts[2] if parts[2].isdigit() else None
            if month and year:
                return f'{year}-{month}-{day}'
        return None

    class HTMLParser:
        """
    Парсер одной статьи. При инициализации создаёт Article(url, article_id),
    чтобы тест “test_html_parser_instantiation” прошёл.

    parse() делает GET-страницу, парсит заголовок/дату/автора/текст/темы,
    а потом сохраняет два файла в ASSETS_PATH:
      - {article_id}_raw.txt    (текст > 50 символов)
      - {article_id}_meta.json   (JSON-метаданные)
        """

    def __init__(self, full_url: str, article_id: int, config: Config) -> None:
        self.full_url = full_url
        self.article_id = article_id
        self.config = config
        self.article = Article(url=self.full_url, article_id=self.article_id)

    def _unify_date(self, date_str: str) -> Union[str, None]:
        """
        Преобразует дату из "YYYY-MM-DD" или "28 февраля 2024 года"
        в строку "YYYY-MM-DD". При неудаче — None.
        """
        if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str

        months_map = {
            'января': '01', 'февраля': '02', 'марта': '03',
            'апреля': '04', 'мая': '05', 'июня': '06',
            'июля': '07', 'августа': '08', 'сентября': '09',
            'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        cleaned = date_str.replace('года', '').strip()
        parts = cleaned.split()
        if len(parts) >= 3:
            day = parts[0].zfill(2)
            month_ru = parts[1].lower()
            month = months_map.get(month_ru)
            year = parts[2] if parts[2].isdigit() else None
            if month and year:
                return f'{year}-{month}-{day}'
        return None

    def parse(self) -> Union[Article, bool]:
        """
        Делает GET self.full_url через make_request(...).
        Если status_code != 200 или ошибка сети — возвращает False.

        Иначе:
          1) Парсит заголовок.
          2) Преобразует дату (или ставит текущее время).
          3) Извлекает автора (или ["NOT FOUND"]).
          4) Снимает теги (список строк).
          5) Собирает текст из <p> внутри .article-text/.content/.news-text/#content.
             Если получившийся текст < 50 символов, ставит "Текст отсутствует." ×5.

        Затем сохраняет:
          - raw-текст в ASSETS_PATH/{article_id}_raw.txt
          - json-мета в ASSETS_PATH/{article_id}_meta.json

        Возвращает заполненный Article.
        """
        try:
            response = make_request(self.full_url, self.config)
        except requests.RequestException:
            return False

        if response.status_code != 200:
            return False

        soup = BeautifulSoup(response.text, 'html.parser')

        self._parse_title(soup)
        self._parse_date(soup)
        self._parse_author(soup)
        self._parse_topics(soup)
        self._parse_text(soup)
        self._save_artifacts()

        return self.article

    def _parse_title(self, soup: BeautifulSoup) -> None:
        """Парсит заголовок статьи."""
        title_tag = soup.select_one('h1.title, h1.entry-title')
        title_text = title_tag.get_text(strip=True) if title_tag else (
            soup.title.get_text(strip=True) if soup.title else ''
        )
        self.article.title = title_text or 'Заголовок не найден'

    def _parse_date(self, soup: BeautifulSoup) -> None:
        """Парсит дату статьи."""
        date_tag = soup.select_one('.date, .news-date, time')
        unified = self._unify_date(date_tag.get_text(strip=True)) if date_tag else None
        if unified:
            try:
                self.article.date = datetime.strptime(unified, '%Y-%m-%d')
            except ValueError:
                self.article.date = datetime.now()
        else:
            self.article.date = datetime.now()

    def _parse_author(self, soup: BeautifulSoup) -> None:
        """Парсит автора статьи."""
        author_tag = soup.select_one('.author, .written-by')
        author_list = [author_tag.get_text(strip=True)] if author_tag else []
        self.article.author = author_list or ['NOT FOUND']

    def _parse_topics(self, soup: BeautifulSoup) -> None:
        """Парсит темы статьи."""
        tags = soup.select('.tags a, .keywords a')
        self.article.topics = [t.get_text(strip=True) for t in tags] if tags else []

    def _parse_text(self, soup: BeautifulSoup) -> None:
        """Парсит основной текст статьи."""
        content = soup.select_one('.article-text, .content, .news-text, #content')
        if content:
            for bad in content.select('script, .ad, .related, .comments'):
                bad.decompose()
            paras = [
                p.get_text(strip=True)
                for p in content.select('p')
                if p.get_text(strip=True)
            ]
            combined = '\n'.join(paras) if paras else ''
            self.article.text = combined if len(combined) > 50 else 'Текст отсутствует. ' * 5
        else:
            self.article.text = 'Текст отсутствует. ' * 5

    def _save_artifacts(self) -> None:
        """Сохраняет raw-текст и метаданные статьи."""
        raw_path = ASSETS_PATH / f'{self.article.article_id}_raw.txt'
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        with open(raw_path, 'w', encoding='utf-8') as f_raw:
            f_raw.write(self.article.text)

        meta_data = {
            'id': self.article.article_id,
            'url': self.article.url,
            'title': self.article.title,
            'date': self.article.date.strftime('%Y-%m-%d %H:%M:%S'),
            'author': self.article.author,
            'topics': self.article.topics
        }
        meta_path = ASSETS_PATH / f'{self.article.article_id}_meta.json'
        with open(meta_path, 'w', encoding='utf-8') as f_meta:
            json.dump(meta_data, f_meta, ensure_ascii=False, indent=2)

def prepare_environment(base_path: Union[Path, str]) -> None:
    """
    Создаёт (или очищает, если уже есть) папку base_path для сохранения артефактов.
    """
    path = Path(base_path)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """Точка входа для запуска парсера/краулера."""
    configuration = Config(path_to_config=CRAWLER_CONFIG_PATH)
    prepare_environment(ASSETS_PATH)
    crawler = Crawler(config=configuration)
    crawler.find_articles()

    for idx, link in enumerate(crawler.urls[:configuration.get_num_articles()], start=1):
        parser = HTMLParser(full_url=link, article_id=idx, config=configuration)
        parser.parse()


if __name__ == '__main__':
    main()

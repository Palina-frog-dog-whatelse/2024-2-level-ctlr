# pylint: disable=too-few-public-methods, disable=too-many-arguments
"""
ConfigDTO class implementation: stores the configuration information.
"""


class ConfigDTO:
    """
    Type annotations for configurations.
    """

    #: List of seed urls
    seed_urls: list[str]

    #: Number of total articles
    total_articles: int

    #: Headers
    headers: dict[str, str]

    #: Encoding
    encoding: str

    #: Number of seconds to wait for response
    timeout: int

    #: Should verify certificate or not
    should_verify_certificate: bool

    #: Require headless mode or not
    headless_mode: bool

    def __init__(
        self,
        seed_urls: list,
        headers: dict,
        total_articles_to_find_and_parse: int,
        encoding: str,
        timeout: int,
        should_verify_certificate: bool,
        headless_mode: bool
    ):
        self.seed_urls = seed_urls
        self.total_articles = total_articles_to_find_and_parse
        self.headers = headers
        self.encoding = encoding
        self.timeout = timeout
        self.should_verify_certificate = should_verify_certificate
        self.headless_mode = headless_mode
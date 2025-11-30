import requests
from . import classproperty


class Network:
    @classproperty
    def session(cls) -> requests.Session:
        return requests.Session()

    @classmethod
    def fetch_url(cls, url: str, params: dict = {}, timeout: int = 10) -> str | None:
        try:
            response = cls.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException:
            return None
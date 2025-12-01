import requests
import time
import json
from tenacity import retry, wait_fixed, stop_after_attempt, retry_if_exception_type, wait_exponential
from . import classproperty


class Network:
    @classproperty
    def session(cls) -> requests.Session:
        return requests.Session()

    @classmethod
    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=5),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((requests.Timeout, requests.HTTPError, requests.ConnectionError)))
    def fetch_url(cls, url: str, params: dict = {}, timeout: int = 10) -> str | None:
        response = cls.session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.text


class EmRequest():
    def __init__(self) -> None:
        pass

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=5),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type((requests.Timeout, requests.HTTPError, requests.ConnectionError)))
    def getRequest(self, headers=None):
        rsp = Network.session.get(self.getUrl(), headers=headers)
        rsp.raise_for_status()
        return rsp.text

    def getUrl(self):
        pass

    async def getNext(self, params=None, proxies=None):
        pass

    def saveFetched(self):
        pass


class EmDataCenterRequest(EmRequest):
    def __init__(self):
        super().__init__()
        self.page = 1
        self.pageSize = 50
        self.fecthed = []
        self.headers = {
            'Host': 'datacenter.eastmoney.com',
            'Referer': 'https://data.eastmoney.com/yjfp/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0',
            'Accept': '/',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }

    def setFilter(self, filter):
        self._filter = filter

    def getUrl(self):
        pass

    async def getNext(self, headers=None):
        dcresponse = json.loads(self.getRequest(headers))
        if not dcresponse['success']:
            print('EmDataCenterRequest getUrl', self.getUrl())
            print('EmDataCenterRequest Error, message', dcresponse['message'], 'code', dcresponse['code'])
            return

        if (dcresponse['result'] and dcresponse['result']['data']):
            self.fecthed += dcresponse['result']['data']

        if (dcresponse['result']['pages'] == self.page):
            await self.saveFecthed()

        if (dcresponse['result']['pages'] > self.page):
            self.page += 1
            await self.getNext(headers)

    async def saveFecthed(self):
        print(self.fecthed)

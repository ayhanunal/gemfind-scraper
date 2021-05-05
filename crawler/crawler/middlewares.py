import random
from scrapy import signals
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.selector import Selector
from scrapy import Request
from scrapy.utils.response import get_meta_refresh
from collections import defaultdict
from scrapy.downloadermiddlewares.cookies import CookiesMiddleware
from scrapy.http.cookies import CookieJar
import time
import re
import os
import base64, random


class CustomCookiesMiddleware(CookiesMiddleware):
    """Same as Scrapy cookies middleware but adds a purge cookies feature
       on the request
    """

    def process_request(self, request, spider):
        if request.meta.get('dont_merge_cookies', False):
            return

        if request.meta.get('purge_cookies', False):
            # Delete the cookiejars
            self.jars = defaultdict(CookieJar)
            return

        cookiejarkey = request.meta.get("cookiejar")
        jar = self.jars[cookiejarkey]
        cookies = self._get_request_cookies(jar, request)
        for cookie in cookies:
            jar.set_cookie_if_ok(cookie, request)

        # set Cookie header
        request.headers.pop('Cookie', None)
        jar.add_cookie_header(request)
        self._debug_cookie(request, spider)


class RandomUserAgentMiddleware(UserAgentMiddleware):
    def __init__(self, settings, user_agent='Scrapy'):
        super(RandomUserAgentMiddleware, self).__init__()
        self.user_agent = user_agent
        self.user_agent_list = settings.get('USER_AGENT_LIST')
        self.mobile_user_agent_list = settings.get('MOBILE_USER_AGENT_LIST')

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        crawler.signals.connect(obj.spider_opened,
                                signal=signals.spider_opened)
        return obj

    def process_request(self, request, spider):
        mobile = None
        if "mobile" in request.meta:
            mobile = request.meta["mobile"]
        user_agent = None
        if not mobile:
            user_agent = random.choice(self.user_agent_list)
        else:
            user_agent = random.choice(self.mobile_user_agent_list)
            
        if user_agent:
            request.headers.setdefault('User-Agent', user_agent)
            
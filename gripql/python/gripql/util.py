from __future__ import absolute_import, print_function, unicode_literals

import os
import json
import requests
import requests.auth

from datetime import datetime
from requests.compat import urlparse, urlunparse


class BaseConnection(object):
    def __init__(self, url, user=None, password=None, token=None, credential_file=None):
        if url is None:
            url = os.getenv("GRIP_URL", None)
        url = process_url(url)
        self.base_url = url
        if user is None:
            user = os.getenv("GRIP_USER", None)
        self.user = user
        if password is None:
            password = os.getenv("GRIP_PASSWORD", None)
        self.password = password
        if token is None:
            token = os.getenv("GRIP_TOKEN", None)
        self.token = token
        if credential_file is None:
            credential_file = os.getenv("GRIP_CREDENTIAL_FILE", None)
        self.credential_file = credential_file

        self.session = Session()
        _ = self.session.headers.update(self._request_header())

    def _request_header(self):
        if self.token:
            header = {'Content-type': 'application/json',
                      'Authorization': 'Bearer ' + self.token}
        elif self.user and self.password:
            header = {'Content-type': 'application/json',
                      'Authorization': requests.auth._basic_auth_str(self.user, self.password)}
        elif self.credential_file:
            with open(self.credential_file, 'rt') as f:
                header = json.load(f)
                header['Content-type'] = 'application/json'
                if 'OauthExpires' in header:
                    header['OauthExpires'] = str(header['OauthExpires'])
        else:
            header = {'Content-type': 'application/json'}
        return header


class Rate:
    def __init__(self, logger, report_every=1000):
        self.i = 0
        self.start = None
        self.first = None
        self.report_every = report_every
        self.logger = logger

    def init(self):
        self.start = datetime.now()

    def close(self):
        if self.i == 0:
            return

        now = datetime.now()
        dt = now - self.start
        rate = self.i / dt.total_seconds()
        m = "rate: {0:,} results received ({1:,d}/sec)".format(
            self.i,
            int(rate)
        )
        self.logger.debug(m)
        m = "{0:,} results received in {1:,d} seconds".format(
            self.i,
            int(dt.total_seconds())
        )
        self.logger.info(m)

    def log(self):
        if self.i == 0:
            return

        dt = datetime.now() - self.current
        self.current = datetime.now()
        rate = self.report_every / dt.total_seconds()
        m = "rate: {0:,} results received ({1:,d}/sec)".format(
            self.i,
            int(rate)
        )
        self.logger.debug(m)

    def tick(self):
        if self.start is None:
            raise RuntimeError("call Rate.init() before the first tick")

        if self.i == 0:
            now = datetime.now()
            self.current = now
            dt = now - self.start
            m = "first result received after {0:,d} seconds".format(
                int(dt.total_seconds())
            )
            self.logger.debug(m)

        self.i += 1

        if self.i % self.report_every == 0:
            self.log()


def process_url(url):
    scheme, netloc, path, params, query, frag = urlparse(url)
    query = ""
    frag = ""
    params = ""
    if scheme == "":
        scheme = "http"
    if netloc == "" and path != "":
        netloc = path.split("/")[0]
        path = ""
    return urlunparse((scheme, netloc, path, params, query, frag))


def raise_for_status(response):
    http_error_msg = ''
    if isinstance(response.reason, bytes):
        # We attempt to decode utf-8 first because some servers
        # choose to localize their reason strings. If the string
        # isn't utf-8, we fall back to iso-8859-1 for all other
        # encodings. (See PR #3538)
        try:
            reason = response.reason.decode('utf-8')
        except UnicodeDecodeError:
            reason = response.reason.decode('iso-8859-1')
    else:
        reason = response.reason

    rsp_body = ''
    try:
        rsp_body = response.json()['error']['message']
    except Exception:
        rsp_body = response.text

    if response.status_code == 302 and 'oauth2' in response.headers.get('Location', None):
        http_error_msg = '%s Client Error: OAuth2 redirect for url: %s redirect url: %s' % (
            response.status_code, response.url, response.headers.get('Location', None)
        )

    elif 400 <= response.status_code < 500:
        http_error_msg = '%s Client Error: %s for url: % response: %s' % (
            response.status_code, reason, response.url, rsp_body
        )

    elif 500 <= response.status_code < 600:
        http_error_msg = '%s Server Error: %s for url: %s response: %s' % (
            response.status_code, reason, response.url, rsp_body
        )

    if http_error_msg:
        raise requests.HTTPError(http_error_msg, response=response)


class Session(requests.Session):
    def __init__(self):
        super(Session, self).__init__()

    def get(self, url, **kwargs):
        kwargs.setdefault('allow_redirects', False)
        return self.request('GET', url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        kwargs.setdefault('allow_redirects', False)
        return self.request('POST', url, data=data, json=json, **kwargs)

    def delete(self, url, **kwargs):
        kwargs.setdefault('allow_redirects', False)
        return self.request('DELETE', url, **kwargs)

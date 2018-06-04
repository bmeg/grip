from __future__ import absolute_import, print_function, unicode_literals

from requests import HTTPError
from requests.compat import urlparse, urlunparse


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

    if 400 <= response.status_code < 500:
        http_error_msg = '%s Client Error: %s for url: % response: %s' % (
            response.status_code, reason,
            response.url, rsp_body
        )

    elif 500 <= response.status_code < 600:
        http_error_msg = '%s Server Error: %s for url: %s response: %s' % (
            response.status_code, reason,
            response.url, rsp_body
        )

    if http_error_msg:
        raise HTTPError(http_error_msg, response=response)

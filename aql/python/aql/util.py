from __future__ import absolute_import, print_function, unicode_literals

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

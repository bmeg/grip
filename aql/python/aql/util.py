from __future__ import absolute_import, print_function, unicode_literals

import urllib2


def do_request(request, raise_exceptions=True):
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError as e:
        e = urllib2.HTTPError(
            request.get_full_url(),
            e.code,
            e.msg + ": " + e.read(),
            e.hdrs,
            e.fp
        )
        if raise_exceptions:
            raise e
        else:
            print(e)

    return response

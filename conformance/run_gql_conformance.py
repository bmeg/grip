"""Run tests starting with the 'gql_' prefix."""

from __future__ import absolute_import, print_function, unicode_literals

import json
import sys

from run_util import Manager, create_arg_parser, create_connection, filter_tests


class GQLManager(Manager):
    """Manager that executes GQL queries through the HTTP endpoint."""

    def run_gql(self, query):
        graph = self.readOnly if self.readOnly is not None else self.curGraph
        url = "%s/gql/%s" % (self._conn.base_url.rstrip("/"), graph)
        response = self._conn.session.post(url, json={"query": query}, timeout=30)
        response.raise_for_status()
        return [
            json.loads(line)
            for line in response.text.splitlines()
            if line.strip()
        ]


if __name__ == "__main__":
    args = create_arg_parser()
    tests = filter_tests(args, prefix="gql_")
    conn = create_connection(args.server, args.user, args.password)
    manager = GQLManager(conn, args.readOnly, server=args.server)
    correct, total = manager.run_tests(tests, args)

    print("Passed %s out of %s" % (correct, total))
    if correct != total:
        sys.exit(1)
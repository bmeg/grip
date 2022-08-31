"""Run tests starting with 'ot_' prefix"""

from __future__ import absolute_import, print_function, unicode_literals
import sys
from run_util import create_arg_parser, Manager, gripql, filter_tests, create_connection

if __name__ == "__main__":
    print("Running Conformance with %s" % gripql.__file__)

    args = create_arg_parser()
    
    # returns test modules starting with "ot_"
    tests = filter_tests(args, prefix="ot_")

    # connect to server
    conn = create_connection(args.server, args.user, args.password)

    # pass connection to manager and run tests
    manager = Manager(conn, args.readOnly)
    correct, total = manager.run_tests(tests, args)

    # return non zero status if error
    print("Passed %s out of %s" % (correct, total))
    if correct != total:
        sys.exit(1)

"""Run tests starting with 'auth_' prefix"""

from __future__ import absolute_import, print_function, unicode_literals
import sys
from run_util import create_arg_parser, Manager, gripql, filter_tests, create_connection

if __name__ == "__main__":
    print("Running Conformance with %s" % gripql.__file__)

    args = create_arg_parser()

    # returns test modules starting with "auth_"
    tests = filter_tests(args, prefix="auth_")

    # pass connection to manager, load policies, and run tests
    manager = Manager(None, args.readOnly, server=args.server, grip_config_file_path=args.grip_config_file_path)
    assert len(manager.policies) > 0, "Could not load policies"
    assert len(manager.accounts) > 0, "Could not create account from grip_config_file_path"

    # find admin user, create graphs
    admin = next(iter(account for account in manager.accounts if account.is_admin), None)
    conn = create_connection(args.server, admin.user, admin.password)
    manager.set_connection(conn)
    manager.create_graphs_from_policies()

    manager.set_connection(None)
    correct, total = manager.run_tests(tests, args)

    # return non zero status if error
    print("Passed %s out of %s" % (correct, total))
    if correct != total:
        sys.exit(1)

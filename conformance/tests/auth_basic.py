from __future__ import absolute_import

import requests


def test_current_user_has_policy(manager):
    """Ensure current user has a policy defined."""

    current_user_policy = next(iter([policy for policy in manager.policies if policy.sub == manager.user]), None)
    if not current_user_policy:
        return [f"{manager.user} not found in polices {[policy.sub for policy in manager.policies]}"]
    return []


def test_current_user_can_query(manager):
    """Ensure current user can query."""

    account = manager.current_user_account()
    assert account, f"Could not find account for {manager.user}"
    policies = account.policies
    assert len(policies) > 0, f"Should have at least one policy"
    errors = []

    if not account.is_admin:
        # non admin user

        users = {}
        for policy in policies:
            if policy.sub not in users:
                users[policy.sub] = {}
            if policy.obj not in users[policy.sub]:
                users[policy.sub][policy.obj] = [policy.act]
            else:
                users[policy.sub][policy.obj].append(policy.act)

        for graph_name in users[manager.user]:
            if 'query' in users[manager.user][graph_name]:
                manager.test_query(graph_name)
            else:
                try:
                    manager.test_query(graph_name)
                    errors.append(f"{manager.user} should not be able to query {graph_name} graph")
                except AssertionError:
                    pass
    else:
        # admin user
        for graph_name in manager.all_graph_names:
            assert graph_name != '*'
            manager.test_query(graph_name)
    return errors


def test_current_user_can_read(manager):
    """Ensure current user can read."""

    account = manager.current_user_account()
    assert account, f"Could not find account for {manager.user}"
    policies = account.policies
    assert len(policies) > 0, f"Should have at least one policy"
    errors = []
    # non admin user
    if not account.is_admin:
        users = {}
        for policy in policies:
            if policy.sub not in users:
                users[policy.sub] = {}
            if policy.obj not in users[policy.sub]:
                users[policy.sub][policy.obj] = [policy.act]
            else:
                users[policy.sub][policy.obj].append(policy.act)

        for graph_name in users[manager.user]:
            if 'read' in users[manager.user][graph_name]:
                manager.test_read(graph_name)
            else:
                try:
                    manager.test_read(graph_name)
                    errors.append(f"{manager.user} should not be able to read {graph_name} graph")
                except AssertionError:
                    pass
    else:
        # admin user
        for graph_name in manager.all_graph_names:
            manager.test_read(graph_name)

    return errors


def test_current_user_can_write(manager):
    """Ensure current user can write."""

    account = manager.current_user_account()
    assert account, f"Could not find account for {manager.user}"
    policies = account.policies
    assert len(policies) > 0, f"Should have at least one policy"
    errors = []
    # non admin user
    if not account.is_admin:
        for policy in policies:
            graph_name = policy.obj
            if policy.act == 'write':
                manager.test_write(graph_name)
            else:
                try:
                    manager.test_write(graph_name)
                    errors.append(f"{manager.user} should not be able to write {graph_name} graph")
                except AssertionError:
                    pass
        try:
            manager.test_write('dummy')
            errors.append(f"{manager.user} should not be able to write dummy graph")
        except AssertionError as e:
            pass

    else:
        # admin user
        for graph_name in manager.all_graph_names:
            manager.test_write(graph_name)
    return errors

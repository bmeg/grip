from __future__ import absolute_import


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
    print(f"policies {manager.user} for {policies}")
    assert len(policies) > 0, f"Should have at least one policy"
    graph_names = set(policy.obj for policy in policies if policy.obj != '*')
    errors = []
    # non admin user
    if len(graph_names) > 0:
        for graph_name in graph_names:
            manager.test_query(graph_name)
            print(f"{manager.user} query {graph_name}")

        try:
            manager.test_query('dummy')
            errors.append(f"{manager.user} should not be able to query dummy graph")
        except Exception as e:
            pass
    else:
        # admin user
        assert account.is_admin, f"{manager.user} should be admin since all objs where '*'"
        for graph_name in manager.all_graph_names:
            manager.test_query(graph_name)
            print(f"{manager.user}, an admin query {graph_name}")
    return errors


def test_current_user_can_read(manager):
    """Ensure current user can read."""

    account = manager.current_user_account()
    assert account, f"Could not find account for {manager.user}"
    policies = account.policies
    print(f"policies {manager.user} for {policies}")
    assert len(policies) > 0, f"Should have at least one policy"
    graph_names = set(policy.obj for policy in policies if policy.obj != '*')
    errors = []
    # non admin user
    if len(graph_names) > 0:
        for graph_name in graph_names:
            manager.test_read(graph_name)
            print(f"{manager.user} query {graph_name}")
        try:
            manager.test_read('dummy')
            errors.append(f"{manager.user} should not be able to read dummy graph")
        except Exception as e:
            pass
    else:
        # admin user
        assert account.is_admin, f"{manager.user} should be admin since all objs where '*'"
        for graph_name in manager.all_graph_names:
            manager.test_read(graph_name)
            print(f"{manager.user}, an admin read {graph_name}")
    return errors


def test_current_user_can_write(manager):
    """Ensure current user can read."""

    account = manager.current_user_account()
    assert account, f"Could not find account for {manager.user}"
    policies = account.policies
    print(f"policies {manager.user} for {policies}")
    assert len(policies) > 0, f"Should have at least one policy"
    graph_names = set(policy.obj for policy in policies if policy.obj != '*')
    errors = []
    # non admin user
    if len(graph_names) > 0:
        for graph_name in graph_names:
            manager.test_read(graph_name)
            print(f"{manager.user} query {graph_name}")
        try:
            manager.test_write('dummy')
            errors.append(f"{manager.user} should not be able to write dummy graph")
        except Exception as e:
            pass
    else:
        # admin user
        assert account.is_admin, f"{manager.user} should be admin since all objs where '*'"
        for graph_name in manager.all_graph_names:
            manager.test_write(graph_name)
            print(f"{manager.user}, an admin write {graph_name}")
    return errors

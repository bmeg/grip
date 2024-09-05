import json
import pathlib
import types
from collections import defaultdict

import pytest

import pygrip
from jsonpath_ng import jsonpath, parse

from typing import Generator, Dict, Any


def resources() -> Generator[Dict[str, Any], None, None]:
    """Read a directory of ndjson files, return dictionary for each line."""
    base = pathlib.Path(__file__).parent.absolute()
    fixture_path = pathlib.Path(base / 'fixtures' / 'fhir-compbio-examples' / 'META')
    assert fixture_path.exists(), f"Fixture path {fixture_path.absolute()} does not exist."
    for file in fixture_path.glob('*.ndjson'):
        with open(str(file)) as fp:
            for l_ in fp.readlines():
                yield json.loads(l_)


@pytest.fixture
def expected_edges() -> list[tuple]:
    """Return the expected edges for the resources [(src, dst, label)]."""
    return [('21f3411d-89a4-4bcc-9ce7-b76edb1c745f', '60c67a06-ea2d-4d24-9249-418dc77a16a9', 'specimen'),
            ('21f3411d-89a4-4bcc-9ce7-b76edb1c745f', '9ae7e542-767f-4b03-a854-7ceed17152cb', 'focus'),
            ('21f3411d-89a4-4bcc-9ce7-b76edb1c745f', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920', 'subject'),
            ('2fc448d6-a23b-4b94-974b-c66110164851', '7dacd4d0-3c8e-470b-bf61-103891627d45', 'study'),
            ('2fc448d6-a23b-4b94-974b-c66110164851', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920', 'subject'),
            ('4e3c6b59-b1fd-5c26-a611-da4cde9fd061', '60c67a06-ea2d-4d24-9249-418dc77a16a9', 'focus'),
            ('4e3c6b59-b1fd-5c26-a611-da4cde9fd061', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920', 'subject'),
            ('60c67a06-ea2d-4d24-9249-418dc77a16a9', '89c8dc4c-2d9c-48c7-8862-241a49a78f14', 'collection_collector'),
            ('60c67a06-ea2d-4d24-9249-418dc77a16a9', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920', 'subject'),
            ('9ae7e542-767f-4b03-a854-7ceed17152cb', '60c67a06-ea2d-4d24-9249-418dc77a16a9', 'subject'),
            ('cec32723-9ede-5f24-ba63-63cb8c6a02cf', '60c67a06-ea2d-4d24-9249-418dc77a16a9', 'specimen'),
            ('cec32723-9ede-5f24-ba63-63cb8c6a02cf', '9ae7e542-767f-4b03-a854-7ceed17152cb', 'focus'),
            ('cec32723-9ede-5f24-ba63-63cb8c6a02cf', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920', 'subject')]


@pytest.fixture
def expected_vertices() -> list[tuple]:
    """Return the expected vertices [(id, label)] for the resources."""
    return [('21f3411d-89a4-4bcc-9ce7-b76edb1c745f', 'Observation'),
            ('2fc448d6-a23b-4b94-974b-c66110164851', 'ResearchSubject'),
            ('4e3c6b59-b1fd-5c26-a611-da4cde9fd061', 'Observation'),
            ('60c67a06-ea2d-4d24-9249-418dc77a16a9', 'Specimen'),
            ('7dacd4d0-3c8e-470b-bf61-103891627d45', 'ResearchStudy'),
            ('89c8dc4c-2d9c-48c7-8862-241a49a78f14', 'Organization'),
            ('9ae7e542-767f-4b03-a854-7ceed17152cb', 'DocumentReference'),
            ('bc4e1aa6-cb52-40e9-8f20-594d9c84f920', 'Patient'),
            ('cec32723-9ede-5f24-ba63-63cb8c6a02cf', 'Observation')]


@pytest.fixture
def expected_dataframe_associations():
    """Return the expected dataframe associations for the resources. { (resource_type, resource_id): [(association_resource_type, association_resource_id)]."""
    return {
        ('ResearchSubject', '2fc448d6-a23b-4b94-974b-c66110164851'): [
            ('ResearchStudy', '7dacd4d0-3c8e-470b-bf61-103891627d45'),
            ('Patient', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920'),
            ('Specimen', '60c67a06-ea2d-4d24-9249-418dc77a16a9')],
        ('Specimen', '60c67a06-ea2d-4d24-9249-418dc77a16a9'): [
            ('ResearchStudy', '7dacd4d0-3c8e-470b-bf61-103891627d45'),
            ('ResearchSubject', '2fc448d6-a23b-4b94-974b-c66110164851'),
            ('Patient', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920'),
            ('Observation', '4e3c6b59-b1fd-5c26-a611-da4cde9fd061')],
        ('ResearchStudy', '7dacd4d0-3c8e-470b-bf61-103891627d45'): [
            ('ResearchSubject', '2fc448d6-a23b-4b94-974b-c66110164851')],
        ('Organization', '89c8dc4c-2d9c-48c7-8862-241a49a78f14'): [
            ('ResearchStudy', '7dacd4d0-3c8e-470b-bf61-103891627d45'),
            ('ResearchSubject', '2fc448d6-a23b-4b94-974b-c66110164851'),
            ('Patient', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920'),
            ('Specimen', '60c67a06-ea2d-4d24-9249-418dc77a16a9'),
            ('DocumentReference', '9ae7e542-767f-4b03-a854-7ceed17152cb')],
        ('DocumentReference', '9ae7e542-767f-4b03-a854-7ceed17152cb'): [
            ('ResearchStudy', '7dacd4d0-3c8e-470b-bf61-103891627d45'),
            ('ResearchSubject', '2fc448d6-a23b-4b94-974b-c66110164851'),
            ('Patient', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920'),
            ('Specimen', '60c67a06-ea2d-4d24-9249-418dc77a16a9'),
            ('Observation', '21f3411d-89a4-4bcc-9ce7-b76edb1c745f'),
            ('Observation', 'cec32723-9ede-5f24-ba63-63cb8c6a02cf')],
        ('Patient', 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920'): [
            ('ResearchStudy', '7dacd4d0-3c8e-470b-bf61-103891627d45'),
            ('ResearchSubject', '2fc448d6-a23b-4b94-974b-c66110164851'),
            ('Specimen', '60c67a06-ea2d-4d24-9249-418dc77a16a9'),
            ('Observation', '21f3411d-89a4-4bcc-9ce7-b76edb1c745f'),
            ('Observation', '4e3c6b59-b1fd-5c26-a611-da4cde9fd061'),
            ('Observation', 'cec32723-9ede-5f24-ba63-63cb8c6a02cf')]
    }


def match_label(self, vertex_gid, label, seen_already=None) -> dict:
    """Recursively find the first vertex of a given label, starting traversals from vertex_gid."""

    # check params
    assert vertex_gid is not None, "Expected vertex_gid to be not None."
    assert label is not None, "Expected label to be not None."
    # mutable default arguments are evil
    # See https://florimond.dev/en/posts/2018/08/python-mutable-defaults-are-the-source-of-all-evil
    if seen_already is None:
        seen_already = []

    # get all edges for vertex
    q = self.V(vertex_gid).both()

    # get all vertices for edges
    # TODO - consider if this should be a vertices_of_label() -> generator[dict] instead
    for _ in q:
        if _['vertex']['label'] == label:
            return _
        else:
            if _['vertex']['gid'] in seen_already:
                continue
            seen_already.append(_['vertex']['gid'])
            return self.match_label(_['vertex']['gid'], label, seen_already=seen_already)


def dataframe_associations(self, vertex_gid, vertex_label, labels=('ResearchStudy', 'ResearchSubject', 'Patient', 'Specimen', 'DocumentReference', 'Observation')) -> list[dict]:
    """Return all objects associated with vertex_gid."""
    associations = []
    for label in labels:
        if label == 'Observation':
            continue
        if vertex_label == label:
            continue
        _ = self.match_label(vertex_gid, label)
        if _ is not None:
            associations.append(_['vertex']['data'])
    if 'Observation' in labels:
        q = self.V(vertex_gid).in_(["focus", "subject"]).hasLabel("Observation")
        for _ in q:
            associations.append(_['vertex']['data'])
    return associations


@pytest.fixture
def graph() -> pygrip.GraphDBWrapper:
    """Load the resources into the graph. Note: this does _not_ consider iceberg schema."""
    # TODO - add parameter or test environment variable to switch between in-memory and remote graph
    graph = pygrip.NewMemServer()
    # use jsonpath to find all references with a resource
    jsonpath_expr = parse('*..reference')
    for _ in resources():
        graph.addVertex(_['id'], _['resourceType'], _)
        for match in jsonpath_expr.find(_):
            # value will be something like "Specimen/60c67a06-ea2d-4d24-9249-418dc77a16a9"
            # full_path will be something like "specimen.reference" or "focus.[0].reference"
            type_, dst_id = match.value.split('/')
            # determine label from full path
            path_parts = str(match.full_path).split('.')
            # strip out array indices and reference
            path_parts = [part for part in path_parts if '[' not in part and part != 'reference']
            # make it a label
            label = '_'.join(path_parts)
            graph.addEdge(_['id'], dst_id, label)

    # monkey patch the graph object with our methods
    # TODO - consider a more formal subclass of pygrip.GraphDBWrapper
    graph.match_label = types.MethodType(match_label, graph)
    graph.dataframe_associations = types.MethodType(dataframe_associations, graph)

    yield graph


def test_graph_vertices(graph, expected_vertices):
    """Test the graph vertices."""

    actual_vertices = []
    for _ in graph.V():
        assert 'vertex' in _, f"Expected 'vertex' in {_}"
        vertex = _['vertex']
        assert 'data' in vertex, f"Expected 'data' in {vertex}"
        assert 'gid' in vertex, f"Expected 'gid' in {vertex}"
        assert 'label' in vertex, f"Expected 'label' in {vertex}"
        assert 'data' in vertex, f"Expected 'data' in {vertex}"
        resource = _['vertex']['data']
        actual_vertices.append((resource['id'], resource['resourceType']))

    print(actual_vertices)
    assert actual_vertices == expected_vertices, f"Expected {expected_vertices} but got {actual_vertices}."


def test_graph_edges(graph, expected_edges):
    """Test the graph vertices."""

    # check edges all edges
    actual_edges = []
    for _ in graph.V().outE():
        assert 'edge' in _, f"Expected 'edge' in {_}"
        edge = _['edge']
        assert 'gid' in edge, f"Expected 'gid' in {edge}"
        assert 'label' in edge, f"Expected 'label' in {edge}"
        assert 'from' in edge, f"Expected 'from' in {edge}"
        assert 'to' in edge, f"Expected 'to' in {edge}"
        assert 'data' in edge, f"Expected 'data' in {edge}"

        actual_edges.append((edge['from'], edge['to'], edge['label']))

    print(actual_edges)
    assert actual_edges == expected_edges, f"Expected {expected_edges} but got {actual_edges}."


def test_graph_methods(graph):
    """Test the methods we expect in a graph object."""
    assert 'V' in dir(graph), f"Expected 'V' in {type(graph)}"
    assert 'match_label' in dir(graph), f"Expected 'match_label' in {type(graph)}"
    assert 'dataframe_associations' in dir(graph), f"Expected 'dataframe_associations' in {type(graph)}"


def test_traversals(graph):
    """Test basic traversals"""

    # specimen -> patient
    q = graph.V().hasLabel("Specimen").out("subject")
    actual_specimen_patient_count = len(list(q))
    assert actual_specimen_patient_count == 1, f"Expected 1 but got {actual_specimen_patient_count}."
    assert list(q)[0]['vertex']['data']['resourceType'] == 'Patient'

    q = graph.V().hasLabel("DocumentReference").outV().hasLabel("Specimen").outV().hasLabel("Patient")
    assert len(list(q)) == 1, f"Expected 1 but got {len(list(q))}."
    actual_document_reference_patient_count = len(list(q))
    assert actual_document_reference_patient_count == 1, f"Expected 1 but got {actual_document_reference_patient_count}."
    assert list(q)[0]['vertex']['data']['resourceType'] == 'Patient'

    # follow edges by edge label
    q = graph.V().hasLabel("DocumentReference").out("subject")
    assert len(list(q)) == 1, f"Expected 1 but got {len(list(q))}."
    for subject in q:
        subject = subject['vertex']['data']
        assert subject['resourceType'] == 'Specimen', f"Expected Specimen but got {subject['resourceType']}."

    # follow all out all edges recursively to a vertex of type X

    q = graph.V().hasLabel("DocumentReference")
    assert len(list(q)) == 1, f"Expected 1 but got {len(list(q))}."
    document_reference_gid = list(q)[0]['vertex']['gid']

    # 1 hop
    specimen = graph.match_label(document_reference_gid, 'Specimen')
    assert specimen is not None, "Expected Specimen"
    assert specimen['vertex']['gid'] == '60c67a06-ea2d-4d24-9249-418dc77a16a9', f"Expected 60c67a06-ea2d-4d24-9249-418dc77a16a9 but got {specimen}."

    # 2 hops
    patient = graph.match_label(document_reference_gid, 'Patient')
    assert patient is not None, "Expected Patient"
    assert patient['vertex']['gid'] == 'bc4e1aa6-cb52-40e9-8f20-594d9c84f920', f"Expected bc4e1aa6-cb52-40e9-8f20-594d9c84f920 but got {patient}."

    # 4 hops
    research_study = graph.match_label(document_reference_gid, 'ResearchStudy')
    assert research_study is not None, "Expected ResearchStudy"
    assert research_study['vertex']['gid'] == '7dacd4d0-3c8e-470b-bf61-103891627d45', f"Expected 7dacd4d0-3c8e-470b-bf61-103891627d45 but got {research_study}."

    # Observations
    q = graph.V(document_reference_gid).in_(["focus", "subject"]).hasLabel("Observation")
    assert len(list(q)) == 2, f"Expected 2 but got {len(list(q))} for {document_reference_gid}."


def test_dataframe_associations(graph, expected_vertices, expected_dataframe_associations):
    """Test the dataframe associations."""

    actual_dataframe_associations = defaultdict(list)
    # for all objects in the graph except Observations, retrieve the associated objects useful for a dataframe
    for vertex_gid, vertex_label in expected_vertices:
        if vertex_label == 'Observation':
            continue
        df = graph.dataframe_associations(vertex_gid, vertex_label)
        actual_dataframe_associations[(vertex_label, vertex_gid)] = [(_['resourceType'], _['id']) for _ in df]
    assert actual_dataframe_associations == expected_dataframe_associations, f"Expected {expected_dataframe_associations} but got {actual_dataframe_associations}."

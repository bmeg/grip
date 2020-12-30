#!/usr/bin/env python

import re
import os
import sys
import json
import grpc
import requests
import gripper_pb2
import gripper_pb2_grpc

from google.protobuf import json_format
from concurrent import futures


def gdcPager(url, params, getElementList):
    while True:
        res = requests.get(url, params=params)
        result = res.json()
        for i in getElementList(result):
            yield i
        p = result['data']['pagination']
        if p['page'] >= p['pages']:
            break
        params["from"] = p["from"] + p["count"]

class GDCCaseCollection:
    def __init__(self):
        self.url = "https://api.gdc.cancer.gov/cases"

    def getFields(self):
        return ["$.id", "$.case_id", "$.submitter_id", "$.project.project_id"]

    def getRows(self):
        for row in gdcPager(self.url, {"size":"100", "expand":"project"}, lambda x: x['data']['hits']):
            yield row['id'], row

    def getRowByID(self, id):
        filter = """{"op":"in","content":{"field":"case_id","value":["%s"]}}""" % id
        res = requests.get(self.url, params={"filters":filter, "expand":"project"})
        j = res.json()
        return j['data']['hits'][0]

    def getRowsByField(self, field, value):
        queryMap = {
            "$.case_id" : """{"op":"in","content":{"field":"case_id","value":["%s"]}}""",
            "$.submitter_id" : """{"op":"in","content":{"field":"submitter_id","value":["%s"]}}""",
            "$.project.project_id" : """{"op":"in","content":{"field":"project.project_id","value":["%s"]}}"""
        }
        filter = queryMap[field] % (value)

        for row in gdcPager(self.url, {"filters":filter, "size":"100", "expand":"project"}, lambda x: x['data']['hits']):
            yield row['id'], row

class GDCProjectCollection:
    def __init__(self):
        self.url = "https://api.gdc.cancer.gov/projects"

    def getFields(self):
        return ['$.project_id']

    def getRows(self):
        for row in gdcPager(self.url, {"size":"100"}, lambda x: x['data']['hits']):
            yield row['id'], row

    def getRowByID(self, id):
        filter = """{"op":"in","content":{"field":"project_id","value":["%s"]}}""" % id
        res = requests.get(self.url, params={"filters":filter})
        j = res.json()
        return j['data']['hits'][0]

    def getRowsByField(self, field, value):
        queryMap = {
            "$.project_id" : """{"op":"in","content":{"field":"project_id","value":["%s"]}}""",
        }
        filter = queryMap[field] % (value)

        for row in gdcPager(self.url, {"filters":filter, "size":"100"}, lambda x: x['data']['hits']):
            yield row['id'], row


class GDCSSMOccurrenceCollection:
    def __init__(self):
        self.url = "https://api.gdc.cancer.gov/ssm_occurrences"

    def getFields(self):
        return ["$.case.case_id", "$.ssm.ssm_id"]

    def getRowByID(self, id):
        filter = """{"op":"in","content":{"field":"ssm_occurrence_id","value":["%s"]}}""" % (id)
        res = requests.get(self.url, params={"filters":filter, "size" : "100", "expand":"case,ssm"})
        j = res.json()
        return j['data']['hits'][0]

    def getRows(self):
        for row in gdcPager(self.url, {"size":"100", "expand":"case,ssm"}, lambda x: x['data']['hits']):
            yield row['id'], row

    def getRowsByField(self, field, value):
        queryMap = {
            "$.case.case_id" : """{"op":"in","content":{"field":"case.case_id","value":["%s"]}}""",
            "$.ssm.ssm_id" : """{"op":"in","content":{"field":"ssm.ssm_id","value":["%s"]}}"""
        }
        filter = queryMap[field] % value
        for row in gdcPager(self.url, {"filters":filter, "expand":"case,ssm"}, lambda x:x['data']['hits']):
            yield row['id'], row

class GDCSSMCollection:
    def __init__(self):
        self.url = "https://api.gdc.cancer.gov/ssms"

    def getFields(self):
        return ["$.cosmic_id", "$.ssm_id"]

    def getRows(self):
        for row in gdcPager(self.url, {"size":"100"}, lambda x: x['data']['hits']):
            yield row['id'], row

    def getRowByID(self, id):
        filter = """{"op":"in","content":{"field":"id","value":["%s"]}}""" % (id)
        res = requests.get(self.url, params={"filters":filter, "size" : "100"})
        j = res.json()
        return j['data']['hits'][0]

    def getRowsByField(self, field, value):
        queryMap = {
            "$.cosmic_id" : """{"op":"in","content":{"field":"cosmic_id","value":["%s"]}}""",
            "$.ssm_id" : """{"op":"in","content":{"field":"ssm_id","value":["%s"]}}"""
        }
        filter = queryMap[field] % value
        for row in gdcPager(self.url, {"filters":filter}, lambda x:x['data']['hits']):
            yield row['id'], row


class PDCPublicCaseCollection:
    def __init__(self):
        self.url = "https://pdc.esacinc.com/graphql"
        query = "{allCases {case_id case_submitter_id project_submitter_id disease_type primary_site}}"
        req = requests.get(self.url, params={"query":query})
        j = req.json()
        self.data = {}
        for row in j['data']['allCases']:
            self.data[row['case_id']] = row

    def getFields(self):
        return ["$.case_submitter_id", "$.project_submitter_id"]

    def getRows(self):
        for k, v in self.data.items():
            yield k, v

    def getRowByID(self, id):
        return self.data[id]

    def getRowsByField(self, field, value):
        f = re.sub( r'^\$\.', '', field)
        for id, row in self.data.items():
            if row.get(f, None) == value:
                yield id, row

class PDBCaseCollection:

    def __init__(self):
        self.url = "https://pdc.esacinc.com/graphql"

    def getRowByID(self, id):
        filter = """
            case_id:
              url: https://pdc.esacinc.com/graphql
              element: "
              params:
                query: >
                  query {
                    case(case_id:"%s") {
                      case_id
                      case_submitter_id
                      project_submitter_id
                      external_case_id
                      tissue_source_site_code
                      days_to_lost_to_followup
                      disease_type
                      index_date
                      lost_to_followup
                      primary_site
                      count
                      demographics {
                        demographic_id
                        demographic_submitter_id
                        ethnicity
                        gender
                        race
                        cause_of_death
                        days_to_birth
                        days_to_death
                        vital_status
                        year_of_birth
                        year_of_death
                      }
                      project {
                        project_id
                      }
                      samples {
                        sample_id
                      }
                    }
                  }""" % (id)
        req = requests.get(self.url, params={"query":query})
        j = req.json()
        return j["data"]["case"]

collectionMap = {
    "GDCCases" : GDCCaseCollection(),
    "GDCProjects" : GDCProjectCollection(),
    "PDCPublicCases" : PDCPublicCaseCollection(),
    "GDCSSM" : GDCSSMCollection(),
    "GDCSSMOccurrence" : GDCSSMOccurrenceCollection()
}

class GDCSource(gripper_pb2_grpc.DigSourceServicer):
    def __init__(self):
        pass

    def GetCollections(self, request, context):
        for i in collectionMap.keys():
            o = gripper_pb2.Collection()
            o.name = i
            yield o

    def GetCollectionInfo(self, request, context):
        o = gripper_pb2.CollectionInfo()
        o.search_fields.extend( collectionMap[request.name].getFields() )
        # request.name
        return o

    def GetIDs(self, request, context):
        # request.name
        for k in []:
            o = gripper_pb2.RowID()
            o.id = k
            yield o

    def GetRows(self, request, context):
        # request.name
        for k, v in collectionMap[request.name].getRows():
            o = gripper_pb2.Row()
            o.id = k
            json_format.ParseDict(v, o.data)
            yield o

    def GetRowsByID(self, request_iterator, context):
        for req in request_iterator:
            d = collectionMap[req.collection].getRowByID(req.id)
            o = gripper_pb2.Row()
            o.id = req.id
            o.requestID = req.requestID
            json_format.ParseDict(d, o.data)
            yield o

    def GetRowsByField(self, request, context):
        for k, v in collectionMap[request.collection].getRowsByField(request.field, request.value):
            o = gripper_pb2.Row()
            o.id = k
            json_format.ParseDict(v, o.data)
            yield o


def serve(port):
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
  gripper_pb2_grpc.add_DigSourceServicer_to_server(
      GDCSource(), server)
  server.add_insecure_port('[::]:%s' % port)
  server.start()
  print("Serving: %s" % (port))
  server.wait_for_termination()


if __name__ == "__main__":
    serve(50051)

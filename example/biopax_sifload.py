#!/usr/bin/env python

import sys
import csv
import ophion

O = ophion.Ophion("http://localhost:8000")

v_set = set()

DEBUG=False

row_count = 0
with open(sys.argv[1]) as handle:
    reader = csv.reader(handle, delimiter="\t")
    edge_mode = True
    header = []
    for row in reader:
        if row_count % 1000 == 0:
            print "Rows: %d" % (row_count)
        row_count += 1
        if len(row):
            if row[0] == "PARTICIPANT_A":
                header = row
                edge_mode = True
            elif row[0] == "PARTICIPANT":
                header = row
                edge_mode = False
            else:
                data = dict(zip(header, row))
                if edge_mode:
                    for i in ["PARTICIPANT_A", "PARTICIPANT_B"]:
                        if data[i] not in v_set:
                            q = O.query().addV(data[i])
                            if DEBUG:
                                print q.render()
                            else:
                                q.execute()
                            v_set.add(data[i])
                    q = O.query().V(data["PARTICIPANT_A"]).addE(data["INTERACTION_TYPE"]).to(data["PARTICIPANT_B"])
                    
                    for k in ["INTERACTION_DATA_SOURCE", "INTERACTION_PUBMED_ID", "PATHWAY_NAMES", "MEDIATOR_IDS"]:
                        if len(data[k]):
                            q = q.property(k, data[k])
                    if DEBUG:
                        print q.render()
                    else:
                        q.execute()
                else:
                    if data["PARTICIPANT"] not in v_set:
                        q = O.query().addV(data["PARTICIPANT"])
                        if DEBUG:
                            print q.render()
                        else:
                            q.execute()
                        v_set.add(data["PARTICIPANT"])
                    
                    q = O.query().V(data["PARTICIPANT"])
                    for k in ["PARTICIPANT_TYPE", "PARTICIPANT_NAME", "UNIFICATION_XREF", "RELATIONSHIP_XREF"]:
                        if len(data[k]):
                            q = q.property(k, data[k])
                    if DEBUG:
                        print q.render()
                    else:
                        q.execute()

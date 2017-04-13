from __future__ import print_function, division

import luigi
import json
import time
import re
import datetime
import subprocess
import base64
from urllib import urlopen

import uuid
from uuid import uuid4
from uuid import uuid5
import os
import sys
import copy

from itertools import groupby
from operator import itemgetter
from elasticsearch import Elasticsearch

#for hack to get around non self signed certificates
import ssl

#Amazon S3 support for writing touch files to S3
from luigi.s3 import S3Target
#luigi S3 uses boto for AWS credentials
import boto

class DockstoreTask(luigi.Task):

    #args here, create json

    def run(self):
        pass

    def output(self):
        pass

class CNVCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_client_path = luigi.Parameter(default='/ucsc-storage-client')
    redwood_host = luigi.Parameter(default='storage.ucsc-cgl.org')
    image_descriptor = luigi.Parameter("must be defined") 
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.8")
    tmp_dir = luigi.Parameter(default='/datastore')
    max_jobs = luigi.Parameter(default='1')
    bundle_uuid_filename_to_file_uuid = {}
    process_sample_uuid = luigi.Parameter(default = "")

    #TODO - verify touch file path
    touch_file_path_prefix = "cgl-core-analysis-run-touch-files/consonance-jobs/CNV/1_0_1"

    test_mode = luigi.BooleanParameter(default = False)

    def requires(self):
        print("\n\n\n\n ** COORDINATOR REQUIRES ** ")

        #hack to get around none self signed certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        print("Opening redwood host: "+str("https://"+self.redwood_host+":8444/entities?page=0"))
        json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page=0"), context=ctx).read()
 
        metadata_struct = json.loads(json_str)
        print("** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"]))
        for i in range(0, metadata_struct["totalPages"]):
            print("** CURRENT METADATA TOTAL PAGES: "+str(i))
            json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page="+str(i)), context=ctx).read()
#            json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page="+str(i))).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        #open elasticsearch index
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])

        listOfJobs = []

        #search es index for metadata telling us the cnv workflow has not yet been run.
        #TODO: switch rna-seq with cnv - temp for testing
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_rna_seq_cgl_workflow_3_0_x" : "false"}},{"term" : {"flags.tumor_rna_seq_cgl_workflow_3_0_x" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)

        print("Got %d Hits:" % res['hits']['total'])

        grouped_by_donor = {}

        for hit in res['hits']['hits']:

            print(hit)
            #print("\n\n\nDonor uuid:%(donor_uuid)s Center Name:%(center_name)s Program:%(program)s Project:%(project)s" % hit["_source"])
            #print("Got %d specimens:" % len(hit["_source"]["specimen"]))

            #group by donor_uuid
            if hit["_source"]["donor_uuid"] not in grouped_by_donor:
                grouped_by_donor[hit["_source"]["donor_uuid"]] = [hit["_source"]]
            else:
                grouped_by_donor[hit["_source"]["donor_uuid"]].append(hit["_source"])

        for key, group in grouped_by_donor:
            print("donor {} with {} samples\n\n".format(key, str(len(group))))

        print("total of {} jobs; max jobs allowed is {}\n\n".format(str(len(listOfJobs)), self.max_jobs))

        # these jobs are yielded to
        print("\n\n** COORDINATOR REQUIRES DONE!!! **")
        return listOfJobs


    def run(self):
        print(" \n\n****COORDINATOR RUN****\n\n")

    def output(self):
        print("\n\n****COORDINATOR OUTPUT****\n\n")

if __name__ == '__main__':
    luigi.run()

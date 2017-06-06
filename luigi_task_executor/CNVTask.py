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
    redwood_host = luigi.Parameter("storage.ucsc-cgl.org")
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_client_path = luigi.Parameter(default='/ucsc-storage-client')
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.8")

    normal_sample = luigi.Parameter("must be define")
    tumor_sample = luigi.Parameter("must be defined")

    test_mode = luigi.BooleanParameter(default=False)

    target_tool_url = luigi.Parameter(default="https://github.com/BD2KGenomics/dockstore_workflow_cnv")
    workflow_type = luigi.Parameter(default="CNV")
    image_descriptor = luigi.Parameter("must be defined")

    tmp_dir = luigi.Parameter(default='/datastore')

    #meta_data_json = luigi.Parameter(default="must input metadata") #is this arg necessary w this weird setup?
    touch_file_path = luigi.Parameter(default='must input touch file path')

    json_dict = {}

    #path will be bath in redwood?
    json_dict["TUMOR_BAM"] = {"class" : "File", "path" : "TODO"}
    json_dict["NORMAL_BAM"] = {"class" : "File", "path" : "TODO"}
    json_dict["SAMPLE_ID"] = {"class" : "File", "path" : "TODO"}
    json_dict["CENTROMERES"] = {"class" : "File", "path" : "TODO"}
    json_dict["TARGETS"] = {"class" : "File", "path" : "TODO"}
    json_dict["GENO_FA_GZ"] = {"class" : "File", "path" : "TODO"}


    def run(self):
        #cmd is:
        #dockstore workflow launch --entry BD2KGenomics/dockstore_workflow_cnv:master --json Dockstore.json

        #todo - create json here. 

        cmd = ["dockstore", "workflow", "lauch", "--entry", "BD2KGenomics/dockstore_workflow_cnv:master", "--json", "todo"]
        cmd_str = ' '.join(cmd)
        if self.test_mode == False:
            try:
                consonance_output_json = subprocess.check_output(cmd)
            except subprocess.CalledProcessError as e:
                return_code = e.returncode
                sys.exit(return_code)
            except Exception as e:
                return_code = 1
                sys.exit(return_code)
        else:
            print("test mode")


    def output(self):
        pass

class CNVCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_host = luigi.Parameter(default='storage.ucsc-cgl.org')
    image_descriptor = luigi.Parameter("must be defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.14")
    tmp_dir = luigi.Parameter(default='/datastore')
    max_jobs = luigi.Parameter(default='-1')
    bundle_uuid_filename_to_file_uuid = {}
    process_sample_uuid = luigi.Parameter(default = "")

    workflow_version = luigi.Parameter(default="1.0")
    touch_file_bucket = luigi.Parameter(default="must be input")

    vm_region = luigi.Parameter(default='us-west-2')

    #Consonance will not be called in test mode
    test_mode = luigi.BoolParameter(default = False)

    def requires(self):
        print("\n\n\n\n ** COORDINATOR REQUIRES ** ")

        #hack to get around none self signed certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        print("Opening redwood host: "+str("https://"+self.redwood_host+":8444/entities?page=0"))
        json_str = urlopen(str("https://metadata."+self.redwood_host+"/entities?page=0"), context=ctx).read()

        metadata_struct = json.loads(json_str)
        print("** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"]))
        for i in range(0, metadata_struct["totalPages"]):
            print("** CURRENT METADATA TOTAL PAGES: "+str(i))
            json_str = urlopen(str("https://metadata."+self.redwood_host+"/entities?page="+str(i)), context=ctx).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        #open elasticsearch index
        es = Elasticsearch(['http://'+self.es_index_host+":"+self.es_index_port])
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_cnv_workflow" : "false"}},{"term" : {"flags.tumor_cnv_workflow" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)

        listOfJobs = []

        #search es index for metadata telling us the cnv workflow has not yet been run.

        print("Got %d Hits:" % res['hits']['total'])

        grouped_by_donor = {}
        i = 0
        for hit in res['hits']['hits']:
            #print("New hit starts here:")
            #print(hit)
            #print("\n\n\nDonor uuid:%(donor_uuid)s Center Name:%(center_name)s Program:%(program)s Project:%(project)s" % hit["_source"])
            #print("Got %d specimens:" % len(hit["_source"]["specimen"]))

            cf = 0
            for specimen in hit["_source"]["specimen"]:
                normal_samples = []
                tumor_samples = []
                for sample in specimen["samples"]:
                    for analysis in sample["analysis"]:
                        if analysis["analysis_type"] != "CNV":
                            cf = 1

                        if specimen["submitter_specimen_type"] == "Normal - blood": #is this right? - need to see data
                            normal_samples.append(sample)
                        else:
                            tumor_samples.append(sample)

                if cf == 1: #if any of the sample analysis types is not CNV, continue
                    continue

                touch_file_path = "" # TODO - or should it be done in dockstoretask?

                #centromeres = some hardcoded path in redwood
                #targets = some hardcoded path in redwood
                #geno_fa_gz = some harcoded path...

                for normal_sample in normal_samples:
                    for tumor_sample in tumor_samples: #run every normal sample vs tumor samples.
                        listOfJobs.append(DockstoreTask(normal_sample=normal_sample, tumor_sample=tumor_sample, redwood_host=self.redwood_host, 
                                                        redwood_token=self.redwood_token, redwood_client_path=self.redwood_client_path, dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool,
                                                        touch_file_path=touch_file_path, test_mode=self.test_mode)) #TODO : insert rest of args


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

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
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.8")

    normal_sample_path = luigi.Parameter("must be defined")
    normal_sample_uuid = luigi.Parameter("must be defined")
    normal_bundle_uuid = luigi.Parameter("must be defined")

    tumor_sample_path = luigi.Parameter("must be defined")
    tumor_sample_uuid = luigi.Parameter("must be defined")
    tumor_bundle_uuid = luigi.Parameter("must be defined")

    hg38bed_bundle_uuid = luigi.Parameter("must be defined")
    hg38bed_file_uuid = luigi.Parameter("must be defined")
    hg38bed_file_name = luigi.Parameter("must be defined")

    hg38fa_bundle_uuid = luigi.Parameter("must be defined")
    hg38fa_file_uuid = luigi.Parameter("must be defined")
    hg38fa_file_name = luigi.Parameter("must be defined")

    seqcap_bundle_uuid = luigi.Parameter("must be defined")
    seqcap_file_uuid = luigi.Parameter("must be defined")
    seqcap_file_name = luigi.Parameter("must be defined")

    test_mode = luigi.BoolParameter(default=False)

    target_tool_url = luigi.Parameter(default="https://github.com/BD2KGenomics/dockstore_workflow_cnv")
    target_tool_prefix = luigi.Parameter(default="https://dockstore.org/workflows/BD2KGenomics/dockstore_workflow_cnv")
    workflow_type = luigi.Parameter(default="cnv_variant_calling")
    workflow_version = luigi.Parameter(default="")
    image_descriptor = luigi.Parameter("must be defined")

    parent_uuids = luigi.Parameter("must be defined")
    vm_region = luigi.Parameter(default="us-west-2")

    tmp_dir = luigi.Parameter(default='/datastore')

    meta_data_json = luigi.Parameter(default="must input metadata") 
    touch_file_path = luigi.Parameter(default='must input touch file path')
    submitter_sample_id = luigi.Parameter(default="must be input")


    def run(self):

        meta_data = json.loads(self.meta_data_json)

        json_dict = {}

        tum_path = "redwood://"+self.redwood_host+"/"+self.tumor_bundle_uuid+"/"+self.tumor_sample_uuid+"/"+self.tumor_sample_path
        norm_path = "redwood://"+self.redwood_host+"/"+self.normal_bundle_uuid+"/"+self.normal_sample_uuid+"/"+self.normal_sample_path
        cent_path = "redwood://"+self.redwood_host+"/"+self.hg38bed_bundle_uuid+"/"+self.hg38bed_file_uuid+"/"+self.hg38bed_file_name
        targ_path = "redwood://"+self.redwood_host+"/"+self.seqcap_bundle_uuid+"/"+self.seqcap_file_uuid+"/"+self.seqcap_file_name
        fagz_path = "redwood://"+self.redwood_host+"/"+self.hg38fa_bundle_uuid+"/"+self.hg38fa_file_uuid+"/"+self.hg38fa_file_name

        json_dict["TUMOR_BAM"] = {"class" : "File", "path" : tum_path}
        json_dict["NORMAL_BAM"] = {"class" : "File", "path" : norm_path}
        json_dict["SAMPLE_ID"] = {"class" : "string", "string" : meta_data["submitter_specimen_id"]}
        json_dict["CENTROMERES"] = {"class" : "File", "path" : cent_path}
        json_dict["TARGETS"] = {"class" : "File", "path" : targ_path}
        json_dict["GENO_FA_GZ"] = {"class" : "File", "path" : fagz_path}
        json_dict["VARSCAN_OUTCNV"] = {"class" : "File", "path" : "/home/ubuntu/thomas_varscan_test_out"} #temp, TODO change
        json_dict["ADTEX_OUTCNV"] = {"class" : "File", "path" : "/home/ubuntu/thomas_adtex_test_out"} #TODO
        #cmd is:
        #dockstore workflow launch --entry BD2KGenomics/dockstore_workflow_cnv:master --json Dockstore.json

        local_json_dir = "/tmp/" + self.touch_file_path
        cmd = ["mkdir", "-p", local_json_dir ]
        cmd_str = ''.join(cmd)
        print(cmd_str)
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as e:
            return_code = e.returncode
            sys.exit(return_code)
        except Exception as e:
            return_code = 1
            sys.exit(return_code)

        output_base_name = meta_data["submitter_sample_id"]

        #todo - create json here. 
        json_str = json.dumps(json_dict)
        print("THE JSON: "+json_str)
        # now make base64 encoded version
        base64_json_str = base64.urlsafe_b64encode(json_str)
        target_tool= self.target_tool_prefix + ":" + self.workflow_version
        parent_uuids = ','.join(map("{0}".format, self.parent_uuids))

        dockstore_json_str = '''{
            "program_name": "%s",
            "json_encoded": "%s",
            "docker_uri": "%s",
            "dockstore_url": "%s",
            "redwood_token": "%s",
            "redwood_host": "%s",
            "parent_uuids": "%s",
            "workflow_type": "%s",
            "tmpdir": "%s",
            "vm_instance_type": "c4.8xlarge",
            "vm_region": "%s",
            "vm_location": "aws",
            "vm_instance_cores": 36,
            "vm_instance_mem_gb": 60,
            "output_metadata_json": "/tmp/final_metadata.json"
        }''' % (meta_data["program"].replace(' ','_'), base64_json_str, target_tool, self.target_tool_url, self.redwood_token, self.redwood_host, parent_uuids, self.workflow_type, self.tmp_dir, self.vm_region )



        cmd = ["dockstore", "workflow", "lauch", "--entry", "BD2KGenomics/dockstore_workflow_cnv:master", "--json", self.save_dockstore_json_local().path]
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

    def save_dockstore_json_local(self):
        #task_uuid = self.get_task_uuid()
        #luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/dockstore_tool.json' % ( task_uuid))
        #return S3Target('%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))
        return luigi.LocalTarget('/tmp/%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))


class CNVCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_host = luigi.Parameter(default='storage.ucsc-cgl.org')
    image_descriptor = luigi.Parameter("must be defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.14")
    tmp_dir = luigi.Parameter(default='/datastore')
    max_jobs = luigi.Parameter(default='1')
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
            print("New hit starts here:")
            print(hit)
            print("\n\n\nDonor uuid:%(donor_uuid)s Center Name:%(center_name)s Program:%(program)s Project:%(project)s" % hit["_source"])
            print("Got %d specimens:" % len(hit["_source"]["specimen"]))

            #get all reference file jsons
            hg38bed_json = urlopen(str("https://metadata."+self.redwood_host+"/entities?fileName=hg38.centromeres.bed"), context=ctx).read()
            hg38fa_json = urlopen(str("https://metadata."+self.redwood_host+"/entities?fileName=hg38.fa.gz"), context=ctx).read()
            seqcap_json = urlopen(str("https://metadata."+self.redwood_host+"/entities?fileName=SeqCap_primary_hg38_uniq.bed"), context=ctx).read()

            hg38bed_data = json.loads(hg38bed_json)
            hg38bed_bundle_uuid = hg38bed_data["content"][0]["gnosId"]
            hg38bed_file_uuid = hg38bed_data["content"][0]["id"]
            hg38bed_file_name = hg38bed_data["content"][0]["fileName"]

            hg38fa_data = json.loads(hg38fa_json)
            hg38fa_bundle_uuid = hg38fa_data["content"][0]["gnosId"]
            hg38fa_file_uuid = hg38fa_data["content"][0]["id"]
            hg38fa_file_name = hg38fa_data["content"][0]["fileName"]

            seqcap_data = json.loads(seqcap_json)
            seqcap_bundle_uuid = seqcap_data["content"][0]["gnosId"]
            seqcap_file_uuid = seqcap_data["content"][0]["id"]
            seqcap_file_name = seqcap_data["content"][0]["fileName"]

            cnflag = False
            for specimen in hit["_source"]["specimen"]:
                for sample in specimen["samples"]:
                    for analysis in sample["analysis"]:
                        if analysis["analysis_type"] == "cnv_variant_calling":
                            cnflag = True

            if cnflag:
                specimen = hit["_source"]["specimen"][0]

                workflow_version_dir = self.workflow_version.replace('.', '_')
                touch_file_path_prefix = self.touch_file_bucket+"/consonance-jobs/cnv/" + workflow_version_dir
                touch_file_path = touch_file_path_prefix+"/"+hit["_source"]["center_name"]+"_"+hit["_source"]["program"] \
                                                        +"_"+hit["_source"]["project"]+"_"+hit["_source"]["submitter_donor_id"] \
                                                        +"_"+specimen["submitter_specimen_id"]

                submitter_sample_id = specimen["samples"][0]["submitter_sample_id"]

                meta_data = {}
                meta_data["program"] = hit["_source"]["program"]
                meta_data["project"] = hit["_source"]["project"]
                meta_data["center_name"] = hit["_source"]["center_name"]
                meta_data["submitter_donor_id"] = hit["_source"]["submitter_donor_id"]
                meta_data["donor_uuid"] = hit["_source"]["donor_uuid"]
                if "submitter_donor_primary_site" in hit["_source"]:
                    meta_data["submitter_donor_primary_site"] = hit["_source"]["submitter_donor_primary_site"]
                else:
                    meta_data["submitter_donor_primary_site"] = "not provided"
                meta_data["submitter_specimen_id"] = specimen["submitter_specimen_id"]
                meta_data["specimen_uuid"] = specimen["specimen_uuid"]
                meta_data["submitter_specimen_type"] = specimen["submitter_specimen_type"]
                meta_data["submitter_experimental_design"] = specimen["submitter_experimental_design"]
                meta_data["submitter_sample_id"] = specimen["samples"][0]["submitter_sample_id"]
                meta_data["sample_uuid"] = specimen["samples"][0]["sample_uuid"]
                meta_data["analysis_type"] = "cnv_variant_calling"
                meta_data["workflow_name"] = "quay.io/ucsc_cgl/cnv-workflow"#todo
                meta_data["workflow_version"] = self.workflow_version

                meta_data_json = json.dumps(meta_data)
                #print("meta data:")
                #print(meta_data_json)


                #print analysis
                print("HIT!!!! " + analysis["analysis_type"] + " " + str(hit["_source"]["flags"]["normal_rna_seq_quantification"])
                               + " " + str(hit["_source"]["flags"]["tumor_rna_seq_quantification"]) + " "
                               + specimen["submitter_specimen_type"]+" "+str(specimen["submitter_experimental_design"]))


                normal_files = []
                normal_file_uuids = []
                normal_bundle_uuids = []

                tumor_files = []
                tumor_file_uuids = []
                tumor_bundle_uuids = []

                parent_uuids = {}

                for specimen in hit["_source"]["specimen"]:
                    for sample in specimen["samples"]:
                        for analysis in sample["analysis"]:

                            for file in analysis["workflow_outputs"]:

                                parent_uuids[sample["sample_uuid"]] = True

                                if file["file_type"] == "bam":

                                    if re.match("^Normal - ", specimen["submitter_specimen_type"]):
                                        normal_files.append(file["file_path"])
                                        normal_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                        normal_bundle_uuids.append(analysis["bundle_uuid"])

                                    else: 
                                        tumor_files.append(file["file_path"])
                                        tumor_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                        tumor_bundle_uuids.append(analysis["bundle_uuid"])

                print("Normal files:", normal_files)
                print("Tumor files:", tumor_files)

                for i in range(0, len(normal_files)):
                    for i in range(0, len(tumor_files)):
                        listOfJobs.append(DockstoreTask(redwood_host=self.redwood_host, redwood_token=self.redwood_token, dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool,
                                                        meta_data_json=meta_data_json, normal_sample_path=normal_files[i], normal_sample_uuid=normal_file_uuids[i],
                                                        normal_bundle_uuid=normal_bundle_uuids[i], tumor_sample_path=tumor_files[i], parent_uuids = parent_uuids.keys(),
                                                        tumor_sample_uuid=tumor_file_uuids[i], tumor_bundle_uuid=tumor_bundle_uuids[i], hg38bed_bundle_uuid=hg38bed_bundle_uuid,
                                                        hg38bed_file_uuid=hg38bed_file_uuid, hg38bed_file_name=hg38bed_file_name, hg38fa_bundle_uuid=hg38fa_bundle_uuid,
                                                        hg38fa_file_uuid=hg38fa_file_uuid, hg38fa_file_name=hg38fa_file_name, seqcap_bundle_uuid=seqcap_bundle_uuid,
                                                        seqcap_file_uuid=seqcap_file_uuid, seqcap_file_name=seqcap_file_name, image_descriptor=self.image_descriptor,
                                                        touch_file_path=touch_file_path, tmp_dir=self.tmp_dir, test_mode=self.test_mode, submitter_sample_id=submitter_sample_id))


                            #what is this for?
                            #parent_uuids = {}


                #touch_file_path = "" # TODO - or should it be done in dockstoretask?

                #centromeres = some hardcoded path in redwood
                #targets = some hardcoded path in redwood
                #geno_fa_gz = some harcoded path...

                #for normal_sample in normal_samples:
                #    for tumor_sample in tumor_samples: #run every normal sample vs tumor samples.
                #        listOfJobs.append(DockstoreTask(normal_sample=normal_sample, tumor_sample=tumor_sample, redwood_host=self.redwood_host, 
                #                                        redwood_token=self.redwood_token, redwood_client_path=self.redwood_client_path, dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool,
                #                                        touch_file_path=touch_file_path, test_mode=self.test_mode)) #TODO : insert rest of args


        print("total of {} jobs; max jobs allowed is {}\n\n".format(str(len(listOfJobs)), self.max_jobs))

        # these jobs are yielded to
        print("\n\n** COORDINATOR REQUIRES DONE!!! **")
        return listOfJobs


    def run(self):
        print(" \n\n****COORDINATOR RUN****\n\n")

    def output(self):
        print("\n\n****COORDINATOR OUTPUT****\n\n")

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]


if __name__ == '__main__':
    luigi.run()

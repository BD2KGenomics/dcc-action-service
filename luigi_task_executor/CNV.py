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
from collections import defaultdict


from elasticsearch import Elasticsearch

#for hack to get around non self signed certificates
import ssl

#Amazon S3 support for writing touch files to S3
from luigi.s3 import S3Target
#luigi S3 uses boto for AWS credentials
import boto

class ConsonanceTask(luigi.Task):

    # TODO : update to reflect pipeline parameters.

    json_dict = {}

    redwood_host = luigi.Parameter("storage.ucsc-cgl.org")
    redwood_token = luigi.Parameter("must_be_defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.17")

    workflow_version = luigi.Parameter(default="must be defined")

    target_tool_prefix = luigi.Parameter(default="BD2KGenomics/dockstore_workflow_cnv")


    target_tool_url = luigi.Parameter(default="https://dockstore.org/workflows/BD2KGenomics/dockstore_workflow_cnv")
    workflow_type = luigi.Parameter(default="cnv_variant_calling")
    image_descriptor = luigi.Parameter("must be defined")

    vm_instance_type = luigi.Parameter(default='c4.8xlarge')
    vm_region = luigi.Parameter(default='us-west-2')

    tmp_dir = luigi.Parameter(default='/datastore') #equivalent of work mount
    sse_key = luigi.Parameter(default="")
    sse_key_is_master = luigi.BoolParameter(default= False)

    sample_name = luigi.Parameter(default='must input sample name')
    specimen_type = luigi.Parameter(default='must input sample name')
    #output_filename = sample_name
    #submitter_sample_id = luigi.Parameter(default='must input submitter sample id')
    cnv_job_json = luigi.Parameter(default="must input metadata")
    cnv_reference_files_json = luigi.Parameter(default="must input reference file metadata")

    touch_file_path = luigi.Parameter(default='must input touch file path')

    #Consonance will not be called in test mode
    test_mode = luigi.BoolParameter(default = False)

    def run(self):
        print("\n\n\n** TASK RUN **")
        #get a unique id for this task based on the some inputs
        #this id will not change if the inputs are the same
#        task_uuid = self.get_task_uuid()

#        print "** MAKE TEMP DIR **"
        # create a unique temp dir
        local_json_dir = "/tmp/" + self.touch_file_path
        cmd = ["mkdir", "-p", local_json_dir ]
        cmd_str = ''.join(cmd)
        print(cmd_str)
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as e:
            #If we get here then the called command return code was non zero
            print("\nERROR!!! MAKING LOCAL JSON DIR : " + cmd_str + " FAILED !!!", file=sys.stderr)
            print("\nReturn code:" + str(e.returncode), file=sys.stderr)
            return_code = e.returncode
            sys.exit(return_code)
        except Exception as e:
            print("\nERROR!!! MAKING LOCAL JSON DIR : " + cmd_str + " THREW AN EXCEPTION !!!", file=sys.stderr)
            print("\nException information:" + str(e), file=sys.stderr)
            #if we get here the called command threw an exception other than just
            #returning a non zero return code, so just set the return code to 1
            return_code = 1
            sys.exit(return_code)

        cnv_job = json.loads(self.cnv_job_json)
        output_base_name = cnv_job['sample_name']
       
        json_dict = defaultdict() 
        json_dict["NORMAL_BAM"] = cnv_job['NORMAL_BAM']
        json_dict["TUMOR_BAM"] = cnv_job['TUMOR_BAM']
        json_dict["SAMPLE_ID"] = cnv_job['SAMPLE_ID']
        json_dict["ADTEX_OUTCNV"] = cnv_job['ADTEX_OUTCNV']
        json_dict["VARSCAN_OUTCNV"] = cnv_job['VARSCAN_OUTCNV']

        cnv_reference_files = json.loads(self.cnv_reference_files_json)

        for option, reference_files_dict in cnv_reference_files.iteritems():
           json_dict[option] = reference_files_dict



        print("\njson dict:")
        print(dict(json_dict))

        print("** MAKE JSON FOR WORKER **")
        # create a json for for the pipeline which will be executed by the dockstore-tool-running-dockstore-tool and passed as base64encoded
        # will need to encode the JSON above in this: https://docs.python.org/2/library/base64.html
        # see http://luigi.readthedocs.io/en/stable/api/luigi.parameter.html?highlight=luigi.parameter
        # TODO: this is tied to the requirements of the tool being targeted
        
        json_str = json.dumps(json_dict, sort_keys=True, indent=4, separators=(',', ': '))
        print("THE JSON: "+json_str)
        # now make base64 encoded version
        base64_json_str = base64.urlsafe_b64encode(json_str)
        print("** MAKE JSON FOR DOCKSTORE TOOL WRAPPER **")

        # create a json for dockstoreRunningDockstoreTool, embed the  JSON as a param
# below used to be a list of parent UUIDs; which is correct????
#            "parent_uuids": "[%s]",
        parent_uuids = ','.join(map("{0}".format, cnv_job['parent_uuids']))

        print("parent uuids:%s" % parent_uuids)

        p = self.save_dockstore_json().open('w')
        p_local = self.save_dockstore_json_local().open('w')

        target_tool= self.target_tool_prefix + ":" + self.workflow_version

        dockstore_json = {}
        dockstore_json["program_name"] = cnv_job["program"].replace(' ','_')
        dockstore_json["json_encoded"] = base64_json_str
        dockstore_json["docker_uri"] = target_tool
        dockstore_json["dockstore_url" ] = self.target_tool_url
        dockstore_json["redwood_token" ] = self.redwood_token
        dockstore_json["redwood_host"] = self.redwood_host
        dockstore_json["parent_uuids"] = parent_uuids
        dockstore_json["workflow_type"] = self.workflow_type
        dockstore_json["launch_type"] = 'workflow'
        dockstore_json["tmpdir"] = self.tmp_dir
        dockstore_json["vm_instance_type"] = self.vm_instance_type
        dockstore_json["vm_region"] = self.vm_region
        dockstore_json["vm_location"] = "aws"
        dockstore_json["vm_instance_cores"] = 36
        dockstore_json["vm_instance_mem_gb"] = 60
        dockstore_json["output_metadata_json"] = "/tmp/final_metadata.json"

        dockstore_json_str = json.dumps(dockstore_json , sort_keys=True, indent=4, separators=(',', ': '))
        print(dockstore_json_str, file=p)
        p.close()
    
        # write the parameterized JSON for input to Consonance
        # to a local file since Consonance cannot read files on s3
        print(dockstore_json_str, file=p_local)
        p_local.close()

        # execute consonance run, parse the job UUID

        #cmd = ["consonance", "run", "--image-descriptor", self.image_descriptor, "--flavour", "c4.8xlarge", "--run-descriptor", self.save_dockstore_json_local().path]

        cmd = ["consonance", "run",  "--tool-dockstore-id", self.dockstore_tool_running_dockstore_tool, "--flavour", self.vm_instance_type, "--run-descriptor", self.save_dockstore_json_local().path]
        cmd_str = ' '.join(cmd)
        if self.test_mode == False:
            print("** SUBMITTING TO CONSONANCE **")
            print("executing:"+ cmd_str)
            print("** WAITING FOR CONSONANCE **")

            try:
                consonance_output_json = subprocess.check_output(cmd)
            except subprocess.CalledProcessError as e:
                #If we get here then the called command return code was non zero
                print("\nERROR!!! CONSONANCE CALL: " + cmd_str + " FAILED !!!", file=sys.stderr)
                print("\nReturn code:" + str(e.returncode), file=sys.stderr)

                return_code = e.returncode
                sys.exit(return_code)
            except Exception as e:
                print("\nERROR!!! CONSONANCE CALL: " + cmd_str + " THREW AN EXCEPTION !!!", file=sys.stderr)
                print("\nException information:" + str(e), file=sys.stderr)
                #if we get here the called command threw an exception other than just
                #returning a non zero return code, so just set the return code to 1
                return_code = 1
                sys.exit(return_code)

            print("Consonance output is:\n\n{}\n--end consonance output---\n\n".format(consonance_output_json))

            #get consonance job uuid from output of consonance command
            consonance_output = json.loads(consonance_output_json)
            if "job_uuid" in consonance_output:
                cnv_job["consonance_job_uuid"] = consonance_output["job_uuid"]
            else:
                print("ERROR: COULD NOT FIND CONSONANCE JOB UUID IN CONSONANCE OUTPUT!", file=sys.stderr)
        else:
            print("TEST MODE: Consonance command would be:"+ cmd_str)
            cnv_job["consonance_job_uuid"] = 'no consonance id in test mode'

        #remove the local parameterized JSON file that
        #was created for the Consonance call
        #since the Consonance call is finished
        self.save_dockstore_json_local().remove()

        #convert the meta data to a string and
        #save the donor metadata for the sample being processed to the touch
        # file directory
        cnv_job_json = json.dumps(cnv_job, sort_keys=True, indent=4, separators=(',', ': '))
        m = self.save_metadata_json().open('w')
        print(cnv_job_json, file=m)
        m.close()


#        if result == 0:
#            cmd = "rm -rf "+self.data_dir+"/"+self.bundle_uuid+"/bamstats_report.zip "+self.data_dir+"/"+self.bundle_uuid+"/datastore/"
#            print "CLEANUP CMD: "+cmd
#            result = subprocess.call(cmd, shell=True)
#            if result == 0:
#                print "CLEANUP SUCCESSFUL"

         # NOW MAke a final report
        f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
        print("Consonance task is complete", file=f)
        f.close()
        print("\n\n\n\n** TASK RUN DONE **")

    def save_metadata_json(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/metadata.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/metadata.json' % ( task_uuid))
        return S3Target('s3://%s/%s_meta_data.json' % (self.touch_file_path, self.sample_name + "_" + self.specimen_type ))

    def save_dockstore_json_local(self):
        #task_uuid = self.get_task_uuid()
        #luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/dockstore_tool.json' % ( task_uuid))
        #return S3Target('%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))
        return luigi.LocalTarget('/tmp/%s/%s_dockstore_tool.json' % (self.touch_file_path, self.sample_name + "_" + self.specimen_type ))

    def save_dockstore_json(self):
        #task_uuid = self.get_task_uuid()
        #luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/dockstore_tool.json' % ( task_uuid))
        return S3Target('s3://%s/%s_dockstore_tool.json' % (self.touch_file_path, self.sample_name + "_" + self.specimen_type  ))

    def output(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/finished.txt' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/finished.txt' % ( task_uuid))
        return S3Target('s3://%s/%s_finished.json' % (self.touch_file_path, self.sample_name + "_" + self.specimen_type  ))


class CNVCoordinator(luigi.Task):
    
    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_host = luigi.Parameter(default='storage.ucsc-cgp.org')
    image_descriptor = luigi.Parameter("must be defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.17")
    tmp_dir = luigi.Parameter(default='/datastore')
    max_jobs = luigi.Parameter(default='-1')
    bundle_uuid_filename_to_file_uuid = {}
    process_sample_uuid = luigi.Parameter(default = "master")

    workflow_version = luigi.Parameter(default="")
    touch_file_bucket = luigi.Parameter(default="must be input")

    vm_instance_type = luigi.Parameter(default='c4.8xlarge')
    vm_region = luigi.Parameter(default='us-west-2')

    #Consonance will not be called in test mode
    test_mode = luigi.BoolParameter(default = False)

    center = luigi.Parameter(default = "")
    program = luigi.Parameter(default = "")
    project = luigi.Parameter(default = "")


    def requires(self):
        print("\n\n\n\n** COORDINATOR REQUIRES **")

        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        print(str("metadata."+self.redwood_host+"/entities?page=0"))

#hack to get around none self signed certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        json_str = urlopen(str("https://metadata."+self.redwood_host+"/entities?page=0"), context=ctx).read()
        metadata_struct = json.loads(json_str)
        print("** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"]))
        for i in range(0, metadata_struct["totalPages"]):
            print("** CURRENT METADATA TOTAL PAGES: "+str(i))
            json_str = urlopen(str("https://metadata."+self.redwood_host+"/entities?page="+str(i)), context=ctx).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        # now query elasticsearch
        print("setting up elastic search Elasticsearch([\"http:\/\/"+self.es_index_host+":"+self.es_index_port+"]")
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_cnv_workflow" : "false"}}, \
                        {"term" : {"flags.tumor_cnv_workflow" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc

        reference_cwl_switch_to_file = {
            'GENO_FA_GZ' : 'hg38.fa.gz', \
            'CENTROMERES' : 'hg38.centromeres.bed', \
            'TARGETS' : 'SeqCap_primary_hg38_uniq.bed'
        }

        listOfJobs = []

        cnv_jobs  = defaultdict(dict)
        cnv_jobs['samples'] = defaultdict(dict)

        #Get the reference file metadata from the storage system
        #and create a file path that the Dockstore tool runner can
        #used to download the reference file from the storage system
        for switch, file_name in reference_cwl_switch_to_file.iteritems():
            print("switch:{} file name {}".format(switch, file_name))
            file_name_metadata_json = urlopen(str("https://metadata."+self.redwood_host+"/entities?fileName="+file_name), context=ctx).read()
            file_name_metadata = json.loads(file_name_metadata_json)
            print(str(file_name_metadata))
            bundle_uuid = file_name_metadata["content"][0]["gnosId"]
            file_uuid = file_name_metadata["content"][0]["id"]
            file_name = file_name_metadata["content"][0]["fileName"]

            ref_file_path = 'redwood' + '/' + bundle_uuid + '/' + \
                        file_uuid + "/" + file_name
            cnv_jobs['reference_files'][switch] = {"class" : "File", "path" : ref_file_path}
            print(str(cnv_jobs['reference_files'][switch]))



        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:

            print("\n\n\nDonor uuid:%(donor_uuid)s Center Name:%(center_name)s Program:%(program)s Project:%(project)s" % hit["_source"])
            print("Got %d specimens:" % len(hit["_source"]["specimen"]))

            #if a particular center, program or project is requested for processing and
            #the current one  does not match go on to the next sample
            if self.center and (self.center != hit["_source"]["center_name"]):
                continue
            if self.program and (self.program != hit["_source"]["program"]):
                continue
            if self.project and (self.project != hit["_source"]["project"]):
                continue


            for specimen in hit["_source"]["specimen"]:
                print("Next sample of %d samples:" % len(specimen["samples"]))
                for sample in specimen["samples"]:
                    print("Next analysis of %d analysis:" % len(sample["analysis"]))
                    #if a particular sample uuid is requested for processing and
                    #the current sample uuid does not match go on to the next sample
                    #if self.process_sample_uuid and (self.process_sample_uuid != sample["sample_uuid"]):
                    #    continue

                    sample_name = hit["_source"]["submitter_donor_id"]
                    print('sample name (donor id):{}'.format(sample_name))
                    specimen_type = specimen["submitter_specimen_id"][8:]
                    print('specimen type:{}'.format(specimen_type))

                    cnv_jobs['samples'][sample_name]['SAMPLE_ID'] = sample_name


                    workflow_version_dir = self.workflow_version.replace('.', '_')
                    touch_file_path_prefix = self.touch_file_bucket+"/consonance-jobs/CNV_Coordinator/" + workflow_version_dir
                    touch_file_path = touch_file_path_prefix+"/" \
                                       +hit["_source"]["center_name"]+"_" \
                                       +hit["_source"]["program"]+"_" \
                                       +hit["_source"]["project"]

                    #should we remove all white space from the path in the case where i.e. the program name is two works separated by blanks?
                    # remove all whitespace from touch file path
                    #touch_file_path = ''.join(touch_file_path.split())


                    for analysis in sample["analysis"]:
                        #print analysis
                        print("HIT!!!! " + analysis["analysis_type"] + " " + str(hit["_source"]["flags"]["normal_sequence"]) 
                              + " " + str(hit["_source"]["flags"]["tumor_sequence"]) + " " 
                              + specimen["submitter_specimen_type"]+" "+str(specimen["submitter_experimental_design"]))


                        for output in analysis["workflow_outputs"]:
                            print(output)
 
                        if ( (analysis["analysis_type"] == "alignment" and \
                        #if ( (analysis["analysis_type"] == "cnv_variant_calling" and \
                              ((
                                #hit["_source"]["flags"]["normal_cnv_workflow"] == False and \
                                #   sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_cnv_workflow"] and \
                                   re.match("^Normal - ", specimen["submitter_specimen_type"])) or \
                               (
                                #hit["_source"]["flags"]["tumor_cnv_workflow"] == False and \
                                #   sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_cnv_workflow"] and \
                                   re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"]))))):


                            for file in analysis["workflow_outputs"]:
                                print("\nfile type:"+file["file_type"])
                                print("\nfile name:"+file["file_path"])

                                #if (file["file_type"] != "fastq" or
                                #    file["file_type"] != "fastq.gz"):

                                file_path = 'redwood' + '/' + analysis['bundle_uuid'] + '/' + \
                                    self.fileToUUID(file["file_path"], analysis["bundle_uuid"]) + \
                                    "/" + file["file_path"]

                                print('keys for ' + sample_name + ':' + ','.join(cnv_jobs['samples'][sample_name].keys()))

                                if specimen_type not in ['Normal', 'Progression', 'Baseline']:
                                    print("ERROR: unknown specimen type! {}".format(specimen_type))   


                                if specimen_type == 'Normal':
                                    if 'normal_bams' not in cnv_jobs['samples'][sample_name].keys():
                                        cnv_jobs['samples'][sample_name]['normal_bams'] = defaultdict(dict)
                                    cnv_jobs['samples'][sample_name]['normal_bams'][specimen_type]['input_json'] = {"class" : "File", "path" : file_path}

                                    if 'parent_uuids' not in cnv_jobs['samples'][sample_name]['normal_bams'][specimen_type].keys():
                                        cnv_jobs['samples'][sample_name]['normal_bams'][specimen_type]["parent_uuids"] = []
                                
                                    if sample["sample_uuid"] not in cnv_jobs['samples'][sample_name]['normal_bams'][specimen_type]["parent_uuids"]: 
                                        cnv_jobs['samples'][sample_name]['normal_bams'][specimen_type]["parent_uuids"].append(sample["sample_uuid"])

                                elif (specimen_type == 'Baseline' or specimen_type == 'Progression') and file_path.endswith('.bam'):

                                    if 'tumor_bams' not in cnv_jobs['samples'][sample_name].keys():
                                        cnv_jobs['samples'][sample_name]['tumor_bams'] = defaultdict(dict)
                                    cnv_jobs['samples'][sample_name]['tumor_bams'][specimen_type]['input_json'] = {"class" : "File", "path" : file_path}

                                    if 'parent_uuids' not in cnv_jobs['samples'][sample_name]['tumor_bams'][specimen_type].keys():
                                        cnv_jobs['samples'][sample_name]['tumor_bams'][specimen_type]["parent_uuids"] = []
                                
                                    if sample["sample_uuid"] not in cnv_jobs['samples'][sample_name]['tumor_bams'][specimen_type]["parent_uuids"]: 
                                        cnv_jobs['samples'][sample_name]['tumor_bams'][specimen_type]["parent_uuids"].append(sample["sample_uuid"])

                                else:
                                    print("ERROR in spinnaker input!!!", file=sys.stderr)


                            #This metadata will be passed to the Consonance Task and some
                            #some of the meta data will be used in the Luigi status page for the job
                            cnv_jobs['samples'][sample_name]["sample_name"] = sample_name
                            cnv_jobs['samples'][sample_name]["program"] = hit["_source"]["program"]
                            cnv_jobs['samples'][sample_name]["project"] = hit["_source"]["project"]
                            cnv_jobs['samples'][sample_name]["center_name"] = hit["_source"]["center_name"]
                            cnv_jobs['samples'][sample_name]["submitter_donor_id"] = hit["_source"]["submitter_donor_id"]
                            cnv_jobs['samples'][sample_name]["donor_uuid"] = hit["_source"]["donor_uuid"]
                            if "submitter_donor_primary_site" in hit["_source"]:
                                cnv_jobs['samples'][sample_name]["submitter_donor_primary_site"] = hit["_source"]["submitter_donor_primary_site"]
                            else:
                                cnv_jobs['samples'][sample_name]["submitter_donor_primary_site"] = "not provided"
                            #cnv_jobs['samples'][sample_name]["submitter_specimen_id"] = specimen["submitter_specimen_id"]
                            #cnv_jobs['samples'][sample_name]["specimen_uuid"] = specimen["specimen_uuid"]
                            cnv_jobs['samples'][sample_name]["submitter_specimen_type"] = specimen["submitter_specimen_type"]
                            cnv_jobs['samples'][sample_name]["submitter_experimental_design"] = specimen["submitter_experimental_design"]
                            #cnv_jobs['samples'][sample_name]["submitter_sample_id"] = sample["submitter_sample_id"]
                            #cnv_jobs['samples'][sample_name]["sample_uuid"] = sample["sample_uuid"]
                            cnv_jobs['samples'][sample_name]["analysis_type"] = "somatic_variant_calling"
                            cnv_jobs['samples'][sample_name]["workflow_name"] = "quay.io/BD2KGenomics/dockstore_workflow_cnv"
                            cnv_jobs['samples'][sample_name]["workflow_version"] = self.workflow_version

                            print("\nCNV jobs with meta data:", cnv_jobs)


        for sample_num, sample_name in enumerate(cnv_jobs['samples']):
            print('sample num:{}'.format(sample_num))
            print('sample:{}'.format(cnv_jobs['samples'][sample_name]))

            if 'tumor_bams' not in cnv_jobs['samples'][sample_name].keys():
                print("ERROR: no tumor BAM files for sample {}".format(sample_name))
                continue

            if 'normal_bams' not in cnv_jobs['samples'][sample_name].keys():
                print("ERROR: no normal BAM file for sample {}".format(sample_name))
                continue

            #get the JSON dict describing either Progression or Baseline
            #and schedule a job for each with the Normal BAM which is already set up
            print('tumor bams:{}'.format(cnv_jobs['samples'][sample_name]['tumor_bams']))
            print('normal bams:{}'.format(cnv_jobs['samples'][sample_name]['normal_bams']))

            cnv_jobs['samples'][sample_name]['NORMAL_BAM'] = cnv_jobs['samples'][sample_name]['normal_bams']['Normal']['input_json']

            for specimen_type, value  in cnv_jobs['samples'][sample_name]['tumor_bams'].iteritems():
                print("specimen type:{}".format(specimen_type))
                cnv_jobs['samples'][sample_name]['TUMOR_BAM'] = cnv_jobs['samples'][sample_name]['tumor_bams'][specimen_type]['input_json']
                cnv_jobs['samples'][sample_name]['parent_uuids'] = \
                    cnv_jobs['samples'][sample_name]['normal_bams']['Normal']['parent_uuids'] + \
                    cnv_jobs['samples'][sample_name]['tumor_bams'][specimen_type]['parent_uuids']

                cnv_jobs['samples'][sample_name]['ADTEX_OUTCNV'] = sample_name + '_' + specimen_type + '_ADTEX.cnv'
                cnv_jobs['samples'][sample_name]['VARSCAN_OUTCNV'] = sample_name + '_' + specimen_type + '_VARSCAN.cnv'
                full_touch_file_path = touch_file_path + "_" + sample_name + "_" + specimen_type

                if (sample_num < int(self.max_jobs) or int(self.max_jobs) < 0):
                    cnv_job_json = json.dumps(cnv_jobs['samples'][sample_name], sort_keys=True, indent=4, separators=(',', ': '))
                    print("\nmeta data:")
                    print(cnv_job_json)

                    cnv_reference_files_json = json.dumps(cnv_jobs['reference_files'], sort_keys=True, indent=4, separators=(',', ': '))
                    print("\nreference files meta data:")
                    print(cnv_reference_files_json)


                    listOfJobs.append(ConsonanceTask(redwood_host=self.redwood_host, \
                        vm_instance_type=self.vm_instance_type,
                        vm_region = self.vm_region, \
                        redwood_token=self.redwood_token, \
                        image_descriptor=self.image_descriptor, \
                        dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool, \
                        sample_name = sample_name, \
                        specimen_type = specimen_type, \
                        tmp_dir=self.tmp_dir, \
                        workflow_version = self.workflow_version, \
                        #submitter_sample_id = cnv_jobs['samples'][sample_name]['submitter_sample_id'], \
                        cnv_job_json = cnv_job_json, \
                        cnv_reference_files_json = cnv_reference_files_json, \
                        touch_file_path = full_touch_file_path, test_mode=self.test_mode))

            
        print("total of {} jobs; max jobs allowed is {}\n\n".format(str(len(listOfJobs)), self.max_jobs))

        # these jobs are yielded to
        print("\n\n** COORDINATOR REQUIRES DONE!!! **")
        return listOfJobs

    def run(self):
        print("\n\n\n\n** COORDINATOR RUN **")
         # now make a final report
        f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
        print("batch is complete", file=f)
        f.close()
        print("\n\n\n\n** COORDINATOR RUN DONE **")

    def output(self):
        print("\n\n\n\n** COORDINATOR OUTPUT **")
        # the final report
        ts = time.time()
        ts_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/RNASeqTask-%s.txt' % (self.tmp_dir, ts_str))
        workflow_version_dir = self.workflow_version.replace('.', '_')
        return S3Target('s3://'+self.touch_file_bucket+'/consonance-jobs/CNV_Coordinator/{}/CNV-{}.txt'.format(workflow_version_dir, ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"

if __name__ == '__main__':
    luigi.run()


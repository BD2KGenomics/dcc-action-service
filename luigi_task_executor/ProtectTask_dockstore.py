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

from elasticsearch import Elasticsearch

#for hack to get around non self signed certificates
import ssl

#Amazon S3 support for writing touch files to S3
from luigi.s3 import S3Target
#luigi S3 uses boto for AWS credentials
import boto

class DockstoreTask(luigi.Task):

    # TODO : update to reflect protect pipeline parameters.

    json_dict = {}

    redwood_host = luigi.Parameter("storage.ucsc-cgl.org")
    redwood_token = luigi.Parameter("must_be_defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.8")
    target_tool = luigi.Parameter(default="quay.io/ucsc_cgl/protect:2.3.0--1.12.3")
    target_tool_url = luigi.Parameter(default="https://dockstore.org/containers/quay.io/ucsc_cgl/protect")
    workflow_type = luigi.Parameter(default="protect") #does this need to be an argument?
    image_descriptor = luigi.Parameter("must be defined")
 
    genome_fasta = luigi.Parameter(default="")
    genome_fai = luigi.Parameter(default="")
    genome_dict = luigi.Parameter(default="")
    cosmic_vcf = luigi.Parameter(default="")
    cosmic_idx = luigi.Parameter(default="")
    dbsnp_vcf = luigi.Parameter(default="")
    dbsnp_idx = luigi.Parameter(default="")
    dbsnp_tbi = luigi.Parameter(default="")
    strelka_config = luigi.Parameter(default="")
    snpeff = luigi.Parameter(default="")
    transgene = luigi.Parameter(default="")
    phlat = luigi.Parameter(default="")
    mhci = luigi.Parameter(default="")
    mhcii = luigi.Parameter(default="")
    mhc_pathway_assessment = luigi.Parameter(default="")

    tumor_dna = luigi.Parameter(default="")
    if tumor_dna:
        json_dict["tumor_dna"] = {"class" : "File", "path" : tumor_dna}
    tumor_rna = luigi.Parameter(default="")
    if tumor_rna:
        json_dict["tumor_rna"] = {"class" : "File", "path" : tumor_rna}
    normal_dna = luigi.Parameter(default="")
    if normal_dna:
        json_dict["normal_dna"] = {"class" : "File", "path" : normal_dna}
    tumor_dna2 = luigi.Parameter(default="")
    if tumor_dna2:
        json_dict["tumor_dna2"] = {"class" : "File", "path" : tumor_dna2}
    tumor_rna2 = luigi.Parameter(default="")
    if tumor_rna2:
        json_dict["tumor_rna2"] = {"class" : "File", "path" : tumor_rna2}
    normal_dna2 = luigi.Parameter(default="")
    if normal_dna2:
        json_dict["normal_dna2"] = {"class" : "File", "path" : normal_dna2}

    star_path = luigi.Parameter(default="")
    if star_path:
        json_dict["star_path"] = {"class" : "File", "path" : star_path}
    bwa_path = luigi.Parameter(default="")
    if bwa_path:
        json_dict["bwa_path"] = {"class" : "File", "path" : bwa_path}
    rsem_path = luigi.Parameter(default="")
    if rsem_path:
        json_dict["rsem_path"] = {"class" : "File", "path" : rsem_path}

    tmp_dir = luigi.Parameter(default='/datastore') #equivalent of work mount
    sse_key = luigi.Parameter(default="")
    sse_key_is_master = luigi.Parameter(default="False")

    submitter_sample_id = luigi.Parameter(default='must input submitter sample id')
    meta_data_json = luigi.Parameter(default="must input metadata")
    touch_file_path = luigi.Parameter(default='must input touch file path')

    #Consonance will not be called in test mode
    test_mode = luigi.BooleanParameter(default = False)
    test_mode_json_path = luigi.Parameter(default = "")


    def run(self):
        print("\n\n\n** TASK RUN **")
        #get a unique id for this task based on the some inputs
        #this id will not change if the inputs are the same
#        task_uuid = self.get_task_uuid()

#        print "** MAKE TEMP DIR **"
        # create a unique temp dir
        cmd = '''mkdir -p /datastore/%s''' % (self.touch_file_path) #TODO - why is datastore hard-coded?
        print(cmd)
        result = subprocess.call(cmd, shell=True)
        if result != 0:
            print("Unable to access work mount!!")

        if self.test_mode == False: #don't deal with touch files for test run.
            #convert the meta data to a python data structure
            meta_data = json.loads(self.meta_data_json)

            print("** MAKE JSON FOR WORKER **")
            # create a json for RNA-Seq which will be executed by the dockstore-tool-running-dockstore-tool and passed as base64encoded
            # will need to encode the JSON above in this: https://docs.python.org/2/library/base64.html
            # see http://luigi.readthedocs.io/en/stable/api/luigi.parameter.html?highlight=luigi.parameter
            # TODO: this is tied to the requirements of the tool being targeted

            json_str = json.dumps(self.json_dict)
            print("THE JSON: "+json_str)
            # now make base64 encoded version
            base64_json_str = base64.urlsafe_b64encode(json_str)
            print("** MAKE JSON FOR DOCKSTORE TOOL WRAPPER **")

            # create a json for dockstoreRunningDockstoreTool, embed the RNA-Seq JSON as a param
    # below used to be a list of parent UUIDs; which is correct????
    #            "parent_uuids": "[%s]",
            parent_uuids = ','.join(map("{0}".format, self.parent_uuids))

            print("parent uuids:%s" % parent_uuids)

            p = self.save_dockstore_json().open('w')
            p_local = self.save_dockstore_json_local().open('w')

            dockstore_json_str = '''{
                "json_encoded": "%s",
                "docker_uri": "%s",
                "dockstore_url": "%s",
                "redwood_token": "%s",
                "redwood_host": "%s",
                "parent_uuids": "%s",
                "workflow_type": "%s",
                "tmpdir": "%s",
                "vm_instance_type": "c4.8xlarge",
                "vm_region": "us-west-2",
                "vm_location": "aws",
                "vm_instance_cores": 36,
                "vm_instance_mem_gb": 60,
                "output_metadata_json": "/tmp/final_metadata.json"
            }''' % (base64_json_str, self.target_tool, self.target_tool_url, self.redwood_token, self.redwood_host, parent_uuids, self.workflow_type, self.tmp_dir )

            print(dockstore_json_str, file=p)
            p.close()
        
            # write the parameterized JSON for input to Consonance
            # to a local file since Consonance cannot read files on s3
            print(dockstore_json_str, file=p_local)
            p_local.close()

        # execute consonance run, parse the job UUID

        if self.test_mode == False:
            print("** SUBMITTING TO DOCKSTORE **")
            print("executing:"+ ' '.join(cmd))
            print("** WAITING FOR DOCKSTORE **")

            cmd = ["dockstore", "tool", "launch", "--entry", self.dockstore_tool_running_dockstore_tool, "--local-entry", "--json", self.save_dockstore_json_local().path]

            try:
                pass
                #consonance_output_json = subprocess.check_output(cmd)
            except subprocess.CalledProcessError as e:
                #If we get here then the called command return code was non zero
                print("\nERROR!!! DOCKSTORE CALL: " + cmd + " FAILED !!!", file=sys.stderr)
                print("\nReturn code:" + str(e.returncode), file=sys.stderr)

                return_code = e.returncode
                sys.exit(return_code)
            except Exception as e:
                print("\nERROR!!! DOCKSTORE CALL: " + cmd + " THREW AN EXCEPTION !!!", file=sys.stderr)
                print("\nException information:" + str(e), file=sys.stderr)
                #if we get here the called command threw an exception other than just
                #returning a non zero return code, so just set the return code to 1
                return_code = 1
                sys.exit(return_code)

            #print("Dockstore output is:\n\n{}\n--end consonance output---\n\n".format(consonance_output_json))

            #get consonance job uuid from output of consonance command
            #consonance_output = json.loads(consonance_output_json)            
            #if "job_uuid" in consonance_output:
            #    meta_data["consonance_job_uuid"] = consonance_output["job_uuid"]
            else:
                print("ERROR: COULD NOT FIND CONSONANCE JOB UUID IN CONSONANCE OUTPUT!", file=sys.stderr)
        else:
            cmd = ["dockstore", "tool", "launch", "--entry", self.dockstore_tool_running_dockstore_tool, "--local-entry", "--json", self.test_mode_json_path]
            if self.test_mode_json_path:
                print("EXECUTING TEST DOCKSTORE COMMAND:" + ' '.join(cmd))
                try:
                    subprocess.check_call(cmd)
                except Exception as e:
                    print("DOCKSTORE THREW EXCEPTION: {}".format(e))
            else:
                print("TEST MODE: Dockstore command would be:"+ ' '.join(cmd))
            #meta_data["consonance_job_uuid"] = 'no consonance id in test mode'

        #remove the local parameterized JSON file that
        #was created for the Consonance call
        #since the Consonance call is finished
#        self.save_dockstore_json_local().remove()

        #convert the meta data to a string and
        #save the donor metadata for the sample being processed to the touch
        # file directory

        ## removed until we start scheduling jobs using decider.
        #meta_data_json = json.dumps(meta_data)
        #m = self.save_metadata_json().open('w')
        #print(meta_data_json, file=m)
        #m.close()

            
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
        return S3Target('s3://%s/%s_meta_data.json' % (self.touch_file_path, self.submitter_sample_id ))

    def save_dockstore_json_local(self):
        #task_uuid = self.get_task_uuid()
        #luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/dockstore_tool.json' % ( task_uuid))
        #return S3Target('%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))
        return luigi.LocalTarget('/datastore/%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))

    def save_dockstore_json(self):
        #task_uuid = self.get_task_uuid()
        #luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/dockstore_tool.json' % ( task_uuid))
        return S3Target('s3://%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))

    def output(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/finished.txt' % (self.tmp_dir, task_uuid))
        #return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/RNASeq_3_1_x_Coordinator/%s/finished.txt' % ( task_uuid))
        return S3Target('s3://%s/%s_finished.json' % (self.touch_file_path, self.submitter_sample_id ))

class ProtectCoordinator(luigi.Task):

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

    workflow_version = luigi.Parameter(default="3.2.1-1")
    touch_file_bucket = luigi.Parameter(default="must be input")

    vm_region = luigi.Parameter(default='us-east-1')

    #Consonance will not be called in test mode
    test_mode = luigi.BoolParameter(default = False)

    #in order to test using locally stored data
    test_mode_json_path = luigi.Parameter(default = "")


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
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc

        listOfJobs = []

        if not self.test_mode or not self.test_mode_json_path:
            #todo - double check this
            res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_protect_workflow_2_3_x" : "false"}},{"term" : {"flags.tumor_protect_workflow_2_3_x" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)

            print("Got %d Hits:" % res['hits']['total'])
            for hit in res['hits']['hits']:

                print("\n\n\nDonor uuid:%(donor_uuid)s Center Name:%(center_name)s Program:%(program)s Project:%(project)s" % hit["_source"])
                print("Got %d specimens:" % len(hit["_source"]["specimen"]))

                for specimen in hit["_source"]["specimen"]:
                    print("Next sample of %d samples:" % len(specimen["samples"]))
                    for sample in specimen["samples"]:
                        print("Next analysis of %d analysis:" % len(sample["analysis"]))
                        #if a particular sample uuid is requested for processing and
                        #the current sample uuid does not match go on to the next sample
                        if self.process_sample_uuid and (self.process_sample_uuid != sample["sample_uuid"]):
                            continue

                        for analysis in sample["analysis"]:

                            for output in analysis["workflow_outputs"]:
                                print(output)
     
                            if ( (analysis["analysis_type"] == "sequence_upload" and \
                                  ((hit["_source"]["flags"]["normal_protect_workflow_2_3_x"] == False and \
                                       sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_protect_workflow_2_3_x"] and \
                                       re.match("^Normal - ", specimen["submitter_specimen_type"])) or \
                                   (hit["_source"]["flags"]["tumor_protect_workflow_2_3_x"] == False and \
                                       sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_protect_workflow_2_3_x"] and \
                                       re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"])))) or \

                                 #if the workload has already been run but we have no
                                 #output from the workload run it again
                                 (analysis["analysis_type"] == "sequence_upload" and \
                                  ((hit["_source"]["flags"]["normal_protect_workflow_2_3_x"] == True and \
                                       (sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_protect_workflow_2_3_x"] or \
                                       (sample["sample_uuid"] in hit["_source"]["present_items"]["normal_protect_workflow_2_3_x"] and 
                                                                                             (True))) and \
                                       re.match("^Normal - ", specimen["submitter_specimen_type"])) or \
                                   (hit["_source"]["flags"]["tumor_protect_workflow_2_3_x"] == True and \
                                       (sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_protect_workflow_2_3_x"] or \
                                       (sample["sample_uuid"] in hit["_source"]["present_items"]["tumor_protect_workflow_2_3_x"] and 
                                                                                             (True))) and \
                                       re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"])))) ):

                                
    #                            touch_file_path = os.path.join(self.touch_file_path_prefix, hit["_source"]["center_name"] + "_" + hit["_source"]["program"] \
    #                                                                    + "_" + hit["_source"]["project"] + "_" + hit["_source"]["submitter_donor_id"] \
    #                                                                    + "_" + specimen["submitter_specimen_id"])


                                touch_file_path = self.touch_file_path_prefix+"/"+hit["_source"]["center_name"]+"_"+hit["_source"]["program"] \
                                                                        +"_"+hit["_source"]["project"]+"_"+hit["_source"]["submitter_donor_id"] \
                                                                        +"_"+specimen["submitter_specimen_id"]
                                submitter_sample_id = sample["submitter_sample_id"]

                                #This metadata will be passed to the Consonance Task and some
                                #some of the meta data will be used in the Luigi status page for the job
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
                                meta_data["submitter_sample_id"] = sample["submitter_sample_id"]
                                meta_data["sample_uuid"] = sample["sample_uuid"]
                                meta_data["analysis_type"] = "protect_immunology" #TODO: change protect_immunology
                                meta_data["workflow_name"] = "quay.io/ucsc_cgl/protect"
                                meta_data["workflow_version"] = "2.3.0"

                                meta_data_json = json.dumps(meta_data)
                                print("meta data:")
                                print(meta_data_json)


                                #print analysis
                                print("HIT!!!! " + analysis["analysis_type"] + " " + str(hit["_source"]["flags"]["normal_protect"]) 
                                               + " " + str(hit["_source"]["flags"]["tumor_protect"]) + " " 
                                               + specimen["submitter_specimen_type"]+" "+str(specimen["submitter_experimental_design"]))


                                paired_files = []
                                paired_file_uuids = []
                                paired_bundle_uuids = []

                                single_files = []
                                single_file_uuids = []
                                single_bundle_uuids = []

                                tar_files = []
                                tar_file_uuids = []
                                tar_bundle_uuids = []

                                parent_uuids = {}

                                for file in analysis["workflow_outputs"]:
                                    print("file type:"+file["file_type"])
                                    print("file name:"+file["file_path"])

                                    if (file["file_type"] == "fastq" or
                                        file["file_type"] == "fastq.gz"):
                                            #if there is only one sequenc upload output then this must
                                            #be a single read sample
                                            if( len(analysis["workflow_outputs"]) == 1): 
                                                print("adding %s of file type %s to files list" % (file["file_path"], file["file_type"]))
                                                single_files.append(file["file_path"])
                                                single_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                                single_bundle_uuids.append(analysis["bundle_uuid"])
                                                parent_uuids[sample["sample_uuid"]] = True
                                            #otherwise we must be dealing with paired reads
                                            else: 
                                                print("adding %s of file type %s to files list" % (file["file_path"], file["file_type"]))
                                                paired_files.append(file["file_path"])
                                                paired_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                                paired_bundle_uuids.append(analysis["bundle_uuid"])
                                                parent_uuids[sample["sample_uuid"]] = True
                                    elif (file["file_type"] == "fastq.tar"):
                                        print("adding %s of file type %s to files list" % (file["file_path"], file["file_type"]))
                                        tar_files.append(file["file_path"])
                                        tar_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                        tar_bundle_uuids.append(analysis["bundle_uuid"])
                                        parent_uuids[sample["sample_uuid"]] = True

                                if len(listOfJobs) < int(self.max_jobs) and (len(paired_files) + len(tar_files) + len(single_files)) > 0:

                                    if len(tar_files) > 0 and (len(paired_files) > 0 or len(single_files) > 0):
                                        print(('\n\nWARNING: mix of tar files and fastq(.gz) files submitted for' 
                                                            ' input for one sample! This is probably an error!'), file=sys.stderr)
                                        print('WARNING: files were\n paired {}\n tar: {}\n single:{}'.format(', '.join(map(str, paired_files)),
                                                                                                              ', '.join(map(str, tar_files)),
                                                                                                              ', '.join(map(str, single_files))), file=sys.stderr)
                                        print('WARNING: sample uuid:{}'.format(parent_uuids.keys()[0]), file=sys.stderr)
                                        print('WARNING: Skipping this job!\n\n', file=sys.stderr)
                                        continue

                                    elif len(paired_files) > 0 and len(single_files) > 0:
                                        print('\n\nWARNING: mix of single and paired fastq(.gz) files submitted for'
                                                       ' input for one sample! This is probably an error!', file=sys.stderr)
                                        print('WARNING: files were\n paired {}\n single:{}'.format(', '.join(map(str, paired_files)),
                                                                                                   ', '.join(map(str, single_files))), file=sys.stderr)
                                        print('WARNING: sample uuid:{}\n'.format(parent_uuids.keys()[0]), file=sys.stderr)
                                        print('WARNING: Skipping this job!\n\n', file=sys.stderr)
                                        continue
     
                                    elif len(tar_files) > 1:
                                        print('\n\nWARNING: More than one tar file submitted for'
                                                       ' input for one sample! This is probably an error!', file=sys.stderr)
                                        print('WARNING: files were\n tar: %s'.format(', '.join(map(str, tar_files))), file=sys.stderr)
                                        print('WARNING: sample uuid:%s'.format(parent_uuids.keys()[0]), file=sys.stderr)
                                        print('WARNING: Skipping this job!\n\n', file=sys.stderr)
                                        continue 

                                    elif len(paired_files) % 2 != 0:
                                        print('\n\nWARNING: Odd number of paired files submitted for'
                                                       ' input for one sample! This is probably an error!', file=sys.stderr)
                                        print('WARNING: files were\n paired: %s'.format(', '.join(map(str, paired_files))), file=sys.stderr)
                                        print('WARNING: sample uuid:%s'.format(parent_uuids.keys()[0]), file=sys.stderr)
                                        print('WARNING: Skipping this job!\n\n', file=sys.stderr)
                                        continue 

                                    else:
                                       print("will run report for {} and {} and {}".format(', '.join(map(str, paired_files)), 
                                                                                           ', '.join(map(str, tar_files)), 
                                                                                           ', '.join(map(str, single_files))))
                                       print("total of {} files in this {} job; job {} of {}".format(str(len(paired_files) + (len(tar_files) + len(single_files))), 
                                                                                                hit["_source"]["program"], str(len(listOfJobs)+1), str(self.max_jobs)))
                                       listOfJobs.append(DockstoreTask(redwood_host=self.redwood_host, redwood_token=self.redwood_token, \
                                            image_descriptor=self.image_descriptor, dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool, \
                                            parent_uuids = parent_uuids.keys(), \
                                            single_filenames=single_files, single_file_uuids = single_file_uuids, single_bundle_uuids = single_bundle_uuids, \
                                            paired_filenames=paired_files, paired_file_uuids = paired_file_uuids, paired_bundle_uuids = paired_bundle_uuids, \
                                            tar_filenames=tar_files, tar_file_uuids = tar_file_uuids, tar_bundle_uuids = tar_bundle_uuids, \
                                            tmp_dir=self.tmp_dir, submitter_sample_id = submitter_sample_id, meta_data_json = meta_data_json, \
                                            touch_file_path = touch_file_path, test_mode=self.test_mode, test_mode_json_path=self.test_mode_json_path))

        else:
            print("=========SUBMITTING A TEST JOB AS DOCKSTORE TASK========")
            listOfJobs.append(DockstoreTask(redwood_host=self.redwood_host, redwood_token=self.redwood_token, \
                image_descriptor=self.image_descriptor, dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool, \
                touch_file_path ="cgl-core-analysis-run-touch-files/consonance-jobs/Protect/1_0_1/thomas_protect_test", test_mode=True, test_mode_json_path=self.test_mode_json_path))
            
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
        return S3Target('s3://cgl-core-analysis-run-touch-files/consonance-jobs/Protect/ProtectTask-%s.txt' % (ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"


if __name__ == '__main__':
    luigi.run()

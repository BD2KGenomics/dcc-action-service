"""
A test to see if the RNA-seq workflow can be run using airflow.
This file will be a "base" for workflows using airflow?
"""

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

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

default_args = {
    'es_index_host' : 'localhost',
    'es_index_port' : '9200',
    'redwood_token' : 'must_be_defined',
    'redwood_host' : 'storage.ucsc-cgl.org',
    'image_descriptor' : 'must_be_defined',
    'dockstore_tool_running_dockstore_tool' : 'quay.io/ucsc_cgl/dockstore-tool-runner:1.0.8',
    'tmp_dir' : '/datastore',
    'max_jobs' : 1,
    'touch_file_bucket' : 'must_be_defined',
    'workflow_name' : 'RNA_Seq_airflow',
    'workflow_path' : './airflow_executors',
    'workflow_version' : '3.2.1-1',
    'process_sample_uuid' : "",
    'start_date': airflow.utils.dates.days_ago(2),
    'test_mode' : False,
}

base_dag = DAG(
    dag_id='rnaseq',
    default_args=default_args,
    schedule_interval=timedelta(hours=1)
)

#save jobs
jobs = []

def populate_jobs():
    #for each iteration of the decider clear jobs (so there aren't any left)
    print "attempt to populate jobs beggining now"
    jobs = []

    bundle_uuid_filename_to_file_uuid = {} #used to 'translate' bundle uuid to file uuid
    def fileToUUID(self, input, bundle_uuid):
        return bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]


    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    json_str = urlopen(str("metadata."+default_args['redwood_host']+"/entities?page=0"), context=ctx).read()

    metadata_struct = json.loads(json_str)
    for i in range(0, metadata_struct["totalPages"]):
        print("** CURRENT METADATA TOTAL PAGES: "+str(i))
        json_str = urlopen(str("metadata."+default_args['redwood_host']+"/entities?page="+str(i)), context=ctx).read()
        metadata_struct = json.loads(json_str)
        for file_hash in metadata_struct["content"]:
            bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

    # now query elasticsearch
    es = Elasticsearch([{'host': default_args['es_index_host'], 'port': default_args['es_index_port']}])
    res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_rna_seq_cgl_workflow_3_0_x" : "false"}},{"term" : {"flags.tumor_rna_seq_cgl_workflow_3_0_x" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)

    for hit in res['hits']['hits']:
        if hit["_source"]["program"] == "PROTECT_NBL":
            continue
        for specimen in hit["_source"]["specimen"]:
            for sample in specimen["samples"]:
                if default_args['process_sample_uuid'] and (default_args['process_sample_uuid'] != sample["sample_uuid"]):
                    continue
                for analysis in sample["analysis"]:
                    for output in analysis["workflow_outputs"]:
                        print(output)
                    rna_seq_outputs_len = 0
                    
                    for filter_analysis in sample["analysis"]:
                            if filter_analysis["analysis_type"] == "rna_seq_quantification":
                                rna_seq_outputs = filter_analysis["workflow_outputs"] 
                                rna_seq_outputs_len = len(filter_analysis["workflow_outputs"])
                                rna_seq_workflow_version = filter_analysis["workflow_version"] 

                    if ( (analysis["analysis_type"] == "sequence_upload" and 
                          ((hit["_source"]["flags"]["normal_rna_seq_cgl_workflow_3_0_x"] == False and 
                               (rna_seq_outputs_len == 0 or (rna_seq_workflow_version != default_args['workflow_version'])) and 
                               sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_rna_seq_cgl_workflow_3_0_x"] and 
                               re.match("^Normal - ", specimen["submitter_specimen_type"]) and 
                               re.match("^RNA-Seq$", specimen["submitter_experimental_design"])) or 
                           (hit["_source"]["flags"]["tumor_rna_seq_cgl_workflow_3_0_x"] == False and 
                               (rna_seq_outputs_len == 0 or rna_seq_workflow_version != default_args['workflow_version']) and 
                               sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_rna_seq_cgl_workflow_3_0_x"] and 
                               re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"]) and 
                               re.match("^RNA-Seq$", specimen["submitter_experimental_design"])))) or 

                         (analysis["analysis_type"] == "sequence_upload" and \
                          ((hit["_source"]["flags"]["normal_rna_seq_cgl_workflow_3_0_x"] == True and \
                               (sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_rna_seq_cgl_workflow_3_0_x"] or \
                               (sample["sample_uuid"] in hit["_source"]["present_items"]["normal_rna_seq_cgl_workflow_3_0_x"] and 
                                                                                     (rna_seq_outputs_len == 0 or rna_seq_workflow_version != default_args['workflow_version']))) and \
                               re.match("^Normal - ", specimen["submitter_specimen_type"]) and \
                               re.match("^RNA-Seq$", specimen["submitter_experimental_design"])) or \
                           (hit["_source"]["flags"]["tumor_rna_seq_cgl_workflow_3_0_x"] == True and \
                               (sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_rna_seq_cgl_workflow_3_0_x"] or \
                               (sample["sample_uuid"] in hit["_source"]["present_items"]["tumor_rna_seq_cgl_workflow_3_0_x"] and 
                                                                                     (rna_seq_outputs_len == 0 or rna_seq_workflow_version != default_args['workflow_version']))) and \
                               re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"]) and \
                               re.match("^RNA-Seq$", specimen["submitter_experimental_design"])))) ):

                        

                        workflow_version_dir = default_args['workflow_version'].replace('.', '_') 
                        touch_file_path_prefix = "self.touch_file_bucket/consonance-jobs/RNASeq_Coordinator/" + workflow_version_dir
                        touch_file_path = touch_file_path_prefix+"/"+hit["_source"]["center_name"]+"_"+hit["_source"]["program"] \
                                                                +"_"+hit["_source"]["project"]+"_"+hit["_source"]["submitter_donor_id"] \
                                                                +"_"+specimen["submitter_specimen_id"]

                        submitter_sample_id = sample["submitter_sample_id"]

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
                        meta_data["analysis_type"] = "rna_seq_quantification"
                        meta_data["workflow_name"] = "quay.io/ucsc_cgl/rnaseq-cgl-pipeline"
                        meta_data["workflow_version"] = default_args['workflow_version']

                        meta_data_json = json.dumps(meta_data)

                        #print analysis
                        print("HIT!!!! " + analysis["analysis_type"] + " " + str(hit["_source"]["flags"]["normal_rna_seq_quantification"]) 
                                       + " " + str(hit["_source"]["flags"]["tumor_rna_seq_quantification"]) + " " 
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
                                        single_file_uuids.append(fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                        single_bundle_uuids.append(analysis["bundle_uuid"])
                                        parent_uuids[sample["sample_uuid"]] = True
                                    #otherwise we must be dealing with paired reads
                                    else: 
                                        print("adding %s of file type %s to files list" % (file["file_path"], file["file_type"]))
                                        paired_files.append(file["file_path"])
                                        paired_file_uuids.append(fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                        paired_bundle_uuids.append(analysis["bundle_uuid"])
                                        parent_uuids[sample["sample_uuid"]] = True
                            elif (file["file_type"] == "fastq.tar"):
                                print("adding %s of file type %s to files list" % (file["file_path"], file["file_type"]))
                                tar_files.append(file["file_path"])
                                tar_file_uuids.append(fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                tar_bundle_uuids.append(analysis["bundle_uuid"])
                                parent_uuids[sample["sample_uuid"]] = True

                        if len(listOfJobs) < int(default_args['max_jobs']) and (len(paired_files) + len(tar_files) + len(single_files)) > 0:

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
                                 print('WARNING: Skipping this job!\n\n', file=stderr)
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
                                                                                         hit["_source"]["program"], str(len(listOfJobs)+1), str(default_args['max_jobs'])))

                                jobs.append({'redwood_host' : default_args['redwood_host'], 'redwood_token' : default_args['redwood_token'], \
                                     'es_index_host' : default_args['es_index_host'], 'es_index_port' : es.index_port, \
                                     'image_descriptor' : default_args['image_descriptor'], 'dockstore_tool_running_dockstore_tool' : default_args['dockstore_tool_running_dockstore_tool'], \
                                     'parent_uuids' : parent_uuids.keys(), \
                                     'single_filenames' : single_files, 'single_file_uuids' : single_file_uuids, 'single_bundle_uuids' : single_bundle_uuids, \
                                     'paired_filenames' : paired_files, 'paired_file_uuids' : paired_file_uuids, 'paired_bundle_uuids' : paired_bundle_uuids, \
                                     'tar_filenames' : tar_files, 'tar_file_uuids' : tar_file_uuids, 'tar_bundle_uuids' : tar_bundle_uuids, \
                                     'tmp_dir' : default_args['tmp_dir'], 'submitter_sample_id' : submitter_sample_id, 'meta_data_json' : meta_data_json, \
                                     'touch_file_path' : touch_file_path, 'workflow_version' : default_args['workflow_version'], 'test_mode' : default_args['test_mode']})




    return "Found "+str(len(jobs))+" jobs to be run through "+default_args['workflow_name']

def execute_workflow():
    print "beggining execution of workflow"
    print (jobs)

    for i in range(0, min(len(jobs), int(default_args['max_jobs']))):

        args = jobs[i]

        local_json_dir = "/datastore/" + args['touch_file_path']
        cmd = ["mkdir", "-p", local_json_dir ]
        print(cmd)
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as e:
            #If we get here then the called command return code was non zero
            print("\nERROR!!! MAKING LOCAL JSON DIR : " + cmd + " FAILED !!!", file=sys.stderr)
            print("\nReturn code:" + str(e.returncode), file=sys.stderr)
            return_code = e.returncode
            sys.exit(return_code)
        except Exception as e:
            print("\nERROR!!! MAKING LOCAL JSON DIR : " + cmd + " THREW AN EXCEPTION !!!", file=sys.stderr)
            print("\nException information:" + str(e), file=sys.stderr)
            #if we get here the called command threw an exception other than just
            #returning a non zero return code, so just set the return code to 1
            return_code = 1
            sys.exit(return_code)

        meta_data = json.loads(args['meta_data_json'])

        print("** MAKE JSON FOR WORKER **")
        # create a json for RNA-Seq which will be executed by the dockstore-tool-running-dockstore-tool and passed as base64encoded
        # will need to encode the JSON above in this: https://docs.python.org/2/library/base64.html
        # see http://luigi.readthedocs.io/en/stable/api/luigi.parameter.html?highlight=luigi.parameter
        # TODO: this is tied to the requirements of the tool being targeted
        json_str = '''
{
'''
        if len(args['paired_filenames']) > 0:
            json_str += '''
"sample-paired": [
        '''
            i = 0
            while i<len(args['paired_filenames']):
                # append file information
                json_str += '''
            {
              "class": "File",
              "path": "redwood://%s/%s/%s/%s"
            }''' % (args['redwood_host'], args['paired_bundle_uuids'][i], args['paired_file_uuids'][i], args['paired_filenames'][i])
                if i < len(args['paired_filenames)']) - 1:
                   json_str += ","
                i += 1
            json_str += '''
  ],
            '''

        if len(args['single_filenames']) > 0:
            json_str += '''
"sample-single": [
        '''
            i = 0
            while i<len(args['single_filenames']):
                # append file information
                json_str += '''
            {
               "class": "File",
               "path": "redwood://%s/%s/%s/%s"
            }''' % (args['redwood_host'], args['single_bundle_uuids'][i], args['single_file_uuids'][i], args['single_filenames'][i])
                if i < len(args['single_filenames']) - 1:
                    json_str += ","
                i += 1
            json_str += '''
  ],
            '''

        if len(args['tar_filenames']) > 0:
            json_str += '''
"sample-tar": [
        '''
            i = 0
            while i<len(args['tar_filenames']):
                # append file information
                json_str += '''
            {
              "class": "File",
              "path": "redwood://%s/%s/%s/%s"
            }''' % (args['redwood_host'], args['tar_bundle_uuids'][i], args['tar_file_uuids'][i], args['tar_filenames'][i])
                if i < len(args['tar_filenames']) - 1:
                    json_str += ","
                i += 1
            json_str += '''
  ],
            '''



        json_str += '''
"rsem":
  {
    "class": "File",
    "path": "%s"
  },
            ''' %  (args['rsemfilename'])

        json_str += '''
"star":
  {
    "class": "File",
    "path": "%s"
  },
            ''' % (args['starfilename'])


        json_str += '''
"kallisto":
  {
    "class": "File",
    "path": "%s"
  },
            ''' % (args['kallistofilename'])

        json_str += '''
"save-wiggle": %s,
''' % args['save_wiggle']

        json_str += '''
"no-clean": %s,
''' % args['no_clean']

        json_str += '''
"save-bam": %s,
''' % args['save_bam']

        json_str += '''
"disable-cutadapt": %s,
''' % args['disable_cutadapt']

        json_str += '''
"resume": "%s",
''' % args['resume']

        json_str += '''
"cores": %d,
''' % args['cores']

        json_str += '''
"work-mount": "%s",
 ''' % args['tmp_dir']

        json_str += '''
"bamqc": %s,
''' % args['bamqc']

        json_str += '''
"output-basename": "%s",
''' % args['submitter_sample_id']

        json_str += '''
"output_files": [
        '''
        new_filename = args['submitter_sample_id'] + '.tar.gz'
        json_str += '''
    {
      "class": "File",
      "path": "/tmp/%s"
    }''' % (new_filename)
 

        json_str += '''
  ]'''


        # if the user wants to save the wiggle output file
        if args['save_wiggle'] == 'true':
            json_str += ''',

"wiggle_files": [
        '''
            new_filename = args['submitter_sample_id'] + '.wiggle.bg'
            json_str += '''
    {
      "class": "File",
      "path": "/tmp/%s"
    }''' % (new_filename)
 
            json_str += '''
  ]'''

        # if the user wants to save the BAM output file
        if args['save_bam'] == 'true':
            json_str += ''',

"bam_files": [
        '''
            new_filename = args['submitter_sample_id'] + '.sortedByCoord.md.bam'
            json_str += '''
    {
      "class": "File",
      "path": "/tmp/%s"
    }''' % (new_filename)
 
            json_str += '''
  ]'''


        json_str += '''
}
'''

        print("THE JSON: "+json_str)
        # now make base64 encoded version
        base64_json_str = base64.urlsafe_b64encode(json_str)
        print("** MAKE JSON FOR DOCKSTORE TOOL WRAPPER **")
        # create a json for dockstoreRunningDockstoreTool, embed the RNA-Seq JSON as a param
# below used to be a list of parent UUIDs; which is correct????
#            "parent_uuids": "[%s]",
        parent_uuids = ','.join(map("{0}".format, args['parent_uuids']))

        print("parent uuids:%s" % parent_uuids)

        #TODO: save json???
        #def save_dockstore_json(self):
        #    return S3Target('s3://%s/%s_dockstore_tool.json' % (args['touch_file_path'], args['submitter_sample_id'] ))

        #def save_dockstore_json_local(self):
        #    return luigi.LocalTarget('/datastore/%s/%s_dockstore_tool.json' % (args['touch_file_path'], args['submitter_sample_id'] ))

        #p = save_dockstore_json().open('w')
        #p_local = self.save_dockstore_json_local().open('w')

        target_tool= args['target_tool_prefix'] + ":" + args['workflow_version']

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
        }''' % (base64_json_str, target_tool, args['target_tool_url'], args['redwood_token'], args['redwood_host'], parent_uuids, args['workflow_type'], args['tmp_dir'] )

        print(dockstore_json_str, file=p)
        p.close()
    
        # write the parameterized JSON for input to Consonance
        # to a local file since Consonance cannot read files on s3
        print(dockstore_json_str, file=p_local)
        p_local.close()

        # execute consonance run, parse the job UUID
        #TODO : fix local json printing!!
#        cmd = ["consonance", "run", "--image-descriptor", self.image_descriptor, "--flavour", "c4.8xlarge", "--run-descriptor", self.save_dockstore_json().path]
        cmd = ["consonance", "run", "--image-descriptor", args['image_descriptor'], "--flavour", "c4.8xlarge", "--run-descriptor", self.save_dockstore_json_local().path]

        if self.test_mode == False:
            print("** SUBMITTING TO CONSONANCE **")
            print("executing:"+ ' '.join(cmd))
            print("** WAITING FOR CONSONANCE **")

            try:
                consonance_output_json = subprocess.check_output(cmd)
            except subprocess.CalledProcessError as e:
                #If we get here then the called command return code was non zero
                print("\nERROR!!! CONSONANCE CALL: " + cmd + " FAILED !!!", file=sys.stderr)
                print("\nReturn code:" + str(e.returncode), file=sys.stderr)

                return_code = e.returncode
                sys.exit(return_code)
            except Exception as e:
                print("\nERROR!!! CONSONANCE CALL: " + cmd + " THREW AN EXCEPTION !!!", file=sys.stderr)
                print("\nException information:" + str(e), file=sys.stderr)
                #if we get here the called command threw an exception other than just
                #returning a non zero return code, so just set the return code to 1
                return_code = 1
                sys.exit(return_code)

            print("Consonance output is:\n\n{}\n--end consonance output---\n\n".format(consonance_output_json))

            #get consonance job uuid from output of consonance command
            consonance_output = json.loads(consonance_output_json)            
            if "job_uuid" in consonance_output:
                meta_data["consonance_job_uuid"] = consonance_output["job_uuid"]
            else:
                print("ERROR: COULD NOT FIND CONSONANCE JOB UUID IN CONSONANCE OUTPUT!", file=sys.stderr)
        else:
            print("TEST MODE: Consonance command would be:"+ ' '.join(cmd))
            meta_data["consonance_job_uuid"] = 'no consonance id in test mode'

        #remove the local parameterized JSON file that
        #was created for the Consonance call
        #since the Consonance call is finished
        #self.save_dockstore_json_local().remove()

        #convert the meta data to a string and
        #save the donor metadata for the sample being processed to the touch
        # file directory
        meta_data_json = json.dumps(meta_data)
        #m = self.save_metadata_json().open('w')
        print(meta_data_json, file=m)
        m.close()

            
#        if result == 0:
#            cmd = "rm -rf "+self.data_dir+"/"+self.bundle_uuid+"/bamstats_report.zip "+self.data_dir+"/"+self.bundle_uuid+"/datastore/"
#            print "CLEANUP CMD: "+cmd
#            result = subprocess.call(cmd, shell=True)
#            if result == 0:
#                print "CLEANUP SUCCESSFUL"

         # NOW MAke a final report
        #f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
        print("Consonance task is complete", file=f) 
        #f.close()
        print("\n\n\n\n** TASK RUN DONE **")

    return str(min(len(jobs), int(default_args['max_jobs']))) + " jobs executed"


#TODO : figure out structure for workflows
""" scan metadata, if jobs are returned run them with dockstore or consonance """
scan_metadata = PythonOperator(
    task_id='scan_metadata',
    python_callable=populate_jobs,
    dag=base_dag)

""" run specified workflow via tool of choice (dockstore, consonance, ???) """
run_execution_tool = PythonOperator(
    task_id='run_execution_tool',
    python_callable=execute_workflow,
    dag=base_dag)

#so that metadata is scanned before consonance/dockstore are run
scan_metadata.set_downstream(run_execution_tool)

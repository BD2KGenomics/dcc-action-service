from __future__ import print_function, division

import json
import time
import re
import datetime
import subprocess
import base64
from urllib import urlopen

import os
import sys
from collections import defaultdict
import copy

#from elasticsearch import Elasticsearch

#for hack to get around non self signed certificates
import ssl

#luigi S3 uses boto for AWS credentials
import boto

import ConfigParser
import requests

class ConsonanceTask(object):

    def __init__(self, redwood_host, redwood_token, dockstore_tool_running_dockstore_tool, \
                 cgp_pipeline_job_metadata_str, touch_file_path, metadata_json_file_name, \
                 workflow_version, file_prefix, vm_instance_type ='c4.8xlarge', \
                 vm_region = 'us-west-2', tmp_dir = '/datastore', auto_scale = False, test_mode = False):

        self.redwood_host = redwood_host
        self.redwood_token = redwood_token
        self.dockstore_tool_running_dockstore_tool = dockstore_tool_running_dockstore_tool
        self.workflow_version = workflow_version
        self.vm_instance_type = vm_instance_type
        self.vm_region = vm_region
        self.tmp_dir = tmp_dir #equivalent of work mount
    
        self.cgp_pipeline_job_metadata_str = cgp_pipeline_job_metadata_str
    #    cgp_pipeline_job_metadata = json.loads(self.cgp_pipeline_job_metadata_str)
    
        self.touch_file_path = touch_file_path
        self.metadata_json_file_name = metadata_json_file_name
        self.file_prefix = file_prefix

        #auto scale is set in the pipeline class derived from base_Coordinator
        self.auto_scale = auto_scale    
        #Consonance will not be called in test mode
        self.test_mode = test_mode
    

    '''
    redwood_host = luigi.Parameter("storage system name such as storage.ucsc-cgp.org must be input")
    redwood_token = luigi.Parameter("must_be_defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="must enter quay.io path to dockstore tool runner and version")
    workflow_version = luigi.Parameter(default="must be defined")
    vm_instance_type = luigi.Parameter(default='c4.8xlarge')
    vm_region = luigi.Parameter(default='us-west-2')
    tmp_dir = luigi.Parameter(default='/datastore') #equivalent of work mount

    cgp_pipeline_job_metadata_str = luigi.Parameter(default="must input metadata")
#    cgp_pipeline_job_metadata = json.loads(self.cgp_pipeline_job_metadata_str)

    touch_file_path = luigi.Parameter(default='must input touch file path')
    metadata_json_file_name = luigi.Parameter(default='must input metadata json file name')
    file_prefix = luigi.Parameter(default='must input file prefix')

    #Consonance will not be called in test mode
    test_mode = luigi.BoolParameter(default = False)
    '''

    '''
    get_dockstore_tool_runner_json(cgp_pipeline_job_metadata["program"].replace(' ','_'),
                               cgp_pipeline_job_metadata["pipeline_job_json"],
                               target_tool,
                             


    def get_dockstore_tool_runner_json(program_name, cgp_pipeline_job_json, ):
        dockstore_tool_runner_json = {}
        dockstore_tool_runner_json["program_name"] = program_name

        json_str = json.dumps(cgp_pipeline_job_json, sort_keys=True, indent=4, separators=(',', ': '))
        print("THE JSON: "+json_str)
        # now make base64 encoded version
        base64_json_str = base64.urlsafe_b64encode(json_str)
        print("** MAKE JSON FOR DOCKSTORE TOOL WRAPPER **")


        dockstore_tool_runner_json["json_encoded"] = base64_json_str
        dockstore_tool_runner_json["docker_uri"] = target_tool
        dockstore_tool_runner_json["dockstore_url" ] = cgp_pipeline_job_metadata['target_tool_url']
        dockstore_tool_runner_json["redwood_token" ] = self.redwood_token
        dockstore_tool_runner_json["redwood_host"] = self.redwood_host

        #use only one parent uuid even though inputs are from more than one bundle?
        #Do this now until file browser code fixed so that it won't 
        #display duplicate workflow outputs
        #parent_uuids = ','.join(map("{0}".format, cgp_job['parent_uuids']))
        #print("parent uuids:%s" % parent_uuids)
        #dockstore_tool_runner_json["parent_uuids"] = parent_uuids
        dockstore_tool_runner_json["parent_uuids"] = cgp_pipeline_job_metadata['parent_uuids'][0]

        dockstore_tool_runner_json["workflow_type"] = cgp_pipeline_job_metadata["analysis_type"]
        dockstore_tool_runner_json["launch_type"] = cgp_pipeline_job_metadata["launch_type"]
        dockstore_tool_runner_json["tmpdir"] = self.tmp_dir
        dockstore_tool_runner_json["vm_instance_type"] = self.vm_instance_type
        dockstore_tool_runner_json["vm_region"] = self.vm_region
        dockstore_tool_runner_json["vm_location"] = "aws"
        dockstore_tool_runner_json["vm_instance_cores"] = 36
        dockstore_tool_runner_json["vm_instance_mem_gb"] = 60
        dockstore_tool_runner_json["output_metadata_json"] = "/tmp/final_metadata.json"

        return dockstore_tool_runner_json
    '''
    def run_workflow_from_GA4GH_endpoint(self, dockstore_tool_runner_json):
        endpoint = "http://172.31.19.48:8080/ga4gh/wes/v1"
        get_service_info = "/service-info"

        config = ConfigParser.ConfigParser()
        #config.read(args.credentials_file)
        config.read('/home/ubuntu/.consonance/config')
        print('credentials file keys:{}'.format(config.sections()))
        #os.environ['AWS_ACCESS_KEY_ID'] = config.get('default','aws_access_key_id')
        #os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('default','aws_secret_access_key')
        #print("OS env vars after config read:\n{}".format(os.environ))
        consonance_token = config.get('webservice','token')


        headers = {"Authorization": 'Bearer {}'.format(consonance_token) }
        

        run_workflow_endpoint = "/workflows"

        #descriptor= "quay.io/ga4gh-dream/dockstore-tool-helloworld:1.0.2"
        descriptor = self.dockstore_tool_running_dockstore_tool
        print('descriptor:{}'.format(descriptor)) 

        #params = '{\n\t"knowngood_file": {\n\t\t"class": "File",\n\t\t"path": "knownoutput.txt"\n\t},\n\t"helloworld_file": {\n\t\t"class": "File",\n\t\t"path": "helloworld.txt"\n\t}\n}'
        params = json.dumps(dockstore_tool_runner_json , sort_keys=True, indent=4, separators=(',', ': '))
        print('params:{}'.format(params))

        workflow_type ="cwl"
        workflow_type_version="1.0"

        key_values = {'flavour' : '{}'.format(dockstore_tool_runner_json["vm_instance_type"]) }
        print('flavour:{}'.format(key_values))
#        key_values = {'flavour' : 'c4.8xlarge'}

        body = {"workflow_descriptor":descriptor, "workflow_params" : params, "workflow_type": workflow_type,
                "workflow_type_version": workflow_type_version, "key_values" : key_values}

        print('\n\n!!!! ConsonanceTask:run_workflow_from_GA4GH_endpoint: POST request !!!!!\n\n')
        request_output = requests.post(endpoint+run_workflow_endpoint,headers=headers, json=body).json()
        print('POST run workflow output:{}'.format(request_output))


        get_service_info = '/service-info'
        request_output = requests.get(endpoint+get_service_info, headers=headers).json()
        print('GET service info output:{}'.format(request_output))
        


    def run(self):
        print("\n\n\n** TASK RUN **")

        cgp_pipeline_job_metadata = json.loads(self.cgp_pipeline_job_metadata_str)

        self.s3_metadata_json_file_path = cgp_pipeline_job_metadata["s3_metadata_json_file_path"]
        self.local_dockstore_tool_runner_json_file_path = cgp_pipeline_job_metadata["local_dockstore_tool_runner_json_file_path"]
        self.s3_dockstore_tool_runner_json_file_path = cgp_pipeline_job_metadata["s3_dockstore_tool_runner_json_file_path"]
#        self.s3_finished_json_file_path = cgp_pipeline_job_metadata["s3_finished_json_file_path"]



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



        print("** MAKE JSON FOR WORKER **")
        # create a json for for the pipeline which will be executed by the 
        #dockstore-tool-running-dockstore-tool and passed as base64encoded
        # will need to encode the JSON above in this: https://docs.python.org/2/library/base64.html
        # see http://luigi.readthedocs.io/en/stable/api/luigi.parameter.html?highlight=luigi.parameter
        # TODO: this is tied to the requirements of the tool being targeted
        
        json_str = json.dumps(cgp_pipeline_job_metadata["pipeline_job_json"], sort_keys=True, indent=4, separators=(',', ': '))
        #print("THE JSON: "+json_str)
        # now make base64 encoded version
        base64_json_str = base64.urlsafe_b64encode(json_str)
        print("** MAKE JSON FOR DOCKSTORE TOOL WRAPPER **")

        # create a json for dockstoreRunningDockstoreTool, embed the  JSON as a param
#        p = self.save_dockstore_tool_runner_json().open('w')
#        p_local = self.save_dockstore_tool_runner_json_local().open('w')

        target_tool= cgp_pipeline_job_metadata['target_tool_prefix'] + ":" + self.workflow_version

        dockstore_tool_runner_json = {}
        #If auto scaling is used with Toil then toil will download the
        #to let dockstore tool runner know it should generate a signed URL
        #to the storage system
        if self.auto_scale:
            dockstore_tool_runner_json["use_signed_urls"] = True
        dockstore_tool_runner_json["program_name"] = cgp_pipeline_job_metadata["program"].replace(' ','_')
        dockstore_tool_runner_json["json_encoded"] = base64_json_str
        dockstore_tool_runner_json["docker_uri"] = target_tool
        dockstore_tool_runner_json["dockstore_url" ] = cgp_pipeline_job_metadata['target_tool_url']
        dockstore_tool_runner_json["redwood_token" ] = self.redwood_token
        dockstore_tool_runner_json["redwood_host"] = self.redwood_host

        #use only one parent uuid even though inputs are from more than one bundle?
        #Do this now until file browser code fixed so that it won't 
        #display duplicate workflow outputs
        #parent_uuids = ','.join(map("{0}".format, cgp_job['parent_uuids']))
        #print("parent uuids:%s" % parent_uuids)
        #dockstore_tool_runner_json["parent_uuids"] = parent_uuids
        dockstore_tool_runner_json["parent_uuids"] = cgp_pipeline_job_metadata['parent_uuids'][0]

        dockstore_tool_runner_json["workflow_type"] = cgp_pipeline_job_metadata["analysis_type"]
        dockstore_tool_runner_json["launch_type"] = cgp_pipeline_job_metadata["launch_type"]
        dockstore_tool_runner_json["tmpdir"] = self.tmp_dir
        dockstore_tool_runner_json["vm_instance_type"] = self.vm_instance_type
        dockstore_tool_runner_json["vm_region"] = self.vm_region
        dockstore_tool_runner_json["vm_location"] = "aws"
        dockstore_tool_runner_json["vm_instance_cores"] = 36
        dockstore_tool_runner_json["vm_instance_mem_gb"] = 60
        dockstore_tool_runner_json["output_metadata_json"] = "/tmp/final_metadata.json"

        dockstore_tool_runner_json_str = json.dumps(dockstore_tool_runner_json , sort_keys=True, indent=4, separators=(',', ': '))
        print("ConsonanceTask.run - dockstore_tool_runner_json_str json:\n{}".format(dockstore_tool_runner_json_str))        
#        print(dockstore_tool_runner_json_str, file=p)
#        p.close()
    
        # write the parameterized JSON for input to Consonance
        # to a local file since Consonance cannot read files on s3
#        print(dockstore_tool_runner_json_str, file=p_local)
#        p_local.close()

        self.save_dockstore_tool_runner_json_local(self.local_dockstore_tool_runner_json_file_path, dockstore_tool_runner_json_str)

        # execute consonance run, parse the job UUID

        cmd = ["consonance", "run",  "--tool-dockstore-id", self.dockstore_tool_running_dockstore_tool, \
                "--flavour", self.vm_instance_type, "--run-descriptor", self.local_dockstore_tool_runner_json_file_path]
#                "--flavour", self.vm_instance_type, "--run-descriptor", self.local_dockstore_tool_runner_json_file_path, '--extra-file', '/root/.aws/credentials=/home/ubuntu/.aws/credentials=true']
        cmd_str = ' '.join(cmd)
        if self.test_mode == False:

            '''
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
                cgp_pipeline_job_metadata["consonance_job_uuid"] = consonance_output["job_uuid"]
            else:
                print("ERROR: COULD NOT FIND CONSONANCE JOB UUID IN CONSONANCE OUTPUT!", file=sys.stderr)
            '''

            self.run_workflow_from_GA4GH_endpoint(dockstore_tool_runner_json)
            cgp_pipeline_job_metadata["consonance_job_uuid"] = 'no consonance id in manifest mode?????'

        else:
            print("TEST MODE: Consonance command would be:"+ cmd_str)
            cgp_pipeline_job_metadata["consonance_job_uuid"] = 'no consonance id in test mode'
             
        #remove the local parameterized JSON file that
        #was created for the Consonance call
        #since the Consonance call is finished
        #self.save_dockstore_tool_runner_json_local().remove()
        os.remove(self.local_dockstore_tool_runner_json_file_path)

        #convert the meta data to a string and
        #save the donor metadata for the sample being processed to the touch
        # file directory
        #cgp_job_json = json.dumps(cgp_pipeline_job_metadata, sort_keys=True, indent=4, separators=(',', ': '))
        #print('ConsonanceTask:run - cgp_pipeline_job_metadata json:{}'.format(cgp_job_json))
#        m = self.save_metadata_json().open('w')
#        print(cgp_job_json, file=m)
#        m.close()


         # NOW MAke a final report
#        f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
#        print("Consonance task is complete", file=f)
#        f.close()
        print("\n\n\n\n** TASK RUN DONE **")

    def save_metadata_json(self):
        return S3Target(self.s3_metadata_json_file_path)

    def save_dockstore_tool_runner_json_local(self):
        return luigi.LocalTarget(self.local_dockstore_tool_runner_json_file_path)

    def save_dockstore_tool_runner_json_local(self, local_dockstore_tool_runner_json_file_path, dockstore_tool_runner_json):
            with open(local_dockstore_tool_runner_json_file_path, 'w') as f:
                f.write(dockstore_tool_runner_json)


    def save_dockstore_tool_runner_json(self):
        return S3Target(self.s3_dockstore_tool_runner_json_file_path)

    def output(self):
#        return S3Target(self.s3_finished_json_file_path)
#        print("output target:{}".format("s3://" + self.touch_file_path + "/" +  self.file_prefix + "_finished.json"))
        return S3Target("s3://" + self.touch_file_path + "/" +  self.file_prefix + "_finished.json")


class base_Coordinator(object):
   

    def __init__(self, touch_file_bucket, redwood_token, \
                 redwood_host, dockstore_tool_running_dockstore_tool, \
                 es_index_host ='localhost', es_index_port ='9200', \
                 tmp_dir ='/datastore', max_jobs = -1, process_sample_uuids = "", \
                 workflow_version = "", \
                 vm_instance_type ='c4.8xlarge', vm_region ='us-west-2', \
                 test_mode = False, center = "", program = "", project = "", \
                 all_samples_in_one_job = False):

        self.es_index_host = es_index_host
        self.es_index_port = es_index_port
        self.redwood_token = redwood_token
        self.redwood_host = redwood_host
        self.dockstore_tool_running_dockstore_tool = dockstore_tool_running_dockstore_tool
        self.tmp_dir = tmp_dir
        self.max_jobs = max_jobs
        self.process_sample_uuids = process_sample_uuids
    
        self.workflow_version = workflow_version
        self.touch_file_bucket = touch_file_bucket
    
        self.vm_instance_type = vm_instance_type
        self.vm_region = vm_region
    
        #Consonance will not be called in test mode
        self.test_mode = test_mode
    
        self.center = center
        self.program = program
        self.project = project
        self.all_samples_in_one_job = all_samples_in_one_job 

 
    '''
    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_host = luigi.Parameter(default='storage.ucsc-cgp.org')
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="must input quay.io path to dockstore tool runner and version")
    tmp_dir = luigi.Parameter(default='/datastore')
    max_jobs = luigi.Parameter(default='-1')
    process_sample_uuids = luigi.Parameter(default = "")

    workflow_version = luigi.Parameter(default="")
    touch_file_bucket = luigi.Parameter(default="must be input")

    vm_instance_type = luigi.Parameter(default='c4.8xlarge')
    vm_region = luigi.Parameter(default='us-west-2')

    #Consonance will not be called in test mode
    test_mode = luigi.BoolParameter(default = False)

    center = luigi.Parameter(default = "")
    program = luigi.Parameter(default = "")
    project = luigi.Parameter(default = "")
    '''

    bundle_uuid_filename_to_file_uuid = {}




    def run_command(self, command_string, max_attempts, delay_in_seconds, ignore_errors=False, cwd='.'):
        print(command_string)
        #command must be formatted as a list of strings; e.g.
        #command = ["dockstore", "tool", "launch", "--debug", "--entry", self.docker_uri, "--json", "transformed_json_path"]
        command = command_string.split()
        print("command list object:")
        print(command)
        for attempt_number in range(1, max_attempts+1):
            if attempt_number > 1:
                #we are about to retry the command, but sleep for a number of seconds before retrying
                print("Waiting for "+str(delay_in_seconds)+" seconds before retrying")
                time.sleep(delay_in_seconds)

            print("\nDockstore tool runner executing command: " + command_string)
            print("Attempt number "+str(attempt_number)+" of "+str(max_attempts))
            try:
                cmd_output = subprocess.check_output(command, cwd=cwd)
            except subprocess.CalledProcessError as e:
                #If we get here then the called command return code was non zero
                print("\nERROR!!! DOCKSTORE TOOL RUNNER CMD:" + command_string + " FAILED !!!", file=sys.stderr)
                print("\nReturn code:" + str(e.returncode), file=sys.stderr)
                print("\nOutput:" + str(e.output), file=sys.stderr)
                return_code = e.returncode
                cmd_output = e.output
                if ignore_errors:
                    break;
            except Exception as e:
                print("\nERROR!!! DOCKSTORE TOOL RUNNER CMD:" + command_string + " THREW AN EXCEPTION !!!", file=sys.stderr)
                print("\nException information:" + str(e), file=sys.stderr)
                #if we get here the called command threw an exception other than just
                #returning a non zero return code, so just set the return code to 1.
                return_code = 1
                cmd_output = ""
                if ignore_errors:
                    break;
            #in try constructs, the else block runs if no exception happened
            #which in this case indicates the command succeeded
            else:
                print("CMD "+ command_string + " SUCCESSFUL IN DOCKSTORE TOOL RUNNER!!")
                return_code = 0
                #break out of the retry loop since the command was successful
                break;
        #the else block is executed if the loop didn't exit abnormally (i.e. with break in
        #the try: else: statement that indicates the command was successful
        else:
            if not ignore_errors:
                print("Exiting Dockstore tool runner due to call error in command "+command_string+" after "+str(max_attempts)+" attempts", file=sys.stderr)
                sys.exit(return_code)
            else:
                print ("There were errors in the call to command "+command_string+" after "+str(max_attempts)+" attempts but ignore_errors=True so ignoring ", file=sys.stderr)
        return cmd_output




    #Classes derived from this class must implement these methods
    '''
    Return true if we can put multiple samples in the pipeline json
    '''     
    def supports_multiple_samples_per_job(self):
        raise NotImplementedError('You need to define a supports_multiple_samples_per_job method!')

    '''
    Return a string that is the name that will be used to name the touchfile path

    E.g.:
        return 'Fusion'
    '''
    def get_pipeline_name(self):
        raise NotImplementedError('You need to define a get_pipeline_name method!')

    '''
    Return a dictionary of CWL option to reference file name so this information
    can be added to the parameterized JSON input to the pipeline

    E.g.:
        cwl_option_to_reference_file_name = defaultdict()

        ######################CUSTOMIZE REFERENCE FILES FOR PIPELINE START####################### 
        cwl_option_to_reference_file_name['index'] = "STARFusion-GRCh38gencode23.tar.gz"
        ######################CUSTOMIZE REFERENCE FILES FOR PIPELINE END####################### 

        return cwl_option_to_reference_file_name
    '''
    def get_cgp_job_reference_files(self):
        raise NotImplementedError('You need to define a get_cgp_job_reference_files method!') 

    '''
    Returns a dictionary of keyword used in the dockstore tool runner parameterized JSON
    and used in Elastic search to find needed samples. The JSON data does not depend
    on samples found in the Elastic search so can be added here
    '''
    def get_pipeline_job_fixed_metadata(self):
        raise NotImplementedError('You need to define a get_pipeline_job_fixed_metadata method!')

    '''
    Returns a dictionary of keywords to metadata that is used to setup the touch file path
    and metadata and dockstore JSON file names. These sometimes depend on the sample name
    or other information found through the Elastic search so is separated from the method
    that gets the fixed metadata. This routine adds to the metadata dictionary for the pipeline.
     
    E.g

       ###########################CUSTOMIZE METADATA FOR PIPELINE  START#########################
       #The items below are usually customized for a particular workflow or tool

       #launch type is either 'workflow' or 'tool'
       cgp_pipeline_job_metadata["launch_type"] = "tool"
       cgp_pipeline_job_metadata["analysis_type"] = "fusion_variant_calling"
       cgp_pipeline_job_metadata['metadata_json_file_name'] = cgp_pipeline_job_metadata['file_prefix'] + '_meta_data.json'
       cgp_pipeline_job_metadata["target_tool_prefix"] = 'registry.hub.docker.com/ucsctreehouse/fusion'
       cgp_pipeline_job_metadata["target_tool_url"] = \
               "https://dockstore.org/containers/registry.hub.docker.com/ucsctreehouse/fusion/"
       cgp_pipeline_job_metadata['file_prefix'] = sample["submitter_sample_id"]
       cgp_pipeline_job_metadata["last_touch_file_folder_suffix"] = cgp_pipeline_job_metadata["submitter_sample_id"]

       ######################CUSTOMIZE METADATA FOR PIPELINE END################################## 
    '''
    def get_pipeline_job_customized_metadata(self, cgp_pipeline_job_metadata):
        raise NotImplementedError('You need to define a get_pipeline_customized_metadata method!')

    '''
    Returns a dictionary of CWL keywords and values that make up the CWL input parameterized
    JSON for the pipeline. This is the input to the pipeline to be run from Dockstore. 

    E.g:
            #Edit the following lines to set up the pipeline tool/workflow CWL options 
            ######################CUSTOMIZE JSON INPUT FOR PIPELINE START################################### 

            cgp_pipeline_job_json = defaultdict()
    
            for file in analysis["workflow_outputs"]:
                print("\nfile type:"+file["file_type"])
                print("\nfile name:"+file["file_path"])
    
                #if (file["file_type"] != "bam"): output an error message?
    
                file_path = 'redwood' + '://' + self.redwood_host + '/' + analysis['bundle_uuid'] + '/' + \
                    self.fileToUUID(file["file_path"], analysis["bundle_uuid"]) + \
                    "/" + file["file_path"]
    
                if 'fastq1' not in cgp_pipeline_job_json.keys():
                    cgp_pipeline_job_json['fastq1'] = defaultdict(dict)
                    cgp_pipeline_job_json['fastq1'] = {"class" : "File", "path" : file_path}
                elif 'fastq2' not in cgp_pipeline_job_json.keys():
                    cgp_pipeline_job_json['fastq2'] = defaultdict(dict)
                    cgp_pipeline_job_json['fastq2'] = {"class" : "File", "path" : file_path}
                else:
                    print("ERROR: too many input files!!!", file=sys.stderr)
    
                if 'parent_uuids' not in cgp_pipeline_job_metadata.keys():
                    cgp_pipeline_job_metadata["parent_uuids"] = []
                
                if sample["sample_uuid"] not in cgp_pipeline_job_metadata["parent_uuids"]: 
                    cgp_pipeline_job_metadata["parent_uuids"].append(sample["sample_uuid"])
    
            cgp_pipeline_job_json["outputdir"] = '.'
            cgp_pipeline_job_json["root-ownership"] = True
    
            # Specify the output files here, using the options in the CWL file as keys
            file_path = "/tmp/star-fusion-gene-list-filtered.final"
            cgp_pipeline_job_json["output1"] = {"class" : "File", "path" : file_path}
            file_path = "/tmp/star-fusion-gene-list-filtered.final.bedpe"
            cgp_pipeline_job_json["output2"] = {"class" : "File", "path" : file_path}
            file_path = "/tmp/star-fusion-non-filtered.final"
            cgp_pipeline_job_json["output3"] = {"class" : "File", "path" : file_path}
            file_path = "/tmp/star-fusion-non-filtered.final.bedpe"
            cgp_pipeline_job_json["output4"] = {"class" : "File", "path" : file_path}

            ####################CUSTOMIZE JSON INPUT FOR PIPELINE END#######################################

    ''' 
    def get_pipeline_parameterized_json(self, cgp_pipeline_job_metadata, analysis):
        raise NotImplementedError('You need to define a get_pipeline_parameterized_json method!')

    def update_jobs_metadata(self, cgp_pipeline_job_json, cgp_jobs_reference_files, \
                            cgp_pipeline_job_metadata, cgp_all_pipeline_jobs_metadata):
            #attach reference file json to pipeline job json
            cgp_pipeline_job_json.update(cgp_jobs_reference_files)
            #print('keys for cgp pipeline job json:' + ','.join(cgp_pipeline_job_json.keys()))

            json_str = json.dumps(cgp_pipeline_job_json, sort_keys=True, indent=4, separators=(',', ': '))
            #print("\nCGP pipeline job json:\n{}".format(json_str))

            #attach the workflow or tool parameterized json to the job metadata
            cgp_pipeline_job_metadata["pipeline_job_json"] = cgp_pipeline_job_json
            json_str = json.dumps(cgp_pipeline_job_metadata, sort_keys=True, indent=4, separators=(',', ': '))
            #print("\nCGP pipeline job metadata:\n{}".format(json_str))

            #attach this jobs metadata to a list of all the jobs metadata
            cgp_all_pipeline_jobs_metadata.append(copy.deepcopy(cgp_pipeline_job_metadata))
            json_str = json.dumps(cgp_all_pipeline_jobs_metadata, sort_keys=True, indent=4, separators=(',', ': '))
            #print("\nCGP all pipeline jobs meta data:\n{}".format(json_str))

            return cgp_all_pipeline_jobs_metadata 


    def get_cgp_pipeline_jobs_metadata(self, hits, cgp_jobs_fixed_metadata, cgp_jobs_reference_files):
        cgp_all_pipeline_jobs_metadata = []
        cgp_pipeline_job_json = defaultdict()

        for hit in hits:
            print("\n\n\nDonor uuid:%(donor_uuid)s Center Name:%(center_name)s Program:%(program)s Project:%(project)s" % hit["_source"])
            print("Got %d specimens:" % len(hit["_source"]["specimen"]))

            #if a particular center, program or project is requested for processing and
            #the current one  does not match go on to the next sample
            if self.center and (self.center != hit["_source"]["center_name"]):
                print('continuing after center test!!!! center is:{} len is:{}'.format(self.center, len(self.center)))
                continue
            if self.program and (self.program != hit["_source"]["program"]):
                print('continuing after program test!!!!')
                continue
            if self.project and (self.project != hit["_source"]["project"]):
                print('continuing after project test!!!!')
                continue


            for specimen in hit["_source"]["specimen"]:
                print("Next sample of %d samples:" % len(specimen["samples"]))
                for sample in specimen["samples"]:
                    print("Next analysis of %d analysis:" % len(sample["analysis"]))

                    #if a particular sample uuid is requested for processing and
                    #the current sample uuid does not match go on to the next sample
                    if self.process_sample_uuids and sample["sample_uuid"] not in self.process_sample_uuids.split():
                        continue

                    for analysis in sample["analysis"]:
                        #print analysis
                        print("HIT!!!!")
                        print("analysis type:{}".format(analysis["analysis_type"]))
                        #print("normal flag:{}".format(str(hit["_source"]["flags"]["normal_sequence"])))
                        #print("tumor flag:{}".format(str(hit["_source"]["flags"]["tumor_sequence"])))

                        for output in analysis["workflow_outputs"]:
                            json_str = json.dumps(output, sort_keys=True, indent=4, separators=(',', ': '))
                            print(json_str)

                        #put together the metadata and then decide whether the job is to be included in the list of jobs
                        #This metadata will be passed to the Consonance Task and some
                        #some of the meta data will be used in the Luigi status page for the job
                        cgp_pipeline_job_metadata = defaultdict()
                        print("constructing pipeline job metadata")

                        #attach fixed metadata to pipeline job json
                        cgp_pipeline_job_metadata.update(cgp_jobs_fixed_metadata)

                        cgp_pipeline_job_metadata["sample_name"] = sample["submitter_sample_id"]
                        cgp_pipeline_job_metadata["program"] = hit["_source"]["program"]
                        cgp_pipeline_job_metadata["project"] = hit["_source"]["project"]
                        cgp_pipeline_job_metadata["center_name"] = hit["_source"]["center_name"]
                        cgp_pipeline_job_metadata["submitter_donor_id"] = hit["_source"]["submitter_donor_id"]
                        cgp_pipeline_job_metadata["donor_uuid"] = hit["_source"]["donor_uuid"]
                        if "submitter_donor_primary_site" in hit["_source"]:
                            cgp_pipeline_job_metadata["submitter_donor_primary_site"] = hit["_source"]["submitter_donor_primary_site"]
                        else:
                            cgp_pipeline_job_metadata["submitter_donor_primary_site"] = "not provided"
                        cgp_pipeline_job_metadata["submitter_specimen_id"] = specimen["submitter_specimen_id"]
                        cgp_pipeline_job_metadata["specimen_uuid"] = specimen["specimen_uuid"]
                        cgp_pipeline_job_metadata["submitter_specimen_type"] = specimen["submitter_specimen_type"]
                        cgp_pipeline_job_metadata["submitter_experimental_design"] = specimen["submitter_experimental_design"]
                        cgp_pipeline_job_metadata["submitter_sample_id"] = sample["submitter_sample_id"]
                        cgp_pipeline_job_metadata["sample_uuid"] = sample["sample_uuid"]
                        cgp_pipeline_job_metadata["workflow_version"] = self.workflow_version


                        #get metadata for the pipeline that depends on the particular sample 
                        cgp_pipeline_job_metadata = self.get_pipeline_job_customized_metadata(cgp_pipeline_job_metadata)

                        #The action service monitor looks for 'workflow_name' when it inserts the job data into its DB
                        cgp_pipeline_job_metadata["workflow_name"] = cgp_pipeline_job_metadata["target_tool_prefix"]

                        workflow_version_dir = self.workflow_version.replace('.', '_')
                        touch_file_path_prefix = self.touch_file_bucket + \
                                   "/consonance-jobs/" +  \
                                   self.get_pipeline_name() + "_Coordinator/" + workflow_version_dir

                        touch_file_path = touch_file_path_prefix + "/" \
                                   + cgp_pipeline_job_metadata["center_name"] + "_" \
                                   + cgp_pipeline_job_metadata["program"] + "_" \
                                   + cgp_pipeline_job_metadata["project"] + "_" \
                                   + cgp_pipeline_job_metadata["submitter_donor_primary_site"] + "_" \
                                   + cgp_pipeline_job_metadata["submitter_specimen_id"] + "_" \
                                   + cgp_pipeline_job_metadata["submitter_sample_id"] + "_" \
                                   + cgp_pipeline_job_metadata["last_touch_file_folder_suffix"] + "_" \
                                   + cgp_pipeline_job_metadata["sample_uuid"]

                        cgp_pipeline_job_metadata["s3_metadata_json_file_path"] = "s3://" +  \
                                   touch_file_path + "/" + \
                                   cgp_pipeline_job_metadata['metadata_json_file_name']
                        cgp_pipeline_job_metadata["s3_dockstore_tool_runner_json_file_path"] = "s3://" + \
                                   touch_file_path + "/" + \
                                   cgp_pipeline_job_metadata['file_prefix'] + \
                                   "_dockstore_tool.json"
                        cgp_pipeline_job_metadata["local_dockstore_tool_runner_json_file_path"] = "/tmp/" + \
                                   touch_file_path + "/" + \
                                   cgp_pipeline_job_metadata['file_prefix'] + \
                                   "_dockstore_tool.json"
                        cgp_pipeline_job_metadata["s3_finished_json_file_path"] = "s3://" + \
                                   touch_file_path + "/" + \
                                   cgp_pipeline_job_metadata['file_prefix'] + \
                                   "_finished.json"
                        cgp_pipeline_job_metadata["touch_file_path"] = touch_file_path
                        #should we remove all white space from the path in the case where 
                        #i.e. the program name is two words separated by blanks?
                        # remove all whitespace from touch file path
                        #touch_file_path = ''.join(touch_file_path.split())
           
                        #json_str = json.dumps(cgp_pipeline_job_metadata, sort_keys=True, indent=4, separators=(',', ': ')) 
                        #print("HIT metadata:\n{}".format(json_str))

                        #If this sample has not already been run through the pipeline add it to the list of jobs
                        if (  (
                                  #Most pipelines work only on a certain data format 
                                  #For instance Fusion and RNA-Seq pipelines work only on uploaded sequences
                                  #and the CNV pipeline works only with uploaded BAMs
                                  analysis["analysis_type"] == cgp_pipeline_job_metadata["input_data_analysis_type"] and \
                                  #Most pipelines work only on specially  prepared data
                                  #For example the Fusion pipeline works only on RNA-Seq prepared data
                                  (re.match("^" + cgp_pipeline_job_metadata["input_data_experimental_design"] + "$", specimen["submitter_experimental_design"]))
                              ) 
                             
                              #and \
                              #(
                              #    (hit["_source"]["flags"][ cgp_pipeline_job_metadata["normal_metadata_flag"] ] == False and \
                              #     sample["sample_uuid"] in hit["_source"]["missing_items"][ cgp_pipeline_job_metadata["normal_missing_item"] ] and \
                              #     re.match("^Normal - ", specimen["submitter_specimen_type"])) 
                              #    or \
                              #    (
                              #     hit["_source"]["flags"][ cgp_pipeline_job_metadata["tumor_metadata_flag"] ] == False and \
                              #     sample["sample_uuid"] in hit["_source"]["missing_items"][ cgp_pipeline_job_metadata["tumor_missing_item"] ] and \
                              #     re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"]))
                              #)
                              
                           ):

                            # if we are putting samples in separate jobs then start with an empty job json information structure
                            if not self.all_samples_in_one_job:
                                cgp_pipeline_job_json.clear()

                            #get the parameterized JSON that is the input to the pipeline registered in Dockstore
                            cgp_pipeline_job_json = self.get_pipeline_parameterized_json(cgp_pipeline_job_metadata, analysis, cgp_pipeline_job_json)
 
                            #if there should be a job for each sample then update the jobs 
                            #metadata with this one sample
                            if not self.all_samples_in_one_job:
                                #If the derived pipeline code was unable to construct valid parameterized
                                #pipeline JSON input file the list will be empty and we don't update
                                #the jobs metadata
                                if cgp_pipeline_job_json:
                                    cgp_all_pipeline_jobs_metadata = self.update_jobs_metadata(cgp_pipeline_job_json, cgp_jobs_reference_files, \
                                        cgp_pipeline_job_metadata, cgp_all_pipeline_jobs_metadata)
                                    '''
                                    #attach reference file json to pipeline job json
                                    cgp_pipeline_job_json.update(cgp_jobs_reference_files)
                                    print('keys for cgp pipeline job json:' + ','.join(cgp_pipeline_job_json.keys()))
    
                                    json_str = json.dumps(cgp_pipeline_job_json, sort_keys=True, indent=4, separators=(',', ': '))
                                    #print("\nCGP pipeline job json:\n{}".format(json_str))
            
                                    #attach the workflow or tool parameterized json to the job metadata
                                    cgp_pipeline_job_metadata["pipeline_job_json"] = cgp_pipeline_job_json
                                    json_str = json.dumps(cgp_pipeline_job_metadata, sort_keys=True, indent=4, separators=(',', ': '))
                                    #print("\nCGP pipeline job metadata:\n{}".format(json_str))
            
                                    #attach this jobs metadata to a list of all the jobs metadata
                                    cgp_all_pipeline_jobs_metadata.append(cgp_pipeline_job_metadata)
                                    json_str = json.dumps(cgp_all_pipeline_jobs_metadata, sort_keys=True, indent=4, separators=(',', ': '))
                                    #print("\nCGP all pipeline jobs meta data:\n{}".format(json_str))
                                    '''
                                else:
                                    json_str = json.dumps(cgp_pipeline_job_metadata, sort_keys=True, indent=4, separators=(',', ': '))
                                    print("\nWARNING: UNABLE TO GET PARAMETERIZED JSON FOR PIPELINE WITH METADATA:{}".format(json_str) , file=sys.stderr)
        if self.all_samples_in_one_job:
            if cgp_pipeline_job_json:
                cgp_all_pipeline_jobs_metadata = self.update_jobs_metadata(cgp_pipeline_job_json, cgp_jobs_reference_files, \
                                    cgp_pipeline_job_metadata, cgp_all_pipeline_jobs_metadata)
            else:
                json_str = json.dumps(cgp_pipeline_job_metadata, sort_keys=True, indent=4, separators=(',', ': '))
                print("\nWARNING: UNABLE TO GET PARAMETERIZED JSON FOR PIPELINE WITH METADATA:{}".format(json_str) , file=sys.stderr)
  


        return cgp_all_pipeline_jobs_metadata

    def get_storage_system_file_path(self, bundle_uuid, file_uuid, 
                           file_path, file_path_prefix = 'redwood'):
        if self.auto_scale:
            #If auto scaling is used with Toil then toil will download the
            #reference files so preface the file path with 'redwood_signed_url'
            #to let dockstore tool runner know it should generate a signed URL
            #to the storage system
#                    file_path_prefix = 'redwood-signed-url'

            cmd = "docker run --rm -it -e ACCESS_TOKEN={} -e REDWOOD_ENDPOINT={}  quay.io/ucsc_cgl/core-client:1.1.2 icgc-storage-client url --object-id {}".format(self.redwood_token, self.redwood_host, file_uuid)
#                    cmd = "icgc-storage-client download --output-dir {} --object-id {} --output-layout bundle --force".format(self.tmp_dir, file_uuid)
            #create list of individual command 'words' for input to run commmand function
            cmd_output = self.run_command(cmd, 3, 5)
            #get the list of strings in the output that begin with https
            # which should be the signed URL
            search_object = re.search(r'https://.*', cmd_output, re.MULTILINE)
            signed_url = search_object.group(0).rstrip()
            print('base_manifest - self signed url is:{}'.format(signed_url))
            file_path = signed_url

        else:
            file_path = file_path_prefix + '://' + self.redwood_host + '/' + bundle_uuid + '/' + \
                    file_uuid + "/" + file_name
        return file_path       

    def requires(self, hits):
        print("\n\n\n\n** COORDINATOR REQUIRES **")

        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        print(str("metadata."+self.redwood_host+"/entities?page=0"))

        #hack to get around none self signed certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        #Go through the metadata and setup the mapping of bundle uuid to file name
        json_str = urlopen(str("https://metadata."+self.redwood_host+"/entities?page=0"), context=ctx).read()
        metadata_struct = json.loads(json_str)
        print("** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"]))
        for i in range(0, metadata_struct["totalPages"]):
            print("** CURRENT METADATA TOTAL PAGES: "+str(i))
            json_str = urlopen(str("https://metadata."+self.redwood_host+"/entities?page="+str(i)), context=ctx).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]


        #Get the reference file metadata from the storage system
        #and create a file path that the Dockstore tool runner can
        #used to download the reference file from the storage system
        cgp_jobs_reference_files = defaultdict()
        cwl_option_to_reference_file_name = self.get_cgp_job_reference_files()

        #Go through the reference files for this pipeline and find the bundle uuid
        #file uuid and file name in the metadata and set up the path to the file 
        #in the storage system so it can be downloaded when the pipeline is run
        for switch, file_name in cwl_option_to_reference_file_name.iteritems():
            print("switch:{} file name {}".format(switch, file_name))
            #if the filename isn't an empty string
            if file_name:
                file_name_metadata_json = urlopen(str("https://metadata."+self.redwood_host+"/entities?fileName="+file_name), context=ctx).read()
                file_name_metadata = json.loads(file_name_metadata_json)
                if file_name_metadata['totalElements'] == 0:
                    print('ERROR: could not find reference file in storage system!')
                #print("reference file metadata:\n{}".format(str(file_name_metadata)))
                bundle_uuid = file_name_metadata["content"][0]["gnosId"]
                file_uuid = file_name_metadata["content"][0]["id"]
                file_name = file_name_metadata["content"][0]["fileName"]
    
                file_path = self.get_storage_system_file_path(bundle_uuid, file_uuid, file_name)
                ''' 
                if self.auto_scale:
                    #If auto scaling is used with Toil then toil will download the
                    #reference files so preface the file path with 'redwood_signed_url'
                    #to let dockstore tool runner know it should generate a signed URL
                    #to the storage system
#                    file_path_prefix = 'redwood-signed-url'

                    cmd = "docker run --rm -it -e ACCESS_TOKEN={} -e REDWOOD_ENDPOINT={}  quay.io/ucsc_cgl/core-client:1.1.2 icgc-storage-client url --object-id {}".format(self.redwood_token, self.redwood_host, file_uuid)
#                    cmd = "icgc-storage-client download --output-dir {} --object-id {} --output-layout bundle --force".format(self.tmp_dir, file_uuid)
                    #create list of individual command 'words' for input to run commmand function
                    signed_url = self.run_command(cmd, 3, 5)
                    print('base_manifest - self signed url is:{}'.format(signed_url))
                    cgp_jobs_reference_files[switch] = signed_url

                else:
                    file_path_prefix = 'redwood'
                    ref_file_path = file_path_prefix + '://' + self.redwood_host + '/' + bundle_uuid + '/' + \
                            file_uuid + "/" + file_name
                    cgp_jobs_reference_files[switch] = {"class" : "File", "path" : ref_file_path}
                '''
                if self.auto_scale:
                    cgp_jobs_reference_files[switch] = file_path
                else:
                    cgp_jobs_reference_files[switch] = {"class" : "File", "path" : file_path}

            else:
                #the file path is empty; some pipelines (e.g Toil RNA-Seq) use
                # this to signal that the reference and or tool is not to be used
                if self.auto_scale:
                   cgp_jobs_reference_files[switch] = ""
                else:
                   cgp_jobs_reference_files[switch] = {"class" : "File", "path" : ""}

            json_str = json.dumps(cgp_jobs_reference_files[switch], sort_keys=True, indent=4, separators=(',', ': '))
            print("Reference files json:\n{}".format(json_str))


        cgp_jobs_fixed_metadata = self.get_pipeline_job_fixed_metadata()

        '''
        # now query elasticsearch to find the samples that been run through the pipeline
        print("setting up elastic search Elasticsearch([\"http:\/\/"+self.es_index_host+":"+self.es_index_port+"]")
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags." + cgp_jobs_fixed_metadata["normal_metadata_flag"] : "false"}}, \
                        {"term" : {"flags." + cgp_jobs_fixed_metadata["tumor_metadata_flag"] : "false" }}],"minimum_should_match" : 1 }}}, size=5000)
        
        print("Got %d Hits:" % res['hits']['total'])
        cgp_pipeline_jobs_metadata = self.get_cgp_pipeline_jobs_metadata(res['hits']['hits'], cgp_jobs_fixed_metadata, cgp_jobs_reference_files)
        '''
        
        cgp_pipeline_jobs_metadata = self.get_cgp_pipeline_jobs_metadata(hits, cgp_jobs_fixed_metadata, cgp_jobs_reference_files)
        #json_str = json.dumps(cgp_pipeline_jobs_metadata, sort_keys=True, indent=4, separators=(',', ': '))
        #print("\nCGP pipeline jobs meta data after self.get_cgp_pipeline_jobs_metadata call :\n{}".format(json_str))

        listOfJobs = []
        #Go through the list of jobs and if the max number of jobs to run has
        #not been reached add it to the list of jobs to run
        for job_num, job in enumerate(cgp_pipeline_jobs_metadata):
            print('job num:{}'.format(job_num))
            if (job_num < int(self.max_jobs) or int(self.max_jobs) < 0):
                cgp_pipeline_job_metadata_str = json.dumps(job, sort_keys=True, indent=4, separators=(',', ': '))
                print("\npipeline job metadata:")
                print(cgp_pipeline_job_metadata_str)

                listOfJobs.append(ConsonanceTask(redwood_host=self.redwood_host,
                    vm_instance_type=self.vm_instance_type,
                    vm_region = self.vm_region,
                    redwood_token=self.redwood_token,
                    dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool,
                    tmp_dir=self.tmp_dir,
                    workflow_version = self.workflow_version,
                    cgp_pipeline_job_metadata_str = cgp_pipeline_job_metadata_str,
                    metadata_json_file_name = job['metadata_json_file_name'],
                    touch_file_path = job['touch_file_path'],
                    file_prefix = job['file_prefix'],
                    auto_scale = self.auto_scale,
                    test_mode=self.test_mode))

            
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
        workflow_version_dir = self.workflow_version.replace('.', '_')
        return S3Target('s3://'+ self.touch_file_bucket + '/consonance-jobs/{}_Coordinator/{}/{}-{}.txt'.format( \
                                  self.get_pipeline_name(), workflow_version_dir, self.get_pipeline_name(), ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"



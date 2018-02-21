#!/usr/bin/env python2.7
from __future__ import print_function, division

import argparse
import time
import sys
import Fusion_manifest
import RNASeq_manifest
import csv
from collections import defaultdict
import json
import ConfigParser

'''
Takes the data read from a CGP manifest and creates a dictionary that
can be converted into the same JSON structure that is returned by
an Elastic Search query

Input:
sample_data_binned_by_sample_uuid - the manifest data with samples
stored in a dict by key sample uuid  

Output:
A dictionary of samples that can be converted to JSON in Elastic Search format
'''
def create_elastic_search_result_formatted_json(sample_data_binned_by_sample_uuid):
    hits = []
    #get the set of files for a sample uuid
    for bundle_uuid, sample_files in sample_data_binned_by_sample_uuid.iteritems():

        #print('sample files:{}'.format(sample_files))

        workflow_outputs = []
        analysis_element = {}
        sample_element = {}
        specimen_element = {}
        source_element = {}
        for file_info in sample_files:

            #print('file info:{}'.format(file_info))

            workflow_outputs_element = {'file_path' : file_info['file_path'], 'file_type' : file_info['file_type']}
            workflow_outputs.append(workflow_outputs_element)

            analysis_element['workflow_outputs'] = workflow_outputs
            analysis_element['analysis_type'] = file_info['analysis_type']
            analysis_element['bundle_uuid'] = file_info['bundle_uuid']
            analysis_element['workflow_name'] = file_info['workflow_name']
            analysis_element['workflow_version'] = file_info['workflow_version']

            sample_element['sample_uuid'] = file_info['sample_uuid']
            sample_element['submitter_sample_id'] = file_info['submitter_sample_id']
 
            specimen_element['specimen_uuid'] = file_info['specimen_uuid']
            specimen_element['submitter_experimental_design'] = file_info['submitter_experimental_design']
            specimen_element['submitter_specimen_id'] = file_info['submitter_specimen_id']
            specimen_element['submitter_specimen_type'] = file_info['submitter_specimen_type']

            source_element['center_name'] = file_info['center_name']
            source_element['donor_uuid'] = file_info['donor_uuid']
            source_element['program'] = file_info['program']
            source_element['project'] = file_info['project']
            source_element['submitter_donor_id'] = file_info['submitter_donor_id']
            source_element['submitter_donor_primary_site'] = file_info['submitter_donor_primary_site']


        analysis = []
        analysis.append(analysis_element)
        sample_element['analysis'] = analysis

        samples = []
        samples.append(sample_element)
        specimen_element['samples'] = samples

        specimen = []
        specimen.append(specimen_element)
       
        source_element['specimen'] = specimen
        hit = {}
        hit['_source'] = source_element
        hits.append(hit)

    #json_str = json.dumps(hits, sort_keys=True, indent=4, separators=(',', ': '))
    #print("create_elastic_search_result_formatted_json - hits json:\n{}".format(json_str))
 
    return hits

'''
Reads a CGP manifest tsv file and stores the data in a dictionary
with samples stored under keys of sample uuids

Input: manifest - string of path to manifest tsv file 

Output: data_by_sample_uuid - dictionary of sample files stored
under the sample uuid key
'''
def get_sample_data_from_manifest(manifest):
    #open manifest as a tsv file
    with open(manifest, 'rb') as tsvin:
        tsvin = csv.reader(tsvin, delimiter = '\t')
        # read each row of the manifest into a dict
        # keeping rows together by sample uuid
        headers = next(tsvin)[1:]
        data_by_sample_uuid = defaultdict(list)
        for i, row in enumerate(tsvin):
            sample_uuid = row[11]
            sample_uuid_element = {}
            sample_uuid_element["program"] = row[0]
            sample_uuid_element["project"] = row[1]
            sample_uuid_element["center_name"] = row[2]
            sample_uuid_element["submitter_donor_id"] = row[3]
            sample_uuid_element["donor_uuid"] = row[4]
            sample_uuid_element["submitter_donor_primary_site"] = row[5]
            sample_uuid_element["submitter_specimen_id"] = row[6]
            sample_uuid_element["specimen_uuid"] = row[7]
            sample_uuid_element["submitter_specimen_type"] = row[8]
            sample_uuid_element["submitter_experimental_design"] = row[9]
            sample_uuid_element["submitter_sample_id"] = row[10]
            sample_uuid_element["sample_uuid"] = row[11]
            sample_uuid_element["analysis_type"] = row[12]
            sample_uuid_element["workflow_name"] = row[13]
            sample_uuid_element["workflow_version"] = row[14]
            sample_uuid_element["file_type"] = row[15]
            sample_uuid_element["file_path"] = row[16]
            sample_uuid_element["upload_file_id"] = row[17]
            sample_uuid_element["bundle_uuid"] = row[18]
            sample_uuid_element["metadata_id"] = row[19]
            data_by_sample_uuid[sample_uuid].append(sample_uuid_element)
    
        #json_str = json.dumps(data_by_sample_uuid, sort_keys=True, indent=4, separators=(',', ': '))
        #print("get_sample_data_from_manifest - data_by_sample_uuid json:\n{}".format(json_str))
    
    return data_by_sample_uuid


def parse_arguments():
    """
    Parse Command Line
    """
    parser = argparse.ArgumentParser(description='Reads samples from a manifest and runs the pipeline')

#    parser.add_argument( 'in_sample_manifest', nargs='?',type=argparse.FileType('r'), default=sys.stdin, const=sys.stdin, help='Input manifest file in tsv format' )
    parser.add_argument( 'in_sample_manifest',  help='Input manifest file in tsv format' )
    parser.add_argument( '-t','--storage-token', nargs='?', default="<my storage token>", const="<my storage token>",
                   type=str, help='Token for accessing the storage server, e.g a3f76853-65jk-8300-9uei-jfdkcu2d22' )
    parser.add_argument( '-s','--storage-server', nargs='?', default="<my storage server>", const="<my storage server>",
                   type=str, help='URL for the storage server; e.g. ucsc-cgp-dev.org' )
    parser.add_argument( '-r','--tool-runner', nargs='?', default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.22", \
                   const="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.22",
                   type=str, help='URL for the storage server; e.g. ucsc-cgp-dev.org' )
    parser.add_argument('--test-mode', action='store_true',
                        help='If this flag is used, workflow is not run')
    parser.add_argument( '-p','--pipeline', nargs='?', default="RNA-Seq", \
                   const="RNA-Seq",
                   type=str, help='Name of the pipeline to run; e.g. RNA-Seq' )
    parser.add_argument( '-v','--workflow_version', nargs='?', default="<workflow version>", \
                   const="<workflow version>",
                   type=str, help='Version of the pipeline to run; e.g. 0.3.1' )
    parser.add_argument('-a', '--all-samples-in-one-job', action='store_true',
                        help='If this flag is used, all samples will be run in one job')
    parser.add_argument('-b','--touch-file-bucket', nargs='?', default="<touch file bucket>", \
                   const="<touch file bucket>",
                        help='S3 bucket for storing touch files.')

    auto_scale_options  = parser.add_argument_group('Auto-scaling options')
    auto_scale_options.add_argument('--auto-scale', action='store_true', default=False,                                                                                                                                                                                  help='Enable Toil autoscaling. Disabled by default')
    auto_scale_options.add_argument('--job-store', default="aws:us-west-2:autoscaling-toil-rnaseq-jobstore-2",
                        help='Directory in cloud where working files will be put; ' \
                        'e.g. aws:us-west-2:autoscaling-toil-rnaseq-jobstore')
    auto_scale_options.add_argument('--cluster-name', default="",
                        help='Name of the Toil cluster. Usually the security group name')
    auto_scale_options.add_argument('--output-location', default="s3://toil-rnaseq-cloud-staging-area",
                        help='Directory in cloud where  output files will be put; e.g. s3://toil-rnaseq-cloud-staging-area')
    auto_scale_options.add_argument('--provisioner', default="aws",
                        help='Cloud provisioner to use. E.g aws')
    auto_scale_options.add_argument('--node-type', default="c3.8xlarge",
                        help='Cloud worker VM type; e.g. c3.8xlarge')
    auto_scale_options.add_argument('--max-nodes', type=int, default=2,
                        help='Maximum worker nodes to launch. E.g. 2')
    auto_scale_options.add_argument('--credentials-id', default="",
                        help='Credentials id')
    auto_scale_options.add_argument('--credentials-secret-key', default="",
                        help='Credentials secret key')
    auto_scale_options.add_argument('--credentials-file', default="/home/ubuntu/.aws/credentials",
                        help='<path/file_name> with access credentials. E.g /ubuntu/.aws/credentials')

    options = parser.parse_args()
    #if options.align != "global" and options.align != "local":
    #    parser.error("ERROR: Alignement must be either local or global")

    return (options)  

def __main__(args):
    """
    The program reads a CGP browser manifest tsv file and creates the parameterized
    JSON that is passed to the pipeline and embeds that into the paramterized
    JSON that is passed to the dockstore tool runner.

    If not in test mode Consonance is called to spin up a VM and the dockstore
    tool runner JSON is input so that dockstore tool runner is run on the VM
    which launches the pipeline with the embedded pipeline JSON. Each job is 
    queued up by Consonance an run.

    The all samples in one job option places all the sample files in one job
    for autoscaling purposes.
 
    This script requires AWS credentials to be on the host system.
    """
    start_time = time.time()    

    options = parse_arguments()
    #print("options:{}".format(options))

    sample_data_binned_by_sample_uuid = get_sample_data_from_manifest(options.in_sample_manifest)
    hits = create_elastic_search_result_formatted_json(sample_data_binned_by_sample_uuid)

    if options.credentials_id and options.credentials_secret_key:
        credentials_id = options.credentials_id
        credentials_secret_key = options.credentials_secret_key
        #print('getting credentials as separate inputs')
    elif options.credentials_file:
        config = ConfigParser.ConfigParser()
        config.read(options.credentials_file)
        #print('credentials file keys:{}'.format(config.sections()))
        credentials_id = config.get('default','aws_access_key_id')
        credentials_secret_key = config.get('default','aws_secret_access_key')


    if options.pipeline == 'Fusion':
        coordinator = Fusion_manifest.FusionCoordinator(
                 options.touch_file_bucket, options.storage_token, \
                 options.storage_server, options.tool_runner, \
                 workflow_version = options.workflow_version, test_mode = options.test_mode)
        if options.all_samples_in_one_job and not coordinator.supports_multiple_samples_per_job():
            print('ERROR: The Fusion pipeline does not support all samples in one job; processing will not be started.')
            sys.exit(1)
    elif options.pipeline == 'RNA-Seq':   
        coordinator = RNASeq_manifest.RNASeqCoordinator(
                 options.touch_file_bucket, options.storage_token, \
                 options.storage_server, options.tool_runner, \
                 workflow_version = options.workflow_version, 
                 all_samples_in_one_job = options.all_samples_in_one_job, \
                 test_mode = options.test_mode, \
                 auto_scale = options.auto_scale, \
                 job_store = options.job_store, \
                 cluster_name = options.cluster_name, \
                 provisioner = options.provisioner, \
                 output_location = options.output_location, \
                 max_nodes = options.max_nodes, \
                 node_type = options.node_type, \
                 credentials_id = credentials_id, \
                 credentials_secret_key = credentials_secret_key)


    list_of_jobs = coordinator.requires(hits)

    for job in list_of_jobs:
        job.run()


    print("----- %s seconds -----" % (time.time() - start_time), file=sys.stderr)

if __name__ == '__main__':
    sys.exit(__main__(sys.argv))


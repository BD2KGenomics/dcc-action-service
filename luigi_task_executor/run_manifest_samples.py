#!/usr/bin/env python2.7
from __future__ import print_function, division

import argparse
import time
import sys
import Fusion_manifest
import csv
from collections import defaultdict
import json

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

    json_str = json.dumps(hits, sort_keys=True, indent=4, separators=(',', ': '))
    print("create_elastic_search_result_formatted_json - hits json:\n{}".format(json_str))
 
    return hits


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
    
        json_str = json.dumps(data_by_sample_uuid, sort_keys=True, indent=4, separators=(',', ': '))
        print("get_sample_data_from_manifest - data_by_sample_uuid json:\n{}".format(json_str))
    
    return data_by_sample_uuid


def parse_arguments():
    """
    Parse Command Line
    """
    parser = argparse.ArgumentParser(description='Reads samples from a manifest and runs the pipeline')

#    parser.add_argument( 'in_sample_manifest', nargs='?',type=argparse.FileType('r'), default=sys.stdin, const=sys.stdin, help='Input manifest file in tsv format' )
    parser.add_argument( 'in_sample_manifest',  help='Input manifest file in tsv format' )

    '''
    parser.add_argument( '-s', '--subst_matrix', nargs='?', type=str, 
    default="https://users.soe.ucsc.edu/~karplus/bme205/f15/BLOSUM62",
      help='Substitution probability table' )


    parser.add_argument( '-a','--align', nargs='?', default="global", const="global",
                   type=str, help='Specifies either global or local alignment' )
    parser.add_argument( '-o','--open', nargs='?', default=12, const=12, 
                   type=int, help='Gap opening penalty')
    parser.add_argument( '-e','--extend', nargs='?', default=1, const=1, 
                             type=int, help='Gap extenstion penalty')
    parser.add_argument( '-d','--double', nargs='?', default=3, const=3,
                 type=int, help='Penalty for moving from a gap in one sequence \
                                            to a gap in another')
    '''

    options = parser.parse_args()
    #if options.align != "global" and options.align != "local":
    #    parser.error("ERROR: Alignement must be either local or global")

    return (options)  

def __main__(args):
    """
    The program reads a fasta file from stdin with multiple sequences and outputs
    an alignment as an A2M file to stdout with the same sequences in the same order,
    and with the same names and comments on the id lines.

    The fasta files may have alignment characters in them, like spaces, periods, 
    and hyphens. These are not part of the sequences and are removed or ignored. 
    The alphabet for the fasta input is taken to be the characters used as indices
    in the substitution matrix. Whether letters in the fasta file are upper- or 
    lower-case is also be ignored. 

    The first sequence of the fasta file is taken to be the "master" sequence
    and is echoed to stdout. All subsequent sequences are aligned to the first
    sequence and output to stdout. The score for each alignment is output to stderr.

    The gap penalties are by default open=12, extend=1, double_gap=3.

    The complete output of the program is be a legal A2M file. 
    """
    start_time = time.time()    

    options = parse_arguments()

    sample_data_binned_by_sample_uuid = get_sample_data_from_manifest(options.in_sample_manifest)
    hits = create_elastic_search_result_formatted_json(sample_data_binned_by_sample_uuid)

    coordinator = Fusion_manifest.FusionCoordinator(
                 'touch_file_bucket', '<myredwoodtoken>', \
                 'walt.ucsc-cgp-dev.org', 'dockstore_tool_running_dockstore_tool')
   
    coordinator.requires(hits)


#    if options.align == "local":

    print("----- %s seconds -----" % (time.time() - start_time), file=sys.stderr)

if __name__ == '__main__':
    sys.exit(__main__(sys.argv))


from __future__ import print_function, division

import sys
from collections import defaultdict
from base_manifest_decider import base_Coordinator

class FusionCoordinator(base_Coordinator):

    def __init__(self, touch_file_bucket, redwood_token, \
                 redwood_host, dockstore_tool_running_dockstore_tool, \
                 es_index_host ='localhost', es_index_port ='9200', \
                 tmp_dir ='/datastore', max_jobs = -1, process_sample_uuids = "", \
                 workflow_version = "", \
                 vm_instance_type ='c4.8xlarge', vm_region ='us-west-2', \
                 test_mode = True, center = "", program = "", project = ""):


            base_Coordinator.__init__(self, touch_file_bucket, redwood_token, \
                 redwood_host, dockstore_tool_running_dockstore_tool, \
                 es_index_host, es_index_port, \
                 tmp_dir, max_jobs, process_sample_uuids, \
                 workflow_version, \
                 vm_instance_type, vm_region, \
                 test_mode, center, program, project)


    '''
    Return a string that is the name that will be used to name the touchfile path
    '''
    def get_pipeline_name(self):
        return 'Fusion'
   
    '''
    Return a dictionary of CWL option to reference file name so this information
    can be added to the parameterized JSON input to the pipeline
    '''
    def get_cgp_job_reference_files(self):
        cwl_option_to_reference_file_name = defaultdict()        

        #Add the correct CWL option and reference file names here
        cwl_option_to_reference_file_name['index'] = "STARFusion-GRCh38gencode23.tar.gz" 

        return cwl_option_to_reference_file_name

    '''
    Returns a dictionary of keyword used in the dockstore tool runner parameterized JSON
    and used in Elastic search to find needed samples. The JSON data does not depend
    on samples found in the Elastic search so can be added here
    '''
    def get_pipeline_job_fixed_metadata(self):
        cgp_pipeline_job_fixed_metadata = defaultdict()

        cgp_pipeline_job_fixed_metadata["launch_type"] = "tool"
        cgp_pipeline_job_fixed_metadata["analysis_type"] = "fusion_variant_calling"
        cgp_pipeline_job_fixed_metadata["target_tool_prefix"] = 'registry.hub.docker.com/ucsctreehouse/fusion'
        cgp_pipeline_job_fixed_metadata["target_tool_url"] = \
                   "https://dockstore.org/containers/registry.hub.docker.com/ucsctreehouse/fusion/"

        cgp_pipeline_job_fixed_metadata["input_data_analysis_type"] = "sequence_upload"
        cgp_pipeline_job_fixed_metadata["input_data_experimental_design"] = "RNA-Seq"
        cgp_pipeline_job_fixed_metadata["normal_missing_item"] = "normal_fusion_workflow_0_2_x" 
        cgp_pipeline_job_fixed_metadata["normal_present_item"] = "normal_fusion_workflow_0_2_x"
        cgp_pipeline_job_fixed_metadata["tumor_missing_item"] = "tumor_fusion_workflow_0_2_x"
        cgp_pipeline_job_fixed_metadata["tumor_present_item"] = "tumor_fusion_workflow_0_2_x"
        cgp_pipeline_job_fixed_metadata["normal_metadata_flag"] = "normal_fusion_workflow_0_2_x"
        cgp_pipeline_job_fixed_metadata["tumor_metadata_flag"] = "tumor_fusion_workflow_0_2_x"


        return cgp_pipeline_job_fixed_metadata

    '''
    Returns a dictionary of keywords to metadata that is used to setup the touch file path
    and metadata and dockstore JSON file names. These sometimes depend on the sample name
    or other information found through the Elastic search so is separated from the method
    that gets the fixed metadata. This routine adds to the metadata dictionary for the pipeline.
    '''
    def get_pipeline_job_customized_metadata(self, cgp_pipeline_job_metadata):
        cgp_pipeline_job_metadata['file_prefix'] = cgp_pipeline_job_metadata["submitter_sample_id"]
        cgp_pipeline_job_metadata['metadata_json_file_name'] = cgp_pipeline_job_metadata['file_prefix'] + '_meta_data.json'
        cgp_pipeline_job_metadata["last_touch_file_folder_suffix"] = ""

        return cgp_pipeline_job_metadata


    '''
    Edit the following lines to set up the pipeline tool/workflow CWL options. This method
    returns a dictionary of CWL keywords and values that make up the CWL input parameterized
    JSON for the pipeline. This is the input to the pipeline to be run from Dockstore. 
    '''
    '''
    def construct_pipeline_parameterized_json(self, cgp_pipeline_job_json, redwood_host,
                 bundle_uuid, file_uuid, file_type, file_path, output_file_basename):

        file_path = 'redwood' + '://' + redwood_host + '/' + bundle_uuid + '/' + \
                       file_uuid + "/" + file["file_path"]

        if 'fastq1' not in cgp_pipeline_job_json.keys():
            cgp_pipeline_job_json['fastq1'] = defaultdict(dict)
            cgp_pipeline_job_json['fastq1'] = {"class" : "File", "path" : file_path}
        elif 'fastq2' not in cgp_pipeline_job_json.keys():
            cgp_pipeline_job_json['fastq2'] = defaultdict(dict)
            cgp_pipeline_job_json['fastq2'] = {"class" : "File", "path" : file_path}
        else:
            print("ERROR: Too many input files for Fusion pipeline in analysis output; extra file is:{}!!!".format(file_path), file=sys.stderr)
            return [];

        cgp_pipeline_job_json["root-ownership"] = True
        cgp_pipeline_job_json["tar_gz"] = output_file_basename
        cgp_pipeline_job_json["cpu"] = 32

        # Specify the output files here, using the options in the CWL file as keys
        file_path = "/tmp/{}.tar.gz".format(output_file_basename)
        cgp_pipeline_job_json["output_files"] = {"class" : "File", "path" : file_path}

        return cgp_pipeline_job_json
   

    def get_pipeline_parameterized_json(self, cgp_pipeline_job_metadata, analysis):
        cgp_pipeline_job_json = defaultdict()

        if 'parent_uuids' not in cgp_pipeline_job_metadata.keys():
            cgp_pipeline_job_metadata["parent_uuids"] = []

        if cgp_pipeline_job_metadata["sample_uuid"] not in cgp_pipeline_job_metadata["parent_uuids"]:
            cgp_pipeline_job_metadata["parent_uuids"].append(cgp_pipeline_job_metadata["sample_uuid"])


        for file in analysis["workflow_outputs"]:
            print("\nfile type:"+file["file_type"])
            print("\nfile name:"+file["file_path"])

            #if (file["file_type"] != "bam"): output an error message?

            cgp_pipeline_job_json = self.construct_pipeline_parameterized_json(self, cgp_pipeline_job_json, redwood_host,
                 bundle_uuid, file_uuid, file_type, file_path, cgp_pipeline_job_metadata["submitter_sample_id"])

        if 'fastq1' not in cgp_pipeline_job_json.keys() or 'fastq2' not in cgp_pipeline_job_json.keys():
            #we must have paired end reads for the Fusion pipeline so return an empty
            #list to indicate an error if we get here
            print("\nERROR: UNABLE TO GET BOTH FASTQ FILES FOR FUSION PIPELINE; INCOMPLETE JSON IS:{}".format(cgp_pipeline_job_json) , file=sys.stderr)
            return [];
        else:
            return cgp_pipeline_job_json


    '''
    def get_pipeline_parameterized_json(self, cgp_pipeline_job_metadata, analysis):
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
                print("ERROR: Too many input files for Fusion pipeline in analysis output; extra file is:{}!!!".format(file_path), file=sys.stderr)
                return [];

            if 'parent_uuids' not in cgp_pipeline_job_metadata.keys():
                cgp_pipeline_job_metadata["parent_uuids"] = []
                                
            if cgp_pipeline_job_metadata["sample_uuid"] not in cgp_pipeline_job_metadata["parent_uuids"]: 
                cgp_pipeline_job_metadata["parent_uuids"].append(cgp_pipeline_job_metadata["sample_uuid"])

            cgp_pipeline_job_json["outputdir"] = '.'
            cgp_pipeline_job_json["root-ownership"] = True
            cgp_pipeline_job_json["cpu"] = 32

            # Specify the output files here, using the options in the CWL file as keys
            file_path = "/tmp/star-fusion-gene-list-filtered.final"
            cgp_pipeline_job_json["output1"] = {"class" : "File", "path" : file_path}
            file_path = "/tmp/star-fusion-gene-list-filtered.final.bedpe"
            cgp_pipeline_job_json["output2"] = {"class" : "File", "path" : file_path}
            file_path = "/tmp/star-fusion-non-filtered.final"
            cgp_pipeline_job_json["output3"] = {"class" : "File", "path" : file_path}
            file_path = "/tmp/star-fusion-non-filtered.final.bedpe"
            cgp_pipeline_job_json["output4"] = {"class" : "File", "path" : file_path}

        if 'fastq1' not in cgp_pipeline_job_json.keys() or 'fastq2' not in cgp_pipeline_job_json.keys():
            #we must have paired end reads for the Fusion pipeline so return an empty
            #list to indicate an error if we get here
            print("\nERROR: UNABLE TO GET BOTH FASTQ FILES FOR FUSION PIPELINE; INCOMPLETE JSON IS:{}".format(cgp_pipeline_job_json) , file=sys.stderr)
            return [];
        else:
            return cgp_pipeline_job_json

if __name__ == '__main__':
    sys.exit(__main__(sys.argv))
    #luigi.run()

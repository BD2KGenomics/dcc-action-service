from __future__ import print_function, division

import sys
from collections import defaultdict
from base_manifest_decider import base_Coordinator

class RNASeqCoordinator(base_Coordinator):

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
        return 'RNA-Seq'
   
    '''
    Return a dictionary of CWL option to reference file name so this information
    can be added to the parameterized JSON input to the pipeline
    '''
    def get_cgp_job_reference_files(self):
        cwl_option_to_reference_file_name = defaultdict()        

        #Add the correct CWL option and reference file names here
        cwl_option_to_reference_file_name['rsem'] = "rsem_ref_hg38_no_alt.tar.gz" 
        cwl_option_to_reference_file_name['star'] = "starIndex_hg38_no_alt.tar.gz" 
        cwl_option_to_reference_file_name['kallisto'] = "kallisto_hg38.idx"

        return cwl_option_to_reference_file_name

    '''
    Returns a dictionary of keyword used in the dockstore tool runner parameterized JSON
    and used in Elastic search to find needed samples. The JSON data does not depend
    on samples found in the Elastic search so can be added here
    '''
    def get_pipeline_job_fixed_metadata(self):
        cgp_pipeline_job_fixed_metadata = defaultdict()

        cgp_pipeline_job_fixed_metadata["launch_type"] = "tool"
        cgp_pipeline_job_fixed_metadata["analysis_type"] = "rna_seq_quantification"
        cgp_pipeline_job_fixed_metadata["target_tool_prefix"] = 'quay.io/ucsc_cgl/rnaseq-cgl-pipeline'
        cgp_pipeline_job_fixed_metadata["target_tool_url"] = \
                   "https://dockstore.org/containers/quay.io/ucsc_cgl/rnaseq-cgl-pipeline"

        cgp_pipeline_job_fixed_metadata["input_data_analysis_type"] = "sequence_upload"
        cgp_pipeline_job_fixed_metadata["input_data_experimental_design"] = "RNA-Seq"
        cgp_pipeline_job_fixed_metadata["normal_missing_item"] = "normal_rna_seq_cgl_workflow_3_2_x" 
        cgp_pipeline_job_fixed_metadata["normal_present_item"] = "normal_rna_seq_cgl_workflow_3_2_x"
        cgp_pipeline_job_fixed_metadata["tumor_missing_item"] = "tumor_rna_seq_cgl_workflow_3_2_x"
        cgp_pipeline_job_fixed_metadata["tumor_present_item"] = "tumor_rna_seq_cgl_workflow_3_2_x"
        cgp_pipeline_job_fixed_metadata["normal_metadata_flag"] = "normal_rna_seq_cgl_workflow_3_2_x"
        cgp_pipeline_job_fixed_metadata["tumor_metadata_flag"] = "tumor_rna_seq_cgl_workflow_3_2_x"


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
    def get_pipeline_parameterized_json(self, cgp_pipeline_job_metadata, analysis):
        cgp_pipeline_job_json = defaultdict()

        paired_files = []
        for file in analysis["workflow_outputs"]:
            print("\nfile type:"+file["file_type"])
            print("\nfile name:"+file["file_path"])

            #if (file["file_type"] != "bam"): output an error message?

            file_path = 'redwood' + '://' + self.redwood_host + '/' + analysis['bundle_uuid'] + '/' + \
                       self.fileToUUID(file["file_path"], analysis["bundle_uuid"]) + \
                       "/" + file["file_path"]

            if (file["file_type"] == "fastq" or
                file["file_type"] == "fastq.gz"):
                    #if there is only one sequence upload output then this must
                    #be a single read sample
                    if( len(analysis["workflow_outputs"]) == 1):
                        if 'sample-single' not in cgp_pipeline_job_json.keys():
                            cgp_pipeline_job_json['sample-single'] = defaultdict(dict)
                        cgp_pipeline_job_json['sample-single'] = {"class" : "File", "path" : file_path}  
                    #otherwise we must be dealing with paired reads
                    else:
                        paired_files.append(file_path)
                        paired_files_comma_separated = ",".join(paired_files)
                        if 'sample-paired' not in cgp_pipeline_job_json.keys():
                            cgp_pipeline_job_json['sample-paired'] = defaultdict(dict)
                        cgp_pipeline_job_json['sample-paired'] = {"class" : "File", "path" : paired_files_comma_separated}
            elif (file["file_type"] == "fastq.tar"):
                if 'sample-tar' not in cgp_pipeline_job_json.keys():
                    cgp_pipeline_job_json['sample-tar'] = defaultdict(dict)
                cgp_pipeline_job_json['sample-tar'] = {"class" : "File", "path" : file_path}


            if 'parent_uuids' not in cgp_pipeline_job_metadata.keys():
                cgp_pipeline_job_metadata["parent_uuids"] = []
                                
            if cgp_pipeline_job_metadata["sample_uuid"] not in cgp_pipeline_job_metadata["parent_uuids"]: 
                cgp_pipeline_job_metadata["parent_uuids"].append(cgp_pipeline_job_metadata["sample_uuid"])

            cgp_pipeline_job_json["save-wiggle"] = False
            cgp_pipeline_job_json["no-clean"] = False
            cgp_pipeline_job_json["save-bam"] = True
            cgp_pipeline_job_json["disable-cutadapt"] = False
            cgp_pipeline_job_json["resume"] = ''
            cgp_pipeline_job_json["save-wiggle"] = False
            cgp_pipeline_job_json["cores"] = 32
            cgp_pipeline_job_json["work-mount"] = '/datastore' 
            cgp_pipeline_job_json["cores"] = 32
            cgp_pipeline_job_json["output-basename"] = cgp_pipeline_job_metadata["submitter_sample_id"]

            # Specify the output files here, using the options in the CWL file as keys
            file_path = "/tmp/{}.tar.gz".format(cgp_pipeline_job_metadata["submitter_sample_id"])
            cgp_pipeline_job_json["output_files"] = [{"class" : "File", "path" : file_path}]

        # Make sure we have a sample file or set of sample files
        a = ['sample-single', 'sample-paired', 'sample-tar']
        cgp_pipeline_json_keys = cgp_pipeline_job_json.keys()
        any_in = lambda a, cgp_pipeline_json_keys: any(i in cgp_pipeline_json_keys for i in a)
        if not any_in(a,cgp_pipeline_json_keys):
            #we must have single or paired end reads or tar file for the RNA-Seq pipeline so return an empty
            #list to indicate an error if we get here
            print("\nERROR: UNABLE TO GET INPUT FASTQ FILES OR TAR FILE FOR RNA-Seq PIPELINE; INCOMPLETE JSON IS:{}".format(cgp_pipeline_job_json) , file=sys.stderr)
            return [];
        # Make sure an even number of sample files are in a paired sample list
        if 'sample-paired' in cgp_pipeline_json_keys:
            paired_sample_list = cgp_pipeline_job_json['sample-paired']['path'].split(',')
            if len(paired_sample_list) % 2 != 0:
                #we must have an even number of sample pairs for the RNA-Seq pipeline so return an empty
                #list to indicate an error if we get here
                print("\nERROR: ODD NUMBER OF INPUT FASTQ FILES IN SAMPLE PAIRS FOR RNA-Seq PIPELINE; INCOMPLETE JSON IS:{}".format(cgp_pipeline_job_json) , file=sys.stderr)
                return [];

        return cgp_pipeline_job_json


if __name__ == '__main__':
    sys.exit(__main__(sys.argv))

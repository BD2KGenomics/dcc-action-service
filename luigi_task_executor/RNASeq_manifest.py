from __future__ import print_function, division

import sys
from collections import defaultdict
from base_manifest_decider import base_Coordinator

class RNASeqCoordinator(base_Coordinator):

    def __init__(self, touch_file_bucket, redwood_token, \
                 redwood_host, dockstore_tool_running_dockstore_tool, \
                 es_index_host ='localhost', es_index_port ='9200', \
                 tmp_dir ='/datastore', max_jobs = -1, process_sample_uuids = "", \
                 workflow_version = "",\
                 vm_instance_type ='c4.8xlarge', vm_region ='us-west-2', \
                 test_mode = True, center = "", program = "", project = "", \
                 all_samples_in_one_job = False, auto_scale = False, \
                 job_store = "", cluster_name = "", provisioner = "aws", \
                 output_location = "", \
                 max_nodes = 1, node_type = "c3.8xlarge", credentials_id = "", \
                 credentials_secret_key = ""):
           

            base_Coordinator.__init__(self, touch_file_bucket, redwood_token, \
                 redwood_host, dockstore_tool_running_dockstore_tool, \
                 es_index_host, es_index_port, \
                 tmp_dir, max_jobs, process_sample_uuids, \
                 workflow_version, \
                 vm_instance_type, vm_region, \
                 test_mode, center, program, project, \
                 all_samples_in_one_job)



            self.auto_scale = auto_scale
            self.job_store = job_store
            self.cluster_name = cluster_name
            self.provisioner = provisioner
            self.output_location = output_location
            self.max_nodes = max_nodes
            self.node_type = node_type
            self.credentials_id = credentials_id
            self.credentials_secret_key = credentials_secret_key

    '''
    Return true if we can put multiple samples in the pipeline json
    '''
    def supports_multiple_samples_per_job(self):
        return True

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
        if self.auto_scale:
            cwl_option_to_reference_file_name['rsem-path'] = "rsem_ref_hg38_no_alt.tar.gz" 
            cwl_option_to_reference_file_name['star-path'] = "starIndex_hg38_no_alt.tar.gz" 
            cwl_option_to_reference_file_name['kallisto-path'] = "kallisto_hg38.idx"
            cwl_option_to_reference_file_name['hera-path'] = ""
        else:
            cwl_option_to_reference_file_name['rsem'] = "rsem_ref_hg38_no_alt.tar.gz" 
            cwl_option_to_reference_file_name['star'] = "starIndex_hg38_no_alt.tar.gz" 
            cwl_option_to_reference_file_name['kallisto'] = "kallisto_hg38.idx"
            cwl_option_to_reference_file_name['hera'] = ""

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
        cgp_pipeline_job_fixed_metadata["normal_missing_item"] = "normal_rna_seq_cgl_workflow_3_3_x" 
        cgp_pipeline_job_fixed_metadata["normal_present_item"] = "normal_rna_seq_cgl_workflow_3_3_x"
        cgp_pipeline_job_fixed_metadata["tumor_missing_item"] = "tumor_rna_seq_cgl_workflow_3_3_x"
        cgp_pipeline_job_fixed_metadata["tumor_present_item"] = "tumor_rna_seq_cgl_workflow_3_3_x"
        cgp_pipeline_job_fixed_metadata["normal_metadata_flag"] = "normal_rna_seq_cgl_workflow_3_3_x"
        cgp_pipeline_job_fixed_metadata["tumor_metadata_flag"] = "tumor_rna_seq_cgl_workflow_3_3_x"


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

    def add_sample_info_to_parameterized_json(self, cgp_pipeline_job_metadata, cgp_pipeline_job_json):
        #now add the rest of the pipeline json parameters
        #specified by the CWL
        if 'parent_uuids' not in cgp_pipeline_job_json.keys():
            cgp_pipeline_job_metadata["parent_uuids"] = []
        if cgp_pipeline_job_metadata["sample_uuid"] not in cgp_pipeline_job_metadata["parent_uuids"]: 
            cgp_pipeline_job_metadata["parent_uuids"].append(cgp_pipeline_job_metadata["sample_uuid"])

        if 'output_basenames' not in cgp_pipeline_job_json.keys():
            cgp_pipeline_job_json['output_basenames'] = []
        cgp_pipeline_job_json["output_basenames"].append(cgp_pipeline_job_metadata["submitter_sample_id"])

        # Specify the output files here, using the options in the CWL file as keys
        tar_file_path = "/tmp/{}.tar.gz".format(cgp_pipeline_job_metadata["submitter_sample_id"])

        if 'output_files' not in cgp_pipeline_job_json.keys():
            cgp_pipeline_job_json["output_files"] = []
        cgp_pipeline_job_json["output_files"].append({"class" : "File", "path" : tar_file_path})

        if cgp_pipeline_job_json["save-bam"]:
            if 'bam_files' not in cgp_pipeline_job_json.keys():
                cgp_pipeline_job_json["bam_files"] = []
            bam_file_path = "/tmp/{}.sortedByCoord.md.bam".format(cgp_pipeline_job_metadata["submitter_sample_id"])
            cgp_pipeline_job_json["bam_files"].append({"class" : "File", "path" : bam_file_path})

        if cgp_pipeline_job_json["save-wiggle"]:
            if 'wiggle_files' not in cgp_pipeline_job_json.keys():
                cgp_pipeline_job_json["wiggle_files"] = []
            wiggle_file_path = "/tmp/{}.wiggle.bg".format(cgp_pipeline_job_metadata["submitter_sample_id"])
            cgp_pipeline_job_json["wiggle_files"].append({"class" : "File", "path" : wiggle_file_path})



    '''
    Edit the following lines to set up the pipeline tool/workflow CWL options. This method
    returns a dictionary of CWL keywords and values that make up the CWL input parameterized
    JSON for the pipeline. This is the input to the pipeline to be run from Dockstore.
    This method is called by base_manifest.py so self is the base_manifest object 
    '''
    def get_pipeline_parameterized_json(self, cgp_pipeline_job_metadata, analysis, cgp_pipeline_job_json):
        #cgp_pipeline_job_json = defaultdict()

        cgp_pipeline_job_json["save-wiggle"] = False
        cgp_pipeline_job_json["no-clean"] = False
        cgp_pipeline_job_json["save-bam"] = True
        cgp_pipeline_job_json["disable-cutadapt"] = False
        cgp_pipeline_job_json["resume"] = ''
        cgp_pipeline_job_json["save-wiggle"] = False
        cgp_pipeline_job_json["cores"] = 32
        cgp_pipeline_job_json["work-mount"] = '/datastore' 
        cgp_pipeline_job_json["cores"] = 32

        paired_files = []
        for file in analysis["workflow_outputs"]:
            print("\nfile type:"+file["file_type"])
            print("\nfile name:"+file["file_path"])

            #if (file["file_type"] != "bam"): output an error message?

#            if self.auto_scale:
                #If auto scaling is used with Toil then toil will download the
                #reference files so preface the file path with 'redwood_signed_url'
                #to let dockstore tool runner know it should generate a signed URL
                #to the storage system
#                file_path_prefix = 'redwood-signed-url'
#            else:
#                file_path_prefix = 'redwood'

            '''
            file_path_prefix = 'redwood'
            file_path = file_path_prefix + '://' + self.redwood_host + '/' + analysis['bundle_uuid'] + '/' + \
                   self.fileToUUID(file["file_path"], analysis["bundle_uuid"]) + \
                   "/" + file["file_path"]
            '''

            bundle_uuid = analysis['bundle_uuid']
            file_uuid = self.fileToUUID(file["file_path"], analysis["bundle_uuid"])
            file_name = file["file_path"]
            file_path = self.get_storage_system_file_path(bundle_uuid, file_uuid, file_name)

            if (file["file_type"] == "fastq" or
                file["file_type"] == "fastq.gz"):
                    file_path = self.get_storage_system_file_path(bundle_uuid, file_uuid, file_name)

                    #if there is only one sequence upload output then this must
                    #be a single read sample
                    if( len(analysis["workflow_outputs"]) == 1):
                        #If we get here there is only one single sample file 
                        #in this sequence upload workflow output
                        # so we can append the dict to the single sample list
                        if self.auto_scale:
                            if 'sample-single-paths' not in cgp_pipeline_job_json.keys():
                                cgp_pipeline_job_json['sample-single-paths'] = []
                            cgp_pipeline_job_json['sample-single-paths'].append(file_path)
                        else:
                            if 'sample-single' not in cgp_pipeline_job_json.keys():
                                cgp_pipeline_job_json['sample-single'] = []
                            #(there could be multiple single sample file dicts in
                            #the list if we are putting all samples in
                            #one pipeline job json)
                            cgp_pipeline_job_json['sample-single'].append({"class" : "File", "path" : file_path})
       
                        self.add_sample_info_to_parameterized_json(cgp_pipeline_job_metadata, cgp_pipeline_job_json)

                    #otherwise we must be dealing with paired reads
                    else:
                        #There are multiple paired read files in the workflow output
                        #So we need to create a list of them for later inclusion
                        #in the pipeline json
                        paired_files.append(file_path)
#                        paired_files_comma_separated = ",".join(paired_files)
#                        if 'sample-paired' not in cgp_pipeline_job_json.keys():
#                            cgp_pipeline_job_json['sample-paired'] = []
#                        cgp_pipeline_job_json['sample-paired'] = [{"class" : "File", "path" : paired_files_comma_separated}]
            elif (file["file_type"] == "fastq.tar"):
                if self.auto_scale:
                    if 'sample-tarp-paths' not in cgp_pipeline_job_json.keys():
                        cgp_pipeline_job_json['sample-tar-paths'] = []
                    cgp_pipeline_job_json['sample-tar-paths'].append(file_path) 
                else:
                    if 'sample-tar' not in cgp_pipeline_job_json.keys():
                        cgp_pipeline_job_json['sample-tar'] = []
                    #If we get here the sequence upload workflow output contains a
                    #tar file. There should only be one of these so we can append
                    #its dictionary to the tar file list (there could be multiple
                    #tar file dicts in the list if we are putting all samples in
                    #one pipeline job json)
                    cgp_pipeline_job_json['sample-tar'].append({"class" : "File", "path" : file_path})

                self.add_sample_info_to_parameterized_json(cgp_pipeline_job_metadata, cgp_pipeline_job_json)


        #if there are paired end read files and an even number of them
        if len(paired_files) > 0:
            if len(paired_files) % 2 != 0:
                #we must have an even number of sample pairs for the RNA-Seq pipeline so return an empty
                #list to indicate an error if we get here
                print('\nERROR: ODD NUMBER OF INPUT FASTQ FILES IN SAMPLE PAIRS '
                     'FOR RNA-Seq PIPELINE; WILL NOT ADD THESE '
                     'SAMPLES:{}'.format(paired_files) , file=sys.stderr)
            else:
                if self.auto_scale:
                    # if there are no paired samples in the pipeline json then
                    # set up the list to hold them
                    if 'sample-paired-paths' not in cgp_pipeline_job_json.keys():
                        cgp_pipeline_job_json['sample-paired-paths'] = []
                    # create a comma separated string of file paths
                    paired_files_comma_separated = ",".join(paired_files)
                    #use the 'sample-paired-paths' key so Dockstore doesn't try to download the files
                    #For auto scaling the Toil pipeline workers will want to download the files instead                   
                    cgp_pipeline_job_json['sample-paired-paths'].append(paired_files_comma_separated)
                else:
                    # if there are no paired samples in the pipeline json then
                    # set up the list to hold them
                    if 'sample-paired' not in cgp_pipeline_job_json.keys():
                        cgp_pipeline_job_json['sample-paired'] = []
                    for file in paired_files:
                        cgp_pipeline_job_json['sample-paired'].append({"class" : "File", "path" : file})
                        
                self.add_sample_info_to_parameterized_json(cgp_pipeline_job_metadata, cgp_pipeline_job_json)

        #now add the rest of the pipeline json parameters
        #specified by the CWL
#        if 'parent_uuids' not in cgp_pipeline_job_metadata.keys():
#            cgp_pipeline_job_metadata["parent_uuids"] = []
#        if cgp_pipeline_job_metadata["sample_uuid"] not in cgp_pipeline_job_metadata["parent_uuids"]: 
#            cgp_pipeline_job_metadata["parent_uuids"].append(cgp_pipeline_job_metadata["sample_uuid"])

      
        '''
        if 'output_basenames' not in cgp_pipeline_job_json.keys():
            cgp_pipeline_job_json['output_basenames'] = []
        cgp_pipeline_job_json["output_basenames"].append(cgp_pipeline_job_metadata["submitter_sample_id"])

        # Specify the output files here, using the options in the CWL file as keys
        tar_file_path = "/tmp/{}.tar.gz".format(cgp_pipeline_job_metadata["submitter_sample_id"])
        cgp_pipeline_job_json["output_files"] = [ {"class" : "File", "path" : tar_file_path} ]

        if cgp_pipeline_job_json["save-bam"]:
            bam_file_path = "/tmp/{}.sortedByCoord.md.bam".format(cgp_pipeline_job_metadata["submitter_sample_id"])
            cgp_pipeline_job_json["bam_files"] = [ {"class" : "File", "path" : bam_file_path} ]

        if cgp_pipeline_job_json["save-wiggle"]:
            wiggle_file_path = "/tmp/{}.wiggle.bg".format(cgp_pipeline_job_metadata["submitter_sample_id"])
            cgp_pipeline_job_json["wiggle_files"] = [ {"class" : "File", "path" : wiggle_file_path} ]
        '''

        #set up options for auto scaling if it is requested
        #otherwise the extra items are not used
        #print('!!!! self name:{}'.format(self.__class__.__name__))
        #from pprint import pprint
        #pprint(vars(self))

        if self.auto_scale:
            cgp_pipeline_job_json["auto-scale"] = True
            cgp_pipeline_job_json["job-store"] = self.job_store
            cgp_pipeline_job_json["cluster_name"] = self.cluster_name 
            cgp_pipeline_job_json["provisioner"] = self.provisioner
            cgp_pipeline_job_json["output-location"] = self.output_location
            cgp_pipeline_job_json["max-nodes"] = self.max_nodes
            cgp_pipeline_job_json["node-type"] = self.node_type
            cgp_pipeline_job_json["credentials-id"] = self.credentials_id
            cgp_pipeline_job_json["credentials-secret-key"] = self.credentials_secret_key

        # Make sure we have a sample file or set of sample filesi
        if self.auto_scale:
            a = ['sample-single-paths', 'sample-paired-paths', 'sample-tar-paths']
        else:
            a = ['sample-single', 'sample-paired', 'sample-tar']
        cgp_pipeline_json_keys = cgp_pipeline_job_json.keys()
        any_in = lambda a, cgp_pipeline_json_keys: any(i in cgp_pipeline_json_keys for i in a)
        if not any_in(a,cgp_pipeline_json_keys):
            #we must have single or paired end reads or tar file for the RNA-Seq pipeline so return an empty
            #dict to indicate an error if we get here
            print("\nERROR: UNABLE TO GET INPUT FASTQ FILES OR TAR FILE FOR RNA-Seq PIPELINE; INCOMPLETE JSON IS:{}".format(cgp_pipeline_job_json) , file=sys.stderr)
            cgp_pipeline_job_json.clear();
        # Make sure an even number of sample files are in a paired sample list
#        if 'sample-paired' in cgp_pipeline_json_keys:
#            for sample in cgp_pipeline_job_json['sample-paired']: 
#                paired_sample_list = sample['path'].split(',')
#                if len(paired_sample_list) % 2 != 0:
                    #we must have an even number of sample pairs for the RNA-Seq pipeline so return an empty
                    #list to indicate an error if we get here
#                    print("\nERROR: ODD NUMBER OF INPUT FASTQ FILES IN SAMPLE PAIRS FOR RNA-Seq PIPELINE; INCOMPLETE JSON IS:{}".format(cgp_pipeline_job_json) , file=sys.stderr)
#                    return {};

        return cgp_pipeline_job_json


if __name__ == '__main__':
    sys.exit(__main__(sys.argv))

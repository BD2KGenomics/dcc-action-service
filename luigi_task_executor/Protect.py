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

    # TODO : update to reflect protect pipeline parameters.

    json_dict = {}

    redwood_host = luigi.Parameter("storage.ucsc-cgl.org")
    redwood_token = luigi.Parameter("must_be_defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.19")

    workflow_version = luigi.Parameter(default="must be defined")

    target_tool_prefix = luigi.Parameter(default="quay.io/ucsc_cgl/protect")


    target_tool_url = luigi.Parameter(default="https://dockstore.org/containers/quay.io/ucsc_cgl/protect")
    workflow_type = luigi.Parameter(default="immuno_target_pipelines")
    image_descriptor = luigi.Parameter("must be defined")

    vm_region = luigi.Parameter(default='us-west-2')
    vm_instance_type = luigi.Parameter(default='c4.8xlarge')

    ''' 
    #so these aren't actually optional, but i can hardcode a path as default
    # TODO : UPDATE ALL THIS STUFF TO REFLECT WHAT WE'LL BE USING (new protect args)
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
    ### TODO ABOVE###
    '''

    tmp_dir = luigi.Parameter(default='/datastore') #equivalent of work mount
    sse_key = luigi.Parameter(default="")
    sse_key_is_master = luigi.BoolParameter(default= False)

    sample_name = luigi.Parameter(default='must input sample name')
    #output_filename = sample_name
    #submitter_sample_id = luigi.Parameter(default='must input submitter sample id')
    protect_job_json = luigi.Parameter(default="must input metadata")
    protect_reference_files_json = luigi.Parameter(default="must input reference file metadata")

    touch_file_path = luigi.Parameter(default='must input touch file path')
    metadata_json_file_name = luigi.Parameter(default='must input metadata json file name')

    #Consonance will not be called in test mode
    test_mode = luigi.BoolParameter(default = False)

    def run(self):
        print("\n\n\n** TASK RUN **")
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

        protect_job = json.loads(self.protect_job_json)
        output_base_name = protect_job['sample_name']
       
        json_dict = defaultdict() 
        if 'tumor_dna' in protect_job['tumor_dna'].keys():
            json_dict["tumor_dna"] = protect_job['tumor_dna']
        else:
            print("ERROR: no tumor dna file!", file=sys.stderr)
        json_dict["normal_dna"] = protect_job['normal_dna']
        json_dict["normal_dna2"] = protect_job['normal_dna2']
        json_dict["tumor_rna"] = protect_job['tumor_rna']
        json_dict["tumor_rna2"] = protect_job['tumor_rna2']
        json_dict["tumor_dna"] = protect_job['tumor_dna']
        json_dict["tumor_dna2"] = protect_job['tumor_dna2']

        json_dict["sample_name"] = protect_job['sample_name']
        json_dict["tumor_type"] = "STAD"
        json_dict["work_mount"] = "/datastore/"
        json_dict["reference_build"] = "hg38"
        json_dict["mail_to"] = "jqpublic@myschool.edu"

        json_dict["binding_predictions"] = [{"class" : "File", "path" : '/tmp/binding_predictions.tar'}]
        json_dict["expression"] = [{"class" : "File", "path" : "/tmp/expression.tar"}]
        json_dict["haplotyping"] = [{"class" : "File", "path" : "/tmp/haplotyping.tar"}]
        json_dict["merged_perchrom"] = [{"class" : "File", "path" : '/tmp/merged_perchrom.tar'}]
        json_dict['muse_perchrom'] = [{"class" : "File", "path" : '/tmp/muse_perchrom.tar'}]
        json_dict['mutect_perchrom'] = [{"class" : "File", "path" : '/tmp/mutect_perchrom.tar'}]
        json_dict['peptides'] = [{"class" : "File", "path" : '/tmp/peptides.tar'}]
        json_dict['radia_perchrom'] = [{"class" : "File", "path" : '/tmp/radia_perchrom.tar'}]
        json_dict['somaticsniper_perchrom'] = [{"class" : "File", "path" : '/tmp/somaticsniper_perchrom.tar'}]
        json_dict['strelka_snv_perchrom'] = [{"class" : "File", "path" : '/tmp/strelka_snv_perchrom.tar'}]
        json_dict['strelka_indel_perchrom'] = [{"class" : "File", "path" : '/tmp/strelka_indel_perchrom.tar'}]
        json_dict['rankboost'] = [{"class" : "File", "path" : '/tmp/rankboost.tar'}]
        json_dict['reports'] = [{"class" : "File", "path" : '/tmp/reports.tar'}]
        json_dict['normal_alignment'] = [{"class" : "File", "path" : '/tmp/normal_dna_fix_pg_sorted.bam'}]
        json_dict['normal_index'] = [{"class" : "File", "path" : '/tmp/normal_dna_fix_pg_sorted.bam.bai'}]
        json_dict['tumor_alignment'] = [{"class" : "File", "path" : '/tmp/tumor_dna_fix_pg_sorted.bam'}]
        json_dict['tumor_index'] = [{"class" : "File", "path" : '/tmp/tumor_dna_fix_pg_sorted.bam.bai'}]
        json_dict['rna_alignment'] = [{"class" : "File", "path" : '/tmp/rna_genome_sorted.bam'}]
        json_dict['rna_index'] = [{"class" : "File", "path" : '/tmp/rna_genome_sorted.bam.bai'}]
        json_dict['rna_transcriptome_alignment'] = [{"class" : "File", "path" : '/tmp/rna_transcriptome.bam'}]
        json_dict['all_merged'] = [{"class" : "File", "path" : '/tmp/all_merged.vcf'}]
        json_dict['mhci_merged'] = [{"class" : "File", "path" : '/tmp/mhci_rankboost_concise_results.tsv'}]
        json_dict['mhcii_merged'] = [{"class" : "File", "path" : '/tmp/mhcii_rankboost_concise_results.tsv'}]
        json_dict['all_snpeffed'] = [{"class" : "File", "path" : '/tmp/all_snpeffed.vcf'}]
        json_dict['all_transgened'] = [{"class" : "File", "path" : '/tmp/all_transgened.vcf'}]

        protect_reference_files = json.loads(self.protect_reference_files_json)

        for option, reference_files_dict in protect_reference_files.iteritems():
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
        parent_uuids = ','.join(map("{0}".format, protect_job['parent_uuids']))
        print("parent uuids:%s" % parent_uuids)

        p = self.save_dockstore_json().open('w')
        p_local = self.save_dockstore_json_local().open('w')

        target_tool= self.target_tool_prefix + ":" + self.workflow_version

        dockstore_json = {}
        dockstore_json["program_name"] = protect_job["program"].replace(' ','_')
        dockstore_json["json_encoded"] = base64_json_str
        dockstore_json["docker_uri"] = target_tool
        dockstore_json["dockstore_url" ] = self.target_tool_url
        dockstore_json["redwood_token" ] = self.redwood_token
        dockstore_json["redwood_host"] = self.redwood_host

        #dockstore_json["parent_uuids"] = parent_uuids
        #use only one parent uuid so the output metatdata
        #is only present once in the final metadata
        #this will ensure the output is presented only
        #once in the file browser
        #also since the pipeline input specifies the 
        #sample uniquely it doesn't matter which input
        #the output metadata is attached to
        dockstore_json["parent_uuids"] = protect_job['parent_uuids'][0]

        dockstore_json["workflow_type"] = self.workflow_type
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
                protect_job["consonance_job_uuid"] = consonance_output["job_uuid"]
            else:
                print("ERROR: COULD NOT FIND CONSONANCE JOB UUID IN CONSONANCE OUTPUT!", file=sys.stderr)
        else:
            print("TEST MODE: Consonance command would be:"+ cmd_str)
            protect_job["consonance_job_uuid"] = 'no consonance id in test mode'

        #remove the local parameterized JSON file that
        #was created for the Consonance call
        #since the Consonance call is finished
        self.save_dockstore_json_local().remove()

        #convert the meta data to a string and
        #save the donor metadata for the sample being processed to the touch
        # file directory
        protect_job_json = json.dumps(protect_job, sort_keys=True, indent=4, separators=(',', ': '))
        m = self.save_metadata_json().open('w')
        print(protect_job_json, file=m)
        m.close()


#        if result == 0:
#            cmd = "rm -rf "+self.data_dir+"/"+self.bundle_uuid+"/bamstats_report.zip "+self.data_dir+"/"+self.bundle_uuid+"/datastore/"
#            print "CLEANUP CMD: "+cmd
#            result = subprocess.call(cmd, shell=True)
#            if result == 0:
#                print "CLEANUP SUCCESSFUL"

         # NOW MAke a final report
#        f = self.output(protect_job['sample_name']).open('w')
        f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
        print("Consonance task is complete", file=f)
        f.close()
        print("\n\n\n\n** TASK RUN DONE **")

    def save_metadata_json(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/metadata.json' % (self.tmp_dir, task_uuid))
        return S3Target('s3://%s/%s_meta_data.json' % (self.touch_file_path, self.sample_name ))

    def save_dockstore_json_local(self):
        #task_uuid = self.get_task_uuid()
        #luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        #return S3Target('%s/%s_dockstore_tool.json' % (self.touch_file_path, self.submitter_sample_id ))
        return luigi.LocalTarget('/tmp/%s/%s_dockstore_tool.json' % (self.touch_file_path, self.sample_name ))

    def save_dockstore_json(self):
        #task_uuid = self.get_task_uuid()
        #luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/dockstore_tool.json' % (self.tmp_dir, task_uuid))
        return S3Target('s3://%s/%s_dockstore_tool.json' % (self.touch_file_path, self.sample_name ))

    def output(self):
        #task_uuid = self.get_task_uuid()
        #return luigi.LocalTarget('%s/consonance-jobs/RNASeq_3_1_x_Coordinator/fastq_gz/%s/finished.txt' % (self.tmp_dir, task_uuid))
        return S3Target('s3://%s/%s_finished.json' % (self.touch_file_path, self.sample_name ))


class ProtectCoordinator(luigi.Task):
    
    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_host = luigi.Parameter(default='storage.ucsc-cgp.org')
    image_descriptor = luigi.Parameter("must be defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/ucsc_cgl/dockstore-tool-runner:1.0.19")
    tmp_dir = luigi.Parameter(default='/datastore')
    max_jobs = luigi.Parameter(default='-1')
    bundle_uuid_filename_to_file_uuid = {}
    process_sample_uuid = luigi.Parameter(default = "")

    workflow_version = luigi.Parameter(default="2.5.0-1.12.3")
    touch_file_bucket = luigi.Parameter(default="must be input")

    vm_region = luigi.Parameter(default='us-west-2')
    vm_instance_type = luigi.Parameter(default='c4.8xlarge')

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
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_protect_cgl_workflow_2_5_x" : "false"}},{"term" : {"flags.tumor_protect_cgl_workflow_2_5_x" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc

        reference_cwl_switch_to_file = {
            'star_index' : 'star_with_fusion_100bp_readlen_indexes.tar.gz', \
            'bwa_index' : 'bwa_index.tar.gz', \
            'rsem_index' : 'rsem_index.tar.gz', \
            'genome_fasta' : 'hg38.fa.tar.gz', \
            'genome_fai' : 'hg38.fa.fai.tar.gz', \
            'genome_dict' : 'hg38.dict.tar.gz', \
            'dbsnp_vcf' : 'dbsnp_coding.vcf.gz', \
            'cosmic_vcf' : 'CosmicCodingMuts.vcf.tar.gz', \
            'cosmic_idx' : 'CosmicCodingMuts.vcf.idx.tar.gz', \
            'dbsnp_idx' : 'dbsnp_coding.vcf.idx.tar.gz', \
            'dbsnp_tbi' : 'dbsnp_coding.vcf.gz.tbi', \
            'cosmic_beds' : 'radia_cosmic.tar.gz', \
            'dbsnp_beds' : 'radia_dbsnp.tar.gz', \
            'retrogene_beds' : 'radia_retrogenes.tar.gz', \
            'pseudogene_beds' : 'radia_pseudogenes.tar.gz', \
            'gencode_beds' : 'radia_gencode.tar.gz', \
            'strelka_config' : 'strelka_bwa_WXS_config.ini.tar.gz', \
            'snpeff' : 'snpeff_index.tar.gz', \
            'transgene_peptide_fasta' : 'gencode.v25.pc_translations_NOPARY.fa.tar.gz', \
            'transgene_transcript_fasta' : 'gencode.v25.pc_transcripts_NOPARY.fa.tar.gz', \
            'transgene_annotation_gtf' : 'gencode.v25.annotation_NOPARY.gtf.tar.gz', \
            'transgene_genome' : 'hg38.fa.tar.gz', \
            'phlat' : 'phlat_index.tar.gz', \
            'mhci' : 'mhci_restrictions.json.tar.gz', \
            'mhcii' : 'mhcii_restrictions.json.tar.gz', \
            'mhc_pathways_file' : 'mhc_pathways.tsv.tar.gz', \
            'itx_resistance_file' : 'itx_resistance.tsv.tar.gz', \
            'immune_resistance_pathways_file' : 'immune_resistance_pathways.json.tar.gz', \
            'car_t_targets_file' : 'car_t_targets.tsv.tar.gz' 
        }

        listOfJobs = []

        protect_jobs  = defaultdict(dict)
        protect_jobs['samples'] = defaultdict(dict)

        #Get the reference file metadata from the storage system
        #and create a file path that the Dockstore tool runner can
        #used to download the reference file from the storage system
        #TODO fix this because there can be different files with 
        #the same name in the storage system?
        for switch, file_name in reference_cwl_switch_to_file.iteritems():
            print("switch:{} file name {}".format(switch, file_name))
            file_name_metadata_json = urlopen(str("https://metadata."+self.redwood_host+"/entities?fileName="+file_name), context=ctx).read()
            file_name_metadata = json.loads(file_name_metadata_json)
            print(str(file_name_metadata))
            bundle_uuid = file_name_metadata["content"][0]["gnosId"]
            file_uuid = file_name_metadata["content"][0]["id"]
            file_name = file_name_metadata["content"][0]["fileName"]

            ref_file_path = 'redwood' + '://' + self.redwood_host + '/' + bundle_uuid + '/' + \
                        file_uuid + "/" + file_name
            protect_jobs['hg38_reference_files'][switch] = {"class" : "File", "path" : ref_file_path}
            print(str(protect_jobs['hg38_reference_files'][switch]))



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

                    sample_name = sample["submitter_sample_id"][:-4]
                    print('sample name:{}'.format(sample_name))
                    sample_name_suffix = sample["submitter_sample_id"][-4:]
                    print('sample name suffix:{}'.format(sample_name_suffix))

                    workflow_version_dir = self.workflow_version.replace('.', '_')
                    touch_file_path_prefix = self.touch_file_bucket+"/consonance-jobs/ProTECT_Coordinator/" + workflow_version_dir
                    touch_file_path = touch_file_path_prefix+"/" \
                                       +hit["_source"]["center_name"]+"_" \
                                       +hit["_source"]["program"]+"_" \
                                       +hit["_source"]["project"]+"_" \
                                       +sample_name

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
 
                        if ( (analysis["analysis_type"] == "sequence_upload" and \
                              ((hit["_source"]["flags"]["normal_protect_cgl_workflow_2_5_x"] == False and \
                                   sample["sample_uuid"] in hit["_source"]["missing_items"]["normal_protect_cgl_workflow_2_5_x"] and \
                                   re.match("^Normal - ", specimen["submitter_specimen_type"])) or \
                               (hit["_source"]["flags"]["tumor_protect_cgl_workflow_2_5_x"] == False and \
                                   sample["sample_uuid"] in hit["_source"]["missing_items"]["tumor_protect_cgl_workflow_2_5_x"] and \
                                   re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour - |^Cell line -", specimen["submitter_specimen_type"]))))):


                            for file in analysis["workflow_outputs"]:
                                print("\nfile type:"+file["file_type"])
                                print("\nfile name:"+file["file_path"])

                                #if (file["file_type"] != "fastq" or
                                #    file["file_type"] != "fastq.gz"):

                                file_path = 'redwood' + '://' + self.redwood_host + '/' + analysis['bundle_uuid'] + '/' + \
                                    self.fileToUUID(file["file_path"], analysis["bundle_uuid"]) + \
                                    "/" + file["file_path"]

                                print('keys for ' + sample_name + ':' + ','.join(protect_jobs['samples'][sample_name].keys()))
                                if sample_name_suffix == '-T-D':
                                    if 'tumor_dna' not in protect_jobs['samples'][sample_name].keys():
                                        key = 'tumor_dna'
                                    else:
                                        key = 'tumor_dna2'
                                elif sample_name_suffix == '-N-D':
                                    if 'normal_dna' not in protect_jobs['samples'][sample_name].keys():
                                        key = 'normal_dna'
                                    else:
                                        key = 'normal_dna2'
                                elif sample_name_suffix == '-T-R':
                                    if 'tumor_rna' not in protect_jobs['samples'][sample_name].keys():
                                        key = 'tumor_rna'
                                    else:
                                        key = 'tumor_rna2'
                                else:
                                    print("ERROR in spinnaker input!!!", file=sys.stderr)

                                print("sample key is:{}".format(key))   
                                protect_jobs['samples'][sample_name][key] = {"class" : "File", "path" : file_path}

                                if 'parent_uuids' not in protect_jobs['samples'][sample_name].keys():
                                    protect_jobs['samples'][sample_name]["parent_uuids"] = []
                                
                                if sample["sample_uuid"] not in protect_jobs['samples'][sample_name]["parent_uuids"]: 
                                    protect_jobs['samples'][sample_name]["parent_uuids"].append(sample["sample_uuid"])


                            #This metadata will be passed to the Consonance Task and some
                            #some of the meta data will be used in the Luigi status page for the job
                            protect_jobs['samples'][sample_name]["sample_name"] = sample_name
                            protect_jobs['samples'][sample_name]["program"] = hit["_source"]["program"]
                            protect_jobs['samples'][sample_name]["project"] = hit["_source"]["project"]
                            protect_jobs['samples'][sample_name]["center_name"] = hit["_source"]["center_name"]
                            protect_jobs['samples'][sample_name]["submitter_donor_id"] = hit["_source"]["submitter_donor_id"]
                            protect_jobs['samples'][sample_name]["donor_uuid"] = hit["_source"]["donor_uuid"]
                            if "submitter_donor_primary_site" in hit["_source"]:
                                protect_jobs['samples'][sample_name]["submitter_donor_primary_site"] = hit["_source"]["submitter_donor_primary_site"]
                            else:
                                protect_jobs['samples'][sample_name]["submitter_donor_primary_site"] = "not provided"
                            protect_jobs['samples'][sample_name]["submitter_specimen_id"] = specimen["submitter_specimen_id"]
                            protect_jobs['samples'][sample_name]["specimen_uuid"] = specimen["specimen_uuid"]
                            protect_jobs['samples'][sample_name]["submitter_specimen_type"] = specimen["submitter_specimen_type"]
                            protect_jobs['samples'][sample_name]["submitter_experimental_design"] = specimen["submitter_experimental_design"]
                            protect_jobs['samples'][sample_name]["submitter_sample_id"] = sample["submitter_sample_id"]
                            protect_jobs['samples'][sample_name]["sample_uuid"] = sample["sample_uuid"]
                            protect_jobs['samples'][sample_name]["analysis_type"] = "immuno_target_pipelines"
                            protect_jobs['samples'][sample_name]["workflow_name"] = "quay.io/ucsc_cgl/protect"
                            protect_jobs['samples'][sample_name]["workflow_version"] = self.workflow_version

                            print("\nprotect jobs with meta data:", protect_jobs)


        for sample_num, sample_name in enumerate(protect_jobs['samples']):
            print('sample num:{}'.format(sample_num)) 
            if (sample_num < int(self.max_jobs) or int(self.max_jobs) < 0):
                protect_job_json = json.dumps(protect_jobs['samples'][sample_name], sort_keys=True, indent=4, separators=(',', ': '))
                print("\nmeta data:")
                print(protect_job_json)

                protect_reference_files_json = json.dumps(protect_jobs['hg38_reference_files'], sort_keys=True, indent=4, separators=(',', ': '))
                print("\nprotect reference files meta data:")
                print(protect_reference_files_json)

                metadata_json_file_name = sample_name + '_meta_data.json' 


                listOfJobs.append(ConsonanceTask(redwood_host=self.redwood_host, \
                    vm_region = self.vm_region, \
                    vm_instance_type=self.vm_instance_type,
                    redwood_token=self.redwood_token, \
                    image_descriptor=self.image_descriptor, \
                    dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool, \
                    sample_name = sample_name, \
                    tmp_dir=self.tmp_dir, \
                    workflow_version = self.workflow_version, \
                    #submitter_sample_id = protect_jobs['samples'][sample_name]['submitter_sample_id'], \
                    protect_job_json = protect_job_json, \
                    protect_reference_files_json = protect_reference_files_json, \
                    metadata_json_file_name = metadata_json_file_name, \
                    touch_file_path = touch_file_path, test_mode=self.test_mode))

            
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
        return S3Target('s3://'+self.touch_file_bucket+'/consonance-jobs/ProTECT_Coordinator/{}/Protect-{}.txt'.format(workflow_version_dir, ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"

if __name__ == '__main__':
    luigi.run()


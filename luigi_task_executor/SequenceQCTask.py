import luigi
import json
import time
import re
import datetime
import subprocess
from urllib import urlopen
from uuid import uuid4
from elasticsearch import Elasticsearch

# TODO:
# * need a tool that takes bundle ID and file path and converts to file_id for downloading by Dockstore CLI
# * I can fire off a Dockstore CLI to write the data back but who makes a metadata.json?  Should I add this to Dockstore CLI?  Add it here?
#   Probably, for now I can do everything in this task (maybe distinct tasks for download, upload, tool run)
#   and then, over time, push this out to Dockstore CLI.
#   So, effectively, I use Luigi really as a workflow but, ultimately, this will need to move out to worker VMs via Consonance and be done via
#   Dockstore CLI (or Toil) in order to gain flexibility.
# * need to move file outputs used to track running of steps to S3
# * am I doing sufficient error checking?
# * how do I deal with concurrency?  How can I run these concurrently?
# FIXME:
# * this doc makes it sounds like I will need to do some tracking of what's previously been run to prevent duplicates from launching? https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies
# RUNNING
# $> rm /tmp/foo-*; PYTHONPATH='' luigi --module SequenceQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200
# * index builder: 1) needs correct filetype and 2) needs just the filename and not the relative file path (exclude directories)
# rm -rf /tmp/SequenceQCTask* /tmp/afb54dff-41ad-50e5-9c66-8671c53a278b; PYTHONPATH='' luigi --module SequenceQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200 &> log.txt

class SequenceQCCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    ucsc_storage_client_path = luigi.Parameter(default='../ucsc-storage-client')
    ucsc_storage_host = luigi.Parameter(default='https://storage2.ucsc-cgl.org')
    tmp_dir = luigi.Parameter(default='/tmp')
    data_dir = luigi.Parameter(default='/tmp/data_dir')
    max_jobs = luigi.Parameter(default='1')
    bundle_uuid_filename_to_file_uuid = {}

    def requires(self):
        print "** COORDINATOR **"
        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        json_str = urlopen(str(self.ucsc_storage_host+":8444/entities?page=0")).read()
        metadata_struct = json.loads(json_str)
        print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
        for i in range(0, metadata_struct["totalPages"]):
            print "** CURRENT METADATA TOTAL PAGES: "+str(i)
            json_str = urlopen(str(self.ucsc_storage_host+":8444/entities?page="+str(i))).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        # now query elasticsearch
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc
        #res = es.search(index="analysis_index", body={"query":{"filtered":{"filter":{"bool":{"must":{"or":[{"terms":{"flags.normal_sequence_qc_report":["false"]}},{"terms":{"flags.tumor_sequence_qc_report":["false"]}}]}}},"query":{"match_all":{}}}}}, size=5000)
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_sequence_qc_report" : "false"}},{"term" : {"flags.tumor_sequence_qc_report" : "false"               }}],"minimum_should_match" : 1}}}, size=5000)

        listOfJobs = []

        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print("%(donor_uuid)s %(center_name)s %(project)s" % hit["_source"])
            for specimen in hit["_source"]["specimen"]:
                for sample in specimen["samples"]:
                    for analysis in sample["analysis"]:
                        print "MAYBE HIT??? "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_sequence_qc_report"])+" "+specimen["submitter_specimen_type"]
                        if analysis["analysis_type"] == "sequence_upload" and (hit["_source"]["flags"]["normal_sequence_qc_report"] == False and re.match("^Normal - ", specimen["submitter_specimen_type"])):
                        #and (hit["_source"]["flags"]["normal_sequence_qc_report"] == False and re.match("^Normal - ", specimen["submitter_specimen_type"])) or (hit["_source"]["flags"]["tumor_sequence_qc_report"] == False and re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", specimen["submitter_specimen_type"])):
                            print analysis
                            print "HIT!!!! "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_sequence_qc_report"])+" "+specimen["submitter_specimen_type"]
                            bamFile = ""
                            for file in analysis["workflow_outputs"]:
                                if (file["file_type"] == "fastq"):
                                    # this will need to be an array
                                    bamFile = file["file_path"]
                            print "will run report for %s" % bamFile
                            #if len(listOfJobs) < int(self.max_jobs):
                            #    listOfJobs.append(SequenceQCTaskUploader(ucsc_storage_client_path=self.ucsc_storage_client_path, ucsc_storage_host=self.ucsc_storage_host, filename=bamFile, uuid=self.fileToUUID(bamFile, analysis["bundle_uuid"]), bundle_uuid=analysis["bundle_uuid"], parent_uuid=sample["sample_uuid"], tmp_dir=self.tmp_dir, data_dir=self.data_dir))

        # these jobs are yielded to
        return listOfJobs

    def run(self):
        # now make a final report
        f = self.output().open('w')
        print >>f, "batch is complete"
        f.close()

    def output(self):
        # the final report
        ts = time.time()
        ts_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
        return luigi.LocalTarget('%s/SequenceQCTask-%s.txt' % (self.tmp_dir, ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"

if __name__ == '__main__':
    luigi.run()

import luigi
import json
import time
import re
import datetime
import subprocess
import base64
from urllib import urlopen
from uuid import uuid4
from elasticsearch import Elasticsearch
#for hack to get around non self signed certificates
import ssl

# TODO
# * I think we want to use S3 for our touch files (aka lock files) since that will be better than local files that could be lost/deleted

class ConsonanceTask(luigi.Task):
    redwood_host = luigi.Parameter("storage.ucsc-cgl.org")
    redwood_token = luigi.Parameter("must_be_defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/briandoconnor/dockstore-tool-running-dockstore-tool:1.0.6")
    target_tool = luigi.Parameter(default="quay.io/briandoconnor/fastqc:0.11.5")
    target_tool_url = luigi.Parameter(default="https://dockstore.org/containers/quay.io/briandoconnor/fastqc")
    workflow_type = luigi.Parameter(default="sequence_upload_qc_report")
    image_descriptor = luigi.Parameter(default="must be defined")
    filenames = luigi.ListParameter(default=["filename"])
    file_uuids = luigi.ListParameter(default=["uuid"])
    bundle_uuids = luigi.ListParameter(default=["bundle_uuid"])
    parent_uuids = luigi.ListParameter(default=["parent_uuid"])
    # tar_files
    tar_filenames = luigi.ListParameter(default=["filename"])
    tar_file_uuids = luigi.ListParameter(default=["uuid"])
    tar_bundle_uuids = luigi.ListParameter(default=["bundle_uuid"])
    tmp_dir = luigi.Parameter(default='/tmp')
    new_uuid = str(uuid4())

    def run(self):
        print "** EXECUTING IN CONSONANCE **"
        print "** MAKE TEMP DIR **"
        # create a unique temp dir
        cmd = '''mkdir -p %s/consonance-jobs/SequenceQCCoordinator/%s/''' % (self.tmp_dir, self.new_uuid)
        print cmd
        result = subprocess.call(cmd, shell=True)
        if result != 0:
            print "PROBLEMS MAKING DIR!!"
        print "** MAKE JSON FOR WORKER **"
        # create a json for FastQC which will be executed by the dockstore-tool-running-dockstore-tool and passed as base64encoded
        # will need to encode the JSON above in this: https://docs.python.org/2/library/base64.html
        # see http://luigi.readthedocs.io/en/stable/api/luigi.parameter.html?highlight=luigi.parameter
        # TODO: this is tied to the requirements of the tool being targeted
        json_str = '''{
"fastq_files": [
        '''
        i = 0
        while i<len(self.filenames):
            # append file information
            json_str += '''
{
    "class": "File",
    "path": "redwood://%s/%s/%s/%s"
}
            ''' % (self.redwood_host, self.bundle_uuids[i], self.file_uuids[i], self.filenames[i])
            if i < len(self.filenames) - 1:
                json_str += ","
            i += 1
        json_str += '''],
"tar_files": [
        '''
        i = 0
        while i<len(self.tar_filenames):
            # append file information
            json_str += '''
{
    "class": "File",
    "path": "redwood://%s/%s/%s/%s"
}
            ''' % (self.redwood_host, self.tar_bundle_uuids[i], self.tar_file_uuids[i], self.tar_filenames[i])
            if i < len(self.tar_filenames) - 1:
                json_str += ","
            i += 1
        json_str += '''],
"zipped_file" :
  {
    "class": "File",
    "path": "./tmp/fastqc_reports.tar.gz"
  }
}
        '''
        print "THE JSON: "+json_str
        # now make base64 encoded version
        base64_json_str = base64.urlsafe_b64encode(json_str)
        print "** MAKE JSON FOR DOCKSTORE TOOL WRAPPER **"
        # create a json for dockstoreRunningDockstoreTool, embed the FastQC JSON as a param
        p = self.output().open('w')
        print >>p, '''{
            "json_encoded": "%s",
            "docker_uri": "%s",
            "dockstore_url": "%s",
            "redwood_token": "%s",
            "redwood_host": "%s",
            "parent_uuids": "[%s]",
            "workflow_type": "%s"
        }''' % (base64_json_str, self.target_tool, self.target_tool_url, self.redwood_token, self.redwood_host, ','.join(map("'{0}'".format, self.parent_uuids)), self.workflow_type )
        p.close()
        # execute consonance run, parse the job UUID
        print "** SUBMITTING TO CONSONANCE **"
        print "consonance run  --flavour c4.8xlarge --image-descriptor %s --run-descriptor %s/consonance-jobs/SequenceQCCoordinator/%s/settings.json" % (self.image_descriptor, self.tmp_dir, self.new_uuid)

    def output(self):
        return luigi.LocalTarget('%s/consonance-jobs/SequenceQCCoordinator/%s/settings.json' % (self.tmp_dir, self.new_uuid))

class SequenceQCCoordinator(luigi.Task):

    es_index_host = luigi.Parameter(default='localhost')
    es_index_port = luigi.Parameter(default='9200')
    redwood_token = luigi.Parameter("must_be_defined")
    redwood_client_path = luigi.Parameter(default='../ucsc-storage-client')
    redwood_host = luigi.Parameter(default='storage.ucsc-cgl.org')
    image_descriptor = luigi.Parameter(default="must be defined")
    dockstore_tool_running_dockstore_tool = luigi.Parameter(default="quay.io/briandoconnor/dockstore-tool-running-dockstore-tool:1.0.6")
    tmp_dir = luigi.Parameter(default='/tmp')
    data_dir = luigi.Parameter(default='/tmp/data_dir')
    max_jobs = luigi.Parameter(default='1')
    bundle_uuid_filename_to_file_uuid = {}

    def requires(self):
        print "** COORDINATOR **"
        # now query the metadata service so I have the mapping of bundle_uuid & file names -> file_uuid
        print str("https://"+self.redwood_host+":8444/entities?page=0")
        #hack to get around none self signed certificates
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page=0"), context=ctx).read()
        metadata_struct = json.loads(json_str)
        print "** METADATA TOTAL PAGES: "+str(metadata_struct["totalPages"])
        for i in range(0, metadata_struct["totalPages"]):
            print "** CURRENT METADATA TOTAL PAGES: "+str(i)
            json_str = urlopen(str("https://"+self.redwood_host+":8444/entities?page="+str(i)), context=ctx).read()
            metadata_struct = json.loads(json_str)
            for file_hash in metadata_struct["content"]:
                self.bundle_uuid_filename_to_file_uuid[file_hash["gnosId"]+"_"+file_hash["fileName"]] = file_hash["id"]

        # now query elasticsearch
        es = Elasticsearch([{'host': self.es_index_host, 'port': self.es_index_port}])
        # see jqueryflag_alignment_qc
        # curl -XPOST http://localhost:9200/analysis_index/_search?pretty -d @jqueryflag_alignment_qc
        res = es.search(index="analysis_index", body={"query" : {"bool" : {"should" : [{"term" : { "flags.normal_sequence_qc_report" : "false"}},{"term" : {"flags.tumor_sequence_qc_report" : "false" }}],"minimum_should_match" : 1 }}}, size=5000)

        listOfJobs = []

        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print("%(donor_uuid)s %(center_name)s %(project)s" % hit["_source"])
            for specimen in hit["_source"]["specimen"]:
                for sample in specimen["samples"]:
                    for analysis in sample["analysis"]:
                        if analysis["analysis_type"] == "sequence_upload" and ((hit["_source"]["flags"]["normal_sequence_qc_report"] == False and re.match("^Normal - ", specimen["submitter_specimen_type"])) or (hit["_source"]["flags"]["tumor_sequence_qc_report"] == False and re.match("^Primary tumour - |^Recurrent tumour - |^Metastatic tumour -", specimen["submitter_specimen_type"]))):
                            print "HIT!!!! "+analysis["analysis_type"]+" "+str(hit["_source"]["flags"]["normal_sequence_qc_report"])+" "+specimen["submitter_specimen_type"]
                            files = []
                            tar_files = []
                            file_uuids = []
                            tar_file_uuids = []
                            bundle_uuids = []
                            tar_bundle_uuids = []
                            parent_uuids = {}
                            tar_parent_uuids = {}
                            for file in analysis["workflow_outputs"]:
                                if (file["file_type"] == "fastq" or file["file_type"] == "fastq.gz"):
                                    # this will need to be an array
                                    files.append(file["file_path"])
                                    file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                    bundle_uuids.append(analysis["bundle_uuid"])
                                    parent_uuids[sample["sample_uuid"]] = True
                                elif (file["file_type"] == "fastq.tar"):
                                    tar_files.append(file["file_path"])
                                    tar_file_uuids.append(self.fileToUUID(file["file_path"], analysis["bundle_uuid"]))
                                    tar_bundle_uuids.append(analysis["bundle_uuid"])
                                    parent_uuids[sample["sample_uuid"]] = True
                            print "  + will run report for %s" % files
                            if len(listOfJobs) < int(self.max_jobs):
                                listOfJobs.append(ConsonanceTask(redwood_host=self.redwood_host, redwood_token=self.redwood_token, dockstore_tool_running_dockstore_tool=self.dockstore_tool_running_dockstore_tool, filenames=files, file_uuids = file_uuids, bundle_uuids = bundle_uuids, parent_uuids = parent_uuids.keys(), tar_filenames= tar_files, tar_file_uuids = tar_file_uuids, tar_bundle_uuids = tar_bundle_uuids, tmp_dir=self.tmp_dir, image_descriptor=self.image_descriptor))
        # these jobs are yielded to
        return listOfJobs

    def run(self):
        # now make a final report
        f = self.output().open('w')
        # TODO: could print report on what was successful and what failed?  Also, provide enough details like donor ID etc
        print >>f, "batch is complete"
        f.close()

    def output(self):
        # the final report
        ts = time.time()
        ts_str = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
        return luigi.LocalTarget('%s/consonance-jobs/SequenceQCCoordinator/SequenceQCTask-%s.txt' % (self.tmp_dir, ts_str))

    def fileToUUID(self, input, bundle_uuid):
        return self.bundle_uuid_filename_to_file_uuid[bundle_uuid+"_"+input]
        #"afb54dff-41ad-50e5-9c66-8671c53a278b"

if __name__ == '__main__':
    luigi.run()

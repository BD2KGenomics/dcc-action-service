# dcc-action-service

## Introduction

A place for our business logic written in Luigi that triggers workflow launches.

## Install

### Ubuntu 14.04

You need to make sure you have system level dependencies installed in the appropriate way for your OS.  For Ubuntu 14.04 you do:

    sudo apt-get install python-dev libxml2-dev libxslt-dev lib32z1-dev

### Elasticsearch

Download and install [elasticsearch](https://www.elastic.co).  I found the debian package install to be easiest on Ubuntu.  Start it using the /etc/init.d/elasticsearch script. Or, if you're on a mac, you can download a tarball and just execute the ./bin/elasticsearch script.  I'm using version 5.0.0.

NOTE: Elasticsearch is not secure. Do not run it on a web server open to the outside world.

### Python

Use python 2.7.x.

See [here](https://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/) for information on setting
up a virtual environment for Python.

If you haven't already installed pip and virtualenv, depending on your system you may
(or may not) need to use `sudo` for these:

    sudo easy_install pip
    sudo pip install virtualenv

Now to setup:

    virtualenv env
    source env/bin/activate
    pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch semver luigi python-dateutil cwl-runner cwltool==1.0.20160316150250 schema-salad==1.7.20160316150109 avro==1.7.7 typing

Alternatively, you may want to use Conda, see [here](http://conda.pydata.org/docs/_downloads/conda-pip-virtualenv-translator.html)
 [here](http://conda.pydata.org/docs/test-drive.html), and [here](http://kylepurdon.com/blog/using-continuum-analytics-conda-as-a-replacement-for-virtualenv-pyenv-and-more.html)
 for more information.

    conda create -n schemas-project python=2.7.11
    source activate schemas-project
    pip install jsonschema jsonmerge openpyxl sets json-spec elasticsearch semver luigi python-dateutil cwl-runner cwltool==1.0.20160316150250 schema-salad==1.7.20160316150109 avro==1.7.7 typing

### Consonance Command Line

TODO: need to document the install of the Consonance command line.
, instead the user will provide a directory that contains json files.

## Manually Calling

```
# example run (local mode)
rm -rf /tmp/AlignmentQCTask* /tmp/afb54dff-41ad-50e5-9c66-8671c53a278b; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --local-scheduler --es-index-host localhost --es-index-port 9200

# example run (local mode) of next gen version
rm -rf /tmp/consonance-jobs/AlignmentQCTask*; PYTHONPATH='.' luigi --module AlignmentQCTaskV2 AlignmentQCCoordinatorV2 --local-scheduler --es-index-host localhost --es-index-port 9200

# run with central scheduler
mkdir -p /mnt/AlignmentQCTask
rm -rf /tmp/AlignmentQCTask* /tmp/9c09bca7-8ffa-54fe-a1c9-3f8a71df515b; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200 --ucsc-storage-client-path ../ucsc-storage2-client --ucsc-storage-host https://storage2.ucsc-cgl.org --tmp-dir `pwd`/luigi_state --data-dir /mnt/AlignmentQCTask

# another test with AlignmentQC
git hf update; git hf pull; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200 --ucsc-storage-client-path ../ucsc-storage2-client --ucsc-storage-host https://storage2.ucsc-cgl.org --tmp-dir `pwd`/luigi_state --data-dir /mnt/AlignmentQCTask --max-jobs 1

# now test sequence QC runner (can use --local-scheduler for local schedule)
cd luigi_task_executor
rm -rf /tmp/consonance-jobs; PYTHONPATH='' luigi --module SequenceQCTask SequenceQCCoordinator --es-index-host localhost --es-index-port 9200 --redwood-token `cat ../accessToken` --redwood-client-path ../ucsc-storage-client --redwood-host storage.ucsc-cgl.org --tmp-dir /tmp --data-dir /mnt/SequenceQCTask --max-jobs 1

# local scheduler
PYTHONPATH='.' luigi --module SequenceQCTask SequenceQCCoordinator --es-index-host 172.31.25.227 --es-index-port 9200 --redwood-token `cat ../accessToken` --redwood-client-path ../ucsc-storage-client --redwood-host storage.ucsc-cgl.org --local-scheduler --tmp-dir /tmp --data-dir /mnt/SequenceQCTask --max-jobs 1 --image-descriptor /home/ubuntu/luigi_consonance_rnaseq_testing/test_here/Dockstore.cwl
```

Monitor on the web GUI:
http://localhost:8082/static/visualiser/index.html#

## Demo

Goal: create sample single donor documents and perform queries on them.

1. Install the needed packages as described above.
1. Generate metadata for multiple donors using `generate_metadata.py`, see command above
1. Create single donor documents using `merge_gen_meta.py`, see command above
1. Load into ES index, see `curl -XPUT` command above
1. Run the queries using `esquery.py`, see command above
1. Optionally, deleted the index using the `curl -XDELETE` command above

The query script, `esquery.py`, produces output whose first line prints the number of documents searched upon.
The next few lines are center, program and project.
Following those lines, are the queries, which give information on:
* specifications of the query
* number of documents that fit the query
* number of documents that fit this query for a particular program
* project name

### services

Make sure both luigi and elasticsearch are running in screen sessions.

    # screen session 1
    ~/elasticsearch-2.3.5/bin/elasticsearch &
    # or if you use service (as you should)
    sudo service elasticsearch restart
    # screen session 2
    source env/bin/activate
    luigid

### simulate_upload.py

This script runs an unlimited number of BAM file uploads at random intervals.  The script will run until killed.

    cd luigi_task_executor
    python simulate_upload.py --bam-url https://s3.amazonaws.com/oconnor-test-bucket/sample-data/NA12878.chrom20.ILLUMINA.bwa.CEU.low_coverage.20121211.bam \
    --input-metadata-schema ../input_metadata.json --metadata-schema ../metadata_schema.json --output-dir output_metadata --receipt-file receipt.tsv \
    --storage-access-token `cat ../ucsc-storage2-client/accessToken` --metadata-server-url https://storage2.ucsc-cgl.org:8444 \
    --storage-server-url https://storage2.ucsc-cgl.org:5431  --ucsc-storage-client-path ../ucsc-storage2-client

Another script, this time it simulates the upload of fastq files:

    cd luigi_task_executor
    python simulate_upload_rnaseq_fastq.py --fastq-r1-path \
    https://s3.amazonaws.com/oconnor-test-bucket/sample-data/ERR030886_1.fastq.gz \
    --fastq-r2-path https://s3.amazonaws.com/oconnor-test-bucket/sample-data/ERR030886_2.fastq.gz \
    --input-metadata-schema ../input_metadata.json --metadata-schema ../metadata_schema.json \
    --output-dir output_metadata --receipt-file receipt.tsv \
    --storage-access-token `cat ../ucsc-storage2-client/accessToken` --metadata-server-url https://storage2.ucsc-cgl.org:8444 \
    --storage-server-url https://storage2.ucsc-cgl.org:5431  --ucsc-storage-client-path ../ucsc-storage2-client

### simulate_indexing.py

    cd luigi_task_executor
    python simulate_indexing.py --storage-access-token `cat ../ucsc-storage2-client/accessToken` --client-path ../ucsc-storage2-client --metadata-schema ../metadata_schema.json --server-host storage2.ucsc-cgl.org

### simulate_analysis.py

    cd luigi_task_executor
    python simulate_analysis.py --es-index-host localhost --es-index-port 9200 --ucsc-storage-client-path ../ucsc-storage2-client --ucsc-storage-host https://storage2.ucsc-cgl.org

    # temp
    git hf update; git hf pull; PYTHONPATH='' luigi --module AlignmentQCTask AlignmentQCCoordinator --es-index-host localhost --es-index-port 9200 --ucsc-storage-client-path ../ucsc-storage2-client --ucsc-storage-host https://storage2.ucsc-cgl.org --tmp-dir `pwd`/luigi_state --data-dir /mnt/AlignmentQCTask --max-jobs 1

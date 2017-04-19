#!/bin/bash

#exit script if we try to use an uninitialized variable.
set -o nounset
#exit the script if any statement returns a non-true return value
set -o errexit


#crontab does not use the PATH from etc/environment so we have to set our 
#own PATH so the consonance command and other tools can be found
#PATH=/home/ubuntu/bin:/home/ubuntu/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/lib/jvm/java-8-oracle/bin:/usr/lib/jvm/java-8-oracle/db/bin:/usr/lib/jvm/java-8-oracle/jre/bin

VIRTUAL_ENV_PATH=/home/ubuntu/luigi_decider_runs/luigienv/bin
LUIGI_RUNS_PATH=/home/ubuntu/luigi_decider_runs
DECIDER_SOURCE_PATH=${LUIGI_RUNS_PATH}
#DECIDER_SOURCE_PATH=/home/ubuntu/RNAseq3_0_x_testing

echo "Starting decider cron job" > ${LUIGI_RUNS_PATH}/cron_decider_log.txt

echo "getting date" >> ${LUIGI_RUNS_PATH}/cron_decider_log.txt
now=$(date +"%T")

#mkdir -p ${LUIGI_RUNS_PATH}

echo "cd ${LUIGI_RUNS_PATH}" >> ${LUIGI_RUNS_PATH}/cron_decider_log.txt
#Go into the appropriate folder
cd "${LUIGI_RUNS_PATH}"

echo "source ${VIRTUAL_ENV_PATH}/activate" >> ${LUIGI_RUNS_PATH}/cron_decider_log.txt
#for some reason set -o nounset thinks activate is an uninitialized variable so turn nounset off
set +o nounset
#Activate the virtualenv
source "${VIRTUAL_ENV_PATH}"/activate
set -o nounset

#echo "REDWOOD_ACCESS_TOKEN= contents of ${LUIGI_RUNS_PATH}/redwood_access_token.txt" >> ${LUIGI_RUNS_PATH}/cron_decider_log.txt
#get the storage system access token from the file that holds it
#REDWOOD_ACCESS_TOKEN=$(<"${LUIGI_RUNS_PATH}"/redwood_access_token.txt)


#start up the Luigi scheduler daemon in case it is not already running
#so we can monitor job status
#once we do this we don't use the --local-scheduler switch in the 
#Luigi command line
echo "Starting Luigi daemon in the background" >> ${LUIGI_RUNS_PATH}/cron_decider_log.txt
sudo luigid --background

echo "Running Luigi RNA-Seq decider" >> ${LUIGI_RUNS_PATH}/cron_decider_log.txt

# run the decider
#PYTHONPATH="${DECIDER_SOURCE_PATH}" luigi --module RNA-Seq RNASeqCoordinator --redwood-client-path /home/ubuntu/ucsc-storage-client/ --redwood-host storage.ucsc-cgl.org --redwood-token $REDWOOD_ACCESS_TOKEN --es-index-host 172.31.25.227 --image-descriptor ~/gitroot/BD2KGenomics/dcc-dockstore-tool-runner/Dockstore.cwl --tmp-dir /datastore --max-jobs 500 > cron_log_RNA-Seq_decider_stdout.txt 2> "${LUIGI_RUNS_PATH}"/cron_log_RNA-Seq_decider_stderr.txt


##PYTHONPATH=${DECIDER_SOURCE_PATH} luigi --module RNA-Seq RNASeqCoordinator --redwood-client-path /home/ubuntu/ucsc-storage-client/ --redwood-host storage.ucsc-cgl.org --redwood-token $REDWOOD_ACCESS_TOKEN --es-index-host 172.31.25.227 --image-descriptor ~/gitroot/BD2KGenomics/dcc-dockstore-tool-runner/Dockstore.cwl --local-scheduler --tmp-dir /datastore --max-jobs 50 > cron_log_RNA-Seq_decider_stdout.txt 2> ${LUIGI_RUNS_PATH}/cron_log_RNA-Seq_decider_stderr.txt
#--test-mode  > >(tee stdout.txt) 2> >(tee stderr.txt >&2)


#This can be used for testing: 
#consonance --version > ${LUIGI_RUNS_PATH}/consonancelogfile.txt
##echo "${now} DEBUG!! run of lugi decider!!! redwood token is ${REDWOOD_ACCESS_TOKEN}" > ${LUIGI_RUNS_PATH}/logfile.txt
echo "executing java -version test" >> ${LUIGI_RUNS_PATH}/logfile.txt
java -version >> ${LUIGI_RUNS_PATH}/logfile.txt 2>&1
echo "${now} DEBUG!! run of lugi decider!!!" >> ${LUIGI_RUNS_PATH}/logfile.txt


#for some reason set -o nounset thinks deactivate is an uninitialized variable so turn nounset off
set +o nounset
# deactivate virtualenv
deactivate
set -o nounset


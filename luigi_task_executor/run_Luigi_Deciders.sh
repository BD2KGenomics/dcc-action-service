#!/bin/bash

#exit script if we try to use an uninitialized variable.
set -o nounset
#exit the script if any statement returns a non-true return value
set -o errexit

#set up storage information variable in case it is not run from Docker compose
#which will poplulate the variables
#set variables to default values if they are not already set
#http://stackoverflow.com/questions/2013547/assigning-default-values-to-shell-variables-with-a-single-command-in-bash
: ${STORAGE_SERVER:=storage_server}
: ${STORAGE_ACCESS_TOKEN:=storage_token}
: ${ELASTIC_SEARCH_SERVER:=elastic_search_server}
: ${ELASTIC_SEARCH_PORT:=elastic_search_port}
: ${TOUCH_FILE_DIRECTORY:=touch_file_directory}
: ${AWS_REGION:=aws_region}

#crontab does not use the PATH from etc/environment so we have to set our 
#own PATH so the consonance command and other tools can be found
#PATH=/home/ubuntu/bin:/home/ubuntu/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/lib/jvm/java-8-oracle/bin:/usr/lib/jvm/java-8-oracle/db/bin:/usr/lib/jvm/java-8-oracle/jre/bin

PIPELINE_RUNS_PATH=/home/ubuntu/pipeline_deciders
VIRTUAL_ENV_PATH=${PIPELINE_RUNS_PATH}/pipeline_env
DECIDER_SOURCE_PATH=${PIPELINE_RUNS_PATH}
LOG_FILE_PATH=/home/ubuntu/logs
mkdir -p ${LOG_FILE_PATH}
sudo chown -R ubuntu:ubuntu ${LOG_FILE_PATH}
touch ${LOG_FILE_PATH}/logfile.txt

echo "Starting decider cron job" > ${LOG_FILE_PATH}/cron_decider_log.txt

echo "getting date" >> ${LOG_FILE_PATH}/cron_decider_log.txt
now=$(date +"%T")

#mkdir -p ${PIPELINE_RUNS_PATH}

echo "cd ${PIPELINE_RUNS_PATH}" >> ${LOG_FILE_PATH}/cron_decider_log.txt
#Go into the appropriate folder
cd "${PIPELINE_RUNS_PATH}"

#don't need virtual env right now; but save in case needed later
#echo "virtualenv ${VIRTUAL_ENV_PATH}" >> ${LOG_FILE_PATH}/cron_decider_log.txt
#virtualenv ${VIRTUAL_ENV_PATH}
#echo "source ${VIRTUAL_ENV_PATH}/bin/activate" >> ${LOG_FILE_PATH}/cron_decider_log.txt
##for some reason set -o nounset thinks activate is an uninitialized variable so turn nounset off
#set +o nounset
##Activate the virtualenv
#source "${VIRTUAL_ENV_PATH}"/bin/activate
#set -o nounset


#start up the Luigi scheduler daemon in case it is not already running
#so we can monitor job status
#once we do this we don't use the --local-scheduler switch in the 
#Luigi command line
echo "Starting Luigi daemon in the background" >> ${LOG_FILE_PATH}/cron_decider_log.txt
sudo luigid --background

echo "Running Luigi deciders" >> ${LOG_FILE_PATH}/cron_decider_log.txt

# run the decider
#use the '--test-mode' switch to skip running Consonance
#This will be the new run commmand:
PYTHONPATH="${DECIDER_SOURCE_PATH}" luigi --module RNA-Seq RNASeqCoordinator --dockstore-tool-running-dockstore-tool "quay.io/ucsc_cgl/dockstore-tool-runner:1.0.22" --workflow-version "3.3.4-1.12.3" --touch-file-bucket ${TOUCH_FILE_DIRECTORY} --redwood-host ${STORAGE_SERVER} --redwood-token ${STORAGE_ACCESS_TOKEN} --es-index-host ${ELASTIC_SEARCH_SERVER} --es-index-port ${ELASTIC_SEARCH_PORT} --vm-instance-type "c4.8xlarge" --vm-region ${AWS_REGION} --program Treehouse --tmp-dir /datastore --test-mode > "${LOG_FILE_PATH}"/cron_log_RNA-Seq_decider_stdout.txt 2> "${LOG_FILE_PATH}"/cron_log_RNA-Seq_decider_stderr.txt

#Run the ProTECT decider:
PYTHONPATH="${DECIDER_SOURCE_PATH}" luigi --module Protect ProtectCoordinator --dockstore-tool-running-dockstore-tool "quay.io/ucsc_cgl/dockstore-tool-runner:1.0.22" --workflow-version "2.5.5-1.12.3" --touch-file-bucket ${TOUCH_FILE_DIRECTORY} --redwood-host ${STORAGE_SERVER} --redwood-token ${STORAGE_ACCESS_TOKEN} --es-index-host ${ELASTIC_SEARCH_SERVER} --es-index-port ${ELASTIC_SEARCH_PORT} --vm-instance-type "c4.8xlarge" --vm-region ${AWS_REGION} --project TARGET --tmp-dir /datastore --test-mode  > "${LOG_FILE_PATH}"/cron_log_Protect_decider_stdout.txt 2> "${LOG_FILE_PATH}"/cron_log_Protect_decider_stderr.txt

PYTHONPATH="${DECIDER_SOURCE_PATH}" luigi --module CNV CNVCoordinator --dockstore-tool-running-dockstore-tool "quay.io/ucsc_cgl/dockstore-tool-runner:1.0.22" --workflow-version "v1.0.0" --touch-file-bucket ${TOUCH_FILE_DIRECTORY} --redwood-host ${STORAGE_SERVER} --redwood-token ${STORAGE_ACCESS_TOKEN} --es-index-host ${ELASTIC_SEARCH_SERVER} --es-index-port ${ELASTIC_SEARCH_PORT} --vm-instance-type "c4.8xlarge" --vm-region ${AWS_REGION} --project WCDT --tmp-dir /datastore --test-mode  > "${LOG_FILE_PATH}"/cron_log_CNV_decider_stdout.txt 2> "${LOG_FILE_PATH}"/cron_log_CNV_decider_stderr.txt

PYTHONPATH="${DECIDER_SOURCE_PATH}" luigi --module Fusion FusionCoordinator --dockstore-tool-running-dockstore-tool "quay.io/ucsc_cgl/dockstore-tool-runner:1.0.22" --workflow-version "0.2.1" --touch-file-bucket ${TOUCH_FILE_DIRECTORY} --redwood-host ${STORAGE_SERVER} --redwood-token ${STORAGE_ACCESS_TOKEN} --es-index-host ${ELASTIC_SEARCH_SERVER} --es-index-port ${ELASTIC_SEARCH_PORT} --vm-instance-type "r4.8xlarge" --vm-region ${AWS_REGION} --program Fusion --tmp-dir /datastore --test-mode > "${LOG_FILE_PATH}"/cron_log_Fusion_decider_stdout.txt 2> "${LOG_FILE_PATH}"/cron_log_Fusion_decider_stderr.txt

#execute commands to run addtional deciders in the extra folder
#if [ -f ${DECIDER_SOURCE_PATH}/extra/run_addtional_deciders.sh ]
#then
#    source ${DECIDER_SOURCE_PATH}/extra/run_addtional_deciders.sh
#fi

#These are log file messages used for testing: 

#echo -e "\n\n"
#echo "${now} DEBUG!! run of luigi decider!!!" >> ${LOG_FILE_PATH}/logfile.txt
#echo "executing consonance --version test" >> ${LOG_FILE_PATH}/logfile.txt
#consonance --version >> ${LOG_FILE_PATH}/logfile.txt

#echo "redwood server is ${STORAGE_SERVER}" >> ${LOG_FILE_PATH}/logfile.txt
#echo "redwood token is ${STORAGE_ACCESS_TOKEN}" >> ${LOG_FILE_PATH}/logfile.txt

#echo "elastic search server is ${ELASTIC_SEARCH_SERVER}" >> ${LOG_FILE_PATH}/logfile.txt
#echo "elastic search port is ${ELASTIC_SEARCH_PORT}" >> ${LOG_FILE_PATH}/logfile.txt

#echo "touch file directory is ${TOUCH_FILE_DIRECTORY}" >> ${LOG_FILE_PATH}/logfile.txt

#echo "executing java -version test" >> ${LOG_FILE_PATH}/logfile.txt
#java -version >> ${LOG_FILE_PATH}/logfile.txt 2>&1

#echo "executing aws test" >> ${LOG_FILE_PATH}/logfile.txt
#aws   >> ${LOG_FILE_PATH}/logfile.txt 2>&1



#don't need virtual env right now; but save in case needed later
##for some reason set -o nounset thinks deactivate is an uninitialized variable so turn nounset off
#set +o nounset
## deactivate virtualenv
#deactivate
#set -o nounset


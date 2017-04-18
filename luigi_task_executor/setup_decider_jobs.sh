#!/bin/bash

#exit script if we try to use an uninitialized variable.
set -o nounset
#exit the script if any statement returns a non-true return value
set -o errexit

#start the Luigi daemon in the background
#so the action service that monitors tasks
#can get information on tasks
#assume we are in the right directory as setup in the Dockerfile
sudo luigid --background

#run cron in forground so container doesn't stop
#if cron is run without '-f' it will launch cron
#in the background the command will end and the 
#container will exit
#sudo cron -f 

#run cron in background and run tail so container
#doesn't exit and we can see log 
sudo cron && sudo tail -f /tmp/decider_log


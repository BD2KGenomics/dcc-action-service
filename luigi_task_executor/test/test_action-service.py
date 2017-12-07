#!/usr/bin/env python
from __future__ import print_function, division
import sys
import subprocess

def test_runLuigiDeciders():
    command = ['/home/ubuntu/pipeline_deciders_and_scripts/run_Luigi_Deciders.sh' ]
    try:
        subprocess.check_call(command)
    except subprocess.CalledProcessError as e:
        print(e.message, file=sys.stderr)
        exit(e.returncode)
    return True

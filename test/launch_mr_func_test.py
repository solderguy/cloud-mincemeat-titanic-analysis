#!/usr/bin/env python

"""
CREDENTIALS
  Module: launch_mr_func_test.py
  Author: John Soper
  Date: Apr 27, 2014
  Rev: 1

SUMMARY
  Starts two processes so mincemeat does not have to be run in a 
  separate window
"""

import os
import subprocess
from time import sleep

subprocess.Popen("python local_mr_func_test.py", shell=True)
sleep(5)
os.system("python mincemeat.py -p changeme localhost")
print "Done processing"


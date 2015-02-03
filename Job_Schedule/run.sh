#!/bin/bash
if [ $# -eq 0 ]; then
  echo -e "\nError: Runner Required { local | hadoop | emr }. Aborting...\n\n"
  exit 1
elif [ $# -gt 1 ]; then
  echo -e "\nError: The program takes one arguments maximum. Aborting...\n\n"
  exit 1
fi

python MRAll.py --conf-path mrjob.conf -r $1 --file jobInfo.json --file application.py --file jobSched.py --file parameters.py  input.txt 


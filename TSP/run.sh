#!/bin/bash
if [ $# -eq 0 ]; then
  echo -e "\nError: Runner Required { local | hadoop | emr }. Aborting...\n\n"
  exit 1
elif [ $# -gt 1 ]; then
  echo -e "\nError: The program takes one arguments maximum. Aborting...\n\n"
  exit 1
fi


python MRAll.py --conf-path mrjob.conf -r $1 --file a280.json --file parameters.py --file application.py  input.txt 


rm -f *.pyc


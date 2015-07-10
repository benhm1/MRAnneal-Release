#  
#  
#  Copyright [2015] [Benjamin Marks and Riley Collins]
#  
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#  
#       http://www.apache.org/licenses/LICENSE-2.0
#  
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#  
#  
'''Parse Intermediate Result: The Python implementation of Phase 3.

Goal: Given the output of Phase 2, determine the number of additional
rounds needed for each solution, keeping track of the max for all
solutions.

Print out the number needed to STDOUT; the runner script will capture
this and use it when starting Phase 4.

Ben Marks, Riley Collins, Kevin Webb
Swarthmore College
MRAnneal
'''

from sys import argv
import json
import sys

if len( argv ) < 2:
    raise Exception("Error: A config file to parse was expected!")

with open( argv[1], 'r' ) as fd:

    sols = fd.readlines()
    numRounds = 0
    numSoFar = 1000
    for each in sols:
        k, v = each.strip().split('\t')
        v = json.loads( v )
        numRounds = max( v['numNeeded'][-1], numRounds )
        numSoFar = min( len( v['numNeeded'] ), numSoFar )
    sys.stderr.write( "Looks like we need {0} rounds total.\n".format( numRounds ))
    sys.stderr.write( "Looks like we've done {0} rounds so far.\n".format( numSoFar ))

    numToDo = numRounds - numSoFar
    sys.stderr.write( "So, we should to {0} more.\n".format( numToDo ))

    print numToDo
        



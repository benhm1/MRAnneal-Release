"""
Copyright [2015] [Benjamin Marks and Riley Collins]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


"""
def initSeed() :
    # Read in the number of machines and list of job lengths
    import json
    ret = {}
    with open('jobInfo.json') as jobs :
        ret = json.load( jobs )
    return ret

def generateSeed( jobInfo ) :
    from random import randrange
    # A solution is a list of 2-tuples: ( machine #, job length )
    ret = []
    for jobLen in jobInfo['lengths'] :
        ret.append( ( randrange( jobInfo['numMachines'] ), jobLen ) )
    return ret 

def initAnneals() :
    import json
    ret = {}
    with open('jobInfo.json') as jobs :
        ret = json.load( jobs )
    return ret

def anneal( args , solution ) :
    # Choose a random element and move it to the other partition
    from random import randrange
    switchNo = randrange( len( solution ) ) 
    old = solution[ switchNo ][0]  # Get the machine currently assigned
    new = randrange( args['numMachines'] )
    solution[switchNo][0] = new
    return ( switchNo, old )

def score( args, solution ) :
    from collections import Counter
    # Sum up all of the edge weights crossing the cut
    ctr = Counter()
    for each in solution :
        ctr[ each[0] ] += each[1]
    maxMachine, maxLoad = ctr.most_common(1)[0]
    return args['totalLoad'] - maxLoad 

def undoAnneal( args, solution, ret ) :
    switchNo, old = ret
    solution[ switchNo ][0] = old # Restore original machine

def processSolution( solution ) :
    import json
    with open('jobInfo.json') as jobs :
        info = json.load( jobs )
    return ( -1.0 * (solution[0] - info['totalLoad'] ), solution[1] )

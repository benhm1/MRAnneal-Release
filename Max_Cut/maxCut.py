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
    # Read in the list of edges to compute the vertices
    import json
    allVertices = set()
    with open('edges.json') as edges :
        edgeList = json.load( edges )
        for src, dest, weight in edgeList :
            allVertices.add( src )
            allVertices.add( dest )
    return list(allVertices)

def generateSeed( allVertices ) :
    from random import shuffle, randrange
    numInPartition = randrange( len(allVertices) + 1 )
    shuffle( allVertices )
    # A solution is a dictionary mapping vertices to their partition
    ret = {}
    for i in xrange( len(allVertices) ) :
        if i < numInPartition :
            ret[ allVertices[i] ] = 0
        else :
            ret[ allVertices[i] ] = 1
    return ret 

def initAnneals() :
    # Read in our edges and edge weights, storing them in a dict
    import json
    from collections import defaultdict
    args = {}
    args['edges'] = defaultdict( dict ) 
    args['vertices'] = set()
    args['negSum' ] = 0 # How much negative weight could we have?
    with open('edges.json') as edges :
        edgeList = json.load( edges )
        for src, dest, weight in edgeList :
            args['edges'][ str(src) ][ str(dest) ] = weight
            args['vertices'].add( str(src) )
            args['vertices'].add( str(dest))
            if weight < 0 :
                args['negSum'] += weight

    import sys
    sys.stderr.write("Negative Sum Is: " + str(args['negSum']) + '\n')

    args['vertices'] = list( args['vertices'] )
    return args

def anneal( args, solution ) :
    # Choose a random element and move it to the other partition
    from random import choice
    switched = choice( args['vertices'] )
    if solution[switched] == 1 :
        solution[ switched ] = 0
    else : # solution[switched] = 0
        solution[ switched ] = 1
    return switched

def score( args, solution ) :
    # Sum up all of the edge weights crossing the cut
    score = -1.0 * args['negSum']  # Ensure score is always positive
    for src in args['edges'] :
        for dest in args['edges'][src] :
            if solution[src] != solution[dest] :
                score += args['edges'][src][dest]
    return score

def undoAnneal( args, solution, switched ) :
    if solution[switched] == 1 :
        solution[ switched ] = 0
    else : # solution[switched] = 0
        solution[ switched ] = 1
    return

def processSolution( solution ) :
    return solution # ( solution[0] + args['negSum'], solution[1] ) 






    

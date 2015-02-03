"""
application.py - Defines all of the necessary functions for a 
parallelized simulated anneal of the search space. 
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

def initSeedSolutions() :
    """
    This function should do any initialization needed
    to make generating individual solutions fast. It
    will get called before any solutions are generated.
    
    The value returned will be passed into generateSolution,
    so this is a good place to read in files or initialize state
    that you may not need to run for each individual solution.
    """
    
    import json
    import random

    toRet = {}
    adjMatrix = json.loads( open('a280.json').read() )
    numCities = len( adjMatrix )
    toRet['numCities'] = numCities
    toRet['starter']   = [ str(x) for x in range( numCities ) ]
    toRet['adjMatrix'] = adjMatrix
    toRet['starterSet'] = set( toRet['starter'] )
    
    return toRet
    
def generateSolution( args ) :
    """
    This function returns a possible solution to the problem,
    in whatever format is desired. It should be in the same 
    format expected by all subsequent functions that will 
    anneal, score, and undo the annealing as needed.
    """
    
    import random

    toVisit = args['starter']

    copy = args['starter'][:]
    random.shuffle( copy )
    return copy

def initAnnealer() :
    """
    This function should do any initialization needed 
    to make doing a single anneal fast. It will be called
    before any annealing takes place. 
    
    The returned value will be passed as the first argument
    to the anneal function defined below.
    """
    
    import json

    toRet = {}
    adjMatrix = json.loads( open('a280.json').read() )
    numCities = len( adjMatrix )
    toRet['numCities'] = numCities
    toRet['matrix']   = adjMatrix

    return toRet

def anneal(args, solution) :
    """
    Perform a single, slight modification to an existing
    solution. 
    
    Parameters: 
    args - the returned arguments from initAnnealer
    solution - the solution to be changed slightly
    
    Returns: 
    Must return enough to undo the last anneal. The
    returned value will be passed to the undoAnneal
    function if the modification is rejected. 
    """
    
    import random

    first = random.randrange( args['numCities'] )
    second = random.randrange( args['numCities'] )
    
    solution[ first ], solution[ second ] \
        = solution[second], solution[first]

    return [first, second]

def scoreSolution( args, solution ) :
    """
    Quantitatively rank a single solution.
    
    Parameters:
    args - the returned arguments from initAnnealer
    solution - the solution to be measured
    
    Higher scores are better.
    
    Returns: A quantitative measure of goodness of the
    proposed solution. The quantitative measure must be
    greater than zero. 

    """
    cost = 0.0
    for i in range(1, len(solution)) :
        cost += args['matrix'][ solution[i-1] ][ solution[i] ]

    return 1000000 - cost # Higher scores are better


def undoAnneal( args, solution, res ) :
    """
    Rollback the changes from the most recent modification.
    This may be called if the modification yields a result
    with a lower score.
    
    Parameters:
    solution - the solution to rollback the change on
    res      - the return value from anneal, which should
    provide enough information to undo the last action.
    args     - the returned arguments from initAnnealer
    
    Returns: Nothing.
    """
    
    first = res[0]
    second = res[1]

    solution[ first ], solution[ second ] \
        = solution[second], solution[first]


def processResult( solution ) :
    """
    Do any processing on the given solution before yielding
    it for the last time. For instance, maybe create a human
    readable string of the solutin and yield that instead.
    
    Parameters:
    Solution is a 2 tuple of (score, solution)
    """
    
    return (1000000 - solution[0], solution[1] )

    



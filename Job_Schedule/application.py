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
"""
application.py - Defines all of the necessary functions for a 
parallelized simulated anneal of the search space. 

Ben Marks, Riley Collins, Kevin Webb
Swarthmore College
MRAnneal
"""

import jobSched

def initSeedSolutions() :
    """
    This function should do any initialization needed
    to make generating individual solutions fast. It
    will get called before any solutions are generated.
    
    The value returned will be passed into generateSolution,
    so this is a good place to read in files or initialize state
    that you may not need to run for each individual solution.
    """
    return jobSched.initSeed()

    
def generateSolution( args ) :
    """
    This function returns a possible solution to the problem,
    in whatever format is desired. It should be in the same 
    format expected by all subsequent functions that will 
    anneal, score, and undo the annealing as needed.
    """
    return jobSched.generateSeed( args )


def initAnnealer() :
    """
    This function should do any initialization needed 
    to make doing a single anneal fast. It will be called
    before any annealing takes place. 
    
    The returned value will be passed as the first argument
    to the anneal function defined below.
    """
    return jobSched.initAnneals()

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
    
    return jobSched.anneal( args, solution )

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
    return jobSched.score( args, solution )


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
    return jobSched.undoAnneal( args, solution, res )


def processResult( solution ) :
    """
    Do any processing on the given solution before yielding
    it for the last time. For instance, maybe create a human
    readable string of the solutin and yield that instead.
    
    Parameters:
    Solution is a 2 tuple of (score, solution)
    """
    
    return jobSched.processSolution( solution )

    





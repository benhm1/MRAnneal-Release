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
'''
MRAll.py - Defines the MRAnneal class and associated
functions for navigating the MapReduce framework.

You should not need to modify this file. Everything you
need to change should be done in application.py and 
parameters.py. If you modify this file, you are going against 
the principle of abstraction by implementing hacky workarounds! 

Ben Marks, Riley Collins, Kevin Webb
Swarthmore College
MRAnneal
'''

# Some useful imports
import sys
import copy
import json
import numpy
import heapq
import socket  # For UID of reducer
from random import random, choice, randrange
from math import sqrt, log, exp, ceil

# The MRJob library
import mrjob
from mrjob.job import MRJob

# User defined input files
import parameters
import application


## Pull in specs from parameters.py

# How many machines should run in parallel.
# Note: numReducers is the name for historical
# reasons; this is the number of reducers and mappers.
numReducers = parameters.numMachines

# Target percentage parameter.
percentageToMaximum = parameters.targetPercentage

# How many results should be returned at the end? 
numFinalResults = parameters.numFinalResults

# How many times should we anneal each solution in a given round?
numAnnealCalls = parameters.numAnnealCalls  

# Bounds on rounds
minRounds = max( 3, parameters.minRounds )  # Need at least 3 for a good fit
maxRounds = parameters.maxRounds

# Bounds on seed solution generation
minSeedSolutions = parameters.minSeedSolutions
maxSeedSolutions = parameters.maxSeedSolutions

class MRAnnealInstance(MRJob):

    def initSeedSolutions(self) :
        """
        This function should do any initialization needed
        to make generating individual solutions fast. It
        will get called before any solutions are generated.
        
        The value returned will be passed into generateSolution,
        so this is a good place to read in files or initialize state
        that you may not need to run for each individual solution.
        """
        import application
        return application.initSeedSolutions()

    def generateSolution(self, args) :
        """
        This function returns a possible solution to the problem,
        in whatever format is desired. It should be in the same 
        format expected by all subsequent functions that will 
        anneal, score, and undo the annealing as needed.
        """

        import application
        base = application.generateSolution( args )

        return base

    def initAnnealer(self) :
        """
        This function should do any initialization needed 
        to make doing a single anneal fast. It will be called
        before any annealing takes place. 
        
        The returned value will be passed as the first argument
        to the anneal function defined below.
        """
        import application
        return application.initAnnealer()

    def anneal(self, args, solution ) :
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
        import application
        return application.anneal( args, solution )
        

    def scoreSolution(self, args, solution) :
        """
        Quantitatively rank a single solution.
        
        Parameters:
        args - the returned arguments from initAnnealer
        solution - the solution to be measured
        
        Returns: A comparable measure of goodness of the
        proposed solution.
        """
        import application
        return application.scoreSolution( args, solution )

        
    def undoAnneal(self, args, solution, res ) :
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
        import application
        return application.undoAnneal( args, solution, res )
       
    def processResult( self, solution ) :
        """
        Parameters: solution is a list of length 2, where the first 
        item is the score and the second item is the solution.

        Whatever is returend will be yielded as the value.
        """
        import application
        return application.processResult( solution )


    ###################################################
    ##            Library Code Begins Here           ##
    ###################################################


    # Specify a non-default partitioner to use for this job
    PARTITIONER = 'org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner'

    # We need values to be sorted to ensure that reducers can determine
    # how many solutions are coming.
    SORT_VALUES=True

    def jobconf(self):
        """
        Configure any additional Hadoop parameters.
        """

        orig_jobconf = super(MRAnnealInstance, self).jobconf()
        custom_jobconf = {
            
            # Anything with the same first field goes to same reducer
            
            # Mapper outputs keys of the form 1,2,3,4 
            'map.output.key.field.separator'      : ',',     
        
            
            # Use the first field as the key, starting 
            # from the first character, and performing 
            # numerical sort on the result. 
            #'mapreduce.partition.keypartitioner.options' : '-k1,1n' ,
            'mapreduce.text.key.partitioner.options' : '-k1,1n',
            'mapred.reduce.tasks' : numReducers,
            'mapred.map.tasks' : numReducers,
            
            # Don't worry about tasks that take a long time
            # Since we read everything in at once (to avoid 
            # repeated calls to init_X) and then do lots of 
            # computation, sometimes Hadoop thinks we've failed
            # even though we haven't. 
            'mapreduce.task.timeout'              : 0
        
            
        }
        
        return mrjob.conf.combine_dicts(orig_jobconf, custom_jobconf)





    def configure_options(self):
        """
        Allow command line arguments: -s to specify the stage
        ( stage 1 = phase 1/2; stage 2 = phase 4 ) and number
        of rounds (in the case of stage 2)
        """
        super(MRAnnealInstance, self).configure_options()
        self.add_passthrough_option(
            '-s', '--stage', type='str',
            default="N/A", help='Stage: {1|2}.'
        )
        self.add_passthrough_option(
            '-d', '--rounds', type='int',
            default=10, help='How many annealing rounds? (for Phase 4).'
        )

    def steps(self):
        '''
	Returns a list of MRJob steps to be executed.
	This list is generated on the fly depending on
	the contents of parameters.py
         '''

        if self.options.stage not in ['1', '2']:
            raise Exception("Error: Invalid stage {0}!".format(
                self.options.stage ) )

        if self.options.stage == '1':

            numRounds = minRounds
    
            toRet = []
            toRet.append( self.mr( mapper  = self.m_getSeedSolutions,
                                   reducer = self.r_getSeedSolutions ) )
            
            for i in xrange( numRounds ) :
            
                toRet.append( self.mr( mapper =self.m_TagAndDistribute,
                                       reducer=self.r_annealAndYield ) )

            return toRet

        else:
            numRounds = self.options.rounds

            toRet = []

            toRet.append( self.mr( mapper = self.m_LoadJson,
                                   reducer = self.h_passThrough) )
            
            for i in xrange( numRounds ) :
            
                toRet.append( self.mr( mapper =self.m_TagAndDistribute,
                                       reducer=self.r_annealAndYield ) )

        
            toRet.append(      
                self.mr(mapper=self.m_coalesce,
                        reducer=self.r_finalFilter) )
        
        return toRet

      
    def m_LoadJson( self, _, line ):
        '''
        Helper for Phase 4 to load in the intermediate 
        JSON results file containing the yielded solutions
        from Phase 2. 
        '''

        ign, val = line.strip().split('\t')
        
        val = json.loads( val )
        toYield = self.h_DoTagAndDistribute( val )
        for each in toYield:
            if each[1][0] in ['D', 'F']:
                sys.stderr.write("Yielding:\n")
                yield each[0], each[1][1]
            



    def m_getSeedSolutions( self, _, line ) :
        """
        Divide up the work of generating seed solutions
        evenly between all of the reducers, using regression
        to estimate the number of seeds to generate.
        """

        seedParams = self.initSeedSolutions()
        scoreParams = self.initAnnealer()
        

        scoreRange = []
        minScore = None
        maxScore = None
        # The range of scores is approximately logarithmic
        
        for i in range(1, minSeedSolutions + 1 ):
            soln = self.generateSolution( seedParams )
            # Add some metadata about the solution
            solnWrapped = { 'base' : soln,
                            'scores' : [],
                            'numNeeded' : [],
                            'solnID' : '{hostname}-{numSolns}'.format \
                            (hostname = socket.gethostname(), numSolns = i )
                        }
            
            score = self.scoreSolution( scoreParams, solnWrapped['base'] )
            
            solnWrapped['scores'].append( score )
            if minScore is None or score < minScore:
                minScore = score
            if maxScore is None or score > maxScore:
                maxScore = score
            scoreRange.append( maxScore - minScore )

            yield str(i % numReducers) + ",1", ( 'SOLN', solnWrapped )

        while len(scoreRange) > 0 and scoreRange[0] == 0.0:
            scoreRange = scoreRange[1:]
        numSolnsNeeded = self.CalculateTaperOff( scoreRange, maxSeedSolutions )

        for i in range( numSolnsNeeded ):
            yield str(i % numReducers) + ",1", ( "SEED", "" )


    def r_getSeedSolutions( self, k, v ) :
        """
        Generate our initial set of solutions.
        
        MapReduce Inputs:
        k: Ignored
        v: The length indicates the number of solutions
           we are in charge of generating.
        """

        seedParams = self.initSeedSolutions()
        scoreParams = self.initAnnealer()


        counter = 0

        for each in v :
            if each[0] == "SEED":

                soln = self.generateSolution( seedParams )
                # Add some metadata about the solution
                solnWrapped = { 'base' : soln,
                                'scores' : [],
                                'numNeeded' : [],
                                'solnID' : '{hostname}-R-{numSolns}'.format( 
                                    hostname= socket.gethostname(), 
                                    numSolns = counter )
                            }
                counter += 1
                score = self.scoreSolution( scoreParams, solnWrapped['base'] )
                
                solnWrapped['scores'].append( score )
                
                yield "ignored, ignored", solnWrapped

            elif each[0] == "SOLN":
                yield "ignored, ignored", each[1]

            else:
                raise( "Invalid solution keyword: {0}".format( each[0] ) )

    def CalculateTaperOff( self, scores, optVal ):
        '''
        Calculate the cutoff value to achieve target percentage of the 
        estimated growth over the course of optVal {rounds|seeds}, based
        on existing score data.
        '''

        if len( scores ) < 2:
            return optVal

        standardizedScores = [ 1.0 * x / scores[0] for x in scores ]
        model = numpy.polyfit( [ log(x) for x in range(1, len(scores) + 1)], 
                               standardizedScores , 1 ) 

        # What would the optimal value be, if we did optVal rounds?
        opt = model[0] * log( optVal ) + model[1]

        diff = opt - standardizedScores[0]

        target = standardizedScores[0] + diff * percentageToMaximum / 100.0 
        # How many rounds would get us <target>% of that difference?

        sys.stderr.write("Target: {0}, model {1}\n".format( target, model ) )

        roundsNeeded = minRounds
        if model[0] > 0:
            # solutions are improving somewhat

            roundsNeeded = exp( 
                ( target - model[1] ) / model[0] 
            )
        
        sys.stderr.write( "Rounds Needed for {0} = {1}\n".format( 
            scores, roundsNeeded ) )

        return int( ceil( min( roundsNeeded, optVal ) ) )

    def m_TagAndDistribute( self, k, v ):
        '''
        Tags solutions and distributes them among reducers.
        '''
        sys.stderr.write("m_TagAndDistribute\n")
        toYield = self.h_DoTagAndDistribute( v )
        sys.stderr.write("m_TagAndDistribute To Yield {0}\n".format( toYield ))
        for each in toYield:
            sys.stderr.write("Tag and Distribute Yielding: \n")
            yield each[0], each[1]

    def h_DoTagAndDistribute( self, v ):
        '''
        Tags solutions based on the estimated number of rounds 
        needed. 
        '''
        # Reducers always yield wrapped solutions
        
        # Calculate the TTL based on the scores
        ttl = 0
        if 'FINALIZED' not in v:
            
            totalRounds = self.CalculateTaperOff( v['scores'], maxRounds )
            roundsSoFar = len( v['scores'] )
            
            ttl = totalRounds - roundsSoFar

            v['numNeeded'].append( totalRounds )
            

        counter = GetNextCount()

        if ttl > 0:
            v['ttl'] = ttl
            # 1 solution to anneal
            a = str(counter % numReducers - 1) + ',1', ('A', 1 )   
            # Barrier 
            c = str(counter % numReducers - 1) + ',1', ('C', "Ignored" )  
            # Solution to anneal
            d = str(counter % numReducers - 1) + ',1', ('D', v )   
            return [a, c, d]

        else:
            v['ttl'] = 0
            e = str( numReducers - 1 ) + ',1', ('E', "Ignored") # Barrier
            f = str( numReducers - 1 ) + ',1', ('F', v)  # Finalized solution
            return [e, f]

    def m_coalesce( self, k, v ) :
        '''
	Funnel everything to a single reducer for
	processing / choosing the best.
	'''
        yield "ignored,ignored", v

    def h_passThrough( self, k, v ) :
        '''
	Nothing to see here ... really!
	'''
        for each in v:
            yield k, each


    def _AnnealOneSolution( self, solnObject, args ):
        '''
        Anneal one solution, returning the best 2 observed incarnations
        of that solution.
        '''

        bestThisSolution = []
        bestThisMaxLen = 2  # We keep the best two incarnations
        
        priorScore = -1

        score = self.scoreSolution( args, solnObject )
        pushToHeap( bestThisSolution, \
                    bestThisMaxLen, \
                    score, solnObject )

        
        for i in xrange( numAnnealCalls ) :

            
            res   = self.anneal( args, solnObject )
            score = self.scoreSolution( args, solnObject )
            
            if score < priorScore :
                r = random()
                if r > (0.25 * score / priorScore ) :
                    self.undoAnneal( args, solnObject, res )
                else :
                    priorScore = score
                    pushToHeap( bestThisSolution, \
                                bestThisMaxLen, \
                                score, solnObject )
                    
            else :
                priorScore = score
                pushToHeap( bestThisSolution, \
                            bestThisMaxLen, \
                            score, solnObject )

        return bestThisSolution

    def r_annealAndYield( self, k, solutions ) :
        '''
	Anneals the input solutions and passes on a
	subset of them as defined by parameters.py.

	'''
       
        args = self.initAnnealer()

        bestOverallSolutions = []

        shouldAnneal = True

        seenSoFar = []

        # First, we'll get information about the number of
        # solutions we can expect
        numSolsComing = 0
        for each in solutions:
            
            typeOf, value = each

            seenSoFar.append( typeOf )

            if typeOf == 'A':  
                numSolsComing += value
            elif typeOf == 'B':
                shouldAnneal = False
            elif typeOf == 'C':
                break
            elif typeOf == 'E':
                shouldAnneal = False
                break
            else:
                raise Exception(
                    "ERROR: Invalid solution type! {0} {1} {2}".format(
                        typeOf, seenSoFar, [ x[0] for x in solutions ] ) )

        bestOverallMaxLen = numSolsComing 
        sys.stderr.write("Number of incoming solutions: {0}\n".format( 
            numSolsComing ))
        

        if shouldAnneal:
            for solution in solutions :
                typeOf, value = solution
                
                seenSoFar.append( typeOf )
                
                if typeOf == 'C':
                    # A barrier between number of solutions and 
                    # actual solutions - ignore it
                    continue
                elif typeOf == 'E':
                    # No more solutions to anneal are coming
                    break
                elif typeOf != 'D':
                    raise Exception(
                        "ERROR: Invalid solution type! {0} {1} {2}".format(
                            typeOf, seenSoFar, [ x[0] for x in solutions ] ) )
                


                bestThisSolution = self._AnnealOneSolution( 
                    value['base'], args )

                for bestScore, bestSolution in bestThisSolution :
                    
                    # Create a new solution object
                    solnNew = { 'base' : bestSolution,
                                'scores' : value['scores'] + [ bestScore ],
                                'numNeeded' : value['numNeeded'] ,
                                'solnID' : value['solnID'] }
                
                    pushToHeap( bestOverallSolutions, \
                                bestOverallMaxLen, \
                                bestScore, solnNew )

        
            for score, sol in bestOverallSolutions :
                yield "ignored", sol


        bestOfFinalized = []
        for solution in solutions:
            typeOf, value = solution

            seenSoFar.append( typeOf )

            if typeOf == 'E':
                continue
            elif typeOf != 'F':
                raise Exception(
                    "ERROR: Invalid solution type! {0} {1} {2}".format(
                        typeOf, seenSoFar, [ x[0] for x in solutions ] ) )

                
            pushToHeap( bestOfFinalized,
                        numFinalResults,
                        value['scores'][-1],
                        value )

        for score, sol in bestOfFinalized:
            sol['FINALIZED'] = True
            yield "ignored", sol
            
            



    def r_finalFilter( self, k, solutions ) :
        '''
	Returns the best k solutions of all the input
	solutions, distributed evenly over the set of 
	reducers

	'''
        best = heapq.nlargest( numFinalResults , solutions )

        scoreArgs = self.initAnnealer()

        counter = 0
        for each in best :
            
            score = self.scoreSolution( scoreArgs, each['base'] )
            
            yield "Result " + str(counter) + ':', \
                {'result' : self.processResult( [ score, each['base'] ] ),
                 'scores' : each['scores'],
                 'solnID' : each['solnID'],
                 'numNeeded' : each['numNeeded'] }
            counter += 1

        
        
def GetNextCount():
    '''
    Helper function for counting upwards.
    '''
    toRet = GetNextCount._count
    GetNextCount._count += 1

    return toRet

GetNextCount._count = 0


def pushToHeap( heap, limit, score, solution ) :
    """
    Function to efficiently keep track of some number
    of best scoring solutions so far.

    Parameters:
    heap - a list storing the solutions so far
    limit - maximum number of solutions to store
    score - score of the particular solution we are 
            considering saving.
    solution- the actual solution we may save
    """

    newCopy = copy.deepcopy( solution )

    if len(heap) == limit :
        heapq.heapreplace( heap, (score,newCopy) )
    else :
        heapq.heappush( heap, (score,newCopy) )

    return



if __name__ == '__main__':
    MRAnnealInstance.run()



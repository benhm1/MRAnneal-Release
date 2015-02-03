'''
MRAll.py - Defines the MRAnneal class and associated
functions for navigating the MapReduce framework.

You should not need to modify this file. Everything you
need to change should be done in application.py and 
parameters.py. If you modify this file, you are going against 
the principle of abstraction by implementing hacky workarounds! 

Ben Marks, Riley Collins
CS91 Final Project
MRAnneal


'''
from itertools import combinations, islice
from math import sqrt
from random import randrange


from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env

import socket  # For UID of reducer

import itertools 
import operator
import sys

import heapq
import copy
import Queue
import sys
import copy

from random import random, choice, randrange

import parameters
import application


# How many reducers should run in parallel?
# Each reducer will evaluate some set of the 
# solutions.
numReducers = parameters.numReducers


# How many total candidate solutions should be
# yielded by a single reducer?
# Example: Yield 100 exam schedules
numSolnsYield = parameters.numSolnsYield

# What's the maximum number of solutions that
# are yielded should be permutations of 
# one solution.
# Example: Yield at most 10 possible permutations 
#          of the same exam schedule course blocks.
#          This ensures that the overall set of yielded
#          solutions will contain some diversity.
numPerSubsol = parameters.numPerSubsol

# How many rounds of annealing should run?
numRounds = parameters.numRounds

# How many starter solutions should be generated?
numSeedSolns = parameters.numSeedSolns

# How many times should we anneal each solution?
numAnnealCalls = parameters.numAnnealCalls

# How many solutions should be passed on to the next round of
# annealing?
topK = parameters.topK

# How many results should be returned at the end? 
numFinalResults = parameters.numFinalResults




class MRScheduler(MRJob):

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
        return application.generateSolution( args )

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

    SORT_VALUES=True

    # Configuration parameters for the partitioner
    JOBCONF = {

        # Anything with the same first field goes to same reducer

        # Mapper outputs keys of the form 1,2,3,4 
        'map.output.key.field.separator'      : ',',     


        # Use the first field as the key, starting 
        # from the first character, and performing 
        # numerical sort on the result. 
        #'mapreduce.partition.keypartitioner.options' : '-k1,1n' ,
        'mapreduce.text.key.partitioner.options' : '-k1,1n',

        'mapred.reduce.tasks' : numReducers,
        'mapred.map.tasks' : 1,

        # Don't worry about tasks that take a long time
        # Since we read everything in at once (to avoid 
        # repeated calls to init_X) and then do lots of 
        # computation, sometimes Hadoop thinks we've failed
        # even though we haven't. 
        'mapreduce.task.timeout'              : 0


    }

    def configure_options(self):
        
        super(MRScheduler, self).configure_options()
      

    def steps(self):
        '''
	Returns a list of MRJob steps to be executed.
	This list is generated on the fly depending on
	the contents of parameters.py
         '''
        toRet = []
        toRet.append( self.mr( mapper =self.m_getSeedSolutions,
                               reducer=self.r_getSeedSolutions ) )

        for i in xrange( numRounds - 1 ) :
            
            toRet.append( self.mr( mapper =self.m_passThrough, 
                                   reducer=self.r_annealAndYield ) )
            toRet.append(      
                self.mr(mapper=self.m_coalesce,
                        reducer=self.r_topK) )
        

        toRet.append( self.mr( mapper =self.m_passThrough, 
                               reducer=self.r_annealAndYield ) )
        
        toRet.append(      
            self.mr(mapper=self.m_coalesce,
                    reducer=self.r_finalFilter) )
       
        
        return toRet

      
        
    def m_getSeedSolutions( self, _, line ) :
        """
        Divide up the work of generating seed solutions
        evenly between all of the reducers
        """
        self.set_status("Dividing Up Seed Solution Todos\n")
        counter = 0
        
        for i in xrange( numSeedSolns ) :
            print "MAPPER: ", str(i % numReducers) + ',1', 1 
            yield str(i % numReducers) + ',1', 1 
            

    def r_getSeedSolutions( self, k, v ) :
        """
        Generate our initial set of solutions.
        
        MapReduce Inputs:
        k: Ignored
        v: The length indicates the number of solutions
           we are in charge of generating.
        """
        self.set_status("Generating Seed Solutions\n")

        args = self.initSeedSolutions()
        
        counter = 0

        for each in v :
            counter += 1
            toYield = [(counter % numReducers), \
                self.generateSolution( args )]
            yield toYield[0], toYield[1]
       

    def m_coalesce( self, k, v ) :
        '''
	Funnel everything to a single reducer for
	processing / choosing the best.
	'''
        yield "ignored,ignored", v

    def m_passThrough( self, k, v ) :
        '''
	Nothing to see here ... really!
	'''
        yield k, v

    def r_annealAndYield( self, k, solutions ) :
        '''
	Anneals the input solutions and passes on a
	subset of them as defined by parameters.py.

	'''
        self.set_status("Annealing Input Solutions And Yielding.\n")
       
        args = self.initAnnealer()

        bestOverallSolutions = []
        bestOverallMaxLen = numSolnsYield

        for solution in solutions :
            self.set_status("\tAnnealing Another Solution ... \n")
            bestThisSolution = []
            bestThisMaxLen = numPerSubsol

            priorScore = -1
            for i in xrange( numAnnealCalls ) :
                res = self.anneal( args, solution )
                score = self.scoreSolution( args, solution )

                #sys.stderr.write("ANNEAL: " +  str(score)  ) 

                if score < priorScore :
                    r = random()
                    if r > (0.25 * score / priorScore ) :
                        #sys.stderr.write("  REJECTED\n")
                        self.undoAnneal( args, solution, res )
                    else :
                        #sys.stderr.write("  ACCEPTED EVEN THOUGH LOWER " + str(r) + '\n')
                        priorScore = score
                        pushToHeap( bestThisSolution, \
                                    bestThisMaxLen, \
                                    score, solution )
                        
                else :
                    #sys.stderr.write("  ACCEPTED\n")
                    priorScore = score
                    pushToHeap( bestThisSolution, \
                                     bestThisMaxLen, \
                                     score, solution )
                
            self.set_status("Done Annealing. Choosing Best Found Solutions.\n")
            for bestScore, bestSolution in bestThisSolution :
                #sys.stderr.write("BEST THIS SOL: " + str( bestScore ) + '\n' )

                pushToHeap( bestOverallSolutions, \
                            bestOverallMaxLen, \
                            bestScore, bestSolution )

        for score, sol in bestOverallSolutions :
            yield "ignored", (score, sol)

            


    def r_topK( self, k, solutions ) :
        '''
	Returns the best k solutions of all the input
	solutions, distributed evenly over the set of 
	reducers

	'''
        self.set_status("Choosing the Top K Solutions\n")
        best = heapq.nlargest( topK , solutions )
        counter = 0
        for score, sol in best :
            yield str(counter % numReducers) + ',1', sol
            counter += 1


    def r_finalFilter( self, k, solutions ) :
        '''
	Returns the best k solutions of all the input
	solutions, distributed evenly over the set of 
	reducers

	'''
        self.set_status("Reducer: Final Filter\n")
        best = heapq.nlargest( numFinalResults , solutions )
        
        counter = 0
        for each in best :
            yield "Result " + str(counter) + ':', self.processResult(each)
            counter += 1

        
        


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
    MRScheduler.run()

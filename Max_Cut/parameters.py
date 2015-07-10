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
parameters.py - Defines the parameters for a MRAnneal Instance.

Ben Marks, Riley Collins, Kevin Webb
Swarthmore College
MRAnneal
'''

# How many machines should we use?
# Higher Values => Generally, faster runtime, especially
#                  for computationally intensive annealing
#                  or seed solution generation.
# Lower Values => Less costly, more cluster resources available
#                 for other tasks.
#
# Recommended Value: 50
numMachines = 50

# How many results should be returned at the end? 
# 
# Recommended Value: 5
numFinalResults = 5

# Target percentage parameter: integer in range [0, 100]
# Higher Values => Longer runtime, higher scoring results
# Lower Values => Shorter runtime, lower scoring results
#
# Recommended Value: 65
targetPercentage = 45

###############################
##    Advanced Parameters    ##
###############################

# What are the bounds on the number of rounds that should run?
minRounds = 10   # Rounds in Phase 2, must be at least 3 for proper
                 # curve estimation in Phase 3.
maxRounds = 100  # Upper bound on rounds

# What are the bounds on the number of seed solutions to generate?
minSeedSolutions = 1000   
maxSeedSolutions = 10000

# How many times should we anneal each non-finalized solution per round
numAnnealCalls = 150








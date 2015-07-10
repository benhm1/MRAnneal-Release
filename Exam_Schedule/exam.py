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
from random import shuffle
import json
from copy import deepcopy
from collections import defaultdict


def get_data():
    """
    Read in the appropriate data files store in JSON format.

    The return value gets passed to the generate solutions function.
    """
    from operator import itemgetter

    toRet = {}

    with open('classSizes2.json') as thing:
        classsize=json.load(thing)
        toRet['classSizes'] = classsize

    with open('idToCRNs2.json') as thing:
        idtoCRN=json.load(thing)
        toRet['idToCRNs'] = idtoCRN

    with open('crnToActual.json') as thing:
        crnmap=json.load(thing)
        toRet['crns'] = list(set(crnmap.values() ) ) 

    with open('roster.json') as thing:
        roster=json.load(thing)
        toRet['roster'] = roster

    with open('roomCapacity.json') as thing:
        capacity=json.load(thing)
        sortedRooms = []
        for room, cap in capacity.items() :
            sortedRooms.append( [room, cap, 0 ] )
        sortedRooms.sort( key=itemgetter(1) )
        toRet['rooms'] = sortedRooms

    with open('conflicts.json') as thing:
        conflicts = json.load(thing)
        # JSON cannot encode sets
        for crn in conflicts :
            conflicts[crn] = set( conflicts[crn] )

        toRet['conflicts'] = conflicts

    with open('acceptableByCRN.json') as thing:
        acceptableByCRN = json.load(thing)
        ddAcceptable = defaultdict( lambda: None )
        for crn in acceptableByCRN :
            ddAcceptable[crn] = set( acceptableByCRN[ crn ] )
        toRet['acceptableByCRN'] = ddAcceptable

        
    with open('lengthParams.json') as lengths :
        toRet['lengthParams'] = json.load( lengths )


    return toRet

    

def generate_clumping( args ): #crns, compatible, size, rooms ):
  """
  crns= list of all crn
  conflicting= dictionary maps CRNs --> set of classes can't be scheduled with crn
  size= dictionary in the form of size[crn] --> int with number
  rooms= a list of tuples, (capacity, used)--- sorted by capacity (small --> large)

  first, generate which clumps are potential based on room size
  take clumps, assign each clump to a 
  """

  # Pull the arguments out of the dictioanry
  crns = args['crns'] 
  conflicting = args['conflicts']
  size = args['classSizes']
  rooms = args['rooms']  
  acceptable_rooms = args['acceptableByCRN']

  
  block_dict={} # Each block is a series of exams and room assisgnments
  room_dict={}  # For each exam slot, maps rooms to taken or not.
  exam_len=0    # How many exam slots do we have so far?
  crn_to_room = defaultdict( dict )  # Exam Slot => CRN => Room assigned
  room_to_crn = defaultdict( dict )  # Exam Slot => Room string => assigned CRN


  # Randomly order the CRNs to yield different results
  shuffle(crns)

  i=0


  for crn in crns:
      assigned = False

      # DECISION: Maybe randomize the order here?
      for i in range(0, exam_len):
          # Can the class fit in this block?

          room = cango(block_dict[i],conflicting[crn], room_dict[i], size[crn], acceptable_rooms[crn] )
          if room is not None :
              assigned = True
              block_dict[i].add(crn)
              crn_to_room[i][ crn ]  = room
              room_to_crn[i][ room ] = crn
              break
            
      if not assigned:
          # Class didn't fit in any blocks ... make a new block
          block_dict[exam_len]=set()
          room_dict[exam_len]=deepcopy(rooms)

          ## TODO :: Modify assign_room to return the room assigned
          room = assign_room(size[crn], room_dict[exam_len], acceptable_rooms[crn] )
          crn_to_room[exam_len][ crn ]  = room
          room_to_crn[exam_len][ room ] = crn
          block_dict[exam_len].add(crn)
          exam_len+=1
          

  ## Check padding and pad if needed
  if args['lengthParams']['padding'] == 'True' :
      while args['lengthParams']['minLen'] > exam_len :
          block_dict[exam_len]=set()
          room_dict[exam_len]=deepcopy(rooms)
          exam_len += 1


  courseToCID={}
  CIDToSlot= {}
  slotToCourse={}
  z=0
  for each in block_dict:
    CIDToSlot[str(z)]=str(each)
    for course in block_dict[each]:
      courseToCID[course]=str(z)
    z+=1


  for each in block_dict :
      block_dict[ each ] = list( block_dict[ each ] )

  schedule={}  


  #Calculate our schedule 
  schedule['CourseToCID']=courseToCID
  schedule['CIDToSlot']= CIDToSlot
  schedule['SlotToCourses']=block_dict
  schedule['NumSlots']= exam_len


  # Return our schedule
  return schedule

def cango(block, conflictingCRNs, rooms, size, acceptable):
    """
    Helper function to check if a given CRN can be 
    placed in a particular block.

    Arguments: 
    => block - proposed block to schedule this CRN in
    => conflictingCRNs - a set of CRNs that conflict with this one
    => rooms - a sorted list of room capacities and whether they are taken yet
    => size - how large is this class?
    => acceptable - a set of room strings that are rooms this exam can 
       be scheduled in, or None if any room is acceptable.

    Returns: Assigned room as a string, or None if no room assigned
    """


    for item in block:
        if item in conflictingCRNs:
            # Conflicts with some CRN that is already here
            return None
  
    return assign_room( size, rooms, acceptable ) 

def assign_room(size, rooms, acceptable):
    """
    Assign a class of size people to a free room,
    and mark that room as taken.

    Arguments:
    => size - how large is this class?
    => rooms - sorted list of rooms by capacity; each item is
       a 3-tuple ( Room ID string, Room Capacity, Taken )
    => acceptable - a set of room strings that are rooms this exam can 
       be scheduled in, or None if any room is acceptable.
    Returns: String ID of assigned room, or None if no room assigned.
    """
    for room in rooms:
        if size <= room[1] and room[2] == 0:

            # If we have a specified list of rooms that 
            # this course could go in, then check if 
            # this room is one of them
            if acceptable is not None :
                if room[0] not in acceptable :
                    continue  # Can't use this room ...

            # Mark the room as taken and return the ID string
            room[2]=1
            return room[0]

    # No acceptable rooms found
    return None


def initAnnealer() :
    '''
	Read in any data that we will need to do processing in
	out annealer. This includes student schedules and faculty
	conflicts.

	Returns an arguments dictionary that is subsequently passed
	to the scorer.

    '''
    toRet = {}

    # Read in the student enrolled CRNs
    toRet['IDToCourses'] = json.loads( open('idToCRNs2.json').read() )

    # Read in the bad slots
    fConflicts = json.loads( open('facultyConflicts.json').read() )
    for k in fConflicts :
        fConflicts[k] = set( fConflicts[k] )
    toRet['FacultyConflicts'] = fConflicts
        
    with open('lengthParams.json') as lengths :
        toRet['lengthParams'] = json.load( lengths )
    

    return toRet


def goodness(args, solution) : #students, c_cid, cid_s, slot_c, bad_slot):
    """
    Calculate the goodness of a given schedule.

    Arguments
    
    From args:
    => students: Given an ID, tell me the CRNs that ID is enrolled in.
    => bad_slot: Given a CRN, tell me the slots it should not be scheduled in.

    From solution:
    => c_cid : Given a class, map it to it's clump ID
    => cid_s : Given a clump ID, map it to its slot
    => slot_c: Given a slot, tell me the courses scheduled in that slot
 
    """

    
    c_cid = solution['CourseToCID'  ]
    cid_s = solution['CIDToSlot'    ]
    slot_c= solution['SlotToCourses']
    examLen = solution['NumSlots']

    students = args['IDToCourses']
    bad_slot = args['FacultyConflicts']
    minLen = args['lengthParams']['minLen']
    maxLen = args['lengthParams']['maxLen']

    # Generate a mapping of student IDs to scheduled exam slots
    stud_info=calc_students(students, c_cid, cid_s, slot_c)

    #import sys
    #for each in stud_info :
    #    sys.stderr.write(str(type(each)) + ' ' + str(each) + ' ' + str(type( stud_info[each])) + ' ' +  str(stud_info[each]) )
        


    # What is the average distance between exams for students?
    # Larger is better.
    # What is the average variance between exams for students? 
    # Smaller is better.
    
    mean, variance = calc_stats(stud_info)

    # How many students have 3 exams in a row?
    # Smaller is better.
    three=calc_three(stud_info)

    # How many exams are scheduled in slots that faculty don't want.
    # Smaller is better.
    faculty=calc_faculty(c_cid, cid_s, bad_slot)

    # Calculate our score, and return it.
    good=(mean-(variance/5))/60.0
    bad=(20*faculty+three)/15.0

    score=good-bad

    # Length correction - penalize long schedules
    numSlots =  len( slot_c )
    score *= 30.0 / numSlots

    # Penalize schedules that are not in the desired constraints
    if examLen > maxLen or examLen < minLen :
        score *= 0.7


    return score




def calc_faculty(c_cid, cid_s, bad):
    """
    c_cid : Given a class, map it to it's clump ID
    cid_s : Given a clump ID, map it to its slot
    bad: Given a CRN, tell me a set of slots that the CRN shouldn't go in. 
    
    Returns: Number of courses scheduled into slots that the professor
    has said will not work for this course.
    """

    total_bad=0
    for clas in c_cid:
        if clas in bad:
            if cid_s[c_cid[clas]] in bad[ clas ] :
                total_bad+=1

    return total_bad




def calc_students(students, c_cid, cid_s, slot_c):
    """
    Generate a mapping of student ID's to slots where they have exams.
    This list should be sorted.

    studnets: Maps student IDs to CRNs they are enrolled in.
    c_cid : Civen a class CRN, map it to its clump ID
    cid_s : Given a clump ID, tell me what slot it's scheduled in
    slot_c: Given a slot, tell me the courses scheduled in that slot
    """

    s_schedule={}
    for s in students:
        
        s_schedule[s]=[]
        for each in students[s]:
            if each in c_cid and c_cid[each] in cid_s :
                s_schedule[s].append( int(cid_s[c_cid[each]]))  ## List/Set dependency
            else :
                print "No exam scheduled for CRN : ", each
        s_schedule[s].sort()
    return s_schedule

def getDistancesBetween( ls ) :
    """
    Given a sorted list of items, return the distances between
    each item.

    Ex: Given [1, 2, 6, 11], returns[ 1, 4, 5 ]
    """
    toRet = []
    intLs = [ int(x) for x in ls ]
    for i in range(1, len(intLs)) :
        toRet.append( intLs[i] - intLs[i-1] )
    return toRet



def calc_stats(students):
    """
    Calculate the mean distance between exams for students. 
    """

    total_mean = 0
    total_var = 0

    for student in students:
        dist=getDistancesBetween( students[ student ] )
        data = stats( dist )
        
        total_mean += data[0]
        total_var += data[1]

    return total_mean, total_var







def calc_three(stud_info):
    """
    How many students have 3 exams in a row?
    """
    from collections import Counter

    three_count=0

    for s in stud_info:

        dist = getDistancesBetween( stud_info[ s ] )
        dist.sort()

        count = Counter()
        for each in dist :
            count[ each ] += 1
        
        if count[1] >= 3 :
            three_count+=1

    return three_count

        
       

def stats( ls ) :
    """
    Calculate the mean and variance in a list.

    Returns mean, variance
    """
    if len(ls) <= 0 :
        return [0.0, 0.0]

    from math import sqrt
    avg = 1.0 * sum(ls) / len(ls)
    
    if len(ls) == 1 :
        return [avg, 0.0]

    s = sqrt(1.0*sum( [ (x - avg)**2 for x in ls ] ) / (len(ls)-1))
    return [avg, s]















def anneal( args, schedule ) :
    '''
	Anneal a single exam schedule by
	swapping two blocks of classes. 
	
	Returns enough information to undo the
	anneal - namely, the two blocks that were
	swapped.
    '''


    from random import randrange

    import sys

    CRNToCID   = schedule['CourseToCID']
    CIDToSlot  = schedule['CIDToSlot']
    SlotToCRNs = schedule['SlotToCourses']
    numSlots   = schedule['NumSlots']

    # We'll swap these two exam clump slots
    CID1 = str(randrange(numSlots))
    CID2 = str(randrange(numSlots))
    slot1 = CIDToSlot[ CID1 ]
    slot2 = CIDToSlot[ CID2 ]

    # Swap the mappings in ClumpID to Slot
    CIDToSlot[ CID1 ] = slot2
    CIDToSlot[ CID2 ] = slot1

    # Swap the courses in SlotToCRNs
    courseListTmp = SlotToCRNs[ slot1 ]
    SlotToCRNs[ slot1 ] = SlotToCRNs[ slot2 ]
    SlotToCRNs[ slot2 ] = courseListTmp

    return( CID1, CID2 )






def undoAnneal( args, schedule, retVal ) :
    '''
	Looks like we didn't like the latest
	swap! So, let's undo it by swapping the
	courses back into their original positons.

    '''
    CRNToCID   = schedule['CourseToCID']
    CIDToSlot  = schedule['CIDToSlot']
    SlotToCRNs = schedule['SlotToCourses']

    numSlots = schedule['NumSlots']

    # We'll swap these two exam clump slots
    CID1 = retVal[0]
    CID2 = retVal[1]
    slot1 = CIDToSlot[ CID1 ]
    slot2 = CIDToSlot[ CID2 ]

    # Swap the mappings in ClumpID to Slot
    CIDToSlot[ CID1 ] = slot2
    CIDToSlot[ CID2 ] = slot1

    # Swap the courses in SlotToCRNs
    courseListTmp = SlotToCRNs[ slot1 ]
    SlotToCRNs[ slot1 ] = SlotToCRNs[ slot2 ]
    SlotToCRNs[ slot2 ] = courseListTmp


def CRNToSlot( sched, crn ) :
    '''
	Helper function to determine the scheduled slot
	of any CRN given the schedule. For efficiency, 
	there is a level of indirection that makes this
	a two step, somewhat easy to mess up, process.
    '''
    clump = sched['CourseToCID'][crn]
    slot  = sched['CIDToSlot'  ][clump]
    assert( crn in sched['SlotToCourses'][slot] )
    return slot



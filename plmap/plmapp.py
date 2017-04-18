#!/usr/bin/python

# This is a generic module for parallelization ( using Processes )
# 
# 
# 
# How to use This ? 
#   Create a function that can do what you want to achieve with 1 parameter or 1 set of parameters
#   pass this function and the set of arguments to the plmapp function
#   plmapp will take care of parallelization for you 
#   It will return lists of errors and outputs for the corresponding inputs.
#   You can even mention the timeout period for individual input sets
# Eg : 

#   Let say you want to add these pairs of numbers. 

#   inputs = [
#   [3, 5],
#   [8, 9] ,
#   [11, 12],
#   [15, 16],
#   ]

#   step 1) define the function for a single set of inputs
#   def add ( a, b ) :
#       return a + b 

#   step 2) Call the plmapp function 
#   error, output = plmapp(add, inputs , [], 4, default_output=None)
#   print output 

# The output will look like this : 
# [8, 17, 23, 31]

from multiprocessing import Queue
from multiprocessing import Process
from pprint import pprint
import datetime
import sys
import os
import time
import signal
import types



PROGRESS_BAR_POLL_TIME = 1
PROGRESS_BAR_LENGTH = 20


global_lock = multiprocessing.Lock()

def increment_after_lock():
    global_lock.acquire()


def generate_loading_string(completed_tasks, total_tasks):
    """  <percentage completed>% [< -- based on percentage completion>] Completed/Total
    """
    try:
        fraction_completed = ( completed_tasks / total_tasks) 
    except:
        fraction_completed = 1 # To avoid division by Zero

    percentage_complete = fraction_completed * 100

    dashes = int(PROGRESS_BAR_LENGTH * fraction_completed)
    blanks = PROGRESS_BAR_LENGTH - dashes

    bar = "[" + "-" * dashes + ">" + " " * blanks + "]"
    fraction_display = "%s/%s" %(completed_tasks, total_tasks)

    loading_string = "%s%% %s %s" %(percentage_complete, bar, fraction_display)

    return loading_string


def display_progress_bar(progress_details, total_tasks):
    """ 50%[---------->          ]5/10 
    """
    completed_tasks = len(progress_details)

    while completed_tasks != total_tasks:

        time.sleep(PROGRESS_BAR_POLL_TIME)

        completed_tasks = len(progress_details)

        print generate_loading_string(completed_tasks, total_tasks)
        sys.stdout.write("\033[F")

    # Display 100% completion
    completed_tasks = len(progress_details)
    print generate_loading_string(completed_tasks, total_tasks)
    sys.exit(0)



class FunctionTimeoutException(Exception):
    """ This exception will be raised when a function takes too long to complete
    """
    pass



def receive_signal(signum, stack):
    details = "Function timeout"
    raise FunctionTimeoutException(details)



def queue_exec(input_queue, p_id, error_list, output_list, progress_details, individual_timeout=20.0, default_output=None):
    """ This function will be executed by all the child Processes spawned by the Parent process
    """
    if p_id != -1: # Not a progress bar process
        signal.signal(signal.SIGALRM, receive_signal)

        while True: 
            try:
                loc, function, args, kwargs = input_queue.get_nowait()
            except Exception, details:
                break
            
            signal.alarm(int(individual_timeout))

            try:
                output = function(*args, **kwargs)
                
                error_list[loc] = (0, None)
                output_list[loc] = output
                print "Got the output %s for %s %s" %(output, loc, output_list)
            except FunctionTimeoutException, details:
                error_list[loc] = (1, details)
                output_list[loc] = default_output
            except Exception, details:
                error_list[loc] = (2, details)
                output_list[loc] = default_output
            finally:
                progress_details.append(1)
            signal.alarm(0)
        sys.exit(0)

    else:
        display_progress_bar(progress_details)





def plmapp(func, args=[], kwargs=[], processes=10, default_output=None, progress_bar=False, individual_timeout=20):
    """ 
    """

    bigger_array = args if len(args) > len(kwargs) else kwargs 
    big_len = len(bigger_array)
    ag = lambda array, index, default : default if index >= len(array) else array[index]

    input_queue = Queue()
    output_errors = [ default_output for _ in xrange(big_len)]
    output_values = [ default_output for _ in xrange(big_len)]
    progress_details = []

    # Load the input queue with tasks
    for i in xrange(big_len):
        task = (i, func, ag(args, i, ()), ag(kwargs, i, {})) 
        input_queue.put(task)

    # Create the processes
    processes = []
    end = min(processes, big_len)
    start = -1 if progress_bar else 0


    for i in xrange(start, end):
        p = Process(target=queue_exec, args=(input_queue, i, output_errors, output_values, progress_details, individual_timeout, default_output))
        processes.append(p)
        p.start()

    # Wait for the processes to complete    
    for process in processes:
        process.join() 

    return output_errors, output_values







if __name__ == "__main__":
    def do_it(a):
        time.sleep(a)
        return a

    # args
    args = [[i] for i in range(10)]
    kwargs = []
    func = do_it
    threads = 8

    # user input ends here
    print "Start time : ", datetime.datetime.now()
    error, output = plmapp(func, args, kwargs, threads, default_output=None)
    print "End time : " , datetime.datetime.now()
    print "Errors : "
    pprint(error)
    print "Output : "
    print(output)






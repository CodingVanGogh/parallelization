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
from __future__ import division
from multiprocessing import Queue
from multiprocessing import Process
from multiprocessing import Value
from multiprocessing import Lock
from pprint import pprint
import datetime
import sys
import os
import time
import signal
import types
from progress_bar import generate_loading_string



PROGRESS_BAR_POLL_TIME = 1





def display_progress_bar(progress_details):
    """ 50%[---------->          ]5/10 
    """
    completed_tasks = progress_details['completed_tasks'].value
    total_tasks = progress_details['total_tasks']

    while completed_tasks != total_tasks:
        time.sleep(PROGRESS_BAR_POLL_TIME)
        completed_tasks = progress_details['completed_tasks'].value
        print generate_loading_string(completed_tasks, total_tasks)
        sys.stdout.write("\033[F")

    # Display 100% completion
    completed_tasks = progress_details['completed_tasks'].value
    print generate_loading_string(completed_tasks, total_tasks)
    sys.exit(0)



class FunctionTimeoutException(Exception):
    """ This exception will be raised when a function takes too long to complete
    """
    pass



def receive_signal(signum, stack):
    details = "Function timeout"
    raise FunctionTimeoutException(details)



def queue_exec(input_queue, output_queue, p_id, progress_details, default_output, individual_timeout=300.0):
    """ This function will be executed by all the child Processes spawned by the Parent process
    """
    if p_id != -1: # Not a progress bar process
        signal.signal(signal.SIGALRM, receive_signal)

        while True: 
            try:
                loc, function, args, kwargs = input_queue.get_nowait()
            except:
                break
            
            signal.alarm(int(individual_timeout))

            try:
                output = function(*args, **kwargs)
                output_queue.put((loc, (0, None), output))
            except FunctionTimeoutException, details:
                output_queue.put((loc, (1, details), default_output))
            except Exception, details:
                output_queue.put((loc, (2, details), default_output))
            finally:
                signal.alarm(0)
                with progress_details['process_lock']:
                    progress_details['completed_tasks'].value += 1

        sys.exit(0)
    else:
        display_progress_bar(progress_details)





def plmapp(func, args=[], kwargs=[], processes=10, progress_bar=False, default_output=None, individual_timeout=20):
    """ 
    """
    bigger_array = args if len(args) > len(kwargs) else kwargs 
    big_len = len(bigger_array)
    ag = lambda array, index, default : default if index >= len(array) else array[index]

    input_queue = Queue()
    output_queue = Queue()
    error_queue = Queue()


    progress_details = {'completed_tasks' : Value('i', 0), 
                        'total_tasks' : big_len, 
                        'process_lock' : Lock()}

    # Load the input queue with tasks
    for i in xrange(big_len):
        task = (i, func, ag(args, i, ()), ag(kwargs, i, {})) 
        input_queue.put(task)

    # Create the processes
    processes = []
    end = min(processes, big_len)
    start = -1 if progress_bar else 0


    for i in xrange(start, end):
        p = Process(target=queue_exec, args=(input_queue, output_queue, i, progress_details, default_output, individual_timeout))
        processes.append(p)
        p.start()

    # Wait for the processes to complete    
    for process in processes:
        process.join() 

    output_errors = [ None for _ in xrange(big_len)]
    output_values = [ None for _ in xrange(big_len)]

    while True:
        try:
            loc, error_value = error_queue.get_nowait()
            output_errors[loc] = error_value
        except:
            break


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
    error, output = plmapp(func, args, kwargs, threads, True)
    print "End time : " , datetime.datetime.now()
    print "Errors : "
    pprint(error)
    print "Output : "
    print(output)






#!/usr/bin/python

# This is a generic module for parallelization ( using Processes )
# 
# 
# 
# How to use This ? 
#   Create a function that can do what you want to achieve with 1 parameter or 1 set of parameters
#   pass this function and the set of arguments to the pmapp function
#   pmapp will take care of parallelization for you 
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

#   step 2) Call the pmapt function 
#   error, output = pmapt(add, inputs , [], 4, default_output=None)
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




class FunctionTimeoutException(Exception):
    """ This exception will be raised when a function takes too long to complete
    """
    pass



def receive_signal(signum, stack):
    details = "Function timeout"
    raise FunctionTimeoutException(details)



def queue_exec(input_queue, output_queue, log_queue, individual_timeout=20.0, default_output=None):
    """ This function will be executed by all the child Processes spawned by the Parent process
    """

    signal.signal(signal.SIGALRM, receive_signal)

    while True: 
        try:
            id, function, args, kwargs = input_queue.get_nowait()
        except :
            break
        
        signal.alarm(int(individual_timeout))

        try:
            output = function(*args, **kwargs)
            output_queue.put({ 'id' : id, 'output' : output, 'error_code' : '0', 'err_desc' : None })
        except FunctionTimeoutException, details:
            output_queue.put({ 'id' : id, 'output' : default_output, 'error_code' : '1', 'err_desc' : str(details) })
            log_queue.put("state:MAJOR, id:{}, output:{}, err:{}, desc:{}".format(id, default_output, '1', str(details)))
        except Exception, details:
            log_queue.put("state:MAJOR, id:{}, output:{}, err:{}, desc:{}".format(id, default_output, '1', str(details)))
            output_queue.put({ 'id' : id, 'output' : default_output, 'error_code' : '1', 'err_desc' : str(details) })
            pass
            # Log the error here 
        signal.alarm(0)

    sys.exit(0)






def plmapp(func, args=[], kwargs=[], processes=10, default_output=None, individual_timeout=20):
    """ 
    """
    input_queue = Queue()
    output_queue = Queue()
    log_queue = Queue()

    bigger_array = args if len(args) > len(kwargs) else kwargs 
    big_len = len(bigger_array)
    ag = lambda array, index, default : default if index >= len(array) else array[index]

    # Load the input queue with tasks
    for _ in xrange(big_len):
        task = ( _ , func, ag(args, _, ()), ag(kwargs, _, {}) ) 
        input_queue.put(task)

    # Create the processes
    processes = []
    for _ in xrange(min(processes, 10)):
        p = Process(target=queue_exec, args=(input_queue, output_queue, log_queue, individual_timeout, default_output))
        processes.append(p)
        p.start()

    # Wait for the processes to complete    
    for _ in processes:
        _.join() 

    # Get all the output dictionaries from the output queue
    output_dicts = []
    for output_dict in xrange(big_len):
        output_dicts.append( output_queue.get_nowait() )

    # Sort the output based on the id
    output_sorted_dicts = sorted(output_dicts, key=lambda x : x['id'])

    error_array = [ (_['error_code'], _['err_desc']) for _ in output_sorted_dicts ]
    output_array = [ _['output'] for _ in output_sorted_dicts ]

    return (error_array, output_array)






if __name__ == "__main__":
    def do_it(a, b=8, c=10):
        time.sleep(1)
        return a + b  + c

    # args
    args = [ (_,) for _ in xrange(10) ]
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






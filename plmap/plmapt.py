#!/usr/bin/env python
"""
This is a generic module for parallelization ( using Threads )


How to use This ? 
  Create a function that can do what you want to achieve with 1 parameter or 1 set of parameters
  pass this function and the set of arguments to the plmapt function
  plmapt will take care of parallelization for you 
  It will return lists of errros and outputs for the corresponding inputs
Eg : 

  Let say you want to add these pairs of numbers. 

  inputs = [
  [3, 5],
  [8, 9] ,
  [11, 12],
  [15, 16],
  ]

  step 1) define the function for a single set of inputs
  def add ( a, b ) :
      return a + b 

  step 2) Call the plmapt function 
  error, output = plmapt(add, inputs , [], 4, default_output=None)
  print output 

The output will look like this : 
[8, 17, 23, 31]
"""


from __future__ import division
from threading import Thread
from Queue import Queue
import sys
import time

from pprint import pprint

PROGRESS_BAR_POLL_TIME = 1
PROGRESS_BAR_LENGTH = 20


class Worker(Thread):
    """ Worker thread is the thread that actually executes the function with the given arguments
    """

    def __init__(self, thread_id, input_queue, default_output=None, progress_details={}, progress_bar_thread=False):
        super(Worker, self).__init__(name='%s' %(thread_id))
        self.input_queue = input_queue
        self.default_output = default_output
        self.progress_details = progress_details
        self.progress_bar_thread = progress_bar_thread


    def generate_loading_string(self, completed_tasks, total_tasks):
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


    def display_progress_bar(self):
        """ 50%[---------->          ]5/10 
        """
        completed_tasks = self.progress_details['completed_tasks']
        total_tasks = self.progress_details['total_tasks']

        while completed_tasks != total_tasks:

            time.sleep(PROGRESS_BAR_POLL_TIME)

            completed_tasks = self.progress_details['completed_tasks']
            total_tasks = self.progress_details['total_tasks']

            print self.generate_loading_string(completed_tasks, total_tasks)
            sys.stdout.write("\033[F")

        # Display 100% completion
        completed_tasks = self.progress_details['completed_tasks']
        total_tasks = self.progress_details['total_tasks']
        print self.generate_loading_string(completed_tasks, total_tasks)
        

    # run method will be called when the thread is started using thread.start() , Hence overriding run method
    def run(self):
        """ Pick up the task from the queue and execute it  
        """ 
        if not self.progress_bar_thread:
            while True:
                try:
                    task = self.input_queue.get_nowait()
                except:
                    break

                func, args, kwargs, error_loc, output_loc = task
                
                try:
                    output = func(*args, **kwargs)
                    error_loc.append(0) # Indicates Success
                    output_loc.append(output) 
                except Exception, details:
                    error_loc.append(1) # Indicates Failure
                    output_loc.append(self.default_output) 
                finally:
                    self.progress_details['completed_tasks'] += 1
        else:
            self.display_progress_bar()



def plmapt(func, args=[], kwargs=[], threads=10, default_output=None, sort_output=True, progress_bar=False):
    """ Create the workers and get them to work.
        Returns the error and output array
        Sorting the output according to the input order is optional (<sort_output>)
        Displaying the Progress bar is optional
    """
    bigger_array = args if len(args) > len(kwargs) else kwargs 
    big_len = len(bigger_array)

    # Get the element of an array given the index, return default if index out of range
    ag = lambda array, index, default : default if index >= len(array) else array[index]

    progress_details = {'total_tasks' : big_len , 'completed_tasks' : 0}

    input_queue = Queue()
    output_errors = [[] for _ in xrange(big_len)]
    output_values = [[] for _ in xrange(big_len)]

    # Load the input_queue  ( big_len = the number of tasks )
    for i in xrange(big_len):
        task = [func, ag(args, i, ()), ag(kwargs, i, {}), output_errors[i], output_values[i]]
        input_queue.put(task)

    # Create workers and start them
    list_of_workers = []
    for i in xrange(min(threads, big_len)):
        worker = Worker(i, input_queue, default_output, progress_details)
        worker.setDaemon(True)
        list_of_workers.append(worker)
        worker.start()

    # If the progress bar needs to be displayed
    if progress_bar:
        progress_worker = Worker("-1", input_queue, default_output, progress_details, True)
        progress_worker.setDaemon(True)
        list_of_workers.append(progress_worker)
        progress_worker.start()

    # Wait till the threads complete
    for _ in list_of_workers:
        _.join()

    return output_errors, output_values



if __name__ == "__main__":

    def add ( a, b ) :
        time.sleep(a)
        if a == 3:
            1 / 0
        return  (a, b,a + b) 

    inputs = [
    [3, 5],
    [8, 9] ,
    [4, 12],
    [1, 16],
    [1, 16],
    [1, 16],
    [1, 16],
    [1, 16],
    [3, 16],
    [1, 16],

    ]
    error, output = plmapt(add, inputs , [], 4, default_output=None, progress_bar=True)
    print output 
    error, output = plmapt(add, inputs , [], 8, default_output=None, sort_output=False, progress_bar=True)
    print output
    error, output = plmapt(add, [] , [], 4, default_output=None, progress_bar=True)
    print output 
    error, output = plmapt(add, inputs , [], 8, default_output=None, sort_output=False, progress_bar=False)
    print output
    print error



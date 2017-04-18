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

import sys
from Queue import Queue
from threading import Lock, Thread
from progress_bar import generate_loading_string

PROGRESS_BAR_POLL_TIME = 1

class Worker(Thread):
    """ Worker thread is the thread that actually executes the function with the given arguments
    """

    def __init__(self, thread_id, input_queue, progress_details, default_output):
        super(Worker, self).__init__(name='%s' % (thread_id))
        self.input_queue = input_queue
        self.default_output = default_output
        self.progress_details = progress_details
        self.progress_bar_thread = (thread_id == -1)


    def display_progress_bar(self):
        """ 50%[---------->          ]5/10 
        """
        completed_tasks = self.progress_details['completed_tasks']
        total_tasks = self.progress_details['total_tasks']
        while completed_tasks != total_tasks:
            time.sleep(PROGRESS_BAR_POLL_TIME)
            completed_tasks = self.progress_details['completed_tasks']
            total_tasks = self.progress_details['total_tasks']
            print generate_loading_string(completed_tasks, total_tasks)
            sys.stdout.write("\033[F")
        # Display 100% completion
        completed_tasks = self.progress_details['completed_tasks']
        total_tasks = self.progress_details['total_tasks']
        print generate_loading_string(completed_tasks, total_tasks)

    # run method will be called when the thread is started using
    # thread.start() , Hence overriding run method
    def run(self):
        """ Pick up the task from the queue and execute it
        """
        if not self.progress_bar_thread:
            while True:
                try:
                    task = self.input_queue.get_nowait()
                except:
                    break

                func, loc, args, kwargs, errors, outputs = task

                try:
                    output = func(*args, **kwargs)
                    errors[loc], outputs[loc] = 0, output
                except Exception, details:
                    errors[loc], outputs[loc] = details, self.default_output
                finally:
                    with self.progress_details['thread_lock']:
                        self.progress_details['completed_tasks'] += 1
        else:
            self.display_progress_bar()





def plmapt(func, args=[], kwargs=[], threads=10, default_output=None, progress_bar=False):
    """ Create the workers and get them to work.
        Returns the error and output array
        Displaying the Progress bar is optional
    """
    bigger_array = args if len(args) > len(kwargs) else kwargs
    big_len = len(bigger_array)

    # Get the element of an array given the index, return default if index out
    # of range
    ag = lambda array, index, default: default if index >= len(array) else array[
        index]

    progress_details = {'total_tasks': big_len,
                        'completed_tasks': 0, 'thread_lock': Lock()}

    input_queue = Queue()
    output_errors = [[] for _ in xrange(big_len)]
    output_values = [[] for _ in xrange(big_len)]

    # Load the input_queue  ( big_len = the number of tasks )
    for i in xrange(big_len):
        task = [func, i, ag(args, i, ()), ag(kwargs, i, {}),
                output_errors, output_values]
        input_queue.put(task)

    # Create workers and start them
    list_of_workers = []
    end = min(threads, big_len)
    start = -1 if progress_bar else 0

    for i in xrange(start, end):
        worker = Worker(i, input_queue, progress_details, default_output)
        worker.setDaemon(True)
        list_of_workers.append(worker)
        worker.start()

    # Wait till the threads complete
    for _ in list_of_workers:
        _.join()

    return output_errors, output_values


if __name__ == "__main__":
    import time
    from pprint import pprint

    def add(a, b):
        time.sleep(1)
        if a == 3:
            1 / 0
        return (a, b, a + b)

    inputs = [
        [3, 5],
        [8, 9],
        [4, 12],
        [1, 16],
        [1, 16],
        [1, 16],
        [1, 16],
        [1, 16],
        [3, 16],
        [1, 16],

    ]
    error, output = plmapt(
        add, inputs, [], 1, default_output=None, progress_bar=True)
    pprint(output)
    error, output = plmapt(
        add, inputs, [], 2, default_output=None, progress_bar=True)
    pprint(output)
    error, output = plmapt(
        add, [], [], 4, default_output=None, progress_bar=True)
    pprint(output)
    error, output = plmapt(
        add, inputs, [], 8, default_output=None, progress_bar=False)
    pprint(output)
    pprint(error)

#!/usr/bin/python

# This is a generic module for parallelization ( using Threads )
# 
# 
# 
# How to use This ? 
# 	Create a function that can do what you want to achieve with 1 parameter or 1 set of parameters
# 	pass this function and the set of arguments to the pmapt function
# 	pmapt will take care of parallelization for you 
# 	It will return lists of errros and outputs for the corresponding inputs
# Eg : 

# 	Let say you want to add these pairs of numbers. 

# 	inputs = [
# 	[3, 5],
# 	[8, 9] ,
# 	[11, 12],
# 	[15, 16],
# 	]

# 	step 1) define the function for a single set of inputs
# 	def add ( a, b ) :
# 		return a + b 

# 	step 2) Call the pmapt function 
# 	error, output = pmapt(add, inputs , [], 4, default_output=None)
# 	print output 

# The output will look like this : 
# [8, 17, 23, 31]



from threading import Thread
from Queue import Queue
from pprint import pprint
import datetime
import sys
import time
import csv
import types



class Worker(Thread):
	""" Worker thread is the thread that actually executes the function with the given arguments
	"""

	def __init__(self, thread_id, input_queue, output_queue, log_queue, default_output=None):
		super(Worker, self).__init__(name='%s' %(thread_id ))
		self.input_queue = input_queue
		self.output_queue = output_queue
		self.log_queue = log_queue
		self.default_output = default_output


	# run method will be called when the thread is started using thread.start() , Hence overriding run method
	def run(self):
		""" Pick up the task from the queue and execute it 
			
		""" 
		while True:
			try:
				task = self.input_queue.get_nowait()
			except:
				break

			task_id, func, args, kwargs = task
			
			try:
				output = func(*args, **kwargs)
				self.output_queue.put({ 'task_id' : task_id, 'error_code' : '0', 'error_desc' : 'None', 'output' : output})
			except Exception, details:
				self.output_queue.put({ 'task_id' : task_id, 'error_code' : '1', 'error_desc' : details, 'output' : self.default_output})
				self.log_queue.put({ 'task_id' : task_id, 'error_code' : '1', 'error_desc' : details, 'output' : self.default_output})



def plmapt(func, args=[], kwargs=[], threads=10, default_output=None):
	""" Creates the workers get them to work.
		Returns the error and output array

	"""
	input_queue = Queue()
	output_queue = Queue()
	log_queue = Queue()

	bigger_array = args if len(args) > len(kwargs) else kwargs 
	big_len = len(bigger_array)

	# Get the element of an array given the index, return default if index out of range
	ag = lambda array, index, default : default if index >= len(array) else array[index]

	# Load the input_queue  ( big_len = the number of tasks )
	for _ in xrange(big_len):
		task = ( _, func, ag(args, _, ()), ag(kwargs, _, {}))
		input_queue.put(task)

	# Create workers and start them
	list_of_workers = []
	for i in xrange(min(threads, big_len)):
		worker = Worker(i, input_queue, output_queue, log_queue, default_output)
		worker.setDaemon(True)
		list_of_workers.append(worker)
		worker.start()

	# Wait till the threads complete
	for _ in list_of_workers:
		_.join()

	# get the outputs in an array
	output_dicts = []
	for output_dict in xrange(big_len):
		output_dicts.append( output_queue.get_nowait() )

	# Sort the output based on the id
	output_sorted_dicts = sorted(output_dicts, key=lambda x : x['task_id'])
	
	error_array = [ (_['error_code'], _['error_desc']) for _ in output_sorted_dicts ]
	output_array = [ _['output'] for _ in output_sorted_dicts ]

	return (error_array, output_array)



if __name__ == "__main__":
	def add ( a, b ) :
		time.sleep(1)
		return a + b 
	inputs = [
	[3, 5],
	[8, 9] ,
	[11, 12],
	[15, 16],
	]
	error, output = plmapt(add, inputs , [], 4, default_output=None)
	print output 



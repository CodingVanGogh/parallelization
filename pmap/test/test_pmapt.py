



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
	error, output = pmapt(add, inputs , [], 4, default_output=None)
	print output 
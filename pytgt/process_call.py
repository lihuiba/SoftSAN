import subprocess

# change the type of output below
		
def process_call_argv(argv):
	process = subprocess.Popen(argv, stdout=subprocess.PIPE, shell=False)
	output = ""
	while True:
		out = process.stdout.readline()
		if out == '' and process.poll() != None: break
		output += out		
	return (process.returncode, output)	

def process_reload_argv(argv):
	process = subprocess.Popen(argv, stdout=subprocess.PIPE, shell=False)
	outlist = []
	while True:
		out = process.stdout.readline()
		if out == '' and process.poll() != None: break
		outlist.append(out)	
	return (process.returncode, outlist)	
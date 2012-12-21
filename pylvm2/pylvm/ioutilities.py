import os, sys
import select
import subprocess

def process_call(command):
	return process_call_argv(command, None)

def process_call2(command, argv):
	if argv:
		return process_call_argv([command, argv])
	else:
		return process_call_argv([command])
		
def process_call_argv(argv):
	process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	output = ""
	while True:
		out = process.stdout.readline()
		if out == '' and process.poll() != None: break
		output += out
	process.stdout.close()
	return (process.returncode, output)	
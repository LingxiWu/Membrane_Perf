#!/home/lw2ef/anaconda3/bin/python3 python3
import math
import random
import os

# ./ramulator configs/SALP-config.cfg --mode=dram Q5_Sieve_Naive.trace --stats=Q5.stats | grep 'run_dramtrace' > output_Q5.txt

# ./ramulator configs/SALP-config.cfg --mode=dram --stats q1.stats Q5_Sieve_Naive.trace

# note: configs/SALP-config.cfg option "record_cmd_trace = on"

import re
import random

# channels = 2
# ranks = 2

record_cmd_trace = 'on' # ignore this for now.




if record_cmd_trace == 'on':
	# for i in range(1, 14):

	# 	command = './ramulator configs/SALP-config.cfg --mode=dram --stats '
	# 	q_s = str(i)

	# 	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/output/Q' + q_s
	# 	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_Two/output/Q' + q_s
	# 	command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/output/filt_only/Q' + q_s
	# 	command += '.stats '

	# 	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/Q' + q_s + '_Fulcrum_Mode_One.trace'
	# 	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_Two/Q' + q_s + '_Fulcrum_Mode_Two.trace'
	# 	command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/filt_only/Fulcrum_Mode_One_Filt.trace'

	# 	# command += ' > output_'
	# 	# command += q_s
	# 	# command += '.txt'
	# 	# print(command)
	# 	print('Executing: ' + command)
	# 	os.system(command)

	## for mode one Filt hybrid
	for query in range(1, 14):

		salp = 1
		command = './ramulator configs/SALP-config.cfg --mode=dram --stats '

		# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/output/filt_only/Q' + str(query) + '_salp_' + str(salp) + '.stats '
		command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/output/alu_only/Q' + str(query) + '_salp_' + str(salp) + '.stats '

		# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/filt_only/Q' + str(query) + '_salp_' + str(salp) + '_Fulcrum_Mode_One_Filt.trace'
		command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/alu_only/Q' + str(query) + '_salp_' + str(salp) + '_Fulcrum_Mode_One_ALU.trace'

		print('Executing: ' + command)
		os.system(command)

		# # process and move the command traces: cmd-trace-chan-0-rank-0.cmdtrace --> Fulcrum-Q01-cmd-trace-chan-0-rank-0.trace
		# # need to (1) convert 'PRER' -> 'PREA' AND (2) keep half of 'SASEL' to: cite SALP PAPER "we estimate the power consumption of SA SEL to be 49.6% of ACTIVATE"
		# q_s = 'Q' + str(i)
		# fout_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_distributed_sf_1/output/cmd_trace/'
		
		# for chan in range(0, channels):
		# 	for rank in range(0, ranks):

		# 		fout_s = fout_prefix + 'Fulcrum_' + q_s + '_cmd-trace-chan-' + str(chan) + '-rank-' + str(rank) + '.trace'
		# 		if os.path.exists(fout_s):
		# 			os.remove(fout_s)
		# 			# print('Old cmd trace file: %s is removed. Generating a new one.' % ('Fulcrum_' + q_s + '_cmd-trace-chan-' + str(chan) + '-rank-' + str(rank) + '.trace'))

		# 		fout = open(fout_s, "w+")

		# 		# replace all 'PRER' to 'PREA'
		# 		cmd_trace_f = 'cmd-trace-chan-' + str(chan) + '-rank-' + str(rank) + '.cmdtrace'
		# 		with open(cmd_trace_f, 'r') as fin:
		# 			for line in fin:
		# 				if re.search('PRER',line):
		# 					fout.write(line.replace('PRER','PREA'))
		# 				elif re.search('SASEL',line):
		# 					num = random.random() # 0.0 ~ 1.0
		# 					if num > 0.5:
		# 						fout.write(line.replace('SASEL','ACT'))
		# 					else:
		# 						continue
		# 				else:
		# 					fout.write(line)

		# 		if os.path.exists(cmd_trace_f):
		# 			os.remove(cmd_trace_f)

		# 		fin.close()
		# 		fout.close()

# accumulate power/energy 
# ./drampower -m memspecs/MICRON_4Gb_DDR4-1866_8bit_A.json -c /home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_distributed_sf_1/output/cmd_trace/Fulcrum_Q1_cmd-trace-chan-0-rank-0.trace -r

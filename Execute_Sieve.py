#!/home/lw2ef/anaconda3/bin/python3 python3
import math
import random
import os

# ./ramulator configs/SALP-config.cfg --mode=dram Q5_Sieve_Naive.trace --stats=Q5.stats | grep 'run_dramtrace' > output_Q5.txt

# ./ramulator configs/SALP-config.cfg --mode=dram --stats q1.stats Q5_Sieve_Naive.trace


for i in range(1, 2):
	command = './ramulator configs/SALP-config.cfg --mode=dram --stats '
	q_s = str(i)

	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_One/output/reads_only/Q' + q_s
	command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_One/output/writes_only/Q' + q_s
	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_agg_and_filt_v/output/Q' + q_s
	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_Two/output/retrieves/Q' + q_s
	command += '.stats '

	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_One/Reads/Q' + q_s + '_Sieve_Mode_One_Reads.trace'
	command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_One/Writes/Q' + q_s + '_Sieve_Mode_One_Writes.trace'
	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_agg_and_filt_v/Q' + q_s + '_Sieve_agg_and_filt_v.trace'
	# command += '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_Two/retrievals_only/Q' + q_s + '_Sieve_Mode_Retrieval.trace'

	# command += ' > output_'
	# command += q_s
	# command += '.txt'
	# print(command)
	print('Executing: ' + command)
	os.system(command)


#!/home/lw2ef/anaconda3/bin/python3 python3
import math
import random
import os



def trace_analysis(query_index):
	## input trace file: "SubarrayID - 0 Block - 0 Row - 1"
	input_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/hybrid_sf_100traces/' 
	if query_index < 9:
		input_q_file = input_prefix + 'q0'+str(query_index+1)+'_hybrid_sf_100.trace'
	else:
		input_q_file = input_prefix + 'q'+str(query_index+1)+'_hybrid_sf_100.trace'

	unique_row_counter = 0

	unique_dbrecords = set()
	unique_rows = set() # [sa, row]

	prev_row_index = 0
	with open(input_q_file) as infile:
		# SubarrayID - 6 Block - 0 Row - 2 Col - 4071 dbRecord-9546
		for line in infile:
			words = line.split(' ')
			if 'dbRecord' in line:

				db_record = words[-1].split('-')[1]
				unique_dbrecords.add(db_record)
				addr = [words[2], words[8]]
				unique_rows.add(tuple(addr))


	print('len(unique_dbrecords): %d' % len(unique_dbrecords))
	print('len(unique_rows): %d' % len(unique_rows))
			
	










# for i in range(0, len(attribute_names)):
for i in range(0,13):
	print('analyze hybrid data layout for query: %d' % (i+1))
	trace_analysis(i)
	# print('')









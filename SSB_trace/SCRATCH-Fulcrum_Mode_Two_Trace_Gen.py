#!/home/lw2ef/anaconda3/bin/python3 python3
import math
import random
import os

##### constants #####
channel_width = 64 # bits?
prefetch_size = 8
tx_bit_length = 6 # size of each transaction: 2^tx_bit_length = 64 bits, They are the LSBs and are discarded 

chip_orgs = ['SALP_512Mb_x4', 'SALP_512Mb_x8', 'SALP_512Mb_x16',
			 'SALP_1Gb_x4', 'SALP_1Gb_x8', 'SALP_1Gb_x16',
			 'SALP_2Gb_x4', 'SALP_2Gb_x8', 'SALP_2Gb_x16',
			 'SALP_4Gb_x4', 'SALP_4Gb_x8', 'SALP_4Gb_x16',
			 'SALP_8Gb_x4', 'SALP_8Gb_x8', 'SALP_8Gb_x16']

chip_levels = ["Channel", "Rank", "Bank", "SubArray", "Row", "Column"]

chip_sizes = [512, 512, 512,
			  1<<10, 1<<10, 1<<10,
			  2<<10, 2<<10, 2<<10,
			  4<<10, 4<<10, 4<<10,
			  8<<10, 8<<10, 8<<10]

chip_col_widths = [4, 8, 16,  # dq, data pin width?
				   4, 8, 16,
				   4, 8, 16,
				   4, 8, 16,
				   4, 8, 16]

chip_level_counts = [[0, 0, 8, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10], [0, 0, 8, 0, 0, 1<<10],   # the number of things at each level, e.g., 8 banks, 1024 columns, etc.
                     [0, 0, 8, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10], [0, 0, 8, 0, 0, 1<<10],
                     [0, 0, 8, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10], [0, 0, 8, 0, 0, 1<<10],
                     [0, 0, 8, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10], [0, 0, 8, 0, 0, 1<<10],
                     [0, 0, 8, 0, 0, 1<<12], [0, 0, 8, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10]]

addr_segment_order = ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel'] # the specific addr mapping adopted in ramulator
addr_segment_bits = [0, 0, 0, 0, 0, 0] # how many bits to represent each addr segment
##### end constants #####

def complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa):

	assert n_sa >=1 and n_sa <= 128 and n_sa & (n_sa-1) == 0, 'n_sa should be power of 2 and between 1 and 128'
	assert n_chan & (n_chan-1) == 0, 'channel numbers should be power of 2'
	assert n_rank & (n_rank-1) == 0, 'channel numbers should be power of 2'

	chip_level_count_entry = chip_level_counts[chip_orgs.index(chip_name)]

	# assign n_sa78
	chip_level_count_entry[chip_levels.index("SubArray")] = n_sa 

	temp = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Bank")] * n_sa * chip_level_count_entry[chip_levels.index("Column")];
	row_num = chip_level_count_entry[chip_levels.index("Row")] = chip_sizes[chip_orgs.index(chip_name)] * (1<<20) / temp;
	
	# assign rows
	chip_level_count_entry[chip_levels.index("Row")] = int(row_num)

	# assign chan
	chip_level_count_entry[chip_levels.index("Channel")] = n_chan

	# assign rank
	chip_level_count_entry[chip_levels.index("Rank")] = n_rank

	return chip_level_count_entry

def totalCapacityInByte(chip_name, chip_level_count_entry):
	n_chips = int(channel_width / chip_col_widths[chip_orgs.index(chip_name)])
	
	totalByte = 1
	for lvl in chip_level_count_entry:
		totalByte *= lvl

	totalByte *= n_chips * chip_col_widths[chip_orgs.index(chip_name)] 
	totalByte /= 8
	return int(totalByte)




def distributeSAs():
	total_n_sa = int(chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")])
	print('In distributeUsedSA() -> Total number of SubArrays (gang of chips): %d' % total_n_sa)

	### build chan list
	num_chans = chip_level_count_entry[chip_levels.index("Channel")]
	chan_lst = []
	chan_bits_len = (int)(math.log2(num_chans))
	if chan_bits_len > 0:
		for c in range((int)(num_chans)):
			chan_fmt_str = '0' + str(chan_bits_len) + 'b'
			b = format(c, chan_fmt_str)
			chan_lst.append(b)
	# print("chan_list: "+str(chan_lst))  
	# print("chan_bits_len: "+str(chan_bits_len))

	### build rank list
	num_ranks = chip_level_count_entry[chip_levels.index("Rank")]
	rank_lst = []
	rank_bits_len = (int)(math.log2(num_ranks))
	if rank_bits_len > 0:
		for c in range((int)(num_ranks)):
			rank_fmt_str = '0' + str(rank_bits_len) + 'b'
			b = format(c, rank_fmt_str)
			rank_lst.append(b)
	# print("rank_lst: "+str(rank_lst))  
	# print("rank_bits_len: "+str(rank_bits_len))

	### build bank list
	num_banks = chip_level_count_entry[chip_levels.index("Bank")]
	bank_lst = []
	bank_bits_len = (int)(math.log2(num_banks))
	if bank_bits_len > 0:
		for c in range((int)(num_banks)):
			bank_fmt_str = '0' + str(bank_bits_len) + 'b'
			b = format(c, bank_fmt_str)
			bank_lst.append(b)
	# print("bank_lst: "+str(bank_lst))  
	# print("bank_bits_len: "+str(bank_bits_len))

	### build subArray list
	num_subArrays = chip_level_count_entry[chip_levels.index("SubArray")]
	subArray_lst = []
	subArray_bits_len = (int)(math.log2(num_subArrays))
	if subArray_bits_len > 0:
		for c in range((int)(num_subArrays)):
			subArray_fmt_str = '0' + str(subArray_bits_len) + 'b'
			b = format(c, subArray_fmt_str)
			subArray_lst.append(b)
	# print("subArray_lst: "+str(subArray_lst))  
	# print("su0.001705bArray_bits_len: "+str(subArray_bits_len))

	saMap = {}
	i = 0
	while i <= total_n_sa:
		for SA in subArray_lst:
			for BA in bank_lst:
				for RA in rank_lst:
					for CH in chan_lst:
						if i == total_n_sa:
							return saMap
						saMap[i] = [CH,RA,BA,SA]
						i += 1

def trace_gen(chip_level_count_entry, saMap, query_index):

	# output trace file
	q_s = 'Q' + str(query_index+1)
	output_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_Two/'
	trace_file = output_prefix + q_s + '_Fulcrum_Mode_Two.trace'

	if os.path.exists(trace_file):
		os.remove(trace_file)
		print('Old %s is removed. Generating a new one.' % trace_file)
	fo = open(trace_file, "w")

	### build row list
	num_rows = chip_level_count_entry[chip_levels.index("Row")]
	row_lst = []
	row_bits_len = (int)(math.log2(num_rows))
	if row_bits_len > 0:
		for c in range((int)(num_rows)):
			row_fmt_str = '0' + str(row_bits_len) + 'b'
			b = format(c, row_fmt_str)
			row_lst.append(b)
	# print("row_lst: "+str(row_lst))  
	# print("row_bits_len: "+str(row_bits_len))

	### build column list
	num_columns = chip_level_count_entry[chip_levels.index("Column")]
	column_lst = []
	column_bits_len = (int)(math.log2(num_columns)) - (int)(math.log2(prefetch_size))
	# account for prefetching: addr_bits[int(T::Level::MAX) - 1] -= calc_log2(spec->prefetch_size); 
	if column_bits_len > 0:
		for c in range((int)(2**column_bits_len)):
			column_fmt_str = '0' + str(column_bits_len) + 'b'
			b = format(c, column_fmt_str)
			column_lst.append(b)
	# print("column_lst: "+str(column_lst))  
	# print("column_bits_len: "+str(column_bits_len))

	for level in chip_levels:
		if level != 'Column':
			addr_segment_bits[addr_segment_order.index(level)] = (int)(math.log2(chip_level_count_entry[chip_levels.index(level)]))
		else:
			addr_segment_bits[addr_segment_order.index(level)] = ((int)(math.log2(chip_level_count_entry[chip_levels.index(level)])) - (int)(math.log2(prefetch_size)))

	print('In trace_gen() -> addr_segment_order:', addr_segment_order)	 # ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel']
	print('In trace_gen() -> addr_segment_bits:' , addr_segment_bits)    # [12, 3, 3, 1, 7, 1]

	input_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/sf_100_row_major_trace_files/' 
	if query_index < 9:
		input_q_file = input_prefix + 'q0'+str(query_index+1)+'_sf_100_row_major.trace'
	else:
		input_q_file = input_prefix + 'q'+str(query_index+1)+'_sf_100_row_major.trace'

	# read_count = 0
	# command_list = []

	filt_trace = []
	retrieval_trace = []

	# prev_row = 1
	with open(input_q_file, 'r') as sa_trace_f:
		## input trace file: "SubarrayID - 0 Block - 0 Row - 1"
		for line in sa_trace_f:
			parsed_line = line.split(' ')
			
			SA_index = int(parsed_line[2])
			row_index = int(parsed_line[5])-1

			addr_seg_map = {
				'Channel': saMap[SA_index][0],
				'Rank': saMap[SA_index][1],
				'Bank': saMap[SA_index][2],
				'SubArray': saMap[SA_index][3],
				'Column': str(random.choice(column_lst)),
				'Row': str(row_lst[row_index])
			}


			addr_temp = ''
			for seg in addr_segment_order:
				addr_temp += addr_seg_map[seg]

			for tx in range(tx_bit_length):
				addr_temp += '0'

			cmd = hex(int(addr_temp,2))+" R"+"\n"

			if len(parsed_line) == 7:
				# filt_trace.append(cmd)
				# prev_row = row_index
				continue
			elif len(parsed_line) == 10:
				# if prev_row == row_index:
				# 	continue
				retrieval_trace.append(cmd)
				# prev_row = row_index

			# command_list.append(cmd)
			# read_count += 1

	# print('In trace_gen() -> Total number of read commands for Filtering: %d' % len(filt_trace))
	print('In trace_gen() -> Total number of read commands for Retrieval: %d' % len(retrieval_trace))

	# random.shuffle(command_list)
	# random.shuffle(filt_trace)
	random.shuffle(retrieval_trace)

	# for cmd in filt_trace:
	# 	fo.write(cmd)
	for cmd in retrieval_trace:
		fo.write(cmd)

	sa_trace_f.close()
	fo.close()












#### variables #####
attribute_names = [['d_year','lo_discount','lo_quantity'], ['d_yearmonthnum','lo_discount','lo_quantity'], ['d_weeknuminyear','d_year','lo_discount','lo_quantity'],
				   ['p_category','s_region'], ['p_brand1','s_region'], ['p_brand1','s_region'], 
				   ['c_region','s_region','d_year'], ['c_nation','s_nation','d_year'], ['c_city','s_city','d_year'],
				   ['c_city','d_yearmonth','s_city'], ['c_region','s_region','p_mfgr'], ['c_region','s_region','d_year','p_mfgr'],
				   ['c_region','s_nation','d_year','p_category']]

attribute_bit_length = [[3,11,6], [7,11,6], [6,3,11,6],
						[5,3], [10,3], [10,3],
						[3,3,3], [5,5,3], [8,8,3],
						[8,8,7], [3,3,3], [3,3,3,3],
						[3,5,3,5]]

# Ranged predicates are repeated twice: one for >=, and one for <=
attribute_repeats = [[1, 2, 1], [1,2,2], [1,1,2,2],
					 [1,1], [2,1], [1,1],
					 [1,1,2], [1,1,2], [2,2,2],
					 [2,1,2], [1,1,2], [1,1,2,2],
					 [1,1,2,1]]

chip_name = 'SALP_4Gb_x16'

n_sa = 128 # number of subarray groups --> subarray that can be independently accessed in parallel
n_chan = 8
n_rank = 2


##### end variables #####

print('')
print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
chip_level_count_entry = complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa)
print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
print('Chip_levels: ', chip_levels)
print('Chip_level_count: ', chip_level_count_entry)
print('')

# convert a subarray index to [CH,RA,BA,SA]
saMap = distributeSAs() 

for i in range(0, 13):
	q_s = 'Q' + str(i+1)
	print('%s Fulcrum Mode Two trace gen ... ' % q_s)
	trace_gen(chip_level_count_entry, saMap, i)
	print('')

# trace_gen(chip_level_count_entry, saMap, 2)
# trace_gen(chip_level_count_entry, saMap, 8)
# trace_gen(chip_level_count_entry, saMap, 10)



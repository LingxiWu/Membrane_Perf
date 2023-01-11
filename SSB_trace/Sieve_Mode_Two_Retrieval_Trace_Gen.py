#!/home/lw2ef/anaconda3/bin/python3 python3

# first, activate rows to filter, then activate to retrieve records
# the number of rows to activate for filtering depends on predicate total bit length
# the number of rows to activate for records retrieval dependes on requested attributes total bit length

import math
import random
import os
import sys

##### chip_organizations
# n_sa = 8 # number of subarray groups --> subarray that can be independently accessed in parallel

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
                     [0, 0, 8, 0, 0, 1<<12], [0, 0, 32, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10]]

addr_segment_order = ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel'] # the specific addr mapping adopted in ramulator
addr_segment_bits = [0, 0, 0, 0, 0, 0] # how many bits to represent each addr segment


def complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa):

	assert n_sa >=1 and n_sa <= 128 and n_sa & (n_sa-1) == 0, 'n_sa should be power of 2 and between 1 and 128'
	assert n_chan & (n_chan-1) == 0, 'channel numbers should be power of 2'
	assert n_rank & (n_rank-1) == 0, 'channel numbers should be power of 2'

	chip_level_count_entry = chip_level_counts[chip_orgs.index(chip_name)]

	# assign n_sa
	chip_level_count_entry[chip_levels.index("SubArray")] = n_sa 

	temp = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Bank")] * n_sa * chip_level_count_entry[chip_levels.index("Column")];
	row_num = chip_level_count_entry[chip_levels.index("Row")] = chip_sizes[chip_orgs.index(chip_name)] * (1<<20) / temp;
	# print('temp: %d, row_num: %d' % (temp, row_num))
	
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

# assign payload blocks to 
def assignDBBlocksToSAs(n_db_records, db_record_bit_length_predicate, db_record_bit_length_payload, chip_level_count_entry):


	# predicate bits laid out vertically
	print('In assignDBBlocksToSAs() -> predicate block stats ... ')
	n_predicate_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block
	print('In assignDBBlocksToSAs() -> n_predicate_records_per_block: %d' % n_predicate_records_per_block)
	total_n_predicate_blocks = math.ceil(n_db_records / n_predicate_records_per_block)
	print('In assignDBBlocksToSAs() -> total_n_predicate_blocks: %d' % total_n_predicate_blocks)


	print('In assignDBBlocksToSAs() -> payload block stats ... ')
	total_n_payload_blocks = total_n_predicate_blocks
	print('In assignDBBlocksToSAs() -> total_n_payload_blocks: %d' % total_n_payload_blocks)
	# num_payload_per_row = math.floor(n_predicate_records_per_block / db_record_bit_length_payload) # row_width / payload_bit_length
	# print('In assignDBBlocksToSAs() -> num_payload_per_row: %d' % num_payload_per_row) # DEBUG 630 rec / row
	# num_rows_for_payload_block = math.ceil(n_predicate_records_per_block / num_payload_per_row)
	# print('In assignDBBlocksToSAs() -> num_rows_for_payload_block: %d' % num_rows_for_payload_block) # DEBUG 106 rows / payload_block
	# num_payload_db_block_per_sa = math.floor(chip_level_count_entry[chip_levels.index("Row")] / num_rows_for_payload_block)
	# print('In assignDBBlocksToSAs() -> num_payload_db_block_per_sa: %d' % num_payload_db_block_per_sa)

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

	i = 0
	payloadToSAMapping = {}
	while i <= total_n_payload_blocks:
		for SA in subArray_lst:
			for BA in bank_lst:
				for RA in rank_lst:
					for CH in chan_lst:
						if i == total_n_payload_blocks:
							return payloadToSAMapping
						payloadToSAMapping[i] = [CH,RA,BA,SA]
						i += 1



def payloadAddr(payload_block_to_sa_map, db_record_bit_length_payload, db_record):
	# predicate bits laid out vertically
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block. Assume chip-level parallelism
	total_n_predicate_blocks = math.ceil(n_db_records / n_records_per_block)

	# payload bits laid out horizontally: row_width / payload_bit_length
	num_payload_per_row = math.floor(n_records_per_block / db_record_bit_length_payload)
	# print('In payloadToRow() -> num_payload_per_row: %d' % num_payload_per_row) # DEBUG XXX rec / row
	num_rows_for_payload_block = math.ceil(n_records_per_block / num_payload_per_row)
	# print('In payloadToRow() -> num_rows_for_payload_block: %d' % num_rows_for_payload_block) # DEBUG XXX rows 

	# determine which payload_block this db_record is in
	payload_block_id = int(db_record / n_records_per_block) 
	sa_addr = payload_block_to_sa_map[payload_block_id] # [CH,RA,BA,SA]
	payload_row = int((db_record - n_records_per_block * payload_block_id) / num_payload_per_row)

	row_bits_len = (int)(math.log2(chip_level_count_entry[chip_levels.index("Row")]))
	row_fmt_str = '0' + str(row_bits_len) + 'b'
	row_bits = format(payload_row, row_fmt_str)

	# print('payload_row: %d, row_bits: %s' % (payload_row, row_bits)) # debug
	temp = sa_addr.copy()
	temp.append(str(int(row_bits)))
	return temp

# read csv file containing survival DB records after passing all predicates
# query = 1 ~ 13
def getPassedRecords(query, n_db_records):

	selectivity = [0.01987482, 0.00070263, 0.00007818, 0.00828539,
	               0.00164805, 0.00020818, 0.03661231, 0.00146022,
	               0.00005513, 0.00000098, 0.01629251, 0.00391048,
	               0.00007772]

	db_records = []
	for i in range(0,n_db_records):
		db_records.append(i)

	return random.sample(db_records, int(selectivity[query-1]*n_db_records))


def trace_gen(chip_level_count_entry, payload_block_to_sa_map, query_index):

	# trace file to store all row activations (DRAM Addresses) to process all predicates for a given DB
	q_s = 'Q' + str(query_index+1)
	output_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_Two/retrievals_only/'
	trace_file = output_prefix + q_s + '_Sieve_Mode_Retrieval.trace'
	
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

	# determine for each address, the number of bits at each DRAM level
	# ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel']
	for level in chip_levels:

		if level != 'Column':
			addr_segment_bits[addr_segment_order.index(level)] = (int)(math.log2(chip_level_count_entry[chip_levels.index(level)]))
		else:
			addr_segment_bits[addr_segment_order.index(level)] = ((int)(math.log2(chip_level_count_entry[chip_levels.index(level)])) - (int)(math.log2(prefetch_size)))

	print('In trace_gen() -> addr_segment_order:', addr_segment_order)	
	print('In trace_gen() -> addr_segment_bits:' , addr_segment_bits)


	##### a series of READ to simulate payload retrieval
	read_count_agg = 0
	read_agg_list = []
	passed_rec_ids_set = getPassedRecords(query_index+1, n_db_records)
	print('len(passed_rec_ids_set): %d' % len(passed_rec_ids_set)) # DEBUG

	# calculate total num of rows to activate for retrieval
	total_n_cols_retrieval = len(payload_attribute_names[query_index])
	print('In trace_gen() -> num_col_activation per DB block (to aggregate): %d' % total_n_cols_retrieval)

	unique_rows = set()
	for rec_id in passed_rec_ids_set:
		payload_addr = payloadAddr(payload_block_to_sa_map, db_record_bit_length_payload, rec_id) # [CH,RA,BA,SA, ROW]
		unique_rows.add(tuple(payload_addr))

	print("len(unique_rows): %d" % len(unique_rows))


	# for rec_id in passed_rec_ids_set:
	# 	payload_addr = payloadAddr(payload_block_to_sa_map, db_record_bit_length_payload, rec_id) # [CH,RA,BA,SA, ROW]
	# 	addr_seg_map = {
	# 		'Channel': payload_addr[0],
	# 		'Rank': payload_addr[1],
	# 		'Bank': payload_addr[2],
	# 		'SubArray': payload_addr[3],
	# 		'Row': payload_addr[4]
	# 	}

	# 	for c in range(total_n_cols_retrieval):
	# 		column = str(random.choice(column_lst))
	# 		addr_temp = ''
	# 		for seg in addr_segment_order:
	# 			if seg == 'Column':
	# 				addr_temp += column
	# 			else:
	# 				addr_temp += addr_seg_map[seg]

	# 		for tx in range(tx_bit_length):
	# 			addr_temp += '0'

	# 		# fo.write(hex(int(addr_temp,2))+" R"+"\n")
	# 		read_agg_list.append(hex(int(addr_temp,2))+" R"+"\n")
	# 		read_count_agg += 1

	# print('In trace_gen() -> Total number of READ for retrieval: %d' % len(read_agg_list))

	########### save to file ###########

	# random.shuffle(read_agg_list) # do not shuffle

	# for cmd in read_agg_list:
	# 	fo.write(cmd)

	# fo.close()






### variables that we can manipulate ###
payload_attribute_names = [
['lo_extendedprice','lo_discount'],
['lo_extendedprice','lo_discount'],
['lo_extendedprice','lo_discount'],
['lo_revenue','d_year','p_brand1'],
['lo_revenue','d_year','p_brand1'],
['lo_revenue','d_year','p_brand1'],
['c_nation','s_nation','d_year','lo_revenue'],
['c_city','s_city','d_year','lo_revenue'],
['c_city','s_city','d_year','lo_revenue'],
['c_city','s_city','d_year','lo_revenue'],
['d_year','c_nation','lo_revenue','lo_supplycost'],
['d_year','s_nation','p_category','lo_revenue','lo_supplycost'],
['d_year','s_city','p_brand1','lo_revenue','lo_supplycost']]

# lo_extendedprice: 21 --> sf_100
# lo_discount:      4
# lo_revenue:       22
# d_year:           3
# p_brand1:         10
# c_nation:         5
# s_nation:         5
# c_city:           8
# s_city:           8
# lo_supplycost:    14
# p_category:       5
# total:            105 --> sf_100


# Ranged predicates are repeated twice: one for >=, and one for <=
predicate_attribute_repeats = [[1, 2, 1], [1,2,2], [1,1,2,2],
					 [1,1], [2,1], [1,1],
					 [1,1,2], [1,1,2], [2,2,2],
					 [2,1,2], [1,1,2], [1,1,2,2],
					 [1,1,2,1]]

# chip_name = 'SALP_4Gb_x16'
chip_name = 'SALP_8Gb_x8'

n_sa = 4
n_chan = 8
n_rank = 2

# n_db_records = 6001171 # SF_1
# n_db_records = 59986217 # SF_10
n_db_records = 600038146 # SF_100

db_record_bit_length_predicate = 118
db_record_bit_length_payload = 105 # 104-bit per DB payload


print('n_db_records: %d' % n_db_records)
print('db_record_bit_length_payload: %d' % db_record_bit_length_payload)

print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
chip_level_count_entry = complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa)
total_n_sa = int(chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")])
print('Total number of SubArrays (gang of chips): %d' % total_n_sa)
print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
print('Chip_levels: ', chip_levels)
print('Chip_level_count: ', chip_level_count_entry)
print('')


payload_block_to_sa_map = assignDBBlocksToSAs(n_db_records, db_record_bit_length_predicate, db_record_bit_length_payload, chip_level_count_entry)

# for i in range(0,13):
# 	trace_gen(chip_level_count_entry, payload_block_to_sa_map, i)
# 	print('')


# short_list = [1, 3, 4, 5, 6, 7, 10, 11]
short_list = [0, 2, 8, 9, 12]
for i in short_list:
	trace_gen(chip_level_count_entry, payload_block_to_sa_map, i)
	print('')

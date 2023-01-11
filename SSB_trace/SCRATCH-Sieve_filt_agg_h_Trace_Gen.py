#!/home/lw2ef/anaconda3/bin/python3 python3

# first, activate rows to filter, then activate to retrieve records
# the number of rows to activate for filtering depends on predicate total bit length
# the number of rows to activate for records retrieval dependes on requested attributes total bit length

import math
import random
import os

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
                     [0, 0, 8, 0, 0, 1<<12], [0, 0, 8, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10]]

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

# map each predicate DB block to a SubArray
def predicateBlockToSAMapping(n_db_records, db_record_bit_length_predicate, chip_level_count_entry, salp):
	# DB records are laid out vertically just like k-mers in Sieve. Each block contains roughly (n_col * col_width) records. 
	# Each subarray can contain multiple blocks, as long as each record bit length < subarray row number
	assert db_record_bit_length_predicate <= chip_level_count_entry[chip_levels.index("Row")], 'predicate pattern bit length exceeds number of rows in a SubArray'
	
	# predicate bits laid out vertically
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block
	print('In PredicateBlockToSAMapping() -> n_records_per_block: %d' % n_records_per_block)
	
	total_n_predicate_blocks = math.ceil(n_db_records / n_records_per_block)
	print('In predicateBlockToSAMapping() -> total_n_predicate_blocks: %d' % total_n_predicate_blocks)

	num_predicate_db_block_per_sa = math.floor(chip_level_count_entry[chip_levels.index("Row")] / db_record_bit_length_predicate)
	print('In predicateBlockToSAMapping() -> num_predicate_db_block_per_sa: %d' % num_predicate_db_block_per_sa)
	
	

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
	# print("subArray_bits_len: "+str(subArray_bits_len))

	### convert subArray list to subArray group list, odd number SubArrays for predicate filtering, even number SubArrays for selection
	index = 0
	predicate_subArray_lst = []
	payload_subArray_lst = []
	for index in range(0, num_subArrays):
		if index % 2 == 0:
			predicate_subArray_lst.append(subArray_lst[index])
		else:
			payload_subArray_lst.append(subArray_lst[index])
	# print("predicate_subArray_lst: ", predicate_subArray_lst) # DEBUG
	# print('payload_subArray_lst: ', payload_subArray_lst) # DEBUG

	# first, try to distribute all predicate DB blocks to smart SAs
	# total_smart_db_block = math.floor(num_db_block_per_sa * salp * chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")])
	# if total_smart_db_block < total_n_blocks:


	# # Group a pair of subarrays into one SubArray Group, which consists of a SA for predicate filtering, and a SA for attribute aggregation
	# predicateBlockToSAMap = {} # {(DB_block_0, [ch_0, ra_0, ba_0, sa_0]), (DB_block_1, [ch_1, ra_0, ba_0, sa_0]), 
	# i = 0
	# while i < total_n_predicate_blocks:
	# 	for SA in subArray_lst:
	# 		for BA in bank_lst:
	# 			for RA in rank_lst:
	# 				for CH in chan_lst:
	# 					if i == total_n_predicate_blocks:
	# 						return predicateBlockToSAMap
	# 					predicateBlockToSAMap[i] = [CH,RA,BA,SA]
	# 					i += 1


def distributeToSmartSA(total_smart_db_block, chan_lst, rank_lst, bank_lst, subArray_lst, smart_sa_index):
	DBBlockToSmartSaMapping = {}	
	smart_sa_list = []
	for sa_idx in smart_sa_index:
		smart_sa_list.append(subArray_lst[sa_idx])
	print("num smart SAs per bank: %d" % len(smart_sa_list))
	# print(smart_sa_list) # debug

	i = 0
	while i <= total_smart_db_block:
		for SA in smart_sa_list:
			for BA in bank_lst:
				for RA in rank_lst:
					for CH in chan_lst:
						if i == total_smart_db_block:
							return DBBlockToSmartSaMapping
						DBBlockToSmartSaMapping[i] = [CH,RA,BA,SA]
						i += 1

# there might be different ways to store payload. For SF_1, there are a number of SubArray groups, each SA_group has a predicate SubArray, and a payload SubArrays
# each predicate SubArray stores a db_block of predicate, in the default case, DB_block stores predicate index for 65536 db records
# that means, we need to store 65536 DB payloads in the payload SubArrays of a SA_group

def payloadBlockToSAMapping(n_db_records, db_record_bit_length_payload, chip_level_count_entry, salp):

	# predicate bits laid out vertically
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block. Assume chip-level parallelism
	total_n_predicate_blocks = math.ceil(n_db_records / n_records_per_block)

	# payload bits laid out horizontally
	num_payload_per_row = math.floor(n_records_per_block / db_record_bit_length_payload)
	print('In payloadBlockToSAMapping() -> num_payload_per_row: %d' % num_payload_per_row) # DEBUG 630 rec / row
	num_rows_for_payload_block = math.ceil(n_records_per_block / num_payload_per_row)
	print('In payloadBlockToSAMapping() -> num_rows_for_payload_block: %d' % num_rows_for_payload_block) # DEBUG 105 rows / 4096 sa rows

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
	# print("subArray_bits_len: "+str(subArray_bits_len))

	### convert subArray list to subArray group list, odd number SubArrays for predicate filtering, even number SubArrays for selection
	index = 0
	predicate_subArray_lst = []
	payload_subArray_lst = []
	for index in range(0, num_subArrays):
		if index % 2 != 0:
			payload_subArray_lst.append(subArray_lst[index])

	# make sure a predicate_block worth of payload (65536) can fit within one subarray
	if num_rows_for_payload_block <= chip_level_count_entry[chip_levels.index("Row")]: # 4096

		### build subArray list
		num_subArrays = chip_level_count_entry[chip_levels.index("SubArray")]
		subArray_lst = []
		subArray_bits_len = (int)(math.log2(num_subArrays))
		if subArray_bits_len > 0:
			for c in range((int)(num_subArrays)):
				subArray_fmt_str = '0' + str(subArray_bits_len) + 'b'
				b = format(c, subArray_fmt_str)
				subArray_lst.append(b)

		### convert subArray list to subArray group list, odd number SubArrays for predicate filtering, even number SubArrays for selection
		index = 0
		payload_subArray_lst = []
		for index in range(0, num_subArrays):
			if index % 2 != 0:
				payload_subArray_lst.append(subArray_lst[index])

		# payloadBlockToSAMap = {} # {(DB_block_0, [ch_0, ra_0, ba_0, sa_0]), (DB_block_1, [ch_1, ra_0, ba_0, sa_0]), 
		# i = 0
		# while i < total_n_predicate_blocks:
		# 	for SA in payload_subArray_lst:
		# 		for BA in bank_lst:
		# 			for RA in rank_lst:
		# 				for CH in chan_lst:
		# 					if i == total_n_predicate_blocks:
		# 					# if i == 1: # DEBUG
		# 						return payloadBlockToSAMap
		# 					payloadBlockToSAMap[i] = [CH,RA,BA,SA]
		# 					i += 1
		payloadBlockToSAMap = {} # {(DB_block_0, [ch_0, ra_0, ba_0, sa_0]), (DB_block_1, [ch_1, ra_0, ba_0, sa_0]), 
		i = 0
		while i < total_n_predicate_blocks:
			for SA in subArray_lst:
				for BA in bank_lst:
					for RA in rank_lst:
						for CH in chan_lst:
							if i == total_n_predicate_blocks:
							# if i == 1: # DEBUG
								return payloadBlockToSAMap
							payloadBlockToSAMap[i] = [CH,RA,BA,SA]
							i += 1






# read csv file containing survival DB records after passing all predicates
# query = 1 ~ 13
def getPassedRecords(query, n_db_records):

	pased_rec_ids_set = set()
	for i in range(n_db_records):
		rec_id = i-1
		pased_rec_ids_set.add(rec_id)

	query_s = 'Q' + str(query)
	predicate_list = predicate_attribute_names[query-1]

	for predicate in predicate_list:
		prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/'
		csv_file = prefix + 'csv/' + query_s + '/' + query_s + '_' + predicate

		passed_rec_ids = set()
		with open(csv_file, "r") as f:
			for _ in range(1): # skip first line
				next(f)
			for line in f:
				rec_id = int(line)
				rec_id -= 1
				passed_rec_ids.add(rec_id)
		f.close()

		# print("predicate: %s, selectivity: %.2f" % (predicate, 100*float(len(passed_rec_ids)/n_db_records))) # DEBUG

		pased_rec_ids_set = pased_rec_ids_set.intersection(passed_rec_ids)

	# print("final_passed_rec_ids: %d" % len(pased_rec_ids_set)) # DEBUG

	return pased_rec_ids_set

def payloadAddr(payload_block_to_sa_map, db_record_bit_length_payload, db_record):
	# predicate bits laid out vertically
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block. Assume chip-level parallelism
	total_n_predicate_blocks = math.ceil(n_db_records / n_records_per_block)

	# payload bits laid out horizontally
	num_payload_per_row = math.floor(n_records_per_block / db_record_bit_length_payload)
	# print('In payloadToRow() -> num_payload_per_row: %d' % num_payload_per_row) # DEBUG 630 rec / row
	num_rows_for_payload_block = math.ceil(n_records_per_block / num_payload_per_row)
	# print('In payloadToRow() -> num_rows_for_payload_block: %d' % num_rows_for_payload_block) # DEBUG 105 rows / 4096 sa rows

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





# query_index = 0 ~ 12
def trace_gen(chip_level_count_entry, predicate_block_to_sa_map, payload_block_to_sa_map, query_index, salp):

	# trace file to store all row activations (DRAM Addresses) to process all predicates for a given DB
	q_s = 'Q' + str(query_index+1)
	output_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_agg_and_filt_h/'
	trace_file = output_prefix + q_s + '_Sieve_agg_and_filt_h.trace'
	
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

	# calculate total num of rows to activate for filtering
	total_n_rows_filter = 0
	for i in range(len(predicate_attribute_bit_length[query_index])):
		total_n_rows_filter += predicate_attribute_bit_length[query_index][i] * predicate_attribute_repeats[query_index][i]
	print('In trace_gen() -> num_row_activation per DB block (to filter): %d' % total_n_rows_filter)

	# # Generate a series of WRITE commands to simulate inputing predicate queries
	write_count = 0
	write_filt_lst = []
	# consider one write per pattern group. select a random column per pattern group
	for DB_block in predicate_block_to_sa_map: # (predicate_block_0, [ch_0, ra_0, ba_0, sa_0])
		# print(predicate_block_to_sa_map[DB_block]) # ['0', '0', '000', '000000']

		addr_seg_map = {
			'Channel': predicate_block_to_sa_map[DB_block][0],
			'Rank': predicate_block_to_sa_map[DB_block][1],
			'Bank': predicate_block_to_sa_map[DB_block][2],
			'SubArray': predicate_block_to_sa_map[DB_block][3],
			# 'Column': str(random.choice(column_lst))
		}

		# addr_segment_order = ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel']
		# randomly select rows to activate
		for r in range(total_n_rows_filter):
			Row = str(random.choice(row_lst))
			# print('Row: %s, len: %d' % (Row,len(Row)))
			for c in range(int((chip_col_widths[chip_orgs.index(chip_name)]*chip_level_count_entry[chip_levels.index("Column")]/512))):
				addr_seg_map['Column'] = str(random.choice(column_lst))
				addr_temp = ''
				for seg in addr_segment_order:
					if seg == 'Row':
						addr_temp += Row
						# print('%s, %s' % (seg, Row)) # DEBUG
					else:
						addr_temp += addr_seg_map[seg]
						# print('%s, %s' % (seg, addr_seg_map[seg])) # DEBUG
						# print('%s: %s, len: %d' % (seg,addr_seg_map[seg],len(addr_seg_map[seg])))

				for tx in range(tx_bit_length):
					addr_temp += '0'

				# print('addr: %s, len: %d' % (addr_temp,len(addr_temp))) # DEBUG PRINT
				# fo.write(hex(int(addr_temp,2))+" W"+"\n") 
				write_filt_lst.append(hex(int(addr_temp,2))+" W"+"\n")
				write_count += 1

	print('In trace_gen() -> Total number of write commands to setup queries: %d' % write_count)

	read_count_filt = 0
	read_filt_lst = []
	total_LISA_hop = 0
	# Generate a series of READ commands to simulate predicate processing
	# 	checking every DB_block
	# 	for each DB_block, activate a set of rows in its subarray 
	for DB_block in predicate_block_to_sa_map: # (predicate_block_0, [ch_0, ra_0, ba_0, sa_0])
		# print(predicate_block_to_sa_map[DB_block]) # ['0', '0', '000', '000000']

		addr_seg_map = {
			'Channel': predicate_block_to_sa_map[DB_block][0],
			'Rank': predicate_block_to_sa_map[DB_block][1],
			'Bank': predicate_block_to_sa_map[DB_block][2],
			'SubArray': predicate_block_to_sa_map[DB_block][3],
			'Column': str(random.choice(column_lst))
		}




		# addr_segment_order = ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel']
		# randomly select rows to activate
		for r in range(total_n_rows_filter):
			Row = str(random.choice(row_lst))
			# print('Row: %s, len: %d' % (Row,len(Row)))
			addr_temp = ''
			for seg in addr_segment_order:
				if seg == 'Row':
					addr_temp += Row
					# print('%s, %s' % (seg, Row)) # DEBUG
				else:
					addr_temp += addr_seg_map[seg]
					# print('%s, %s' % (seg, addr_seg_map[seg])) # DEBUG
					# print('%s: %s, len: %d' % (seg,addr_seg_map[seg],len(addr_seg_map[seg])))

			for tx in range(tx_bit_length):
				addr_temp += '0'

			# print('addr: %s, len: %d' % (addr_temp,len(addr_temp))) # DEBUG PRINT
			# fo.write(hex(int(addr_temp,2))+" R"+"\n")
			read_filt_lst.append(hex(int(addr_temp,2))+" R"+"\n")
			read_count_filt += 1

	print('In trace_gen() -> Total number of read commands for filter: %d' % read_count_filt)


	# aggregate on record payload that passed the predicate
	read_count_agg = 0
	read_agg_list = []
	pased_rec_ids_set = getPassedRecords(query_index+1, n_db_records)
	print('len(pased_rec_ids_set): %d' % len(pased_rec_ids_set)) # DEBUG

	for rec_id in pased_rec_ids_set:
		payload_addr = payloadAddr(payload_block_to_sa_map, db_record_bit_length_payload, rec_id) # [CH,RA,BA,SA, ROW]

		addr_seg_map = {
			'Channel': payload_addr[0],
			'Rank': payload_addr[1],
			'Bank': payload_addr[2],
			'SubArray': payload_addr[3],
			'Row': payload_addr[4],
			'Column': str(random.choice(column_lst))
		}

		addr_temp = ''
		for seg in addr_segment_order:
			addr_temp += addr_seg_map[seg]

		for tx in range(tx_bit_length):
			addr_temp += '0'

		# fo.write(hex(int(addr_temp,2))+" R"+"\n")
		read_agg_list.append(hex(int(addr_temp,2))+" R"+"\n")
		read_count_agg += 1


	print('In trace_gen() -> Total number of read commands for aggregation: %d' % read_count_agg)

	fo.close()



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


payload_attribute_bit_length = [[], [], [],
						[], [], [],
						[], [], [],
						[], [], [],
						[]]

predicate_attribute_names = [['d_year','lo_discount','lo_quantity'], ['d_yearmonthnum','lo_discount','lo_quantity'], ['d_weeknuminyear','d_year','lo_discount','lo_quantity'],
				   ['p_category','s_region'], ['p_brand1','s_region'], ['p_brand1','s_region'], 
				   ['c_region','s_region','d_year'], ['c_nation','s_nation','d_year'], ['c_city','s_city','d_year'],
				   ['c_city','d_yearmonth','s_city'], ['c_region','s_region','p_mfgr'], ['c_region','s_region','d_year','p_mfgr'],
				   ['c_region','s_nation','d_year','p_category']]

predicate_attribute_bit_length = [[3,11,6], [7,11,6], [6,3,11,6],
						[5,3], [10,3], [10,3],
						[3,3,3], [5,5,3], [8,8,3],
						[8,8,7], [3,3,3], [3,3,3,3],
						[3,5,3,5]]

# Ranged predicates are repeated twice: one for >=, and one for <=
predicate_attribute_repeats = [[1, 2, 1], [1,2,2], [1,1,2,2],
					 [1,1], [2,1], [1,1],
					 [1,1,2], [1,1,2], [2,2,2],
					 [2,1,2], [1,1,2], [1,1,2,2],
					 [1,1,2,1]]

# chip_name = 'SALP_4Gb_x16'
chip_name = 'SALP_8Gb_x8'

n_sa = 128
n_chan = 8
n_rank = 2

salp = 1

# n_db_records = 6001171 # SF_1
# n_db_records = 59986217 # SF_10
n_db_records = 600038146 # SF_100


db_record_bit_length_predicate = 118 # 118-bit per DB predicate for SF_100 --> Only predicate attributes
db_record_bit_length_payload = 105 # 104-bit per DB payload


print('n_db_records: %d' % n_db_records)
print('db_record_bit_length_predicate: %d' % db_record_bit_length_predicate)
print('db_record_bit_length_payload: %d' % db_record_bit_length_payload)

print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
chip_level_count_entry = complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa)
total_n_sa = int(chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")])
print('Total number of SubArrays (gang of chips): %d' % total_n_sa)
print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
print('Chip_levels: ', chip_levels)
print('Chip_level_count: ', chip_level_count_entry)
print('')

print('Assigning predicate and payload DB blocks to Subarrays ... ')
# predicate_block_to_sa_map = predicateBlockToSAMapping(n_db_records, db_record_bit_length_predicate, chip_level_count_entry)
predicate_block_to_sa_map = predicateBlockToSAMapping(n_db_records, db_record_bit_length_predicate, chip_level_count_entry)
# print(predicate_block_to_sa_map)
# payload_block_to_sa_map = payloadBlockToSAMapping(n_db_records, db_record_bit_length_payload, chip_level_count_entry)
payload_block_to_sa_map = payloadBlockToSAMapping(n_db_records, db_record_bit_length_payload, chip_level_count_entry)
# print(payload_db_block_to_sa_map)

# addr = payloadAddr(payload_block_to_sa_map, db_record_bit_length_payload, 65535) # DEBUG
# print(addr) # DEBUG


print('Total number of predicate DB blocks: %d' % len(predicate_block_to_sa_map))
print('Total number of payload DB blocks: %d' % len(payload_block_to_sa_map))
print('')


# for i in range(0, 13):

# 	print('')
# 	print('Predicate attributes: ', predicate_attribute_names[i])
# 	# print('Attribute bit lengths: ', predicate_attribute_names[i])
# 	# print('Repeat for each attribute: ', predicate_attribute_names[i])
# 	# print('')

# 	q_s = 'Q' + str(i+1)
# 	print('%s Sieve trace gen filter and aggregation HORIZONTAL ... ' % q_s)
# 	# trace_gen(chip_level_count_entry, predicate_block_to_sa_map, payload_block_to_sa_map, i)
# 	trace_gen(chip_level_count_entry, predicate_block_to_sa_map, payload_block_to_sa_map, i, salp)

# 	# pased_rec_ids_set = getPassedRecords(i+1, n_db_records) # DEBUG
# 	# print(pased_rec_ids_set) # DEBUG
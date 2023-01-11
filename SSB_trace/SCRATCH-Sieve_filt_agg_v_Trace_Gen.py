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

# map each DB block to a SubArray
def DBBlockToSAMapping(n_db_records, db_record_bit_length, chip_level_count_entry):
	assert db_record_bit_length <= chip_level_count_entry[chip_levels.index("Row")], 'pattern bit length exceeds number of rows in a SubArray'
	
	# DB records are laid out vertically just like k-mers in Sieve. Each block contains roughly (n_col * col_width) records. 
	# Each subarray can contain multiple blocks, as long as each record bit length < subarray row number
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block. Assume chip-level parallelism
	print('In DBBlockToSAMapping() -> n_records_per_block: %d' % n_records_per_block)

	# total_n_SAs = chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")] * (int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]))
	total_n_blocks = math.ceil(n_db_records / n_records_per_block)
	print('In DBBlockToSAMapping() -> Total number of DB blocks: %d' % total_n_blocks)
	total_n_sa = int(chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")])
	print('In DBBlockToSAMapping() -> Total number of SubArrays (gang of chips): %d' % total_n_sa)
	max_db_block_per_SA = math.floor(chip_level_count_entry[chip_levels.index("Row")] / db_record_bit_length)

	assert total_n_blocks <= total_n_sa * max_db_block_per_SA, 'too many DB blocks, consider increasing the channel or rank nuumbers'
	
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


	# build up a map of {DB_block,[ch, ra, ba, sa]} 
	# distribute records as evenly as possible. Round-Robin. No ETM
	DBBlockToSAMap = {} # {(DB_block_0, [ch_0, ra_0, ba_0, sa_0]), (DB_block_1, [ch_1, ra_0, ba_0, sa_0]), 
	i = 0
	while i < total_n_blocks:
		for SA in subArray_lst:
			for BA in bank_lst:
				for RA in rank_lst:
					for CH in chan_lst:
						if i == total_n_blocks:
						# if i == 1: # DEBUG
							return DBBlockToSAMap
						DBBlockToSAMap[i] = [CH,RA,BA,SA]
						i += 1

# read csv file containing survival DB records after passing all predicates
# query = 1 ~ 13
def getPassedRecords(query):

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

# given a DB record ID, return the DB block it's in
def DBRecordToDBBlockMapping(chip_level_count_entry, db_block_to_sa_map, db_record):
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block. Assume chip-level parallelism
	db_block_index = int(db_record / n_records_per_block)
	# print(db_block_to_sa_map[db_block_index]) # DEBUG
	return db_block_to_sa_map[db_block_index] # [CH,RA,BA,SA]

def trace_gen(chip_level_count_entry, db_block_to_sa_map, query_index):

	# trace file to store all row activations (DRAM Addresses) to process all predicates for a given DB
	q_s = 'Q' + str(query_index+1)
	output_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_agg_and_filt_v/'
	trace_file = output_prefix + q_s + '_Sieve_agg_and_filt_v.trace'
	
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

	# consider one write per pattern group. select a random column per pattern group
	for DB_block in db_block_to_sa_map: # (DB_block_0, [ch_0, ra_0, ba_0, sa_0])
		# print(db_block_to_sa_map[DB_block]) # ['0', '0', '000', '000000']

		addr_seg_map = {
			'Channel': db_block_to_sa_map[DB_block][0],
			'Rank': db_block_to_sa_map[DB_block][1],
			'Bank': db_block_to_sa_map[DB_block][2],
			'SubArray': db_block_to_sa_map[DB_block][3],
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
				fo.write(hex(int(addr_temp,2))+" W"+"\n") 
				write_count += 1

	print('In trace_gen() -> Total number of write commands to setup queries: %d' % write_count)

	read_count_filt = 0
	# Generate a series of READ commands to simulate predicate processing
	# 	checking every DB_block
	# 	for each DB_block, activate a set of rows in its subarray 
	for DB_block in db_block_to_sa_map: # (DB_block_0, [ch_0, ra_0, ba_0, sa_0])
		# print(db_block_to_sa_map[DB_block]) # ['0', '0', '000', '000000']

		addr_seg_map = {
			'Channel': db_block_to_sa_map[DB_block][0],
			'Rank': db_block_to_sa_map[DB_block][1],
			'Bank': db_block_to_sa_map[DB_block][2],
			'SubArray': db_block_to_sa_map[DB_block][3],
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
			fo.write(hex(int(addr_temp,2))+" R"+"\n")
			read_count_filt += 1

	print('In trace_gen() -> Total number of read commands for filter: %d' % read_count_filt)

	read_count_agg = 0
	
	pased_rec_ids_set = getPassedRecords(query_index+1)
	print('len(pased_rec_ids_set): %d' % len(pased_rec_ids_set)) # DEBU
	for rec_id in pased_rec_ids_set:
		sa_addr = DBRecordToDBBlockMapping(chip_level_count_entry, db_block_to_sa_map, rec_id)

		addr_seg_map = {
			'Channel': sa_addr[0],
			'Rank': sa_addr[1],
			'Bank': sa_addr[2],
			'SubArray': sa_addr[3],
			'Column': str(random.choice(column_lst))
		}

		# calculate total num of rows to activate for aggregation for each passed record
		total_n_rows_aggregate = 0
		for i in range(len(payload_attribute_bit_length[query_index])):
			total_n_rows_aggregate += payload_attribute_bit_length[query_index][i]
		# print('In trace_gen() -> num_row_activation per DB block (to aggregate): %d' % total_n_rows_aggregate) # debug

		for r in range(total_n_rows_aggregate):
			Row = str(random.choice(row_lst))
			addr_temp = ''

			for seg in addr_segment_order:
				if seg == 'Row':
					addr_temp += Row
				else:
					addr_temp += addr_seg_map[seg]

			for tx in range(tx_bit_length):
				addr_temp += '0'

			fo.write(hex(int(addr_temp,2))+" R"+"\n")
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

# lo_extendedprice: 20
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
# total:            104

payload_attribute_bit_length = [[20, 4], [20, 4], [20, 4],
						[22, 3, 10], [22, 3, 10], [22, 3, 10],
						[5, 5, 3, 22], [8, 8, 3, 22], [8, 8, 3, 22],
						[8, 8, 3, 22], [3, 5, 22, 14], [3, 5, 5, 22, 14],
						[3, 8, 10, 22, 14]]

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

chip_name = 'SALP_4Gb_x16'

n_sa = 8
n_chan = 8
n_rank = 2

n_db_records = 6001171
# n_db_records = 59986217

# n_db_records = 512 # DEBUG
db_record_bit_length_predicate = 90 # 90-bit per DB predicate
db_record_bit_length_payload = 104 # 104-bit per DB payload
# 

print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
chip_level_count_entry = complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa)
print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
print('Chip_levels: ', chip_levels)
print('Chip_level_count: ', chip_level_count_entry)
print('')

print('Assigning DB blocks to Subarrays ... ')
db_record_bit_length = db_record_bit_length_predicate + db_record_bit_length_payload
db_block_to_sa_map = DBBlockToSAMapping(n_db_records, db_record_bit_length, chip_level_count_entry)
# print(db_block_to_sa_map)
print('Total number of DB blocks: %d' % len(db_block_to_sa_map))
print('')




for i in range(0, 13):

	print('')
	print('Predicate attributes: ', predicate_attribute_names[i])
	# print('Attribute bit lengths: ', predicate_attribute_names[i])
	# print('Repeat for each attribute: ', predicate_attribute_names[i])
	# print('')

	q_s = 'Q' + str(i+1)
	print('%s Sieve trace gen filter and aggregation VERTICAL ... ' % q_s)
	trace_gen(chip_level_count_entry, db_block_to_sa_map, i)

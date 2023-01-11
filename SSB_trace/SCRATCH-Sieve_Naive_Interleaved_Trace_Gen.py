#!/home/lw2ef/anaconda3/bin/python3 python3
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
                     [0, 0, 8, 0, 0, 1<<12], [0, 0, 2, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10]]

addr_segment_order = ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel'] # the specific addr mapping adopted in ramulator
addr_segment_bits = [0, 0, 0, 0, 0, 0] # how many bits to represent each addr segment


def completeChipLevelEntry(chip_name, n_chan, n_rank, n_sa):

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


	# build up a map of {DB_block,[ch, ra, ba, sa]} mapping
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
						

def trace_gen(chip_level_count_entry, db_block_to_sa_map, query_index):

	# trace file to store all row activations (DRAM Addresses) to process all predicates for a given DB
	q_s = 'Q' + str(query_index+1)
	output_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Sieve_Mode_One/'
	trace_file = output_prefix + q_s + '_Sieve_Mode_One.trace'

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


	# calculate total num of rows to activate
	total_n_rows = 0
	for i in range(len(attribute_bit_length[query_index])):
		total_n_rows += attribute_bit_length[query_index][i] * attribute_repeats[query_index][i]
	print('In trace_gen() -> num_row_activation per DB block (to process all attributes): %d' % total_n_rows)

	# interleave READ and WRITE commands, for each row opening, first write to the row, then do a read to simulate 
	# for each DB block, select a series of rows

	write_count = 0
	# read_count = 0
	read_list = []
	write_list = []
	for DB_block in db_block_to_sa_map: # (DB_block_0, [ch_0, ra_0, ba_0, sa_0])
		
		addr_seg_map = {
			'Channel': db_block_to_sa_map[DB_block][0],
			'Rank': db_block_to_sa_map[DB_block][1],
			'Bank': db_block_to_sa_map[DB_block][2],
			'SubArray': db_block_to_sa_map[DB_block][3]
		}

		# addr_segment_order = ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel']
		# randomly select rows to activate

		for r in range(total_n_rows):
			addr_seg_map['Row'] = str(random.choice(row_lst))
			
			# write predicates
			for c in range(int((chip_col_widths[chip_orgs.index(chip_name)]*chip_level_count_entry[chip_levels.index("Column")]/512)*total_n_rows)):
				addr_seg_map['Column'] = str(random.choice(column_lst))
				addr_temp = ''

				for seg in addr_segment_order:
					addr_temp += addr_seg_map[seg]

				for tx in range(tx_bit_length):
					addr_temp += '0'

				write_list.append(hex(int(addr_temp,2))+" R"+"\n")
				write_count += 1

			# # compare predicates
			# addr_seg_map['Column'] = str(random.choice(column_lst))
			# addr_temp = ''
			# for seg in addr_segment_order:
			# 		addr_temp += addr_seg_map[seg]

			# for tx in range(tx_bit_length):
			# 		addr_temp += '0'

			# read_list.append(hex(int(addr_temp,2))+" R"+"\n")


			# read_count += 1

	# random.shuffle(read_list)
	random.shuffle(write_list)
	# for read in read_list:
	# 	fo.write(read)
	for write in write_list:
		fo.write(write)

	fo.close()

	# print('In trace_gen() -> Total number of read commands: %d' % read_count)
	print('In trace_gen() -> Total number of write commands: %d' % write_count)


##### query predicate bit length #####

attribute_names = [['d_year','lo_discount','lo_quantity'], ['d_yearmonthnum','lo_discount','lo_quantity'], ['d_weeknuminyear','d_year','lo_discount','lo_quantity'],
				   ['p_category','s_region'], ['p_brand1','s_region'], ['p_brand1','s_region'], 
				   ['c_region','s_region','d_year'], ['c_nation','s_nation','d_year'], ['c_city','s_city','d_year'],
				   ['c_city','d_yearmonth','s_city'], ['c_region','s_region','p_mfgr'], ['c_region','s_region','d_year','p_mfgr'],
				   ['c_region','s_nation','d_year','p_category']]

attribute_bit_length = [[3,4,6], [7,4,6], [6,3,4,6],
						[5,3], [10,3], [10,3],
						[3,3,3], [5,5,3], [8,8,3],
						[8,7,8], [3,3,3], [3,3,3,3],
						[3,5,3,5]]

# Ranged predicates are repeated twice: one for >=, and one for <=
attribute_repeats = [[1, 2, 1], [1,2,2], [1,1,2,2],
					 [1,1], [2,1], [1,1],
					 [1,1,2], [1,1,2], [2,2,2],
					 [2,1,2], [1,1,2], [1,1,2,2],
					 [1,1,2,1]]

chip_name = 'SALP_4Gb_x16'

n_sa = 64
n_chan = 2
n_rank = 2

# n_db_records = 6001171 # SF_1
n_db_records = 59986217 # SF_10
# n_db_records = 29993108
db_record_bit_length = 90 # 90-bit per DB record


print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
chip_level_count_entry = completeChipLevelEntry(chip_name, n_chan, n_rank, n_sa)
print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
print('Chip_levels: ', chip_levels)
print('Chip_level_count: ', chip_level_count_entry)
print('')

print('Assigning DB blocks to Subarrays ... ')
db_block_to_sa_map = DBBlockToSAMapping(n_db_records, db_record_bit_length, chip_level_count_entry)
# print(db_block_to_sa_map)
print('Total number of DB blocks: %d' % len(db_block_to_sa_map))
print('')

for i in range(3, 4):
	q_s = 'Q' + str(i+1)

	print('Generating traces ...')
	trace_gen(chip_level_count_entry, db_block_to_sa_map, i)








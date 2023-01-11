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


def complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa, salp):

	assert n_sa >=1 and n_sa <= 128 and n_sa & (n_sa-1) == 0, 'n_sa should be power of 2 and between 1 and 128'
	assert n_chan & (n_chan-1) == 0, 'channel numbers should be power of 2'
	assert n_rank & (n_rank-1) == 0, 'channel numbers should be power of 2'

	# modify bank numbers according to salp
	## SALP 1: bank = 2, n_sa = 2 (64 SA) = 8 * 2 * 2 * 2          |0 --- check
	## SALP 2: bank = 4, n_sa = 2 (128 SA) = 8 * 2 * 4 * 2         |1 --- check
	## SALP 4: bank = 8, n_sa = 2 (256 SA) = 8 * 2 * 8 * 2         |2 --- check
	## SALP 8: bank = 16, n_sa = 2 (512 SA) = 8 * 2 * 16 * 2       |3 --- check *
	## SALP 16: bank = 32, n_sa = 2 (1024 SA) = 8 * 2 * 32 * 2     |4 --- check
	## SALP 32: bank = 64, n_sa = 2 (2048 SA) = 8 * 2 * 64 * 2     |5 --- check
	## SALP 64: bank = 128, n_sa = 2 (4096 SA) = 8 * 2 * 128 * 2   |6 --- check
	## SALP 128: bank = 128, n_sa = 4 (8192 SA) = 8 * 2 * 128 * 4  |7 --- check
	salp_bank = {1:2, 2:4, 4:8, 8:16, 16:32, 32:64, 64:128, 128:128}

	chip_level_count_entry = chip_level_counts[chip_orgs.index(chip_name)]

	chip_level_count_entry[chip_levels.index('Bank')] = salp_bank[salp]

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


def trace_gen(n_db_records, db_record_bit_length, chip_level_count_entry,salp):

	# trace file to store all row activations (DRAM Addresses) to process all predicates for a given DB
	output_prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/Fulcrum_Mode_One/alu_only/'
	trace_file = output_prefix + 'salp_' + str(salp) +  '_Fulcrum_Mode_One_alu.trace'
	
	if os.path.exists(trace_file):
		os.remove(trace_file)
		print('Old %s is removed. Generating a new one.' % trace_file)
	fo = open(trace_file, "w")


	# calculate how many records per row
	db_block_row_width = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) 
	num_rec_per_row = int(db_block_row_width / db_record_bit_length)
	print('In distributeDBBlocks() -> num_rec_per_row: %d' % num_rec_per_row)
	
	# calculate how many rows are needed
	total_rec_rows = int(n_db_records / num_rec_per_row)
	print('In distributeDBBlocks() -> total_rec_rows: %d' % total_rec_rows)
	
	# calculate how many subarrays in this SALP setup
	total_n_sa = int(chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")])
	print('In distributeDBBlocks() -> Total number of SubArrays (gang of chips): %d' % total_n_sa)

	# each subarray needs this many rows to store DB records
	rows_per_sa_to_store_db = int(total_rec_rows / total_n_sa)
	print('In distributeDBBlocks() -> rows_per_sa_to_store_db: %d' % rows_per_sa_to_store_db)

	assert rows_per_sa_to_store_db < chip_level_count_entry[chip_levels.index("Row")], 'too few rows per subarray'
	
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

	### the filtering process in Fulcrum is basically walkthrough all SAs and open each SA rows (READ)
	### 1. For each activated row, send 32-bit to ALU, each 32-bit takes ~1 ns, processing one row requires 10 tRC
	### 	we simulate by generating additional 10 READS per fulcrum row access
	### 2. Since two subarrays share one ALU, the true SALP is only half of Sieve (half of effective subarray)

	read_alu_list = []
	num_filter_read = 0
	for SA in subArray_lst:
		for BA in bank_lst:
			for RA in rank_lst:
				for CH in chan_lst:
					for row_index in range(0,rows_per_sa_to_store_db):
						num_filter_read += 1
						
	# we estimate for each row latched into the row buffer, 10 additional reads are generated to simulate ALU time
	
	for i in range(num_filter_read * 10):
		addr_seg_map = {
			'Channel': str(random.choice(chan_lst)),
			'Rank': str(random.choice(rank_lst)),
			'Bank': str(random.choice(bank_lst)),
			'SubArray': str(random.choice(subArray_lst)),
			'Column': str(random.choice(column_lst)),
			'Row': str(random.choice(row_lst))
		}

		addr_temp = ''
		for seg in addr_segment_order:
			addr_temp += addr_seg_map[seg]

		for tx in range(tx_bit_length):
			addr_temp += '0'

		read_alu_list.append(hex(int(addr_temp,2))+" R"+"\n")

	random.shuffle(read_alu_list)

	for read in read_alu_list:
		fo.write(read)

	print('In trace_gen() -> total READS for ALUs: %d' % len(read_alu_list))

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

chip_name = 'SALP_8Gb_x8'

### the SALP in Fulcrum is different from Sieve. Since two ALU shares one 

## SALP 1: bank = 2, n_sa = 2 (64 SA) = 8 * 2 * 2 * 2          |0 --- check
## SALP 2: bank = 4, n_sa = 2 (128 SA) = 8 * 2 * 4 * 2         |1 --- check
## SALP 4: bank = 8, n_sa = 2 (256 SA) = 8 * 2 * 8 * 2         |2 --- check
## SALP 8: bank = 16, n_sa = 2 (512 SA) = 8 * 2 * 16 * 2       |3 --- check 
## SALP 16: bank = 32, n_sa = 2 (1024 SA) = 8 * 2 * 32 * 2     |4 --- check
## SALP 32: bank = 64, n_sa = 2 (2048 SA) = 8 * 2 * 64 * 2     |5 --- check
## SALP 64: bank = 128, n_sa = 2 (4096 SA) = 8 * 2 * 128 * 2   |6 --- check
## SALP 128: bank = 128, n_sa = 4 (8192 SA) = 8 * 2 * 128 * 4  |7 --- check


n_chan = 8
n_rank = 2

db_record_bit_length = 118
n_db_records = 600038146 # SF_100

# salp_list = [1, 2, 4, 8, 16, 32, 64, 128] # to estimate LISA mechanism
salp_list = [1]



for salp in salp_list:
	print('n_db_records: %d' % n_db_records)
	print('db_record_bit_length: %d' % db_record_bit_length)

	if salp == 128:
		n_sa = 4
	else:
		n_sa = 2

	print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
	chip_level_count_entry = complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa, salp)

	print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
	print('Chip_levels: ', chip_levels)
	print('Chip_level_count: ', chip_level_count_entry)
	print('')
	trace_gen(n_db_records, db_record_bit_length, chip_level_count_entry,salp)
	print(' ')


### notes: to simulate salp_128, set 128 banks, 

##### end variables #####

### todo, need to fix the logic to calculate ALU latency
### total chip subarray width 16384 / db_record_bit_length = 138 records per row
### each record contains several attributes (2 ~ 4) for ALU to check, each check is 1 ns
### Q1, 2, 7, 8, 9, 10, 11: 138 / 512 --> 0.81, 
### Q3, 12, 13: 138 / 512 -> 1.07
### Q4, 5, 6: 138 / 512 -> 0.54
### ABOVE code uses 10 reads which are 512 ns
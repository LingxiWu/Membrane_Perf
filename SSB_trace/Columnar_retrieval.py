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
                     [0, 0, 8, 0, 0, 1<<12], [0, 0, 128, 0, 0, 1<<11], [0, 0, 8, 0, 0, 1<<10]]

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
	## SALP 8: bank = 16, n_sa = 2 (512 SA) = 8 * 2 * 16 * 2       |3 --- check 
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


# assign payload blocks to 
def assignDBBlocksToSAs(n_db_records, rec_width, chip_level_count_entry):

	bits_per_row = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) 
	print('DEBUG bits_per_row: %d' % bits_per_row)

	# rec_width lo revenue
	num_attributes_per_row = math.ceil(bits_per_row / rec_width)
	print('DEBUG num_attributes_per_row: %d' % num_attributes_per_row)

	num_rec_per_block = num_attributes_per_row
	print('DEBUG num_rec_per_block: %d' % num_rec_per_block)

	db_block_height = 18
	print('DEBUG db_block_height: %d' % db_block_height)

	total_db_blocks = math.ceil(n_db_records / num_rec_per_block)
	print('DEBUG total_db_blocks:%d' % total_db_blocks)

	total_n_sa = math.ceil(chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")])
	print('DEBUG total_n_sa: %d' % total_n_sa)

	num_db_blocks_per_sa = math.ceil(total_db_blocks / total_n_sa)
	print('DEBUG num_db_blocks_per_sa: %d' % num_db_blocks_per_sa)

	num_db_rows_per_sa = math.ceil(num_db_blocks_per_sa * db_block_height)
	print('DEBUG num_db_rows_per_sa: %d' % num_db_rows_per_sa)

	print('DEBUG num_rows_per_sa: %d' % chip_level_count_entry[chip_levels.index("Row")])
		
	assert chip_level_count_entry[chip_levels.index("Row")] >= num_db_rows_per_sa, 'too few rows per subarray'

	

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
	DBBLockToSAMapping = {}
	while i <= total_db_blocks:
		for SA in subArray_lst:
			for BA in bank_lst:
				for RA in rank_lst:
					for CH in chan_lst:
						if i == total_db_blocks:
							return DBBLockToSAMapping
						DBBLockToSAMapping[i] = [CH,RA,BA,SA]
						i += 1


# read csv file containing survival DB records after passing all predicates
# query = 1 ~ 13
def getPassedRecords(query_index, n_db_records):

	selectivity = [0.01987482, 0.00070263, 0.00007818, 0.00828539,
	               0.00164805, 0.00020818, 0.03661231, 0.00146022,
	               0.00005513, 0.00000098, 0.01629251, 0.00391048,
	               0.00007772]

	db_records = []
	for i in range(0,n_db_records):
		db_records.append(i)

	return random.sample(db_records, int(selectivity[query_index]*n_db_records))


def payloadAddr(db_block_to_sa_mapping, record_width, rec_id, attribute_name): # return [CH,RA,BA,SA, payload_block_id, Row]
	
	bits_per_row = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)])
	total_db_blocks = len(db_block_to_sa_mapping)

	num_attributes_per_row = math.ceil(bits_per_row / record_width)
	num_rec_per_block = num_attributes_per_row # 131072/23 = 5699 records per db_block

	# determine which payload_block this db_record is in
	payload_block_id = int(rec_id / num_rec_per_block) 
	sa_addr = db_block_to_sa_mapping[payload_block_id] # [CH,RA,BA,SA]

	attribute_to_row_mapping = {'lo_extendedprice':0, 'lo_discount':1, 'lo_revenue':2, 'd_year':3, 'p_brand1':4, 'c_nation':5, 's_nation':6, 'c_city':7,
								's_city':8, 'lo_supplycost':9, 'p_category':10, 'd_yearMonthNum':11, 'd_weekNumInYear':12, 'd_yearMonth':13,
								'lo_quantity':14, 'p_mfgr':15, 's_region':16, 'c_region':17}

	payload_row = attribute_to_row_mapping[attribute_name]

	row_bits_len = (int)(math.log2(chip_level_count_entry[chip_levels.index("Row")]))
	row_fmt_str = '0' + str(row_bits_len) + 'b'
	row_bits = format(payload_row, row_fmt_str)

	# print('payload_row: %d, row_bits: %s' % (payload_row, row_bits)) # debug
	temp = sa_addr.copy()
	temp.append(str(payload_block_id))
	temp.append(str(int(row_bits)))
	return temp



def trace_analysis(n_db_records, db_block_to_sa_mapping, record_width, query, payload_attribute_names, salp):

	passed_records = getPassedRecords(query, n_db_records)

	attributes_to_retrieve = payload_attribute_names[query]

	
	unique_rows = set()

	for passed_rec in passed_records:
		for attribute in attributes_to_retrieve:
			addr = payloadAddr(db_block_to_sa_mapping, record_width, passed_rec, attribute)
			unique_rows.add(tuple(addr))


	print('len(passed_records): %d' % len(passed_records))
	print('len(unique_rows): %d' % len(unique_rows))
			








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


chip_name = 'SALP_8Gb_x8'

n_sa = 4
n_chan = 8
n_rank = 2

record_width = 23 # widest row is lo_revenue


n_db_records = 600038146 # SF_100

# salp_list = [1, 2, 4, 8, 16, 32, 64, 128] # to estimate LISA mechanism
salp_list = [1]

salp = 128
chip_level_count_entry = complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa, salp)

db_block_to_sa_mapping = assignDBBlocksToSAs(n_db_records, record_width, chip_level_count_entry)

for query_index in range(0,13):
	print('columnar retrieval analysis for Query: %d' % (query_index+1))
	trace_analysis(n_db_records, db_block_to_sa_mapping, record_width, query_index, payload_attribute_names, salp)


# for query in range(2,3):
# 	print('n_db_records: %d' % n_db_records)

# 	for salp in salp_list:

# 		if salp == 128:
# 			n_sa = 4
# 		else:
# 			n_sa = 2

# 		print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
# 		chip_level_count_entry = complateChipLevelEntry(chip_name, n_chan, n_rank, n_sa, salp)

# 		print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
# 		print('Chip_levels: ', chip_levels)
# 		print('Chip_level_count: ', chip_level_count_entry)

# 		print('')
# 		trace_gen(n_db_records, chip_level_count_entry, query, query_row_opening, salp)
# 		print(' ')

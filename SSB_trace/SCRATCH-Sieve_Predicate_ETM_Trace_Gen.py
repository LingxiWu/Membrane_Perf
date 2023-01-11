#!/home/lw2ef/anaconda3/bin/python3 python3
import math
import random
import os
from itertools import permutations



# distribute DB records to SubArrays --> map record to [chan, rank, bank, SA]
## note we assume chip level parallelism, each SA is col_width * col_num

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

def totalCapacityInByte(chip_name, chip_level_count_entry):
	n_chips = int(channel_width / chip_col_widths[chip_orgs.index(chip_name)])
	
	totalByte = 1
	for lvl in chip_level_count_entry:
		totalByte *= lvl

	totalByte *= n_chips * chip_col_widths[chip_orgs.index(chip_name)] 
	totalByte /= 8
	return int(totalByte)


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


def DBRecordToDBBlockMapping_noCLP(n_db_records, db_record_bit_length, chip_level_count_entry):
	assert db_record_bit_length <= chip_level_count_entry[chip_levels.index("Row")], 'pattern bit length exceeds number of rows in a SubArray'

	max_db_block_per_SA = math.floor(chip_level_count_entry[chip_levels.index("Row")] / db_record_bit_length)
	print('In DBRecordToDBBlockMapping_noCLP() -> max_db_block_per_SA: %d' % max_db_block_per_SA)
	
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block. Assume chip-level parallelism
	print('In DBRecordToDBBlockMapping_noCLP() -> n_records_per_block: %d' % n_records_per_block)

	total_n_blocks = math.ceil(n_db_records / n_records_per_block)
	print('In DBRecordToDBBlockMapping_noCLP() -> Total number of DB blocks: %d' % total_n_blocks)
	total_n_sa = int(chip_level_count_entry[chip_levels.index("Channel")] * chip_level_count_entry[chip_levels.index("Rank")] * chip_level_count_entry[chip_levels.index("Bank")] * chip_level_count_entry[chip_levels.index("SubArray")])
	print('In DBRecordToDBBlockMapping_noCLP() -> Total number of SubArrays: %d' % total_n_sa)

	assert total_n_blocks <= total_n_sa * max_db_block_per_SA, 'too many DB blocks, consider increasing the channel or rank nuumbers'

	# map db records to db blocks
	DBRecord_to_DBBlock_map = {} # {[db_rec_id: db_block_id]}
	for db_record in range(n_db_records):
		DBRecord_to_DBBlock_map[db_record] = int(db_record / n_records_per_block) 

	return DBRecord_to_DBBlock_map


def DBBlockToSAMapping(n_db_records, chip_level_count_entry):
	
	n_records_per_block = chip_col_widths[chip_orgs.index(chip_name)] * chip_level_count_entry[chip_levels.index("Column")] * int(channel_width / chip_col_widths[chip_orgs.index(chip_name)]) # how many DB records fit inside a block. Assume chip-level parallelism
	total_n_blocks = math.ceil(n_db_records / n_records_per_block)

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


# determine which DB_blocks survived after passing this predicate
def passedDBBlocks(passed_rec_ids, DBRecord_to_DBBlock_map):

	# print('len(passed_rec_ids): %d' % len(passed_rec_ids))
	DB_block_to_keep = set()
	for rec in passed_rec_ids:
		DB_block_to_keep.add(DBRecord_to_DBBlock_map[rec])

	# print('len(DB_block_to_keep): %d' % len(DB_block_to_keep)) 
	return DB_block_to_keep

# read csv file containing survival DB records after passing the predicate
def getPassedRecords(query, predicate):
	# open the csv file that has record IDs that pass the predicate
	prefix = '/home/lw2ef/Documents/workspace/DRAM_DB/ramulator-master/SSB_trace/'
	csv_file = prefix + 'csv/' + query + '/' + query + '_' + predicate

	passed_rec_ids = []
	with open(csv_file, "r") as f:
		for _ in range(1): # skip first line
			next(f)
		for line in f:
			rec_id = int(line)
			rec_id -= 1
			passed_rec_ids.append(rec_id)
	f.close()

	return passed_rec_ids



def processQueryPlan(query, attribute_list, DBRecord_to_DBBlock_map, DBBlock_to_SA_map):

	query_id = int(query[1:]) - 1

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

	print('In processQueryPlan() -> addr_segment_order:', addr_segment_order)	
	print('In processQueryPlan() -> addr_segment_bits:' , addr_segment_bits)


	write_count = 0
	read_count = 0
	dram_commands = [] # a series of DRAM commands to simulate processing this query plan (predicate_list)

	# the first round of row activations has to check every DB_block. 
	DB_block_to_check = []
	for i in range(len(DBBlock_to_SA_map)):
		DB_block_to_check.append(i)

	for attribute in attribute_list:
		
		bit_length = attribute_bit_length[query_id][attribute_names[query_id].index(attribute)]
		repeat = attribute_repeats[query_id][attribute_names[query_id].index(attribute)]
		num_row_act = bit_length * repeat

		# for each participant DB_block for that attribute
		for DB_block in DB_block_to_check:
			
			addr_seg_map = {
				'Channel': DBBlock_to_SA_map[DB_block][0],
				'Rank': DBBlock_to_SA_map[DB_block][1],
				'Bank': DBBlock_to_SA_map[DB_block][2],
				'SubArray': DBBlock_to_SA_map[DB_block][3]
			}
		
			for r in range(num_row_act):
				addr_seg_map['Row'] = str(random.choice(row_lst))

				# first setup queries by sending a series of writes. 512 is the pattern group size
				for c in range(int((chip_col_widths[chip_orgs.index(chip_name)]*chip_level_count_entry[chip_levels.index("Column")]/512))):
					addr_seg_map['Column'] = str(random.choice(column_lst))
					addr_temp = ''

					for seg in addr_segment_order: # addr_segment_order = ['Row', 'SubArray', 'Bank', 'Rank', 'Column', 'Channel']
						addr_temp += addr_seg_map[seg]

					for tx in range(tx_bit_length):
						addr_temp += '0'

					dram_commands.append(hex(int(addr_temp,2))+" W"+"\n")
					write_count += 1

				# next, send a row wide activate (READ) to simulate comparison
				addr_seg_map['Column'] = str(random.choice(column_lst))
				addr_temp = ''
				for seg in addr_segment_order:
					addr_temp += addr_seg_map[seg]

				for tx in range(tx_bit_length):
					addr_temp += '0'

				dram_commands.append(hex(int(addr_temp,2))+" R"+"\n")
				read_count += 1

			
		# update DB_block_to_check for the next attribute. take intersection of previous db_blocks_to_check and surviving db blocks of this attributes
		passed_db_records = getPassedRecords(query, attribute)
		print('DEBUG processQueryPlan(): %s len(passed_db_records): %d. Selectivity: %.2f (percent)' % (attribute, len(passed_db_records), 100*float(len(passed_db_records)/n_db_records)))
		# convert list of db_records to list of db_blocks
		passed_db_blocks = passedDBBlocks(passed_db_records,DBRecord_to_DBBlock_map)

		DB_block_to_check = list(set(DB_block_to_check).intersection(passed_db_blocks)) # take the intersection of passed DB block and previous activated DB blocks
		print('attribute: %s, num_passed_db_blocks: %d, num_db_block_to_check_next_round: %d' % (attribute, len(passed_db_blocks), len(DB_block_to_check)))

	print('In processQueryPlan() -> Total number of READ/WRITE commands: %d/%d. Total: %d' % (read_count,write_count,read_count+write_count))

	return dram_commands



def analyzeQueryPlan(query, DBRecord_to_DBBlock_map, DBBlock_to_SA_map):

	etm_status_file = query + '_Predicate_ETM_Status.txt'
	if os.path.exists(etm_status_file):
		os.remove(etm_status_file)
		print('Old %s is removed. Generating a new one.' % etm_status_file)
	fo = open(etm_status_file, "w")
	
	query_id = int(query[1:]) - 1

	naive_plan = attribute_names[query_id] # default predicate order
	naive_trace = processQueryPlan(query, naive_plan, DBRecord_to_DBBlock_map, DBBlock_to_SA_map)

	best_plan = naive_plan
	worst_plan = naive_plan

	best_trace = naive_trace
	worst_trace = naive_trace

	attribute_permutations = list(permutations(attribute_names[query_id]))
	for attributes in attribute_permutations:
		attribute_list = list(attributes)
		if attribute_list == naive_plan:
			continue
		else:
			dram_commands = processQueryPlan(query, attribute_list, DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
			if len(dram_commands) < len(best_trace):
				best_trace = dram_commands
				best_plan = attribute_list
			elif len(dram_commands) > len(worst_trace):
				worst_trace = dram_commands
				worst_plan = attribute_list

	print('=== %s In analyzeQueryPlan() naive_plan: ' % query, naive_plan, ' num DRAM commands: %d' % len(naive_trace))
	print('=== %s In analyzeQueryPlan() best_plan: ' % query, best_plan, ' num DRAM commands: %d' % len(best_trace))
	print('=== %s In analyzeQueryPlan() worst_plan: ' % query, worst_plan, ' num DRAM commands: %d' % len(worst_trace))

	s_naive = query + ' naive_plan: ' + str(naive_plan) + ' num DRAM commands: ' + str(len(naive_trace)) + '\n'
	s_best = query + ' best_plan: ' + str(best_plan) + ' num DRAM commands: ' + str(len(best_trace)) + '\n'
	s_worst = query + ' worst_plan: ' + str(worst_plan) + ' num DRAM commands: ' + str(len(worst_trace)) + '\n'

	fo.write(s_naive)
	fo.write(s_best)
	fo.write(s_worst)
	fo.close()

	# trace = processQueryPlan(query, attribute_list, DBRecord_to_DBBlock_map, DBBlock_to_SA_map)


def executeAllQueries():
	for i in range(0, len(attribute_names)):
		query = 'Q' + str(i+1)
		analyzeQueryPlan(query, DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
	










	

###### some global variables

chip_name = 'SALP_4Gb_x16'

n_sa = 8
n_chan = 2
n_rank = 2

n_db_records = 6001171
# n_db_records = 128 # DEBUG
db_record_bit_length = 90 # 90-bit per DB record

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


print('Chip_Name: %s, #_Chan: %d, #_Rrank: %d, #_Subarray/Bank: %d' % (chip_name, n_chan, n_rank, n_sa))
chip_level_count_entry = completeChipLevelEntry(chip_name, n_chan, n_rank, n_sa)
print('Total capacity (Bytes): %d' % (totalCapacityInByte(chip_name, chip_level_count_entry)))
print('Chip_levels: ', chip_levels)
print('Chip_level_count: ', chip_level_count_entry)
print('')

print('Assigning DB records to DB blocks ... ')
# determine which DB block each DB record would go do
DBRecord_to_DBBlock_map = DBRecordToDBBlockMapping_noCLP(n_db_records, db_record_bit_length, chip_level_count_entry)
print('')

print('Assigning DB blocks to SubArrays Round-Robin')
DBBlock_to_SA_map = DBBlockToSAMapping(n_db_records, chip_level_count_entry)
print('')


# TESTING & DEBUG
# q1_d_year DB_block = 2
# processQueryPlan('Q1', ['lo_quantity','d_year','lo_discount'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map) # 872/27904. Total: 28776
# processQueryPlan('Q1', ['lo_discount','d_year','lo_quantity'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map) # 2312/73984. Total: 76296
# processQueryPlan('Q1', ['d_year','lo_quantity','lo_discount'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map) # 332/10624. Total: 10956
# processQueryPlan('Q1', ['d_year','lo_discount','lo_quantity'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map) # 332/10624. Total: 10956
# processQueryPlan('Q1', ['lo_discount','lo_quantity','d_year'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map) # 2852/91264. Total: 94116
# processQueryPlan('Q1', ['lo_quantity','lo_discount','d_year'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map) # 2852/91264. Total: 94116

# processQueryPlan('Q2', ['d_yearmonthnum','lo_discount','lo_quantity'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q3', ['d_weeknuminyear','d_year','lo_discount','lo_quantity'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q4', ['p_category','s_region'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q5', ['p_brand1','s_region'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q6', ['p_brand1','s_region'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q7', ['c_region','s_region','d_year'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q8', ['c_nation','s_nation','d_year'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q9', ['c_city','s_city','d_year'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q10', ['c_city','d_yearmonth','s_city'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q11', ['c_region','s_region','p_mfgr'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q12', ['c_region','s_region','d_year','p_mfgr'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)
# processQueryPlan('Q13', ['c_region','s_nation','d_year','p_category'], DBRecord_to_DBBlock_map, DBBlock_to_SA_map)

# result = passedDBBlocks([0,1, 2, 3, 78,99289, 82736, 65536, 131072, 6001169, 6001170], DBRecord_to_DBBlock_map)
# print(result)

# analyzeQueryPlan('Q1',DBRecord_to_DBBlock_map, DBBlock_to_SA_map)

executeAllQueries()








import csv
import re
from dateutil.parser import parse
import itertools
from fuzzywuzzy import process
from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd
import gc
import os

# this works on Unix based OS only
#os.system("taskset -p 0xff %d" % os.getpid())

def is_date(string):
	try:
		parse(string)
		return True
	except ValueError:
		return False

def comparator(pair_input):
	if pair_input[0] == pair_input[1][0]:
		return pair_input[1][1]
	# else:
	# 	return None

outfile = open('full_matching_log.csv', 'wt', encoding='utf_8', newline='')
writer = csv.writer(outfile, delimiter='|', quotechar='^')
writer.writerow(['parent_msg', 'path', 'filename', 'fuzzymatched_filename'])

rbcpy24 = open('robocopy_fixed_ev1.txt', 'wt', encoding='utf_8') 
rbcpy26 = open('robocopy_fixed_ev2.txt', 'wt', encoding='utf_8')
missing_files = []

# create multiprocessing pool
if __name__ == '__main__':

	pool = ThreadPool(4)

	with open('full_output.csv', 'rt', encoding='utf_8') as file:
		reader = csv.reader(file, delimiter='|', quotechar='^')
		# headers: ['parent_msg', 'missing_files', 'missing_count']
		for row in reader:
			att_lst = row[1].split(';')
			# this needs to be loaded all into memory in order to be sorted
			for entry in att_lst:
				# split entry into src folder and file
				last_bslash = entry.rfind('\\')
				src_folder = entry[:last_bslash]
				src_file = entry[last_bslash+1:]
				missing_files.append([row[0], src_folder, src_file])

	# fix date issue (if master string begins with a date, find the date in the slave msg and move it to the beginning of the file)
	for row in missing_files:
		fixed_entry = None
		if (is_date(row[0][:10])):
			match = re.search(r'\d{2}-\d{2}-\d{4}', row[2])
			# date can now be moved, but format needs to be fixed -- entry[match.start():match.end() + 1]
			if (match != None):
				fixed_date = parse(row[2][match.start():match.end() + 1]).strftime('%Y-%m-%d')
				fixed_entry = fixed_date +' '+ row[2][:match.start()] + row[2][match.end()+1:]
				if fixed_entry:
					row[2] = fixed_entry


	ev1_lst = [x for x in missing_files if x[1].find('ev1') != -1]
	ev2_lst = [x for x in missing_files if x[1].find('ev2') != -1]

	# uk101b24
	chunksize = 10 ** 7
	df1 = pd.read_csv('path\\to\\ev1\\file_listing.csv', delimiter=',', quotechar = '"')
	df1_size = len(df1)
	df1_it = 0
	#    process(chunk)
	for listing in ev1_lst:
		df1_it += 1
		scorelist = []
		result = None
		score = None
		paramlist24 = list(itertools.product([listing[1][listing[1].find('ev1\\root_folder'):] + '\\'], [(x.Path[x.Path.find('ev1\\root_folder'):], x.Filename) for x in df1.itertuples()]))
		#print(paramlist)

		mp_result = pool.map(comparator, paramlist24)
		#print([x for x in mp_result if x is not None])

		if mp_result:
			result, score = process.extractOne(listing[2], mp_result)
			#print(result)

		if listing[2]:
			print('Original string: ' + listing[2])
		if result:
			print('Fuzzymatched string: ' + result)
		if score:
			print('Score: ' + str(score))

		produced_result = result if result is not None else listing[2]
		rbcpy24.write('robocopy "' + listing[1] + '" "<ROOTPATH>'+ listing[1][listing[1].find('ev1\\root_folder'):] +'" "'+ produced_result +'" /r:3 /w:3 /xo /tee /e /np /log+:"<logdestination>\\log.txt" \n')
		# log
		writer.writerow([listing[0], listing[1], listing[2], produced_result])
		print('Processed: {0:.2f}%'.format(100 * df1_it/df1_size))

	# dereference and collect garbage
	del df1
	del paramlist24
	del mp_result
	gc.collect()

	if rbcpy24.closed == False:
		rbcpy24.close()

	# uk101b26
	df2 = pd.read_csv('path\\to\\ev2\\file_listing.csv', delimiter=',', quotechar = '"')
	df2_size = len(df2)
	df2_it = 0

	for listing in ev2_lst:
		df2_it += 1
		scorelist = []
		result = None
		score = None
		paramlist26 = list(itertools.product([listing[1][listing[1].find('ev2\\root_folder'):] + '\\'], [(x.Path[x.Path.find('ev2\\root_folder'):], x.Filename) for x in df2.itertuples()]))

		mp_result = pool.map(comparator, paramlist26)
		
		if mp_result:
			result, score = process.extractOne(listing[2], mp_result)
			#print(result)

		if listing[2]:
			print('Original string: ' + listing[2])
		if result:
			print('Fuzzymatched string: ' + result)
		if score:
			print('Score: ' + str(score))

		produced_result = result if result is not None else listing[2]
		rbcpy26.write('robocopy "' + listing[1] + '" "<ROOTPATH>'+ listing[1][listing[1].find('ev2\\root_folder'):] +'" "'+ produced_result +'" /r:3 /w:3 /xo /tee /e /np /log+:"<logdestination>\\log.txt" \n')
		# log
		writer.writerow([listing[0], listing[1], listing[2], produced_result])
		print('Processed: {0:.2f}%'.format(100 * df2_it/df2_size))

	del df2
	gc.collect()

	if rbcpy26.closed == False:
		rbcpy26.close()
	if outfile.closed == False:
		outfile.close()

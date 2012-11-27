#!/usr/bin/env python
import sys
 
# maps words to their counts
subcategoryCount = {}
subcategorywithWords = {}
subcategoryTotalWords = {}
 
# input comes from STDIN
for line in sys.stdin:
	# remove leading and trailing whitespace
	line = line.strip()
 
	# parse the input we got from mapper.py
	try: word, count = line.split('\t', 1)
	except ValueError: continue 
	# convert count (currently a string) to int
	try:
		count = int(count)
	except ValueError:
		continue
 
	try:
		subcategoryCount[word] = subcategoryCount[word]+1
	except:
		subcategoryCount[word] = 1
	wordsBool = int(bool(count))
	try:
		subcategorywithWords[word] = subcategorywithWords[word]+wordsBool 
	except:
		subcategorywithWords[word] = wordsBool
	try:
		subcategoryTotalWords[word] = subcategoryTotalWords[word]+count
	except:
		subcategoryTotalWords[word] = count
# write the tuples to stdout
# Note: they are unsorted
for word in subcategoryCount.keys():
	print 'PERCENTAGE\t%s\t%s'% ( word, float(subcategorywithWords[word])/ (subcategoryCount[word]))
for word in subcategoryCount.keys():
	print 'AVERAGE\t%s\t%s'% ( word, float(subcategoryTotalWords[word])/ (subcategoryCount[word]))

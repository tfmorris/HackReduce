#!/usr/bin/env python
import sys
import random 
#--- get all lines from stdin ---
for line in sys.stdin:
	#--- remove leading and trailing whitespace---
	line = line.strip()
 
	#--- split the line into words ---
	words = line.split('\t')
	if random.random() < .01:
		for word in words:
			if len(words) > 5: descriptionLength = len(words[5])
			else: descriptionLength = 0
			categories = words[4].split(',')
			#--- output tuples [word, 1] in tab-delimited format---
			for category in categories:
				catList = category.split('/') 
				for subcategory in catList:
					print '%s\t%s' % (subcategory, descriptionLength)


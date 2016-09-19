
import sys
import codecs

"""
Script for cleaning google bigram data, discarding records containing single 
character tokens (except 'I') and non-alphabetic characters. Remaining bigrams are sorted by count.
Usage: cat input-file* | python clean-and-sort-google-bigrams.py | gzip > cleaned-and-sorted.txt.gz
"""

if __name__ == "__main__":

	reload(sys)  
	sys.setdefaultencoding('utf8')

	def valid(s): return not (len(s) == 1 and s != 'i') and s.isalpha()

	# Put valid bigram counts in dictionary
	counts = {}
	for line in sys.stdin:
		line = line.decode('utf-8').strip()
		[c, si, sj] = line.split(',')
		if valid(si) and valid(sj): counts[(si, sj)] = int(c)

	# Sort by count and print
	for (si, sj), c in sorted(counts.iteritems(), key=lambda (k,v): (v,k), reverse=True):
		print str(c) + ',' + si.encode('utf-8', 'replace') + ',' + sj.encode('utf-8', 'replace')
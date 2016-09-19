
import sys
import codecs

"""
Script for cleaning google bigram data, discarding records containing single 
character tokens (except 'I') and non-alphabetic characters.
Usage: cat googlebooks-eng-all-2gram-20120701* | python clean-google-bigrams.py | gzip > cleaned-bigrams.txt.gz
"""

if __name__ == "__main__":

	reload(sys)  
	sys.setdefaultencoding('utf8')

	def valid(s): return not (len(s) == 1 and s != 'i') and s.isalpha()

	for line in sys.stdin:
		line = line.decode('utf-8').strip()
		[c, si, sj] = line.split(',')
		if valid(si) and valid(sj): print line
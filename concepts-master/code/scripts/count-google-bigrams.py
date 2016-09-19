import sys
import codecs
import collections
from google_ngram_downloader import readline_google_store

"""
Script for fetching and aggregating bigram data provided by Google:
	http://storage.googleapis.com/books/ngrams/books/datasetsv2.html
Requires google_ngram_downloader library: 
	https://pypi.python.org/pypi/google-ngram-downloader
Usage: python count-google-bigrams.py
"""

if __name__ == "__main__":

	reload(sys)  
	sys.setdefaultencoding('utf8')

	chunks = readline_google_store(ngram_len=2, lang='eng')

	for fileName, url, records in chunks:
		if fileName[-14:] == 'punctuation.gz': break
		print "Processing " + fileName + "..."
		counts = collections.defaultdict(int)
		for r in records:
			bigram = r.ngram
			# Ignore if containing part of speech tag or comma (later used as delimiter)
			if '_' not in bigram and ',' not in bigram:
				# Set to lowercase and split at space
				[i, j] = bigram.lower().split()
				counts[(i, j)] += r.match_count
		
		# Write counts to file per chunk
		output = codecs.open(fileName[:-3] + "-aggregated.txt", "w", "utf-8")
		for (i, j), c in sorted(counts.iteritems(), key=lambda (k,v): (v,k), reverse=True):
			output.write(str(c) + ',' + i.encode('utf-8', 'replace') + ',' + j.encode('utf-8', 'replace') + '\n')
		output.close()
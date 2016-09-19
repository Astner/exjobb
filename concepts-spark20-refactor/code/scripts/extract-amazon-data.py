
import sys
import codecs

"""
Script for extracting category labels and higher-order OR concepts from 
Amazon product metadata available at http://snap.stanford.edu/data/amazon-meta.html

Usage: cat amazon-meta-data.txt | python extract-amazon-data.py
"""

if __name__ == "__main__":

	reload(sys)  
	sys.setdefaultencoding('utf8')

	labelIdPairs = set()
	orConcepts = set()

	def getId(line): 
		return int(line.split(' ')[-1].strip())

	def extractLabel(line, id):
		 label = ":".join(line.split(':')[1:]).strip().replace (' ', '_')
		 labelIdPairs.add((label, id))

	def extractConcepts(line, id):

		def getLabelId(c):
			p = c[:-1].split('[')
			return (p[0].replace (' ', '_'), int(p[-1]))

		categorySequence = line[1:].split('|')		
		labelPairs = [getLabelId(c) for c in categorySequence]
		for p in labelPairs: labelIdPairs.add(p)
		ids = [i for (l, i) in labelPairs]
		for (i, j) in zip(ids[:-1], ids[1:]): orConcepts.add((j, i))
		orConcepts.add((id, ids[-1]))

	for line in sys.stdin:
		l = line.strip()
		if l.startswith("Id"): id = getId(l)
		elif l.startswith("title"): extractLabel(l, id)
		elif l.startswith("|"): extractConcepts(l, id)

	labelsFile = open("amazon-labels.txt", 'w')
	conceptsFile = open("amazon-or-concepts.txt", 'w')

	for (l, i) in labelIdPairs: labelsFile.write(l + " " + str(i) + "\n")
	for (i, j) in orConcepts: conceptsFile.write(str(i) + " " + str(j) + "\n")
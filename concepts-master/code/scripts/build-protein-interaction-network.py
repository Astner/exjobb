import sys
import itertools

""" 
Script for building a protein interaction network from data provided by
Human Protein Database (http://www.hprd.org).

Use BINARY_PROTEIN_PROTEIN_INTERACTIONS.txt as input.

Note that the input graph is binary and undirected. All vertices and edges 
in the output graph are therefore given weight 1, and edges are duplicated 
in order to be directed.

Usage: python build-protein-interaction-network.py input-file.txt graph-prefix
"""

if __name__ == '__main__' :

    # Parse line and return gene symbols
    def getPair(line):
        fields = line.split('\t')
        return (fields[0], fields[3])

    file = open(sys.argv[1], 'r')
    pairs = [getPair(line) for line in file]
    file.close()

    # Get unique proteins by flattening and removing duplicates
    proteins = list(set(itertools.chain.from_iterable(pairs)))

    # Dictionary from gene symbols to indices
    proteinToIndex = dict(zip(proteins, range(len(proteins))))

    # Graph files
    verticesFile = open(sys.argv[2] + "-vertices.txt", 'w')
    edgesFile = open(sys.argv[2] + "-edges.txt", 'w')

    for p in proteins: 
        verticesFile.write(str(proteinToIndex[p]) + ',' + p + ',1.0\n')

    for (p1, p2) in pairs:
        # Discard self-interactors
        if p1 != p2:
            s1 = str(proteinToIndex[p1])
            s2 = str(proteinToIndex[p2])
            # Make directed graph by adding flipped edge
            edgesFile.write(s1 + ',' + s2 + ',1.0\n')
            edgesFile.write(s2 + ',' + s1 + ',1.0\n')


    verticesFile.close()
    edgesFile.close()
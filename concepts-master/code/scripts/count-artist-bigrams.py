import sys
import collections
import operator

""" 
Script for counting artist bigrams of last fm dataset available at
http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html .
Assumes tab separated input where user id is in the first location and
artist is in the fourth location.

Usage: python count-artist-bigrams.py < input-file.txt > output-file.txt
"""

if __name__ == '__main__' :

    bigramCounts = collections.defaultdict(int)
    previousUser = ""

    for line in sys.stdin:

        fields = line.split("\t")
        user = fields[0]
        artist = fields[3].replace(' ', '_')

        if user == previousUser and not artist == previousArtist:
            bigramCounts[(previousArtist, artist)] += 1

        previousUser = user
        previousArtist = artist

    # Discard counts occurring less than 3 times
    bigramCounts = [((v, w), c) for ((v, w), c) in bigramCounts.items() if c >= 3]

    # Sort in descending order
    bigramCounts = sorted(bigramCounts, key=operator.itemgetter(1), reverse=True)

    for ((v, w), c) in bigramCounts:
        print str(c) + "," + v + "," + w
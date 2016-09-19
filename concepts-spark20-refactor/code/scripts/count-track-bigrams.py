import sys
import collections
import operator

""" 
Script for counting track bigrams in last fm dataset available at
http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html .
Assumes tab separated input where user id is in the first location and
track is in the sixth location.

Usage: python count-track-bigrams.py < input-file.txt > output-file.txt
"""

if __name__ == '__main__' :

    bigramCounts = collections.defaultdict(int)
    previousUser = ""

    for line in sys.stdin:

        fields = line.split("\t")
        user = fields[0]
        track = fields[5].replace(' ', '_').strip()

        if user == previousUser and not track == previousTrack:
            bigramCounts[(previousTrack, track)] += 1

        previousUser = user
        previousTrack = track

    # Discard counts occurring less than 3 times
    bigramCounts = [((v, w), c) for ((v, w), c) in bigramCounts.items() if c >= 3]

    # Sort in descending order
    bigramCounts = sorted(bigramCounts, key=operator.itemgetter(1), reverse=True)

    for ((v, w), c) in bigramCounts:
        print str(c) + "," + v + "," + w
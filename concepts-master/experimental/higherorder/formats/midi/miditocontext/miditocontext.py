# Copyright 2015 Daniel Gillblad (dgi@sics.se).

# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from mido import MidiFile
import os
import sys
from midiutils import *

# Convert contexts as list of lists to indexed data
def toIndexedContexts(collected_contexts, start_index = 0):
	i = start_index
	indexdata = []
	for c in collected_contexts:
		for k in c:
			indexdata += [(i, k)]
		i += 1
	return indexdata

# Write indexed concepts to file
def writeIndexedConcepts(indexed_concepts, filehandle):
	for i in indexed_concepts:
		filehandle.write(str(i[0]) + ' ' + str(i[1]) + '\n')

# Find all note contexts where notes are struck together (harmonies)
def groupedNoteOnMessages(midifile):
	mid = MidiFile(midifile)
	groups = []
	for i, track in enumerate(mid.tracks):
		for channel in activeChannels:
			cTime = 0
			gTime = 0
			nSet = set()
			for message in track:
				cTime += message.time
				if message.type == 'note_on' and message.velocity > 0 and message.channel == channel:
					if (cTime - gTime) >= simTimeLimit:
						if len(nSet) > 1:
							groups += [list(nSet)]
						gTime = cTime
						nSet.clear()
						nSet.add(message.note)
					else:
						nSet.add(message.note)
	return groups

# Consecutive notes / harmonies as contexts
def consecutiveNoteOnMessages(midifile):
	mid = MidiFile(midifile)
	groups = []
	for i, track in enumerate(mid.tracks):
		for channel in activeChannels:
			cTime = 0
			gTime = 0
			nSet = set()
			lSet = set()
			for message in track:
				cTime += message.time
				if message.type == 'note_on' and message.velocity > 0 and message.channel == channel:
					if (cTime - gTime) >= simTimeLimit:
						clist = list(lSet) + list(nSet)
						if len(clist) >= 2:
							groups += [clist]
						gTime = cTime
						lSet = nSet.copy()
						nSet.clear()
						nSet.add(message.note)
					else:
						nSet.add(message.note)
	return groups

# Convert a collection of MIDI files to contexts and write them to a single file.
# The function traverses all sub-directories of root_dir, and applies context_func
# (a function that should return a list of lists representing note co-occurrences)
# to all .mid files it finds. The result is written to output_file in indexed concept
# format combining the contexts of all found MIDI files.
def midiFilesToContexts(root_dir, output_file, context_func, verbose = True):
	base_dir = os.path.abspath(root_dir)
	context_id_start = 0
	with open(output_file, 'w') as of:
		for root, subdirs, files in os.walk(base_dir):
			if verbose:
				print('# Directory: ' + root)
			for filename in files:
				if os.path.splitext(filename)[1] == '.mid':
					if verbose:
						print('  - %s ' % (filename))
					file_path = os.path.join(root, filename)
					contexts = context_func(file_path)
					nr_contexts = len(contexts)
					indexed_concepts = toIndexedContexts(contexts, context_id_start)
					writeIndexedConcepts(indexed_concepts, of)
					context_id_start += nr_contexts

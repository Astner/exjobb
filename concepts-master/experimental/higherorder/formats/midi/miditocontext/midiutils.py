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

import os
import sys
from mido import MidiFile
from operator import itemgetter

# Selecte active channels
# Here, all channels except 10 are used, as this MIDI channel is used for percussion only
# in General MIDI files (note that we index channels from 0, not 1)
activeChannels = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 14, 15)

# Time during which events are considered simultaneous
simTimeLimit = 50

# Fraction of time notes need to be overlapping to be considered played together
overlapFracLimit = 0.95

# Print info on specified MIDI file
def printMidiFileInfo(midifile):
	mid = MidiFile(midifile)
	print("File:", os.path.basename(midifile),
	      "Type:", mid.type,
	      "Length: ",
	      end = "")
	if mid.type < 2:
		print(mid.length)
	else:
		print("N/A")

# Print messages in MIDI file
def printMidiMessages(midifile):
	mid = MidiFile(midifile)
	for i, track in enumerate(mid.tracks):
		print('Track {}: {}'.format(i, track.name))
		for message in track:
			print(message)

# Simple class for representing current note state,
# given a series of messages given in order to the update method.
class NoteState:
	nstate = [-1] * 128

	def printState(self):
		print('|', end = '')
		for i in self.nstate[22:116]:
			if i >= 0:
				print('*', end = '')
			else:
				print('-', end = '')
		print('|')

	def update(self, message, time = 0):
		if message.type == 'all_notes_off':
			self.nstate = [-1] * 128
		elif message.type == 'note_off' or (message.type == 'note_on' and message.velocity <= 0):
			rmsg = [(self.nstate[message.note], time, message.note)]
			self.nstate[message.note] = -1
			return rmsg
		elif message.type == 'note_on':
			self.nstate[message.note] = time
			return []
		return []

# Determines if a message is a "note" message: note on, note off, or all notes off
def isNoteMessage(msg):
	if msg.type == 'note_on' or msg.type == 'note_off' or msg.type == 'all_notes_off':
		return True
	else:
		return False

# Determines if a message is a "note" message on a specific channel
def isNoteMessageOnChannel(msg, ch):
	if isNoteMessage(msg) and msg.channel == ch:
		return True
	else:
		return False

# Convert a MIDI file to a list of note events, each note given by
# (start-time, end-time, note number). Returns a nested list with
# one list per channel per track.
def fileToNoteList(midifile):
	mid = MidiFile(midifile)
	allnotes = []
	for i, track in enumerate(mid.tracks):
		tracknotes = []
		for channel in activeChannels:
			channelnotes = []
			state = NoteState()
			cTime = 0
			for message in track:
				cTime += message.time
				if isNoteMessageOnChannel(message, channel):
					channelnotes += state.update(message, cTime)
			if len(channelnotes) >= 1:
				tracknotes += [sorted(channelnotes, key=itemgetter(0,1))]
		if len(tracknotes) >= 1:
			allnotes += [tracknotes]
	return allnotes

# Convert a note list to a list of notes played together.
def playedTogether(notelist):
	together = []
	for i, note in enumerate(notelist):
		# Find overlaps
		for fnote in notelist[i + 1:]:
			if fnote[0] > note[1]:
				break
			overlap_a = (note[1] - fnote[0]) / (note[1] - note[0])
			overlap_b = (fnote[1] - note[1]) / (fnote[1] - fnote[0])
			if overlap_a >= overlapFracLimit or overlap_b >= overlapFracLimit:
				together += [[note[2], fnote[2]]]
	return together

# Find all notes played together in midifile, separated by track and channel
def allNotesPlayedTogether(midifile):
	notelists = fileToNoteList(midifile)
	alltogether = []
	for t in notelists:
		for c in t:
			alltogether += playedTogether(c)
	return alltogether

# Print a graphic representation of all note messages in a MIDI file
# on a (reasonable) easy to read format.
def printNoteMessages(midifile):
	mid = MidiFile(midifile)
	for i, track in enumerate(mid.tracks):
		print('Track {}: {}'.format(i, track.name))
		for channel in activeChannels:
			print('Channel:', channel)
			state = NoteState()
			cTime = 0
			simTime = 0
			for message in track:
				cTime += message.time
				if isNoteMessageOnChannel(message, channel):
					# If message is outside what would be considered simultaneous,
					# emit last state and reset simultaneous time counter
					if (simTime + message.time) >= simTimeLimit:
						print(cTime, end = '')
						state.printState()
						simTime = 0
					else:
						simTime += message.time
					state.update(message, cTime)

# Print a graphic representation of all note messages in a MIDI file
# on a (reasonable) easy to read format, grouping near simultaneous note events
# for readability.
def printGroupedNoteOnMessages(midifile):
	mid = MidiFile(midifile)
	for i, track in enumerate(mid.tracks):
		print('Track {}: {}'.format(i, track.name))
		for channel in activeChannels:
			print('Channel:', channel)
			cTime = 0
			gTime = 0
			nSet = set()
			for message in track:
				cTime += message.time
				if message.type == 'note_on' and message.velocity > 0 and message.channel == channel:
					if (cTime - gTime) >= simTimeLimit:
						print(cTime, nSet)
						gTime = cTime
						nSet.clear()
						nSet.add(message.note)
					else:
						nSet.add(message.note)

# Collect a set of statistics for a specified MIDI file object.
def collectMidiNoteStatistics(midifileobj, channel_activity, played_notes, note_offs,
                              running_status, all_notes_off):
	for i, track in enumerate(midifileobj.tracks):
		for channel in activeChannels:
			for message in track:
				if(isNoteMessage(message)):
					channel_activity[message.channel] += 1
					if message.type == 'note_on' and message.velocity > 0:
						played_notes[message.note] += 1
					if message.type == 'note_off':
						note_offs[message.channel] += 1
					if message.type == 'note_on' and message.velocity <= 0:
						running_status[message.channel] += 1
					if message.type == 'all_notes_off':
						all_notes_off[message.channel] += 1

# Print MIDI file statistics compiled for all MIDI files found in the specified directory
# and all sub-directories recursively.
def printMidiFileStatistics(root_dir):
	nr_files = 0
	nr_file_types = [0] * 3
	nr_length_files = 0
	total_length = 0
	channel_activity = [0] * 16
	played_notes = [0] * 127
	note_offs = [0] * 16 # Per channel
	running_status = [0] * 16 # Per channel
	all_notes_off = [0] * 16 # Per channel

	base_dir = os.path.abspath(root_dir)
	for root, subdirs, files in os.walk(base_dir):
		for filename in files:
			if os.path.splitext(filename)[1] == '.mid':
				file_path = os.path.join(root, filename)

				mid = MidiFile(file_path)
				nr_files += 1
				nr_file_types[mid.type] += 1
				if mid.type < 2:
					nr_length_files += 1
					total_length += mid.length
				collectMidiNoteStatistics(mid, channel_activity, played_notes, note_offs,
				                          running_status, all_notes_off)

	print('--------------------------------------------------------------------------')
	print('Number of .mid files:', nr_files, '      Type 0:', nr_file_types[0],
	      ' Type 1:', nr_file_types[1], ' Type 2:', nr_file_types[2])
	print('Average length:       {:.1f}'.format(total_length / nr_length_files))
	print('--------------------------------------------------------------------------')
	print('Note on messages:    ', sum(played_notes))
	print('Note off messages:   ', sum(note_offs))
	print('Running status off:  ', sum(running_status))
	print('All notes off:       ', sum(all_notes_off))
	print('--------------------------------------------------------------------------')
	print('Channel activity distribution:')
	total_activity = sum(channel_activity)
	for i, act in enumerate(channel_activity):
		if act > 0:
			print('{:2d}: {:.1%} '.format(i + 1, act / total_activity))
	print('--------------------------------------------------------------------------')
	print('Note distribution:')
	total_notes = sum(played_notes)
	for i, nt in enumerate(played_notes):
		if nt > 0:
			print('{:2d}: {:.2%} '.format(i, nt / total_notes))
	print('--------------------------------------------------------------------------')


# Visit files recursively from a root directory.
def visitFilesRecursively(root_dir, extension, apply_func, verbose = True):
	base_dir = os.path.abspath(root_dir)
	for root, subdirs, files in os.walk(base_dir):
		if verbose:
			print('# Directory: ' + root)
		for filename in files:
			if os.path.splitext(filename)[1] == extension:
				if verbose:
					print('  - %s ' % (filename))
				file_path = os.path.join(root, filename)
				res = apply_func(file_path)

#!/bin/python
import json, time, sys
from math import sqrt

class user(object):
	def __init__(self, uid):
		# uid: user's id
		# purchases: list of purchases along with their timestamp in chronological order
		# friends: set of friends (set avoids duplicated befriends and makes unfriend effcient as well)
		self.uid = uid
		self.purchases = []
		self.friends = set()

def read_batch_log(file_loc):
	"""reads batch_log file and builds the network and purchase list of each user"""

	users={}
	D=2
	T=50

	for e, line in enumerate(open(file_loc)):
		if line=='' or line in ' \n\t':
			print 'bad json line --- skipped!'
			continue
		try:
			json_line = json.loads(line)
		except ValueError, err:
			continue
		if e==0:
			D = int(json_line['D']) # need to define them properly (class? global?)
			T = int(json_line['T']) # need to define them properly (class? global?)
		else:
			if json_line["event_type"]=="purchase":
				# since we have too many users searching keys of dictinary everytime will be time consuming
				# using try catch will boost our time performance
				try:
					users[json_line["id"]].purchases+=[(float(json_line["amount"]), json_line["timestamp"], time.time())]
				except:
					users[json_line["id"]]=user(json_line["id"])
					users[json_line["id"]].purchases+=[(float(json_line["amount"]), json_line["timestamp"], time.time())]
			elif json_line["event_type"]=="befriend":
				try:
					users[json_line["id1"]].friends.add(json_line["id2"])
				except:
					users[json_line["id1"]]=user(json_line["id1"])
					users[json_line["id1"]].friends.add(json_line["id2"])
				try:
					users[json_line["id2"]].friends.add(json_line["id1"])
				except:
					users[json_line["id2"]]=user(json_line["id2"])
					users[json_line["id2"]].friends.add(json_line["id1"])
			elif json_line["event_type"]=="unfriend":
				try:
					users[json_line["id1"]].friends.remove(json_line["id2"])
				except:
					pass
				try:
					users[json_line["id2"]].friends.remove(json_line["id1"])
				except:
					pass
			else:
				print "Invalid event_type:", json_line["event_type"]
			
	return users, D, T

def read_stream_log(stream_log_loc, flagged_loc, users, D, T):
	""" reads stream_log file using current network of users 
		checks for anomalouse purchases and updates the network and purchase lists"""

	for line in open(stream_log_loc):
		if line=='' or line in ' \n\t':
			continue
		try:
			json_line = json.loads(line)
		except ValueError, err:
			continue
		if json_line["event_type"]=="purchase":
			try:
				users[json_line["id"]].purchases+=[(float(json_line["amount"]), json_line["timestamp"], time.time())]
			except:
				users[json_line["id"]]=user(json_line["id"])
				users[json_line["id"]].purchases+=[(float(json_line["amount"]), json_line["timestamp"], time.time())]
			
			# print 'User:', json_line["id"], 'friends', users[json_line["id"]].friends
			sn = find_social_network(users, json_line["id"], D)
			# print 'social network:', sn
			
			mean, sd = get_mean_sd(users, sn, T)
			# print 'mean', mean, 'sd', sd
			
			if sd!=-1 and float(json_line["amount"]) > mean + 3 * sd:
				ind = -1
				while line[ind]!='}':
					ind-=1
				s = line[:ind]+', "mean": "'+get_decimal(mean)+'", "sd": "'+get_decimal(sd)+'"}'
				# since the amount of output data is a lot less than the input it's okay to open and close the file after each flagged_log #
				write_output(flagged_loc, s)

		elif json_line["event_type"]=="befriend":
			try:
				users[json_line["id1"]].friends.add(json_line["id2"])
			except:
				users[json_line["id1"]]=user(json_line["id1"])
				users[json_line["id1"]].friends.add(json_line["id2"])
			try:
				users[json_line["id2"]].friends.add(json_line["id1"])
			except:
				users[json_line["id2"]]=user(json_line["id2"])
				users[json_line["id2"]].friends.add(json_line["id1"])
		elif json_line["event_type"]=="unfriend":
			try:
				users[json_line["id1"]].friends.remove(json_line["id2"])
			except:
				pass
			try:
				users[json_line["id2"]].friends.remove(json_line["id1"])
			except:
				pass
		else:
				print "Invalid event_type:", json_line["event_type"]
			
	return users

def find_social_network(users, uid, D=2):
	"""Using BFS finding neighbors of max level D from uid
	given network of users and a specific user of uid
	returns the id of D th degree social network"""

	visit_lvl = {}
	q = []

	q.append(uid)
	visit_lvl[uid] = 0

	while q:
		s = q.pop(0) # Dequeue in a FIFO manner
		for i in users[s].friends:
			if i not in visit_lvl:
				visit_lvl[i] = visit_lvl[s]+1
				if visit_lvl[i]<D: # Avoid exploring network more than necessary
					q.append(i)
	del visit_lvl[uid]
	return visit_lvl.keys()
				
def get_mean_sd(users, sn, T=50):
	""" returns mean and standard deviation of list of purchases of a social network
		given network of users and a list of user ids in a social network 
		finds last T purchases made in this social network and return mean and sd
		returns 0, -1 as invalid if the amount of purchases is less than 2"""

	# keeping track of index of each user's purchase list in social network
	indices = [len(users[i].purchases)-1 for i in sn] 
	sn_purchase_list = [] # purchase list of social network
	
	# while did not found last T purchases 
	# and there still exist a purchase in any of the social networks users purchases
	while len(sn_purchase_list)<T and sum(indices)> -len(sn):
		last = (0,'',0) # (amount, timestamp, time())
		last_i = -1
		for i, uid in enumerate(sn):
			if indices[i]<0: # visited all this user's purchase list
				continue
			# find latest purchase among last purchases of each user in the social net
			if users[uid].purchases[indices[i]][2]>last[2]: 
				last = users[uid].purchases[indices[i]]
				# and track the indices
				last_uid = uid
				last_i = i
		if last_i ==-1: # avoid potential error (if all users' puchase lists were empty)
			break
		else:
			# add last purchased item
			sn_purchase_list += [(last[1], last_uid, last[0])] # list of tuples (timestamp, user_id, amount)
			indices[last_i] -= 1 # update the index of added item

	if len(sn_purchase_list)>=2:
		# calculate mean and standard deviation
		mean = sum(i for _,_,i in sn_purchase_list)/len(sn_purchase_list)
		sd = sqrt(sum([(i-mean)*(i-mean) for _,_,i in sn_purchase_list])/len(sn_purchase_list))
		return mean, sd
	return 0, -1

def write_output(file_loc, line):
	""""write string of json objects in output file"""

	with open(file_loc, "a") as f:
		f.write(line+'\n')

def get_decimal(n, i=2):
	"""truncate decimal point of number n after i and return the string (avoid rounding)"""

	before_dec, after_dec = str(n).split('.')
	return '.'.join((before_dec, after_dec[:i]))

if __name__=='__main__':
	""" it sould take 3 arguments
	python ./src/process_log.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json
	"""

	batch_log_loc = sys.argv[1]
	stream_log_loc = sys.argv[2]
	flagged_loc = sys.argv[3]

	print 'Reading batch logs...'
	users, D, T = read_batch_log(batch_log_loc)
	print "D:", D, "T:", T
	print 'Number of users: ', len(users)
	
	f = open(flagged_loc, 'w')
	f.close()
	print 'Empty output file created'
	
	print 'Reading stream logs...'
	users = read_stream_log(stream_log_loc, flagged_loc, users, D, T) # make sure users is getting updated
	print 'Done!'
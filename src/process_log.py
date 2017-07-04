import json, time, sys
from math import sqrt

class user(object):
	def __init__(self, uid):
		self.uid = uid
		self.purchases = []
		self.friends = set()

	def purchase_size(self):
		return len(self.purchases)

def read_batch_log(file_loc):
	users={}
	D=2
	T=50
	for e, line in enumerate(open(file_loc)):
		json_line = json.loads(line)
		if e==0:
			D = int(json_line['D']) # need to define them properly (class? global?)
			T = int(json_line['T']) # need to define them properly (class? global?)
		else:
			if json_line["event_type"]=="purchase":
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
					users[json_line["id1"]]=user(json_line["id1"])
				try:
					users[json_line["id2"]].friends.remove(json_line["id1"])
				except:
					users[json_line["id2"]]=user(json_line["id2"])
			
	return users, D, T

def read_stream_log(stream_log_loc, flagged_loc, users, D, T):
	for line in open(stream_log_loc):
		json_line = json.loads(line)
		if json_line["event_type"]=="purchase":
			try:
				users[json_line["id"]].purchases+=[(float(json_line["amount"]), json_line["timestamp"], time.time())]
			except:
				users[json_line["id"]]=user(json_line["id"])
				users[json_line["id"]].purchases+=[(float(json_line["amount"]), json_line["timestamp"], time.time())]
			sn = find_social_network(users, json_line["id"], D) # TODO: make sure users getting updated not only in this function but globally
			mean, sd = get_mean_sd(users, sn, T)
			if sd!=-1 and float(json_line["amount"]) > mean + 3 * sd:
				s = line[:-1]+', "mean": "'+'%.2f'%(mean)+'", "sd": "'+'%.2f'%(sd)+'"}'
				################## try writing in file faster #################
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
				users[json_line["id1"]]=user(json_line["id1"])
			try:
				users[json_line["id2"]].friends.remove(json_line["id1"])
			except:
				users[json_line["id2"]]=user(json_line["id2"])
			
	return users

def find_social_network(users, uid, D=2):
	"""Using BFS finding neighbors of max level D from uid
	given network of users and a specific user of uid
	returns the id of Dth degree social network"""

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
	indices = [len(users[i].purchases)-1 for i in sn] # keeping track of index of each user's purchase list in social network
	sn_purchase_list = [] # purchase list of social network
	while len(sn_purchase_list)<T and sum(indices)> -len(sn):
		last = (0,'',0) # (amount, timestamp, time())
		last_i = -1
		for i, uid in enumerate(sn):
			if indices[i]<0:
				continue
			if users[uid].purchases[indices[i]][2]>last[2]:
				last = users[uid].purchases[indices[i]]
				last_uid = uid
				last_i = i
		if last_i ==-1: # potential error
			break
		else:
			sn_purchase_list+=[(last[1], last_uid, last[0])] # list of tuples (timestamp, user_id, amount)
			indices[last_i]-=1

	if len(sn_purchase_list)>=2:
		mean = sum(i for _,_,i in sn_purchase_list)/len(sn_purchase_list)
		sd = sqrt(sum([(i-mean)*(i-mean) for _,_,i in sn_purchase_list])/len(sn_purchase_list))
		return mean, sd
	return 0, -1
		
	if purchase_amount > mean + 3 * sd:
		s = '{"event_type":"purchase", "timestamp":"'+tstamp+'", "id": "'+uid+'", "amount": "'+'%.2f'%(amount)+'", "mean": "'+'%.2f'%(mean)+'", "sd": "'+'%.2f'%(sd)+'"}'

def write_output(file_loc, line):
	with open(file_loc, "a") as f:
		f.write(line+'\n')

if __name__=='__main__':
	batch_log_loc = sys.argv[1]
	stream_log_loc = sys.argv[2]
	flagged_loc = sys.argv[3]
	users, D, T = read_batch_log(batch_log_loc)
	print "D: ", D, "T: ", T
	print len(users)
	users = read_stream_log(stream_log_loc, flagged_loc, users, D, T) # make sure users is getting updated
	print len(users)
	# c=0
	# for i in users:
	# 	c+=1
	# 	print users[i].friends
	# 	print i, find_social_network(users, i, 3)
	# 	if c==5:
	# 		break

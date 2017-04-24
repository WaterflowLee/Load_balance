#!coding:utf-8
from collections import defaultdict
import numpy as np
import pprint
import copy


def machine_mutual_distance(m_1, m_2):
	return np.sqrt((m_1.position[0] - m_2.position[0]) ** 2 + (m_1.position[1] - m_2.position[1]) ** 2)


class Dispatcher(object):
	def __init__(self, sim, machine_master_num, master_slave_distance_threshold):
		super(Dispatcher, self).__init__()
		self._simulator = sim
		self._machine_master_num = machine_master_num
		self._master_slave_distance_threshold = master_slave_distance_threshold

		self._machine_load_cnt = defaultdict(int)  # dict key:id; value:load
		self._machine_load_rank = None  # list of tuple (id, load) in load's desc order
		self._machine_masters = []  # list of ids in load's desc order
		self._machine_slaves = []  # list of ids in load's desc order
		self._to_dispatch_machine_slaves = []
		self._to_dispatch_machine_districts = []
		self._machine_load_district_average = len(self._simulator.requests.values())/float(self._machine_master_num)
		self._machine_load_district_dist = {}  # dict key:id; value:district load sum 用master的id赋予区域的id
		self._machine_district_dispatch_result = defaultdict(set)
		self._master_slave_distance_matrix = {}

	def find_nearest_slave(self, master, too_busy_slave_list):
		master_slave_distance_row = self._master_slave_distance_matrix[master]
		to_dispatch_slave_distance_row = [d for d in master_slave_distance_row.iteritems()
											if d[0] in self._to_dispatch_machine_slaves and d[0] not in too_busy_slave_list]
		try:
			slave_id_distance = min(to_dispatch_slave_distance_row, key=lambda d: d[1])
		except ValueError:
			slave_id_distance = None
		return slave_id_distance

	def find_idlest_district(self):
		can_be_dispatched_district = [d for d in self._machine_load_district_dist.iteritems() if d[0] in self._to_dispatch_machine_districts]
		return min(can_be_dispatched_district, key=lambda d: d[1])

	def find_nearest_master(self, slave):
		master_slave_distance_column = [(d[0], d[1][slave]) for d in self._master_slave_distance_matrix.iteritems()]
		return min(master_slave_distance_column, key=lambda d: d[1])

	def machine_load_rank(self):
		for r in self._simulator.requests.values():
			self._machine_load_cnt[r.machine] += 1
		# self._machine_load_rank = sorted(self._machine_load_cnt.iteritems(), key=lambda cnt: cnt[1], reverse=True)
		self._machine_load_rank = sorted(self._machine_load_cnt.iteritems(), cmp=lambda cnt_1, cnt_2: cnt_1[1]-cnt_2[1],
										reverse=True)
		return self

	def master_slave_distance_matrix(self):
		for master in self._machine_masters:
			self._master_slave_distance_matrix[master] = {}
			for slave in self._machine_slaves:
				m_1 = self._simulator.machines[master]
				m_2 = self._simulator.machines[slave]
				self._master_slave_distance_matrix[master][slave] = machine_mutual_distance(m_1, m_2)

	def machine_slave_dispatch_round_0(self):
		self._machine_masters = [m[0] for m in self._machine_load_rank[:self._machine_master_num]]
		self._machine_load_district_dist = dict(self._machine_load_rank[:self._machine_master_num])
		self._machine_slaves = [m[0] for m in self._machine_load_rank[self._machine_master_num:]]
		# 和js一样, 指向同一内存对象
		# self._to_dispatch_machine_slaves = self._machine_slaves
		# self._to_dispatch_machine_districts = self._machine_masters
		self._to_dispatch_machine_slaves = copy.copy(self._machine_slaves)
		self._to_dispatch_machine_districts = copy.copy(self._machine_masters)
		self.master_slave_distance_matrix()
		print "Finish Round 0 Dispatching"
		return self

	def machine_slave_dispatch_round_1(self):
		too_busy_slave_list = []
		while len(self._to_dispatch_machine_slaves) and len(self._to_dispatch_machine_districts):
			idlest_district_id, idlest_district_load = self.find_idlest_district()
			ret = self.find_nearest_slave(idlest_district_id, too_busy_slave_list)
			if ret and ret[1] < self._master_slave_distance_threshold:
				nearest_slave_id, nearest_slave_distance = ret
				if idlest_district_load + self._machine_load_cnt[nearest_slave_id] < self._machine_load_district_average:
					self._machine_load_district_dist[idlest_district_id] += self._machine_load_cnt[nearest_slave_id]
					self._machine_district_dispatch_result[idlest_district_id].add(nearest_slave_id)
					self._to_dispatch_machine_slaves.remove(nearest_slave_id)
					too_busy_slave_list = []
				else:
					too_busy_slave_list.append(nearest_slave_id)
			else:
				print "District %s can not be dispatched anymore" % idlest_district_id
				self._to_dispatch_machine_districts.remove(idlest_district_id)
				too_busy_slave_list = []
		print "Finish Round 1 Dispatching"
		return self

	def machine_slave_dispatch_round_2(self):
		for slave_id in self._to_dispatch_machine_slaves:
			district_id, district_distance = self.find_nearest_master(slave_id)
			self._machine_load_district_dist[district_id] += self._machine_load_cnt[slave_id]
			self._machine_district_dispatch_result[district_id].add(slave_id)
		print "Finish Round 2 Dispatching"
		return self

	def print_info(self):
		for attr in dir(self):
			if attr[0] == "_" and attr[-1] != "_" and attr != "_master_slave_distance_matrix":
				print attr + ":"
				pprint.pprint(self.__getattribute__(attr))
				print "\n"

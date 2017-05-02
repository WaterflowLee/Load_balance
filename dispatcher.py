#!coding:utf-8
from collections import defaultdict
import numpy as np
import pprint
import copy
import json
from system import District

LOG_INTO_JSON_FILE = False
LOGS = []
INFOS = {}
BACKEND_DISTANCE = 1000


class Dispatcher(object):
	def __init__(self, sim, machine_master_num, distance_threshold, delay_distance_discount=0.5, local_delay=0):
		super(Dispatcher, self).__init__()
		self._simulator = sim
		self._machine_master_num = machine_master_num
		self._delay_distance_discount = delay_distance_discount
		District.max_radius = distance_threshold
		self._local_delay = local_delay

		self._machine_load_rank = None  # list of id in load's desc order
		self._master_slave_distance_matrix = {}
		self._incomplete_dispatch = defaultdict(list)

		self.machine_load_rank()

	def machine_slave_dispatch_round_0(self):
		for master_id in self._machine_load_rank[:self._machine_master_num]:
			master = self._simulator.machines[master_id]
			District(master)
		self.master_slave_distance_matrix()
		print "Finish Round 0 Dispatching"
		return self

	def machine_slave_dispatch_round_1(self):
		too_busy_slave_list = []
		while len([machine for machine in self._simulator.machines.values() if not machine.district]) \
				and len([district for district in District.districts.values() if district.expandable]):
			idlest_district = Dispatcher.find_idlest_district()
			slave = self.find_nearest_slave(idlest_district.master, too_busy_slave_list)
			distance = self._master_slave_distance_matrix[idlest_district.master.unique_id][slave.unique_id]
			if slave and distance < District.max_radius:
				if idlest_district.load + slave.load < District.average_load():
					idlest_district.add_slave(slave)
					too_busy_slave_list = []
					if LOG_INTO_JSON_FILE:
						LOGS.append({"machine_load_district_dist": copy.deepcopy(self._machine_load_district_dist),
								"machine_district_dispatch_result": copy.deepcopy(self._district_machine_dispatch_result)})
				else:
					too_busy_slave_list.append(slave.unique_id)
			else:
				print "District %s can not be dispatched anymore" % idlest_district.unique_id
				idlest_district.expandable = False
				too_busy_slave_list = []
		if LOG_INTO_JSON_FILE:
			INFOS["position"] = dict([(m.unique_id, m.position) for m in self._simulator.machines.values()])
			INFOS["load"] = self._machine_load_rank
			json.dump({"LOGS": LOGS, "INFOS": INFOS}, open("round_1.json", "w"))
		print "Finish Round 1 Dispatching"
		return self

	def machine_slave_dispatch_round_2(self):
		for slave in [machine for machine in self._simulator.machines.values() if not machine.district]:
			master = self.find_nearest_master(slave)
			district = District.districts[master.unique_id]
			# self._machine_load_district_dist[district_id] += slave.load
			# self._district_machine_dispatch_result[district_id].append(slave_id)
			district.add_slave(slave)
		print "Finish Round 2 Dispatching"
		return self

	# 该函数执行完毕后, 会将服务部署到该区域中对该服务请求最多的 spots_num 个机器上
	def init_service_server_in_district(self, service, district, spots_num):
		machines_in_district = district.machines
		machines_in_district_get_request_for_this_service = []
		for machine in machines_in_district:
			# 这个服务不一定部署了，不能从服务池中看
			if service.unique_id in machine.service_access_log.keys():
				machines_in_district_get_request_for_this_service.append(machine)
		hot_machines_for_this_service = sorted(machines_in_district_get_request_for_this_service, key=lambda m: m.load,
											reverse=True)
		cur_spots_num = 0
		# 只考虑本机的访问量
		for machine in hot_machines_for_this_service:
			if cur_spots_num > spots_num - 1:
				break
			consumed_bandwidth = service.consumed_bandwidth * machine.service_access_log[service.unique_id]
			if machine.cur_bandwidth > consumed_bandwidth:
				cur_spots_num += 1
				request_from_user_list = [r for r in machine.request_queue if r.source == "user" and r.service.unique_id == service.unique_id]
				machine.deploy_service(service).serve_request(request_from_user_list)
				# self._district_service_dispatch_result[district_id][service_id].append(machine_id)
				# 上面这一语句被简化掉，是因为上面部署服务的时候机器、服务、机器内含区域信息综合替代了调度器
				# 维护的这一个字典
			else:
				pass
				# 可以使用伪用户请求处理incomplete TODO
		if cur_spots_num < spots_num - 1:
			self._incomplete_dispatch[district.unique_id].append(service.unique_id)

	def dispatch_server_in_district(self, service, district):
		for client in district.machines:
			if service.unique_id not in client.service_pool.keys():
				server = Dispatcher.find_nearest_server(service, district, client)
				if server:
					requests_to_be_sent_forth = []
					for r in client.request_queue:
						if r.service.unique_id == service.unique_id:
							requests_to_be_sent_forth.append(r)
					client.send_request(requests_to_be_sent_forth, server)
					server.receive_request(requests_to_be_sent_forth)
					server.serve_request(requests_to_be_sent_forth)
				else:
					print "for client machine %s can not find a server" % client.unique_id
		for machine in district.machines:
			if machine.cur_bandwidth < 0:
				unloaded_clients, status = self.unload_overload_server(machine, service)
				if not status:
					print "unload service %s from server %s failed" % (service.unique_id, machine.unique_id)
				else:
					print "unload %s from server %s" % (", ".join(unloaded_clients), machine.unique_id)

	def minimize_service_delay_in_district(self, service, district):
		service_id = service.unique_id
		district_id = district.unique_id
		server_list = district.service_deploy_log[service_id]
		for server_id in [server.unique_id for server in server_list]:
			cur_best_in_this_cluster = self._simulator.snapshot()
			simulator_snapshot = self._simulator.snapshot()
			# 下面的5行语句同义
			# for next_generation_server in \
			# 		[s for s in self._simulator.machines[server_id].service_client[service.unique_id] if s != "user"]:
			next_generation_server_id_list = [s.unique_id for s in self._simulator.machines[server_id]
				.service_client[service_id] if s != "user"]
			for next_generation_server_id in next_generation_server_id_list:
				server = self._simulator.machines[server_id]
				next_generation_server = self._simulator.machines[next_generation_server_id]
				cur_delay = self.cal_service_delay_in_district(self._simulator.services[service_id],
																	self._simulator.districts[district_id])
				requests_to_be_stopped = [r for r in server.serving_request if r.service.unique_id == service_id]
				server.stop_serving_request(requests_to_be_stopped)
				# 由于 undeploy_service 会修改 service 对象，因此也必须使用回滚后的 service
				server.undeploy_service(self._simulator.services[service_id])
				requests_to_be_sent_back = [r for r in server.request_queue if r.service.unique_id == service_id
											and r.source != "user"]
				requests_to_be_sent_forth = [r for r in server.request_queue if r.service.unique_id == service_id
											and r.source == "user"]
				server.send_request(requests_to_be_sent_back)
				server.send_request(requests_to_be_sent_forth, next_generation_server)
				next_generation_server.deploy_service(self._simulator.services[service_id])
				next_generation_server.receive_request(requests_to_be_sent_forth)
				for client in server.service_client[service_id]:
					if client == next_generation_server:
						continue
					else:
						requests_to_be_sent_forth = [r for r in client.request_queue if r.service.unique_id == service_id]
						client.send_request(requests_to_be_sent_forth, next_generation_server)
						next_generation_server.receive_request(requests_to_be_sent_forth)
				next_generation_server.serve_request([r for r in next_generation_server.request_queue
														if r.service.unique_id == service_id])
				if next_generation_server.cur_bandwidth < 0:
					Dispatcher.unload_overload_server(next_generation_server, self._simulator.services[service_id])
				new_delay = self.cal_service_delay_in_district(self._simulator.services[service_id],
																self._simulator.districts[district_id])
				# 如果新的延迟小于现在的延迟，记录此时的模拟器状态
				if new_delay < cur_delay:
					cur_best_in_this_cluster = self._simulator.snapshot()
				# 无论结果如何进行模拟器的状态回滚，准备进行此簇中的下一次尝试
				self._simulator = simulator_snapshot
			# 完成一个簇的优化后，更新模拟器的状态，并在此基础上进行下一个簇的优化
			self._simulator = cur_best_in_this_cluster

	def stage_1(self):
		self.machine_slave_dispatch_round_0()
		self.machine_slave_dispatch_round_1()
		self.machine_slave_dispatch_round_2()

	def stage_2(self):
		for district in self._simulator.districts.values():
			for 
			get_service_spots_num_in_district
#####################################################################################################################
	def find_nearest_slave(self, master, too_busy_slave_list):
		master_slave_distance_row = self._master_slave_distance_matrix[master.unique_id]
		can_be_dispatched_slave = [d for d in master_slave_distance_row.iteritems()
											if not self._simulator.machines[d[0]].district and d[0] not in too_busy_slave_list]
		try:
			slave_id, distance = min(can_be_dispatched_slave, key=lambda dd: dd[1])
			slave = self._simulator.machines[slave_id]
		except ValueError:
			slave = None
		return slave

	def find_nearest_master(self, slave):
		master_slave_distance_column = [(d[0], d[1][slave.unique_id]) for d in self._master_slave_distance_matrix.iteritems()]
		try:
			master_id, distance = min(master_slave_distance_column, key=lambda dd: dd[1])
			master = self._simulator.machines[master_id]
		except ValueError:
			master = None
		return master

	def machine_load_rank(self):
		machine_load = {}
		for m in self._simulator.machines.values():
			machine_load[m.unique_id] = m.load
		# 结果是放在 lambda 之后的元素的 list
		# 暗含 for _ in dictionary 在其中
		self._machine_load_rank = sorted(machine_load, key=lambda k: machine_load[k], reverse=True)

	def master_slave_distance_matrix(self):
		for master_id in self._machine_load_rank[:self._machine_master_num]:
			self._master_slave_distance_matrix[master_id] = {}
			for slave_id in self._machine_load_rank[self._machine_master_num:]:
				master = self._simulator.machines[master_id]
				slave = self._simulator.machines[slave_id]
				self._master_slave_distance_matrix[master_id][slave_id] = Dispatcher.machine_mutual_distance(master, slave)

	def get_service_spots_num_in_district(self, service, district):
		access_num = district.service_access_log[service.unique_id]
		consumed_bandwidth = service.consumed_bandwidth * access_num
		return int(float(consumed_bandwidth)/self._simulator.machine_average_bandwidth) + 1

	def unload_overload_server(self, server, service):
		# 卸载只能卸载非本地的请求
		requests_from_machine = [r for r in server.serving_request
													if r.service.unique_id == service.unique_id and r.source != "user"]
		requests_to_be_sent_back = defaultdict(list)
		requests_num_groupby_client = defaultdict(int)
		for r in requests_from_machine:
			requests_to_be_sent_back[r.source.unique_id].append(r)
			requests_num_groupby_client[r.source.unique_id] += 1
		unloaded_client_list = []
		for client_id in sorted(requests_num_groupby_client, key=lambda k: requests_from_machine[k]):
			client = self._simulator.machines[client_id]
			server.stop_serving_request(requests_to_be_sent_back[client_id])
			# 伪装user将请求 send back
			server.send_request(requests_to_be_sent_back[client_id], client)
			# 由于在这些请求被client发送到server之前，已经消除过client端的服务访问日志，除了这些请求的source被修改成了client之外好似这些请求并未到达过client
			# 因此可以安全地使用client的receive_request接受请求并增加服务访问日志
			client.receive_request(requests_to_be_sent_back[client_id])
			unloaded_client_list.append(client_id)
			if server.cur_bandwidth > 0:
				break
		if server.cur_bandwidth > 0:
			return unloaded_client_list, True
		else:
			return unloaded_client_list, False

	def cal_service_delay_in_district(self, service, district):
		delay = 0
		for client in district.machines:
			server = client.service_server.get(service.unique_id, None)
			# get的默认值是None 而 service_server 本身其中的值有可能是None，两者都需要走后台
			if server and server != client:
				distance = Dispatcher.machine_mutual_distance(server, client) * \
						len([r for r in server.serving_request if r.source == client])
			elif server and server == client:
				distance = self._local_delay * \
						len([r for r in server.serving_request if r.source == "user"])
			else:
				# 服务器既不是自己也不是该区域中的其他机器，对于该服务的请求（可能未发送出去或者曾经被退回）只能走后台
				distance = self._simulator.backend.distance * client.service_access_log[service.unique_id]
			delay = delay + distance * self._delay_distance_discount
		return delay

	@staticmethod
	def machine_mutual_distance(m_1, m_2):
		return np.sqrt((m_1.position[0] - m_2.position[0]) ** 2 + (m_1.position[1] - m_2.position[1]) ** 2)

	@staticmethod
	def find_idlest_district():
		idlest_district = min([district for district in District.districts.values() if district.expandable], key=lambda d: d.load)
		return idlest_district

	@staticmethod
	def find_nearest_server(service, district, client):
		server_list = district.service_deploy_log[service.unique_id]
		if client in server_list:
			return client
		else:
			client_server_distance = []
			for server in server_list:
				client_server_distance.append((server, Dispatcher.machine_mutual_distance(server, client)))
			for server, distance in sorted(client_server_distance, key=lambda d: d[1]):
				if server.cur_bandwidth > 0:
					return server
			return None

	def print_info(self):
		for attr in dir(self):
			if attr[0] == "_" and attr[-1] != "_" and attr != "_master_slave_distance_matrix":
				print attr + ":"
				pprint.pprint(self.__getattribute__(attr))
				print "\n"

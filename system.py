#!coding:utf-8
import numpy as np
from collections import defaultdict
import copy
np.random.seed(100)


class CommonObject(object):
	def __init__(self, unique_id):
		super(CommonObject, self).__init__()
		self._id = unique_id

	@property
	def unique_id(self):
		return self._id

	@unique_id.setter
	def unique_id(self, v):
		if not isinstance(v, int):
			raise ValueError('id must be an integer!')
		self._id = v


# position (x, y) tuple
class Machine(CommonObject):
	# unique_id = 0
	# 用 unique_id, 类属性和从父类继承来的实例属性重名了, 造成了 obj.unique_id 访问的是本类的类属性,
	# 而不是从父类继承来的实例属性,按照一般来说是实例属性覆盖类属性的，但是在此处不适用从父类继承来的实例属性
	static_unique_id = 0

	def __init__(self, ram, bandwidth, position):
		super(Machine, self).__init__(Machine.static_unique_id)
		self._district = None
		self._ram = ram
		self._bandwidth = bandwidth
		self._position = position
		self._cur_ram = ram
		self._cur_bandwidth = bandwidth
		self._request_queue = []
		self._serving_request = []
		self._sent_out_request = []
		self._service_pool = {}
		self._service_access_log = defaultdict(int)
		self._service_server = {}
		self._service_client = defaultdict(list)
		self._simulator = None
		Machine.static_unique_id += 1

	# request : Request Object
	def receive_request(self, request):
		def receive_single_request(r):
			self._request_queue.append(r)
			self.service_access_logging(r)

		if isinstance(request, list):
			for r in request:
				receive_single_request(r)
		else:
			receive_single_request(request)
		return self

	# request : Request Object
	def serve_request(self, request):
		def serve_single_request(r):
			consumed_bandwidth = r.service.consumed_bandwidth
			self._cur_bandwidth -= consumed_bandwidth
			self._request_queue.remove(r)
			self._serving_request.append(r)
			if r.source not in self._service_client[r.service.unique_id]:
				self._service_client[r.service.unique_id].append(r.source)
				# 如果是非本地请求则通知客户端机器更新自己的_service_server字典
				if r.source != "user":
					r.source.set_service_server(r.service, self)
				else:
					self.set_service_server(r.service, self)

		if isinstance(request, list):
			for r in request:
				serve_single_request(r)
		else:
			serve_single_request(request)
		return self

	def stop_serving_request(self, request):
		def stop_serving_single_request(r):
			self._serving_request.remove(r)
			self._request_queue.append(r)
			self._cur_bandwidth += r.service.consumed_bandwidth
			if r.source in self._service_client[r.service.unique_id]:
				self._service_client[r.service.unique_id].remove(r.source)
				# 如果是非本地请求则通知客户端机器更新自己的_service_server字典
				# 设置成None是暗示需要走后台
				if r.source != "user":
					r.source.set_service_server(r.service, None)
				else:
					self.set_service_server(r.service, None)

		if isinstance(request, list):
			for r in request:
				stop_serving_single_request(r)
		else:
			stop_serving_single_request(request)

	def send_request(self, request, server=None):
		def send_single_request(r):
			self._request_queue.remove(r)
			self._sent_out_request.append(r)
			self.service_access_logging(r, mode=False)
			if r.source == "user":
				# send forth
				r.source = self
				if server:
					r.machine = server
				else:
					raise ValueError("When sending requests forth, the server is needed")
			else:
				# send back
				r.machine = r.source
				r.source = "user"

		if isinstance(request, list):
			for r in request:
				send_single_request(r)
		else:
			send_single_request(request)

	# service : Service Object
	def deploy_service(self, service):
		self._service_pool[service.unique_id] = service
		self._cur_ram -= service.consumed_ram
		service.deploy(self)
		return self

	def undeploy_service(self, service):
		del self._service_pool[service.unique_id]
		self._cur_ram += service.consumed_ram
		service.undeploy(self)

	def service_access_logging(self, request, mode=True):
		if mode:
			self._service_access_log[request.service.unique_id] += 1
		else:
			self._service_access_log[request.service.unique_id] -= 1

	def set_service_server(self, service, server):
		self._service_server[service.unique_id] = server

	def startup(self, service_id=None, test=True):
		for request in self._request_queue:
			if service_id is None or request.service.unique_id == service_id:
				service = request.service
				if service.unique_id in self._service_pool.keys() and service.consumed_bandwidth < self._cur_bandwidth:
					if not test:
						self.serve_request(request)
					request.delay = self._simulator.local_delay

	def find_nearest_machine(self, request, district_mode, test):
		service = request.service
		distance_row = self._simulator.distance_matrix[self.unique_id]
		if district_mode:
			filtered_distance_row = {}
			district_machine_ids = map(lambda m: m.unique_id, self._district.machines)
			for m_id, distance in distance_row.items():
				if m_id in district_machine_ids:
					filtered_distance_row[m_id] = distance
		else:
			filtered_distance_row = distance_row
		distance_rank = sorted(filtered_distance_row, key=lambda k: filtered_distance_row[k], reverse=False)
		for machine_id in distance_rank:
			server = self._simulator.machines[machine_id]
			if service.unique_id in server.service_pool.keys() and service.consumed_bandwidth < server.cur_bandwidth:
				if filtered_distance_row[machine_id] < self._simulator.backend.distance:
					if not test:
						self.send_request(request, server)
						server.receive_request(request)
						server.serve_request(request)
					request.delay = self._simulator.local_delay + filtered_distance_row[machine_id] * self._simulator.delay_distance_discount

				else:
					if not test:
						self.send_request(request, self._simulator.backend)
						self._simulator.backend.receive_request(request)
						self._simulator.backend.serve_request(request)
					request.delay = self._simulator.local_delay + self._simulator.backend.distance * self._simulator.delay_distance_discount
				return True
		return False

	def ask_for_help(self, district_mode=False, service_id=None, test=True):
		for request in self._request_queue:
			if service_id is None or request.service.unique_id == service_id:
				ret = self.find_nearest_machine(request, district_mode, test=test)
				if not ret:
					if not test:
						self.send_request(request, self._simulator.backend)
						self._simulator.backend.receive_request(request)
						self._simulator.backend.serve_request(request)
					request.delay = self._simulator.local_delay + self._simulator.backend.distance * self._simulator.delay_distance_discount


	@property
	def district(self):
		return self._district

	@district.setter
	def district(self, d):
		self._district = d

	@property
	def ram(self):
		return self._ram

	@property
	def bandwidth(self):
		return self._bandwidth

	@property
	def position(self):
		return self._position

	@property
	def cur_ram(self):
		return self._cur_ram

	@property
	def cur_bandwidth(self):
		return self._cur_bandwidth

	@property
	def request_queue(self):
		return self._request_queue

	@property
	def serving_request(self):
		return self._serving_request

	@property
	def sent_out_request(self):
		return self._sent_out_request

	@property
	def load(self):
		return np.sum(self._service_access_log.values())

	@property
	def service_access_log(self):
		return self._service_access_log

	@property
	def service_pool(self):
		return self._service_pool

	@property
	def service_server(self):
		return self._service_server

	@property
	def service_client(self):
		return self._service_client

	def serialize(self):
		serialized_service_access_log = dict(self._service_access_log.items())
		serialized_service_pool = self._service_pool.keys()
		serialized_service_server = [{service: getattr(server, "unique_id", None)} for service, server in self._service_server.items()]
		serialized_service_client = [{service: map(lambda c: getattr(c, "unique_id", "user"), clients)}
											for service, clients in self._service_client.items()]
		return {"ram": self._ram,
						"bandwidth": self._bandwidth,
						"cur_ram": self._cur_ram,
						"cur_bandwidth": self._cur_bandwidth,
						"unique_id": self.unique_id,
						"service_access_log": serialized_service_access_log,
						"service_pool": serialized_service_pool,
						"service_server": serialized_service_server,
						"service_client": serialized_service_client,
						"position": self._position,
						"distinct": getattr(self._district, "unique_id", None)
				}

	@property
	def simulator(self):
		return self._simulator

	@simulator.setter
	def simulator(self, sim):
		self._simulator = sim


class Backend(Machine):
	def __init__(self):
		super(Backend, self).__init__(np.inf, np.inf, (np.inf, np.inf))
		self._distance = None
		# 此处为特殊设置，Backend的实例化会增加Machine的类属性static_unique_id，因此在此处通过实例访问类属性并修改的方式去掉这个影响
		# 虽然这样的设计实在诡异，但是就这样吧
		# fatal error 上面提及的方式存在覆盖，因此并不起作用
		# self.static_unique_id -= 1
		Machine.static_unique_id -= 1

	@property
	def distance(self):
		return self._distance

	@distance.setter
	def distance(self, d):
		self._distance = d


class Service(CommonObject):
	static_unique_id = 0
	services = {}
	test_1 = []

	@classmethod
	def get_service_by_id(cls, unique_id):
		return cls.services[unique_id]

	def __init__(self, ram, bandwidth):
		super(Service, self).__init__(Service.static_unique_id)
		self._ram = ram
		self._bandwidth = bandwidth
		self._deployment = []
		Service.services[Service.static_unique_id] = self
		Service.static_unique_id += 1

	@property
	def consumed_ram(self):
		return self._ram

	@property
	def consumed_bandwidth(self):
		return self._bandwidth

	@property
	def deployment(self):
		return self._deployment

	def deploy(self, machine):
		self._deployment.append(machine)

	def undeploy(self, machine):
		self._deployment.remove(machine)


class Request(CommonObject):
	# 静态属性(类属性)
	static_unique_id = 0
	# machine :  Machine object
	# service : Service object
	# source : "user" or Machine object

	def __init__(self, machine, service, source="user"):
		super(Request, self).__init__(Request.static_unique_id)
		self._machine = machine
		self._service = service
		self._source = source
		Request.static_unique_id += 1
		self._delay = 0
	@property
	def machine(self):
		return self._machine

	@machine.setter
	def machine(self, m):
		self._machine = m

	@property
	def service(self):
		return self._service

	@property
	def source(self):
		return self._source

	@source.setter
	def source(self, s):
		self._source = s

	@property
	def delay(self):
		return self._delay

	@delay.setter
	def delay(self, d):
		self._delay = d


class District(CommonObject):
	# 用类属性保证区域的唯一性
	# 让类本身来管理自己的实例
	districts = {}
	max_radius = 0
	test_2 = []

	# 类方法 而不是静态方法
	@classmethod
	def average_load(cls):
		loads = [district.load for district in cls.districts.values()]
		return np.average(loads)

	def __init__(self, master):
		super(District, self).__init__(master.unique_id)
		if master in District.districts.values():
			raise ValueError("This %s district already exists!" % master.unique_id)
		self._master = master
		self._slaves = []
		District.districts[master.unique_id] = self
		master.district = self
		self._expandable = True

	def add_slave(self, slave):
		if isinstance(slave, list):
			for s in slave:
				self._slaves.append(s)
				s.district = self
		else:
			self._slaves.append(slave)
			slave.district = self

	@property
	def machines(self):
		return [self._master] + self._slaves

	@property
	def master(self):
		return self._master

	@property
	def slaves(self):
		return self._slaves

	@property
	def load(self):
		load_in_district = 0
		for m in self.machines:
			load_in_district += m.load
		return load_in_district

	@property
	def expandable(self):
		return self._expandable

	@expandable.setter
	def expandable(self, flag):
		self._expandable = flag

	@property
	def service_access_log(self):
		service_access_log = defaultdict(int)
		for machine in self.machines:
			for service_id in machine.service_access_log:
				service_access_log[service_id] += machine.service_access_log[service_id]
		return service_access_log

	@property
	def service_deploy_log(self):
		service_deploy_log = defaultdict(list)
		for machine in self.machines:
			for service in machine.service_pool.values():
				service_deploy_log[service.unique_id].append(machine)
		return service_deploy_log

	def serialize(self):
		return {"unique_id": self.unique_id,
				"master": self._master.serialize(),
				"slaves": [s.serialize() for s in self._slaves]
				}


class Simulator(object):
	def __init__(self, machine_num, service_num, request_num, distance, delay_distance_discount=0.5, local_delay=20):
		super(Simulator, self).__init__()
		self._backend = Backend()
		self._backend.distance = distance
		self._machine_num = machine_num
		self._service_num = service_num
		self._request_num = request_num
		self._delay_distance_discount = delay_distance_discount
		self._local_delay = local_delay
		self._machines = {}
		self._services = Service.services
		self._requests = {}
		self._districts = District.districts
		self._distance_matrix = defaultdict(dict)
		self._machine_ram_generator = None
		self._service_ram_generator = None
		self._machine_bandwidth_generator = None
		self._service_bandwidth_generator = None
		self._request_generator = None
		self._position_generator = None
		self._test_1 = Service.test_1
		self._test_2 = District.test_2

	def ram_generator(self, generators):
		self._machine_ram_generator = generators[0]
		self._service_ram_generator = generators[1]
		return self

	def bandwidth_generator(self, generators):
		self._machine_bandwidth_generator = generators[0]
		self._service_bandwidth_generator = generators[1]
		return self

	# generator returns (machine_id, service_id) tuple
	def request_generator(self, generator):
		self._request_generator = generator
		return self

	# generator returns (x, y) tuple
	def position_generator(self, generator):
		self._position_generator = generator
		return self

	def machine_factory(self):
		for _ in range(self._machine_num):
			m = Machine(self._machine_ram_generator(), self._machine_bandwidth_generator(), self._position_generator())
			m.simulator = self
			self._machines[m.unique_id] = m
		return self

	def service_factory(self):
		for _ in range(self._service_num):
			s = Service(self._service_ram_generator(), self._service_bandwidth_generator())
			# self._services[s.unique_id] = s
		return self

	def request_factory(self):
		for _ in range(self._request_num):
			machine_id, service_id = self._request_generator()
			r = Request(self._machines[machine_id], self._services[service_id])
			self._requests[r.unique_id] = r
			self._machines[machine_id].receive_request(r)
		return self

	def active(self):
		self.machine_factory()
		self.service_factory()
		self.cal_distance_matrix()
		District.test_2.append("jack in active")
		Service.test_1.append("jones in active")
		return self

	def snapshot(self):
		simulator_snapshot = copy.deepcopy(self)
		return simulator_snapshot

	def cal_distance_matrix(self):
		for machine_1 in self._machines.values():
			for machine_2 in self._machines.values():
				self._distance_matrix[machine_1.unique_id][machine_2.unique_id] = \
					Simulator.machine_mutual_distance(machine_1, machine_2)

	@staticmethod
	def machine_mutual_distance(m_1, m_2):
		return np.sqrt((m_1.position[0] - m_2.position[0]) ** 2 + (m_1.position[1] - m_2.position[1]) ** 2)

	@property
	def machines(self):
		return self._machines

	@property
	def services(self):
		return self._services

	@property
	def requests(self):
		return self._requests

	@property
	def districts(self):
		return self._districts

	@property
	def machine_average_bandwidth(self):
		bandwidth = 0
		for machine in self._machines.values():
			bandwidth += machine.bandwidth
		return float(bandwidth)/self._machine_num

	@property
	def backend(self):
		return self._backend

	@backend.setter
	def backend(self, backend_machine):
		self._backend = backend_machine

	@property
	def machine_num(self):
		return self._machine_num

	@property
	def distance_matrix(self):
		return self._distance_matrix

	@property
	def local_delay(self):
		return self._local_delay

	@property
	def delay_distance_discount(self):
		return self._delay_distance_discount


def ram_generator_factory(lower, upper, generator_type="uniform_distribution"):
	if generator_type == "uniform_distribution":
		def ram_generator():
			return np.random.randint(lower, upper, size=1)[0]
		return ram_generator


def bandwidth_generator_factory(lower, upper, generator_type="uniform_distribution"):
	if generator_type == "uniform_distribution":
		def bandwidth_generator():
			return np.random.randint(lower, upper, size=1)[0]
		return bandwidth_generator


def request_generator_factory(machine_id_list, service_id_list, generator_type="uniform_distribution"):
	if generator_type == "uniform_distribution":
		def request_generator():
			# p: The probabilities associated with each entry in a.If not given the sample assumes a uniform
			# distribution over all entries in a.
			m_id = np.random.choice(machine_id_list, size=1)[0]
			s_id = np.random.choice(service_id_list, size=1)[0]
			return m_id, s_id
		return request_generator


# x_range:(x_lower, x_upper)
# y_range:(y_lower, y_upper)
def position_generator_factory(x_range, y_range, generator_type="uniform_distribution"):
	if generator_type == "uniform_distribution":
		def position_generator():
			x = np.random.randint(x_range[0], x_range[1], size=1)[0]
			y = np.random.randint(y_range[0], y_range[1], size=1)[0]
			return x, y
		return position_generator

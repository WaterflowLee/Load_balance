#!coding:utf-8
import numpy as np
np.random.seed(0)
from collections import defaultdict


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
		self._service_pool = {}
		self._service_access_log = defaultdict(int)
		Machine.static_unique_id += 1

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
	def load(self):
		return np.sum(self._service_access_log.values())

	@property
	def service_access_log(self):
		return self._service_access_log

	def service_access_logging(self, request, mode=True):
		if mode:
			self._service_access_log[request.service.unique_id] += 1
		else:
			self._service_access_log[request.service.unique_id] -= 1

	@property
	def service_pool(self):
		return self._service_pool

	# request : Request Object
	def receive_request(self, request):
		self._request_queue.append(request)
		self.service_access_logging(request)

	# request : Request Object
	def serve_request(self, request):
		if isinstance(request, list):
			for r in request:
				consumed_bandwidth = r.service.consumed_bandwidth
				self._cur_bandwidth -= consumed_bandwidth
				self._request_queue.remove(r)
				self._serving_request.append(r)
		else:
			consumed_bandwidth = request.service.consumed_bandwidth
			self._cur_bandwidth -= consumed_bandwidth
			self._request_queue.remove(request)
			self._serving_request.append(request)
		return self

	def stop_serving_request(self, request):
		if isinstance(request, list):
			for r in request:
				self._serving_request.remove(r)
				self._request_queue.append(r)
				self._cur_bandwidth += r.service.consumed_bandwidth
		else:
			self._serving_request.remove(request)
			self._request_queue.append(request)
			self._cur_bandwidth += request.service.consumed_bandwidth

	def send_request(self, request):
			if isinstance(request, list):
				for r in request:
					self._request_queue.remove(r)
					self.service_access_logging(r, mode=False)
					if r.machine.unique_id == self.unique_id:
						# send forth
						r.source = self
					else:
						# send back
						r.source = "user"
			else:
				self._request_queue.remove(request)
				self.service_access_logging(request, mode=False)
				if request.machine.unique_id == self.unique_id:
					# send forth
					request.source = self
				else:
					# send back
					request.source = "user"

	# service : Service Object
	def deploy_service(self, service):
		self._service_pool[service.unique_id] = service
		self._cur_ram -= service.consumed_ram
		service.deploy(self)
		return self


class Service(CommonObject):
	static_unique_id = 0

	def __init__(self, ram, bandwidth):
		super(Service, self).__init__(Service.static_unique_id)
		self._ram = ram
		self._bandwidth = bandwidth
		self._deployment = []
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

	@property
	def machine(self):
		return self._machine

	@property
	def service(self):
		return self._service

	@property
	def source(self):
		return self._source


class District(CommonObject):
	# 用类属性保证区域的唯一性
	# 让类本身来管理自己的实例
	districts = {}
	max_radius = 0

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

	@property
	def machines(self):
		return [self._master] + self._slaves

	@property
	def master(self):
		return self._master

	@property
	def slaves(self):
		return self._slaves

	def add_slave(self, slave):
		if isinstance(slave, list):
			for s in slave:
				self._slaves.append(s)
				s.district = self
		else:
			self._slaves.append(slave)
			slave.district = self

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
			for service in machine.service_pool:
				service_deploy_log[service.unique_id].append(machine)
		return service_deploy_log


class Simulator(object):
	def __init__(self, machine_num, service_num, request_num):
		super(Simulator, self).__init__()
		self._machine_num = machine_num
		self._service_num = service_num
		self._request_num = request_num
		self._machines = {}
		self._services = {}
		self._requests = {}
		self._machine_ram_generator = None
		self._service_ram_generator = None
		self._machine_bandwidth_generator = None
		self._service_bandwidth_generator = None
		self._request_generator = None
		self._position_generator = None

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
			self._machines[m.unique_id] = m
		return self

	def service_factory(self):
		for _ in range(self._service_num):
			s = Service(self._service_ram_generator(), self._service_bandwidth_generator())
			self._services[s.unique_id] = s
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
	def machine_average_bandwidth(self):
		bandwidth = 0
		for machine in self._machines.values():
			bandwidth += machine.bandwidth
		return float(bandwidth)/self._machine_num


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

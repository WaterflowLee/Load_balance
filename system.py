import numpy as np
np.random.seed(0)


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
	def __init__(self, ram, bandwidth, position, unique_id):
		super(Machine, self).__init__(unique_id)
		self._ram = ram
		self._bandwidth = bandwidth
		self._position = position
		self._cur_ram = ram
		self._cur_bandwidth = bandwidth

	@property
	def position(self):
		return self._position

	@property
	def cur_ram(self):
		return self._cur_ram

	@cur_ram.setter
	def cur_ram(self, r):
		self._cur_ram = r

	@property
	def cur_bandwidth(self):
		return self._cur_bandwidth

	@cur_bandwidth.setter
	def cur_bandwidth(self, b):
		self._cur_bandwidth = b


class Service(CommonObject):
	def __init__(self, ram, bandwidth, unique_id):
		super(Service, self).__init__(unique_id)
		self._ram = ram
		self._bandwidth = bandwidth


class Request(CommonObject):
	def __init__(self, machine_service_tuple, unique_id):
		super(Request, self).__init__(unique_id)
		self._machine = machine_service_tuple[0]
		self._service = machine_service_tuple[1]

	@property
	def machine(self):
		return self._machine

	@property
	def service(self):
		return self._service



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
		for i in range(self._machine_num):
			m = Machine(self._machine_ram_generator(), self._machine_bandwidth_generator(), self._position_generator(), i)
			self._machines[i] = m
		return self

	def service_factory(self):
		for i in range(self._service_num):
			s = Service(self._service_ram_generator(), self._service_bandwidth_generator(), i)
			self._services[i] = s
		return self

	def request_factory(self):
		for i in range(self._request_num):
			r = Request(self._request_generator(), i)
			self._requests[i] = r
		return self

	def active(self):
		self.machine_factory()
		self.service_factory()
		self.request_factory()

	@property
	def machines(self):
		return self._machines

	@property
	def services(self):
		return self._services

	@property
	def requests(self):
		return self._requests


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

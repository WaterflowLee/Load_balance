from collections import defaultdict
# from functools import partial
import numpy as np
np.random.seed(0)


# def saiyanize_machine(machine):
	# machine.ask_for_help = partial(ask_for_help, machine)


class RandomDispatcher(object):
	def __init__(self, sim):
		super(RandomDispatcher, self).__init__()
		self._simulator = sim

	def service_deploy(self):
		service_access_log = defaultdict(int)
		for machine in self._simulator.machines.values():
			for service_id, time in machine.service_access_log.items():
				service_access_log[service_id] += time

		service_consumed_bandwidth = map(lambda x: (x[0], x[1]*self._simulator.services[x[0]].consumed_bandwidth), service_access_log.items())
		service_deployed_machine_num = map(lambda x: (x[0], x[1]/self._simulator.machine_average_bandwidth), service_consumed_bandwidth)

		for service_id, num in service_deployed_machine_num:
			service = self._simulator.services[service_id]
			for _ in range(int(num)):
				machine_id = np.random.randint(0, self._simulator.machine_num, 1)
				machine = self._simulator.machines[machine_id[0]]
				if machine.cur_ram > service.consumed_ram:
					machine.deploy_service(service)

	def delay_sum(self):
		delay_sum = 0
		for r in self._simulator.requests.values():
			delay_sum += r.delay
		print "Sum of delay is %s" % delay_sum

	def dispatch(self):
		for machine in self._simulator.machines.values():
			machine.startup()
			machine.ask_for_help()

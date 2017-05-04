from collections import defaultdict


class NaiveDispatcher(object):
	def __init__(self, sim):
		super(NaiveDispatcher, self).__init__()
		self._simulator = sim

	def service_deploy(self):
		service_access = defaultdict(int)
		for machine in self._simulator.machines.values():
			for service_id, time in machine.service_access_log.items():
				service_access[service_id] += time

		service_consumed_bandwidth = map(lambda x: (x[0], x[1]*self._simulator.services[x[0]].consumed_bandwidth), service_access.items())
		service_deployed_machine_num = dict(map(lambda x: (x[0], x[1]/self._simulator.machine_average_bandwidth), service_consumed_bandwidth))

		service_id_list = sorted(service_access, key=lambda k: service_access[k], reverse=True)
		for service_id in service_id_list:
			service = self._simulator.services[service_id]
			spots_num = service_deployed_machine_num[service_id]
			hot_machine_for_this_service = \
				sorted(self._simulator.machines.values(), key=lambda m: m.service_access_log.get(service_id, 0), reverse=True)
			cur_spots_num = 0
			for machine in hot_machine_for_this_service:
				if machine.cur_ram > service.consumed_ram:
					cur_spots_num += 1
					machine.deploy_service(service)
				if cur_spots_num >= spots_num:
					break

	def delay_sum(self):
		delay_sum = 0
		for r in self._simulator.requests.values():
			delay_sum += r.delay
		print "Sum of delay is %s" % delay_sum

	def dispatch(self):
		for machine in self._simulator.machines.values():
			machine.startup()
			machine.ask_for_help()

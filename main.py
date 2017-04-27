#!coding:utf-8
from system import ram_generator_factory,\
	bandwidth_generator_factory, request_generator_factory, position_generator_factory, Simulator
from dispatcher import Dispatcher

machine_num = 100
service_num = 20
request_num = 1000
sim = Simulator(machine_num, service_num, request_num)

machine_ram_generator = ram_generator_factory(1024, 1024*4)
service_ram_generator = ram_generator_factory(128, 128*2)

machine_bandwidth_generator = bandwidth_generator_factory(100, 100*4)
service_bandwidth_generator = bandwidth_generator_factory(10, 10*2)

position_generator = position_generator_factory((0, 1000), (0, 1000))

# chain rule
sim.ram_generator([machine_ram_generator, service_ram_generator])\
	.bandwidth_generator([machine_bandwidth_generator, service_bandwidth_generator])\
	.position_generator(position_generator)

# sim.active() 等价于 sim.machine_factory().service_factory()
sim.machine_factory().service_factory()

service_id_list = sim.services.keys()
machine_id_list = sim.machines.keys()
request_generator = request_generator_factory(machine_id_list, service_id_list)

sim.request_generator(request_generator).request_factory()


print sim.services

dispatcher = Dispatcher(sim, 10, 300)\
	.machine_slave_dispatch_round_0().machine_slave_dispatch_round_1().machine_slave_dispatch_round_2()
# dispatcher.print_info()
# print dispatcher._district_machine_dispatch_result[98]

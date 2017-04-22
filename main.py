#!coding:utf-8
from system import ram_generator_factory,\
	bandwidth_generator_factory, request_generator_factory, position_generator_factory, Simulator

machine_num = 100
service_num = 20
request_num = 1000
sim = Simulator(machine_num, service_num, request_num)

machine_ram_generator = ram_generator_factory(1024, 1024*4)
service_ram_generator = ram_generator_factory(128, 128*2)

machine_bandwidth_generator = bandwidth_generator_factory(100, 100*4)
service_bandwidth_generator = bandwidth_generator_factory(10, 10*2)

machine_id_list = range(machine_num)
service_id_list = range(service_num)
request_generator = request_generator_factory(machine_id_list, service_id_list)

position_generator = position_generator_factory((0, 1000), (0, 1000))

# chain rule
sim.ram_generator([machine_ram_generator, service_ram_generator])\
	.bandwidth_generator([machine_bandwidth_generator, service_bandwidth_generator])\
	.request_generator(request_generator)\
	.position_generator(position_generator)

# sim.active() 等价于 sim.machine_factory().service_factory().request_factory()
sim.machine_factory().service_factory().request_factory()
print sim._services[2].unique_id

#!coding:utf-8
from system import ram_generator_factory,\
	bandwidth_generator_factory, request_generator_factory, position_generator_factory, Simulator
from random_dispatcher import RandomDispatcher
from naive_dispatcher import NaiveDispatcher
from dispatcher import Dispatcher
import numpy as np
import copy


machine_num = 100
service_num = 20
request_num = 1000
backend_distance = np.sqrt(2) * 1000

sim = Simulator(machine_num, service_num, request_num, backend_distance)

machine_ram_generator = ram_generator_factory(1024, 1024*4)
service_ram_generator = ram_generator_factory(128, 128*2)

machine_bandwidth_generator = bandwidth_generator_factory(100, 100*4)
service_bandwidth_generator = bandwidth_generator_factory(10, 10*2)

position_generator = position_generator_factory((0, 1000), (0, 1000))

sim.ram_generator([machine_ram_generator, service_ram_generator])\
	.bandwidth_generator([machine_bandwidth_generator, service_bandwidth_generator])\
	.position_generator(position_generator)

# sim.machine_factory().service_factory().cal_distance_matrix()
sim.active().cal_distance_matrix()

service_id_list = sim.services.keys()
machine_id_list = sim.machines.keys()
request_generator = request_generator_factory(machine_id_list, service_id_list)

sim.request_generator(request_generator).request_factory()

random_dispatcher = RandomDispatcher(copy.deepcopy(sim))
random_dispatcher.service_deploy()
random_dispatcher.dispatch()
random_dispatcher.delay_sum()

naive_dispatcher = NaiveDispatcher(copy.deepcopy(sim))
naive_dispatcher.service_deploy()
naive_dispatcher.dispatch()
naive_dispatcher.delay_sum()

# 代码结构限制使得这个调度器的sim不能使用拷贝
dispatcher = Dispatcher(sim, 20, 100)
dispatcher.stage_1()
# dispatcher.stage_2_ergodic()
dispatcher.stage_2()

import pickle
import json


class Logger(object):
	loggers = {}
	log_filename = None
	on_off = True

	@classmethod
	def persist(cls):
		if cls.on_off:
			if cls.log_filename:
				# pickle.dump(cls.loggers, open(cls.log_filename, "w"))
				json.dump(cls.loggers, open(cls.log_filename, "w"))
				print "Logs persistence is successful"
			else:
				raise ValueError("The class attribute log_filename must be set, current value is None")
		else:
			print "The Logger switch is turned off"

	def __init__(self, name):
		super(Logger, self).__init__()
		self.name = name
		self.logs = []
		Logger.loggers[self.name] = self.logs

	def log(self, content):
		if Logger.on_off:
			self.logs.append(content)


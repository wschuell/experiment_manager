#!/usr/bin/python
# -*- coding: latin-1 -*-

import copy

class CustomFunc(object):

	def __init__(self,func,*level,**kwargs):
		if len(level)!=0:
			self.level=level[0]

		def dataname(data):
			out=""
			try:
				out=data.__name__
			except AttributeError:
				pass
			return out

		def yname(data):
			out=""
			try:
				out=func.__name__+"("+data.__name__+")"
			except AttributeError:
				pass
			return out

		self.func=func
		self.graph_config={"xlabel":dataname,"ylabel":yname}
		for key, value in kwargs.iteritems():
			self.graph_config[key]=value
		self.graph_config_temp={}
		#self.graph_config_temp=copy.deepcopy(self.graph_config)

	def apply(self,data,**kwargs):
		for key,value in self.graph_config.iteritems():
			self.graph_config_temp[key]=value(data)
		if "progress_info" in kwargs.keys():
			return self.func(data,progress_info=kwargs["progress_info"])
		return self.func(data)

	def modify_graph_config(self,**kwargs):
		tempcfg=self.graph_config_temp
		for key, value in kwargs.iteritems():
			self.graph_config_temp[key]=value


	def get_graph_config(self):
		return self.graph_config_temp

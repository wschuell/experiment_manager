#!/usr/bin/python

import matplotlib.pyplot as plt
import time
import numpy as np
import pickle
import copy

import matplotlib
import seaborn as sns
#sns.set(rc={'image.cmap': 'Purples_r'})

sns.set_style('darkgrid')
matplotlib.rcParams['pdf.fonttype'] = 42  #set font type to true type, avoids possible incompatibility while submitting papers
matplotlib.rcParams['ps.fonttype'] = 42

def load_graph(filename):
	with open(filename, 'rb') as fichier:
		mon_depickler=pickle.Unpickler(fichier)
		tempgr=mon_depickler.load()
	return tempgr


class CustomGraph(object):
	def __init__(self,Y,*arg,**kwargs):
		self.keepwinopen=0
		self.sort=1
		self.filename="graph"+time.strftime("%Y%m%d%H%M%S", time.localtime())
		if "filename" in kwargs.keys():
			self.filename=kwargs["filename"]
		self.title=self.filename
		self.xlabel="X"
		self.ylabel="Y"
		self.alpha=0.3

		self.Yoptions=[{}]
		self.legendoptions = {}
		self.legend_permut = []

		self.xmin=[0,0]
		self.xmax=[0,5]
		self.ymin=[0,0]
		self.ymax=[0,5]

		self.std=0

		self._Y=[Y]
		self.stdvec=[0]*len(Y)

		if len(arg)!=0:
			self._X=[Y]
			self._Y=[arg[0]]
		else:
			self._X=[range(0,len(Y))]


		self.extensions=["eps","png","pdf"]

		for key,value in kwargs.iteritems():
			setattr(self,key,value)

		self.stdvec=[self.stdvec]

		if not isinstance(self.xmin,list):
			temp=self.xmin
			self.xmin=[1,temp]
		if not isinstance(self.xmax,list):
			temp=self.xmax
			self.xmax=[1,temp]
		if not isinstance(self.ymin,list):
			temp=self.ymin
			self.ymin=[1,temp]
		if not isinstance(self.ymax,list):
			temp=self.ymax
			self.ymax=[1,temp]

		self.init_time=time.strftime("%Y%m%d%H%M%S", time.localtime())
		self.modif_time=time.strftime("%Y%m%d%H%M%S", time.localtime())

	def show(self):
		plt.figure()
		plt.ion()
		self.draw()
		plt.show()

	def save(self,*path):
		if path:
			out_path=path[0]
		else:
			out_path="graphs/"
		with open(out_path+self.filename+".b", 'wb') as fichier:
			mon_pickler=pickle.Pickler(fichier)
			mon_pickler.dump(self)

	def write_files(self,*path):
		if len(path)!=0:
			out_path=path[0]
		else:
			out_path=""

		self.save(out_path)
		self.draw()
		for extension in self.extensions:
			plt.savefig(out_path+self.filename+"."+extension,format=extension,bbox_inches='tight')


	def draw(self):

		#colormap=['blue','black','green','red','yellow','cyan','magenta']
		#colormap=['black','green','red','blue','yellow','cyan','magenta']
		#colormap=['blue','red','green','black','yellow','cyan','magenta']
		#colormap=['black','green','blue','red','yellow','cyan','magenta']
		#colormap=['darkolivegreen','green','darkorange','red','yellow','cyan','magenta']


		#plt.figure()
		plt.ion()
		plt.cla()
		plt.clf()
		current_palette=sns.color_palette()
		for i in range(0,len(self._Y)):

			Xtemp=copy.deepcopy(self._X[i])
			Ytemp=copy.deepcopy(self._Y[i])
			stdtemp=copy.deepcopy(self.stdvec[i])
			if self.sort: # WARNING!!!!! No X value should appear 2 times -> bug to solve
				tempdic={}
				for j in range(0,len(Xtemp)):
					tempdic[Xtemp[j]]=[Ytemp[j],stdtemp[j]]
				temptup=sorted(tempdic.items())
				for j in range(0,len(temptup)):
					Xtemp[j]=temptup[j][0]
					Ytemp[j]=temptup[j][1][0]
					stdtemp[j]=temptup[j][1][1]

			base_line=plt.plot(Xtemp,Ytemp,**self.Yoptions[i])[0]
			if self.std:
				Ytempmin=[0]*len(Ytemp)
				Ytempmax=[0]*len(Ytemp)
				for j in range(0,len(Ytemp)):
					Ytempmax[j]=Ytemp[j]+stdtemp[j]
					Ytempmin[j]=Ytemp[j]-stdtemp[j]
				if 'color' in self.Yoptions[i].keys():
					plt.fill_between(Xtemp,Ytempmin,Ytempmax, alpha=self.alpha,**self.Yoptions[i])
				else:
					plt.fill_between(Xtemp,Ytempmin,Ytempmax, alpha=self.alpha, facecolor=base_line.get_color(), **self.Yoptions[i])

		plt.xlabel(self.xlabel)
		plt.ylabel(self.ylabel)
		plt.title(self.title)

		if self.xmin[0]:
			plt.xlim(xmin=self.xmin[1])
		if self.xmax[0]:
			plt.xlim(xmax=self.xmax[1])
		if self.ymin[0]:
			plt.ylim(ymin=self.ymin[1])
		if self.ymax[0]:
			plt.ylim(ymax=self.ymax[1])

		handles, labels = plt.axes().get_legend_handles_labels()
		handles2, labels2 = [], []
		for tr in range(len(self.legend_permut)):
			handles2.append(handles[self.legend_permut[tr]])
			#handles2[self.legend_permut[tr]] = handles[tr]
			labels2.append(labels[self.legend_permut[tr]])
			#labels2[self.legend_permut[tr]] = labels[tr]



		plt.legend(handles2, labels2, **self.legendoptions)

		#plt.legend(bbox_to_anchor=(0,0,0.55,0.8))
		#plt.legend(bbox_to_anchor=(0,0,0.5,1))
		#
		#plt.legend(bbox_to_anchor=(0,0,1,0.7))
		#plt.legend(bbox_to_anchor=(0,0,1,0.54))
		if hasattr(self, 'fontsize'):
			matplotlib.rcParams['font.size'] = self.fontsize
			matplotlib.rcParams['xtick.labelsize'] = self.fontsize
			matplotlib.rcParams['ytick.labelsize'] = self.fontsize
			matplotlib.rcParams['axes.titlesize'] = self.fontsize
			matplotlib.rcParams['axes.labelsize'] = self.fontsize
			matplotlib.rcParams['legend.fontsize'] = self.fontsize
		if hasattr(self, 'rcparams'):
			for key,value in self.rcparams:
				matplotlib.rcParams[key] = value
		plt.draw()


	def add_graph(self,other_graph):
		self._X=self._X+other_graph._X
		self._Y=self._Y+other_graph._Y
		self.Yoptions=self.Yoptions+other_graph.Yoptions
		self.stdvec=self.stdvec+other_graph.stdvec

	def complete_with(self,other_graph):
		for i in range(0,len(self._X)):
			self._X[i]=list(copy.deepcopy(self._X[i]))+list(copy.deepcopy(other_graph._X[i]))
			self._Y[i]=list(copy.deepcopy(self._Y[i]))+list(copy.deepcopy(other_graph._Y[i]))
			self.stdvec[i]=list(copy.deepcopy(self.stdvec[i]))+list(copy.deepcopy(other_graph.stdvec[i]))
		self.modif_time=time.strftime("%Y%m%d%H%M%S", time.localtime())

	def merge(self):
		Yarray=np.array(self._Y)
		stdarray=np.array(self.stdvec)
		stdtemp=[]
		Ytemp=[]
		self.Yoptions=[self.Yoptions[0]]
		self.std=1

		for i in range(0,len(self._Y[0])):
			Ytemp.append(np.mean(list(Yarray[:,i])))
			stdtemp.append(np.std(list(Yarray[:,i])))
		self._Y=[Ytemp]
		self.stdvec=[stdtemp]
		self._X=[self._X[0]]
		self.modif_time=time.strftime("%Y%m%d%H%M%S", time.localtime())



	def wise_merge(self):
		param_list=[]
		for i in range(len(self.Yoptions)):
			param_list.append(self.Yoptions[i]["label"])
		param_values={}
		for ind,param in enumerate(param_list):
			if param not in param_values.keys():
				param_values[param]=copy.deepcopy(self)
				param_values[param]._X=[self._X[ind]]
				param_values[param]._Y=[self._Y[ind]]
				param_values[param].Yoptions=[self.Yoptions[ind]]
			else:
				tempgraph=copy.deepcopy(self)
				tempgraph._X=[self._X[ind]]
				tempgraph._Y=[self._Y[ind]]
				tempgraph.Yoptions=[self.Yoptions[ind]]
				param_values[param].add_graph(copy.deepcopy(tempgraph))
		tempgraph=copy.deepcopy(self)
		tempgraph._X=[]
		tempgraph._Y=[]
		tempgraph.Yoptions=[]
		tempgraph.stdvec=[]
		for key in param_values.keys():
			param_values[key].merge()
			tempgraph.add_graph(param_values[key])
		self.modif_time=time.strftime("%Y%m%d%H%M%S", time.localtime())
		return tempgraph

	def empty(self):
		self._Y=[]
		self._X=[]
		self.Yoptions=[]
		self.stdvec=[]
		self.modif_time=time.strftime("%Y%m%d%H%M%S", time.localtime())


	def func_of(self,graph2):
		newgraph=copy.deepcopy(self)
		for i in range(0,len(newgraph._X)):
			newgraph._X[i]=graph2._Y[i]
			newgraph.xlabel=graph2.title[6:]
			newgraph.title=self.title+"_func_of_"+newgraph.xlabel
		return newgraph

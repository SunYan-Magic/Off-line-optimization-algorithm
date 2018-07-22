#!/usr/bin/env python
#coding:utf-8

import os
import sys
import pdb
import xlrd
import threading
import datetime
import time
import logging
import logging.config
import traceback
import calendar
from apscheduler.schedulers.blocking import BlockingScheduler
from collections import Counter
import YN_configuration as config
import YN_Kairosdb as kai
import YN_mongo as mg
import numpy
import json

class RECUM(object):
	def __init__(self):
		print '-------ini start-------'
		#---------conn----
		self.mongo = mg.Mongo(config.read('Mongo', 'Server'), int(config.read('Mongo', 'Port')))	
		
		self.mongo.connection()

		self.kairos = kai.KairosDB(config.read('Kairos', 'Server'), config.read('Kairos', 'Port'))
		
		#self.mysql = my.MySQLInter()
		
		#----------------
		self.project = config.get('Project', 'Name')
		self.companys_list = self.mongo.getCompanys(self.project)
		
		#---------------dict-------------
		#=======================MODIFY(外层是type的字典)========================
		self.WT_Farms_Dict = self.mongo.getFarms_ByProjectByJoin(self.project, 'WT')
		self.PV_Farms_Dict = self.mongo.getFarms_ByProjectByJoin(self.project, 'PV')
		'''
		self.farms_dict = {'WT':self.WT_Farms_Dict, 'PV':self.PV_Farms_Dict}
		self.farmTypeList = ['WT', 'PV']
		self.periods_dict = self.mongo.getPeriods_ByFarmDict(self.project, self.farms_dict)
		
		self.devs_dict, self.devKeys_dict = self.getDevs_ByPeriod(self.periods_dict)
		
		self.periodKeys_list = self.getKeys_ByPeriod(self.periods_dict)
		self.company_dict, self.company_keys_dict = self.getFarms_ByCompany()
		#-------keys---------------
		self.companyKey_list = [':'.join([self.project, company]) for company in self.companys_list]
		self.farmKeys_list = self.getFarmKeyByFarm()
		
		self.windMeasur_list = self.getWindMeasurByFarmKey()
		self.devKeys_list = self.getKeys_ByDev(self.devs_dict)
		
		self.TypekeyList, self.keyListWithOutCompPro, self.keyListGroup = [], [], []
		self.TypekeyList, self.keyListWithOutCompPro, self.keyListGroup = self.getKeyList()
		self.all_keyList = []
		self.all_keyList.append(self.project)
		self.all_keyList.extend(self.companyKey_list)
		self.all_keyList.extend(self.keyListWithOutCompPro)
		# TypekeyList形如{type[keylist]};keyListWithOutCompPro形如[keylist]
		
		self.companyDicts = self.getDevs_ByCompany()
		self.capDicts = self.getCapByFarms()
		self.devTypeDict = self.getDevTypeByDev()
		
		self.production_plan = self.mongo.getTagsByStatisticsTags('CMPT_ProductionPlan')
		
		self.wt_devTypes = self.mongo.getDevTypesByType(self.project, 'wtg')
		#--------tags--------------
		self.devType = ['wtg', 'pv_inverter']
		self.devTypeList = []
		self.devTypeList = self.mongo.getAllDevTypeByType(self.devType)
		
		self.all_dev_obj_dict = self.getObj_ByAllDev()
		self.Caps = self.getCap()
		self.PF = self.getPF()
		self.lines = self.getLineByPeriod()
		'''
		self.Structure = self.mongo.getAllDevsByProject(self.project)
		self.Structure_Dict, self.Count_Dict, self.Cap_Dict, self.AllKeyList, self.CompanyKeyList, self.PV_FarmKeyList, self.WT_FarmKeyList, self.PV_PeriodKeyList, self.WT_PeriodKeyList, self.PV_LineKeyList, self.WT_LineKeyList, self.PV_DevKeyList, self.WT_DevKeyList, self.PVKeyList, self.WTKeyList = self.getAllDict()
		self.devType = self.mongo.getAllDevType()
		self.DevType_Dict = self.mongo.getAllDevTypeByType_new(self.devType)
		print '-------ini end-------'
	def getAllDict(self):
		all_dict, count_dict, cap_dict = {}, {}, {}
		List = []
		project_count = 0
		allkeylist, companykeylist, pv_farmkeylist, wt_farmkeylist, pv_linekeylist, wt_linekeylist, pv_periodkeylist, wt_periodkeylist, pv_devkeylist, wt_devkeylist , Pvkeylist, Wtkeylist= [], [], [], [], [], [], [], [], [], [], [], []
		cap_dict[self.project] = self.mongo.getCapacityByProject(self.project) * 1000.0
		for company in self.Structure:
			company_count = 0
			company_key = ':'.join([self.project, company])
			companykeylist.append(company_key)
			all_dict[company_key] = {}
			cap = self.mongo.getCapacityByCompany(self.project, company)
			cap_dict[company_key] = cap*1000.0
			type_key_WT = ':'.join([self.project, company, 'WT'])
			Pvkeylist.append(type_key_WT)
			type_key_PV = ':'.join([self.project, company, 'PV'])
			Wtkeylist.append(type_key_PV)
			all_dict[company_key][type_key_WT] = {}
			all_dict[company_key][type_key_PV] = {}
			for farm in self.Structure[company]:
				farm_count_wt = 0
				farm_count_pv = 0
				farm_key = ':'.join([self.project, farm])
				cap = self.mongo.getCapacityByFarm(self.project, farm)
				cap_dict[farm_key] = cap*1000.0
				if farm in self.WT_Farms_Dict:
					wt_farmkeylist.append(farm_key)
					all_dict[company_key][type_key_WT][farm_key] = {}
					for period in self.Structure[company][farm]:
						period_count = 0
						period_key = ':'.join([self.project, farm, period])
						wt_periodkeylist.append(period_key)
						all_dict[company_key][type_key_WT][farm_key][period_key] = {}
						cap = self.mongo.getCapacityByPeriod(self.project, farm, period)
						cap_dict[period_key] = cap * 1000.0
						for line in self.Structure[company][farm][period]:
							line_count = 0
							line_key = ':'.join([self.project, farm, line])
							wt_linekeylist.append(line_key)
							all_dict[company_key][type_key_WT][farm_key][period_key][line_key] = []
							own_dev = []
							linecap = 0.0
							for dev in self.Structure[company][farm][period][line]:
								dev_key = ':'.join([self.project, farm, dev])
								own_dev.append(dev_key)
								wt_devkeylist.append(dev_key)
								line_count += 1
								count_dict[dev_key] = 1
								cap = self.mongo.getCapacityByDevice(self.project, farm, dev)
								cap_dict[dev_key] = cap
								linecap = linecap + cap
							cap_dict[line_key] = linecap
							all_dict[company_key][type_key_WT][farm_key][period_key][line_key] = own_dev
							period_count += line_count
							count_dict[line_key] = line_count
						count_dict[period_key] = period_count
						farm_count_wt += period_count
					count_dict[type_key_WT] = farm_count_wt
				if farm in self.PV_Farms_Dict:
					pv_farmkeylist.append(farm_key)
					all_dict[company_key][type_key_PV][farm_key] = {}
					for period in self.Structure[company][farm]:
						period_count = 0
						period_key = ':'.join([self.project, farm, period])
						pv_periodkeylist.append(period_key)
						all_dict[company_key][type_key_PV][farm_key][period_key] = {}
						cap = self.mongo.getCapacityByPeriod(self.project, farm, period)
						cap_dict[period_key] = cap * 1000.0
						for line in self.Structure[company][farm][period]:
							line_count = 0
							line_key = ':'.join([self.project, farm, line])
							pv_linekeylist.append(line_key)
							all_dict[company_key][type_key_PV][farm_key][period_key][line_key] = []
							own_pv = []
							linecap = 0.0
							for dev in self.Structure[company][farm][period][line]:
								dev_key = ':'.join([self.project, farm, dev])
								own_pv.append(dev_key)
								pv_devkeylist.append(dev_key)
								line_count += 1
								count_dict[dev_key] = 1
								cap = self.mongo.getCapacityByDevice(self.project, farm, dev)
								cap_dict[dev_key] = cap
								linecap = linecap + cap
							cap_dict[line_key] = linecap
							all_dict[company_key][type_key_PV][farm_key][period_key][line_key] = own_pv
							period_count += line_count
							count_dict[line_key] = line_count
						count_dict[period_key] = period_count
						farm_count_pv += period_count
					count_dict[type_key_PV] = farm_count_pv
				farm_count = farm_count_wt + farm_count_pv
				count_dict[farm_key] = farm_count
				company_count +=farm_count
			count_dict[company_key] = company_count
			project_count += company_count
		count_dict[self.project] = project_count
		allkeylist.append(self.project)
		allkeylist.extend(companykeylist)
		allkeylist.extend(pv_farmkeylist)
		allkeylist.extend(wt_farmkeylist)
		allkeylist.extend(pv_periodkeylist)
		allkeylist.extend(wt_periodkeylist)
		allkeylist.extend(pv_devkeylist)
		allkeylist.extend(wt_devkeylist)
		return all_dict, count_dict, cap_dict, allkeylist, companykeylist, pv_farmkeylist, wt_farmkeylist, pv_periodkeylist, wt_periodkeylist, pv_linekeylist, wt_linekeylist, pv_devkeylist, wt_devkeylist, Pvkeylist, Wtkeylist
	
	def setDataLine(self, value_dict, date, yesterday, tagName, option, key, flag):
		#修改对象key是device级别，则修改line级及以上对应tag的值
		taglist, datalist, Type_list = [], [], []
		company_list = []
		for company in self.Structure_Dict :
			type_list = []
			for farmtype in self.Structure_Dict[company] :
				farm_list = []
				for farm in self.Structure_Dict[company][farmtype]:
					period_list = []
					for period in self.Structure_Dict[company][farmtype][farm]:
						line_list = []
						for line in self.Structure_Dict[company][farmtype][farm][period]:
							dev_list = []
							for dev in self.Structure_Dict[company][farmtype][farm][period][line]:
								if tagName in value_dict and dev in value_dict[tagName] and date in value_dict[tagName][dev]:
									try:
										value = float(value_dict[tagName][dev][date])
										dev_list.append(value)
									except:
										dev_list.append(0.0)
								#else?????
							if option == 'max':
								temp = round(max(dev_list), 4)
							elif option == 'min':
								temp = round(min(dev_list), 4)
							elif option == 'avg_rate':
								temp = round(numpy.mean(dev_list) * 100, 4)
							elif option == 'avg':
								temp = round(numpy.mean(dev_list), 4)
							elif option == 'sum':
								temp = round(sum(dev_list), 4)
							line_list.append(temp)
							if flag ==1 and date != yesterday:
								taglist.append({'object':line, 'date':date})
								datalist.append({tagName:temp})
								taglist.append({'object':line, 'date':yesterday})
								datalist.append({tagName:temp})
							else:
								taglist.append({'object':line, 'date':date})
								datalist.append({tagName:temp})
						if option == 'max':
							temp = round(max(line_list), 4)
						elif option == 'min':
							temp = round(min(line_list), 4)
						elif option == 'avg_rate':
							temp = round(numpy.mean(line_list) * 100, 4)
						elif option == 'avg':
							temp = round(numpy.mean(line_list), 4)
						elif option == 'sum':
							temp = round(sum(line_list), 4)
						period_list.append(temp)
						if flag ==1 and date != yesterday:
							taglist.append({'object':period, 'date':date})
							datalist.append({tagName:temp})
							taglist.append({'object':period, 'date':yesterday})
							datalist.append({tagName:temp})
						else:
							taglist.append({'object':period, 'date':date})
							datalist.append({tagName:temp})
					if option == 'max':
						temp = round(max(period_list), 4)
					elif option == 'min':
						temp = round(min(period_list), 4)
					elif option == 'avg_rate':
						temp = round(numpy.mean(period_list) * 100, 4)
					elif option == 'avg':
						temp = round(numpy.mean(period_list), 4)
					elif option == 'sum':
						temp = round(sum(period_list), 4)
					farm_list.append(temp)
					if flag ==1 and date != yesterday:
						taglist.append({'object':farm, 'date':date})
						datalist.append({tagName:temp})
						taglist.append({'object':farm, 'date':yesterday})
						datalist.append({tagName:temp})
					else:
						taglist.append({'object':farm, 'date':date})
						datalist.append({tagName:temp})
				if option == 'max':
					temp = round(max(farm_list), 4)
				elif option == 'min':
					temp = round(min(farm_list), 4)
				elif option == 'avg_rate':
					temp = round(numpy.mean(farm_list) * 100, 4)
				elif option == 'avg':
					temp = round(numpy.mean(farm_list), 4)
				elif option == 'sum':
					temp = round(sum(farm_list), 4)
				type_list.append(temp)
				if flag ==1 and date != yesterday:
					taglist.append({'object':farmtype, 'date':date})
					datalist.append({tagName:temp})
					taglist.append({'object':farmtype, 'date':yesterday})
					datalist.append({tagName:temp})
				else:
					taglist.append({'object':farmtype, 'date':date})
					datalist.append({tagName:temp})
			if option == 'max':
				temp = round(max(type_list), 4)
			elif option == 'min':
				temp = round(min(type_list), 4)
			elif option == 'avg_rate':
				temp = round(numpy.mean(type_list) * 100, 4)
			elif option == 'avg':
				temp = round(numpy.mean(type_list), 4)
			elif option == 'sum':
				temp = round(sum(type_list), 4)
			company_list.append(temp)
			if flag ==1 and date != yesterday:
				taglist.append({'object':company, 'date':date})
				datalist.append({tagName:temp})
				taglist.append({'object':company, 'date':yesterday})
				datalist.append({tagName:temp})
			else:
				taglist.append({'object':company, 'date':date})
				datalist.append({tagName:temp})
		if option == 'max':
			temp = round(max(company_list), 4)
		elif option == 'min':
			temp = round(min(company_list), 4)
		elif option == 'avg_rate':
			temp = round(numpy.mean(company_list) * 100, 4)
		elif option == 'avg':
			temp = round(numpy.mean(company_list), 4)
		elif option == 'sum':
			temp = round(sum(company_list), 4)
		if flag ==1 and date != yesterday:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
			taglist.append({'object':self.project, 'date':yesterday})
			datalist.append({tagName:temp})
		else:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})

		for typee in self.DevType_Dict:
			if key in self.DevType_Dict[typee]:
				Type = typee
				for dev in self.DevType_Dict[typee]:
					if tagName in value_dict and dev in value_dict[tagName] and date in value_dict[tagName][dev]:
						try:
							value = float(value_dict[tagName][dev][date])
							Type_list.append(value)
						except:
							Type_list.append(0.0)
					else :
						Type_list.append(0.0)
				if option == 'max':
					temp = round(max(Type_list), 4)
				elif option == 'min':
					temp = round(min(Type_list), 4)
				elif option == 'avg_rate':
					temp = round(numpy.mean(Type_list) * 100, 4)
				elif option == 'avg':
					temp = round(numpy.mean(Type_list), 4)
				elif option == 'sum':
					temp = round(sum(Type_list), 4)
				if flag ==1 and date != yesterday:
					taglist.append({'object':Type, 'date':date})
					datalist.append({tagName:temp})
					taglist.append({'object':Type, 'date':yesterday})
					datalist.append({tagName:temp})
				else:
					taglist.append({'object':Type, 'date':date})
					datalist.append({tagName:temp})
		print '---------------here-----------------'
		self.mongo.setData(self.project, taglist, datalist)
	def setDataPeriod(self, value_dict, date, yesterday, tagName, option, flag):
		taglist = []
		datalist = []
		company_list = []
		for company in self.Structure_Dict :
			type_list = []
			for farmtype in self.Structure_Dict[company] :
				farm_list = []
				for farm in self.Structure_Dict[company][farmtype]:
					period_list = []
					for period in self.Structure_Dict[company][farmtype][farm]:
						line_list = []
						for line in self.Structure_Dict[company][farmtype][farm][period]:
							if tagName in value_dict and line in value_dict[tagName] and date in value_dict[tagName][line]:
								try:
									value = float(value_dict[tagName][line][date])
									line_list.append(value)
								except:
									line_list.append(0.0)
						if option == 'max':
							temp = round(max(line_list), 4)
						elif option == 'min':
							temp = round(min(line_list), 4)
						elif option == 'avg_rate':
							temp = round(numpy.mean(line_list) * 100, 4)
						elif option == 'avg':
							temp = round(numpy.mean(line_list), 4)
						elif option == 'sum':
							temp = round(sum(line_list), 4)
						period_list.append(temp)
						if flag ==1 and date != yesterday:
							taglist.append({'object':period, 'date':date})
							datalist.append({tagName:temp})
							taglist.append({'object':period, 'date':yesterday})
							datalist.append({tagName:temp})
						else:
							taglist.append({'object':period, 'date':date})
							datalist.append({tagName:temp})
					if option == 'max':
						temp = round(max(period_list), 4)
					elif option == 'min':
						temp = round(min(period_list), 4)
					elif option == 'avg_rate':
						temp = round(numpy.mean(period_list) * 100, 4)
					elif option == 'avg':
						temp = round(numpy.mean(period_list), 4)
					elif option == 'sum':
						temp = round(sum(period_list), 4)
					farm_list.append(temp)
					if flag ==1 and date != yesterday:
						taglist.append({'object':farm, 'date':date})
						datalist.append({tagName:temp})
						taglist.append({'object':farm, 'date':yesterday})
						datalist.append({tagName:temp})
					else:
						taglist.append({'object':farm, 'date':date})
						datalist.append({tagName:temp})
				if option == 'max':
					temp = round(max(farm_list), 4)
				elif option == 'min':
					temp = round(min(farm_list), 4)
				elif option == 'avg_rate':
					temp = round(numpy.mean(farm_list) * 100, 4)
				elif option == 'avg':
					temp = round(numpy.mean(farm_list), 4)
				elif option == 'sum':
					temp = round(sum(farm_list), 4)
				type_list.append(temp)
				if flag ==1 and date != yesterday:
					taglist.append({'object':farmtype, 'date':date})
					datalist.append({tagName:temp})
					taglist.append({'object':farmtype, 'date':yesterday})
					datalist.append({tagName:temp})
				else:
					taglist.append({'object':farmtype, 'date':date})
					datalist.append({tagName:temp})
			if option == 'max':
				temp = round(max(type_list), 4)
			elif option == 'min':
				temp = round(min(type_list), 4)
			elif option == 'avg_rate':
				temp = round(numpy.mean(type_list) * 100, 4)
			elif option == 'avg':
				temp = round(numpy.mean(type_list), 4)
			elif option == 'sum':
				temp = round(sum(type_list), 4)
			company_list.append(temp)
			if flag ==1 and date != yesterday:
				taglist.append({'object':company, 'date':date})
				datalist.append({tagName:temp})
				taglist.append({'object':company, 'date':yesterday})
				datalist.append({tagName:temp})
			else:
				taglist.append({'object':company, 'date':date})
				datalist.append({tagName:temp})
		if option == 'max':
			temp = round(max(company_list), 4)
		elif option == 'min':
			temp = round(min(company_list), 4)
		elif option == 'avg_rate':
			temp = round(numpy.mean(company_list) * 100, 4)
		elif option == 'avg':
			temp = round(numpy.mean(company_list), 4)
		elif option == 'sum':
			temp = round(sum(company_list), 4)
		if flag ==1 and date != yesterday:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
			taglist.append({'object':self.project, 'date':yesterday})
			datalist.append({tagName:temp})
		else:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
		self.mongo.setData(self.project, taglist, datalist)
	def setDataFarm(self, value_dict, date, yesterday, tagName, option, flag):
		taglist, datalist, company_list = [], [], []
		for company in self.Structure_Dict :
			for farmtype in self.Structure_Dict[company] :
				farm_list = []
				for farm in self.Structure_Dict[company][farmtype]:
					period_list = []
					for period in self.Structure_Dict[company][farmtype][farm]:
						if tagName in value_dict and period in value_dict[tagName] and date in value_dict[tagName][period]:
							try:
								value = float(value_dict[tagName][period][date])
								period_list.append(value)
							except:
								period_list.append(0.0)
					if option == 'max':
						temp = round(max(period_list), 4)
					elif option == 'min':
						temp = round(min(period_list), 4)
					elif option == 'avg_rate':
						temp = round(numpy.mean(period_list) * 100, 4)
					elif option == 'avg':
						temp = round(numpy.mean(period_list), 4)
					elif option == 'sum':
						temp = round(sum(period_list), 4)
					farm_list.append(temp)
					if flag ==1 and date != yesterday:
						taglist.append({'object':farm, 'date':date})
						datalist.append({tagName:temp})
						taglist.append({'object':farm, 'date':yesterday})
						datalist.append({tagName:temp})
					else:
						taglist.append({'object':farm, 'date':date})
						datalist.append({tagName:temp})
				if option == 'max':
					temp = round(max(farm_list), 4)
				elif option == 'min':
					temp = round(min(farm_list), 4)
				elif option == 'avg_rate':
					temp = round(numpy.mean(farm_list) * 100, 4)
				elif option == 'avg':
					temp = round(numpy.mean(farm_list), 4)
				elif option == 'sum':
					temp = round(sum(farm_list), 4)
				type_list.append(temp)
				if flag ==1 and date != yesterday:
					taglist.append({'object':farmtype, 'date':date})
					datalist.append({tagName:temp})
					taglist.append({'object':farmtype, 'date':yesterday})
					datalist.append({tagName:temp})
				else:
					taglist.append({'object':farmtype, 'date':date})
					datalist.append({tagName:temp})
			if option == 'max':
				temp = round(max(type_list), 4)
			elif option == 'min':
				temp = round(min(type_list), 4)
			elif option == 'avg_rate':
				temp = round(numpy.mean(type_list) * 100, 4)
			elif option == 'avg':
				temp = round(numpy.mean(type_list), 4)
			elif option == 'sum':
				temp = round(sum(type_list), 4)
			company_list.append(temp)
			if flag ==1 and date != yesterday:
				taglist.append({'object':company, 'date':date})
				datalist.append({tagName:temp})
				taglist.append({'object':company, 'date':yesterday})
				datalist.append({tagName:temp})
			else:
				taglist.append({'object':company, 'date':date})
				datalist.append({tagName:temp})
		if option == 'max':
			temp = round(max(company_list), 4)
		elif option == 'min':
			temp = round(min(company_list), 4)
		elif option == 'avg_rate':
			temp = round(numpy.mean(company_list) * 100, 4)
		elif option == 'avg':
			temp = round(numpy.mean(company_list), 4)
		elif option == 'sum':
			temp = round(sum(company_list), 4)
		if flag ==1 and date != yesterday:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
			taglist.append({'object':self.project, 'date':yesterday})
			datalist.append({tagName:temp})
		else:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
		self.mongo.setData(self.project, taglist, datalist)
	def setDataCompany(self, value_dict, date, yesterday, tagName, option, flag):
		print '-----------------setDataCompany-------------------'
		print value_dict
		taglist, datalist, company_list = [], [], []
		for company in self.Structure_Dict :
			for farmtype in self.Structure_Dict[company] :
				farm_list = []
				for farm in self.Structure_Dict[company][farmtype]:
					if tagName in value_dict and farm in value_dict[tagName] and date in value_dict[tagName][farm]:
						try:
							value = float(value_dict[tagName][farm][date])
							farm_list.append(value)
						except:
							farm_list.append(0.0)
				if option == 'max':
					temp = round(max(farm_list), 4)
				elif option == 'min':
					temp = round(min(farm_list), 4)
				elif option == 'avg_rate':
					temp = round(numpy.mean(farm_list) * 100, 4)
				elif option == 'avg':
					temp = round(numpy.mean(farm_list), 4)
				elif option == 'sum':
					temp = round(sum(farm_list), 4)
				type_list.append(temp)
				if flag ==1 and date != yesterday:
					taglist.append({'object':company, 'date':date})
					datalist.append({tagName:temp})
					taglist.append({'object':company, 'date':yesterday})
					datalist.append({tagName:temp})
				else:
					taglist.append({'object':company, 'date':date})
					datalist.append({tagName:temp})
			if option == 'max':
				temp = round(max(type_list), 4)
			elif option == 'min':
				temp = round(min(type_list), 4)
			elif option == 'avg_rate':
				temp = round(numpy.mean(type_list) * 100, 4)
			elif option == 'avg':
				temp = round(numpy.mean(type_list), 4)
			elif option == 'sum':
				temp = round(sum(type_list), 4)
			company_list.append(temp)
			if flag ==1 and date != yesterday:
				taglist.append({'object':farmtype, 'date':date})
				datalist.append({tagName:temp})
				taglist.append({'object':farmtype, 'date':yesterday})
				datalist.append({tagName:temp})
			else:
				taglist.append({'object':farmtype, 'date':date})
				datalist.append({tagName:temp})
		if option == 'max':
			temp = round(max(company_list), 4)
		elif option == 'min':
			temp = round(min(company_list), 4)
		elif option == 'avg_rate':
			temp = round(numpy.mean(company_list) * 100, 4)
		elif option == 'avg':
			temp = round(numpy.mean(company_list), 4)
		elif option == 'sum':
			temp = round(sum(company_list), 4)
		if flag ==1 and date != yesterday:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
			taglist.append({'object':self.project, 'date':yesterday})
			datalist.append({tagName:temp})
		else:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
		self.mongo.setData(self.project, taglist, datalist)
	def setDataProject(self, value_dict, date, yesterday, tagName, option, flag):
		taglist, datalist, company_list = [], [], []
		for company in self.Structure_Dict :
			if tagName in value_dict and company in value_dict[tagName] and date in value_dict[tagName][company]:
				try:
					value = float(value_dict[tagName][company][date])
					company_list.append(value)
				except:
					company_list.append(0.0)
		if option == 'max':
			temp = round(max(company_list), 4)
		elif option == 'min':
			temp = round(min(company_list), 4)
		elif option == 'avg_rate':
			temp = round(numpy.mean(company_list) * 100, 4)
		elif option == 'avg':
			temp = round(numpy.mean(company_list), 4)
		elif option == 'sum':
			temp = round(sum(company_list), 4)
		if flag ==1 and date != yesterday:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
			taglist.append({'object':self.project, 'date':yesterday})
			datalist.append({tagName:temp})
		else:
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tagName:temp})
		self.mongo.setData(self.project, taglist, datalist)
	def judgeTagOption(self, tagName):
		option = ''
		if tagName in self.mongo.getStatisticsMap_float('max_float'):
			option = 'max'
		elif tagName in self.mongo.getStatisticsMap_float('min_float'):
			option = 'min'
		elif tagName in self.mongo.getStatisticsMap_float('avg_float'):
			option = 'avg'
		elif tagName in self.mongo.getStatisticsMap_float('avg_rate'):
			option = 'avg_rate'
		elif tagName in self.mongo.getStatisticsMap_float('sum_float'):
			option = 'sum'
		return option
	def judgeKeyLevel(self, key, date, yesterday, tagName, option, flag):
		#取key所在级别及以上的keylist，并记录key所在的级别
		#setData()是取keylist的tag的值，设置key上级的tag值
		#返回removekeylist，是因为修改tag相关指标的值只涉及到key和key的上级，和key同级的无关
		keyList, removekeylist = [], []
		if len(key.split(':')) == 2:
			if key in self.CompanyKeyList:
				keyList.append(self.project)
				keyList.extend(self.CompanyKeyList)
				removekeylist = self.CompanyKeyList
				value_dict = self.mongo.getStatisticDict(self.project, [date], keylist, [tagName])
				self.setDataProject(value_dict, date, yesterday, tagName, option, flag)
				
			if key in self.PV_FarmKeyList + self.WT_FarmKeyList:
				keyList.append(self.project)
				keyList.extend(self.CompanyKeyList)
				keyList.extend(self.PV_FarmKeyList + self.WT_FarmKeyList)
				keyList.extend(self.PVKeyList + self.WTKeyList)
				removekeylist = self.PV_FarmKeyList + self.WT_FarmKeyList
				value_dict = self.mongo.getStatisticDict(self.project, [date], keylist, [tagName])
				self.setDataCompany(value_dict, date, yesterday, tagName, option, flag)
		if len(key.split(':')) == 3:
			if key in self.PV_PeriodKeyList + self.WT_PeriodKeyList:
				keyList.append(self.project)
				keyList.extend(self.CompanyKeyList)
				keyList.extend(self.PV_FarmKeyList + self.WT_FarmKeyList)
				keyList.extend(self.PVKeyList + self.WTKeyList)
				keyList.extend(self.PV_PeriodKeyList + self.WT_PeriodKeyList)
				removekeylist = self.PV_PeriodKeyList + self.WT_PeriodKeyList
				value_dict = self.mongo.getStatisticDict(self.project, [date], keylist, [tagName])
				self.setDataFarm(value_dict, date, yesterday, tagName, option, flag)
			if key in self.PV_LineKeyList + self.WT_LineKeyList:
				keyList.append(self.project)
				keyList.extend(self.CompanyKeyList)
				keyList.extend(self.PV_FarmKeyList + self.WT_FarmKeyList)
				keyList.extend(self.PVKeyList + self.WTKeyList)
				keyList.extend(self.PV_PeriodKeyList + self.WT_PeriodKeyList)
				keyList.extend(self.PV_LineKeyList + self.WT_LineKeyList)
				removekeylist = self.PV_LineKeyList + self.WT_LineKeyList
				value_dict = self.mongo.getStatisticDict(self.project, [date], keylist, [tagName])
				self.setDataPeriod(value_dict, date, yesterday, tagName, option, flag)
			if key in self.PV_DevKeyList + self.WT_DevKeyList:
				keyList.append(self.project)
				keyList.extend(self.CompanyKeyList)
				keyList.extend(self.PV_FarmKeyList + self.WT_FarmKeyList)
				keyList.extend(self.PVKeyList + self.WTKeyList)
				keyList.extend(self.PV_PeriodKeyList + self.WT_PeriodKeyList)
				keyList.extend(self.PV_LineKeyList + self.WT_LineKeyList)
				keyList.extend(self.PV_DevKeyList + self.WT_DevKeyList)
				removekeylist = self.PV_DevKeyList + self.WT_DevKeyList
				value_dict = self.mongo.getStatisticDict(self.project, [date], keyList, [tagName])
				self.setDataLine(value_dict, date, yesterday, tagName, option, key, flag)
				print '------------judgeKeyLevel-------------'
		return keyList, removekeylist
	def setDataByTagName(self, key, date, tagName, value, flag):
		try:
			#flag = 0 美化数值(在day外侧修改tagvalue值)， flag = 1 修正数值(更新到day下)
			if len(date.split('/')) == 3:
				return self.setDayDataByTagName(key, date, tagName, value)
			elif len(date.split('/')) == 2:
				return self.setMonthDataByTagName(key, date, tagName, value, flag)
			elif len(date.split('/')) == 1:
				return self.setYearDataByTagName(key, date, tagName, value, flag)
			else:
				return False
		except:
			return False
	def setDayDataByTagName(self, key, date, tagName, value):
		try:
			#修改日的tagvalue，直接修改day下，所以flag默认是1
			flag = 1
			special = 'Day'
			year,month, day = date.split('/')
			onlyTagName = tagName.replace('_Day','')
			#修改key的tag值
			try:
				self.mongo.update(key, date, {tagName:float(value)})
			except:
				self.mongo.update(key, date, {tagName:value})
			#判断tagName涉及计算的属性
			option = self.judgeTagOption(onlyTagName)
			#修改key以上级别tagName的值，返回含key所在级别及以上的keyList，和key所在级别的removekeylist
			#keyList用来取出关联tag计算的数据源，而关联tag计算和key所在级别的其他key无关
			keyList, removekeylist = self.judgeKeyLevel(key, date, date, tagName, option, flag)
			
			#找tagName直接影响的tag
			relTag_list = self.mongo.getStatisticsMap_New(onlyTagName)
			if relTag_list <> []:
				relTag_tagList = self.FindAllTagList(relTag_list, [])
				#将tagName和tagName直接影响的tag加入relTag_tagList
				relTag_tagList.append(onlyTagName)
				relTag_tagList.extend(relTag_list)
				#tag去重
				relTag_tagList = list(set(relTag_tagList))
				print '--------now------'
				#修改所有tag计算所需要取出的tag
				own_reltagList = self.mongo.getTagSource(relTag_tagList)
				own_reltagList.extend(relTag_tagList)
				own_reltagList = list(set(own_reltagList))
				#加day下标，取值用
				taglist_hasindex = []
				for tag in own_reltagList :
					taglist_hasindex.append(tag + '_Day')
				#取出key级及以上所有tag的值
				data = self.mongo.getStatisticDict(self.project, [date], keyList, taglist_hasindex)
				#对key及key级以上关联tag值的修改，key同级的其他key不需要计算
				for Key in removekeylist:
					keyList.remove(Key)
				keyList.append(key)
				if data <> {}:
					#关联tag的计算，tag的已经修改，不需要关联计算
					relTag_tagList.remove(onlyTagName)
					for tag in relTag_tagList:
						print '---------------------------------'
						Tag = tag + '_Day'
						for key in keyList:
							value = self.getFormulaByTagName(key, Tag, data, date, date, special, flag)
							self.mongo.setData(self.project, [{'object':key, 'date':date}], [{Tag:value}])
			return True
		except:
			return False
	
	def FindAllTagList(self, fatherlist, resultlist):
		#取fatherlist内tag会影响的所有tag放入resultlist返回
		templist = []
		for tag in fatherlist:
			sonlist = self.mongo.getStatisticsMap_New(tag)
			if sonlist <> []:
				templist.extend(sonlist)
		if templist <> []:
			resultlist.extend(templist)
			resultlist = list(set(resultlist))
			return self.FindAllTagList(templist, resultlist)
		else :
			return resultlist
	def setMonthDataByTagName(self, key, date, tagName, value, flag):
		try:
			special = 'Month'
			#修改的日期
			year,month = date.split('/')
			onlyTagName = tagName.replace('_Month','')
			#修改日日期
			thatDate = (datetime.datetime.now()).strftime('%Y/%m/%d')
			thatYear,thatMonth,thatDay = thatDate.split('/')
			if year < thatYear or month < thatMonth :#修改之前某个月
				firstday,lastday = calendar.monthrange(int(year),int(month))
				yesterday = year + '/' + month + '/' + str(lastday)
			else :
				thatday = datetime.datetime.now()
				yesterday = (thatday-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
			if flag == 1 :
				try:
					self.mongo.update(key, date, {tagName:float(value)})
					self.mongo.update(key, yesterday, {tagName:float(value)})
				except:
					self.mongo.update(key, date, {tagName:value})
					self.mongo.update(key, yesterday, {tagName:value})
			elif flag == 0 :
				try:
					self.mongo.update(key, date, {tagName:float(value)})
				except:
					self.mongo.update(key, date, {tagName:value})
			option = self.judgeTagOption(onlyTagName)
			keyList, removekeylist = self.judgeKeyLevel(key, yesterday, tagName, option)
			relTag_list = self.mongo.getStatisticsMap_New(onlyTagName)
			if relTag_list <> []:
				relTag_tagList = self.FindAllTagList(relTag_list, [])
				relTag_tagList.append(onlyTagName)
				relTag_tagList.extend(relTag_list)
				relTag_tagList = list(set(relTag_tagList))
				
				own_reltagList = self.mongo.getTagSource(relTag_tagList)
				own_reltagList.extend(relTag_tagList)
				own_reltagList = list(set(own_reltagList))
				
				taglist_hasindex = []
				for tag in own_reltagList :
					taglist_hasindex.append(tag +'_Month')
				data = self.mongo.getStatisticDict(self.project, [yesterday,date], keyList, taglist_hasindex)
				#print data['CMPT_Production_Month']['JXDTJK:JS']['2017/10']
				for Key in removekeylist:
					keyList.remove(Key)
				keyList.append(key)
				if data <> {}:
					relTag_tagList.remove(onlyTagName)
					for tag in relTag_tagList:
						print '---------------------------------'
						Tag = tag + '_Month'
						for key in keyList:
							if flag == 0:
								value = self.getFormulaByTagName(key, Tag, data, date, yesterday, special, flag)
								self.mongo.setData(self.project, [{'object':key, 'date':date}], [{Tag:value}])
							elif flag == 1:
								value = self.getFormulaByTagName(key, Tag, data, date, yesterday, special, flag)
								self.mongo.setData(self.project, [{'object':key, 'date':date}], [{Tag:value}])
								self.mongo.setData(self.project, [{'object':key, 'date':yesterday}], [{Tag:value}])
			return True
		except:
			return False
			
	def setYearDataByTagName(self, key, date, tagName, value, flag):
		try:
			special = 'Year'
			year = date
			onlyTagName = tagName.replace('_Year','')
			thatDate = (datetime.datetime.now()).strftime('%Y/%m/%d')
			thatYear,thatMonth,thatDay = thatDate.split('/')
			if year < thatYear :#修改去年
				yesterday = year + '/' + str(12) + '/' + str(31)
			else :
				thatday = datetime.datetime.now()
				yesterday = (thatday-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
			if flag == 1 :
				try:
					self.mongo.update(key, date, {tagName:float(value)})
					self.mongo.update(key, yesterday, {tagName:float(value)})
				except:
					self.mongo.update(key, date, {tagName:value})
					self.mongo.update(key, yesterday, {tagName:value})
			elif flag == 0 :
				try:
					self.mongo.update(key, date, {tagName:float(value)})
				except:
					self.mongo.update(key, date, {tagName:value})
			option = self.judgeTagOption(onlyTagName)
			#print 'option:',option,'------'
			keyList, removekeylist = self.judgeKeyLevel(key, yesterday, tagName, option)
			#print 'keyList',keyList,'------'
			relTag_list = self.mongo.getStatisticsMap_New(onlyTagName)
			if relTag_list <> []:
				relTag_tagList = self.FindAllTagList(relTag_list, [])
				relTag_tagList.append(onlyTagName)
				relTag_tagList.extend(relTag_list)
				relTag_tagList = list(set(relTag_tagList))
				
				own_reltagList = self.mongo.getTagSource(relTag_tagList)
				own_reltagList.extend(relTag_tagList)
				own_reltagList = list(set(own_reltagList))
				#print own_reltagList,'------'
				taglist_hasindex = []
				for tag in own_reltagList :
					taglist_hasindex.append(tag + '_Year')
				#print '---tag data need to be bring out:',taglist_hasindex
				data = self.mongo.getStatisticDict(self.project, [yesterday,date], keyList, taglist_hasindex)
				#print data['CMPT_Production_Month']['JXDTJK:JS']['2017/10']
				for Key in removekeylist:
					keyList.remove(Key)
				keyList.append(key)
				if data <> {}:
					relTag_tagList.remove(onlyTagName)
					for tag in relTag_tagList:
						print '---------------------------------'
						Tag = tag + '_Year'
						for key in keyList:
							if flag == 0:
								value = self.getFormulaByTagName(key, Tag, data, date, yesterday, special, flag)
								self.mongo.setData(self.project, [{'object':key, 'date':date}], [{Tag:value}])
							elif flag == 1:
								value = self.getFormulaByTagName(key, Tag, data, date, yesterday, special, flag)
								self.mongo.setData(self.project, [{'object':key, 'date':date}], [{Tag:value}])
								self.mongo.setData(self.project, [{'object':key, 'date':yesterday}], [{Tag:value}])
			return True
		except:
			return False
	def getFormulaByTagName(self, key, input_tagName, data, date, yesterday, special, flag):
		if special == 'Month' :
			tagName = input_tagName.replace('_Month', '')
		elif special == 'Year' :
			tagName = input_tagName.replace('_Year', '')
		else:
			tagName = input_tagName.replace('_Day', '')
		#print tagName, '---------getFormulaByTagName------------'
		if tagName == 'CMPT_UseHours':
			return self.getUsehours(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_CAH': 
			return self.getCAH(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_MTBF': 
			return self.getMTBF(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_ExposeRatio': 
			return self.getExposeRatio(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_CompreHouseProduction':
			return self.getCompreHouseProduction(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_RateOfHousePower':
			return self.getHousePowerRate(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_HouseLost':
			return self.getHouseLost(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_HouseLostRate':
			return self.getHouseLostRate(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_UseRatio':
			return self.getUseRatio(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_OutPowerRatio': 
			return self.getOutPowerRatio(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_GenerateRate':
			return self.getGenerateRate(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_PowerLoadRate': 
			return self.getPowerLoadRate(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_Availability': 
			return self.getAvailability(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_CompleteOfPlan':
			return self.getCompleteOfPlan(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_GeneratingEfficiency':
			return self.getGeneratingEfficiency(key, data, date, yesterday, special, flag)
		elif tagName == 'CMPT_HouseRate':
			return self.getHouseRate(key, data, date, yesterday, special, flag)
		else :
			return self.getSingleTagValue(tagName, key, data, date, yesterday, special, flag)
	def getSingleTagValue(self, tagName, key, data, date, yesterday, special, flag):
		#只需取相关tag的值，不涉及与其他tag的关联计算
		#为未完成的get函数存在，要算关联tag则只取该tag值
		print '----------------------------getSingleTagValue----------------------'
		value = 0.0
		tag = tagName + '_' + special
		if tag in data and key in data[tag] :
			if flag == 0 and date in data[tag][key]:
				try:
					value = float(data[tag][key][date])
				except:
					pass
			elif flag == 1 :
				if yesterday in data[tag][key] :
					try:
						value = float(data[tag][key][yesterday])
						print value
					except:
						pass
				elif date != yesterday and date in data[tag][key] :
					try:
						value = float(data[tag][key][date])
					except:
						pass
		return value
	def getCompleteOfPlan(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_pro = 'CMPT_Production_' + special
		tag_pla = 'CMPT_ProductionPlan_' + special
		if tag_pro in data and key in data[tag_pro] and tag_pla in data and key in data[tag_pla] :
			if flag == 0 and date in data[tag_pro][key] and date in data[tag_pla][key]:
				plan = float(data[tag_pla][key][date]) if data[tag_pla][key][date] <> '' else 0.0
				production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
				if plan <> 0.0:
					vlaue = production/plan
					return round(vlaue, 4)
			elif flag == 1 :
				if yesterday in data[tag_pro][key] and yesterday in data[tag_pla][key]:
					plan = float(data[tag_pla][key][yesterday]) if data[tag_pla][key][yesterday] <> '' else 0.0
					production = float(data[tag_pro][key][yesterday]) if data[tag_pro][key][yesterday] <> '' else 0.0
					if plan <> 0.0:
						vlaue = production/plan
						return round(vlaue, 4)
				elif date != yesterday and date in data[tag_pro][key] and date in data[tag_pla][key]:
					plan = float(data[tag_pla][key][date]) if data[tag_pla][key][date] <> '' else 0.0
					production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
					if plan <> 0.0:
						vlaue = production/plan
						return round(vlaue, 4)
		return value
	def getGeneratingEfficiency(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_hou = 'CMPT_HouseRate_' + special
		tag_com = 'CMPT_CompleteOfPlan_' + special
		if tag_hou in data and key in data[tag_hou] and tag_com in data and key in data[tag_com] :
			if flag == 0 and date in data[tag_hou][key] and date in data[tag_com][key]:
				CompleteOfPlan = float(data[tag_com][key][date]) if data[tag_com][key][date] <> '' else 0.0
				HouseRate = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
				if CompleteOfPlan <> 0.0:
					vlaue = HouseRate/CompleteOfPlan
					return round(vlaue, 4)
			elif flag == 1 :
				if yesterday in data[tag_hou][key] and yesterday in data[tag_com][key]:
					CompleteOfPlan = float(data[tag_com][key][yesterday]) if data[tag_com][key][yesterday] <> '' else 0.0
					HouseRate = float(data[tag_hou][key][yesterday]) if data[tag_hou][key][yesterday] <> '' else 0.0
					if CompleteOfPlan <> 0.0:
						vlaue = HouseRate/CompleteOfPlan
						return round(vlaue, 4)
				elif date != yesterday and date in data[tag_hou][key] and date in data[tag_com][key]:
					CompleteOfPlan = float(data[tag_com][key][date]) if data[tag_com][key][date] <> '' else 0.0
					HouseRate = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
					if CompleteOfPlan <> 0.0:
						vlaue = HouseRate/CompleteOfPlan
						return round(vlaue, 4)
		return value
	def getExposeRatio(key, data, date, yesterday, special, flag):
		value = 0.0
		tag_run = 'CMPT_RunHours_' + special
		tag_use = 'CMPT_UserForGenerationHours_' + special
		if tag_run in data and key in data[tag_run] and tag_use in data and key in data[tag_use] :
			if flag == 0 and date in data[tag_run][key] and date in data[tag_use][key]:
				canusehour = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
				runhour = float(data[tag_run][key][date]) if data[tag_run][key][date] <> '' else 0.0
				if canusehour <> 0.0:
					vlaue = runhour/canusehour
					return round(vlaue, 4)
			elif flag == 1 :
				if yesterday in data[tag_run][key] and yesterday in data[tag_use][key]:
					canusehour = float(data[tag_use][key][yesterday]) if data[tag_use][key][yesterday] <> '' else 0.0
					runhour = float(data[tag_run][key][yesterday]) if data[tag_run][key][yesterday] <> '' else 0.0
					if canusehour <> 0.0:
						vlaue = runhour/canusehour
						return round(vlaue, 4)
				elif date != yesterday and date in data[tag_run][key] and date in data[tag_use][key]:
					canusehour = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
					runhour = float(data[tag_run][key][date]) if data[tag_run][key][date] <> '' else 0.0
					if canusehour <> 0.0:
						vlaue = runhour/canusehour
						return round(vlaue, 4)
		return value
	#--------------------------------------------------------------
	def getUnFaultHours(key, data, date, yesterday, special, flag):
		#总时间-小时数
		pass
	def getUserForGenerationHours(key, data, date, yesterday, special, flag):
		#统计期间-故障-维修-检修
		pass
	def getUserForGenerRatio(key, data, date, yesterday, special, flag):
		#可用小时/统计期间小时*100%
		pass
	def getRunRatio(key, data, date, yesterday, special, flag):
		#运行小时/统计期间小时*100%
		pass
	def getUnConnectRatio(key, data, date, yesterday, special, flag):
		#通讯中断时间/统计周期*100%
		pass
	def getLimPwrRate(key, data, date, yesterday, special, flag):
		#限电量/理论电量
		pass
	def getFaultCnt_Avg(key, data, date, yesterday, special, flag):
		#故障停机次数/台数
		pass
	def getFaultHours_Avg(key, data, date, yesterday, special, flag):
		#故障停机时间/台数
		pass
	def getFaultStopLost_Avg(key, data, date, yesterday, special, flag):
		#故障停机损失电量/台数
		pass
	def getGenrationHours(key, data, date, yesterday, special, flag):
		#功率>0 且<=额定1.2 CMPT_ActPower
		pass
	def getFullHours(key, data, date, yesterday, special, flag):
		#统计周期内功率达到额定功率的累计时间，小于等于1.2倍额定功率
		pass
	def getFaultStopLost(key, data, date, yesterday, special, flag):
		#计算方式有两种：1、停机期间样机的发电量 2、停机风机的理论功率在时间上积分；
		pass
	def getOnGridHours(key, data, date, yesterday, special, flag):
		#统计周期内有效功率值的累计小时数,报表统计用
		pass
	def getRepairHours(key, data, date, yesterday, special, flag):
		#CMPT_ServiceHours
		pass
	#--------------------------------------------------------------
	def getUsehours(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag = 'CMPT_Production_' + special
		if tag in data and key in data[tag] :
			if flag == 0 and date in data[tag][key]:
				try :
					value = round(float(data[tag][key][date]) / float(self.Cap_Dict[key]), 4) 
				except:
					pass
			elif flag == 1 :
				if yesterday in data[tag][key] :
					try :
						value = round(float(data[tag][key][yesterday]) / float(self.Cap_Dict[key]), 4) 
					except:
						pass
				elif date != yesterday and date in data[tag][key] :
					try :
						value = round(float(data[tag][key][date]) / float(self.Cap_Dict[key]), 4) 
					except:
						pass
		return value
	def getGenerateRate(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_pro = 'CMPT_Production_' + special
		tag_wind = 'CMPT_WindEnerge_' + special
		tag_tot = 'CMPT_TotRadiation_' + special
		WTkeys = self.WT_FarmKeyList + self.WT_PeriodKeyList + self.WT_LineKeyList + self.WT_DevKeyList
		WTkeys.append(self.project)
		PVkeys = self.PV_FarmKeyList + self.PV_PeriodKeyList + self.PV_LineKeyList + self.PV_DevKeyList
		PVkeys.append(self.project)
		if tag_pro in data and key in data[tag_pro] :
			if flag == 0 and date in data[tag_pro][key] :
				production = float(data[tag_pro][key][date]) if data[tag_pro][key][yesterday] <> '' else 0.0
				if key in WTkeys:
					windEnerge = float(data[tag_wind][key][date]) if tag_wind in data and key in data[tag_wind] and date in data[tag_wind][key] else 0.0
					if windEnerge <> 0.0:
						return round(production / windEnerge * 100 , 4) if ( production / windEnerge >= 0 ) and ( production / windEnerge <= 1 ) else 0.0
				if key in PVkeys:
					totRadiation = float(data[tag_tot][key][date]) if tag_tot in data and key in data[tag_tot] and date in data[tag_tot][key] else 0.0
					if totRadiation <> 0.0:
						return round(production / totRadiation * 100, 4) if ( production / totRadiation >= 0 ) and ( production / totRadiation <= 1 ) else 0.0
			elif flag == 1 :
				if yesterday in data[tag_pro][key] :
					production = float(data[tag_pro][key][yesterday]) if data[tag_pro][key][yesterday] <> '' else 0.0
					if key in WTkeys:
						windEnerge = float(data[tag_wind][key][yesterday]) if tag_wind in data and key in data[tag_wind] and yesterday in data[tag_wind][key] else 0.0
						if windEnerge <> 0.0:
							return round(production / windEnerge * 100 , 4) if ( production / windEnerge >= 0 ) and ( production / windEnerge <= 1 ) else 0.0
					if key in PVkeys:
						totRadiation = float(data[tag_tot][key][yesterday]) if tag_tot in data and key in data[tag_tot] and yesterday in data[tag_tot][key] else 0.0
						if totRadiation <> 0.0:
							return round(production / totRadiation * 100, 4) if ( production / totRadiation >= 0 ) and ( production / totRadiation <= 1 ) else 0.0
				elif date != yesterday and date in data[tag_pro][key] :
					production = float(data[tag_pro][key][date]) if data[tag_pro][key][yesterday] <> '' else 0.0
					if key in WTkeys:
						windEnerge = float(data[tag_wind][key][date]) if tag_wind in data and key in data[tag_wind] and date in data[tag_wind][key] else 0.0
						if windEnerge <> 0.0:
							return round(production / windEnerge * 100 , 4) if ( production / windEnerge >= 0 ) and ( production / windEnerge <= 1 ) else 0.0
					if key in PVkeys:
						totRadiation = float(data[tag_tot][key][date]) if tag_tot in data and key in data[tag_tot] and date in data[tag_tot][key] else 0.0
						if totRadiation <> 0.0:
							return round(production / totRadiation * 100, 4) if ( production / totRadiation >= 0 ) and ( production / totRadiation <= 1 ) else 0.0
		return value
	def getHouseRate(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_hou = 'CMPT_HouseProduction_' + special
		tag_pro = 'CMPT_Production_' + special
		#Allkey = self.PV_FarmKeyList + self.WT_FarmKeyList + self.CompanyKeyList 
		#Allkey.append(self.project)
		#if key in Allkey:
		if tag_hou in data and key in data[tag_hou] and tag_pro in data and key in data[tag_pro] :
			if flag == 0 and date in data[tag_hou][key] and date in data[tag_pro][key] :
				production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
				houseRate = float(data[tag_hou][key][date]) if data[tag_hou][key][date] else 0.0
				if production <> 0.0:
					return round(houseRate * 100 / production, 4)  if ( houseRate / production >= 0 ) and ( houseRate / production <= 1 ) else 0.0
			elif flag == 1 :
				if tag_pro in data and key in data[tag_pro] :
					if yesterday in data[tag_pro][key]:
						production = float(data[tag_pro][key][yesterday]) if data[tag_pro][key][yesterday] <> '' else 0.0
						houseRate = float(data[tag_hou][key][yesterday]) if tag_hou in data and key in data[tag_hou] and yesterday in data[tag_hou][key] else 0.0
						if production <> 0.0:
							return round(houseRate * 100 / production, 4)  if ( houseRate / production >= 0 ) and ( houseRate / production <= 1 ) else 0.0
					elif date != yesterday and date in data[tag_pro][key]:
						production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
						houseRate = float(data[tag_hou][key][date]) if tag_hou in data and key in data[tag_hou] and date in data[tag_hou][key] else 0.0
						if production <> 0.0:
							return round(houseRate * 100 / production, 4)  if ( houseRate / production >= 0 ) and ( houseRate / production <= 1 ) else 0.0
		return value
	def getHousePowerRate(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_on = 'CMPT_OnGridProduction_' + special
		tag_pro = 'CMPT_Production_' + special
		tag_pur = 'CMPT_PurchaseProduction_' + special
		if tag_pro in data and key in data[tag_pro] and tag_on in data and key in data[tag_on] and tag_pur in data and key in data[tag_pur] :
			if flag == 0 and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
				onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
				purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
				production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
				if production <> 0.0:
					rate = (production - onGrid + purProduction) * 100 / production if ((production - onGrid + purProduction) / production >= 0.0 ) and ((production - onGrid + purProduction) / production <= 1.0) else 0.0
					return round(rate, 4)
			elif flag == 1 :
				if yesterday in data[tag_pro][key] and yesterday in data[tag_on][key] and yesterday in data[tag_pur][key]:
					onGrid = float(data[tag_on][key][yesterday]) if data[tag_on][key][yesterday] <> '' else 0.0
					purProduction = float(data[tag_pur][key][yesterday]) if data[tag_pur][key][yesterday] <> '' else 0.0
					production = float(data[tag_pro][key][yesterday]) if data[tag_pro][key][yesterday] <> '' else 0.0
					if production <> 0.0:
						rate = (production - onGrid + purProduction) * 100 / production if ((production - onGrid + purProduction) / production >= 0.0 ) and ((production - onGrid + purProduction) / production <= 1.0) else 0.0
						return round(rate, 4)
				elif date != yesterday and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
					onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
					purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
					production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
					if production <> 0.0:
						rate = (production - onGrid + purProduction) * 100 / production if ((production - onGrid + purProduction) / production >= 0.0 ) and ((production - onGrid + purProduction) / production <= 1.0) else 0.0
						return round(rate, 4)
		return value
	def getCompreHouseProduction(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_on = 'CMPT_OnGridProduction_' + special
		tag_pro = 'CMPT_Production_' + special
		tag_pur = 'CMPT_PurchaseProduction_' + special
		if tag_pro in data and key in data[tag_pro] and tag_on in data and key in data[tag_on] and tag_pur in data and key in data[tag_pur] :
			if flag == 0 and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
				onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
				purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
				production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
				return round(production - onGrid + purProduction, 4) if (production - onGrid + purProduction) >= 0.0 else 0.0
			elif flag == 1 :
				if yesterday in data[tag_pro][key] and yesterday in data[tag_on][key] and yesterday in data[tag_pur][key]:
					onGrid = float(data[tag_on][key][yesterday]) if data[tag_on][key][yesterday] <> '' else 0.0
					purProduction = float(data[tag_pur][key][yesterday]) if data[tag_pur][key][yesterday] <> '' else 0.0
					production = float(data[tag_pro][key][yesterday]) if data[tag_pro][key][yesterday] <> '' else 0.0
					return round(production - onGrid + purProduction, 4) if (production - onGrid + purProduction) >= 0.0 else 0.0
				elif date != yesterday and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
					onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
					purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
					production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
					return round(production - onGrid + purProduction, 4) if (production - onGrid + purProduction) >= 0.0 else 0.0
		return value
	def getHouseLost(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_on = 'CMPT_OnGridProduction_' + special
		tag_pro = 'CMPT_Production_' + special
		tag_pur = 'CMPT_PurchaseProduction_' + special
		tag_hou = 'CMPT_HouseProduction_' + special
		if tag_pro in data and key in data[tag_pro] and tag_on in data and key in data[tag_on] and tag_pur in data and key in data[tag_pur] and tag_hou in data and key in data[tag_hou]:
			if flag == 0 and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
				onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
				purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
				production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
				houseProduction = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
				return round(production - onGrid + purProduction - houseProduction, 4) if (production - onGrid + purProduction - houseProduction) >= 0.0 else 0.0
			elif flag == 1 :
				if yesterday in data[tag_pro][key] and yesterday in data[tag_on][key] and yesterday in data[tag_pur][key] and yesterday in data[tag_hou][key]:
					onGrid = float(data[tag_on][key][yesterday]) if data[tag_on][key][yesterday] <> '' else 0.0
					purProduction = float(data[tag_pur][key][yesterday]) if data[tag_pur][key][yesterday] <> '' else 0.0
					production = float(data[tag_pro][key][yesterday]) if data[tag_pro][key][yesterday] <> '' else 0.0
					houseProduction = float(data[tag_hou][key][yesterday]) if data[tag_hou][key][yesterday] <> '' else 0.0
					return round(production - onGrid + purProduction - houseProduction, 4) if (production - onGrid + purProduction - houseProduction) >= 0.0 else 0.0
				elif date != yesterday and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
					onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
					purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
					production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
					houseProduction = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
					return round(production - onGrid + purProduction - houseProduction, 4) if (production - onGrid + purProduction - houseProduction) >= 0.0 else 0.0
		return value
	def getHouseLostRate(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_on = 'CMPT_OnGridProduction_' + special
		tag_pro = 'CMPT_Production_' + special
		tag_pur = 'CMPT_PurchaseProduction_' + special
		tag_hou = 'CMPT_HouseProduction_' + special
		if tag_pro in data and key in data[tag_pro] and tag_on in data and key in data[tag_on] and tag_pur in data and key in data[tag_pur] and tag_hou in data and key in data[tag_hou]:
			if flag == 0 and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
				onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
				purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
				production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
				houseProduction = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
				if production <> 0.0:
					return round((production - onGrid + purProduction - houseProduction) * 100 / production, 4) if ((production - onGrid + purProduction - houseProduction)/production >= 0.0)  and ((production - onGrid + purProduction - houseProduction)/production <= 1.0) else 0.0
			elif flag == 1 :
				if yesterday in data[tag_pro][key] and yesterday in data[tag_on][key] and yesterday in data[tag_pur][key] and yesterday in data[tag_hou][key]:
					onGrid = float(data[tag_on][key][yesterday]) if data[tag_on][key][yesterday] <> '' else 0.0
					purProduction = float(data[tag_pur][key][yesterday]) if data[tag_pur][key][yesterday] <> '' else 0.0
					production = float(data[tag_pro][key][yesterday]) if data[tag_pro][key][yesterday] <> '' else 0.0
					houseProduction = float(data[tag_hou][key][yesterday]) if data[tag_hou][key][yesterday] <> '' else 0.0
					if production <> 0.0:
						return round((production - onGrid + purProduction - houseProduction) * 100 / production, 4) if ((production - onGrid + purProduction - houseProduction)/production >= 0.0)  and ((production - onGrid + purProduction - houseProduction)/production <= 1.0) else 0.0
				elif date != yesterday and date in data[tag_pro][key] and date in data[tag_on][key] and date in data[tag_pur][key]:
					onGrid = float(data[tag_on][key][date]) if data[tag_on][key][date] <> '' else 0.0
					purProduction = float(data[tag_pur][key][date]) if data[tag_pur][key][date] <> '' else 0.0
					production = float(data[tag_pro][key][date]) if data[tag_pro][key][date] <> '' else 0.0
					houseProduction = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
					if production <> 0.0:
						return round((production - onGrid + purProduction - houseProduction) * 100 / production, 4) if ((production - onGrid + purProduction - houseProduction)/production >= 0.0)  and ((production - onGrid + purProduction - houseProduction)/production <= 1.0) else 0.0
		return value
	def getUseRatio(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_use = 'CMPT_UseHours_' + special
		if tag_use in data and key in data[tag_use] :
			if flag == 0 and date in data[tag_use][key] :
				useHours = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
				if key in self.WT_DevKeyList:
					return round(useHours *100/ 24.0 , 4)
				elif key in self.PV_DevKeyList:
					return round(useHours *100/ 9.0 , 4)
				else:
					return round(useHours *100/ (24.0 * self.Count_Dict[key]), 4)
			elif flag == 1 :
				if yesterday in data[tag_use][key]:
					useHours = float(data[tag_use][key][yesterday]) if data[tag_use][key][yesterday] <> '' else 0.0
					if key in self.WT_DevKeyList:
						return round(useHours *100/ 24.0 , 4)
					elif key in self.PV_DevKeyList:
						return round(useHours *100/ 9.0 , 4)
					else:
						return round(useHours *100/ (24.0 * self.Count_Dict[key]), 4)
				elif date != yesterday and date in data[tag_use][key] :
					useHours = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
					if key in self.WT_DevKeyList:
						return round(useHours *100/ 24.0 , 4)
					elif key in self.PV_DevKeyList:
						return round(useHours *100/ 9.0 , 4)
					else:
						return round(useHours *100/ (24.0 * self.Count_Dict[key]), 4)
		return value
	def getOutPowerRatio(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_run = 'CMPT_RunHours_' + special
		tag_use = 'CMPT_UseHours_' + special
		if tag_use in data and key in data[tag_use] and tag_run in data and key in data[tag_run] :
			if flag == 0 and date in data[tag_use][key] and date in data[tag_run][key]:
				useHours = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
				runHours = float(data[tag_run][key][date]) if data[tag_run][key][date] <> '' else 0.0
				return round(useHours *100/ runHours , 4) if runHours <> 0.0 else 0.0
			elif flag == 1 :
				if yesterday in data[tag_use][key] and yesterday in data[tag_run][key]:
					useHours = float(data[tag_use][key][yesterday]) if data[tag_use][key][yesterday] <> '' else 0.0
					runHours = float(data[tag_run][key][yesterday]) if data[tag_run][key][yesterday] <> '' else 0.0
					return round(useHours *100/ runHours , 4) if runHours <> 0.0 else 0.0
				elif date != yesterday and date in data[tag_use][key] and date in data[tag_run][key]:
					useHours = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
					runHours = float(data[tag_run][key][date]) if data[tag_run][key][date] <> '' else 0.0
					return round(useHours *100/ runHours , 4) if runHours <> 0.0 else 0.0
		return value
	def getPowerLoadRate(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_hou = 'CMPT_UseHours_' + special
		tag_gen = 'CMPT_UserForGenerationHours_' + special
		if tag_hou in data and key in data[tag_hou] and tag_gen in data and key in data[tag_gen] :
			if flag == 0 and date in data[tag_hou][key] and date in data[tag_gen][key]:
				useHours = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
				userForGenerationHours = float(data[tag_gen][key][date]) if data[tag_gen][key][date] <> '' else 0.0
				return round(useHours *100/ userForGenerationHours , 4) if userForGenerationHours <> 0.0 else 0.0
			elif flag == 1 :
				if yesterday in data[tag_hou][key] and yesterday in data[tag_gen][key]:
					useHours = float(data[tag_hou][key][yesterday]) if data[tag_hou][key][yesterday] <> '' else 0.0
					userForGenerationHours = float(data[tag_gen][key][yesterday]) if data[tag_gen][key][yesterday] <> '' else 0.0
					return round(useHours *100/ userForGenerationHours , 4) if userForGenerationHours <> 0.0 else 0.0
				elif date != yesterday and date in data[tag_hou][key] and date in data[tag_gen][key]:
					useHours = float(data[tag_hou][key][date]) if data[tag_hou][key][date] <> '' else 0.0
					userForGenerationHours = float(data[tag_gen][key][date]) if data[tag_gen][key][date] <> '' else 0.0
					return round(useHours *100/ userForGenerationHours , 4) if userForGenerationHours <> 0.0 else 0.0
		return value
	def getAvailability(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_fau = 'CMPT_FaultHours_' + special
		if tag_fau in data and key in data[tag_fau]:
			if flag == 0 and date in data[tag_fau][key]:
				faultHours = float(data[tag_fau][key][date]) if data[tag_fau][key][date] <> '' else 0.0
				if key in self.WT_DevKeyList:
					return round((24.0-faultHours) *100/ 24.0 , 4) if 24.0-faultHours >= 0.0 else 0.0
				elif key in self.PV_DevKeyList:
					return round((9.0 - faultHours) *100/ 9.0 , 4) if 9.0-faultHours >= 0.0 else 0.0
				else:
					return round(((24.0 * self.Count_Dict[key]) - faultHours) *100/ (24.0 * self.Count_Dict[key]), 4) if (24.0 * self.Count_Dict[key] - faultHours) >= 0.0 else 0.0
			elif flag == 1 :
				if yesterday in data[tag_fau][key]:
					faultHours = float(data[tag_fau][key][yesterday]) if data[tag_fau][key][yesterday] <> '' else 0.0
					if key in self.WT_DevKeyList:
						return round((24.0-faultHours) *100/ 24.0 , 4) if 24.0-faultHours >= 0.0 else 0.0
					elif key in self.PV_DevKeyList:
						return round((9.0 - faultHours) *100/ 9.0 , 4) if 9.0-faultHours >= 0.0 else 0.0
					else:
						return round(((24.0 * self.Count_Dict[key]) - faultHours) *100/ (24.0 * self.Count_Dict[key]), 4) if (24.0 * self.Count_Dict[key] - faultHours) >= 0.0 else 0.0
				elif date != yesterday and date in data[tag_fau][key]:
					faultHours = float(data[tag_fau][key][date]) if data[tag_fau][key][date] <> '' else 0.0
					if key in self.WT_DevKeyList:
						return round((24.0-faultHours) *100/ 24.0 , 4) if 24.0-faultHours >= 0.0 else 0.0
					elif key in self.PV_DevKeyList:
						return round((9.0 - faultHours) *100/ 9.0 , 4) if 9.0-faultHours >= 0.0 else 0.0
					else:
						return round(((24.0 * self.Count_Dict[key]) - faultHours) *100/ (24.0 * self.Count_Dict[key]), 4) if (24.0 * self.Count_Dict[key] - faultHours) >= 0.0 else 0.0
		return value
	def getMTBF(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_fau = 'CMPT_FaultHours_' + special
		tag_cnt = 'CMPT_FaultCnt_' + special
		if tag_fau in data and key in data[tag_fau] and tag_cnt in data and key in data[tag_cnt]:
			if flag == 0 and date in data[tag_fau][key] and date in data[tag_cnt][key]:
				faultHours = float(data[tag_fau][key][date]) if data[tag_fau][key][date] <> '' else 0.0
				faultCnt = float(data[tag_cnt][key][date]) if data[tag_cnt][key][date] <> '' else 0.0
				if key in self.WT_DevKeyList:
					return round((24.0-faultHours) / faultCnt , 4) if 24.0-faultHours >= 0.0 else 0.0
				elif key in self.PV_DevKeyList:
					return round((9.0 - faultHours) / faultCnt , 4) if 9.0-faultHours >= 0.0 else 0.0
				else:
					return round(((24.0 * self.Count_Dict[key]) - faultHours)/ faultCnt, 4) if (24.0 * self.Count_Dict[key] - faultHours) >= 0.0 else 0.0
			elif flag == 1 :
				if yesterday in data[tag_fau][key] and yesterday in data[tag_cnt][key]:
					faultHours = float(data[tag_fau][key][yesterday]) if data[tag_fau][key][yesterday] <> '' else 0.0
					faultCnt = float(data[tag_cnt][key][yesterday]) if data[tag_cnt][key][yesterday] <> '' else 0.0
					if key in self.WT_DevKeyList:
						return round((24.0-faultHours) / faultCnt , 4) if 24.0-faultHours >= 0.0 else 0.0
					elif key in self.PV_DevKeyList:
						return round((9.0 - faultHours) / faultCnt , 4) if 9.0-faultHours >= 0.0 else 0.0
					else:
						return round(((24.0 * self.Count_Dict[key]) - faultHours)/ faultCnt, 4) if (24.0 * self.Count_Dict[key] - faultHours) >= 0.0 else 0.0
				elif date != yesterday and date in data[tag_fau][key] and date in data[tag_cnt][key]:
					faultHours = float(data[tag_fau][key][date]) if data[tag_fau][key][date] <> '' else 0.0
					faultCnt = float(data[tag_cnt][key][date]) if data[tag_cnt][key][date] <> '' else 0.0
					if key in self.WT_DevKeyList:
						return round((24.0-faultHours) / faultCnt , 4) if 24.0-faultHours >= 0.0 else 0.0
					elif key in self.PV_DevKeyList:
						return round((9.0 - faultHours) / faultCnt , 4) if 9.0-faultHours >= 0.0 else 0.0
					else:
						return round(((24.0 * self.Count_Dict[key]) - faultHours)/ faultCnt, 4) if (24.0 * self.Count_Dict[key] - faultHours) >= 0.0 else 0.0
		return value
	def getCAH(self, key, data, date, yesterday, special, flag):
		value = 0.0
		tag_use = 'CMPT_UserForGenerationHours_' + special
		tag_cnt = 'CMPT_FaultCnt_' + special
		if tag_use in data and key in data[tag_use] and tag_cnt in data and key in data[tag_cnt]:
			if flag == 0 and date in data[tag_use][key] and date in data[tag_cnt][key]:
				userForGenerationHours = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
				faultCnt = float(data[tag_cnt][key][date]) if data[tag_cnt][key][date] <> '' else 0.0
				return round(userForGenerationHours / faultCnt , 4) if faultCnt <> 0.0 else 0.0
			elif flag == 1 :
				if yesterday in data[tag_use][key] and yesterday in data[tag_cnt][key]:
					userForGenerationHours = float(data[tag_use][key][yesterday]) if data[tag_use][key][yesterday] <> '' else 0.0
					faultCnt = float(data[tag_cnt][key][yesterday]) if data[tag_cnt][key][yesterday] <> '' else 0.0
					return round(userForGenerationHours / faultCnt , 4) if faultCnt <> 0.0 else 0.0
				elif date != yesterday and date in data[tag_use][key] and date in data[tag_cnt][key]:
					userForGenerationHours = float(data[tag_use][key][date]) if data[tag_use][key][date] <> '' else 0.0
					faultCnt = float(data[tag_cnt][key][date]) if data[tag_cnt][key][date] <> '' else 0.0
					return round(userForGenerationHours / faultCnt , 4) if faultCnt <> 0.0 else 0.0
		return value
	def getOnGridProduction_(self, onGridDict_start, onGridDict_end):
		ong_dict = {}
		
		for period in self.PV_PeriodKeyList + self.WT_FarmKeyList+ self.CompanyKeyList + self.project:
		
			start = onGridDict_start[period]
			
			end = onGridDict_end[period]
			
			ong_dict[period] = end - start if end >= start else 0.0
		
		return ong_dict
	def getPurchaseProduction_(self, purchaseDict_start, purchaseDict_end):
		
		ong_dict = {}
		
		for period in self.PV_PeriodKeyList + self.WT_FarmKeyList:
		
			start = purchaseDict_start[period]
			
			end = purchaseDict_end[period]
			
			ong_dict[period] = end - start if end >= start else 0.0
		
		return ong_dict
	def getHouseProduction_(self, houseDict_start, houseDict_end):
		pass
		ong_dict = {}
		
		for period in self.PV_PeriodKeyList + self.WT_FarmKeyList:
		
			start = houseDict_start[period]
			
			end = houseDict_end[period]
			
			ong_dict[period] = end - start if end >= start else 0.0
		
		return ong_dict
	def Print(self):
		#self.Structure_Dict, self.Count_Dict, self.Cap_Dict, self.AllKeyList, self.CompanyKeyList, self.PV_FarmKeyList, self.WT_FarmKeyList, self.PV_PeriodKeyList, self.WT_PeriodKeyList, self.PV_LineKeyList, self.WT_LineKeyList, self.PV_DevKeyList, self.WT_DevKeyList = self.getAllDict()
		# print '--------------structure_dict---------------'
		# print self.Structure_Dict
		# print '--------------Structure---------------'
		# print self.Structure
		# print '------------count_dict-----------------'
		# print self.Count_Dict
		# print '------------Cap_Dict-----------------'
		# print self.Cap_Dict
		# print '--------------AllKeyList---------------'
		# print self.AllKeyList
		
		print '-------------companykeylist----------------'
		print self.CompanyKeyList
		print '------------pv_farmkeylist-----------------'
		print self.PV_FarmKeyList
		print '-----------wt_farmkeylist------------------'
		print self.WT_FarmKeyList
		print '-------------PV_PeriodKeyList----------------'
		print self.PV_PeriodKeyList
		print '-----------WT_PeriodKeyList------------------'
		print self.WT_PeriodKeyList
		print '-------------PV_LineKeyList----------------'
		print self.PV_LineKeyList
		print '-------------WT_LineKeyList----------------'
		print self.WT_LineKeyList
		# print '-------------PV_DevKeyList----------------'
		# print self.PV_DevKeyList
		# print '-------------wt_devkeylist----------------'
		# print self.WT_DevKeyList
		print '-------------project----------------'
		print self.project
		print '-------------DevType_Dict----------------'
		print self.DevType_Dict

if __name__ == "__main__":
	#pass
	date_now = datetime.datetime.now()
	print date_now
	re_cum = RECUM()
	re_cum.setDataByTagName('JXDTJK:SL:WTG001', '2017/10/31', 'CMPT_HouseProduction_Day', 26, 1)
	#re_cum.setDataByTagName('JXDTJK:XWLLZ', '2017/10/31', 'CMPT_HouseProduction_Day', 60, 1)
	#re_cum.setDataByTagName('JXDTJK:JS', '2017/10', 'CMPT_HouseProduction_Month', 5500, 1)
	#re_cum.setDataByTagName('JXDTJK:JS', '2017', 'CMPT_HouseProduction_Year', 666000, 1)
	#re_cum.setDataByTagName('JXDTJK:JS', '2017/10/19', 'CMPT_ProductionTheory_Day', 40)
	#re_cum.Print()
				
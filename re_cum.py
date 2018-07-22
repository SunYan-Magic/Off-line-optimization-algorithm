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

import YN_mongo as mg
import numpy
import json

class RECUM(object):

	def __init__(self):
		print '-------ini start-------'
		#---------conn----
		self.mongo = mg.Mongo(config.read('Mongo', 'Server'), int(config.read('Mongo', 'Port')))	
		
		self.mongo.connection()

		#self.kairos = kai.KairosDB(config.read('Kairos', 'Server'), config.read('Kairos', 'Port'))
		
		#self.kairos_new = kai_new.KairosDB(config.read('Kairos', 'Server'), config.read('Kairos', 'Port'))
		
		#self.mysql = my.MySQLInter()
		
		#----------------
		self.project = config.get('Project', 'Name')

		self.companys_list = self.mongo.getCompanys(self.project)
		
		#---------------dict-------------
		
		self.wt_farms_dict = self.mongo.getFarms_ByProject(self.project, 'WT')
		
		self.pv_farms_dict = self.mongo.getFarms_ByProject(self.project, 'PV')
		
		self.wt_periods_dict = dict((farm, self.mongo.getPeriods_ByFarm(self.project, farm)) for farm in self.wt_farms_dict)
		
		self.pv_periods_dict = dict((farm, self.mongo.getPeriods_ByFarm(self.project, farm)) for farm in self.pv_farms_dict)
		
		self.wt_devs_dict, self.wt_devKeys_dict = self.getDevs_ByPeriod(self.wt_periods_dict)
		
		self.pv_devs_dict, self.pv_devKeys_dict = self.getDevs_ByPeriod(self.pv_periods_dict)
		
		self.company_dict, self.company_keys_dict = self.getFarms_ByCompany()
		
		#-------keys---------------
		
		self.companyKey_list = [':'.join([self.project, company]) for company in self.companys_list]
		
		self.wt_farmKey_list = [':'.join([self.project, farm]) for farm in self.wt_farms_dict]
		
		self.pv_farmKey_list = [':'.join([self.project, farm]) for farm in self.pv_farms_dict]
		
		self.farmKeys_list = []
		
		self.farmKeys_list.extend(self.wt_farmKey_list)
		
		self.farmKeys_list.extend(self.pv_farmKey_list)
		
		self.windMeasur_list = list(farm +':WM01' for farm in self.farmKeys_list)
		
		self.wt_periodKey_list = self.getKeys_ByPeriod(self.wt_periods_dict)
		
		self.pv_periodKey_list = self.getKeys_ByPeriod(self.pv_periods_dict)
		
		self.periodKeys_list = []
		
		self.periodKeys_list.extend(self.wt_periodKey_list)
		
		self.periodKeys_list.extend(self.pv_periodKey_list)
		
		self.wt_devKey_list = self.getKeys_ByDev(self.wt_devs_dict)

		self.pv_devKey_list = self.getKeys_ByDev(self.pv_devs_dict)
		
		self.devKeys_list = []
		
		self.devKeys_list.extend(self.wt_devKey_list)
		
		self.devKeys_list.extend(self.pv_devKey_list)
		
		self.all_keyList = []
		
		self.all_keyList.append(self.project)
		
		self.all_keyList.extend(self.companyKey_list)
		
		self.all_keyList.extend(self.farmKeys_list)
		
		self.all_keyList.extend(self.periodKeys_list)
		
		self.all_keyList.extend(self.devKeys_list)
		
		self.wt_keyList = []
		
		self.wt_keyList.append(self.project)
		
		self.wt_keyList.extend(self.companyKey_list)
		
		self.wt_keyList.extend(self.wt_farmKey_list)
		
		self.wt_keyList.extend(self.wt_periodKey_list)
		
		self.wt_keyList.extend(self.wt_devKey_list)
		
		self.pv_keyList = []
		
		self.pv_keyList.append(self.project)
		
		self.pv_keyList.extend(self.companyKey_list)
		
		self.pv_keyList.extend(self.pv_farmKey_list)
		
		self.pv_keyList.extend(self.pv_periodKey_list)
		
		self.pv_keyList.extend(self.pv_devKey_list)
		
		self.keyListWithOutCompPro = []
		
		self.keyListWithOutCompPro.extend(self.wt_farmKey_list)
		
		self.keyListWithOutCompPro.extend(self.pv_farmKey_list)
		
		self.keyListWithOutCompPro.extend(self.wt_periodKey_list)
		
		self.keyListWithOutCompPro.extend(self.pv_periodKey_list)
		
		self.keyListWithOutCompPro.extend(self.wt_devKey_list)
		
		self.keyListWithOutCompPro.extend(self.pv_devKey_list)
		
		self.group = []
		
		self.group.append(self.project)
		
		self.group.extend(self.wt_farmKey_list)
		
		self.group.extend(self.pv_farmKey_list)
		
		self.group.extend(self.wt_periodKey_list)
		
		self.group.extend(self.pv_periodKey_list)
		
		self.group.extend(self.companyKey_list)
		
		self.companyDicts = self.getDevs_ByCompany()
		
		self.capDicts = self.getCapByFarms()
		
		self.production_plan = self.mongo.getTagsByStatisticsTags('CMPT_ProductionPlan')
		
		self.devTypeDict = self.getDevTypeByDev()
		
		self.wt_devTypes = self.mongo.getDevTypesByType(self.project, 'wtg')
		
		#--------tags--------------
		
		self.devTypeList = []
		
		self.devTypeList_wtg = self.mongo.getDevTypeByType('wtg')
		
		self.devTypeList_pv = self.mongo.getDevTypeByType('pv_inverter')
		
		self.devTypeList.extend(self.devTypeList_wtg)
		
		self.devTypeList.extend(self.devTypeList_pv)
		
		self.all_dev_obj_dict = self.getObj_ByAllDev()
		
		self.Caps = self.getCap()
		
		self.PF = list(farm + ':PF01' for farm in self.farmKeys_list)
		
		self.lines = self.getLineByPeriod()
		
		self.structure = self.mongo.getAllDevsByProject(self.project)
		
		self.structure_dict ,self.count_dict = self.getAllDict()
		
		print '-------ini end-------'
		
	def getAllDict(self):
		all_dict = {}
		count_dict = {}
		count = 0
		for company in self.structure:
			company_key = ':'.join([self.project, company])
			all_dict[company_key] = {}
			for farm in self.structure[company]:
				farm_key = ':'.join([self.project, farm])
				all_dict[company_key][farm_key] = {}
				for period in self.structure[company][farm]:
					period_key =  ':'.join([self.project, farm, period])
					all_dict[company_key][farm_key][period_key] = []
					for line in self.structure[company][farm][period]:
						
						for dev in self.structure[company][farm][period][line]:
							
							key = ':'.join([self.project, farm, dev])
							
							all_dict[company_key][farm_key][period_key].append(key)
							
							count += 1
							
					count_dict[period_key] = count
					
				count_dict[farm_key] = count
				
			count_dict[company_key] = count
				
		count_dict[self.project] = count
		
		return all_dict, count_dict
		
	def getCapByFarms(self):
		try:
			cap_dict = {}
			
			for farm in self.farmKeys_list:
			
				cap_dict[farm] = self.mongo.getCapacityByFarm(self.project, farm.split(':')[1])
			
			return cap_dict
		except:
			raise Exception(traceback.format_exc())
		
	def getLineByPeriod(self):
		
		lines_dict = {}
		
		for farm in self.wt_periods_dict:
			
			lines_dict[self.project+':'+farm] = {}
			
			for period in self.wt_periods_dict[farm]:
				
				lines_dict[self.project+':'+farm][self.project+':'+farm+':'+period] = {}
				
				lines = self.mongo.getLinesByPeriod(self.project, farm, period)
				
				for line in lines:
					
					lines_dict[self.project+':'+farm][self.project+':'+farm+':'+period][self.project+':'+farm+':'+line] = []
					
					for dev in self.mongo.getDevsByLine(self.project, farm, period, line):
						
						lines_dict[self.project+':'+farm][self.project+':'+farm+':'+period][self.project+':'+farm+':'+line].append(self.project+':'+farm+':'+dev)
						
		for farm in self.pv_periods_dict:
			
			lines_dict[self.project+':'+farm] = {}
			
			for period in self.pv_periods_dict[farm]:
				
				lines_dict[self.project+':'+farm][self.project+':'+farm+':'+period] = {}
				
				lines = self.mongo.getLinesByPeriod(self.project, farm, period)
				
				for line in lines:
					
					lines_dict[self.project+':'+farm][self.project+':'+farm+':'+period][self.project+':'+farm+':'+line] = []
					
					for dev in self.mongo.getDevsByLine(self.project, farm, period, line):
						
						lines_dict[self.project+':'+farm][self.project+':'+farm+':'+period][self.project+':'+farm+':'+line].append(self.project+':'+farm+':'+dev)
						
		return lines_dict
		
	def getDevTypeByDev(self):
		try:
			devDict = {}
			
			for farm in self.wt_devs_dict:
				
				devDict[self.project+':'+farm] = {}
				
				for period in self.wt_devs_dict[farm]:
					
					for dev in self.wt_devs_dict[farm][period]:
						
						devType = self.wt_devs_dict[farm][period][dev]['deviceType']
						
						if not devDict[self.project+':'+farm].has_key(devType):
							
							devDict[self.project+':'+farm][devType] = [self.project+':'+farm+':'+dev]
							
						else:
							
							devDict[self.project+':'+farm][devType].append(self.project+':'+farm+':'+dev)
							
			for farm in self.pv_devs_dict:
				
				devDict[self.project+':'+farm] = {}
				
				for period in self.pv_devs_dict[farm]:
					
					for dev in self.pv_devs_dict[farm][period]:
						
						devType = self.pv_devs_dict[farm][period][dev]['deviceType']
						
						if not devDict[self.project+':'+farm].has_key(devType):
							
							devDict[self.project+':'+farm][devType] = [self.project+':'+farm+':'+dev]
							
						else:
							
							devDict[self.project+':'+farm][devType].append(self.project+':'+farm+':'+dev) 
							
			return devDict
		except:
			raise Exception(traceback.format_exc())
		
	def getCap(self):
		try:
			cap_dict = {}
			
			for company in self.company_keys_dict:
				
				for farm in self.company_keys_dict[company]:
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								project_t, farm_t, device_t = dev.split(':')
								
								cap = self.mongo.getCapacityByDevice(project_t, farm_t, device_t)
								
								cap_dict[dev] = cap
								
							project_t, farm_t, period_t = period.split(':')
							
							cap = self.mongo.getCapacityByPeriod(project_t, farm_t, period_t)
							
							cap_dict[period] = cap * 1000.0
					
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								project_t, farm_t, device_t = dev.split(':')
								
								cap = self.mongo.getCapacityByDevice(project_t, farm_t, device_t)
								
								cap_dict[dev] = cap
								
							project_t, farm_t, period_t = period.split(':')
							
							cap = self.mongo.getCapacityByPeriod(project_t, farm_t, period_t)
							
							cap_dict[period] = cap*1000.0
					
					project_t, farm_t = farm.split(':')
					
					cap = self.mongo.getCapacityByFarm(project_t, farm_t)
					
					cap_dict[farm] = cap*1000.0
				
				project_t, company_t = company.split(':')
				
				cap = self.mongo.getCapacityByCompany(project_t, company_t)
					
				cap_dict[company] = cap*1000.0
					
			cap_dict[self.project] = self.mongo.getCapacityByProject(self.project) * 1000.0
			
			return cap_dict
		except:
			raise Exception(traceback.format_exc())
		
	def getObj_ByAllDev(self):
		try:
			allDev_dict = {}
			
			for farm in self.wt_devs_dict:
				
				for period in self.wt_devs_dict[farm]:
					
					for dev in self.wt_devs_dict[farm][period]:
						
						allDev_dict[':'.join([self.project, farm, dev])] = self.wt_devs_dict[farm][period][dev]
					
			for farm in self.pv_devs_dict:
				
				for period in self.pv_devs_dict[farm]:
					
					for dev in self.pv_devs_dict[farm][period]:
						
						allDev_dict[':'.join([self.project, farm, dev])] = self.pv_devs_dict[farm][period][dev]
			
			return allDev_dict
		except:
			raise Exception(traceback.format_exc())
			
	def getFarms_ByCompany(self):
		try:
			company_dict = {}
			
			companyList = []
			
			company_keys_dict = {}
			
			for farm in self.wt_farms_dict:
				
				company = self.wt_farms_dict[farm]['company']
				
				if company not in companyList:
					
					company_dict[company] = [farm]
					
					companyList.append(company)
					
					company_keys_dict[':'.join([self.project, company])] = [':'.join([self.project, farm])]
					
				else:
					company_dict[company].append(farm) 
					
					company_keys_dict[':'.join([self.project, company])].append(':'.join([self.project, farm]))
			
			for farm in self.pv_farms_dict:
				
				company = self.pv_farms_dict[farm]['company']
				
				if company not in companyList:
					
					company_dict[company] = [farm] 
					
					companyList.append(company)
					
					company_keys_dict[':'.join([self.project, company])]=[':'.join([self.project, farm])]
					
				else:
					company_dict[company].append(farm) 
					
					company_keys_dict[':'.join([self.project, company])].append(':'.join([self.project, farm]))
				
			return company_dict, company_keys_dict
		except:
			raise Exception(traceback.format_exc())
			
	def getKeys_ByDev(self, devDic):
		try:
			devs_list = []
			
			for farm in devDic:
				
				for period in devDic[farm]:
					
					devs_list.extend([':'.join([self.project, farm, dev]) for dev in devDic[farm][period]])
			
			return devs_list
		except:
			raise Exception(traceback.format_exc())
			
	def getKeys_ByPeriod(self, periods_dict):
		try:
			periods_list = []
			
			for farm in periods_dict:
				
				periods_list.extend([':'.join([self.project, farm, period]) for period in periods_dict[farm]])
				
			return periods_list
		except:
			raise Exception(traceback.format_exc())
			
	def getDevs_ByCompany(self):
		try:
			company_dicts = {}
			
			for company in self.company_dict:
				
				company_list = []
				
				for farm in self.company_dict[company]:
					
					if farm in self.wt_devs_dict:
						
						for period in self.wt_devs_dict[farm]:
						
							company_list.extend([':'.join([self.project, farm, dev]) for dev in self.wt_devs_dict[farm][period]])
						
					if farm in self.pv_devs_dict:
						
						for period in self.pv_devs_dict[farm]:
						
							company_list.extend([':'.join([self.project, farm, dev]) for dev in self.pv_devs_dict[farm][period]])
				
				company_dicts[':'.join([self.project, company])] = company_list
			
			return company_dicts
		except:
			raise Exception(traceback.format_exc())
			
	def getDevs_ByPeriod(self, periodDict):
		try:
			farmDic = {}
			
			farmKeys_Dic = {}
			
			for farm in periodDict:
				
				peDict = {}
				
				peKeys_Dict = {}
				
				for period in periodDict[farm]:
					
					peDict[period] = self.mongo.getDevs_ByPeriod(self.project, farm, period)
					
						
					peKeys_Dict[':'.join([self.project, farm, period])] = [':'.join([self.project, farm, dev]) for dev in peDict[period]]
					
				farmDic[farm] = peDict
				
				farmKeys_Dic[':'.join([self.project, farm])] = peKeys_Dict
			
			return farmDic, farmKeys_Dic
		except:
			raise Exception(traceback.format_exc())
		
	def setDataDev(self, value_dict, date, tagName, option):
		
		company_list = []
			
		for company in self.company_keys_dict:
			
			farm_list = []
			
			for farm in self.company_keys_dict[company]:
				
				if self.wt_devKeys_dict.has_key(farm):
					
					period_list = []
					
					for period in self.wt_devKeys_dict[farm]:
							
						value_List = []
							
						for dev in self.wt_devKeys_dict[farm][period]:
							
							if value_dict.has_key(dev):
								
								if value_dict[dev].has_key(date):
									
									if value_dict[dev][date].has_key(tagName):
										
										try:
											
											value = float(value_dict[dev][date][tagName])
											
											value_List.append(value)
										
										except:
											
											value_List.append(0.0)
						
						if option == 'max':
						
							period_list.append(self.max(period, date, tagName, value_List))
						
						elif option == 'min':
						
							period_list(self.min(period, date, tagName, value_List)) 
							
						elif option == 'avg_rate':
							
							period_list.append(self.avg_rate(period, date, tagName, value_List)) 
							
						elif option == 'avg':
							
							period_list.append(self.avg(period, date, tagName, value_List))
							
						elif option == 'sum':
							
							period_list.append(self.sum(period, date, tagName, value_List)) 
						
				if self.pv_devKeys_dict.has_key(farm):
					
					period_list = []
					
					for period in self.pv_devKeys_dict[farm]:
							
						value_List = []
							
						for dev in self.pv_devKeys_dict[farm][period]:
							
							if value_dict.has_key(dev):
								
								if value_dict[dev].has_key(date):
									
									if value_dict[dev][date].has_key(tagName):
										
										try:
											
											value = float(value_dict[dev][date][tagName])
											
											value_List.append(value)
										
										except:
											
											value_List.append(0.0)
						
						if option == 'max':
						
							period_list.append(self.max(period, date, tagName, value_List))
						
						elif option == 'min':
						
							period_list(self.min(period, date, tagName, value_List)) 
							
						elif option == 'avg_rate':
							
							period_list.append(self.avg_rate(period, date, tagName, value_List)) 
							
						elif option == 'avg':
							
							period_list.append(self.avg(period, date, tagName, value_List))
						
						elif option == 'sum':
							
							period_list.append(self.sum(period, date, tagName, value_List)) 
						
				if option == 'max':
				
					farm_list.append(self.max(farm, date, tagName, period_list))
				
				elif option == 'min':
				
					farm_list(self.min(farm, date, tagName, period_list)) 
					
				elif option == 'avg_rate':
					
					farm_list.append(self.avg_rate(farm, date, tagName, period_list)) 
					
				elif option == 'avg':
					
					farm_list.append(self.avg(farm, date, tagName, period_list))
					
				elif option == 'sum':
					
					farm_list.append(self.sum(farm, date, tagName, period_list))
			
			if option == 'max':
				
				company_list.append(self.max(company, date, tagName, farm_list))
			
			elif option == 'min':
			
				company_list(self.min(company, date, tagName, farm_list)) 
				
			elif option == 'avg_rate':
				
				company_list.append(self.avg_rate(company, date, tagName, farm_list)) 
				
			elif option == 'avg':
				
				company_list.append(self.avg(company, date, tagName, farm_list))
			
			elif option == 'sum':
				
				company_list.append(self.sum(company, date, tagName, farm_list)) 
					
		if option == 'max':
				
			return self.max(self.project, date, tagName, company_list)
		
		elif option == 'min':
		
			return self.min(self.project, date, tagName, company_list) 
			
		elif option == 'avg_rate':
			
			return self.avg_rate(self.project, date, tagName, company_list)
			
		elif option == 'avg':
			
			return self.avg(self.project, date, tagName, company_list)
		
		elif option == 'sum':
			
			return self.sum(self.project, date, tagName, company_list)
		
	def setDataPeriod(self, value_dict, date, tagName, option):
		
		company_list = []
			
		for company in self.company_keys_dict:
			
			farm_list = []
			
			for farm in self.company_keys_dict[company]:
				
				value_List = []
				
				if self.wt_devKeys_dict.has_key(farm):
					
					for period in self.wt_devKeys_dict[farm]:
							
						if value_dict.has_key(period):
							
							if value_dict[period].has_key(date):
								
								if value_dict[period][date].has_key(tagName):
									
									try:
										
										value = float(value_dict[period][date][tagName])
										#print period, date, tagName,value, '666666666666'
										value_List.append(value)
									
									except:
										
										value_List.append(0.0)
						
				if self.pv_devKeys_dict.has_key(farm):
					
					for period in self.pv_devKeys_dict[farm]:
							
						if value_dict.has_key(period):
							
							if value_dict[period].has_key(date):
								
								if value_dict[period][date].has_key(tagName):
									
									try:
										
										value = float(value_dict[period][date][tagName])
										
										value_List.append(value)
									
									except:
										
										value_List.append(0.0)
				'''							
				if value_dict.has_key(farm):
					
					if value_dict[farm].has_key(date):
						
						if value_dict[farm][date].has_key(tagName):
							
							try:
								
								value = float(value_dict[farm][date][tagName])
								
								value_List.append(value)
							
							except:
								
								value_List.append(0.0)
				'''
				#print farm, value_List, '222222222'
				
				if option == 'max':
				
					farm_list.append(self.max(farm, date, tagName, value_List))
				
				elif option == 'min':
				
					farm_list(self.min(farm, date, tagName, value_List)) 
					
				elif option == 'avg_rate':
					
					farm_list.append(self.avg_rate(farm, date, tagName, value_List)) 
					
				elif option == 'avg':
					
					farm_list.append(self.avg(farm, date, tagName, value_List))
				
				elif option == 'sum':
					
					#print farm, date, tagName, value_List
					
					farm_list.append(self.sum(farm, date, tagName, value_List)) 
			
			if option == 'max':
				
				company_list.append(self.max(company, date, tagName, farm_list))
			
			elif option == 'min':
			
				company_list(self.min(company, date, tagName, farm_list)) 
				
			elif option == 'avg_rate':
				
				company_list.append(self.avg_rate(company, date, tagName, farm_list)) 
				
			elif option == 'avg':
				
				company_list.append(self.avg(company, date, tagName, farm_list))
				
			elif option == 'sum':
				
				company_list.append(self.sum(company, date, tagName, farm_list)) 
					
		if option == 'max':
				
			return self.max(self.project, date, tagName, company_list)
		
		elif option == 'min':
		
			return self.min(self.project, date, tagName, company_list) 
			
		elif option == 'avg_rate':
			
			return self.avg_rate(self.project, date, tagName, company_list)
			
		elif option == 'avg':
			
			return self.avg(self.project, date, tagName, company_list)
			
		elif option == 'sum':
			
			return self.sum(self.project, date, tagName, company_list)
	
	def setDataFarm(self, value_dict, date, tagName, option):
		
		company_list = []
			
		for company in self.company_keys_dict:
			
			value_List = []
			
			for farm in self.company_keys_dict[company]:
				
				if value_dict.has_key(farm):
					
					if value_dict[farm].has_key(date):
						
						if value_dict[farm][date].has_key(tagName):
							
							try:
								
								value = float(value_dict[farm][date][tagName])
								
								value_List.append(value)
							
							except:
								
								value_List.append(0.0)
					
			if option == 'max':
				
				company_list.append(self.max(company, date, tagName, value_List))
			
			elif option == 'min':
			
				company_list(self.min(company, date, tagName, value_List)) 
				
			elif option == 'avg_rate':
				
				company_list.append(self.avg_rate(company, date, tagName, value_List)) 
				
			elif option == 'avg':
				
				company_list.append(self.avg(company, date, tagName, value_List))
				
			elif option == 'sum':
				
				company_list.append(self.sum(company, date, tagName, value_List)) 
					
		if option == 'max':
				
			return self.max(self.project, date, tagName, company_list)
		
		elif option == 'min':
		
			return self.min(self.project, date, tagName, company_list) 
			
		elif option == 'avg_rate':
			
			return self.avg_rate(self.project, date, tagName, company_list)
			
		elif option == 'avg':
			
			return self.avg(self.project, date, tagName, company_list)
			
		elif option == 'sum':
			
			return self.sum(self.project, date, tagName, company_list)
		
	def setDataCompany(self, value_dict, date, tagName, option):
		
		value_List = []
			
		for company in self.companyKey_list:
			
			if value_dict.has_key(company):
				
				if value_dict[company].has_key(date):
					
					if value_dict[company][date].has_key(tagName):
						
						try:
							
							value = float(value_dict[company][date][tagName])
							
							value_List.append(value)
						
						except:
							
							value_List.append(0.0)
		
		if option == 'max':
			
			return self.max(self.project, date, tagName, value_List)
		
		elif option == 'min':
			
			return self.min(self.project, date, tagName, value_List)
			
		elif option == 'avg_rate':
			
			return self.avg_rate(self.project, date, tagName, value_List)
			
		elif option == 'avg':
			
			return self.avg(self.project, date, tagName, value_List)
			
		elif option == 'sum':
			
			return self.sum(self.project, date, tagName, value_List)
			
	def max(self, key, date, tagName, value_List):
		
		if value_List <> []:
			try:
				self.mongo.update(key, date, {tagName:round(max(value_List), 4)})
				
				return round(max(value_List), 4)
			
			except:
				
				return 0.0
		else:
			return 0.0
		
	def min(self, key, date, tagName, value_List):
		
		if value_List <> []:
			
			try:
				self.mongo.update(key, date, {tagName:round(min(value_List), 4)})
				
				return round(min(value_List), 4)
			
			except:
				
				return 0.0
				
		else:
			return 0.0
		
	def avg_rate(self, key, date, tagName, value_List):
		
		if value_List <> []:
			try:
				self.mongo.update(key, date, {tagName:round(numpy.mean(value_List) * 100, 4)})
				
				return round(numpy.mean(value_List) * 100, 4)
			
			except:
				
				return 0.0
		else:
			return 0.0
				
	def avg(self, key, date, tagName, value_List):
				
		if value_List <> []:
			
			try:
				self.mongo.update(key, date, {tagName:round(numpy.mean(value_List), 4)})
				
				return round(numpy.mean(value_List), 4)
			
			except:
				
				return 0.0
		else:
			return 0.0
			
	def sum(self, key, date, tagName, value_List):
				
		if value_List <> []:
			
			try:
				self.mongo.update(key, date, {tagName:round(sum(value_List), 4)})
				
				return round(sum(value_List), 4)
			
			except:
				
				return 0.0
		else:
			return 0.0
		
	def setDataByOption(self, keyList, key_dict, date, tagName, option):
		#print key_dict
		value_dict = self.mongo.getStatisticsByKeyList_DateList(self.project, [date], keyList, [tagName])
		
		if 'device' in key_dict:
			
			self.setDataDev(value_dict, date, tagName, option)
		
		elif 'period' in key_dict:
			#print '0000'
			self.setDataPeriod(value_dict, date, tagName, option)
		
		elif 'farm' in key_dict:
			#print '1111'
			self.setDataFarm(value_dict, date, tagName, option)
			
		elif 'company' in key_dict:
			
			self.setDataCompany(value_dict, date, tagName, option)
		
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
	
	def judgeKeyLevel(self, key, date, tagName, option):
		
		keyList = []
		
		key_dict = {}
		
		if len(key.split(':')) == 2:
			
			if key in self.companyKey_list:
				
				keyList.append(self.project)
				
				keyList.extend(self.companyKey_list)
				
				key_dict['project'] = self.project
				
				key_dict['company'] = key
				
				self.setDataByOption(keyList, key_dict, date, tagName, option)
				
			if key in self.farmKeys_list:
				
				keyList.append(self.project)
				
				keyList.extend(self.companyKey_list)
				
				keyList.extend(self.farmKeys_list)
				
				key_dict['project'] = self.project
				
				key_dict['company'] = self.companyKey_list
				
				key_dict['farm'] = key
				
				self.setDataByOption(keyList, key_dict, date, tagName, option)
				
		if len(key.split(':')) == 3:
			
			if key in self.periodKeys_list:
				
				keyList.append(self.project)
				
				keyList.extend(self.companyKey_list)
				
				keyList.extend(self.farmKeys_list)
				
				keyList.extend(self.periodKeys_list)
				
				key_dict['project'] = self.project
				
				key_dict['company'] = self.companyKey_list
				
				key_dict['farm'] = self.farmKeys_list
				
				key_dict['period'] = key
				
				#print 'period, ----------'
				
				self.setDataByOption(keyList, key_dict, date, tagName, option)
				
			if key in self.devKeys_list:
				
				keyList = self.all_keyList
				
				key_dict['project'] = self.project
				
				key_dict['company'] = self.companyKey_list
				
				key_dict['farm'] = self.farmKeys_list
				
				key_dict['period'] = self.periodKeys_list
				
				key_dict['device'] = self.devKeys_list
				
				self.setDataByOption(keyList, key_dict, date, tagName, option)
				
					
		return keyList
	
	def setDataByTagName(self, key, date, tagName, value):
		try:
			#--------self-----------
			
			try:
				self.mongo.update(key, date, {tagName:float(value)}) 
				#print key, date, tagName, value
			except:
				
				self.mongo.update(key, date, {tagName:value})
			#------rel--------------
			relTag_dict = self.mongo.getStatisticsMap(tagName)
			
			if relTag_dict <> {}:
				
				relTag_tagList = [tagName]
				
				for tag in relTag_dict:
					
					relTag_tagList.extend(relTag_dict[tag])
					
				relTag_tagList = list(set(relTag_tagList))
				
				option = self.judgeTagOption(tagName)
				
				#print option
				
				keyList = self.judgeKeyLevel(key, date, tagName, option)
				
				#print key, date,  tagName, option, keyList, relTag_tagList
				#print relTag_tagList
				
				data = self.mongo.getStatisticsByKeyList_DateList(self.project, [date], keyList, relTag_tagList)
				
				if data <> {}:
				
					for tag in relTag_dict:
						
						self.cumTagByFormula_dev(data, keyList, date, tag)
			else:
				
				option = self.judgeTagOption(tagName)
				
				self.judgeKeyLevel(key, date, tagName, option)
			return True
		except:
			return False
	def setMyDataByTagName(self, key, date, tagName, value):
		try:
			self.mongo.update(key, date, {tagName:float(value)}) 
			#print key, date, tagName, value
		except:
			
			self.mongo.update(key, date, {tagName:value})
		#------rel--------------
		relTag_dict = self.mongo.getStatisticsMap(tagName)
		
		if relTag_dict <> {}:
			
			relTag_tagList = [tagName]
			
			for tag in relTag_dict:
				
				relTag_tagList.extend(relTag_dict[tag])
				
			relTag_tagList = list(set(relTag_tagList))
			
			option = self.judgeTagOption(tagName)
			
			#print option
			
			keyList = self.judgeKeyLevel(key, date, tagName, option)
			
			#print key, date,  tagName, option, keyList, relTag_tagList
			#print relTag_tagList
			
			data = self.mongo.getStatisticsByKeyList_DateList(self.project, [date], keyList, relTag_tagList)
			
			if data <> {}:
			
				for tag in relTag_dict:
					
					self.cumTagByFormula_dev(data, keyList, date, tag)
		else:
			
			option = self.judgeTagOption(tagName)
			
			self.judgeKeyLevel(key, date, tagName, option)
		
	def getFormulaByTagName(self, key, tagName, data, date):
		
		if tagName == 'CMPT_UseHours_Day':
		
			return self.getUsehours(key, data, date)
		
		elif tagName == 'CMPT_GenerateRate_Day':
			
			return self.getGenerateRate(key, data, date)
		
		elif tagName == 'CMPT_HouseRate_Day':
			
			return self.getHouseRate(key, data, date)
		
		elif tagName == 'CMPT_CompreHouseProduction_Day':
		
			return self.getCompreHouseProduction(key, data, date)
		
		elif tagName == 'CMPT_RateOfHousePower_Day':
		
			return self.getHousePowerRate(key, data, date)
			
		elif tagName == 'CMPT_HouseLost_Day':
		
			return self.getHouseLost(key, data, date)
			
		elif tagName == 'CMPT_HouseLostRate_Day':
		
			return self.getHouseLostRate(key, data, date)
		
		elif tagName == 'CMPT_UseRatio_Day':
			
			return self.getUseRatio(key, data, date)
			
		elif tagName == 'CMPT_OutPowerRatio_Day': 
			
			return self.getOutPowerRatio(key, data, date)
		
		elif tagName == 'CMPT_PowerLoadRate_Day': 
			
			return self.getPowerLoadRate(key, data, date)
			
		elif tagName == 'CMPT_Availability_Day': 
			
			return self.getAvailability(key, data, date)
		
		elif tagName == 'CMPT_MTBF_Day': 
			
			return self.getMTBF(key, data, date)
			
		elif tagName == 'CMPT_CAH_Day': 
			
			return self.getCAH(key, data, date)
			
		elif tagName == 'CMPT_Production_Day':
			
			return self.getProduction(key, data, date)
			
			
		
	def cumTagByFormula_dev(self, data, keyList, date, tagName):
		
		devs = {}
		devs_taglist = []
		devs_datalist = []
		
		for company in self.structure_dict:
			
			for farm in self.structure_dict[company]:
				
				for period in self.structure_dict[company][farm]:
				
					for dev in self.structure_dict[company][farm][period]:
						
						value = self.getFormulaByTagName(dev, tagName, data, date)
						devs_taglist.append({'object':dev, 'date':date})
						devs_datalist.append({tagName:value})
									
					value = self.getFormulaByTagName(period, tagName, data, date)
					devs_taglist.append({'object':period, 'date':date})
					devs_datalist.append({tagName:value})
									
				value = self.getFormulaByTagName(farm, tagName, data, date)
				devs_taglist.append({'object':farm, 'date':date})
				devs_datalist.append({tagName:value})
				
			value = self.getFormulaByTagName(company, tagName, data, date)
			devs_taglist.append({'object':company, 'date':date})
			devs_datalist.append({tagName:value})
		
		value = self.getFormulaByTagName(self.project, tagName, data, date)
		devs_taglist.append({'object':self.project, 'date':date})
		devs_datalist.append({tagName:value})
		
		self.mongo.setData(self.project, devs_taglist, devs_datalist)
		
	
	def getProduction(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
							
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_Production_Day'):
					try:
						value = float(data[key][date]['CMPT_Production_Day'])
					except:
						pass
		return value
		
	
	def getUsehours(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
							
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_Production_Day'):
					
					value = round(float(data[key][date]['CMPT_Production_Day']) / float(self.Caps[key]), 4) if data[key][date].has_key('CMPT_Production_Day') else 0.0
					
		return value
		
	def getGenerateRate(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_Production_Day'):
					
					production = float(data[key][date]['CMPT_Production_Day']) 
					
					if key in self.wt_devKey_list:
						
						windEnerge = float(data[key][date]['CMPT_WindEnerge_Day']) if data[key][date].has_key('CMPT_WindEnerge_Day') else 0.0
						
						if windEnerge <> 0.0:
						
							return round(production / windEnerge * 100 , 4) if ( production / windEnerge >= 0 ) and ( production / windEnerge <= 1 ) else 0.0
					
					if key in self.pv_devKey_list:
						
						totRadiation = float(data[key][date]['CMPT_TotRadiation_Day'])  if data[key][date].has_key('CMPT_TotRadiation_Day') else 0.0
						
						if totRadiation <> 0.0:
						
							return round(production / totRadiation * 100, 4) if ( production / totRadiation >= 0 ) and ( production / totRadiation <= 1 ) else 0.0
		
		return value
		
	def getHouseRate(self, key, data, date):
		
		value = 0.0
		
		if key in self.farmKeys_list:
		
			if data.has_key(key):
				
				if data[key].has_key(date):
					
					if data[key][date].has_key('CMPT_HouseProduction_Day') and data[key][date].has_key('CMPT_Production_Day'):
						
						houseRate = float(data[key][date]['CMPT_HouseProduction_Day']) if data[key][date]['CMPT_HouseProduction_Day'] <> '' else 0.0
						
						production = float(data[key][date]['CMPT_Production_Day']) if data[key][date]['CMPT_Production_Day'] <> '' else 0.0
						
						if production <> 0.0:
						
							return round(houseRate * 100 / production, 4)  if ( houseRate / production >= 0 ) and ( houseRate / production <= 1 ) else 0.0
						
		return value
			
	def getHousePowerRate(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_OnGridProduction_Day') and data[key][date].has_key('CMPT_PurchaseProduction_Day') and data[key][date].has_key('CMPT_Production_Day'):
					
					onGrid = float(data[key][date]['CMPT_OnGridProduction_Day']) if data[key][date]['CMPT_OnGridProduction_Day'] <> '' else 0.0
					
					purProduction = float(data[key][date]['CMPT_PurchaseProduction_Day']) if data[key][date]['CMPT_PurchaseProduction_Day'] <> '' else 0.0
					
					production = float(data[key][date]['CMPT_Production_Day']) if data[key][date]['CMPT_Production_Day'] <> '' else 0.0
					
					if production <> 0.0:
						
						rate = (production - onGrid + purProduction) * 100 / production if ((production - onGrid + purProduction) / production >= 0.0 ) and ((production - onGrid + purProduction) / production <= 1.0) else 0.0
						
						return round(rate, 4)
						
					
		return value
	
	def getCompreHouseProduction(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_OnGridProduction_Day') and data[key][date].has_key('CMPT_PurchaseProduction_Day') and data[key][date].has_key('CMPT_Production_Day'):
					
					onGrid = float(data[key][date]['CMPT_OnGridProduction_Day']) if data[key][date]['CMPT_OnGridProduction_Day'] <> '' else 0.0
					
					purProduction = float(data[key][date]['CMPT_PurchaseProduction_Day']) if data[key][date]['CMPT_PurchaseProduction_Day'] <> '' else 0.0
					
					production = float(data[key][date]['CMPT_Production_Day']) if data[key][date]['CMPT_Production_Day'] <> '' else 0.0
					
					#print key, date, onGrid, purProduction, production
					
					return round(production - onGrid + purProduction, 4) if (production - onGrid + purProduction) >= 0.0 else 0.0
					
			
		return value
	
	def getHouseLost(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_OnGridProduction_Day') and data[key][date].has_key('CMPT_PurchaseProduction_Day') and data[key][date].has_key('CMPT_Production_Day') and data[key][date].has_key('CMPT_HouseProduction_Day'):
					
					onGrid = float(data[key][date]['CMPT_OnGridProduction_Day']) if data[key][date]['CMPT_OnGridProduction_Day'] <> '' else 0.0
					
					purProduction = float(data[key][date]['CMPT_PurchaseProduction_Day']) if data[key][date]['CMPT_PurchaseProduction_Day'] <> '' else 0.0
					
					houseProduction = float(data[key][date]['CMPT_HouseProduction_Day']) if data[key][date]['CMPT_HouseProduction_Day'] <> '' else 0.0
					
					production = float(data[key][date]['CMPT_Production_Day']) if data[key][date]['CMPT_Production_Day'] <> '' else 0.0
					
					return round(production - onGrid + purProduction - houseProduction, 4) if (production - onGrid + purProduction - houseProduction) >= 0.0 else 0.0
					
		return value
		
	def getHouseLostRate(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_OnGridProduction_Day') and data[key][date].has_key('CMPT_PurchaseProduction_Day') and data[key][date].has_key('CMPT_Production_Day') and data[key][date].has_key('CMPT_HouseProduction_Day'):
					
					onGrid = float(data[key][date]['CMPT_OnGridProduction_Day']) if data[key][date]['CMPT_OnGridProduction_Day'] <> '' else 0.0
					
					purProduction = float(data[key][date]['CMPT_PurchaseProduction_Day']) if data[key][date]['CMPT_PurchaseProduction_Day'] <> '' else 0.0
					
					houseProduction = float(data[key][date]['CMPT_HouseProduction_Day']) if data[key][date]['CMPT_HouseProduction_Day'] <> '' else 0.0
					
					production = float(data[key][date]['CMPT_Production_Day']) if data[key][date]['CMPT_Production_Day'] <> '' else 0.0
					
					if production <> 0.0:
						
						return round((production - onGrid + purProduction - houseProduction) * 100 / production, 4) if ((production - onGrid + purProduction - houseProduction)/production >= 0.0)  and ((production - onGrid + purProduction - houseProduction)/production <= 1.0) else 0.0
					
		return value
		
	def getUseRatio(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_UseHours_Day'):
					
					useHours = float(data[key][date]['CMPT_UseHours_Day']) if data[key][date]['CMPT_UseHours_Day'] <> '' else 0.0
					
					if key in self.wt_devKey_list:
						
						return round(useHours *100/ 24.0 , 4)
						
					elif key in self.pv_devKey_list:
						
						return round(useHours *100/ 9.0 , 4)
					
					else:
						
						return round(useHours *100/ (24.0 * self.count_dict[key]), 4)
					
		return value
		
	def getOutPowerRatio(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_UseHours_Day') and data[key][date].has_key('CMPT_RunHours_Day'):
					
					useHours = float(data[key][date]['CMPT_UseHours_Day']) if data[key][date]['CMPT_UseHours_Day'] <> '' else 0.0
					
					runHours = float(data[key][date]['CMPT_RunHours_Day']) if data[key][date]['CMPT_RunHours_Day'] <> '' else 0.0
					
					return round(useHours *100/ runHours , 4) if runHours <> 0.0 else 0.0
		
		return value
		
	def getPowerLoadRate(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_UseHours_Day') and data[key][date].has_key('CMPT_UserForGenerationHours_Day'):
					
					useHours = float(data[key][date]['CMPT_UseHours_Day']) if data[key][date]['CMPT_UseHours_Day'] <> '' else 0.0
					
					userForGenerationHours = float(data[key][date]['CMPT_UserForGenerationHours_Day']) if data[key][date]['CMPT_UserForGenerationHours_Day'] <> '' else 0.0
					
					return round(useHours *100/ userForGenerationHours , 4) if userForGenerationHours <> 0.0 else 0.0
		
		return value
		
	def getAvailability(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_FaultHours_Day'):
					
					faultHours = float(data[key][date]['CMPT_FaultHours_Day']) if data[key][date]['CMPT_FaultHours_Day'] <> '' else 0.0
					
					if key in self.wt_devKey_list:
						
						return round((24.0-faultHours) *100/ 24.0 , 4) if 24.0-faultHours >= 0.0 else 0.0
						
					elif key in self.pv_devKey_list:
						
						return round((9.0 - faultHours) *100/ 9.0 , 4) if 9.0-faultHours >= 0.0 else 0.0
					
					else:
						
						return round(((24.0 * self.count_dict[key]) - faultHours) *100/ (24.0 * self.count_dict[key]), 4) if (24.0 * self.count_dict[key] - faultHours) >= 0.0 else 0.0
					
		return value
		
	def getMTBF(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_FaultHours_Day') and data[key][date].has_key('CMPT_FaultCnt_Day') :
					
					faultHours = float(data[key][date]['CMPT_FaultHours_Day']) if data[key][date]['CMPT_FaultHours_Day'] <> '' else 0.0
					
					faultCnt = float(data[key][date]['CMPT_FaultCnt_Day']) if data[key][date]['CMPT_FaultCnt_Day'] <> '' else 0.0
					
					if key in self.wt_devKey_list:
						
						return round((24.0-faultHours) / faultCnt , 4) if 24.0-faultHours >= 0.0 else 0.0
						
					elif key in self.pv_devKey_list:
						
						return round((9.0 - faultHours) / faultCnt , 4) if 9.0-faultHours >= 0.0 else 0.0
					
					else:
						
						return round(((24.0 * self.count_dict[key]) - faultHours)/ faultCnt, 4) if (24.0 * self.count_dict[key] - faultHours) >= 0.0 else 0.0
					
		return value
		
	def getCAH(self, key, data, date):
		
		value = 0.0
		
		if data.has_key(key):
			
			if data[key].has_key(date):
				
				if data[key][date].has_key('CMPT_UserForGenerationHours_Day') and data[key][date].has_key('CMPT_FaultCnt_Day') :
					
					userForGenerationHours = float(data[key][date]['CMPT_UserForGenerationHours_Day']) if data[key][date]['CMPT_UserForGenerationHours_Day'] <> '' else 0.0
					
					faultCnt = float(data[key][date]['CMPT_FaultCnt_Day']) if data[key][date]['CMPT_FaultCnt_Day'] <> '' else 0.0
					
					return round(userForGenerationHours / faultCnt , 4) if faultCnt <> 0.0 else 0.0
						
		return value
		
	def getOnGridProduction(self, onGridDict_start, onGridDict_end):
		
		ong_dict = {}
		
		for period in self.periodKeys_list:
		
			start = onGridDict_start[period]
			
			end = onGridDict_end[period]
			
			ong_dict[period] = end - start if end >= start else 0.0
		
		return ong_dict
		
	def getPurchaseProduction(self, purchaseDict_start, purchaseDict_end):
		
		ong_dict = {}
		
		for period in self.periodKeys_list:
		
			start = purchaseDict_start[period]
			
			end = purchaseDict_end[period]
			
			ong_dict[period] = end - start if end >= start else 0.0
		
		return ong_dict
		
	def getHouseProduction(self, houseDict_start, houseDict_end):
		
		ong_dict = {}
		
		for period in self.periodKeys_list:
		
			start = houseDict_start[period]
			
			end = houseDict_end[period]
			
			ong_dict[period] = end - start if end >= start else 0.0
		
		return ong_dict
if __name__ == "__main__":
	pass
	#re_cum = RECUM()
	#re_cum.setDataByTagName('GHDB:SYFD:WTG001', '2017/07/20', 'CMPT_Production_Day', 500)
				
				
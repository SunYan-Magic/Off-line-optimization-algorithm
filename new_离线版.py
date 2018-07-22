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
		#self.Pure_Dict为不分PV和WT的字典结构
		self.Pure_Dict, self.AllFarmKeyList, self.AllPeriodKeyList, self.AllLineKeyList ,self.AllDevKeyList = self.getPureDict()
		#得到全部的类型，mongo方法都在mongo那个文件的最下面，没有的话需要Ctrl+F
		self.devType = self.mongo.getAllDevType()
		#各类型下的设备
		self.DevType_Dict = self.mongo.getAllDevTypeByType_new(self.devType)
		print '-------ini end-------'
	def getAllDict(self):
		all_dict, count_dict, cap_dict = {}, {}, {}
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
	def getPureDict(self):
		
		pure_dict = {}
		farmkeylist, periodkeylist, linekeylist ,devkeylist = [], [], [], []
		for company in self.Structure:
			company_key = ':'.join([self.project, company])
			pure_dict[company_key] = {}
			for farm in self.Structure[company]:
				farm_key = ':'.join([self.project, farm])
				pure_dict[company_key][farm_key] = {}
				farmkeylist.append(farm_key)
					for period in self.Structure[company][farm]:
						period_key = ':'.join([self.project, farm, period])
						pure_dict[company_key][farm_key][period_key] = {}
						periodkeylist.append(period_key)
						for line in self.Structure[company][farm][period]:
							line_key = ':'.join([self.project, farm, line])
							pure_dict[company_key][farm_key][period_key][line_key] = []
							linekeylist.append(line_key)
							for dev in self.Structure[company][farm][period][line]:
								dev_key = ':'.join([self.project, farm, dev])
								pure_dict[company_key][farm_key][period_key][line_key].append(dev_key)
								devkeylist.append(dev_key)
		return pure_dict,farmkeylist, periodkeylist, linekeylist ,devkeylist
	
	
	
	
	
	
	
	
	def getDataByKairos(self, date_now)
		
		timestamp = (date_now-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
		starttime = (date_now-datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
		endtime = date_now.strftime('%Y-%m-%d 00:00:00')
		
		#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^风电^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		wind_dict = self.kairos.read_exDict(self.WT_DevKeyList, ['CMPT_WindSpeed_Avg'], starttime, endtime, '1')
		#-----MaxAvgMinOfWindSpeed
		myflag = self.getMaxAvgMinOfWindSpeed(wind_dict, timestamp)
		print 'MaxAvgMinOfWindSpeed=====', myflag, '====='
		#-----CMPT_WindSpeedValid
		myflag = self.getWindSpeedValid(wind_dict, timestamp)
		print 'WindSpeedValid=====', myflag, '====='
		
		airDen_dict = self.kairos.read_exDict(self.farmKeys_list, ['CMPT_AirDensity'], starttime, endtime, '600')
		#-----CMPT_WindEnerge
		myflag = self.getWindEnerge(wind_dict, airDen_dict, timestamp)
		print 'CMPT_WindEnerge=====', myflag, '====='
		del wind_dict, airDen_dict
		#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		rose_dict = self.kairos.read_exDict(self.WTKeyList, ['WTUR_WindEnerge', 'CMPT_WindSpeed_Avg'], starttime, endtime, '600')
		windir_dict = self.kairos.read_exDict(self.WTKeyList, ['CMPT_WindDir'], starttime, endtime, '600')
		
		#--------测风塔--------
		windMeasur_list = list(farm +':WM01' for farm in self.WT_FarmKeyList)
		windMeasur = self.kairos.read_exDict(windMeasur_list, ['WindMeasur_WindSpeed_10m','WindMeasur_Tmp'], starttime, endtime, '1')
		#-----MaxAvgOfWindSpeed_10
		myflag = self.getMaxAvgOfWindSpeed_10(windMeasur, timestamp)
		print 'MaxAvgOfWindSpeed_10=====', myflag, '====='
		#-----CMPT_Temp_Avg
		myflag = self.getTemp_Avg(windMeasur, timestamp)
		print 'CMPT_Temp_Avg=====', myflag, '====='
		
		del windMeasur_list, windMeasur
		#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		
		
		
		
		#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^光伏^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		radiation_dict = self.kairos.read_exDict(self.PV_DevKeyList, ['CMPT_Radiation'], starttime, endtime, '1')
		#-----CMPT_TotRadiation
		myflag = self.getTotRadiation(radiation_dict, timestamp)
		print 'CMPT_TotRadiation=====', myflag, '====='
		#-----CMPT_Radiation_Max/CMPT_Radiation_Avg
		myflag = self.getMaxAvgRadiation(radiation_dict, timestamp)
		print 'MaxAvgRadiation=====', myflag, '====='
		
		
		
		#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^不分风电和光伏^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
		act_dict = self.kairos.read_exDict(self.WT_DevKeyList, ['CMPT_ActPower'], starttime, endtime, '1')
		#----MaxAvgMinOfActPower
		myflag = self.getMaxAvgMinOfActPower(act_dict, timestamp)
		print 'MaxAvgMinOfActPower=====', myflag, '====='
		#----MaxMinOfActPower_Tm
		myflag = self.getMaxMinOfActPower_Tm(act_dict, timestamp)
		print 'MaxMinOfActPower_Tm=====', myflag, '====='
		#----CMPT_FullHours
		myflag = self.getFullHours(act_dict, timestamp)
		print 'CMPT_FullHours=====', myflag, '====='
		#----CMPT_GenrationHours/CMPT_OnGridHours
		myflag = self.getGenrationHours(act_dict, timestamp)
		print 'CMPT_GenrationHours/CMPT_OnGridHours=====', myflag, '====='
		del act_dict
		#-----------------------------未完成-------------------------------------
		hourCnt_dict = self.kairos.read_exDict(self.AllDevKeyList, ['CMPT_StandardStatus'], starttime, endtime, '1')
		#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
		agc_dict = self.kairos.read_exDict(self.AllFarmKeyList+self.AllPeriodKeyList, ['CMPT_AGCPower'], starttime, endtime, '1')
		#------------------------------------------------------------------------
		airDensity = self.kairos.read_exDict(self.AllFarmKeyList, ['CMPT_AirDensity'], starttime, endtime, '1')
		#----CMPT_AirDevsity_Avg
		myflag = self.getAirDevsity_Avg(airDensity, timestamp)
		print 'CMPT_AirDevsity_Avg=====', myflag, '====='
		
		
		
		
	def getDataByMongo(self, date_now)
		timestamp = (date_now-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
		starttime = (date_now-datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
		endtime = date_now.strftime('%Y-%m-%d 00:00:00')
		
		hours_end = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp], self.devKeys_list, ['CMPT_RunHours_Day','CMPT_ReadyHours_Day','CMPT_StopHours_Day','CMPT_FaultHours_Day','CMPT_ServiceHours_Day','CMPT_UnConnectHours_Day','CMPT_LimPwrHours_Day','CMPT_RepairHours_Day'])
		#----runHours, faultHours, serviceHours, repairHours, readyHours, stopHours, unConnectHours, limPwrHours
		myflag = self.CMPT_Hours(hours_end, timestamp)
		print 'runHours, faultHours, serviceHours, repairHours, readyHours, stopHours, unConnectHours, limPwrHours=====', myflag, '====='

		
		
		
		
		
		
		keylist = []
		keylist.append(self.project)
		keylist.extand(self.AllPeriodKeyList+self.AllFarmKeyList+self.CompanyKeyList)
		faultHours = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp], keylist, ['CMPT_FaultHours_Day'])
		#----CMPT_Availability
		myflag = self.getAvailability(faultHours, timestamp)
		print 'CMPT_Availability=====', myflag, '====='
		del faultHours
		
		
		
		
	
	def getMaxAvgMinOfWindSpeed(self, ex_dict, date):
		try:
			#求最大平均最小风速
			tag = 'CMPT_WindSpeed_Avg'
			Max_tag = 'CMPT_WindSpeed_Max_Day'
			Avg_tag = 'CMPT_WindSpeed_Avg_Day'
			Min_tag = 'CMPT_WindSpeed_Min_Day'
			taglist, datalist = [], []
			project_list = []
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'WT' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							farm_list = []
							for period in self.Structure_Dict[company][farmtype][farm]:
								period_list = []
								for line in self.Structure_Dict[company][farmtype][farm][period]:
									line_list = []
									for dev in self.Structure_Dict[company][farmtype][farm][period][line]:
										if tag in ex_dict and dev in ex_dict[tag] and date in ex_dict[tag][dev]:
											temp = ex_dict[tag][dev][date]
											line_list.append(temp)
										else: line_list.append(0.0)
									taglist.append({'object':line, 'date':date})
									datalist.append({Max_tag:round(max(line_list), 4)})
									taglist.append({'object':line, 'date':date})
									datalist.append({Avg_tag:round(numpy.mean(line_list), 4)})
									taglist.append({'object':line, 'date':date})
									datalist.append({Min_tag:round(min(line_list), 4)})
									period_list.extend(line_list)
								taglist.append({'object':period, 'date':date})
								datalist.append({Max_tag:round(max(period_list), 4)})
								taglist.append({'object':period, 'date':date})
								datalist.append({Avg_tag:round(numpy.mean(period_list), 4)})
								taglist.append({'object':period, 'date':date})
								datalist.append({Min_tag:round(min(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({Max_tag:round(max(farm_list), 4)})
							taglist.append({'object':farm, 'date':date})
							datalist.append({Avg_tag:round(numpy.mean(farm_list), 4)})
							taglist.append({'object':farm, 'date':date})
							datalist.append({Min_tag:round(min(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({Max_tag:round(max(company_list), 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({Avg_tag:round(numpy.mean(company_list), 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({Min_tag:round(min(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({Max_tag:round(max(project_list), 4)})
			taglist.append({'object':self.project, 'date':date})
			datalist.append({Avg_tag:round(numpy.mean(project_list), 4)})
			taglist.append({'object':self.project, 'date':date})
			datalist.append({Min_tag:round(min(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
		
	def getWindSpeedValid(self, ex_dict, date):
		try:
			#求最大平均最小风速
			tag = 'CMPT_WindSpeed_Avg'
			tag_wind = 'CMPT_WindSpeedValid_Day'
			taglist, datalist = [], []
			project_list = []
			
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'WT' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							farm_list = []
							for period in self.Structure_Dict[company][farmtype][farm]:
								period_list = []
								for line in self.Structure_Dict[company][farmtype][farm][period]:
									line_list = []
									count = 0
									for dev in self.Structure_Dict[company][farmtype][farm][period][line]:
										if tag in ex_dict and dev in ex_dict[tag]:
											for Date in ex_dict[tag][dev]:
												temp = float(ex_dict[tag][dev][Date]) if ex_dict[tag][dev][Date] <> '' else 0.0
												if temp <=25.0 and temp >=3.0:
												count += 1
										value = count/3600
										taglist.append({'object':dev, 'date':date})
										datalist.append({tag_wind:round(value, 4)})
										line_list.append(value)
									period_list.extend(line_list)
								taglist.append({'object':period, 'date':date})
								datalist.append({tag_wind:round(numpy.mean(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({tag_wind:round(numpy.mean(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({tag_wind:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tag_wind:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	
	def getWindEnerge(self, ex_dict, airDen_dict, date):
		try:
			#求最大平均最小风速
			tag_air = 'CMPT_AirDensity'
			tag_wind = 'CMPT_WindSpeed_Avg'
			tag = 'CMPT_WindEnerge_Day'
			taglist, datalist = [], []
			project_list = []
			swepArea = 5000
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'WT' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							farm_list = []
							dateList = airDen_dict[farm][tag_air].keys()
							for period in self.Structure_Dict[company][farmtype][farm]:
								period_list = []
								for line in self.Structure_Dict[company][farmtype][farm][period]:
									line_list = []
									for dev in self.Structure_Dict[company][farmtype][farm][period][line]:
										sum = 0.0
										if tag_wind in ex_dict and dev in ex_dict[tag_wind] and tag_air in airDen_dict and dev in airDen_dict[tag_air]:
											for Date in dateList:
												if Date in ex_dict[tag_wind][dev] and Date in airDen_dict[tag_air][farm] :
													air = float(airDen_dict[tag_air][farm][Date]) if airDen_dict[tag_air][farm][Date] <> '' else 0.0
													windSpeed = float(ex_dict[tag_wind][dev][Date]) if ex_dict[tag_wind][dev][Date] <> '' else 0.0
												sum += 0.5*air*swepArea*pow(windSpeed,3)
										taglist.append({'object':dev, 'date':date})
										datalist.append({tag:round(sum, 4)})
										line_list.append(sum)
									taglist.append({'object':line, 'date':date})
									datalist.append({tag:round(numpy.mean(line_list), 4)})
									period_list.extend(line_list)
								taglist.append({'object':period, 'date':date})
								datalist.append({tag:round(numpy.mean(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({tag:round(numpy.mean(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({tag:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tag:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	#看不明白离线里的getWindDir_rose,getRose未完成
	def getRose(self, rose_dict, windir_dict, date):
		try:
			tag_dir = 'CMPT_WindDir'
			tag_wind = 'CMPT_WindSpeed_Avg'
			wind_dir_list = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']
			wind_speed_group_list = ['0-3','3-4','4-5','5-6','6-7','7-8','8-9','9-10','10-11','11-12','12-13','13-14','14-15','15-25','25-99']
			wind_speed_group_dict = dict((wind_speed_group.split('-')) for wind_speed_group in wind_speed_group_list)
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'WT' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							sum = 0
							for period in self.Structure_Dict[company][farmtype][farm]:
								for line in self.Structure_Dict[company][farmtype][farm][period]:
									for dev in self.Structure_Dict[company][farmtype][farm][period][line]:
										count = 0.0
										timeList = sorted(windir_dict[tag_dir][dev])
										for date in timeList:
											if tag_dir in windir_dict and dev in windir_dict[tag_dir] and date in windir_dict[tag_dir][dev] and tag_wind in rose_dict and dev in rose_dict[tag_wind] and date in rose_dict[tag_wind][dev]:
												wind_dir = int(windir_dict[tag_dir][dev][date])
												wind_speed = float(ex_dict[tag_wind][dev][date])
												for wind_speed_min in wind_speed_group_dict:
													if wind_speed >= float(wind_speed_min) and wind_speed < float(wind_speed_group_dict[wind_speed_min]):
														count += 1
														if str(wind_dir) in wind_dir_list:
															wind_dir_dict[str(wind_dir)][wind_speed_min] += 1
															break
										sum += count
							
										
										
										taglist.append({'object':dev, 'date':date})
										datalist.append({tag:round(sum, 4)})
										line_list.append(sum)
									taglist.append({'object':line, 'date':date})
									datalist.append({tag:round(numpy.mean(line_list), 4)})
									period_list.extend(line_list)
								taglist.append({'object':period, 'date':date})
								datalist.append({tag:round(numpy.mean(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({tag:round(numpy.mean(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({tag:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tag:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
		
	def getMaxAvgOfWindSpeed_10(self, windSpeed_10m, date)
		try:
			tag = 'WindMeasur_WindSpeed_10m'
			Max_tag = 'CMPT_WindSpeed_10_Max_Day'
			Avg_tag = 'CMPT_WindSpeed_10_Avg_Day'
			taglist, datalist = [], []
			project_list = []
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'WT' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							farm_list = []
							windMeasur = farm+':WM01'
							for period in self.Structure_Dict[company][farmtype][farm]:
								period_list = []
								if tag in windSpeed_10m and windMeasur in windSpeed_10m[tag]:
									values = windSpeed_10m[tag][windMeasur].values()
									if values <> []:
										for value in values:
											if value <> '':
												period_list.append(float(value))
											else:
												period_list.append(0.0)
								taglist.append({'object':period, 'date':date})
								datalist.append({Max_tag:round(max(period_list), 4)})
								taglist.append({'object':period, 'date':date})
								datalist.append({Avg_tag:round(numpy.mean(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({Max_tag:round(max(farm_list), 4)})
							taglist.append({'object':farm, 'date':date})
							datalist.append({Avg_tag:round(numpy.mean(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({Max_tag:round(max(company_list), 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({Avg_tag:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({Max_tag:round(max(project_list), 4)})
			taglist.append({'object':self.project, 'date':date})
			datalist.append({Avg_tag:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
		
	def getTemp_Avg(self, windSpeed_10m, date)
		try:
			tag = 'WindMeasur_Tmp'
			Avg_tag = 'CMPT_Temp_Avg_Day'
			taglist, datalist = [], []
			project_list = []
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'WT' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							farm_list = []
							windMeasur = farm+':WM01'
							for period in self.Structure_Dict[company][farmtype][farm]:
								period_list = []
								if tag in windSpeed_10m and windMeasur in windSpeed_10m[tag]:
									values = windSpeed_10m[tag][windMeasur].values()
									if values <> []:
										for value in values:
											if value <> '':
												period_list.append(float(value))
											else:
												period_list.append(0.0)
								taglist.append({'object':period, 'date':date})
								datalist.append({Avg_tag:round(numpy.mean(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({Avg_tag:round(numpy.mean(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({Avg_tag:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({Avg_tag:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
		
	def getTotRadiation(radiation_dict, date)
		try:
			#求最大平均最小风速
			tag = 'CMPT_Radiation'
			tag_tot = 'CMPT_TotRadiation_Day'
			taglist, datalist = [], []
			project_list = []
			
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'PV' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							farm_list = []
							for period in self.Structure_Dict[company][farmtype][farm]:
								period_list = []
								for line in self.Structure_Dict[company][farmtype][farm][period]:
									line_list = []
									for dev in self.Structure_Dict[company][farmtype][farm][period][line]:
										sum = 0.0
										if tag in radiation_dict and dev in radiation_dict[tag]:
											for Date in radiation_dict[tag][dev]:
												sum = radiation_dict[tag][dev][Date] if radiation_dict[tag][dev][Date] <> '' else 0.0
										value = sum / (3600 * 3.6)
										taglist.append({'object':dev, 'date':date})
										datalist.append({tag_tot:round(value, 4)})
										line_list.append(value)
									taglist.append({'object':line, 'date':date})
									datalist.append({tag_tot:round(numpy.mean(line_list), 4)})
									period_list.extend(line_list)
								taglist.append({'object':period, 'date':date})
								datalist.append({tag_tot:round(numpy.mean(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({tag_tot:round(numpy.mean(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({tag_tot:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tag_tot:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
		
	def getMaxAvgRadiation(radiation_dict, date)
		try:
			tag = 'CMPT_Radiation'
			tag_max = 'CMPT_Radiation_Max_Day'
			tag_avg = 'CMPT_Radiation_Avg_Day'
			taglist, datalist = [], []
			project_list = []
			
			for company in self.Structure_Dict:
				company_list = []
				for farmtype in self.Structure_Dict[company]:
					if 'PV' in farmtype:
						for farm in self.Structure_Dict[company][farmtype]:
							farm_list = []
							for period in self.Structure_Dict[company][farmtype][farm]:
								period_list = []
								for line in self.Structure_Dict[company][farmtype][farm][period]:
									line_list = []
									for dev in self.Structure_Dict[company][farmtype][farm][period][line]:
										dev_list = []
										if tag in radiation_dict and dev in radiation_dict[tag]:
											for Date in radiation_dict[tag][dev]:
												temp = radiation_dict[tag][dev][Date] if radiation_dict[tag][dev][Date] <> '' else 0.0
												dev_list.append(temp)
										taglist.append({'object':dev, 'date':date})
										datalist.append({tag_max:round(max(dev_list), 4)})
										taglist.append({'object':dev, 'date':date})
										datalist.append({tag_avg:round(numpy.mean(dev_list), 4)})
										line_list.extend(dev_list)
									taglist.append({'object':line, 'date':date})
									datalist.append({tag_max:round(max(line_list), 4)})
									taglist.append({'object':line, 'date':date})
									datalist.append({tag_avg:round(numpy.mean(line_list), 4)})
									period_list.extend(line_list)
								taglist.append({'object':period, 'date':date})
								datalist.append({tag_max:round(max(period_list), 4)})
								taglist.append({'object':period, 'date':date})
								datalist.append({tag_avg:round(numpy.mean(period_list), 4)})
								farm_list.extend(period_list)
							taglist.append({'object':farm, 'date':date})
							datalist.append({tag_max:round(max(farm_list), 4)})
							taglist.append({'object':farm, 'date':date})
							datalist.append({tag_avg:round(numpy.mean(farm_list), 4)})
							company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({tag_max:round(max(company_list), 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({tag_avg:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tag_max:round(max(project_list), 4)})
			taglist.append({'object':self.project, 'date':date})
			datalist.append({tag_avg:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
		
		
		
		
	def getMaxAvgMinOfActPower(self, ex_dict, date):
		try:
			tag = 'CMPT_ActPower'
			Max_tag = 'CMPT_ActPower_Max_Day'
			Avg_tag = 'CMPT_ActPower_Avg_Day'
			Min_tag = 'CMPT_ActPower_Min_Day'
			taglist, datalist = [], []
			
			project_list = []
			for company in self.Pure_Dict:
				company_list = []
				for farm in self.Pure_Dict[company]:
					farm_list = []
					for period in self.Pure_Dict[company][farm]:
						period_list = []
						for line in self.Pure_Dict[company][farm][period]:
							line_list = []
							for dev in self.Pure_Dict[company][farm][period][line]:
								if tag in ex_dict and dev in ex_dict[tag] and date in ex_dict[tag][dev]:
									temp = ex_dict[tag][dev][date]
									line_list.append(temp)
								else: line_list.append(0.0)
							taglist.append({'object':line, 'date':date})
							datalist.append({Max_tag:round(max(line_list), 4)})
							taglist.append({'object':line, 'date':date})
							datalist.append({Avg_tag:round(numpy.mean(line_list), 4)})
							taglist.append({'object':line, 'date':date})
							datalist.append({Min_tag:round(min(line_list), 4)})
							period_list.extend(line_list)
						taglist.append({'object':period, 'date':date})
						datalist.append({Max_tag:round(max(period_list), 4)})
						taglist.append({'object':period, 'date':date})
						datalist.append({Avg_tag:round(numpy.mean(period_list), 4)})
						taglist.append({'object':period, 'date':date})
						datalist.append({Min_tag:round(min(period_list), 4)})
						farm_list.extend(period_list)
					taglist.append({'object':farm, 'date':date})
					datalist.append({Max_tag:round(max(farm_list), 4)})
					taglist.append({'object':farm, 'date':date})
					datalist.append({Avg_tag:round(numpy.mean(farm_list), 4)})
					taglist.append({'object':farm, 'date':date})
					datalist.append({Min_tag:round(min(farm_list), 4)})
					company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({Max_tag:round(max(company_list), 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({Avg_tag:round(numpy.mean(company_list), 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({Min_tag:round(min(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':company, 'date':date})
			datalist.append({Max_tag:round(max(project_list), 4)})
			taglist.append({'object':company, 'date':date})
			datalist.append({Avg_tag:round(numpy.mean(project_list), 4)})
			taglist.append({'object':company, 'date':date})
			datalist.append({Min_tag:round(min(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	
	def getMaxMinOfActPower_Tm(self, ex_dict, date):
		try:
			tag = 'CMPT_ActPower'
			Max_tag = 'CMPT_ActPower_Max_Tm_Day'
			Min_tag = 'CMPT_ActPower_Min_Tm_Day'
			taglist, datalist = [], []
			
			project_list = []
			for company in self.Pure_Dict:
				company_list = []
				for farm in self.Pure_Dict[company]:
					farm_list = []
					for period in self.Pure_Dict[company][farm]:
						period_list = []
						for line in self.Pure_Dict[company][farm][period]:
							line_list = []
							for dev in self.Pure_Dict[company][farm][period][line]:
								if tag in ex_dict and dev in ex_dict[tag] and date in ex_dict[tag][dev]:
									temp = ex_dict[tag][dev][date]
									line_list.append(temp)
								else: line_list.append(0.0)
							taglist.append({'object':line, 'date':date})
							datalist.append({Max_tag:round(max(line_list), 4)})
							taglist.append({'object':line, 'date':date})
							datalist.append({Min_tag:round(min(line_list), 4)})
							period_list.extend(line_list)
						taglist.append({'object':period, 'date':date})
						datalist.append({Max_tag:round(max(period_list), 4)})
						taglist.append({'object':period, 'date':date})
						datalist.append({Min_tag:round(min(period_list), 4)})
						farm_list.extend(period_list)
					taglist.append({'object':farm, 'date':date})
					datalist.append({Max_tag:round(max(farm_list), 4)})
					taglist.append({'object':farm, 'date':date})
					datalist.append({Min_tag:round(min(farm_list), 4)})
					company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({Max_tag:round(max(company_list), 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({Min_tag:round(min(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':company, 'date':date})
			datalist.append({Max_tag:round(max(project_list), 4)})
			taglist.append({'object':company, 'date':date})
			datalist.append({Min_tag:round(min(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	
	def getFullHours(self, ex_dict, date):
		try:
			tag = 'CMPT_ActPower'
			full_tag = 'CMPT_FullHours_Day'
			taglist, datalist = [], []
			
			project_list = []
			for company in self.Pure_Dict:
				company_list = []
				for farm in self.Pure_Dict[company]:
					farm_list = []
					for period in self.Pure_Dict[company][farm]:
						period_list = []
						for line in self.Pure_Dict[company][farm][period]:
							line_list = []
							for dev in self.Pure_Dict[company][farm][period][line]:
								devsum = 0.0
								if tag in ex_dict and dev in ex_dict[tag]:
									for Date in ex_dict[tag][dev]:
										if ex_dict[tag][dev][Date] >= self.Cap_Dict[dev] or ex_dict[tag][dev][Date] <= 1.2*self.Cap_Dict[dev]:
											devsum += 1
										taglist.append({'object':dev, 'date':date})
										datalist.append({full_tag:round(devsum/3600.0, 4)})
								line_list.append(devsum)
							taglist.append({'object':line, 'date':date})
							datalist.append({full_tag:round(numpy.mean(line_list), 4)})
							period_list.extend(line_list)
						taglist.append({'object':period, 'date':date})
						datalist.append({full_tag:round(numpy.mean(period_list), 4)})
						farm_list.extend(period_list)
					taglist.append({'object':farm, 'date':date})
					datalist.append({full_tag:round(numpy.mean(farm_list), 4)})
					company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({full_tag:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({full_tag:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	
	def getGenrationHours(self, ex_dict, date):
		try:
			tag = 'CMPT_ActPower'
			gen_tag = 'CMPT_GenrationHours_Day'
			#并网时间同实发电小时数一样
			ongrid_tag = 'CMPT_OnGridHours_Day'
			taglist, datalist = [], []
			
			project_list = []
			for company in self.Pure_Dict:
				company_list = []
				for farm in self.Pure_Dict[company]:
					farm_list = []
					for period in self.Pure_Dict[company][farm]:
						period_list = []
						for line in self.Pure_Dict[company][farm][period]:
							line_list = []
							for dev in self.Pure_Dict[company][farm][period][line]:
								devsum = 0.0
								if tag in ex_dict and dev in ex_dict[tag]:
									for Date in ex_dict[tag][dev]:
										if ex_dict[tag][dev][Date] >= 0 or ex_dict[tag][dev][Date] <= 1.2*self.Cap_Dict[dev]:
											devsum += 1
								temp = devsum/3600.0
								taglist.append({'object':dev, 'date':date})
								datalist.append({gen_tag:round(temp, 4)})
								taglist.append({'object':dev, 'date':date})
								datalist.append({ongrid_tag:round(temp, 4)})
								line_list.append(devsum)
							temp = numpy.mean(line_list)
							taglist.append({'object':line, 'date':date})
							datalist.append({gen_tag:round(temp, 4)})
							taglist.append({'object':line, 'date':date})
							datalist.append({ongrid_tag:round(temp, 4)})
							period_list.extend(line_list)
						temp = numpy.mean(period_list)
						taglist.append({'object':period, 'date':date})
						datalist.append({gen_tag:round(temp, 4)})
						taglist.append({'object':period, 'date':date})
						datalist.append({ongrid_tag:round(temp, 4)})
						farm_list.extend(period_list)
					temp = numpy.mean(farm_list)
					taglist.append({'object':farm, 'date':date})
					datalist.append({gen_tag:round(temp, 4)})
					taglist.append({'object':farm, 'date':date})
					datalist.append({ongrid_tag:round(temp, 4)})
					company_list.extend(farm_list)
				temp = numpy.mean(company_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({gen_tag:round(temp, 4)})
				taglist.append({'object':company, 'date':date})
				datalist.append({ongrid_tag:round(temp, 4)})
				project_list.extend(company_list)
			temp = numpy.mean(project_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({gen_tag:round(temp, 4)})
			taglist.append({'object':self.project, 'date':date})
			datalist.append({ongrid_tag:round(temp, 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	
	def getAirDevsity_Avg(self, airDensity, date)
		try:
			tag = 'CMPT_AirDensity'
			Avg_tag = 'CMPT_AirDensity_Avg_Day'
			taglist, datalist = [], []
			
			project_list = []
			for company in self.Pure_Dict:
				company_list = []
				for farm in self.Pure_Dict[company]:
					farm_list = []
					if tag in airDensity and farm in airDensity[tag]:
						values = airDensity[tag][farm].values()
						if values <> []:
							for value in values:
								if value <> '':
									farm_list.append(float(value))
								else:
									farm_list.append(0.0)
					taglist.append({'object':farm, 'date':date})
					datalist.append({Avg_tag:round(numpy.mean(farm_list), 4)})
					company_list.extend(farm_list)
				taglist.append({'object':company, 'date':date})
				datalist.append({Avg_tag:round(numpy.mean(company_list), 4)})
				project_list.extend(company_list)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({Avg_tag:round(numpy.mean(project_list), 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	
	
	def getTagsValueByStatisticsTags(self, tagName, keyList, start, end, diff):
		try:
			tag_dict = self.mongo.getTagsByStatisticsTags(tagName)
			time_list = []
			if diff == '':
				key_dict = dict((key , 0.0) for key in keyList)
			else:
				stoptime = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
				starttime = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
				starttime_stamp = time.mktime(starttime.timetuple())*1000
				stoptime_stamp = time.mktime(stoptime.timetuple())*1000
				
				timeList = list(range(int(starttime_stamp),int(stoptime_stamp)+int(diff)*1000,int(diff)*1000))
				key_dict = {}
				for key in keyList:
					key_dict[key] = {}
					for timeTemp in timeList:
						timestruct = time.localtime(timeTemp/1000)
						timeT = time.strftime('%Y-%m-%d %H:%M:%S',timestruct)
						time_list.append(timeT)
						key_dict[key][timeT] = 0.0
			for key in keyList:
				if tag_dict.has_key(key):
					value_dict = {}
					for value in tag_dict[key]:
						key_temp = value.keys()[0]
						tagName_temp = value[key_temp].keys()[0]
						ratio = value[key_temp][tagName_temp]
						new_value_dict = {}
						if diff <> '':
							value_temp_dict = self.kairos.readAchiveDataTimePer(key_temp,tagName_temp,start,end,diff,'')
							for time_temp in value_temp_dict:
								new_value_dict[time_temp] = float(value_temp_dict[time_temp]) * float(ratio) if value_temp_dict[time_temp] <> '' else 0.0
						else:
							value_temp_dict = self.kairos.readArchiveData(key_temp, tagName_temp, end)
							new_value_dict[end] = float(value_temp_dict[tagName_temp]) * float(ratio) if value_temp_dict[tagName_temp] <> '' else 0.0
						value_dict[key_temp] = new_value_dict
					sum = 0.0
					temp_dict= dict((timeT, 0.0) for timeT in time_list)
					if diff == '':
						for tag in value_dict:
							sum += float(value_dict[tag][end])
						key_dict[key] = sum
					else:
						for tag in value_dict:
							for timeT in time_list:
								temp_dict[timeT] += float(value_dict[tag][timeT])
						key_dict[key] = temp_dict
			return key_dict
		except:
			raise Exception(traceback.format_exc())
	
	
	
	
	
	def CMPT_Hours(self, hours_end, date):
		try:
			taglist = ['CMPT_RunHours_Day', 'CMPT_FaultHours_Day', 'CMPT_ServiceHours_Day','CMPT_RepairHours_Day','CMPT_ReadyHours_Day', 'CMPT_StopHours_Day', 'CMPT_UnConnectHours_Day', 'CMPT_LimPwrHours_Day']
			project_list = [0.0]*8
			for company in self.Pure_Dict:
				company_list = [0.0]*8
				for farm in self.Pure_Dict[company]:
					farm_list = [0.0]*8
					for period in self.Pure_Dict[company][farm]:
						period_list = [0.0]*8
						for line in self.Pure_Dict[company][farm][period]:
							line_list = [0.0]*8
							for dev in self.Pure_Dict[company][farm][period][line]:
								for i in range(8):
									tag = taglist[i]
									if tag in hours_end and dev in hours_end[tag] and date in hours_end[tag][dev]:
										line_list[i] += float(hours_end[tag][dev][date]) if hours_end[tag][dev][date] <> '' else 0.0 
							for i in range(8):
								period_list[i] += line_list[i] 
								taglist.append({'object':line, 'date':date})
								datalist.append({taglist[i]:round(line_list[i], 4)})
						for i in range(8):
							farm_list[i] += period_list[i] 
							taglist.append({'object':period, 'date':date})
							datalist.append({taglist[i]:round(period_list[i], 4)})
					for i in range(8):
						company_list[i] += farm_list[i] 
						taglist.append({'object':farm, 'date':date})
						datalist.append({taglist[i]:round(farm_list[i], 4)})
				for i in range(8):
					project_list[i] += company_list[i] 
					taglist.append({'object':company, 'date':date})
					datalist.append({taglist[i]:round(company_list[i], 4)})
			for i in range(8):
				taglist.append({'object':self.project, 'date':date})
				datalist.append({taglist[i]:round(project_list[i], 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
			
			
			
			
			
			runHours_project, faultHours_project, serviceHours_project, repairHours_project, readyHours_project, stopHours_project, unConnectHours_project, limPwrHours_project = 0.0, 0.0, 0.0, 0.0,0.0, 0.0, 0.0, 0.0
			for company in self.company_keys_dict:
				runHours_company, faultHours_company, serviceHours_company, repairHours_company, readyHours_company, stopHours_company, unConnectHours_company, limPwrHours_company = 0.0, 0.0, 0.0, 0.0,0.0, 0.0, 0.0, 0.0
				
				for farm in self.company_keys_dict[company]:
					
					runHours_farm, faultHours_farm, serviceHours_farm, repairHours_farm, readyHours_farm, stopHours_farm, unConnectHours_farm, limPwrHours_farm = 0.0, 0.0, 0.0, 0.0,0.0, 0.0, 0.0, 0.0
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							runHours_period, faultHours_period, serviceHours_period, repairHours_period, readyHours_period, stopHours_period, unConnectHours_period, limPwrHours_period = 0.0, 0.0, 0.0, 0.0,0.0, 0.0, 0.0, 0.0
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								if hours_end.has_key(dev):
									
									if hours_end[dev].has_key(day_date):
										
										runHours_period += float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
										
										faultHours_period += float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
										
										serviceHours_period += float(hours_end[dev][day_date]['CMPT_ServiceHours_Day']) if hours_end[dev][day_date].has_key('CMPT_ServiceHours_Day') else 0.0
										
										repairHours_period += float(hours_end[dev][day_date]['CMPT_RepairHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RepairHours_Day') else 0.0
										
										readyHours_period += float(hours_end[dev][day_date]['CMPT_ReadyHours_Day']) if hours_end[dev][day_date].has_key('CMPT_ReadyHours_Day') else 0.0
										
										stopHours_period += float(hours_end[dev][day_date]['CMPT_StopHours_Day']) if hours_end[dev][day_date].has_key('CMPT_StopHours_Day') else 0.0
										
										unConnectHours_period += float(hours_end[dev][day_date]['CMPT_UnConnectHours_Day']) if hours_end[dev][day_date].has_key('CMPT_UnConnectHours_Day') else 0.0
										
										limPwrHours_period += float(hours_end[dev][day_date]['CMPT_LimPwrHours_Day']) if hours_end[dev][day_date].has_key('CMPT_LimPwrHours_Day') else 0.0
										
							
							runHours[period] = round(runHours_period, 4)
							
							faultHours[period] = round(faultHours_period, 4)
							
							serviceHours[period] = round(serviceHours_period, 4)
							
							repairHours[period] = round(repairHours_period, 4)
							
							readyHours[period] = round(readyHours_period, 4)
							
							stopHours[period] = round(stopHours_period, 4)
							
							unConnectHours[period] = round(unConnectHours_period, 4)
							
							limPwrHours[period] = round(limPwrHours_period, 4)
							
							runHours_farm += runHours_period
									
							faultHours_farm += faultHours_period
							
							serviceHours_farm += serviceHours_period
							
							repairHours_farm += repairHours_period
							
							readyHours_farm += readyHours_period
							
							stopHours_farm += stopHours_period
							
							unConnectHours_farm += unConnectHours_period
							
							limPwrHours_farm += limPwrHours_period
					
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							runHours_period, faultHours_period, serviceHours_period, repairHours_period, readyHours_period, stopHours_period, unConnectHours_period, limPwrHours_period = 0.0, 0.0, 0.0, 0.0,0.0, 0.0, 0.0, 0.0
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								if hours_end.has_key(dev):
									
									if hours_end[dev].has_key(day_date):
										
										runHours_period += float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
										
										faultHours_period += float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
										
										serviceHours_period += float(hours_end[dev][day_date]['CMPT_ServiceHours_Day']) if hours_end[dev][day_date].has_key('CMPT_ServiceHours_Day') else 0.0
										
										repairHours_period += float(hours_end[dev][day_date]['CMPT_RepairHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RepairHours_Day') else 0.0
										
										readyHours_period += float(hours_end[dev][day_date]['CMPT_ReadyHours_Day']) if hours_end[dev][day_date].has_key('CMPT_ReadyHours_Day') else 0.0
										
										stopHours_period += float(hours_end[dev][day_date]['CMPT_StopHours_Day']) if hours_end[dev][day_date].has_key('CMPT_StopHours_Day') else 0.0
										
										unConnectHours_period += float(hours_end[dev][day_date]['CMPT_UnConnectHours_Day']) if hours_end[dev][day_date].has_key('CMPT_UnConnectHours_Day') else 0.0
										
										limPwrHours_period += float(hours_end[dev][day_date]['CMPT_LimPwrHours_Day']) if hours_end[dev][day_date].has_key('CMPT_LimPwrHours_Day') else 0.0
										
							
							runHours[period] = round(runHours_period, 4)
							
							faultHours[period] = round(faultHours_period, 4)
							
							serviceHours[period] = round(serviceHours_period, 4)
							
							repairHours[period] = round(repairHours_period, 4)
							
							readyHours[period] = round(readyHours_period, 4)
							
							stopHours[period] = round(stopHours_period, 4)
							
							unConnectHours[period] = round(unConnectHours_period, 4)
							
							limPwrHours[period] = round(limPwrHours_period, 4)
					
							runHours_farm += runHours_period
									
							faultHours_farm += faultHours_period
							
							serviceHours_farm += serviceHours_period
							
							repairHours_farm += repairHours_period
							
							readyHours_farm += readyHours_period
							
							stopHours_farm += stopHours_period
							
							unConnectHours_farm += unConnectHours_period
							
							limPwrHours_farm += limPwrHours_period
					
					runHours[farm] = round(runHours_farm, 4)
									
					faultHours[farm] = round(faultHours_farm, 4)
					
					serviceHours[farm] = round(serviceHours_farm, 4)
					
					repairHours[farm] = round(repairHours_farm, 4)
					
					readyHours[farm] = round(readyHours_farm, 4)
					
					stopHours[farm] = round(stopHours_farm, 4)
					
					unConnectHours[farm] = round(unConnectHours_farm, 4)
					
					limPwrHours[farm] = round(limPwrHours_farm, 4)
					
					runHours_company += runHours_farm
									
					faultHours_company += faultHours_farm
					
					serviceHours_company += serviceHours_farm
					
					repairHours_company += repairHours_farm
					
					readyHours_company += readyHours_farm
					
					stopHours_company += stopHours_farm
					
					unConnectHours_company += unConnectHours_farm
					
					limPwrHours_company += limPwrHours_farm
					
				runHours[company] = round(runHours_company, 4)
									
				faultHours[company] = round(faultHours_company, 4)
				
				serviceHours[company] = round(serviceHours_company, 4)
				
				repairHours[company] = round(repairHours_company, 4)
				
				readyHours[company] = round(readyHours_company, 4)
				
				stopHours[company] = round(stopHours_company, 4)
				
				unConnectHours[company] = round(unConnectHours_company, 4)
				
				limPwrHours[company] = round(limPwrHours_company, 4)
				
				runHours_project += runHours_company
									
				faultHours_project += faultHours_company
				
				serviceHours_project += serviceHours_company
				
				repairHours_project += repairHours_company
				
				readyHours_project += readyHours_company
				
				stopHours_project += stopHours_company
				
				unConnectHours_project += unConnectHours_company
				
				limPwrHours_project += limPwrHours_company
				
			runHours[self.project] = round(runHours_project, 4)
									
			faultHours[self.project] = round(faultHours_project, 4)
			
			serviceHours[self.project] = round(serviceHours_project, 4)
			
			repairHours[self.project] = round(repairHours_project, 4)
			
			readyHours[self.project] = round(readyHours_project, 4)
			
			stopHours[self.project] = round(stopHours_project, 4)
			
			unConnectHours[self.project] = round(unConnectHours_project, 4)
			
			limPwrHours[self.project] = round(limPwrHours_project, 4)
			
			return runHours, faultHours, serviceHours, repairHours, readyHours, stopHours, unConnectHours, limPwrHours
		except:
			raise Exception(traceback.format_exc())
	
	def getAvailability(self, faultHours, date)
		try:
			ava_tag = 'CMPT_Availability_Day'
			taglist, datalist = [], []
			
			project_count = 0.0
			for company in self.Pure_Dict:
				company_count = 0.0
				for farm in self.Pure_Dict[company]:
					farm_count = 0.0
					for period in self.Pure_Dict[company][farm]:
						period_count = 0.0
						for line in self.Pure_Dict[company][farm][period]:
							line_count = 0.0
							for dev in self.Pure_Dict[company][farm][period][line]:
								line_count += 1
							line_ava = (24.0 * line_count - faultHours[line]) * 100/ (24.0* line_count)
							taglist.append({'object':line, 'date':date})
							datalist.append({full_tag:round(line_ava, 4)})
							period_count += line_count
						period_ava = (24.0 * period_count - faultHours[period]) * 100/ (24.0* period_count)
						taglist.append({'object':period, 'date':date})
						datalist.append({full_tag:round(period_ava, 4)})
						farm_count += period_count
					farm_ava = (24.0 * farm_count - faultHours[farm]) * 100/ (24.0* farm_count)
					taglist.append({'object':farm, 'date':date})
					datalist.append({full_tag:round(farm_ava, 4)})
					company_count += farm_count
				company_ava = (24.0 * company_count - faultHours[company]) * 100/ (24.0* company_count)
				taglist.append({'object':company, 'date':date})
				datalist.append({full_tag:round(company_ava, 4)})
				project_count += company_count
			project_ava = (24.0 * project_count - faultHours[self.project]) * 100/ (24.0* project_count)
			taglist.append({'object':self.project, 'date':date})
			datalist.append({full_tag:round(project_ava, 4)})
			self.mongo.setData(self.project, taglist, datalist)
			return True
		except:
			return False
	
	
	
	
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
				
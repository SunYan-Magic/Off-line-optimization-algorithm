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
import YN_Kairosdb as kai
import YN_Kairosdb_Pool as kai_new
import YN_configuration as config
import YN_redisdb as red
import YN_mongo as mg
import numpy
import json

class off(object):

	def __init__(self):
		print '-------ini start-------'
		#---------conn----
		self.mongo = mg.Mongo(config.read('Mongo', 'Server'), int(config.read('Mongo', 'Port')))	
		
		self.mongo.connection()

		self.kairos = kai.KairosDB(config.read('Kairos', 'Server'), config.read('Kairos', 'Port'))
		
		self.kairos_new = kai_new.KairosDB(config.read('Kairos', 'Server'), config.read('Kairos', 'Port'))
		
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
		
		self.production_dict = self.getProduction()
		
		self.wt_devTypes = self.mongo.getDevTypesByType(self.project, 'wtg')
		
		self.formatWindPowerByDevType = self.getFormatWindPowerByDevType()
		
		self.hardThresholdFiltering = self.getHardThresholdFiltering()
		
		#--------tags--------------
		
		self.devTypeList = []
		
		self.devTypeList_wtg = self.mongo.getDevTypeByType('wtg')
		
		self.devTypeList_pv = self.mongo.getDevTypeByType('pv_inverter')
		
		self.devTypeList.extend(self.devTypeList_wtg)
		
		self.devTypeList.extend(self.devTypeList_pv)
		
		#self.devType_windSpeedValid_dict = self.getValidWindSpeed_ByDevType()
		
		#self.devType_sweptArea_dict = self.getSweptArea_ByDevType()
		
		self.all_dev_obj_dict = self.getObj_ByAllDev()
		
		self.Caps = self.getCap()
		
		self.PF = list(farm + ':PF01' for farm in self.farmKeys_list)
		
		#self.structure = self.mongo.getAllDevsByProject(self.project)
		
		self.lines = self.getLineByPeriod()
		
		
		#-------------------------------------------------------
		#---all----dev---period---farm---company----project---
		#--sum---
		self.sum_all = ['CMPT_Production_Day','CMPT_ProductionIntegal_Day','CMPT_ProductionTheory_Day', 'CMPT_RunCnt_Day', 'CMPT_ReadyCnt_Day', 'CMPT_StopCnt_Day', 'CMPT_FaultCnt_Day', 'CMPT_ServiceCnt_Day', 'CMPT_UnConnectCnt_Day', 'CMPT_LimPwrCnt_Day', 'CMPT_RepairCnt_Day', 'CMPT_UnFaultHours_Day', 'CMPT_UserForGenerationHours_Day', 'CMPT_FullHours_Day', 'CMPT_GenrationHours_Day', 'CMPT_OnGridHours_Day', 'CMPT_RunHours_Day', 'CMPT_ReadyHours_Day', 'CMPT_StopHours_Day', 'CMPT_FaultHours_Day', 'CMPT_ServiceHours_Day', 'CMPT_UnConnectHours_Day', 'CMPT_LimPwrHours_Day','CMPT_RepairHours_Day','CMPT_DispatchStopHours_Day','CMPT_DispatchStopLost_Day','CMPT_DispatchStopLostPower_Day','CMPT_DispatchStopCnt_Day','CMPT_In_StopHours_Day','CMPT_In_StopLost_Day','CMPT_In_StopCnt_Day','CMPT_Out_StopHours_Day','CMPT_Out_StopLost_Day','CMPT_Out_StopCnt_Day','CMPT_UnConnectLost_Day','CMPT_PlanStopLost_Day','CMPT_FaultStopLost_Day','CMPT_ProductionLost_Day','CMPT_LostPower_Day','CMPT_WindSpeedValid_Day','CMPT_TotRadiation_Day', 'CMPT_LimPwrRunLost_Day', 'CMPT_LimPwrShutLost_Day']
		
		#--avg---
		self.avg_all = ['CMPT_WindSpeed_10_Avg_Day', 'CMPT_MTBF_Day', 'CMPT_CAH_Day', 'CMPT_WindSpeed_Avg_Day', 'CMPT_ActPower_Avg_Day', 'CMPT_LimPwrRate_Day','CMPT_FaultCnt_Avg_Day', 'CMPT_FaultHours_Avg_Day', 'CMPT_FaultStopLost_Avg_Day','CMPT_WindEnerge_Day', 'CMPT_Radiation_Avg_Day', 'CMPT_RunRatio_Day', 'CMPT_UseRatio_Day', 'CMPT_UserForGenerRatio_Day', 'CMPT_ExposeRatio_Day', 'CMPT_UnConnectRatio_Day','CMPT_OutPowerRatio_Day','CMPT_Availability_Day','CMPT_Temp_Avg_Day', 'CMPT_AirDevsity_Avg_Day']
		
		#self.avg_all_rate = []
		
		#--max---
		self.max_all = ['CMPT_WindSpeed_Max_Day', 'CMPT_ActPower_Max_Day', 'CMPT_Radiation_Max_Day', 'CMPT_LimPwrMax_Day', 'CMPT_WindSpeed_10_Max_Lim_Day', 'CMPT_WindSpeed_10_Max_Day']
		
		#--min---
		self.min_all = ['CMPT_WindSpeed_Min_Day', 'CMPT_ActPower_Min_Day']
		
		#-----farm-------------------------------------------
		#--sum--
		self.sum_farm = ['CMPT_66KV_Day', 'CMPT_OutPowerTime_100_Day', 'CMPT_OutPowerTime_75_Day', 'CMPT_OutPowerTime_50_Day', 'CMPT_OutPowerTime_25_Day', 'CMPT_OutPowerTime_0_Day']
		
		#--avg--
		self.avg_farm = [ 'CMPT_AccuracyRate_CDQ_Day', 'CMPT_AccuracyRate_DQ_Day']
		
		#self.avg_farm_rate = []
		
		#self.sum_group = ['CMPT_OnGridProduction_Day', 'CMPT_PurchaseProduction_Day', 'CMPT_HouseProduction_Day','CMPT_CompreHouseProduction_Day', 'CMPT_HouseLost_Day']
		
		#self.avg_group_rate = ['CMPT_HouseRate_Day', 'CMPT_RateOfHousePower_Day', 'CMPT_HouseLostRate_Day']
		#--farm---company----project-------------------------------
		#self.sum_group = ['CMPT_CompleteOfPlan_Day', 'CMPT_GeneratingEfficiency_Day']
		
		#--dev--period--farm------------
		#--avg--
		self.avg_devs = ['CMPT_ActPower_Avg_Day', 'CMPT_GenerateRate_Day']
		#self.avg_devs_rate = []
		
		print '-------ini end-------'
		
	def setData_Day(self, date_now):
		global off_logger, logger
		try:
			
			print 'day start ---------'
			print date_now
			timestamp = (date_now-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
			date_1 = date_now.strftime('%Y/%m/%d')
			date_2 = (date_now+datetime.timedelta(days=1)).strftime('%Y/%m/%d')
			date_3 = (date_now+datetime.timedelta(days=2)).strftime('%Y/%m/%d')
			date_4 = (date_now+datetime.timedelta(days=3)).strftime('%Y/%m/%d')
			date_5 = (date_now+datetime.timedelta(days=4)).strftime('%Y/%m/%d')
			
			
			starttime = (date_now-datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
			
			endtime = date_now.strftime('%Y-%m-%d 00:00:00')
			
			day_date = timestamp
			
			timestamp_month, list_timestamps_month = self.getDate_ByMonth(date_now, 0)
			timestamp_year, list_timestamps_year = self.getDate_ByYear(date_now, 0)
			
			#---ex:max,min,avg---
			ex_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.all_keyList, ['CMPT_WindSpeed_Avg', 'CMPT_ActPower', 'CMPT_ActPowerTheory'], starttime, endtime, '1', '')
			#ex_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.devKeys_list, ['CMPT_ActPower', 'CMPT_ActPowerTheory'], starttime, endtime, '1', '')
			off_logger.info(str(datetime.datetime.now())+':ex_dict')
			print 'ex_dict'
			
			
			
			actPower_timeLen = self.cmpt_actPower_timeLen(ex_dict)

			airDen_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.farmKeys_list, ['CMPT_AirDensity'], starttime, endtime, '600', '')
			
			#--rose---
			rose_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.wt_keyList, ['WTUR_WindEnerge', 'CMPT_WindSpeed_Avg', ], starttime, endtime, '600', '')
			windDir_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.wt_keyList, ['CMPT_WindDir'], starttime, endtime, '600', '')
			
			off_logger.info(str(datetime.datetime.now())+':windDir_dict')
			
			#-CMPT_Rose------------
			rose = self.CMPT_Rose(ex_dict, windDir_dict)
			print 'CMPT_Rose'
			off_logger.info(str(datetime.datetime.now())+':CMPT_Rose')
			
			#-CMPT_WindEnerge_Rose----------
			windEnerge_Rose = self.getWindEnergy(ex_dict, rose_dict, airDen_dict, windDir_dict)
			print 'CMPT_WindEnerge_Rose'
			off_logger.info(str(datetime.datetime.now())+':CMPT_WindEnerge_Rose')
			
			#---准确率-----
			
			hourCnt_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.devKeys_list, ['CMPT_StandardStatus'], starttime, endtime, '1', '')
			
			print 'hourCnt_dict'
			off_logger.info(str(datetime.datetime.now())+':hourCnt_dict')
			
			runTimeLen, readyTimeLen, stopTimeLen, faultTimeLen, serviceTimeLen, unConnectTimeLen, limPwrTimeLen, repairTimeLen = {}, {}, {}, {}, {}, {}, {}, {}
			
			runTimeLen_wt, runTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 1)
			print 'runTimeLen_wt'
			off_logger.info(str(datetime.datetime.now())+':runTimeLen_wt')
			readyTimeLen_wt, readyTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 2)
			stopTimeLen_wt, stopTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 3)
			
			faultTimeLen_wt, faultTimeLen_pv = self.getFaultTimeLen(hourCnt_dict, 4, 5, 3600)
			
			#print faultTimeLen_wt, faultTimeLen_pv
			
			print 'faultTimeLen_wt, faultTimeLen_pv'
			off_logger.info(str(datetime.datetime.now())+':faultTimeLen_wt, faultTimeLen_pv')
			
			serviceTimeLen_wt, serviceTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 5)
			
			unConnectTimeLen_wt, unConnectTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 6)
			print 'unConnectTimeLen_wt'
			
			limPwrTimeLen_wt, limPwrTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 7)
			print 'limPwrTimeLen_wt'
			repairTimeLen_wt, repairTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 8)
			print 'repairTimeLen_wt'
			
			off_logger.info(str(datetime.datetime.now())+':repairTimeLen_wt')
			
			runTimeLen = dict(runTimeLen_wt.items()+runTimeLen_pv.items())
			readyTimeLen = dict(readyTimeLen_wt.items()+readyTimeLen_pv.items())
			stopTimeLen = dict(stopTimeLen_wt.items()+stopTimeLen_pv.items())
			
			faultTimeLen = dict(faultTimeLen_wt.items()+faultTimeLen_pv.items())
			
			serviceTimeLen = dict(serviceTimeLen_wt.items()+serviceTimeLen_pv.items())
			unConnectTimeLen = dict(unConnectTimeLen_wt.items()+unConnectTimeLen_pv.items())
			
			limPwrTimeLen = dict(limPwrTimeLen_wt.items()+limPwrTimeLen_pv.items())
			repairTimeLen = dict(repairTimeLen_wt.items()+repairTimeLen_pv.items())
			print 'timelen'
			off_logger.info(str(datetime.datetime.now())+':timelen')
			
			runHours = self.getStatusHours(runTimeLen_wt, runTimeLen_pv)
			readyHours = self.getStatusHours(readyTimeLen_wt, readyTimeLen_pv)
			stopHours = self.getStatusHours(stopTimeLen_wt, stopTimeLen_pv)
			
			faultHours = self.getStatusHours(faultTimeLen_wt, faultTimeLen_pv)
			
			serviceHours = self.getStatusHours(serviceTimeLen_wt, serviceTimeLen_pv)
			unConnectHours = self.getStatusHours(unConnectTimeLen_wt, unConnectTimeLen_pv)
			limPwrHours = self.getStatusHours(limPwrTimeLen_wt, limPwrTimeLen_pv)
			repairHours = self.getStatusHours(repairTimeLen_wt, repairTimeLen_pv)
			print 'hours'
			off_logger.info(str(datetime.datetime.now())+':hours')
			
			#-CMPT_FaultCnt---------
			faultCnt = self.CMPT_StopCnt(faultTimeLen_wt, faultTimeLen_pv)
			print 'CMPT_FaultCnt'
			off_logger.info(str(datetime.datetime.now())+':CMPT_FaultCnt')
			
			#-CMPT_StopCnt--------
			stopCnt = self.CMPT_StopCnt(stopTimeLen_wt, stopTimeLen_pv)
			print 'CMPT_StopCnt'
			off_logger.info(str(datetime.datetime.now())+':CMPT_StopCnt')
			
			#-CMPT_RunCnt---------
			runCnt = self.CMPT_StopCnt(runTimeLen_wt, runTimeLen_pv)
			print 'CMPT_RunCnt'
			
			#-CMPT_ReadyCnt-------
			readyCnt = self.CMPT_StopCnt(readyTimeLen_wt, readyTimeLen_pv)
			print 'CMPT_ReadyCnt'
			
			#-CMPT_ServiceCnt---------
			serviceCnt = self.CMPT_StopCnt(serviceTimeLen_wt, serviceTimeLen_pv)
			print 'CMPT_ServiceCnt'
			
			#-CMPT_UnConnectCnt---------
			unConnectCnt = self.CMPT_StopCnt(unConnectTimeLen_wt, unConnectTimeLen_pv)
			print 'CMPT_UnConnectCnt'
			
			#-CMPT_LimPwrCnt---------
			limPwrCnt = self.CMPT_StopCnt(limPwrTimeLen_wt, limPwrTimeLen_pv)
			print 'CMPT_LimPwrCnt'
			
			#-CMPT_RepairCnt---------
			repairCnt = self.CMPT_StopCnt(repairTimeLen_wt, repairTimeLen_pv)
			print 'CMPT_RepairCnt'
			
			#-CMPT_FaultCnt_Avg--
			faultCnt_avg = self.CMPT_FaultCnt_Avg(faultCnt)
			print 'CMPT_FaultCnt_Avg'
			
			#-CMPT_LimPwrRunLost---
			limPwrRun = self.CMPT_Lost(repairTimeLen_wt, repairTimeLen_pv, ex_dict)
			print 'CMPT_LimPwrRunLost'
			
			#-CMPT_LimPwrShutLost---
			limPwrShut = self.CMPT_Lost(limPwrTimeLen_wt, limPwrTimeLen_pv, ex_dict)
			print 'CMPT_LimPwrShutLost'
			
			#-CMPT_DispatchStopLostPower---
			dispatchStopLostPower = self.CMPT_DispatchStopLostPower(limPwrRun, limPwrShut)
			print 'CMPT_DispatchStopLostPower'
			
			off_logger.info(str(datetime.datetime.now())+':CMPT_DispatchStopLostPower')
			
			#-CMPT_UnConnectLost-----
			unConnectLost = self.CMPT_Lost(unConnectTimeLen_wt, unConnectTimeLen_pv, ex_dict)
			print 'CMPT_UnConnectLost'
			
			#-CMPT_FaultStopLost---
			faultStopLost = self.CMPT_Lost(faultTimeLen_wt, faultTimeLen_pv, ex_dict)
			print 'CMPT_FaultStopLost'
			
			#-CMPT_FaultStopLost_Avg--
			faultStopLost_avg = self.CMPT_FaultStopLost_Avg(faultStopLost)
			print 'CMPT_FaultStopLost_Avg'
			
			#-CMPT_FaultStopLostInfo--
			faultStopLostInfo = self.getStatusLostInfo(faultTimeLen_wt, faultTimeLen_pv, ex_dict)
			print 'CMPT_FaultStopLostInfo'
			#print faultStopLostInfo
			#-CMPT_ProductionLost--
			productionLost = self.CMPT_ProductionLost(dispatchStopLostPower, unConnectLost, faultStopLost)
			print 'CMPT_ProductionLost'
			off_logger.info(str(datetime.datetime.now())+':CMPT_ProductionLost')
			
			#-CMPT_Availability_dev----
			availability_dev = self.getAvailability_dev(faultHours)
			
			devs = {}
			devs_taglist = []
			devs_datalist = []
			
			for key in self.devKeys_list:
				
				devs[key] = {}
				
				devs[key]['CMPT_RunHours_Day'] = runHours[key] if runHours.has_key(key) else 0.0
				devs[key]['CMPT_ReadyHours_Day'] = readyHours[key] if readyHours.has_key(key) else 0.0
				devs[key]['CMPT_StopHours_Day'] = stopHours[key] if stopHours.has_key(key) else 0.0
				
				devs[key]['CMPT_FaultHours_Day'] = faultHours[key] if faultHours.has_key(key) else 0.0
				devs[key]['CMPT_FaultStopLostInfo_Day'] = str(faultStopLostInfo[key]) if faultStopLostInfo.has_key(key) else ''
				
				devs[key]['CMPT_ServiceHours_Day'] = serviceHours[key] if serviceHours.has_key(key) else 0.0
				devs[key]['CMPT_UnConnectHours_Day'] = unConnectHours[key] if unConnectHours.has_key(key) else 0.0
				devs[key]['CMPT_LimPwrHours_Day'] = limPwrHours[key] if limPwrHours.has_key(key) else 0.0
				devs[key]['CMPT_RepairHours_Day'] = repairHours[key] if repairHours.has_key(key) else 0.0
				
				devs[key]['CMPT_RunHoursTimeLen_Day'] = str(runTimeLen[key]) if runTimeLen.has_key(key) else ''
				devs[key]['CMPT_ReadyHoursTimeLen_Day'] = str(readyTimeLen[key]) if readyTimeLen.has_key(key) else ''
				devs[key]['CMPT_StopHoursTimeLen_Day'] = str(stopTimeLen[key]) if stopTimeLen.has_key(key) else ''
				
				devs[key]['CMPT_FaultHoursTimeLen_Day'] = str(faultTimeLen[key]) if faultTimeLen.has_key(key) else ''
				
				devs[key]['CMPT_ServiceHoursTimeLen_Day'] = str(serviceTimeLen[key]) if serviceTimeLen.has_key(key) else ''
				devs[key]['CMPT_UnConnectHoursTimeLen_Day'] = str(unConnectTimeLen[key]) if unConnectTimeLen.has_key(key) else ''
				devs[key]['CMPT_LimPwrHoursTimeLen_Day'] = str(limPwrTimeLen[key]) if limPwrTimeLen.has_key(key) else ''
				devs[key]['CMPT_RepairHoursTimeLen_Day'] = str(repairTimeLen[key]) if repairTimeLen.has_key(key) else ''
				
				devs[key]['CMPT_Availability_Day'] = availability_dev[key] if availability_dev.has_key(key) else 100
				
				devs_taglist.append({'object':key, 'date':timestamp})
				devs_datalist.append(devs[key])
			
			self.mongo.setData(self.project, devs_taglist, devs_datalist)
			print 'hours'
			off_logger.info(str(datetime.datetime.now())+':hours')
			
			
			
			#--production-----------------
			production_start_tot = self.kairos.readActiveDataByDevsTags(self.production_dict['CMPT_TotProduction'], ['CMPT_TotProduction'], starttime)
			print 'production_start'
			
			production_end_tot = self.kairos.readActiveDataByDevsTags(self.production_dict['CMPT_TotProduction'], ['CMPT_TotProduction'], endtime)
			print 'production_end'
			
			temp = datetime.datetime.strptime(endtime, '%Y-%m-%d %H:%M:%S')
			
			start = temp - datetime.timedelta(minutes = 60)
			
			end = temp + datetime.timedelta(minutes = 60)
			
			production_day = self.kairos.readAchiveDataByDevsTimePer(self.production_dict['WTUR_Production_Day'], 'WTUR_Production_Day',  str(start), str(end), '1', '')
			
			#-CMPT_Production-------------------
			production_dict_day = self.CMPT_Production(production_start_tot, production_end_tot, production_day, starttime, endtime)
			print 'production_dict_day'
			off_logger.info(str(datetime.datetime.now())+':production_dict_day')
			#-CMPT_GenerateRate---------------------
			
			generateRate = self.CMPT_GenerateRate(production_dict_day, windEnerge, totRadition)
			print 'CMPT_GenerateRate'
			
			
			#--period--farm------------
			
			keyList = []
			
			keyList.extend(self.wt_farmKey_list)
			
			keyList.extend(self.pv_farmKey_list)
			
			keyList.extend(self.wt_periodKey_list)
			
			keyList.extend(self.pv_periodKey_list)
			
			agc_dict = self.kairos.readAchiveDataByDevsTimePer(keyList, 'CMPT_AGCPower', starttime, endtime, '1', '')
			print 'agc'
			
			limPwrLostTimeLen = self.getLimPwrLostTime(agc_dict, ex_dict, keyList)
			
			limPwrPowerMax = self.getLimPwrPowerMax(limPwrLostTimeLen, keyList, ex_dict)
			print 'limPwrPowerMax'
			
			limPwrLostInfo = self.limPwrLostFarmPeriod(limPwrLostTimeLen, hourCnt_dict, ex_dict)
			print 'limPwrLostInfo'
			off_logger.info(str(datetime.datetime.now())+':limPwrLostInfo')
			
			#--上网电量--购网电量--场用电量--场出力-
			#onGridDict_start = self.getTagsValueByStatisticsTags('CMPT_OnGridProduction', self.farmKeys_list, '', starttime, '')
			#onGridDict_end = self.getTagsValueByStatisticsTags('CMPT_OnGridProduction', self.farmKeys_list, '', endtime, '')
			#-CMPT_OnGridProduction---
			#onGrid = self.CMPT_OnGridProduction(onGridDict_start, onGridDict_end, production_dict_day)
			
			print 'CMPT_OnGridProduction'
			#purchaseDict_start = self.getTagsValueByStatisticsTags('CMPT_PurchaseProduction', self.farmKeys_list, '', starttime, '')
			#purchaseDict_end = self.getTagsValueByStatisticsTags('CMPT_PurchaseProduction', self.farmKeys_list, '', endtime, '')
			#-CMPT_PurchaseProduction---
			#purProduction = self.CMPT_OtherProduction(purchaseDict_start, purchaseDict_end)
			
			print 'CMPT_PurchaseProduction'
			#houseDict_start = self.getTagsValueByStatisticsTags('CMPT_HouseProduction', self.farmKeys_list, '', starttime, '')
			#houseDict_end = self.getTagsValueByStatisticsTags('CMPT_HouseProduction', self.farmKeys_list, '', endtime, '')
			#-CMPT_HouseProduction---
			#houseProduction = self.CMPT_OtherProduction(houseDict_start, houseDict_end)
			print 'CMPT_HouseProduction'
			
			#-CMPT_RateOfHousePower--
			#housePowerRate = self.CMPT_RateOfHousePower(production_dict_day, onGrid, purProduction)
			print 'CMPT_RateOfHousePower'
			
			#-CMPT_CompreHouseProduction--
			#compreHouseProduction = self.CMPT_CompreHouseProduction(production_dict_day, onGrid, purProduction)
			print 'CMPT_CompreHouseProduction'
			
			off_logger.info(str(datetime.datetime.now())+':CMPT_CompreHouseProduction')
			
			#CMPT_HouseLost----
			#housLost = self.CMPT_HouseLost(production_dict_day, onGrid, purProduction, houseProduction)
			
			#CMPT_HouseLostRate--
			#houseLostRate = self.CMPT_HouseLostRate(production_dict_day, onGrid, purProduction, houseProduction)
			
			#-CMPT_HouseRate---
			#houseRate = self.CMPT_HouseRate(houseProduction, production_dict_day)
			print 'CMPT_HouseRate'
			
			#--CMPT_UseHours-------------------
			useHours = self.CMPT_UseHours(production_dict_day)
			print 'CMPT_UseHours'
			
			#-CMPT_UseRatio--------------
			useRatio = self.CMPT_UseRatio(useHours)
			print 'CMPT_UseRatio'
			
			off_logger.info(str(datetime.datetime.now())+':CMPT_UseRatio')
			
			group = {}
			group_taglist = []
			group_datalist = []
			
			keyList = []
			
			keyList.extend(self.wt_farmKey_list)
			
			keyList.extend(self.pv_farmKey_list)
			
			keyList.extend(self.wt_periodKey_list)
			
			keyList.extend(self.pv_periodKey_list)
			
			for key in keyList:
				
				group[key] = {}
				
				group[key]['CMPT_LimPwrLostInfo_Day'] = str(limPwrLostInfo[key]) if limPwrLostInfo.has_key(key) else ''
				group[key]['CMPT_LimPwrLostTimeLen_Day'] = str(limPwrLostTimeLen[key]) if limPwrLostTimeLen.has_key(key) else ''
				group[key]['CMPT_LimPwrMax_Day'] = limPwrPowerMax[key] if limPwrPowerMax.has_key(key) else 0.0
				
				group_taglist.append({'object':key, 'date':timestamp})
				group_datalist.append(group[key])
			
			self.mongo.setData(self.project, group_taglist, group_datalist)
			
			#---测风塔-----
			
			windMeasur = self.kairos_new.readActiveDataByDevsTagsTimePer(self.windMeasur_list, ['WindMeasur_WindSpeed_10m','WindMeasur_Tmp'], starttime, endtime, '1', '')
			print 'windSpeed_10m'
			
			windSpeed_10_Max = self.cmpt_windSpeed_10_Max(windMeasur, 'WindMeasur_WindSpeed_10m')
			
			
			
			#---hours---
			hours_end = self.mongo.getStatisticsByKeyList_DateList(self.project, [day_date], self.devKeys_list, ['CMPT_RunHours_Day','CMPT_ReadyHours_Day','CMPT_StopHours_Day','CMPT_FaultHours_Day','CMPT_ServiceHours_Day','CMPT_UnConnectHours_Day','CMPT_LimPwrHours_Day','CMPT_RepairHours_Day'])
			
			
			
			
			#-CMPT_OutPowerRatio--
			outPowerRatio = self.CMPT_OutPowerRatio(useHours, hours_end, day_date)
			print 'CMPT_OutPowerRatio'
			
			#--production---
			pro_start = self.kairos.readActiveDataByDevsTags(self.all_keyList, ['CMPT_TotProductionIntegal','CMPT_TotProductionTheory'], starttime)
			print 'production_start'
			
			pro_end = self.kairos.readActiveDataByDevsTags(self.all_keyList, ['CMPT_TotProductionIntegal','CMPT_TotProductionTheory'], endtime)
			print 'production_end'
			
			#-CMPT_ProductionIntegal-------------
			productionIntegal_dict_day = self.CMPT_ProductionIntegal(pro_start, pro_end)
			print 'productionIntegal_dict_day'
			
			#-CMPT_ProductionTheory---------------
			productionTheory_dict_day = self.CMPT_ProductionTheory(pro_start, pro_end)
			print 'productionTheory_dict_day'
			
			#-CMPT_LimPwrRate-----
			limPwrRate = self.CMPT_LimPwrRate(dispatchStopLostPower, productionTheory_dict_day)
			print 'CMPT_LimPwrRate'
			
			#-CMPT_UnFaultHours------
			unFaultHours = self.CMPT_UnFaultHours(hours_end, day_date)
			print 'CMPT_UnFaultHours'
			
			#-CMPT_ExposeRatio------
			exportRatio = self.CMPT_ExposeRatio(hours_end, unFaultHours, day_date)
			print 'CMPT_ExposeRatio'
			
			#-CMPT_RunRatio----------------
			runRatio = self.CMPT_RunRatio(hours_end, day_date)
			print 'CMPT_RunRatio' 
			
			#-CMPT_UnConnectRatio-----
			unConnectRatio = self.CMPT_UnConnectRatio(hours_end, day_date)
			print 'CMPT_UnConnectRatio'
			
			#-CMPT_FaultHours_Avg--
			faultHourse_avg = self.CMPT_FaultHours_Avg(hours_end, day_date)
			print 'CMPT_FaultHours_Avg'
			
			#-CMPT_MTBF-----
			mtbf = self.CMPT_MTBF(hours_end, faultCnt, day_date)
			print 'CMPT_MTBF'
			
			#-CMPT_UserForGenerationHours--
			userForGenerationHours = self.CMPT_UserForGenerationHours(hours_end, day_date)
			print 'CMPT_UserForGenerationHours'
			
			#-CMPT_PowerLoadRate----
			powerLoadRate = self.getPowerLoadRate(useHours, userForGenerationHours)
			print 'CMPT_PowerLoadRate'
			
			#-CMPT_CAH-------
			cah =  self.CMPT_CAH(userForGenerationHours, faultCnt)
			print 'CMPT_CAH'
			
			#-CMPT_UserForGenerRatio---
			userForGenerRatio = self.CMPT_UserForGenerRatio(userForGenerationHours)
			print 'CMPT_UserForGenerRatio'
			off_logger.info(str(datetime.datetime.now())+':CMPT_UserForGenerRatio')
			
			#-----all----------
			all = {}
			all_taglist = []
			all_datalist = []
			
			for key in self.all_keyList:
				
				all[key] = {}
				
				all[key]['CMPT_Production_Day'] = production_dict_day[key] if production_dict_day.has_key(key) else 0.0
				
				all[key]['CMPT_ProductionTheory_Day'] = productionTheory_dict_day[key] if productionTheory_dict_day.has_key(key) else 0.0
				all[key]['CMPT_RunCnt_Day'] = runCnt[key] if runCnt.has_key(key) else 0.0
				all[key]['CMPT_ReadyCnt_Day'] = readyCnt[key] if readyCnt.has_key(key) else 0.0
				all[key]['CMPT_StopCnt_Day'] = stopCnt[key] if stopCnt.has_key(key) else 0.0
				
				all[key]['CMPT_FaultCnt_Day'] = faultCnt[key] if faultCnt.has_key(key) else 0.0
				
				all[key]['CMPT_ServiceCnt_Day'] = serviceCnt[key] if serviceCnt.has_key(key) else 0.0
				all[key]['CMPT_UnConnectCnt_Day'] = unConnectCnt[key] if unConnectCnt.has_key(key) else 0.0
				all[key]['CMPT_UnConnectRatio_Day'] = unConnectRatio[key] if unConnectRatio.has_key(key) else 0.0
				all[key]['CMPT_LimPwrCnt_Day'] = limPwrCnt[key] if limPwrCnt.has_key(key) else 0.0
				all[key]['CMPT_RepairCnt_Day'] = repairCnt[key] if repairCnt.has_key(key) else 0.0
				
				all[key]['CMPT_UnFaultHours_Day'] = unFaultHours[key] if unFaultHours.has_key(key) else 0.0
				
				all[key]['CMPT_RunRatio_Day'] = runRatio[key] if runRatio.has_key(key) else 0.0
				all[key]['CMPT_UseHours_Day'] = useHours[key] if useHours.has_key(key) else 0.0
				all[key]['CMPT_UseRatio_Day'] = useRatio[key] if useRatio.has_key(key) else 0.0
				all[key]['CMPT_OutPowerRatio_Day'] = outPowerRatio[key] if outPowerRatio.has_key(key) else 0.0
				all[key]['CMPT_UserForGenerationHours_Day'] = userForGenerationHours[key] if userForGenerationHours.has_key(key) else 0.0
				all[key]['CMPT_UserForGenerRatio_Day'] = userForGenerRatio[key] if userForGenerRatio.has_key(key) else 0.0
				all[key]['CMPT_ExposeRatio_Day'] = exportRatio[key] if exportRatio.has_key(key) else 0.0
				all[key]['CMPT_FullHours_Day'] = fullHours[key] if fullHours.has_key(key) else 0.0
				all[key]['CMPT_GenrationHours_Day'] = generHours[key] if generHours.has_key(key) else 0.0
				all[key]['CMPT_OnGridHours_Day'] = onGridHours[key] if onGridHours.has_key(key) else 0.0
				all[key]['CMPT_MTBF_Day'] = mtbf[key] if mtbf.has_key(key) else 0.0
				all[key]['CMPT_CAH_Day'] = cah[key] if cah.has_key(key) else 0.0
				all[key]['CMPT_ProductionIntegal_Day'] = productionIntegal_dict_day[key] if productionIntegal_dict_day.has_key(key) else 0.0
				all[key]['CMPT_DispatchStopLostPower_Day'] = dispatchStopLostPower[key] if dispatchStopLostPower.has_key(key) else 0.0
				all[key]['CMPT_LimPwrRunLost_Day'] = limPwrRun[key] if limPwrRun.has_key(key) else 0.0
				all[key]['CMPT_LimPwrShutLost_Day'] = limPwrShut[key] if limPwrShut.has_key(key) else 0.0
				all[key]['CMPT_UnConnectLost_Day'] = unConnectLost[key] if unConnectLost.has_key(key) else 0.0
				
				all[key]['CMPT_FaultStopLost_Day'] = faultStopLost[key] if faultStopLost.has_key(key) else 0.0
				all[key]['CMPT_ProductionLost_Day'] = productionLost[key] if productionLost.has_key(key) else 0.0
				
				all[key]['CMPT_WindSpeed_Max_Day'] = windSpeed_max_dict_day[key] if windSpeed_max_dict_day.has_key(key) else 0.0
				
				all[key]['CMPT_WindSpeed_Min_Day'] = windSpeed_min_dict_day[key] if windSpeed_min_dict_day.has_key(key) else 0.0
				
				all[key]['CMPT_WindSpeed_Avg_Day'] = windSpeed_avg_dict_day[key] if windSpeed_avg_dict_day.has_key(key) else 0.0
				all[key]['CMPT_ActPower_Max_Day'] = actPower_max_dict_day[key] if actPower_max_dict_day.has_key(key) else 0.0
				all[key]['CMPT_ActPower_Min_Day'] = actPower_min_dict_day[key] if actPower_min_dict_day.has_key(key) else 0.0
				all[key]['CMPT_ActPower_Avg_Day'] = actPower_avg_dict_day[key] if actPower_avg_dict_day.has_key(key) else 0.0
				all[key]['CMPT_WindSpeedValid_Day'] = windSpeedValid[key] if windSpeedValid.has_key(key) else 0.0
				#all[key]['CMPT_TotRadiation_Day'] = totRadition[key] if totRadition.has_key(key) else 0.0
				#all[key]['CMPT_Radiation_Max_Day'] = radition_max[key] if radition_max.has_key(key) else 0.0
				#all[key]['CMPT_Radiation_Avg_Day'] = radition_avg[key] if radition_avg.has_key(key) else 0.0
				all[key]['CMPT_WindEnerge_Day'] = windEnerge[key] if windEnerge.has_key(key) else 0.0
				all[key]['CMPT_LimPwrRate_Day'] = limPwrRate[key] if limPwrRate.has_key(key) else 0.0
				
				all[key]['CMPT_FaultCnt_Avg_Day'] = faultCnt_avg[key] if faultCnt_avg.has_key(key) else 0.0
				all[key]['CMPT_FaultHours_Avg_Day'] = faultHourse_avg[key] if faultHourse_avg.has_key(key) else 0.0
				all[key]['CMPT_FaultStopLost_Avg_Day'] = faultStopLost_avg[key] if faultStopLost_avg.has_key(key) else 0.0
				
				
				all[key]['CMPT_PlanStopLost_Day'] = 0.0
				all[key]['CMPT_LostPower_Day'] = 0.0
				all[key]['CMPT_PlanStopLost_Day'] = 0.0
				
				all[key]['CMPT_PowerLoadRate_Day'] = powerLoadRate[key] if powerLoadRate.has_key(key) else 0.0
				
				
				all_taglist.append({'object':key, 'date':timestamp})
				all_datalist.append(all[key])
			
			self.mongo.setData(self.project, all_taglist, all_datalist)
			
			accRate = self.kairos_new.readActiveDataByDevsTagsTimePer(self.farmKeys_list, ['CMPT_QualifiedRate'], starttime, endtime, '900', '')
			acc = self.kairos_new.readActiveDataByDevsTagsTimePer(self.PF, ['WTUR_PowerForecast_CDQ'], starttime, endtime, '900', '')
			
			print 'accRate'
			off_logger.info(str(datetime.datetime.now())+':accRate')
			
			qualifiedRate = self.getQualifiedRate(accRate)
			cdq = self.CMPT_AccuracyRate_CDQ(ex_dict, acc)
			print 'CMPT_QualifiedRate_Day'
			off_logger.info(str(datetime.datetime.now())+':CMPT_QualifiedRate_Day')    
			
			#---farm--------------
			farm = {}
			farm_taglist = []
			farm_datalist = []
			
			for key in self.farmKeys_list:
				
				farm[key] = {}
				
				farm[key]['CMPT_AccuracyRate_CDQ_Day'] = cdq[key] if cdq.has_key(key) else 0.0
				farm[key]['CMPT_QualifiedRate_Day'] = qualifiedRate[key] if qualifiedRate.has_key(key) else 0.0
				
				farm[key]['CMPT_Rose_Day'] = rose[key] if rose.has_key(key) else 0.0
				farm[key]['CMPT_WindEnerge_Rose_Day'] = windEnerge_Rose[key] if windEnerge_Rose.has_key(key) else 0.0
				
				farm_taglist.append({'object':key, 'date':timestamp})
				farm_datalist.append(farm[key])
			
			self.mongo.setData(self.project, farm_taglist, farm_datalist)
			
			#--CMPT_ProductionForecast-
			productionForeCast1, productionForeCast2, productionForeCast3, productionForeCast4, productionForeCast5 = self.CMPT_ProductionForecast(starttime, endtime)
			off_logger.info(str(datetime.datetime.now())+':CMPT_ProductionForecast')
			
			for key in self.farmKeys_list:
				
				self.mongo.update(key, date_1, {'CMPT_ProductionForecast_Day' : round(productionForeCast1[key],4)})
				self.mongo.update(key, date_2, {'CMPT_ProductionForecast_Day' : round(productionForeCast2[key],4)})
				self.mongo.update(key, date_3, {'CMPT_ProductionForecast_Day' : round(productionForeCast3[key],4)})
				self.mongo.update(key, date_4, {'CMPT_ProductionForecast_Day' : round(productionForeCast4[key],4)})
				self.mongo.update(key, date_5, {'CMPT_ProductionForecast_Day' : round(productionForeCast5[key],4)})
				
				
			#----dev--period---farm-------------
			devs = {}
			devs_taglist = []
			devs_datalist = []
			
			for key in self.keyListWithOutCompPro:
				devs[key] = {}
				
				devs[key]['CMPT_GenerateRate_Day'] = generateRate[key] if generateRate.has_key(key) else 0.0
				devs[key]['CMPT_ActPower_Max_Tm_Day'] = actPower_max_tm[key] if actPower_max_tm.has_key(key) else ''
				devs[key]['CMPT_ActPower_Min_Tm_Day'] = actPower_min_tm[key] if actPower_min_tm.has_key(key) else ''
				
				devs_taglist.append({'object':key, 'date':timestamp})
				devs_datalist.append(devs[key])
				
			self.mongo.setData(self.project, devs_taglist, devs_datalist)
			off_logger.info(str(datetime.datetime.now())+':day, over~')
			
			print 'day'
		
		except Exception, e:
			logger.error(traceback.format_exc())
		
			
	def setData_timper(self, date_now, flag):
		global off_logger, logger
		#==============WEEK===MONTH===YEAR================================================================
		#date_now = datetime.datetime.strptime(date_now,'%Y-%m-%d %H:%M:%S')
		
		try:
			timestamp_week, list_timestamps_week = self.getDate_ByWeek(date_now, 0)
				
			timestamp_month, list_timestamps_month = self.getDate_ByMonth(date_now, 0)
			
			timestamp_year, list_timestamps_year = self.getDate_ByYear(date_now, 0)
			
			all_keysDict_timePer = [timestamp_week, timestamp_month, timestamp_year]
			
			timestamp = (date_now-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
			
			if flag == 0:
				taglist_timePer = []
				datalist_timePer = []
				sum_all_data = self.cmpt_sum_timeper(self.all_keyList, self.sum_all, date_now)
				sum_farm_data = self.cmpt_sum_timeper(self.farmKeys_list, self.sum_farm, date_now)
				#sum_group_data = self.cmpt_sum_timeper(self.group, self.sum_group, date_now)
				for date in all_keysDict_timePer:
					for key in self.all_keyList:
						taglist_timePer.append({'object':key, 'date':date})
						datalist_timePer.append(sum_all_data[date][key])
					for key in self.farmKeys_list:
						taglist_timePer.append({'object':key, 'date':date})
						datalist_timePer.append(sum_farm_data[date][key])
					#for key in self.group:
					#	taglist_timePer.append({'object':key, 'date':date})
					#	datalist_timePer.append(sum_group_data[date][key])
						
				for key in self.all_keyList:
					taglist_timePer.append({'object':key, 'date':timestamp_month})
					datalist_timePer.append(sum_all_data[timestamp_year][key])
				for key in self.farmKeys_list:
					taglist_timePer.append({'object':key, 'date':timestamp_month})
					datalist_timePer.append(sum_farm_data[timestamp_year][key])
				#for key in self.group:
				#	taglist_timePer.append({'object':key, 'date':timestamp_month})
				#	datalist_timePer.append(sum_group_data[timestamp_year][key])
				self.mongo.setData(self.project, taglist_timePer, datalist_timePer)
			
			elif flag == 1:
				yeste_sum_all_data = self.cmpt_sum_timeper_1(self.all_keyList, self.sum_all, date_now)
				yeste_sum_farm_data = self.cmpt_sum_timeper_1(self.farmKeys_list, self.sum_farm, date_now)
				yeste_sum_group_data = self.cmpt_sum_timeper_1(self.group, self.sum_group, date_now)
				yesterday = (date_now-datetime.timedelta(days=1)).strftime('%Y/%m/%d')
				self.setDay(yesterday, timestamp_week, timestamp_month, timestamp_year, yeste_sum_all_data, yeste_sum_farm_data, yeste_sum_group_data)
				self.setWeekMonth(timestamp_week, timestamp_month, timestamp_year, yeste_sum_all_data, yeste_sum_farm_data, yeste_sum_group_data)
				
				mongo_taglist_timePer = []
				mongo_datalist_timePer = []
			
			avg_all_data = self.cmpt_ex_timeper(self.all_keyList, self.avg_all, date_now, 'avg')
				
			max_all_data = self.cmpt_ex_timeper(self.all_keyList, self.max_all, date_now, 'max')
				
			min_all_data = self.cmpt_ex_timeper(self.all_keyList, self.min_all, date_now, 'min')
				
			avg_farm_data = self.cmpt_ex_timeper(self.farmKeys_list, self.avg_farm, date_now, 'avg')
			
			#max_farm_data = self.cmpt_ex_timeper(self.farmKeys_list, self.max_farm, date_now, 'max')
				
			avg_devs_data = self.cmpt_ex_timeper(self.keyListWithOutCompPro, self.avg_devs, date_now, 'avg')
			
			for date in all_keysDict_timePer:
				
				for key in self.all_keyList:
					
					mongo_taglist_timePer.append({'object':key, 'date':date})
					mongo_datalist_timePer.append(avg_all_data[date][key])
					
					mongo_taglist_timePer.append({'object':key, 'date':date})
					mongo_datalist_timePer.append(max_all_data[date][key])
					
					mongo_taglist_timePer.append({'object':key, 'date':date})
					mongo_datalist_timePer.append(min_all_data[date][key])
					
				for key in self.farmKeys_list:
						
					mongo_taglist_timePer.append({'object':key, 'date':date})
					mongo_datalist_timePer.append(avg_farm_data[date][key])
					
					#mongo_taglist_timePer.append({'object':key, 'date':date})
					#mongo_datalist_timePer.append(max_farm_data[date][key])
				
				for key in self.keyListWithOutCompPro:
						
					mongo_taglist_timePer.append({'object':key, 'date':date})
					mongo_datalist_timePer.append(avg_devs_data[date][key])
					
			
			for key in self.all_keyList:
				
				mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
				mongo_datalist_timePer.append(avg_all_data[timestamp_year][key])
				
				mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
				mongo_datalist_timePer.append(max_all_data[timestamp_year][key])
				
				mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
				mongo_datalist_timePer.append(min_all_data[timestamp_year][key])
				
			for key in self.farmKeys_list:
					
				mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
				mongo_datalist_timePer.append(avg_farm_data[timestamp_year][key])
				
				#mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
				#mongo_datalist_timePer.append(max_farm_data[timestamp_year][key])
				
			for key in self.keyListWithOutCompPro:
					
				mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
				mongo_datalist_timePer.append(avg_devs_data[timestamp_year][key])
				
			self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
			
			#CMPT_CompleteOfPlan---
			self.CMPT_CompleteOfPlan(timestamp_month)
			print 'CMPT_CompleteOfPlan'
			
			self.useTimper(all_keysDict_timePer, timestamp_month, timestamp_year)
			
			self.faultStopLostTimper(timestamp_month, list_timestamps_month)
				
			self.limPwrLostTimper(timestamp_month, list_timestamps_month)
				
			self.generatingEfficiency(timestamp_month)
				
			self.maxPowerTm(timestamp_month, list_timestamps_month)
			
			#self.getHouseRateTimper(timestamp_week, timestamp_month, timestamp_year)
		
			#self.getRateOfHousePowerTimper(timestamp_week, timestamp_month, timestamp_year)
			
			self.getAvailabilityTimper(timestamp_week, timestamp_month, timestamp_year, timestamp)
			
			self.getGenerateRateTimper(timestamp_week, timestamp_month, timestamp_year)
			
			off_logger.info(str(datetime.datetime.now())+':timePer, over~')
		except Exception, e:
			logger.error(traceback.format_exc())
			
		print 'timePer'
	def setData_hour(self):
		global off_logger, logger
		try:
			off_logger.info(str(datetime.datetime.now())+':setData_hour, start~')
			date_now = datetime.datetime.now()
			print date_now,'----------------'
			timestamp = ''
			hour = date_now.strftime('%H')
			if hour=='00':
				hour = '24'
				timestamp = (date_now -datetime.timedelta(days=1)).strftime('%Y/%m/%d')
			else:
				timestamp = date_now.strftime('%Y/%m/%d')
			
			starttime = (date_now-datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
			print starttime
			endtime = date_now.strftime('%Y-%m-%d %H:%M:%S')
			print endtime
			production_start_tot = self.kairos.readActiveDataByDevsTags(self.devKeys_list, ['CMPT_TotProduction'], starttime)
			print 'production_start'
			
			production_end_tot = self.kairos.readActiveDataByDevsTags(self.devKeys_list, ['CMPT_TotProduction'], endtime)
			print 'production_end'
			
			#CMPT_Production_1h
			production_1h = self.CMPT_Production_1h(production_start_tot, production_end_tot)
			print 'production_1h'
			#CMPT_DispatchStopLost_1h
			hourCnt_dict = self.kairos.readAchiveDataByDevsTimePer(self.devKeys_list, 'CMPT_StandardStatus', starttime, endtime, '1', '')
			ex_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.all_keyList, ['CMPT_WindSpeed_Avg', 'CMPT_ActPower', 'CMPT_ActPowerTheory'], starttime, endtime, '1', '')
			limPwrTimeLen_wt, limPwrTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 7)
			repairTimeLen_wt, repairTimeLen_pv = self.getStopTimeLen(hourCnt_dict, 8)
			limPwrRun = self.CMPT_Lost(repairTimeLen_wt, repairTimeLen_pv, ex_dict)
			limPwrShut = self.CMPT_Lost(limPwrTimeLen_wt, limPwrTimeLen_pv, ex_dict)
			
			dispatchStopLost_1h = self.CMPT_DispatchStopLostPower(limPwrRun, limPwrShut)
			print 'dispatchStopLost_1h'
			
			#CMPT_WindSpeed_Avg_1h
			windSpeed_avg_1h = self.CMPT_WindSpeed_Avg(ex_dict)
			print 'CMPT_WindSpeed_Avg_1h'
			
			#CMPT_WindSpeed_Max_1h
			windSpeed_max_1h = self.CMPT_WindSpeed_Max(ex_dict)
			print 'CMPT_WindSpeed_Max_1h'
			
			#CMPT_WindSpeed_Min_1h
			windSpeed_min_1h = self.CMPT_WindSpeed_Min(ex_dict)
			print 'CMPT_WindSpeed_Min_1h'
			
			#CMPT_ActPower_Max_1h
			actPower_max_1h = self.CMPT_ActPower_Max(ex_dict)
			print 'CMPT_ActPower_Max_1h'
			
			#CMPT_ActPower_Min_1h
			actPower_min_1h = self.CMPT_ActPower_Min(ex_dict)
			print 'CMPT_ActPower_Min_1h'
			
			#CMPT_ActPower_Max_Tm_1h
			actPower_max_tm_1h = self.CMPT_ActPower_Max_Tm(ex_dict)
			print 'CMPT_ActPower_Max_Tm_1h'
			
			keyList = []
			
			keyList.extend(self.wt_farmKey_list)
			
			keyList.extend(self.pv_farmKey_list)
			
			keyList.extend(self.wt_periodKey_list)
			
			keyList.extend(self.pv_periodKey_list)
			
			group = {}
			group_taglist = []
			group_datalist = []
			
			for key in keyList:
				
				group[key] = {}
				
				group[key]['CMPT_Production_1h'] = {}
				group[key]['CMPT_DispatchStopLost_1h'] = {}
				group[key]['CMPT_WindSpeed_Avg_1h'] = {}
				group[key]['CMPT_WindSpeed_Max_1h'] = {}
				group[key]['CMPT_WindSpeed_Min_1h'] = {}
				group[key]['CMPT_ActPower_Max_1h'] = {}
				group[key]['CMPT_ActPower_Min_1h'] = {}
				group[key]['CMPT_ActPower_Max_Tm_1h'] = {}
				
				group[key]['CMPT_Production_1h'] = {hour:production_1h[key]} if production_1h.has_key(key) else 0.0
				group[key]['CMPT_DispatchStopLost_1h'][hour] = dispatchStopLost_1h[key] if dispatchStopLost_1h.has_key(key) else 0.0
				group[key]['CMPT_WindSpeed_Avg_1h'][hour] = windSpeed_avg_1h[key] if windSpeed_avg_1h.has_key(key) else 0.0
				group[key]['CMPT_WindSpeed_Max_1h'][hour] = windSpeed_max_1h[key] if windSpeed_max_1h.has_key(key) else 0.0
				group[key]['CMPT_WindSpeed_Min_1h'][hour] = windSpeed_min_1h[key] if windSpeed_min_1h.has_key(key) else 0.0
				group[key]['CMPT_ActPower_Max_1h'][hour] = actPower_max_1h[key] if actPower_max_1h.has_key(key) else 0.0
				group[key]['CMPT_ActPower_Min_1h'][hour] = actPower_min_1h[key] if actPower_min_1h.has_key(key) else 0.0
				group[key]['CMPT_ActPower_Max_Tm_1h'][hour] = actPower_max_tm_1h[key] if actPower_max_tm_1h.has_key(key) else ''
				
				group_taglist.append({'object':key, 'date':timestamp})
				group_datalist.append(group[key])
			
			self.mongo.setAvgData(self.project, group_taglist, group_datalist)
			off_logger.info(str(datetime.datetime.now())+':setData_hour, over~')
		except Exception, e:
			logger.error(traceback.format_exc())
			
	def setData_month(self):
		global off_logger, logger
		try:
			off_logger.info(str(datetime.datetime.now())+':setData_month, start~')
			date_now = datetime.datetime.now()
			
			#date_now = datetime.datetime.strptime('2017-10-01 00:00:00','%Y-%m-%d %H:%M:%S')
			
			time.sleep(3600*4)
			
			print date_now
			
			timestamp = ''
			
			year, month = date_now.strftime('%Y/%m').split('/')
			
			endtime = date_now.strftime('%Y-%m-%d %H:%M:%S')
			
			starttime = ''
			
			if month == '01':
				
				timestamp = str(int(year) - 1)+'/12'
				starttime = str(int(year) - 1)+'-12-01 00:00:00'
			else:
			
				if int(month) < 11:
				
					timestamp = year +'/0'+str(int(month) - 1)
					
					starttime = year +'-0'+str(int(month) - 1)+'-01 00:00:00'
					
				else:
					
					timestamp = year +'/'+str(int(month) - 1)
					
					starttime = year +'-'+str(int(month) - 1)+'-01 00:00:00'
			
			ex_dict = self.kairos_new.readActiveDataByDevsTagsTimePer(self.devKeys_list, ['CMPT_WindSpeed_Avg', 'CMPT_ActPower'], starttime, endtime, '600', '')
			
			print 'ex_dict'
			#-CMPT_WindPower------------------
			windPower = self.CMPT_WindPower(ex_dict)
			print 'windPower'
			
			pointDict = self.draw_windPower(ex_dict, self.devKeys_list, ['CMPT_WindSpeed_Avg', 'CMPT_ActPower'])
			
			devs = {}
			dev_taglist = []
			dev_datalist = []
			
			for key in self.devKeys_list:
				
				devs[key] = {}
				
				devs[key]['CMPT_WindPower_Month'] = str(windPower[key]) if windPower.has_key(key) else ''
				
				devs[key]['CMPT_WindPowerPoint_Month'] = str(pointDict[key]) if pointDict.has_key(key) else ''
				
				dev_taglist.append({'object':key, 'date':timestamp})
				dev_datalist.append(devs[key])
			
			self.mongo.setData(self.project, dev_taglist, dev_datalist)
			
			off_logger.info(str(datetime.datetime.now())+':setData_month, over~')
		except Exception, e:
			logger.error(traceback.format_exc())
		print 'month'
		
	def draw_windPower(self, result, keyList, tagList):
		
		dev_dict = {}
		
		for dev in keyList:
			
			dev_dict[dev] = []
			
			if result[dev] <> {}:
				
				for date in result[dev][tagList[0]]:
					
					temp_list = []
					
					for tag in tagList:
						
						temp_list.append(result[dev][tag][date])
						
					if '' not in temp_list:
						
						dev_dict[dev].append(temp_list)
						
		return dev_dict
		
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
		
		
	def productionTimper(self, timestamp_month, timestamp_year):
		
		farm_taglist = []
		
		farm_datalist = []
		
		year, month = timestamp_month.split('/')
		
		mon = int(month)
		
		dateList = [timestamp_month]
		
		project_year = 0.0
		
		while mon > 1:
			
			if mon < 11:
			
				date = year +'/0'+ str(int(mon) - 1) 
				
				mon -= 1
				
				dateList.append(date)
				
			else:
				date = year + str(int(mon) - 1) 
				
				mon -= 1
				
				dateList.append(date)
		
		
		data = self.mongo.getStatisticsByKeyList_DateList(self.project, dateList, self.farmKeys_list, ['CMPT_Production_Month'])
		
		for farm in self.farmKeys_list:
			
			farm_year = 0.0
			
			for date in dateList:
				
				if data[farm][date].has_key('CMPT_Production_Month'):
				
					farm_year += float(data[farm][date]['CMPT_Production_Month']) if data[farm][date]['CMPT_Production_Month'] <> '' else 0.0
				
			farm_taglist.append({'object':farm, 'date':timestamp_year})
			farm_datalist.append({'CMPT_Production_Year':farm_year})
			
			project_year += farm_year
		
		farm_taglist.append({'object':self.project, 'date':timestamp_year})
		farm_datalist.append({'CMPT_Production_Year':project_year})
			
		for date in dateList:
			
			project_month = 0.0
			
			for farm in self.farmKeys_list:
				if 'CMPT_Production_Month' in data[farm][date]:
					project_month += float(data[farm][date]['CMPT_Production_Month']) if data[farm][date]['CMPT_Production_Month'] <> '' else 0.0
				
			farm_taglist.append({'object':self.project, 'date':date})
			farm_datalist.append({'CMPT_Production_Month':project_month})
				
				
		self.mongo.setData(self.project, farm_taglist, farm_datalist)
		
		
	def maxPowerTm(self, timestamp_month, list_timestamps_month):
		try:
			data = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_month, self.all_keyList, ['CMPT_ActPower_Max_Tm_Day', 'CMPT_ActPower_Max_Day'])
			
			key_taglist = []
			
			key_datalist = []
			
			for key in self.all_keyList:
				
				max = 0.0
				
				tm = ''
				
				for date in list_timestamps_month:
					
					if data[key].has_key(date):
						
						if data[key][date].has_key('CMPT_ActPower_Max_Tm_Day') and data[key][date].has_key('CMPT_ActPower_Max_Day'):
						
							max_tm = data[key][date]['CMPT_ActPower_Max_Tm_Day'] if data[key][date]['CMPT_ActPower_Max_Tm_Day'] <> '' else 0.0
							
							max_value = float(data[key][date]['CMPT_ActPower_Max_Day']) if data[key][date]['CMPT_ActPower_Max_Day'] <> '' else 0.0
							
							if max_value > max:
								
								max = max_value
								
								tm = max_tm
					
				key_taglist.append({'object':key, 'date':timestamp_month})
				key_datalist.append({'CMPT_ActPower_Max_Tm_Month':tm})
				
				key_taglist.append({'object':key, 'date':timestamp_month})
				key_datalist.append({'CMPT_ActPower_Max_Month':max})
				
			self.mongo.setData(self.project, key_taglist, key_datalist)
		except:
			raise Exception(traceback.format_exc())
	
	def generatingEfficiency(self, date):
		try:
			farm_taglist = []
			farm_datalist = []
			
			data = self.mongo.getStatisticsByKeyList_DateList(self.project, [date], self.farmKeys_list, ['CMPT_HouseRate_Month', 'CMPT_CompleteOfPlan_Month'])
			
			for farm in self.farmKeys_list:
				
				houseRate = float(data[farm][date]['CMPT_HouseRate_Month']) if data[farm][date].has_key('CMPT_HouseRate_Month') and data[farm][date]['CMPT_HouseRate_Month'] <> '' else 0.0
				
				comletePlan = float(data[farm][date]['CMPT_CompleteOfPlan_Month']) if data[farm][date].has_key('CMPT_CompleteOfPlan_Month') and data[farm][date]['CMPT_CompleteOfPlan_Month'] <> '' else 0.0
				
				rate = (houseRate / comletePlan) * 100 if comletePlan <> 0.0 else 0.0
				
				efficiency = round(houseRate / comletePlan, 4)  if comletePlan <> 0.0 else 0.0
				
				farm_taglist.append({'object':farm, 'date':date})
				farm_datalist.append({'CMPT_GeneratingEfficiency_Month':efficiency})
				
			self.mongo.setData(self.project, farm_taglist, farm_datalist)
		except:
			raise Exception(traceback.format_exc())
		
		
	def CMPT_ProductionForecast(self, starttime, endtime):
		
		try:
		
			end1 = self.nDays(endtime)
			
			end2 = self.nDays(end1)
			
			power_2 = self.kairos.readAchiveDataByDevsTimePerN(self.PF, 'WTUR_PowerForecast_DQ', end1, end2, '900', '')
			
			end3 = self.nDays(end2)
			
			power_3 = self.kairos.readAchiveDataByDevsTimePerN(self.PF, 'WTUR_PowerForecast_DQ', end2, end3, '900', '')
			
			end4 = self.nDays(end3)
			
			power_4 = self.kairos.readAchiveDataByDevsTimePerN(self.PF, 'WTUR_PowerForecast_DQ', end3, end4, '900', '')
			
			end5 = self.nDays(end4)
			
			power_5 = self.kairos.readAchiveDataByDevsTimePerN(self.PF, 'WTUR_PowerForecast_DQ', end4, end5, '900', '')
			
			end6 = self.nDays(end4)
			
			power_6 = self.kairos.readAchiveDataByDevsTimePerN(self.PF, 'WTUR_PowerForecast_DQ', end5, end6, '900', '')
			
			
			production1 = self.getorCastProduction(power_2)
			production2 = self.getorCastProduction(power_3)
			production3 = self.getorCastProduction(power_4)
			production4 = self.getorCastProduction(power_5)
			production5 = self.getorCastProduction(power_6)
			
			return production1, production2, production3, production4, production5
		except:
			raise Exception(traceback.format_exc())
		
	def getorCastProduction(self, power):
		
		try:
			
			production = {}
			
			for pf in self.PF:
				
				project, farm, PF01 = pf.split(':')
				
				farmKey = project+':'+farm
				
				production[farmKey] = 0.0
				
				sum = 0.0
				
				if power.has_key(pf):
					
					for timeT in power[pf]:
						
						if power[pf].has_key(timeT):
						
							if power[pf][timeT] <> '':
								
								sum += float(power[pf][timeT]) * 1000.0 * 900.0
				
				production[farmKey] = round(sum / 3600.0, 4)
				
			return production
		except:
			raise Exception(traceback.format_exc())
		
	def nDays(self, endtime):
		
		starttime2 = endtime
		
		start2 = datetime.datetime.strptime(starttime2,'%Y-%m-%d %H:%M:%S')
		
		end = (start2 + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
		
		return end
		
	def nDaysBefor(self, starttime):
		
		end = starttime
		
		end_2 = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
		
		start = (end_2 - datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
		
		return start
		
	
	def useTimper(self, all_keysDict_timePer, timestamp_month, timestamp_year):
		try:
			useHours = self.cmpt_useHours(all_keysDict_timePer)
			
			mongo_taglist_timePer = []
			mongo_datalist_timePer = []
			
			for date in all_keysDict_timePer:
				
				for key in self.all_keyList:
					
					mongo_taglist_timePer.append({'object':key, 'date':date})
					mongo_datalist_timePer.append(useHours[date][key])
			
			for key in self.all_keyList:
					
				mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
				mongo_datalist_timePer.append(useHours[timestamp_year][key])
		
			self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
		except:
			raise Exception(traceback.format_exc())
		
	def faultStopLostTimper(self, timestamp_month, list_timestamps_month):
		try:
			all_dict = {}
			devs_taglist = []
			devs_datalist = []
			dataDict = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_month, self.devKeys_list, ['CMPT_FaultHoursTimeLen_Day', 'CMPT_FaultStopLostInfo_Day'])
			
			for key in self.devKeys_list:
				
				all_dict[key] = {}
				
				if dataDict.has_key(key):
					
					timeList = []
					
					lost = {}
					
					for date in list_timestamps_month:
					
						if dataDict[key].has_key(date):
						
							if dataDict[key][date].has_key('CMPT_FaultHoursTimeLen_Day') and  dataDict[key][date].has_key('CMPT_FaultStopLostInfo_Day'):
								
								timeList.extend(eval(dataDict[key][date]['CMPT_FaultHoursTimeLen_Day'])) if dataDict[key][date]['CMPT_FaultHoursTimeLen_Day'] <> '[]' else []
								
								lost_temp = eval(dataDict[key][date]['CMPT_FaultStopLostInfo_Day']) if dataDict[key][date]['CMPT_FaultStopLostInfo_Day'] <> '' else {}
								
								if lost_temp <> {}:
									
									for timeT in lost_temp:
										
										lost[timeT] = lost_temp[timeT]
										
								
				all_dict[key]['CMPT_FaultHoursTimeLen_Month'] = str(timeList)
				
				all_dict[key]['CMPT_FaultStopLostInfo_Month'] = str(lost)
				
				devs_taglist.append({'object':key, 'date':timestamp_month})
				devs_datalist.append(all_dict[key])
				
			self.mongo.setData(self.project, devs_taglist, devs_datalist)
		except:
			raise Exception(traceback.format_exc())
	
	def limPwrLostTimper(self, timestamp_month, list_timestamps_month):
		try:
			all_dict = {}
			devs_taglist = []
			devs_datalist = []
			
			keyList = []
			
			keyList.extend(self.wt_farmKey_list)
			
			keyList.extend(self.pv_farmKey_list)
			
			keyList.extend(self.wt_periodKey_list)
			
			keyList.extend(self.pv_periodKey_list)
			
			
			dataDict = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_month, keyList, ['CMPT_LimPwrLostTimeLen_Day', 'CMPT_LimPwrLostInfo_Day'])
			
			for key in keyList:
				
				all_dict[key] = {}
				
				if dataDict.has_key(key):
					
					timeList = []
					
					lost = {}
					
					for date in list_timestamps_month:
					
						if dataDict[key].has_key(date):
						
							if dataDict[key][date].has_key('CMPT_LimPwrLostTimeLen_Day') and  dataDict[key][date].has_key('CMPT_LimPwrLostInfo_Day'):
								
								timeList.extend(eval(dataDict[key][date]['CMPT_LimPwrLostTimeLen_Day'])) if dataDict[key][date]['CMPT_LimPwrLostTimeLen_Day'] <> '[]' else []
								
								lost_temp = eval(dataDict[key][date]['CMPT_LimPwrLostInfo_Day']) if dataDict[key][date]['CMPT_LimPwrLostInfo_Day'] <> '' else {}
								
								if lost_temp <> {}:
									
									for timeT in lost_temp:
										
										lost[timeT] = lost_temp[timeT]
								
				all_dict[key]['CMPT_LimPwrLostTimeLen_Month'] = str(timeList)
				
				all_dict[key]['CMPT_LimPwrLostInfo_Month'] = str(lost)
				
				devs_taglist.append({'object':key, 'date':timestamp_month})
				devs_datalist.append(all_dict[key])
				
			self.mongo.setData(self.project, devs_taglist, devs_datalist)
		except:
			raise Exception(traceback.format_exc())
	def getHardThresholdFiltering(self):
		try:
			hard = {}
			
			for devType in self.wt_devTypes:
				
				data = self.mongo.getHardThresholdFiltering(devType)
				
				if data <> '':
				
					hard[devType] = eval(data)
				
			return hard
		except:
			raise Exception(traceback.format_exc())
	def getFormatWindPowerByDevType(self):
		try:
			format = {}
			
			for devType in self.wt_devTypes:
				
				data = self.mongo.getFormatWindPowerByDevType(devType)
				
				if data <> '':
					
					format[devType] = eval(data)
				
			return format
		except:
			raise Exception(traceback.format_exc())
	def getProduction(self):
		try:
			pro_dict = self.mongo.getTagsByStatisticsTags('CMPT_Production')
			
			pro_list_dict = {}
			
			for pro_Name in pro_dict:
				
				pro_list_dict[pro_Name] = []
				
				for devType in pro_dict[pro_Name]:
					
					for farm in self.devTypeDict:
						
						if devType in self.devTypeDict[farm]:
							
							pro_list_dict[pro_Name].extend(self.devTypeDict[farm][devType])
			return pro_list_dict
		except:
			raise Exception(traceback.format_exc())
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
	def get_median(self, data):
		try:
			data.sort()
			
			half = len(data) // 2
			
			return (data[half] + data[~half]) / 2
		except:
			raise Exception(traceback.format_exc())
	
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
					
	def getSweptArea_ByDevType(self):
		try:
			sweptArea_dict = {}
			
			for devType in self.devTypeList:
				
				sweptArea_dict[devType] = self.mongo.getSweptAreaByDevType(devType)
			
			return sweptArea_dict
		except:
			raise Exception(traceback.format_exc())
	def getValidWindSpeed_ByDevType(self):
		try:
			wsValid_dict = {}
			
			for devType in self.devTypeList:
				
				wsValid_dict[devType] = self.mongo.getValidWindSpeedByDevType(devType)
			
			return wsValid_dict
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
			
	def getDate_ByTimePer(self, startTime, stopTime):
		try:
			start = datetime.datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S')
				
			stop = datetime.datetime.strptime(stopTime,'%Y-%m-%d %H:%M:%S')
			
			xDay = (stop - start).days
			
			aDay = datetime.timedelta(days=1)
			
			i = 0
			
			datelist = []
			
			while i <= xDay:
				
				datelist.append(start.strftime('%Y/%m/%d'))
				
				start += aDay
				
				i += 1
				
			return datelist
		except:
			raise Exception(traceback.format_exc())
			
	def getDate_ByWeek(self, date, flag):
		try:
			timestamp = ''
			list_timestamps = []
			if flag == 1:
				if date.day==1:
					timestamp = date.strftime('%Y/%m') + '-' + str(int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1)
					date = date - datetime.timedelta(days=1)
					list_timestamps = [timestamp, date.strftime('%Y/%m/%d')]
				else:
					week_day = date.weekday()		
					if week_day:			
						week = int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1		
						timestamp = date.strftime('%Y/%m') + '-' + str(int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1)
						date = date - datetime.timedelta(days=1)
						list_timestamps = [timestamp, date.strftime('%Y/%m/%d')]
					
					else:
						date = date-datetime.timedelta(days=1)		
						week = int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1	
						timestamp = date.strftime('%Y/%m') + '-' + str(int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1)	
						date = date - datetime.timedelta(days=1)
						list_timestamps = [timestamp, date.strftime('%Y/%m/%d')]
			else:
				if date.day==1:
					week_day = date.weekday()		
					if week_day:
						list_timestamps = [(date-datetime.timedelta(days=i+1)).strftime('%Y/%m/%d') for i in xrange(week_day)]	
					else:		
						list_timestamps = [(date-datetime.timedelta(days=i+1)).strftime('%Y/%m/%d')  for i in xrange(7)]			
					date = date-datetime.timedelta(days=1)		
					timestamp = date.strftime('%Y/%m') + '-' + str(int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1)
				else:
					week_day = date.weekday()		
					if week_day:			
						week = int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1		
						timestamp = date.strftime('%Y/%m') + '-' + str(int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1)		
						if week==1:
							list_timestamps = [(date-datetime.timedelta(days=i+1)).strftime('%Y/%m/%d') for i in xrange(date.day-1)]
						else:		
							list_timestamps = [(date-datetime.timedelta(days=i+1)).strftime('%Y/%m/%d') for i in xrange(week_day)]
					else:
						date = date-datetime.timedelta(days=1)		
						week = int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1	
						timestamp = date.strftime('%Y/%m') + '-' + str(int(date.strftime("%W")) - int(datetime.datetime(date.year, date.month, 1).strftime("%W")) + 1)	
						if week==1:
							list_timestamps = [(date-datetime.timedelta(days=i)).strftime('%Y/%m/%d') for i in xrange(date.day)]
						else:
							list_timestamps = [(date-datetime.timedelta(days=i)).strftime('%Y/%m/%d') for i in xrange(7)]
				
			return timestamp, list_timestamps
		except:
			raise Exception(traceback.format_exc())
			
	def getDate_ByMonth(self, date, flag):
		try:
			timestamp = ''
			list_timestamps = []
			
			if flag == 1:
				
				date = date - datetime.timedelta(days=1)
				timestamp = date.strftime('%Y/%m')
				list_timestamps = [timestamp, date.strftime('%Y/%m/%d')]
			else:
				date = date - datetime.timedelta(days=1)
				timestamp = date.strftime('%Y/%m')
				list_timestamps = [timestamp + '/' + str(i+1) if i >= 9 else timestamp + '/0' + str(i+1) for i in xrange(date.day)]
				
			return timestamp, list_timestamps
		except:
			raise Exception(traceback.format_exc())
			
	def getDate_ByYear(self, date, flag):
		try:
			date = date - datetime.timedelta(days=1)
			timestamp = ''
			list_timestamps = []
			year = date.year
			month = date.month
			day = date.day
			if flag == 1:
				
				timestamp = date.strftime('%Y')
				list_timestamps = [timestamp, date.strftime('%Y/%m/%d')]
				
			else:
				
				for m in xrange(1, month + 1, 1):
					day_cnt = calendar.monthrange(year, m)[1]
					if m < month:
						day_cnt = calendar.monthrange(int(year), int(m))[1]
						m_ = '0' + str(m) if m < 10 else str(m)
						list_timestamps.extend([str(year) + '/' + m_ + '/' + str(i + 1) if i >= 9 else str(year) + '/' + m_ + '/0' + str(i + 1) for i in xrange(day_cnt)])
					elif m == month:
						m_ = '0' + str(m) if m < 10 else str(m)
						list_timestamps.extend([str(year) + '/' + m_ + '/' + str(i + 1) if i >= 9 else str(year) + '/' + m_ + '/0' + str(i + 1) for i in xrange(int(day))])
				timestamp = date.strftime('%Y')
				
			return timestamp, list_timestamps
		except:
			raise Exception(traceback.format_exc())
			
	def getDate_BySeason(self, date):
		try:
			date = date - datetime.timedelta(days=1)
			timestamp = ''
			list_timestamps = []
			year = str(date.year)
			season = ''
			month = str(date.month)
			day = str(date.day)
			for k, v in dict_season.iteritems():
				if month in v:
					season = k
					for m in v:
						if int(m) < int(month):
							day_cnt = calendar.monthrange(int(year), int(m))[1]
							m_ = '0' + m if int(m) < 10 else m
							list_timestamps.extend([year + '/' + m_ + '/' + str(i + 1) if i >= 9 else year + '/' + m_ + '/0' + str(i + 1) for i in xrange(day_cnt)])
						elif int(m) == int(month):
							m_ = '0' + m if int(m) < 10 else m
							list_timestamps.extend([year + '/' + m_ + '/' + str(i + 1) if i >= 9 else year + '/' + m_ + '/0' + str(i + 1) for i in xrange(int(day))])
					break
			timestamp = year + '-' + season
			return timestamp, list_timestamps
		except:
			raise Exception(traceback.format_exc())
			
	def sum_dev_day(self, cmpt_start, cmpt_end, cmpt_tag, keyList):
		try:
			cmpt_dev_dict = dict((dev, 0.0) for dev in keyList)
			
			for devKey in keyList:
				
				if cmpt_end.has_key(devKey) and cmpt_start.has_key(devKey):
				
					if cmpt_end[devKey].has_key(cmpt_tag) and cmpt_start[devKey].has_key(cmpt_tag):
						
						if cmpt_end[devKey][cmpt_tag] <> '' and cmpt_start[devKey][cmpt_tag] <> '':
							
							end_cmpt_day = float(cmpt_end[devKey][cmpt_tag])
							
							start_cmpt_day = float(cmpt_start[devKey][cmpt_tag])
								
							result =  end_cmpt_day - start_cmpt_day if end_cmpt_day >= start_cmpt_day else 0.0
							
							cmpt_dev_dict[devKey] = round(result, 4)
			return cmpt_dev_dict
		except:
			raise Exception(traceback.format_exc())
			
	def sum_dev_day_pro(self, cmpt_start, cmpt_end, cmpt_tag, keyList, starttime, endtime):
		try:
			cmpt_dev_dict = dict((dev, 0) for dev in keyList)
			
			for devKey in keyList:
				
				if cmpt_end.has_key(devKey) and cmpt_start.has_key(devKey):
				
					if cmpt_end[devKey].has_key(cmpt_tag) and cmpt_start[devKey].has_key(cmpt_tag):
						
						if cmpt_end[devKey][cmpt_tag] <> '' and cmpt_start[devKey][cmpt_tag] <> '':
							
							end_cmpt_day = cmpt_end[devKey][cmpt_tag]
							
							start_cmpt_day = cmpt_start[devKey][cmpt_tag]
								
							#print start_cmpt_day, end_cmpt_day, devKey, cmpt_tag
								
							if (end_cmpt_day - start_cmpt_day)>=0 and (end_cmpt_day - start_cmpt_day) <= float(self.Caps[devKey]+100) * 24.0:
								
								result = round(float(end_cmpt_day - start_cmpt_day), 4)
								
							else:
								
								result = self.getProductionByDev(devKey, starttime, endtime)
								
							cmpt_dev_dict[devKey] = round(float(result), 4)
								
						else:
							
							result = self.getProductionByDev(devKey, starttime, endtime)
							
							cmpt_dev_dict[devKey] = round(float(result), 4)
							
							
			return cmpt_dev_dict
		except:
			raise Exception(traceback.format_exc())
			
	def sum_dev_day_proN(self, cmpt_start, cmpt_end, cmpt_tag_start, cmpt_tag_end, keyList, starttime, endtime):
		try:
			cmpt_dev_dict = dict((dev, 0) for dev in keyList)
			
			for devKey in keyList:
				
				if cmpt_end.has_key(devKey) and cmpt_start.has_key(devKey):
				
					if cmpt_end[devKey].has_key(cmpt_tag_end) and cmpt_start[devKey].has_key(cmpt_tag_start):
						
						if cmpt_end[devKey][cmpt_tag_end] <> '' and cmpt_start[devKey][cmpt_tag_start] <> '':
							
							end_cmpt_day = cmpt_end[devKey][cmpt_tag_end]
							
							start_cmpt_day = cmpt_start[devKey][cmpt_tag_start]
								
							if (end_cmpt_day - start_cmpt_day)>=0 and (end_cmpt_day - start_cmpt_day) <= (self.Caps[devKey]+100) * 24.0:
								
								result = round(float(end_cmpt_day - start_cmpt_day), 4)
								
							else:
								
								result = self.getProductionByDev(devKey, starttime, endtime)
								
							cmpt_dev_dict[devKey] = round(float(result), 4)
								
						else:
							
							result = self.getProductionByDev(devKey, starttime, endtime)
							
							cmpt_dev_dict[devKey] = round(float(result), 4)
							
			return cmpt_dev_dict
		
		except:
			
			raise Exception(traceback.format_exc())
			
			
			
	def sum_group1_day(self, cmpt_dev_dict, periodDict):
		try:
			cmpt_farm_dict = dict((farmKey, 0.0) for farmKey in periodDict)
			
			cmpt_period_dict = {}
			
			for farmKey in periodDict:
				
				cmp_farm_sum = 0.0
					
				for periodKey in periodDict[farmKey]:
					
					cmp_period_sum = 0.0
					
					for devKey in periodDict[farmKey][periodKey]:
						
						if cmpt_dev_dict.has_key(devKey):
						
							cmp_period_sum += cmpt_dev_dict[devKey]
						
					cmpt_period_dict[periodKey] = round(cmp_period_sum, 4)
					
					cmp_farm_sum += cmp_period_sum
				
				cmpt_farm_dict[farmKey] = round(cmp_farm_sum, 4)
				
			return cmpt_farm_dict,cmpt_period_dict
		except:
			raise Exception(traceback.format_exc())
			
	def sum_group2_day(self, wt_cmpt_group_day, pv_cmpt_group_day):
		try:
			cmpt_company_dict = {}
					
			cmpt_project_sum = 0.0
			
			for companyKey in self.company_keys_dict:
				
				cmpt_company_sum = 0.0
				
				if wt_cmpt_group_day <> {}:
					
					for wt_farm in wt_cmpt_group_day:
						
						if wt_farm in self.company_keys_dict[companyKey]:
							
							cmpt_company_sum += wt_cmpt_group_day[wt_farm]
				
				if pv_cmpt_group_day <> {}:
					
					for pv_farm in pv_cmpt_group_day:
						
						if pv_farm in self.company_keys_dict[companyKey]:
							
							cmpt_company_sum += pv_cmpt_group_day[pv_farm]
				
				cmpt_company_dict[companyKey] = round(cmpt_company_sum, 4)
				
				cmpt_project_sum += cmpt_company_sum
				
			return {self.project:cmpt_project_sum}, cmpt_company_dict
		except:
			raise Exception(traceback.format_exc())
			
	def cmpt_sum_day(self, kairos_start, kairos_end, tagName):
		try:
			all_day_dict = {}
			
			wt_dev_day = self.sum_dev_day(kairos_start, kairos_end, tagName, self.wt_devKey_list)
			
			pv_dev_day = self.sum_dev_day(kairos_start, kairos_end, tagName, self.pv_devKey_list)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day(wt_farm_day, pv_farm_day)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def ex_dev_day(self, ex_dict, cmpt_tag, keyList, ex):
		try:
			ex_dev_dict = {}
			
			for devKey in keyList:
				
				ex_day = 0.0
				
				if ex_dict[devKey].has_key(cmpt_tag):
					
					value_list = []
					
					for value in ex_dict[devKey][cmpt_tag].values():
						
						if value == '':
							
							value_list.append(0.0)
						else:
							value_list.append(round(float(value), 4))
					
					if ex == 'max':
						
						ex_day = max(value_list) if value_list <> [] else 0.0
						
					elif ex == 'min':
						
						ex_day = min(value_list) if value_list <> [] else 0.0
						
					elif ex == 'avg':
						
						ex_day = numpy.mean(value_list) if value_list <> [] else 0.0
						
					ex_dev_dict[devKey] = ex_day
						
				else:
					ex_dev_dict[devKey] = 0.0
			
			return ex_dev_dict
		except:
			raise Exception(traceback.format_exc())
			
	def ex_dev_time(self, ex_dict, cmpt_tag, keyList, ex):
		try:
			ex_dev_dict = {}
			
			for devKey in keyList:
				
				ex_day = 0.0
				
				if ex_dict[devKey].has_key(cmpt_tag):
					
					value_list = {}
					
					for date in ex_dict[devKey][cmpt_tag]:
						
						if ex_dict[devKey][cmpt_tag][date] == '':
							
							value_list[date] = 0.0
						else:
							value_list[date] = round(float(ex_dict[devKey][cmpt_tag][date]), 4)
					
					if ex == 'max':
						
						ex_day = max(value_list.items(), key=lambda x: x[1])[0] if value_list <> {} else ex_dict[devKey][cmpt_tag].keys()[0]
						
					elif ex == 'min':
						
						ex_day = min(value_list.items(), key=lambda x: x[1])[0] if value_list <> {} else ex_dict[devKey][cmpt_tag].keys()[0]
						
					ex_dev_dict[devKey] = ex_day
						
				else:
					ex_dev_dict[devKey] = ''
					
			return ex_dev_dict
		except:
			raise Exception(traceback.format_exc())
	
	def ex_group1_day(self, ex_dev_dict, periodDict, ex):
		try:
			ex_farm_dict = {}
			
			ex_period_dict = {}
			
			for farmKey in periodDict:
				
				ex_farm_list = []
					
				for periodKey in periodDict[farmKey]:
					
					ex_period_list = []
					
					for devKey in periodDict[farmKey][periodKey]:
						
						if ex_dev_dict[devKey] <> '':
						
							ex_period_list.append(round(float(ex_dev_dict[devKey]), 4))
						else:
							ex_period_list.append(0.0)
					
					if ex_period_list <> []:
					
						if ex == 'max':
							
							ex_period_dict[periodKey] = max(ex_period_list)
							
						elif ex == 'min':
							
							ex_period_dict[periodKey] = min(ex_period_list)
								
							
						elif ex == 'avg':
							
							ex_period_dict[periodKey] = numpy.mean(ex_period_list)
					else:
						ex_period_dict[periodKey] = 0.0
				
				ex_farm_list.extend(ex_period_list)
					
				if ex_farm_list <> []:
					
					if ex == 'max':
					
						ex_farm_dict[farmKey] = max(ex_farm_list)
						
						
					elif ex == 'min':
						
						ex_farm_dict[farmKey] = min(ex_farm_list)
						
					elif ex == 'avg':
						
						ex_farm_dict[farmKey] = numpy.mean(ex_farm_list)
				else:
					ex_farm_dict[farmKey] = 0.0
			
			return ex_farm_dict,ex_period_dict
		except:
			raise Exception(traceback.format_exc())
			
	def ex_group2_day(self, wt_ex_dev_dict, pv_ex_dev_dict, ex):
		try:
			ex_project_dict = {}
			
			ex_company_dict = {}
					
			projectList = []
			
			for companyKey in self.companyDicts:
				
				companyList = []
				
				for dev in self.companyDicts[companyKey]:
					
					if wt_ex_dev_dict <> {}:
					
						if dev in self.wt_devKey_list:
							
							companyList.append(wt_ex_dev_dict[dev])
					
					if pv_ex_dev_dict <> {}:
						
						if dev in self.pv_devKey_list:
							
							companyList.append(pv_ex_dev_dict[dev]) 
					
					if companyList <> []:
					
						if ex == 'max':
						
							ex_company_dict[companyKey] = round(max(companyList), 4)
						
						elif ex == 'min':
							
							ex_company_dict[companyKey] = round(min(companyList), 4)
						
						elif ex == 'avg':
						
							ex_company_dict[companyKey] = round(numpy.mean(companyList), 4)
					else:
						ex_company_dict[companyKey]= 0.0
					
					projectList.extend(companyList)
					
			if ex == 'max':
				
				ex_project_dict[self.project] = round(max(projectList), 4) if projectList <> [] else 0.0
			
			elif ex == 'min':
				
				ex_project_dict[self.project] = round(min(projectList), 4) if projectList <> [] else 0.0
			
			elif ex == 'avg':
			
				ex_project_dict[self.project] = round(numpy.mean(projectList), 4) if projectList <> [] else 0.0
					
			return ex_project_dict, ex_company_dict
		except:
			raise Exception(traceback.format_exc())
			
	def cmpt_ex_day(self, ex_dict, tagName, ex):
		try:
			all_day_dict = {}
			
			wt_dev_day = self.ex_dev_day(ex_dict, tagName, self.wt_devKey_list, ex)
			
			pv_dev_day = self.ex_dev_day(ex_dict, tagName, self.pv_devKey_list, ex)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, ex)
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, ex)
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, ex)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def cmpt_mix_day(self, ex_dict, tagName, ex):
		try:
			all_day_dict = {}
			
			wt_dev_day = self.ex_dev_day(ex_dict, tagName, self.wt_devKey_list, ex)
			
			pv_dev_day = self.ex_dev_day(ex_dict, tagName, self.pv_devKey_list, ex)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day(wt_farm_day, pv_farm_day)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	def cmpt_sum_timeper(self, keyList, tagList, date_now):
		try:
			timestamp_week, list_timestamps_week = self.getDate_ByWeek(date_now, 0)
				
			timestamp_month, list_timestamps_month = self.getDate_ByMonth(date_now, 0)
			
			timestamp_year, list_timestamps_year = self.getDate_ByYear(date_now, 0)
			
			keyDicts_week = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_week, keyList, tagList)
			
			keyDicts_month = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_month, keyList, tagList)
			
			keyDicts_year = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_year, keyList, tagList)
			
			keyDics, week_dict, month_dict, year_dict  = {}, {}, {}, {}
			
			for key in keyList:
				
				week_dict[key], month_dict[key], year_dict[key] = {}, {}, {}
				
				for tag in tagList:
					
					sum_week, sum_month, sum_year = 0.0, 0.0, 0.0
					
					for date in list_timestamps_week:
						if keyDicts_week.has_key(key):
							if keyDicts_week[key].has_key(date):
								
								if keyDicts_week[key][date].has_key(tag):
									
									sum_week += float(keyDicts_week[key][date][tag]) if keyDicts_week[key][date][tag] <> '' else 0.0
						
					for date in list_timestamps_month:
						if keyDicts_month.has_key(key):
							if keyDicts_month[key].has_key(date):
								
								if keyDicts_month[key][date].has_key(tag):
								
									sum_month += float(keyDicts_month[key][date][tag]) if keyDicts_month[key][date][tag] <> '' else 0.0
						
					for date in list_timestamps_year:
						if keyDicts_year.has_key(key):
							if keyDicts_year[key].has_key(date):
								
								if keyDicts_year[key][date].has_key(tag):
									
									sum_year += float(keyDicts_year[key][date][tag]) if keyDicts_year[key][date][tag] <> '' else 0.0
					
					week_dict[key][tag.split('Day')[0]+'Week'] = round(sum_week, 4)
					
					month_dict[key][tag.split('Day')[0]+'Month'] = round(sum_month, 4)
					
					year_dict[key][tag.split('Day')[0]+'Year'] = round(sum_year, 4)
					
			keyDics = {timestamp_week:week_dict, timestamp_month:month_dict, timestamp_year:year_dict}
			return keyDics
		except:
			raise Exception(traceback.format_exc())
	def cmpt_ex_timeper(self, keyList, tagList, date_now, ex):
		try:
			timestamp_week, list_timestamps_week = self.getDate_ByWeek(date_now, 0)
				
			timestamp_month, list_timestamps_month = self.getDate_ByMonth(date_now, 0)
			
			timestamp_year, list_timestamps_year = self.getDate_ByYear(date_now, 0)
			
			keyDicts_week = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_week, keyList, tagList)
			
			keyDicts_month = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_month, keyList, tagList)
			
			keyDicts_year = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_year, keyList, tagList)
			
			keyDics, week_dict, month_dict, year_dict = {}, {}, {}, {}
			
			
			for key in keyList:
				
				week_dict[key], month_dict[key], year_dict[key] = {}, {}, {}
				
				for tag in tagList:
					
					ex_week, ex_month, ex_year = 0.0, 0.0, 0.0
					
					tempList_week = []
					
					for date in list_timestamps_week:
						
						if keyDicts_week.has_key(key):
						
							if keyDicts_week[key].has_key(date):
								
								if keyDicts_week[key][date].has_key(tag):
									
									value = keyDicts_week[key][date][tag]
									
									if value <> '':
										
										try:
										
											tempList_week.append(float(value))
										
										except:
											
											tempList_week.append(0.0)
									else:
										
										tempList_week.append(0.0)
					if ex == 'max':
						
						ex_week = round(max(tempList_week), 4) if tempList_week <> [] else 0.0
						
					elif ex == 'min':
						
						ex_week = round(min(tempList_week), 4) if tempList_week <> [] else 0.0
					
					elif ex == 'avg':
						
						ex_week = round(numpy.mean(tempList_week), 4) if tempList_week <> [] else 0.0
					
					
					tempList_month = []
					
					for date in list_timestamps_month:
						
						if keyDicts_month.has_key(key):
						
							if keyDicts_month[key].has_key(date):
								
								if keyDicts_month[key][date].has_key(tag):
									
									value =  keyDicts_month[key][date][tag]
									
									if value <> '':
										
										try:
										
											tempList_month.append(float(value))
										
										except:
											
											tempList_month.append(0.0)
										
									else:
										
										tempList_month.append(0.0)
						
					if ex == 'max':
						
						ex_month = round(max(tempList_month), 4) if tempList_month <> [] else 0.0
						
					elif ex == 'min':
						
						ex_month = round(min(tempList_month), 4) if tempList_month <> [] else 0.0
					
					elif ex == 'avg':
						
						ex_month = round(numpy.mean(tempList_month), 4) if tempList_month <> [] else 0.0
						
					tempList_year = []
						
					for date in list_timestamps_year:
						
						if keyDicts_year.has_key(key):
						
							if keyDicts_year[key].has_key(date):
								
								if keyDicts_year[key][date].has_key(tag):
									
									value = keyDicts_year[key][date][tag]
									
									if value <> '':
										
										try:
										
											tempList_year.append(float(value))
										
										except:
											
											tempList_year.append(0.0)
											
									else:
										tempList_year.append(0.0)
						
					if ex == 'max':
						
						ex_year = round(max(tempList_year), 4) if tempList_year <> [] else 0.0
						
					elif ex == 'min':
						
						ex_year = round(min(tempList_year), 4) if tempList_year <> [] else 0.0
					
					elif ex == 'avg':
						
						ex_year = round(numpy.mean(tempList_year), 4) if tempList_year <> [] else 0.0
						
					week_dict[key][tag.split('Day')[0]+'Week'] = round(ex_week, 4)
					
					month_dict[key][tag.split('Day')[0]+'Month'] = round(ex_month, 4)
					
					year_dict[key][tag.split('Day')[0]+'Year'] = round(ex_year, 4)
				
			keyDics = {timestamp_week:week_dict, timestamp_month:month_dict, timestamp_year:year_dict}
			
			return keyDics
		except:
			raise Exception(traceback.format_exc())
			
			
	def cmpt_ex_timeper_rate(self, keyList, tagList, date_now, ex):
		try:
			timestamp_week, list_timestamps_week = self.getDate_ByWeek(date_now, 0)
				
			timestamp_month, list_timestamps_month = self.getDate_ByMonth(date_now, 0)
			
			timestamp_year, list_timestamps_year = self.getDate_ByYear(date_now, 0)
			
			keyDicts_week = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_week, keyList, tagList)
			
			keyDicts_month = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_month, keyList, tagList)
			
			keyDicts_year = self.mongo.getStatisticsByKeyList_DateList(self.project, list_timestamps_year, keyList, tagList)
			
			keyDics, week_dict, month_dict, year_dict = {}, {}, {}, {}
			
			
			for key in keyList:
				
				week_dict[key], month_dict[key], year_dict[key] = {}, {}, {}
				
				for tag in tagList:
					
					ex_week, ex_month, ex_year = 0.0, 0.0, 0.0
					
					tempList_week = []
					
					for date in list_timestamps_week:
						
						if keyDicts_week.has_key(key):
						
							if keyDicts_week[key].has_key(date):
								
								if keyDicts_week[key][date].has_key(tag):
									
									value = keyDicts_week[key][date][tag]
									
									if value <> '':
										
										try:
											
											tempList_week.append(float(value)) if float(value)>= 0.0 else 0.0
											
										except:
											
											tempList_week.append(0.0)
										
									else:
										
										tempList_week.append(0.0)
					if ex == 'max':
						
						ex_week = round(max(tempList_week), 4) if tempList_week <> [] else 0.0
						
					elif ex == 'min':
						
						ex_week = round(min(tempList_week), 4) if tempList_week <> [] else 0.0
					
					elif ex == 'avg':
						
						avg_temp = numpy.mean(tempList_week) if tempList_week <> [] else 0.0
						
						ex_week = round(avg_temp * 100, 4) if avg_temp <=1.0 else 100
					
					tempList_month = []
					
					for date in list_timestamps_month:
						
						if keyDicts_month.has_key(key):
						
							if keyDicts_month[key].has_key(date):
								
								if keyDicts_month[key][date].has_key(tag):
									
									value =  keyDicts_month[key][date][tag]
									
									if value <> '':
										
										try:
											
											tempList_month.append(float(value)) if float(value)>= 0.0 else 0.0
											
										except:
											
											tempList_month.append(0.0)
									else:
										
										tempList_month.append(0.0)
						
					if ex == 'max':
						
						ex_month = round(max(tempList_month), 4) if tempList_month <> [] else 0
						
					elif ex == 'min':
						
						ex_month = round(min(tempList_month), 4) if tempList_month <> [] else 0
					
					elif ex == 'avg':
						
						avg_temp = numpy.mean(tempList_month) if tempList_month <> [] else 0.0
						
						ex_month = round(avg_temp * 100, 4) if avg_temp <=1.0 else 100
						
					tempList_year = []
						
					for date in list_timestamps_year:
						
						if keyDicts_year.has_key(key):
						
							if keyDicts_year[key].has_key(date):
								
								if keyDicts_year[key][date].has_key(tag):
									
									value = keyDicts_year[key][date][tag]
									
									if value <> '':
									
										try:
											
											tempList_year.append(float(value)) if float(value)>= 0.0 else 0.0
											
										except:
											
											tempList_year.append(0.0)
									else:
										tempList_year.append(0.0)
						
					if ex == 'max':
						
						ex_year = round(max(tempList_year), 4) if tempList_year <> [] else 0
						
					elif ex == 'min':
						
						ex_year = round(min(tempList_year), 4) if tempList_year <> [] else 0
					
					elif ex == 'avg':
						
						avg_temp = numpy.mean(tempList_year) if tempList_year <> [] else 0.0
						
						ex_year = round(avg_temp * 100, 4) if avg_temp <=1.0 else 100
						
					week_dict[key][tag.split('Day')[0]+'Week'] = round(ex_week, 4)
					
					month_dict[key][tag.split('Day')[0]+'Month'] = round(ex_month, 4)
					
					year_dict[key][tag.split('Day')[0]+'Year'] = round(ex_year, 4)
				
			keyDics = {timestamp_week:week_dict, timestamp_month:month_dict, timestamp_year:year_dict}
			
			return keyDics
		except:
			raise Exception(traceback.format_exc())
			
			
	def cmpt_useHours(self, all_keysDict_timePer):
		try:
			useHours_dict = dict((date, {}) for date in all_keysDict_timePer)
			
			for date in all_keysDict_timePer:
				if '-' in date:
					week = date
				elif '/' not in date:
					year = date
				else:
					month = date 
			
			production_week = self.mongo.getStatisticsByKeyList_DateList(self.project, [week], self.all_keyList, ['CMPT_Production_Week'])
			
			production_month = self.mongo.getStatisticsByKeyList_DateList(self.project, [month], self.all_keyList, ['CMPT_Production_Month'])
			
			production_year = self.mongo.getStatisticsByKeyList_DateList(self.project, [year], self.all_keyList, ['CMPT_Production_Year'])
			
			for key in self.all_keyList:
				
				if production_week.has_key(key):
					
					if production_week[key].has_key(week):
						
						if production_week[key][week].has_key('CMPT_Production_Week'):
								
							useHours_dict[week][key] = {'CMPT_UseHours_Week': round(production_week[key][week]['CMPT_Production_Week']/self.Caps[key], 4)}
						else:
							useHours_dict[week][key] = {'CMPT_UseHours_Week':0.0}
					else:
						useHours_dict[week][key] = {'CMPT_UseHours_Week':0.0}
				else:
					useHours_dict[week][key] = {'CMPT_UseHours_Week':0.0}
				
				if production_month.has_key(key):
					
					if production_month[key].has_key(month):
						
						if production_month[key][month].has_key('CMPT_Production_Month'):
								
							useHours_dict[month][key] = {'CMPT_UseHours_Month': round(production_month[key][month]['CMPT_Production_Month']/self.Caps[key], 4)}
						else:
							useHours_dict[month][key] = {'CMPT_UseHours_Month':0.0}
					else:
						useHours_dict[month][key] = {'CMPT_UseHours_Month':0.0}
				else:
					useHours_dict[month][key] = {'CMPT_UseHours_Month':0.0}
				
				if production_year.has_key(key):
					
					if production_year[key].has_key(year):
						
						if production_year[key][year].has_key('CMPT_Production_Year'):
								
							useHours_dict[year][key] = {'CMPT_UseHours_Year': round(production_year[key][year]['CMPT_Production_Year']/self.Caps[key], 4)}
						else:
							useHours_dict[year][key] = {'CMPT_UseHours_Year':0.0}
					else:
						useHours_dict[year][key] = {'CMPT_UseHours_Year':0.0}
				else:
					useHours_dict[year][key] = {'CMPT_UseHours_Year':0.0}
						
						
			return useHours_dict
		except:
			raise Exception(traceback.format_exc())
		
	def windSpeedValid_dev_day(self, ex_dict):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for devKey in self.wt_devKey_list:
				
				#devType = self.all_dev_obj_dict[devKey]['deviceType']
				
				#valid_min, valid_max = 3, 25#self.devType_windSpeedValid_dict[devType].split('-')
				
				count = 0
				
				if ex_dict[devKey].has_key('CMPT_WindSpeed_Avg'):
				
					for date in ex_dict[devKey]['CMPT_WindSpeed_Avg']:
						
						windspeed = float(ex_dict[devKey]['CMPT_WindSpeed_Avg'][date]) if ex_dict[devKey]['CMPT_WindSpeed_Avg'][date] <> '' else 0.0
						
						if windspeed <= 25.0 and windspeed >= 3.0:
							
							#print windspeed
							
							count += 1
					
				devDict_wt[devKey] = round(count/3600, 4)
					
			for devKey in self.pv_devKey_list:
				
				count = 0
				
				if ex_dict[devKey].has_key('CMPT_WindSpeed_Avg'):
				
					for date in ex_dict[devKey]['CMPT_WindSpeed_Avg']:
						
						windspeed = float(ex_dict[devKey]['CMPT_WindSpeed_Avg'][date]) if ex_dict[devKey]['CMPT_WindSpeed_Avg'][date] <> '' else 0.0
						
						if windspeed <= 25.0 and windspeed >= 3.0:
							
							count += 1
					
				devDict_pv[devKey] = round(count/3600, 4)
			
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def windEnerge_dev_day(self, ex_dict, airDen_dict):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			swepArea = 5000#float(self.devType_sweptArea_dict[devType])
			
			project_list = []
			
			devDict_wt[self.project] = 0.0
			
			for company in self.company_keys_dict:
						
				devDict_wt[company] = 0.0
				
				company_list = []
				
				for farm in self.company_keys_dict[company]:
					
					devDict_wt[farm] = 0.0
					
					if farm in self.wt_devKeys_dict:
						
						farm_list = []
						
						dateList = airDen_dict[farm]['CMPT_AirDensity'].keys()
						
						for period in self.wt_devKeys_dict[farm]:
							
							devDict_wt[period] = 0.0
							
							period_list = []
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								devType = self.all_dev_obj_dict[dev]['deviceType']
								
								sum = 0.0
								
								if ex_dict[dev].has_key('CMPT_WindSpeed_Avg') and airDen_dict[farm].has_key('CMPT_AirDensity'):
									
									for date in dateList:
									
										airDen = float(airDen_dict[farm]['CMPT_AirDensity'][date]) if airDen_dict[farm]['CMPT_AirDensity'].has_key(date) and airDen_dict[farm]['CMPT_AirDensity'][date] <> '' else 0.0
										
										windSpeed = float(ex_dict[dev]['CMPT_WindSpeed_Avg'][date]) if ex_dict[dev]['CMPT_WindSpeed_Avg'].has_key(date) and ex_dict[dev]['CMPT_WindSpeed_Avg'][date] <> '' else 0.0
										
										sum += 0.5*airDen*swepArea*pow(windSpeed,3)
									
								devDict_wt[dev] = round(sum, 4)
								
								period_list.append(round(sum, 4))
								
							devDict_wt[period] = numpy.mean(period_list) if period_list <> 0.0 else 0.0
								
							farm_list.extend(period_list)
								
						devDict_wt[farm] = numpy.mean(farm_list) if farm_list <> 0.0 else 0.0
						
						company_list.extend(farm_list)
						
				devDict_wt[company] = numpy.mean(company_list) if company_list <> 0.0 else 0.0
						
				project_list.extend(company_list)
						
			devDict_wt[self.project] = numpy.mean(project_list) if project_list <> 0.0 else 0.0
						
						
			return devDict_wt
		except:
			raise Exception(traceback.format_exc())
				
	def getWindEnergy(self, ex_dict, rose_dict, airDen_dict, windDir_dict):
		try:
			farm_dict = {}
			
			swepArea = 5000
			
			for farm in self.wt_devKeys_dict:
				
				farm_dict[farm] =  dict((dir, 0.0) for dir in ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15'])
				
				dateList = airDen_dict[farm]['CMPT_AirDensity'].keys()
				
				for period in self.wt_devKeys_dict[farm]:
				
					for dev in self.wt_devKeys_dict[farm][period]:
						
						devType = self.all_dev_obj_dict[dev]['deviceType']
						
						if ex_dict[dev].has_key('CMPT_WindSpeed_Avg') and rose_dict[dev].has_key('WTUR_WindEnerge') and windDir_dict[dev].has_key('CMPT_WindDir'):
							
							for date in dateList:
								
								airDen = float(airDen_dict[farm]['CMPT_AirDensity'][date]) if airDen_dict[farm]['CMPT_AirDensity'].has_key(date) and airDen_dict[farm]['CMPT_AirDensity'][date] <> '' else 0.0
								
								windSpeed = float(ex_dict[dev]['CMPT_WindSpeed_Avg'][date]) if ex_dict[dev]['CMPT_WindSpeed_Avg'].has_key(date) and ex_dict[dev]['CMPT_WindSpeed_Avg'][date] <> '' else 0.0
								
								if windDir_dict[dev]['CMPT_WindDir'].has_key(date):
									
									if windDir_dict[dev]['CMPT_WindDir'][date] <> '':
									
										farm_dict[farm][str(int(float(windDir_dict[dev]['CMPT_WindDir'][date])))] += round((0.5*airDen*swepArea*pow(windSpeed,3)/3600.0), 4)
			farm_dicts = {}
			
			for farm in farm_dict:
				
				farm_dicts[farm] = str(farm_dict[farm])
			
			return farm_dicts
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_WindPower(self, ex_dict):
		try:
			tag_power = 'CMPT_ActPower'
			
			tag_wind = 'CMPT_WindSpeed_Avg'
			
			dev_dict = {}
			
			for farm in self.farmKeys_list:
				
				for devType in self.devTypeDict[farm]:
					
					if self.formatWindPowerByDevType.has_key(devType):
						
						format = self.formatWindPowerByDevType[devType] 
						
						format_dict = {}
							
						for wind in format:
							
							format_dict[float(wind.split('-')[0])] = format[wind]
						
						winds = sorted(format_dict)
						
						windList_format ,powerList_format = [], []
						
						format_dict_new = {}
						
						for wind in winds:
							
							windList_format.append(str(wind))
							
							powerList_format.append(format_dict[wind])
							
						format_dict_new = {'wind':windList_format,'power':powerList_format}
							
						for dev in self.devTypeDict[farm][devType]:
							
							dev_dict[dev] = {'format':format_dict_new,'real':format_dict_new}
							
							hard_filtering = self.hardThresholdFiltering[devType]
							
							wind_speed_group_dict = dict((wind_speed_group.split('-')) for wind_speed_group in hard_filtering )
							
							power_group_dict = dict((power_group.split('-')) for power_group in hard_filtering.values() )
							
							timeList = sorted(ex_dict[dev][tag_wind]) if ex_dict.has_key(dev) and ex_dict[dev].has_key(tag_wind) else []
							
							fit_2 = dict((str(wind_speed)+'-'+str(wind_speed_group_dict[wind_speed]) , []) for wind_speed in wind_speed_group_dict)
							
							if timeList <> []:
							
								for date in timeList:
									
									wind = ex_dict[dev][tag_wind][date] if ex_dict[dev][tag_wind][date] <> '' else 0.0
									
									power = ex_dict[dev][tag_power][date] if ex_dict[dev][tag_power][date] <> '' else 0.0
									
									for wind_speed_min in wind_speed_group_dict:
										
										if wind >= float(wind_speed_min) and wind < float(wind_speed_group_dict[wind_speed_min]):
											
											wind_key = str(wind_speed_min)+'-'+str(wind_speed_group_dict[wind_speed_min])
										
											power_low, power_up = hard_filtering[wind_key].split('-')
											
											if not fit_2.has_key(wind_key):
											
												fit_2[wind_key] = []
											
											if power >= float(power_low) and power < float(power_up):
												
												fit_2[wind_key].append(power)

							fit_1 = dict((str(wind_speed)+'-'+str(wind_speed_group_dict[wind_speed]) , '') for wind_speed in wind_speed_group_dict)
										
							for wind in fit_2:
								
								if fit_2[wind] <> []:
									
									mid = self.get_median(fit_2[wind])
									
									low, up = hard_filtering[wind].split('-')
									
									deviations  = float(up) - mid
										
									low = mid - deviations
										
									if low >= 0 :
										
										fit_1[wind] = str(mid - deviations)+'-'+up 
									else:
										fit_1[wind] = '0.0-'+up 
							
							wind_speed_group_dict = dict((wind_speed_group.split('-')) for wind_speed_group in format )
							
							power = {}
									
							power_list_dict = dict((wind_speed , []) for wind_speed in wind_speed_group_dict)
							
							timeList = sorted(ex_dict[dev][tag_wind]) if ex_dict.has_key(dev) and ex_dict[dev].has_key(tag_wind) else []
										
							if timeList <> []:
										
								for date in timeList:
								
									wind = ex_dict[dev][tag_wind][date] if ex_dict[dev][tag_wind][date] <> '' else 0.0
									
									power = ex_dict[dev][tag_power][date] if ex_dict[dev][tag_power][date] <> '' else 0.0
									
									for wind_speed_min in wind_speed_group_dict:
													
										if wind >= float(wind_speed_min) and wind < float(wind_speed_group_dict[wind_speed_min]):
											
											if fit_1[wind_speed_min+'-'+wind_speed_group_dict[wind_speed_min]] <> '':
												
												power_low ,power_up = fit_1[wind_speed_min+'-'+wind_speed_group_dict[wind_speed_min]].split('-')
												
												if power >= float(power_low) and power < float(power_up):
												
													power_list_dict[wind_speed_min].append(float(power))
							
							fit = {}
							
							for wind_speed_min in wind_speed_group_dict:
								
								if len(power_list_dict[wind_speed_min]) > 0:
									
									fit[float(wind_speed_min)] = round(numpy.mean(power_list_dict[wind_speed_min]), 4)
									
								else:
									
									fit[float(wind_speed_min)] = format[str(wind_speed_min)+'-'+str(wind_speed_group_dict[wind_speed_min])]
							
							winds = sorted(fit)
							
							powerList ,windList = [], []
							
							for wind in winds:
								
								powerList.append(str(fit[wind]))
								
								windList.append(str(wind))
							
							dev_dict[dev] = {'format':format_dict_new,'real':{'wind':windList,'power':powerList}}
			
			return dev_dict
		except:
			raise Exception(traceback.format_exc())
		
	def getProductionByDev(self, dev, starttime, endtime):
		try:
			production_end_dict = self.kairos.readArchiveData(dev, 'CMPT_TotProduction', endtime)
							
			production_start_dict = self.kairos.readArchiveData(dev, 'CMPT_TotProduction', starttime)
			
			if production_end_dict <> {}:
				
				production_end = int(production_end_dict['CMPT_TotProduction']) if production_end_dict['CMPT_TotProduction'] <> '' else 0
				
			if production_start_dict <> {}:
				
				production_start = int(production_start_dict['CMPT_TotProduction']) if production_start_dict['CMPT_TotProduction'] <> '' else 0
				
				
			value = 0
			
			if production_end <> 0 :
				
				if production_start <> 0:
					
					value = production_end - production_start 
					
					if value <0:
						
						value = 0
					
					elif (value>float(self.Caps[dev]+100) * 24.0):
						
						value = float(self.Caps[dev]+100) * 24.0
					
				else:
					
					start = self.nDaysBefor(starttime)
					
					production_list = self.kairos.readAllAchiveDataTimePer(dev, 'CMPT_TotProduction', start, starttime, '')
					
					production_start = int(max(production_list.values())) if production_list <> {} else 0.0
					
					#value = production_end - production_start if (production_end - production_start) >= 0 and (production_end - production_start) <= float(self.Caps[dev]) * 24.0 else 0
					
					value = production_end - production_start 
					
					if value <0:
						
						value = 0
					
					elif (value>float(self.Caps[dev]+100) * 24.0):
						
						value = float(self.Caps[dev]+100) * 24.0
					
			else:
				
				production_list = self.kairos.readAllAchiveDataTimePer(dev, 'CMPT_TotProduction', starttime, endtime, '')
					
				production_end = int(max(production_list.values())) if production_list <> {} else 0.0
				
				if production_start <> 0:
					
					#value = production_end - production_start if (production_end - production_start) >= 0 and (production_end - production_start) <= float(self.Caps[dev]) * 24.0 else 0
					
					value = production_end - production_start 
					
					if value <0:
						
						value = 0
					
					elif (value>float(self.Caps[dev]+100) * 24.0):
						
						value = float(self.Caps[dev]+100) * 24.0
					
				else:
					
					start = self.nDaysBefor(starttime)
					
					production_list = self.kairos.readAllAchiveDataTimePer(dev, 'CMPT_TotProduction', start, starttime, '')
					
					production_start = int(max(production_list.values())) if production_list <> {} else 0.0
					
					#value = production_end - production_start if (production_end - production_start) >= 0 and (production_end - production_start) <= float(self.Caps[dev]) * 24.0 else 0
					
					value = production_end - production_start 
					
					if value <0:
						
						value = 0
					
					elif (value>float(self.Caps[dev]+100) * 24.0):
						
						value = float(self.Caps[dev]+100) * 24.0
					
					
			return value
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_Production(self, production_start_tot, production_end_tot, production_day, starttime, endtime):
		try:
			all_day_dict = {}
			
			wt_dev_day = self.sum_dev_day_pro(production_start_tot, production_end_tot, 'CMPT_TotProduction', self.wt_devKey_list, starttime, endtime)
			
			pv_dev_day = self.sum_dev_day_pro(production_start_tot, production_end_tot, 'CMPT_TotProduction', self.pv_devKey_list, starttime, endtime)
			
			
			for farm in self.farmKeys_list:
				
				if farm == 'DTXYJK:SL':
					
					#一期 G58_850_1 二期GW_1500_1071
					
					for period in self.wt_devKeys_dict[farm]:
						
						devs = self.wt_devKeys_dict[farm][period]
						
						if period == 'DTXYJK:SL:SLFD1':
							
							data_periods = self.production_sl(devs, starttime, endtime)
							
							for dev in devs:
								
								wt_dev_day[dev] = round(float(data_periods[dev]), 4)
						'''
						elif period == 'SLFD2':
							
							start = self.readActiveDataByDevsTags(keyList, ['WTUR_Production_Month'], starttime)
		
							end = self.readActiveDataByDevsTags(keyList,['WTUR_Production_Month'], endtime)
							
							data_periods = self.sum_dev_day_pro(start, end, 'WTUR_Production_Month', keyList, starttime, endtime)
							
							for dev in keyList:
								
								wt_dev_day[dev] = round(float(data_periods[dev]), 4)
						'''
			for dev in self.production_dict['WTUR_Production_Day']:
				
				valueList = []
				
				for time in production_day[dev]:
					
					value = production_day[dev][time] if production_day.has_key(dev) else 0.0
					
					if value <> '':
						
						valueList.append(round(float(value), 4))
							
				if dev in self.wt_devKey_list:
					
					value = round(max(valueList), 4) if valueList <> [] else 0.0
					
					if value > float(self.Caps[dev]+100) * 24.0:
					
						wt_dev_day[dev] = self.getProductionByDev(dev, starttime, endtime)
					else:
						wt_dev_day[dev] = value
						
				elif dev in self.pv_devKey_list:
					
					value = round(max(valueList), 4) if valueList <> [] else 0.0
					
					if value > float(self.Caps[dev]+100) * 24.0:
					
						pv_dev_day[dev] = self.getProductionByDev(dev, starttime, endtime)
					
					else:
					
						pv_dev_day[dev] = value
						
			for dev in self.wt_devKey_list:
				
				value = wt_dev_day[dev]
				
				if value > float(self.Caps[dev]+100) * 24.0:
					
					wt_dev_day[dev] = self.getProductionByDev(dev, starttime, endtime)
						
			for dev in self.pv_devKey_list:
				
				value = pv_dev_day[dev]
				
				if value > float(self.Caps[dev]+100) * 24.0:
					
					pv_dev_day[dev] = self.getProductionByDev(dev, starttime, endtime)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day(wt_farm_day, pv_farm_day)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
					
					
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
			
	def production_sl(self, keyList, starttime, endtime):
		
		date,time = endtime.split(' ')
		
		year, month, day = date.split('-')
			
		if int(day) <> 1:
			
			start = self.kairos.readActiveDataByDevsTags(keyList, ['WTUR_Production_Month'], starttime)
		
			end = self.kairos.readActiveDataByDevsTags(keyList,['WTUR_Production_Month'], endtime)
			
			data_periods = self.sum_dev_day_pro(start, end, 'WTUR_Production_Month', keyList, starttime, endtime)
			
		else:
			
			start = self.kairos.readActiveDataByDevsTags(keyList, ['WTUR_Production_Month'], starttime)
		
			end = self.kairos.readActiveDataByDevsTags(keyList,['WTUR_Production_LastMonth'], endtime)
			
			
			data_periods = self.sum_dev_day_proN(start, end, 'WTUR_Production_Month','WTUR_Production_LastMonth', keyList, starttime, endtime)
			
		return data_periods
		
		
	def CMPT_Production_1h(self, production_start_tot, production_end_tot):
		try:
			wt_dev_day = self.sum_dev_day(production_start_tot, production_end_tot, 'CMPT_TotProduction', self.wt_devKey_list)
			
			pv_dev_day = self.sum_dev_day(production_start_tot, production_end_tot, 'CMPT_TotProduction', self.pv_devKey_list)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
				
			return dict(wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items())
		except:
			raise Exception(traceback.format_exc())
		
		
	def CMPT_UseHours(self, production_dict_day):
		try:
			useHours_dict = {}
			
			for key in self.all_keyList:
				
				useHours_dict[key] = round(production_dict_day[key] / self.Caps[key] ,4) if production_dict_day.has_key(key) else 0.0
				
			return useHours_dict
		except:
			raise Exception(traceback.format_exc())
	def CMPT_UseRatio(self, useHours):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getUseRatio(useHours)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	
		
	def CMPT_OutPowerRatio(self, useHours, hours_end, day_date):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getOutPowerRatio(useHours, hours_end, day_date)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	def CMPT_UserForGenerationHours(self, hours_end, day_date):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getUserForGenerationHours(hours_end, day_date)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	def CMPT_UserForGenerRatio(self, userForGenerationHours):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getUserForGenerRatio(userForGenerationHours)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_Availability(self, faultHours):
		try:
			return self.getAvailability(faultHours)
		except:
			raise Exception(traceback.format_exc())
		
	def getPowerLoadRate(self, useHours, userForGenerationHours):
		
		powerLoadRate = {}
		
		for key in self.all_keyList:
			
			if useHours.has_key(key) and userForGenerationHours.has_key(key):
			
				powerLoadRate[key] = round((useHours[key] / userForGenerationHours[key]) * 100, 4) if userForGenerationHours[key] <> 0.0 else 0.0
			
			else:
				
				powerLoadRate[key] = 0.0
		
		return powerLoadRate
		
	def CMPT_GenerateRate(self, production_dict_day, windEnerge, totRadition):
		try:
			return self.getGenerateRate(production_dict_day, windEnerge, totRadition)
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_ProductionIntegal(self, production_start, production_end):
		try:
			return self.cmpt_sum_day(production_start, production_end, 'CMPT_TotProductionIntegal')
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_ProductionTheory(self, production_start, production_end):
		try:
			return self.cmpt_sum_day(production_start, production_end, 'CMPT_TotProductionTheory')
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_ProductionPlan(self, year):
		try:
			mongo_taglist_timePer = []
			
			mongo_datalist_timePer = []
			
			all_dict = {}
			
			company_dict = {}
			
			farm_dict = {}
			
			period_dict = {}
			
			monthlist = list('0'+str(i) for i in range(1, 10))
			monthlist.extend(list(str(i) for i in range(10, 13)))
			
			all_keysDict_timePer = list(year +'/'+ month for month in monthlist)
				
			all_keysDict_timePer.append(year)
				
			for date in all_keysDict_timePer:
				
				all_dict[self.project] = 0.0
				
				if '/' not in date:
					
					for company in self.company_keys_dict:
						
						company_dict[company] = 0.0
						
						for farm in self.company_keys_dict[company]:
							
							if farm in self.wt_devKeys_dict:
								
								for period in self.wt_devKeys_dict[farm]:
									
									if self.production_plan.has_key(period):
									
										period_dict[period] = sum(self.production_plan[period])
										mongo_taglist_timePer.append({'object':period, 'date':date})
										mongo_datalist_timePer.append({'CMPT_ProductionPlan_Year': float(period_dict[period])})
									
							if farm in self.pv_devKeys_dict:
								
								for period in self.pv_devKeys_dict[farm]:
									
									if self.production_plan.has_key(period):
									
										period_dict[period] = sum(self.production_plan[period])
										mongo_taglist_timePer.append({'object':period, 'date':date})
										mongo_datalist_timePer.append({'CMPT_ProductionPlan_Year': float(period_dict[period])})
									
							farm_dict[farm] = sum(self.production_plan[farm])
							mongo_taglist_timePer.append({'object':farm, 'date':date})
							mongo_datalist_timePer.append({'CMPT_ProductionPlan_Year': float(farm_dict[farm])})
							
							company_dict[company] += farm_dict[farm]
						
						mongo_taglist_timePer.append({'object':company, 'date':date})
						mongo_datalist_timePer.append({'CMPT_ProductionPlan_Year': float(company_dict[company])})
						
						all_dict[self.project] += company_dict[company]
				
					mongo_taglist_timePer.append({'object':self.project, 'date':date})
					mongo_datalist_timePer.append({'CMPT_ProductionPlan_Year': float(all_dict[self.project])})
				
				elif ('-' not in date) and (len(date.split('/')) == 2):
					
					for company in self.company_keys_dict:
						
						company_dict[company] = 0.0
						
						year, month = date.split('/')
						
						for farm in self.company_keys_dict[company]:
							
							if farm in self.wt_devKeys_dict:
								
								for period in self.wt_devKeys_dict[farm]:
									
									if self.production_plan.has_key(period):
									
										period_dict[period] = self.production_plan[period][int(month)-1]
										mongo_taglist_timePer.append({'object':period, 'date':date})
										mongo_datalist_timePer.append({'CMPT_ProductionPlan_Month': float(period_dict[period])})
									
							if farm in self.pv_devKeys_dict:
								
								for period in self.pv_devKeys_dict[farm]:
									
									if self.production_plan.has_key(period):
									
										period_dict[period] = self.production_plan[period][int(month)-1]
										mongo_taglist_timePer.append({'object':period, 'date':date})
										mongo_datalist_timePer.append({'CMPT_ProductionPlan_Month': float(period_dict[period])})
							
							farm_dict[farm] = self.production_plan[farm][int(month)-1]
							
							mongo_taglist_timePer.append({'object':farm, 'date':date})
							mongo_datalist_timePer.append({'CMPT_ProductionPlan_Month': float(farm_dict[farm])})
							
							company_dict[company] += farm_dict[farm]
						
						mongo_taglist_timePer.append({'object':company, 'date':date})
						mongo_datalist_timePer.append({'CMPT_ProductionPlan_Month': float(company_dict[company])})
						
						all_dict[self.project] += company_dict[company]
				
					mongo_taglist_timePer.append({'object':self.project, 'date':date})
					mongo_datalist_timePer.append({'CMPT_ProductionPlan_Month': float(all_dict[self.project])})
					
			self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
		except:
			raise Exception(traceback.format_exc())
	
	def CMPT_WindSpeed_Max(self, ex_dict):
		try:
			
			all_dict = {}
			
			project_list = []
			
			for company in self.company_keys_dict:
						
				company_list = []
				
				for farm in self.company_keys_dict[company]:
					
					farm_list = []
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							period_list = []
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								if ex_dict.has_key(dev):
									
									if ex_dict[dev].has_key('CMPT_WindSpeed_Avg'):
									
										dev_list = self.cum_avg(ex_dict[dev]['CMPT_WindSpeed_Avg'], 600)
										
										max_value = round(max(dev_list), 4) if dev_list <> [] else 0.0
										
										all_dict[dev] = max_value
									
										period_list.append(max_value)
							
							all_dict[period] = max(period_list) if period_list <> [] else 0.0
						
							farm_list.append(all_dict[period])
					
						all_dict[farm] = max(farm_list) if farm_list <> [] else 0.0
					
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							period_list = []
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								if ex_dict.has_key(dev):
									
									if ex_dict[dev].has_key('CMPT_WindSpeed_Avg'):
								
										dev_list = self.cum_avg(ex_dict[dev]['CMPT_WindSpeed_Avg'], 600)
										
										max_value = round(max(dev_list), 4) if dev_list <> [] else 0.0
										
										all_dict[dev] = max_value
									
										period_list.append(max_value)
							
							all_dict[period] = max(period_list) if period_list <> [] else 0.0
						
							farm_list.append(all_dict[period])
					
						all_dict[farm] = max(farm_list) if farm_list <> [] else 0.0
				
					company_list.append(all_dict[farm])
					
				all_dict[company] = max(company_list) if company_list <> [] else 0.0
					
				project_list.append(all_dict[company])
				
			all_dict[self.project] = max(project_list) if project_list <> [] else 0.0
			
			return all_dict
		
		except:
			
			raise Exception(traceback.format_exc())
			
	def cmpt_windMeasur(self, windSpeed_10m, tagName, option):
		try:
			data_dict = {}
			
			data_dict[self.project] = 0.0
				
			project_list = []
			
			for company in self.company_keys_dict:
				
				data_dict[company] = 0.0
				
				company_list = []
				for farm in self.company_keys_dict[company]:
					
					data_dict[farm] = 0.0
					
					windMeasur = farm+':WM01'
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							projectT, farmT, periodT = period.split(':')
							
							data_dict[period] = 0.0
								
							data_list = []
								
							if windSpeed_10m.has_key(windMeasur):
						
								if windSpeed_10m[windMeasur].has_key(tagName):
									
									values = windSpeed_10m[windMeasur][tagName].values()
									
									if values <> []:
										
										for value in values:
											
											if value <> '':
												
												data_list.append(float(value))
											else:
												data_list.append(0.0)
											
								if option == 'max':
									
									data_dict[period] = round(max(data_list), 4)
									
									
								elif option == 'avg':
									
									data_dict[period] = round(numpy.mean(data_list), 4)
									
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							projectT, farmT, periodT = period.split(':')
							
							data_dict[period] = 0.0
								
							data_list = []
								
							if windSpeed_10m.has_key(windMeasur):
						
								if windSpeed_10m[windMeasur].has_key(tagName):
									
									values = windSpeed_10m[windMeasur][tagName].values()
									
									if values <> []:
										
										for value in values:
											
											if value <> '':
												
												data_list.append(float(value))
											else:
												data_list.append(0.0)
											
								if option == 'max':
									
									data_dict[period] = round(max(data_list), 4)
									
								elif option == 'avg':
									
									data_dict[period] = round(numpy.mean(data_list), 4)
								
					if windSpeed_10m.has_key(windMeasur):
						
						if windSpeed_10m[windMeasur].has_key(tagName):
							
							values = windSpeed_10m[windMeasur][tagName].values()
							
							if values <> []:
								
								for value in values:
									
									if value <> '':
										
										data_list.append(float(value))
									else:
										data_list.append(0.0)
									
						if option == 'max':
							
							data_dict[farm] = round(max(data_list), 4)
							
						elif option == 'avg':
							
							data_dict[farm] = round(numpy.mean(data_list), 4)
								
						company_list.append(data_dict[farm])
				
				if option == 'max':
							
					data_dict[company] = round(max(company_list), 4)
					
				elif option == 'avg':
					
					data_dict[company] = round(numpy.mean(company_list), 4)
					
				project_list.append(data_dict[company])
				
			if option == 'max':
							
				data_dict[self.project] = round(max(project_list), 4)
				
			elif option == 'avg':
				
				data_dict[self.project] = round(numpy.mean(project_list), 4)
				
			return data_dict
		except:
			raise Exception(traceback.format_exc())
			
	def cmpt_windSpeed_10_Max(self, windMeasur, tagName):
		try:
			max_dict = {}
			
			project_list = []
			
			for company in self.company_keys_dict:
				
				max_dict[company] = 0.0
				
				company_list = []
				
				for farm in self.company_keys_dict[company]:
					
					max_dict[farm] = 0.0
					
					windM = farm+':WM01'
					
					max_farm = max(self.cum_avg(windMeasur[windM][tagName], 600))
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							projectT, farmT, periodT = period.split(':')
							
							max_dict[period] = 0.0
								
							
								
							if windMeasur.has_key(windM):
								
								max_value = max(self.cum_avg(windMeasur[windM][tagName], 600))
								
								max_dict[period] = round(max_value, 4)
								
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
						
							projectT, farmT, periodT = period.split(':')
							
							max_dict[period] = 0.0
								
							if windMeasur.has_key(windM):
								
								max_value = max(self.cum_avg(windMeasur[windM][tagName], 600))
								
								max_dict[period] = round(max_value, 4)
					
					max_dict[farm] = round(max_farm, 4)
					
					
					company_list.append(max_dict[farm])
				
				max_dict[company] = max(company_list)
				
				project_list.append(max_dict[company])
			
			max_dict[self.project] = max(project_list)
						
			return max_dict
		except:
			raise Exception(traceback.format_exc())
			
	def cum_avg(self, value_dict, timeLen):
		try:
			timeList = sorted(value_dict)
			avg_list = []
			for i in range(0, len(timeList)-1-timeLen, timeLen):
					
				acc_list = []
				
				for j in range(i, i + timeLen):
					
					value = float(value_dict[timeList[j]]) if value_dict[timeList[j]] <> '' else 0.0
					
					acc_list.append(value)
					
				avg = round(numpy.mean(acc_list), 4)
					
				avg_list.append(avg)
			
			if avg_list == []:
				return [0]
			else:
				return avg_list
		except:
			raise Exception(traceback.format_exc())
		
	def cmpt_airDensity(self, airDensity, tagName, option):
		try:
			data_dict = {}
			
			data_dict[self.project] = 0.0
				
			project_list = []
			
			for company in self.company_keys_dict:
				
				data_dict[company] = 0.0
				
				company_list = []
				
				for farmKey in self.company_keys_dict[company]:
					
					data_dict[farmKey] = 0.0
					
					data_list = []
					
					if airDensity.has_key(farmKey):
						
						if airDensity[farmKey].has_key(tagName):
							
							values = airDensity[farmKey][tagName].values()
							
							if values <> []:
								
								for value in values:
									
									if value <> '':
										
										data_list.append(float(value))
									else:
										data_list.append(0.0)
						
						if option == 'max':
							
							data_dict[farmKey] = round(max(data_list), 4) if data_list <> [] else 0.0
							
						elif option == 'avg':
							
							data_dict[farmKey] = round(numpy.mean(data_list), 4) if data_list <> [] else 0.0
						
						company_list.append(data_dict[farmKey])
				
				if option == 'max':
					
					data_dict[company] = round(max(company_list), 4) if company_list <> [] else 0.0
					
				elif option == 'avg':
					
					data_dict[company] = round(numpy.mean(company_list), 4) if company_list <> [] else 0.0
						
				project_list.append(data_dict[company])
			
			if option == 'max':
					
				data_dict[self.project] = round(max(project_list), 4) if company_list <> [] else 0.0
				
			elif option == 'avg':
				
				data_dict[self.project] = round(numpy.mean(project_list), 4) if project_list <> [] else 0.0
					
			return data_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_WindSpeed_Min(self, ex_dict):
		try:
			dict = self.cmpt_ex_day(ex_dict, 'CMPT_WindSpeed_Avg', 'min')
			project_list = []
			for company in self.company_keys_dict:
				
				company_list = []
				
				for farm in self.company_keys_dict[company]:
					
					farm_list = []
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							period_list = []
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								try:
								
									period_list.append(dict[dev])
									farm_list.append(dict[dev])
								except:
									period_list.append(0.0)
									
									farm_list.append(0.0)
									
							dict[period] = numpy.mean(period_list) if period_list<> [] else 0.0
					
					elif farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							period_list = []
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								try:
								
									period_list.append(dict[dev])
									farm_list.append(dict[dev])
								except:
									period_list.append(0.0)
									
									farm_list.append(0.0)
									
							dict[period] = numpy.mean(period_list) if period_list<> [] else 0.0
					
					dict[farm] = numpy.mean(farm_list) if farm_list <> [] else 0.0
					
					company_list.extend(farm_list)
				
				dict[company] = numpy.mean(company_list) if company_list <> [] else 0.0
				
				project_list.extend(company_list)
				
			dict[self.project] = numpy.mean(project_list) if project_list <> [] else 0.0
				
			
			return dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_WindSpeed_Avg(self, ex_dict):
		try:
			return self.cmpt_ex_day(ex_dict, 'CMPT_WindSpeed_Avg', 'avg')
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_WindSpeedValid(self, ex_dict):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.windSpeedValid_dev_day(ex_dict)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_WindEnerge(self, ex_dict, airDen_dict):
		try:
			all_day_dict = {}
			
			wt_dev_day = self.windEnerge_dev_day(ex_dict, airDen_dict)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, {}, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_ActPower_Max(self, ex_dict):
		try:
			all_dict = self.cmpt_ex_day(ex_dict, 'CMPT_ActPower', 'max')
			
			for farm in self.farmKeys_list:
				
				all_dict[farm] = round(self.getFarmPower(ex_dict, 'max')[farm], 4)
			
			return all_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_ActPower_Min(self, ex_dict):
		try:
			all_dict = self.cmpt_ex_day(ex_dict, 'CMPT_ActPower', 'min')
			
			for farm in self.farmKeys_list:
				
				all_dict[farm] = round(self.getFarmPower(ex_dict, 'min')[farm], 4)
			
			return all_dict
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_ActPower_Max_Tm(self, ex_dict):
		try:
			wt_dev_day = self.ex_dev_time(ex_dict, 'CMPT_ActPower', self.wt_devKey_list, 'max')
			
			pv_dev_day = self.ex_dev_time(ex_dict, 'CMPT_ActPower', self.pv_devKey_list, 'max')
			
			farmDict = self.getFarmPowerTime(ex_dict, 'max')
			
			return dict(wt_dev_day.items()+pv_dev_day.items()+farmDict.items())
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_ActPower_Min_Tm(self, ex_dict):
		try:
			wt_dev_day = self.ex_dev_time(ex_dict, 'CMPT_ActPower', self.wt_devKey_list, 'min')
			
			pv_dev_day = self.ex_dev_time(ex_dict, 'CMPT_ActPower', self.pv_devKey_list, 'min')
			
			farmDict = self.getFarmPowerTime(ex_dict, 'min')
			
			return dict(wt_dev_day.items()+pv_dev_day.items()+farmDict.items())
		except:
			raise Exception(traceback.format_exc())
	
	def CMPT_ActPower_Avg(self, ex_dict):
		try:
			all_dict = self.cmpt_mix_day(ex_dict, 'CMPT_ActPower', 'avg')
			
			for farm in self.farmKeys_list:
				
				all_dict[farm] = round(self.getFarmPower(ex_dict, 'avg')[farm], 4)
			
			return all_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_ExposeRatio(self, hours_end, unFaultHours, day_date):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getExposeRatio(hours_end, unFaultHours, day_date)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	def CMPT_TotRadiation(self, radiation_dict):
		try:
			all_day_dict = {}
			
			pv_dev_day = self.getRadiation(radiation_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day({}, pv_farm_day)
			
			all_day_dict = dict(pv_dev_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
			
	def CMPT_Radiation_Max(self, radiation_dict):
		try:
			all_day_dict = {}
			
			pv_dev_day = self.getMaxRadiation(radiation_dict)
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'max')
			
			project_day, company_day = self.ex_group2_day({}, pv_dev_day, 'max')
			
			all_day_dict = dict(pv_dev_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_Radiation_Avg(self, radiation_dict):
		try:
			all_day_dict = {}
			
			pv_dev_day = self.getMaxRadiation(radiation_dict)
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day({}, pv_dev_day, 'avg')
			
			all_day_dict = dict(pv_dev_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_CAH(self, userForGenerationHours, faultCnt):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getCAH(userForGenerationHours, faultCnt)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_MTBF(self, hours_end, faultCnt, day_date):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getMTBF(hours_end, faultCnt, day_date)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_OnGridHours(self):
		#与实发电小时数相同
		pass
		
	def CMPT_OnGridProduction(self, onGridDict_start, onGridDict_end, production):
		try:
			ong_dict = {}
			
			sum_project = 0.0
			
			for company in self.company_keys_dict:
				
				sum_company = 0.0
				
				for farm in self.company_keys_dict[company]:
					
					start = onGridDict_start[farm]
					
					end = onGridDict_end[farm]
					
					ong_dict[farm] = round(end - start, 4) if end >= start else 0.0
					
					if farm in self.wt_devKeys_dict:
					
						for period in self.wt_devKeys_dict[farm]:
						
							ong_dict[period] = round((production[period] / production[farm]) * ong_dict[farm], 4) if production[farm] <> 0.0 else 0.0
						
					if farm in self.pv_devKeys_dict:
					
						for period in self.pv_devKeys_dict[farm]:
						
							ong_dict[period] = round((production[period] / production[farm]) * ong_dict[farm], 4) if production[farm] <> 0.0 else 0.0
					
					sum_company += ong_dict[farm]
					
					ong_dict[company] = sum_company
					
				sum_project += ong_dict[company]
				
				ong_dict[self.project] = sum_project
				
			return ong_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_OtherProduction(self, production_start, production_end):
		try:
			ong_dict = {}
			
			sum_project = 0.0
			
			for company in self.company_keys_dict:
				
				sum_company = 0.0
				
				for farm in self.company_keys_dict[company]:
				
					start = production_start[farm]
					
					end = production_end[farm]
					
					ong_dict[farm] = round(end - start, 4) if end >= start else 0.0
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
						
							ong_dict[period] = round((self.Caps[period] / self.Caps[farm]) * (ong_dict[farm]), 4)
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
						
							ong_dict[period] = round((self.Caps[period] / self.Caps[farm]) * (ong_dict[farm]), 4)
					
					sum_company += ong_dict[farm]
					
					ong_dict[company] = sum_company
					
				sum_project += ong_dict[company]
				
				ong_dict[self.project] = sum_project
					
					
			return ong_dict
		except:
			raise Exception(traceback.format_exc())
			
	def getFarmPower(self, power_farm, ex):
		try:
			ong_dict = {}
			
			for farm in self.farmKeys_list:
					
				ong_dict[farm] = 0.0
					
				if power_farm[farm].has_key('CMPT_ActPower'):
					
					value_list = power_farm[farm]['CMPT_ActPower'].values() if power_farm[farm]['CMPT_ActPower'].values() <> [] else [0.0]
						
					value_list_new = []
						
					for value in value_list:
						
						if value <> '':
							
							value_list_new.append(float(value))
						
						else:
							
							value_list_new.append(0.0)
					
					if ex == 'max':
						
						max_value = max(value_list_new)
						
						ong_dict[farm] = round(max_value, 4)
						
					elif ex == 'min':
						
						min_value = min(value_list_new)
						
						ong_dict[farm] = round(min_value, 4)
						
					elif ex == 'avg':
						
						avg_value = numpy.mean(value_list_new)
						
						ong_dict[farm] = round(avg_value, 4) 
			
			return ong_dict
		except:
			raise Exception(traceback.format_exc())
		
	def getFarmPowerTime(self, power_farm, ex):
		try:
			ong_dict = {}
			
			for farm in self.farmKeys_list:
			
				if ex == 'max':
					
					if power_farm.has_key(farm) and power_farm[farm].has_key('CMPT_ActPower'):
					
						ong_dict[farm] = max(power_farm[farm]['CMPT_ActPower'].items(), key=lambda x: x[1])[0] if power_farm[farm]['CMPT_ActPower'] <> [] else ''
					else:
						
						ong_dict[farm] = ''
						
				elif ex == 'min':
					
					if power_farm.has_key(farm) and power_farm[farm].has_key('CMPT_ActPower'):
					
						ong_dict[farm] = min(power_farm[farm]['CMPT_ActPower'].items(), key=lambda x: x[1])[0] if power_farm[farm]['CMPT_ActPower'] <> [] else ''
					else:
						ong_dict[farm] = ''
						
			return ong_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_HouseRate(self, houseProduction, production_dict_day):
		try:
			houseRate = {}
			
			for company in self.company_keys_dict:
				
				for farm in self.company_keys_dict[company]:
				
					if farm in self.wt_devKeys_dict:
			
						if houseProduction.has_key(farm) and production_dict_day.has_key(farm):
							
							for period in self.wt_devKeys_dict[farm]:
								
								houseRate[period] = round(houseProduction[period] * 100 / production_dict_day[period], 4) if production_dict_day[period] <> 0.0 else 0.0
								
							houseRate[farm] = round(houseProduction[farm] * 100 / production_dict_day[farm], 4) if production_dict_day[farm] <> 0.0 else 0.0
						else:
							
							houseRate[farm] = 0.0
							
					if farm in self.pv_devKeys_dict:
						
						if houseProduction.has_key(farm) and production_dict_day.has_key(farm):
							
							for period in self.pv_devKeys_dict[farm]:
								
								houseRate[period] = round(houseProduction[period] * 100 / production_dict_day[period], 4) if production_dict_day[period] <> 0.0 else 0.0
								
							houseRate[farm] = round(houseProduction[farm] * 100 / production_dict_day[farm], 4) if production_dict_day[farm] <> 0.0 else 0.0
						
						else:
							
							houseRate[farm] = 0.0
				
				if houseProduction.has_key(company) and production_dict_day.has_key(company):
					houseRate[company] = round(houseProduction[company] * 100 / production_dict_day[company], 4) if production_dict_day[company] <> 0.0 else 0.0
				else:
					houseRate[company] = 0.0
			
			if houseProduction.has_key(self.project) and production_dict_day.has_key(self.project):
				
				houseRate[self.project] = round(houseProduction[self.project] * 100 / production_dict_day[self.project], 2) if production_dict_day[self.project] <> 0.0 else 0.0
			
			else:
				
				houseRate[self.project] = 0.0 
			
			
			return houseRate
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_RateOfHousePower(self, production_dict_day, onGrid, purProduction):
		try:
			housePowerRate = {}
			
			for company in self.company_keys_dict:
				
				for farm in self.company_keys_dict[company]:
				
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							HousePower = production_dict_day[period] - onGrid[period] + purProduction[period] if production_dict_day[period] - onGrid[period] + purProduction[period] >= 0.0 else 0.0
							
							housePowerRate[period] = round(HousePower * 100 /  production_dict_day[period], 4) if production_dict_day[period] <> 0.0 else 0.0 
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							HousePower = production_dict_day[period] - onGrid[period] + purProduction[period] if production_dict_day[period] - onGrid[period] + purProduction[period] >= 0.0 else 0.0
							
							housePowerRate[period] = round(HousePower * 100 /  production_dict_day[period], 4) if production_dict_day[period] <> 0.0 else 0.0 
						
					HousePower = production_dict_day[farm] - onGrid[farm] + purProduction[farm] if production_dict_day[farm] - onGrid[farm] + purProduction[farm] >= 0.0 else 0.0
						
					housePowerRate[farm] = round(HousePower * 100 /  production_dict_day[farm], 4) if production_dict_day[farm] <> 0.0 else 0.0 
				
				if production_dict_day.has_key(company) and onGrid.has_key(company) and purProduction.has_key(company):
					
					HousePower = production_dict_day[company] - onGrid[company] + purProduction[company] if production_dict_day[company] - onGrid[company] + purProduction[company] >= 0.0 else 0.0
					
					housePowerRate[company] = round(HousePower * 100 /  production_dict_day[company], 4) if production_dict_day[company] <> 0.0 else 0.0 
				
				else:
					
					housePowerRate[company] = 0.0
					
			if production_dict_day.has_key(self.project) and onGrid.has_key(self.project) and purProduction.has_key(self.project):
					
				HousePower = production_dict_day[self.project] - onGrid[self.project] + purProduction[self.project] if production_dict_day[self.project] - onGrid[self.project] + purProduction[self.project] >= 0.0 else 0.0
					
				housePowerRate[self.project] = round(HousePower * 100 /  production_dict_day[self.project], 4) if production_dict_day[self.project] <> 0.0 else 0.0 
			else:
				
				housePowerRate[self.project] = 0.0
			
			return housePowerRate
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_CompreHouseProduction(self, production_dict_day, onGrid, purProduction):
		try:
			comHouseProduction = {}
			
			for company in self.company_keys_dict:
				
				for farm in self.company_keys_dict[company]:
				
					if farm in self.wt_devKeys_dict:
					
						for period in self.wt_devKeys_dict[farm]:
							
							comHouseProduction[period] = round(production_dict_day[period] - onGrid[period] + purProduction[period], 4) if production_dict_day[period] - onGrid[period] + purProduction[period] >= 0 else 0.0
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							comHouseProduction[period] = round(production_dict_day[period] - onGrid[period] + purProduction[period], 4) if production_dict_day[period] - onGrid[period] + purProduction[period] >= 0 else 0.0
						
					comHouseProduction[farm] = round(production_dict_day[farm] - onGrid[farm] + purProduction[farm], 4) if production_dict_day[farm] - onGrid[farm] + purProduction[farm] >= 0 else 0.0
				
				comHouseProduction[company] = round(production_dict_day[company] - onGrid[company] + purProduction[company], 4) if production_dict_day[company] - onGrid[company] + purProduction[company] >= 0 else 0.0
				
			comHouseProduction[self.project] = round(production_dict_day[self.project] - onGrid[self.project] + purProduction[self.project], 4) if production_dict_day[self.project] - onGrid[self.project] + purProduction[self.project] >= 0 else 0.0
				
			return comHouseProduction
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_HouseLost(self, production_dict_day, onGrid, purProduction, houseProduction):
		try:
			houseLost = {}
			
			for company in self.company_keys_dict:
				
				for farm in self.company_keys_dict[company]:
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							houseLost[period] = round(production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period], 4) if production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period] >= 0 else 0.0
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							houseLost[period] = round(production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period], 4) if production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period] >= 0 else 0.0
				
					houseLost[farm] = round(production_dict_day[farm] - onGrid[farm] + purProduction[farm] - houseProduction[farm], 4) if production_dict_day[farm] - onGrid[farm] + purProduction[farm] - houseProduction[farm] >= 0 else 0.0
				
				houseLost[company] = round(production_dict_day[company]- onGrid[company] + purProduction[company] - houseProduction[company], 4) if production_dict_day[company] - onGrid[company] + purProduction[company] - houseProduction[company] >= 0 else 0.0
			
			houseLost[self.project] = round(production_dict_day[self.project]- onGrid[self.project] + purProduction[self.project] - houseProduction[self.project], 4) if production_dict_day[self.project] - onGrid[self.project] + purProduction[self.project] - houseProduction[self.project] >= 0 else 0.0
			
			
			return houseLost
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_HouseLostRate(self, production_dict_day, onGrid, purProduction, houseProduction):
		try:
			houseLostRate = {}
			
			for company in self.company_keys_dict:
				
				for farm in self.company_keys_dict[company]:
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
						
							houseLost = production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period] if production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period] >= 0 else 0.0
						
							houseLostRate[period] = round((houseLost / production_dict_day[period]) * 100, 4) if production_dict_day[period] <> 0.0 else 0.0
					
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
						
							houseLost = production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period] if production_dict_day[period] - onGrid[period] + purProduction[period] - houseProduction[period] >= 0 else 0.0
						
							houseLostRate[period] = round((houseLost / production_dict_day[period]) * 100, 4) if production_dict_day[period] <> 0.0 else 0.0
				
				
					houseLost = production_dict_day[farm] - onGrid[farm] + purProduction[farm] - houseProduction[farm] if production_dict_day[farm] - onGrid[farm] + purProduction[farm] - houseProduction[farm] >= 0 else 0.0
					
					houseLostRate[farm] = round((houseLost / production_dict_day[farm]) * 100, 4) if production_dict_day[farm] <> 0.0 else 0.0
				
				houseLost = production_dict_day[company] - onGrid[company] + purProduction[company] - houseProduction[company] if production_dict_day[company] - onGrid[company] + purProduction[company] - houseProduction[company] >= 0 else 0.0
					
				houseLostRate[company] = round((houseLost / production_dict_day[company]) * 100, 4) if production_dict_day[company] <> 0.0 else 0.0
				
			houseLost = production_dict_day[self.project] - onGrid[self.project] + purProduction[self.project] - houseProduction[self.project] if production_dict_day[self.project] - onGrid[self.project] + purProduction[self.project] - houseProduction[self.project] >= 0 else 0.0
					
			houseLostRate[self.project] = round((houseLost / production_dict_day[self.project]) * 100, 4) if production_dict_day[self.project] <> 0.0 else 0.0
				
			return houseLostRate
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_FaultCnt_Avg(self, faultCnt):
		try:
			fault_avg = {}
			
			count_company = 0.0
			
			for company in self.company_keys_dict:
				
				count_farm = 0.0
				
				for farm in self.company_keys_dict[company]:
					
					count_period = 0.0
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							fault_avg[period] = round(float(faultCnt[period]) / float(len(self.wt_devKeys_dict[farm][period])), 4) if len(self.wt_devKeys_dict[farm][period]) <> 0.0 else 0
							
							count_period += len(self.wt_devKeys_dict[farm][period])
							
						fault_avg[farm] = round(float(faultCnt[farm]) / float(count_period), 4)
						
						count_farm += count_period
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							fault_avg[period] = round(float(faultCnt[period]) / float(len(self.pv_devKeys_dict[farm][period])), 4) if len(self.pv_devKeys_dict[farm][period]) <> 0.0 else 0
							
							count_period += len(self.pv_devKeys_dict[farm][period])
							
						fault_avg[farm] = round(float(faultCnt[farm]) / float(count_period), 4)
						
						count_farm += count_period
						
				
				fault_avg[company] = round(float(faultCnt[company]) / float(count_farm), 4)
				
				count_company += count_farm
				
			fault_avg[self.project] = round(float(faultCnt[self.project]) / float(count_company), 4)
			
			return fault_avg
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_FaultHours_Avg(self, hours_end, day_date):
		try:
			fault_avg = {}
			count_company = 0.0
			faultHours_project = 0.0
			
			for company in self.company_keys_dict:
				
				count_farm = 0.0
				faultHours_company = 0.0
				
				for farm in self.company_keys_dict[company]:
					
					count_period = 0.0
					faultHours_farm = 0.0
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							faultHours_period = 0.0
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								if hours_end.has_key(dev):
									
									if hours_end[dev].has_key(day_date):
										
										faultHours_period += float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
										
							
							fault_avg[period] = round(faultHours_period / float(len(self.wt_devKeys_dict[farm][period])), 4) if len(self.wt_devKeys_dict[farm][period]) <> 0.0 else 0.0
							faultHours_farm += faultHours_period
							
							count_period += len(self.wt_devKeys_dict[farm][period])
							
						fault_avg[farm] = round(faultHours_farm / float(count_period), 4) if count_period <> 0.0 else 0.0
						count_farm += count_period
						faultHours_company += faultHours_farm
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							faultHours_period = 0.0
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								if hours_end.has_key(dev):
									
									if hours_end[dev].has_key(day_date):
										
										faultHours_period += float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
										
							
							fault_avg[period] = round(faultHours_period / float(len(self.pv_devKeys_dict[farm][period])), 4) if len(self.pv_devKeys_dict[farm][period]) <> 0.0 else 0.0
							faultHours_farm += faultHours_period
							
							count_period += len(self.pv_devKeys_dict[farm][period])
							
						fault_avg[farm] = round(faultHours_farm / float(count_period), 4) if count_period <> 0.0 else 0.0
						count_farm += count_period
						faultHours_company += faultHours_farm
						
				fault_avg[company] = round(faultHours_company / float(count_farm), 4) if count_farm <> 0.0 else 0.0
				count_company += count_farm
				faultHours_project += faultHours_company
			fault_avg[self.project] = round(faultHours_project / float(count_company), 4)
			
			return fault_avg
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_FaultStopLost_Avg(self, faultStopLost):
		try:
			fault_avg = {}
			count_company = 0.0
			for company in self.company_keys_dict:
				
				count_farm = 0.0
				
				for farm in self.company_keys_dict[company]:
					
					count_period = 0.0
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							fault_avg[period] = round(float(faultStopLost[period]) / float(len(self.wt_devKeys_dict[farm][period])), 4) if len(self.wt_devKeys_dict[farm][period]) <> 0.0 else 0.0
							
							count_period += len(self.wt_devKeys_dict[farm][period])
							
						fault_avg[farm] = round(float(faultStopLost[farm]) / float(count_period), 4) if count_period <> 0.0 else 0.0
						count_farm += count_period
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							fault_avg[period] = round(float(faultStopLost[period]) / float(len(self.pv_devKeys_dict[farm][period])), 4) if len(self.pv_devKeys_dict[farm][period]) <> 0.0 else 0.0
							
							count_period += len(self.pv_devKeys_dict[farm][period])
							
						fault_avg[farm] = round(float(faultStopLost[farm]) / float(count_period), 4) if count_period <> 0.0 else 0.0
						count_farm += count_period
						
				
				fault_avg[company] = round(float(faultStopLost[company]) / float(count_farm), 4) if count_farm <> 0.0 else 0.0
				count_company += count_farm
				
			fault_avg[self.project] = round(float(faultStopLost[self.project]) / float(count_company), 4) if count_company <> 0.0 else 0.0
			
			return fault_avg
		except:
			raise Exception(traceback.format_exc())
		
	def cmpt_ex_day_avg(self, ex_dict, tagName):
		try:
			fault_avg = {}
			
			count_company = 0.0
			
			wt_dict = self.ex_dev_day(ex_dict, tagName, self.wt_devKey_list, 'avg')
			
			pv_dict = self.ex_dev_day(ex_dict, tagName, self.pv_devKey_list, 'avg')
			
			all_dict = {}
			
			project_list = []
			
			for company in self.company_keys_dict:
				
				company_list = []
				
				for farm in self.company_keys_dict[company]:
					
					farm_list = []
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							period_list = []
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								all_dict[dev] = wt_dict[dev]
								
								period_list.append(wt_dict[dev])
								
								farm_list.append(wt_dict[dev])
								
								company_list.append(wt_dict[dev])
								
								project_list.append(wt_dict[dev])
								
							all_dict[period] = round(float(numpy.mean(period_list)), 4) if period_list <> [] else 0.0
							
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							period_list = []
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								all_dict[dev] = pv_dict[dev]
								
								period_list.append(pv_dict[dev])
								
								farm_list.append(pv_dict[dev])
								
								company_list.append(pv_dict[dev])
								
								project_list.append(pv_dict[dev])
								
							all_dict[period] = round(float(numpy.mean(period_list)), 4) if period_list <> [] else 0.0
							
							
					all_dict[farm] = round(float(numpy.mean(farm_list)), 4) if farm_list <> [] else 0.0
					
				all_dict[company] = round(float(numpy.mean(company_list)), 4) if company_list <> [] else 0.0
				
			all_dict[self.project] = round(float(numpy.mean(project_list)), 4) if project_list <> [] else 0.0
			
			return all_dict
		except:
			raise Exception(traceback.format_exc())
	def CMPT_FullHours(self, ex_dict):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day  = self.getFullHours(ex_dict)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	def CMPT_GenrationHours(self, ex_dict):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day  = self.getGenerHours(ex_dict)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	
	def CMPT_Hours(self, hours_end, day_date):
		try:
			runHours, faultHours, serviceHours, repairHours, readyHours, stopHours, unConnectHours, limPwrHours = {}, {}, {},{}, {},{}, {},{}
			
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
		
		
	def CMPT_RunRatio(self ,hours_end, day_date):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getRunRatio(hours_end, day_date)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_CompleteOfPlan(self, timestamp_month):
		try:
			mongo_taglist_timePer = []
			
			mongo_datalist_timePer = []
			
			all_dict = {}
			
			company_dict = {}
			
			farm_dict = {}
			
			year, month = timestamp_month.split('/') 
					
			monthlist = list('0'+str(i) for i in range(1, 10))
			monthlist.extend(list(str(i) for i in range(10, 13)))
			
			dateList = list(year+'/'+mon for mon in monthlist)
			
			for timestamp_month in dateList:
				
				project_plan_month = 0.0
			
				project_pro_month = 0.0
				
				for company in self.company_keys_dict:
					
					production = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_month], self.all_keyList, ['CMPT_Production_Month', 'CMPT_ProductionPlan_Month'])
					
					com_plan = 0.0
					
					com_production = 0.0
					
					for farm in self.company_keys_dict[company]:
						
						if farm in self.wt_devKeys_dict:
							
							production_period = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_month], self.wt_devKeys_dict[farm].keys(), ['CMPT_Production_Month', 'CMPT_ProductionPlan_Month'])
							
							for period in self.wt_devKeys_dict[farm]:
								
								if production_period.has_key(period):
									
									if production_period[period].has_key(timestamp_month):
										
										if production_period[period][timestamp_month].has_key('CMPT_ProductionPlan_Month'):
									
											period_plan = float(production_period[period][timestamp_month]['CMPT_ProductionPlan_Month']) 
											
											period_production = float(production_period[period][timestamp_month]['CMPT_Production_Month']) if production_period[period][timestamp_month].has_key('CMPT_Production_Month') else 0.0
											
											mongo_taglist_timePer.append({'object':period, 'date':timestamp_month})
											
											comletePlan = round(period_production *100 / period_plan , 2) if period_plan <> 0.0 else 0.0
											
											mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Month': comletePlan})
								
						if farm in self.pv_devKeys_dict:
							
							production_period = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_month], self.pv_devKeys_dict[farm].keys(), ['CMPT_Production_Month', 'CMPT_ProductionPlan_Month'])
							
							for period in self.pv_devKeys_dict[farm]:
								
								if production_period.has_key(period):
									
									if production_period[period].has_key(timestamp_month):
										
										if production_period[period][timestamp_month].has_key('CMPT_ProductionPlan_Month'):
										
										
											period_plan = float(production_period[period][timestamp_month]['CMPT_ProductionPlan_Month']) 
											
											period_production = float(production_period[period][timestamp_month]['CMPT_Production_Month']) if production_period[period][timestamp_month].has_key('CMPT_Production_Month') else 0.0
											
											mongo_taglist_timePer.append({'object':period, 'date':timestamp_month})
											
											comletePlan = round(period_production *100 / period_plan , 2) if period_plan <> 0.0 else 0.0
											
											mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Month': comletePlan})
						
						farm_plan = float(production[farm][timestamp_month]['CMPT_ProductionPlan_Month'])
						
						farm_production = float(production[farm][timestamp_month]['CMPT_Production_Month']) if production[farm][timestamp_month].has_key('CMPT_Production_Month') else 0.0
						
						comletePlan = round(farm_production *100 / farm_plan , 2) if farm_plan <> 0.0 else 0.0
						
						mongo_taglist_timePer.append({'object':farm, 'date':timestamp_month})
						mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Month': comletePlan})
						
						com_plan += farm_plan
						com_production += farm_production
					
					mongo_taglist_timePer.append({'object':company, 'date':timestamp_month})
					mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Month': round(com_production *100 / com_plan , 2)})
					
					project_plan_month += com_plan
					project_pro_month += com_production
				
				mongo_taglist_timePer.append({'object':self.project, 'date':timestamp_month})
				mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Month': round(project_pro_month *100 / project_plan_month , 2)})
						
				
			project_plan = 0.0
			
			project_pro = 0.0
			
			for company in self.company_keys_dict:
				
				production = self.mongo.getStatisticsByKeyList_DateList(self.project, [year], self.all_keyList, ['CMPT_Production_Year', 'CMPT_ProductionPlan_Year'])
				
				#com_plan = 0.0
				
				com_production = 0.0
				
				for farm in self.company_keys_dict[company]:
					
					if farm in self.wt_devKeys_dict:
							
						production_period = self.mongo.getStatisticsByKeyList_DateList(self.project, [year], self.wt_devKeys_dict[farm].keys(), ['CMPT_Production_Year', 'CMPT_ProductionPlan_Year'])
						
						for period in self.wt_devKeys_dict[farm]:
							
							if production_period.has_key(period):
									
								if production_period[period].has_key(year):
									
									if production_period[period][year].has_key('CMPT_ProductionPlan_Year'):
									
										
										period_plan = float(production_period[period][year]['CMPT_ProductionPlan_Year']) 
										period_production = float(production_period[period][year]['CMPT_Production_Year']) if production_period[period][year].has_key('CMPT_Production_Year') else 0.0
										
										comletePlan = round(period_production *100 / period_plan , 2) if period_plan <> 0.0 else 0.0
										
										mongo_taglist_timePer.append({'object':period, 'date':year})
										mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Year': comletePlan})
					
					if farm in self.pv_devKeys_dict:
							
						production_period = self.mongo.getStatisticsByKeyList_DateList(self.project, [year], self.pv_devKeys_dict[farm].keys(), ['CMPT_Production_Year', 'CMPT_ProductionPlan_Year'])
						
						for period in self.pv_devKeys_dict[farm]:
							
							if production_period.has_key(period):
									
								if production_period[period].has_key(year):
									
									if production_period[period][year].has_key('CMPT_ProductionPlan_Year'):
										
										period_plan = float(production_period[period][year]['CMPT_ProductionPlan_Year']) 
										period_production = float(production_period[period][year]['CMPT_Production_Year']) if production_period[period][year].has_key('CMPT_Production_Year') else 0.0
										
										comletePlan = round(period_production *100 / period_plan , 2) if period_plan <> 0.0 else 0.0
										
										mongo_taglist_timePer.append({'object':period, 'date':year})
										mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Year': comletePlan})
					
					
					farm_plan = float(production[farm][year]['CMPT_ProductionPlan_Year'])
					farm_production = float(production[farm][year]['CMPT_Production_Year'])
					
					comletePlan = round(farm_production *100 / farm_plan , 2) if farm_plan <> 0.0 else 0.0
					
					mongo_taglist_timePer.append({'object':farm, 'date':year})
					mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Year': comletePlan})
					
				com_plan = float(production[company][year]['CMPT_ProductionPlan_Year'])
				com_production = float(production[company][year]['CMPT_Production_Year'])
				
				mongo_taglist_timePer.append({'object':company, 'date':year})
				mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Year': round(com_production *100 / com_plan , 2)})
				
			project_plan =  float(production[self.project][year]['CMPT_ProductionPlan_Year'])
			project_pro = float(production[self.project][year]['CMPT_Production_Year'])
			
			mongo_taglist_timePer.append({'object':self.project, 'date':year})
			mongo_datalist_timePer.append({'CMPT_CompleteOfPlan_Year': round(project_pro *100 / project_plan , 2)})
				
			#print mongo_taglist_timePer, mongo_datalist_timePer
				
			self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
				
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_ReadyHours(self, hours_end):
		try:
			return self.cmpt_sum_day({}, hours_end, 'CMPT_ReadyHours')
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_StopHours(self, hours_end):
		try:
			return self.cmpt_sum_day({}, hours_end, 'CMPT_StopHours')
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_FaultHours(self, hours_end):
		try:
			return self.cmpt_sum_day({}, hours_end, 'CMPT_FaultHours')
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_UnFaultHours(self, hours_end, day_date):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getUnFaultHours(hours_end, day_date) 
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_ServiceHours(self, hours_end):
		try:
			return self.cmpt_sum_day({}, hours_end, 'CMPT_ServiceHours')
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_UnConnectHours(self, hours_end):
		try:
			return self.cmpt_sum_day({}, hours_end, 'CMPT_UnConnectHours')
		except:
			raise Exception(traceback.format_exc())
	def CMPT_UnConnectRatio(self, hours_end, day_date):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getUnConnectRatio(hours_end, day_date)
			
			wt_farm_day, wt_period_day = self.ex_group1_day(wt_dev_day, self.wt_devKeys_dict, 'avg')
			
			pv_farm_day, pv_period_day = self.ex_group1_day(pv_dev_day, self.pv_devKeys_dict, 'avg')
			
			project_day, company_day = self.ex_group2_day(wt_dev_day, pv_dev_day, 'avg')
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_LimPwrHours(self, hours_end):
		try:
			return self.cmpt_sum_day({}, hours_end, 'CMPT_LimPwrHours')
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_RepairHours(self, hours_end):
		try:
			return self.cmpt_sum_day({}, hours_end, 'CMPT_RepairHours')
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_LimPwrRate(self, dispatchStopLostPower, productionTheory_dict_day):
		try:
			return  self.getLimPwrRate(dispatchStopLostPower, productionTheory_dict_day)
		except:
			raise Exception(traceback.format_exc())
	
			
	def CMPT_StopCnt(self, stopTimeLen_wt, stopTimeLen_pv):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day  = self.getStatusCnt(stopTimeLen_wt, stopTimeLen_pv)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day(wt_farm_day, pv_farm_day)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
		
	def CMPT_DispatchStopLostPower(self, limPwrRun, limPwrShut):
		try:
			all_day_dict = {}
			
			for key in self.all_keyList:
				
				run = float(limPwrRun[key]) if limPwrRun.has_key(key) else 0.0
				
				shut = float(limPwrShut[key]) if limPwrShut.has_key(key) else 0.0
				
				all_day_dict[key] = run + shut
				
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_Lost(self, devDict_wt, devDict_pv, ex_dict):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day  = self.getStatusLost(devDict_wt, devDict_pv, ex_dict)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day(wt_farm_day, pv_farm_day)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
		
	def CMPT_LostN(self, devDict_wt, devDict_pv, ex_dict, starttime, endtime):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day  = self.getStatusLostN(devDict_wt, devDict_pv, ex_dict, starttime, endtime)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day(wt_farm_day, pv_farm_day)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
	
	
	
	def CMPT_ProductionLost(self, dispatchStopLostPower, unConnectLost, faultStopLost):
		try:
			all_day_dict = {}
			
			wt_dev_day, pv_dev_day = self.getProductionLost(dispatchStopLostPower, unConnectLost, faultStopLost)
			
			wt_farm_day, wt_period_day = self.sum_group1_day(wt_dev_day, self.wt_devKeys_dict)
			
			pv_farm_day, pv_period_day = self.sum_group1_day(pv_dev_day, self.pv_devKeys_dict)
			
			project_day, company_day = self.sum_group2_day(wt_farm_day, pv_farm_day)
			
			all_day_dict = dict(wt_dev_day.items()+pv_dev_day.items()+wt_farm_day.items()+wt_period_day.items()+pv_farm_day.items()+pv_period_day.items()+project_day.items()+company_day.items())
			
			return all_day_dict
		except:
			raise Exception(traceback.format_exc())
			
	def getQualifiedRate(self, accRate):
		
		try:
			qualifiedRate = {}
			
			for farm in self.farmKeys_list:
				
				qualifiedRate[farm] = 0.0
				
				farm_sum = 0.0
				
				if accRate.has_key(farm) and accRate[farm].has_key('CMPT_QualifiedRate'):
					
					if accRate[farm]['CMPT_QualifiedRate'] <> {}:
					
						for date in accRate[farm]['CMPT_QualifiedRate']:
							
							
							farm_sum += float(accRate[farm]['CMPT_QualifiedRate'][date]) if accRate[farm]['CMPT_QualifiedRate'][date] <> '' else 0.0
							
				qualifiedRate[farm] = round(farm_sum * 100 / 96, 4)
			
			return qualifiedRate
		
		except:
			raise Exception(traceback.format_exc())
		
			
	def CMPT_AccuracyRate_DQ(self, ex_dict, value_dict):
		try:
			accuracyRate_DQ_Dict = {}
			
			for farm in self.farmKeys_list:
				
				if ex_dict.has_key(farm) and ex_dict[farm].has_key('CMPT_ActPower'):
				
					actPowerSum = self.acc_timePer(ex_dict[farm]['CMPT_ActPower'], 900, 1)
				else:
					actPowerSum = {}
				
				powerForecast_DQ = value_dict[farm]['WTUR_PowerForecast_DQ'] if value_dict.has_key(farm) and value_dict[farm].has_key('WTUR_PowerForecast_DQ') else {}
				
				sum = 0.0
				
				if actPowerSum <> {} and powerForecast_DQ <> {}:
				
					for timeTemp in actPowerSum:
						
						actPowerSum_temp= float(actPowerSum[timeTemp]) if actPowerSum[timeTemp] <> '' else 0.0
						
						powerForecast_DQ_temp= float(powerForecast_DQ[timeTemp]) if powerForecast_DQ[timeTemp] <> '' else 0.0
						
						sum += numpy.square(actPowerSum_temp - powerForecast_DQ_temp if actPowerSum_temp >= powerForecast_DQ_temp else 0.0 )
					
				cap = self.capDicts[farm]
				
				ratio = numpy.sqrt(sum)/(cap*len(actPowerSum)) if cap*len(actPowerSum) <> 0 else 0.0
				
				accuracyRate_DQ_Dict[farm] = round((1 - ratio)*100, 4) if ratio <= 1 else 100
				
			return accuracyRate_DQ_Dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_AccuracyRate_CDQ(self, value_dict, acc):
		try:
			accuracyRate_CDQ_Dict = {}
			
			for farm in self.farmKeys_list:
				
				sum = 0.0
				
				cap = self.capDicts[farm]
				
				if value_dict.has_key(farm) and value_dict[farm].has_key('CMPT_ActPower'):
					
					actPowerSum = self.acc_timePer(value_dict[farm]['CMPT_ActPower'], 900, 1)
					
					#print actPowerSum, '222222222222'
					
					powerForecast_CDQ = acc[farm+':PF01']['WTUR_PowerForecast_CDQ'] if acc.has_key(farm+':PF01') and acc[farm+':PF01'].has_key('WTUR_PowerForecast_CDQ') else 0.0
					
					#print powerForecast_CDQ, '3333333333'
					
					if actPowerSum <> {} and powerForecast_CDQ <> {}:
					
						for timeTemp in actPowerSum:
							
							actPowerSum_temp = float(actPowerSum[timeTemp]) if actPowerSum[timeTemp] <> '' else 0.0
							
							powerForecast_CDQ_temp = float(powerForecast_CDQ[timeTemp]) if powerForecast_CDQ[timeTemp] <> '' else 0.0
							
							sum += numpy.square((actPowerSum_temp - powerForecast_CDQ_temp * 1000) / cap)
							
				sum = sum / 96.0
				
				ratio = numpy.sqrt(sum)
				
				#print ratio
				
				accuracyRate_CDQ_Dict[farm] = round((1 - ratio)*100, 4) if ratio <= 1 else 0.0
				
			return accuracyRate_CDQ_Dict
		except:
			raise Exception(traceback.format_exc())
			
	def CMPT_Rose(self, ex_dict, windDir_dict):
		try:
			return self.getWindDir_rose(ex_dict, windDir_dict)
		except:
			raise Exception(traceback.format_exc())
			
	def acc_timePer(self, value_dict, timeLen, ratio= None):
		try:
			timeList = sorted(value_dict)
			
			acc_dict = {}
			
			for i in range(0, len(timeList)-1-timeLen, timeLen):
					
				acc_sum = 0.0
				
				for j in range(i, i + timeLen):
					
					acc_sum += float(value_dict[timeList[j]]) if value_dict[timeList[j]] <> '' else 0.0
					
				acc_sum = acc_sum/timeLen
					
				if ratio is not None:		
				
					acc_dict[timeList[i]] = round(acc_sum/ratio, 4)
				
				else:
					
					acc_dict[timeList[i]] = round(acc_sum, 4)
			
			return acc_dict
		except:
			raise Exception(traceback.format_exc())
	def getCapByFarms(self):
		try:
			cap_dict = {}
			
			for farm in self.farmKeys_list:
			
				cap_dict[farm] = self.mongo.getCapacityByFarm(self.project, farm.split(':')[1])
			
			return cap_dict
		except:
			raise Exception(traceback.format_exc())
		
	def getRadiation(self, radiation_dict):
		try:
			radition = {}
			
			for dev in self.pv_devKey_list:
				
				sum = 0.0
				
				for date in radiation_dict[dev]:
				
					sum += radiation_dict[dev][date] if radiation_dict[dev][date] <> '' else 0.0
					
				radition[dev] = round(sum / (3600 * 3.6), 4)
			
			return radition
		except:
			raise Exception(traceback.format_exc())
			
	def getMaxRadiation(self, radiation_dict):
		try:
			radition = {}
			
			temp = {}
			
			for dev in self.pv_devKey_list:
				
				temp[dev] = []
				
				for date in radiation_dict[dev]:
					
					temp[dev].append(radiation_dict[dev][date] if radiation_dict[dev][date] <> '' else 0.0)
					
			for dev in self.pv_devKey_list:
				
				radition[dev] = round(max(temp[dev]), 4) if temp[dev] <> [] else 0.0
				
			return radition
		except:
			raise Exception(traceback.format_exc())
			
	def getGenerateRate(self, production_dict_day, windEnerge, totRadiation):
		try:
			generateRate = {}
			
			for company in self.company_keys_dict:
			
				for farm in self.company_keys_dict[company]:
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								if production_dict_day.has_key(dev):
								
									generateRate[dev] =  round(production_dict_day[dev] / windEnerge[dev], 4) if windEnerge[dev] <> 0.0 else 0.0
								else:
									
									generateRate[dev] = 0.0
							
							if generateRate.has_key(period):
								
								generateRate[period] =  round(production_dict_day[period] / windEnerge[period], 4) if windEnerge[period] <> 0.0 else 0.0
							
							else:
								
								generateRate[period] =  0.0
						
						if generateRate.has_key(farm):
							
							generateRate[farm] =  round(production_dict_day[farm] / windEnerge[farm], 4) if windEnerge[farm] <> 0.0 else 0.0
						
						else:
							
							generateRate[farm] =  0.0
					
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								if generateRate.has_key(dev):
									
									generateRate[dev] =  round(production_dict_day[dev] / totRadiation[dev], 4) if totRadiation[dev] <> 0.0 else 0.0
								else:
									generateRate[dev] = 0.0
							if generateRate.has_key(period):
								generateRate[period] =  round(production_dict_day[period] / totRadiation[period], 4) if totRadiation[period] <> 0.0 else 0.0
							else:
								generateRate[period] =  0.0
						if generateRate.has_key(farm):
							generateRate[farm] = round(production_dict_day[farm] / totRadiation[farm], 4) if totRadiation[farm] <> 0.0 else 0.0
						else:
							generateRate[farm] = 0.0
			
			return generateRate
		except:
			raise Exception(traceback.format_exc())
			
	def getStatusHours(self, devDict_wt, devDict_pv):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			hours_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			hours_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
					
				if devDict_wt[dev] <> []:
					
					seconds = 0
					
					for timeT in devDict_wt[dev]:
						
						start, end = timeT.split(',')
						
						startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
						
						endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
						
						seconds += (endT - startT).total_seconds()
						
					hours_wt[dev] = round(float(seconds) / 3600.0, 4)
					
				else:
					
					hours_wt[dev] = 0.0
			
			for dev in self.pv_devKey_list:
					
				if devDict_pv[dev] <> []:
					
					seconds = 0
					
					for timeT in devDict_pv[dev]:
						
						start, end = timeT.split(',')
					
						startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
						
						endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
						
						seconds += (endT - startT).total_seconds()
						
					hours_pv[dev] = round(float(seconds) / 3600.0, 4)
					
				else:
					
					hours_pv[dev] = 0.0
			
			
			
			return dict(hours_wt.items()+hours_pv.items())
		except:
			raise Exception(traceback.format_exc())
		
	def getStatusCnt(self, devDict_wt, devDict_pv):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			
			lost_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			lost_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
					
				if devDict_wt[dev] <> []:
					
					lost_wt[dev] = len(devDict_wt[dev])
					
				else:
					
					lost_wt[dev] = 0
			
			for dev in self.pv_devKey_list:
					
				if devDict_pv[dev] <> []:
					
					lost_pv[dev] = len(devDict_pv[dev])
					
				else:
					
					lost_pv[dev] = 0
						
			return lost_wt, lost_pv
		except:
			raise Exception(traceback.format_exc())
			
		
		#(hourCnt_dict, 4, 5, 1)
	def getFaultTimeLen(self, hourCnt_dict, status, status2, timeLen):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			devDict_wt = {}
			devDict_pv = {}
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = []
			
			
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = []
			
			if hourCnt_dict == {}:
				
				return devDict_wt, devDict_pv
			
			else:
				
				for dev in self.wt_devKey_list:
					
					timeList = sorted(hourCnt_dict[dev]['CMPT_StandardStatus']) if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') else []
					
					i = 0
					
					while i <= len(timeList) - 2:
						
						if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') and hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[i]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]])) in [status, status2]:
							
							timeT = timeList[i]
							
							count = 1
							
							for j in range(i, len(timeList)-1, 1):
								
								if hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[j+1]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]] <> '':
								
									if (int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]])) in [status, status2]) and j < len(timeList) - 2:
										
										count += 1
										i += 1
									else:
										
										if count >= timeLen:
											
											devDict_wt[dev].append(timeT+','+timeList[j])
											
											i += 1
											
											break
												
										else:
											i += 1
								else:
									
									i += 1
						
						else:
							i += 1
				
				for dev in self.pv_devKey_list:
					
					timeList = sorted(hourCnt_dict[dev]['CMPT_StandardStatus']) if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') else []
						
					i = 0
					
					while i <= len(timeList) - 2:
						
						if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') and hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[i]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]])) in [status, status2]:
							
							timeT = timeList[i]
							
							count = 1
							
							for j in range(i, len(timeList)-1, 1):
								
								if hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[j+1]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]] <> '':
									
									if (int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]])) in [status, status2]) and j < len(timeList) - 2:
										
										count += 1
										i += 1
									else:
										
										if count >= timeLen:
											
											devDict_pv[dev].append(timeT+','+timeList[j])
											
											i += 1
											
											break
												
										else:
											i += 1
								else:
									
									i += 1
						else:
							i += 1
								
				return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
			
	def getStopTimeLen(self, hourCnt_dict, status):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			devDict_wt = {}
			devDict_pv = {}
			status2 = 0
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = []
				
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = []
			
			if hourCnt_dict == {}:
				
				return devDict_wt, devDict_pv
			
			else:
				
				for dev in self.wt_devKey_list:
					
					timeList = sorted(hourCnt_dict[dev]['CMPT_StandardStatus']) if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') else []
					
					i = 0
					
					if len(timeList) > 2:
					
						while i <= len(timeList) - 2:
							
							if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') and hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[i]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]])) == status:
								
								timeT = timeList[i]
								
								count = 1
								
								for j in range(i, len(timeList)-1, 1):
									
									if hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]])) == status and j < len(timeList) - 2:
											
										count += 1
										i += 1
										
									else:
										
										if count >= 60:
											
											devDict_wt[dev].append(timeT+','+timeList[j])
											
										i += 1
										
										break
								
							else:
								i += 1
								
				for dev in self.pv_devKey_list:
					
					timeList = sorted(hourCnt_dict[dev]['CMPT_StandardStatus']) if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') else []
					
					i = 0
					
					if len(timeList) > 2:
					
						while i <= len(timeList) - 2:
							
							if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') and hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[i]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]])) == status:
								
								timeT = timeList[i]
								
								count = 1
								
								for j in range(i, len(timeList)-1, 1):
									
									if hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]])) == status and j < len(timeList) - 2:
											
										count += 1
										i += 1
										
									else:
										
										if count >= 60:
											
											devDict_pv[dev].append(timeT+','+timeList[j])
											
										i += 1
										
										break
								
							else:
								i += 1
				
				return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getStopTimeLenN(self, hourCnt_dict, status, starttime, endtime):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			devDict_wt = {}
			devDict_pv = {}
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = []
				
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = []
			
			if hourCnt_dict == {}:
				
				return devDict_wt, devDict_pv
			
			else:
				
				end = datetime.datetime.strptime(endtime,'%Y-%m-%d %H:%M:%S')
				
				start = datetime.datetime.strptime(starttime,'%Y-%m-%d %H:%M:%S')
				
				start_stamp = time.mktime(start.timetuple())
				
				end_stamp = time.mktime(end.timetuple())
				
				timeListT = list(range(int(start_stamp), int(end_stamp)+1, 1))
				
				timeList = []
				
				for timeTemp in timeListT:
							
					timestruct = time.localtime(timeTemp)
						
					timeT = time.strftime('%Y-%m-%d %H:%M:%S',timestruct)
					
					timeList.append(timeT)
					
				for dev in self.wt_devKey_list:
					
					i = 0
					
					while i <= len(timeList) - 2:
						
						if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') and hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[i]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]])) == status:
						
							timeT = timeList[i]
							
							count = 1
							
							for j in range(i, len(timeList)-1, 1):
								
								if hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]])) == status and j < len(timeList) - 2:
									
									count += 1
									
									i += 1
								
								else:
									
									if count >= 60:
										
										devDict_wt[dev].append(timeT+','+timeList[j])
										
									i += 1
									
									break
										
							
						else:
							i += 1
							
				for dev in self.pv_devKey_list:
					
					i = 0
					
					while i <= len(timeList) - 2:
						
						if hourCnt_dict.has_key(dev) and hourCnt_dict[dev].has_key('CMPT_StandardStatus') and hourCnt_dict[dev]['CMPT_StandardStatus'].has_key(timeList[i]) and hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[i]])) == status:
						
							timeT = timeList[i]
							
							count = 1
							
							for j in range(i, len(timeList)-1, 1):
								
								if hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]] <> '' and int(float(hourCnt_dict[dev]['CMPT_StandardStatus'][timeList[j+1]])) == status and j < len(timeList) - 2:
									
									count += 1
									
									i += 1
								
								else:
									
									if count >= 60:
										
										devDict_pv[dev].append(timeT+','+timeList[j])
										
									i += 1
									
									break
										
							
						else:
							i += 1
				
				return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getStatusLostInfo(self, devDict_wt, devDict_pv, ex_dict):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			lost_wt = {}
			
			lost_pv = {}
			
			for dev in self.wt_devKey_list:
					
				if ex_dict.has_key(dev):
				
					timeList = sorted(ex_dict[dev]['CMPT_ActPower']) if ex_dict[dev].has_key('CMPT_ActPower') else []
				
					i = 0
						
					if devDict_wt[dev] <> []:
						
						lost_wt[dev] = {}
						
						for timeLen in devDict_wt[dev]:
							
							lost_wt[dev][timeLen] = 0.0
							
							start , end = timeLen.split(',')
							
							startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
							
							endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
							
							while i < len(timeList):
								
								timeT = datetime.datetime.strptime(timeList[i],'%Y-%m-%d %H:%M:%S')
								
								if startT <= timeT:
									
									if endT >= timeT:
										
										if ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]] <> '' and ex_dict[dev]['CMPT_ActPower'][timeList[i]] <> '':
										
											lost_wt[dev][timeLen] = round((float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]])) / 3600.0, 4) if (float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]])) >= 0.0 else 0.0
									else:
										
										break
										
								i += 1
						
			for dev in self.pv_devKey_list:
				
				if ex_dict.has_key(dev):
					
					timeList = sorted(ex_dict[dev]['CMPT_ActPower']) if ex_dict[dev].has_key('CMPT_ActPower') else []
				
					i = 0
						
					if devDict_pv[dev] <> []:
						
						lost_pv[dev] = {}
						
						for timeLen in devDict_pv[dev]:
							
							lost_pv[dev][timeLen] = 0.0
							
							start , end = timeLen.split(',')
							
							startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
							
							endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
							
							while i < len(timeList):
								
								timeT = datetime.datetime.strptime(timeList[i],'%Y-%m-%d %H:%M:%S')
								
								if startT <= timeT:
									
									if endT >= timeT:
										
										if ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]] <> '' and ex_dict[dev]['CMPT_ActPower'][timeList[i]] <> '':
										
											lost_pv[dev][timeLen]= round((float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]])) / 3600.0, 4) if (float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]])) >= 0.0 else 0.0
									else:
										
										break
										
								i += 1
						
			return dict(lost_wt.items()+lost_pv.items())
		except:
			raise Exception(traceback.format_exc())
		
	def getStatusLost(self, devDict_wt, devDict_pv, ex_dict):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			lost_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			lost_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			lost_wt_n = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			lost_pv_n = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
					
				if ex_dict.has_key(dev):
					
					timeList = sorted(ex_dict[dev]['CMPT_ActPower']) if ex_dict[dev].has_key('CMPT_ActPower') else []
					
					i = 0
						
					if devDict_wt[dev] <> []:
						
						for timeLen in devDict_wt[dev]:
							
							start , end = timeLen.split(',')
							
							startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
							
							endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
							
							while i < len(timeList):
								
								timeT = datetime.datetime.strptime(timeList[i],'%Y-%m-%d %H:%M:%S')
								
								if startT <= timeT:
									
									if endT >= timeT:
										
										if ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]] <> '' and ex_dict[dev]['CMPT_ActPower'][timeList[i]] <> '':
										
											lost_wt[dev] += float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) if float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) >= 0.0 else 0.0
									else:
										
										break
										
								i += 1
						
			for dev in self.pv_devKey_list:
				if ex_dict.has_key(dev):
				
					timeList = sorted(ex_dict[dev]['CMPT_ActPower']) if ex_dict[dev].has_key('CMPT_ActPower') else []
					
					i = 0
						
					if devDict_pv[dev] <> []:
						
						for timeLen in devDict_pv[dev]:
							
							start , end = timeLen.split(',')
							
							startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
							
							endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
							
							while i < len(timeList):
								
								timeT = datetime.datetime.strptime(timeList[i],'%Y-%m-%d %H:%M:%S')
								
								if startT <= timeT:
									
									if endT >= timeT:
										
										if ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]] <> '' and ex_dict[dev]['CMPT_ActPower'][timeList[i]] <> '':
										
											lost_pv[dev] += float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) if float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) >= 0.0 else 0.0
									else:
										
										break
										
								i += 1
						
			for dev in lost_wt:
				
				lost_wt_n[dev] = round(lost_wt[dev] / 3600.0, 4) if lost_wt[dev] <> [] else 0.0
				
			for dev in lost_pv:
				
				lost_pv_n[dev] = round(lost_pv[dev] / 3600.0, 4) if lost_pv[dev] <> [] else 0.0
				
			return lost_wt_n, lost_pv_n
		except:
			raise Exception(traceback.format_exc())
	
	
	
	def getStatusLostN(self, devDict_wt, devDict_pv, ex_dict, starttime, endtime):
		try:
			#--'1':u'运行','2':u'待机','3':u'正常停机','4':u'故障','5':u'维护','6':u'通讯中断','7':u'限电','8':u'检修'--
			lost_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			lost_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			lost_wt_n = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			lost_pv_n = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			timeList = []
			
			end = datetime.datetime.strptime(endtime,'%Y-%m-%d %H:%M:%S')
				
			start = datetime.datetime.strptime(starttime,'%Y-%m-%d %H:%M:%S')
			
			start_stamp = time.mktime(start.timetuple())
			
			end_stamp = time.mktime(end.timetuple())
			
			timeListT = list(range(int(start_stamp), int(end_stamp)+1, 1))
			
			for timeTemp in timeListT:
						
				timestruct = time.localtime(timeTemp)
					
				timeT = time.strftime('%Y-%m-%d %H:%M:%S',timestruct)
				
				timeList.append(timeT)
			
			for dev in self.wt_devKey_list:
					
				i = 0
					
				if devDict_wt[dev] <> []:
					
					for timeLen in devDict_wt[dev]:
						
						start , end = timeLen.split(',')
						
						startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
						
						endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
						
						while i < len(timeList):
							
							timeT = datetime.datetime.strptime(timeList[i],'%Y-%m-%d %H:%M:%S')
							
							if startT <= timeT:
								
								if endT >= timeT:
									
									if ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]] <> '' and ex_dict[dev]['CMPT_ActPower'][timeList[i]] <> '':
									
										lost_wt[dev] += float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) if float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) >= 0.0 else 0.0
								else:
									
									break
									
							i += 1
						
			for dev in self.pv_devKey_list:
					
				i = 0
					
				if devDict_pv[dev] <> []:
					
					for timeLen in devDict_pv[dev]:
						
						start , end = timeLen.split(',')
						
						startT = datetime.datetime.strptime(start,'%Y-%m-%d %H:%M:%S')
						
						endT = datetime.datetime.strptime(end,'%Y-%m-%d %H:%M:%S')
						
						while i < len(timeList):
							
							timeT = datetime.datetime.strptime(timeList[i],'%Y-%m-%d %H:%M:%S')
							
							if startT <= timeT:
								
								if endT >= timeT:
									
									if ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]] <> '' and ex_dict[dev]['CMPT_ActPower'][timeList[i]] <> '':
									
										lost_pv[dev] += float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) if float(ex_dict[dev]['CMPT_ActPowerTheory'][timeList[i]]) - float(ex_dict[dev]['CMPT_ActPower'][timeList[i]]) >= 0.0 else 0.0
								else:
									
									break
									
							i += 1
						
			for dev in lost_wt:
				
				lost_wt_n[dev] = round(lost_wt[dev] / 3600.0, 4) if lost_wt[dev] <> [] else 0.0
				
			for dev in lost_pv:
				
				lost_pv_n[dev] = round(lost_pv[dev] / 3600.0, 4) if lost_pv[dev] <> [] else 0.0
				
			return lost_wt_n, lost_pv_n
		except:
			raise Exception(traceback.format_exc())
	
	def getUnFaultHours(self, hours_end, day_date):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						faultHours = round(float(hours_end[dev][day_date]['CMPT_FaultHours_Day']), 4) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
						
						devDict_wt[dev] = 24.0 - faultHours if faultHours <= 24.0 else 0.0
				
			for dev in self.pv_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						faultHours = round(float(hours_end[dev][day_date]['CMPT_FaultHours_Day']), 4) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
				
						devDict_pv[dev] = 9.0 - faultHours if faultHours <=  9.0 else 0.0 #
				
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
		
	def getExposeRatio(self, hours_end, unFaultHours, day_date):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date) and unFaultHours.has_key(dev):
						
						runHours = float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
						
						devDict_wt[dev] = round(runHours *100 / unFaultHours[dev], 4) if unFaultHours[dev] <> 0.0 else 0.0
			
			for dev in self.pv_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date) and unFaultHours.has_key(dev):
						
						runHours = float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
						
						devDict_pv[dev] = round(runHours *100 / unFaultHours[dev], 4) if unFaultHours[dev] <> 0.0 else 0.0
				
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getRunRatio(self ,hours_end, day_date):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						runHours = float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
						
						devDict_wt[dev] = round(runHours *100 / 24.0 ,4)
				
			for dev in self.pv_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						runHours = float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
						
						devDict_pv[dev] = round(runHours *100 / 9.0 ,4)
			
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
		
	def getUseRatio(self ,useHours):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = round(useHours[dev] *100 / 24.0 ,4)
				
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = round(useHours[dev] *100 / 9.0 ,4)
			
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
		
	def getOutPowerRatio(self, useHours, hours_end, day_date):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						runHours = float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
						
						devDict_wt[dev] = round(useHours[dev] *100 / runHours, 4) if runHours <> 0.0 else 0.0
				
			for dev in self.pv_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						runHours = float(hours_end[dev][day_date]['CMPT_RunHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RunHours_Day') else 0.0
						
						devDict_pv[dev] = round(useHours[dev] *100 / runHours, 4) if runHours <> 0.0 else 0.0
			
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getUserForGenerationHours(self, hours_end, day_date):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						faultHours = float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
						
						serviceHours = float(hours_end[dev][day_date]['CMPT_ServiceHours_Day']) if hours_end[dev][day_date].has_key('CMPT_ServiceHours_Day') else 0.0
						
						#repairHours = float(hours_end[dev][day_date]['CMPT_RepairHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RepairHours_Day') else 0.0
						
						devDict_wt[dev] = round(24.0 - faultHours, 4) if 24.0 - faultHours >= 0 else 0.0
						
			for dev in self.pv_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						faultHours = float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
						
						serviceHours = float(hours_end[dev][day_date]['CMPT_ServiceHours_Day']) if hours_end[dev][day_date].has_key('CMPT_ServiceHours_Day') else 0.0
						
						#repairHours = float(hours_end[dev][day_date]['CMPT_RepairHours_Day']) if hours_end[dev][day_date].has_key('CMPT_RepairHours_Day') else 0.0
						
						devDict_pv[dev] = round(9.0 - faultHours, 4) if 9.0 - faultHours >= 0 else 0.0
				
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getUserForGenerRatio(self, userForGenerationHours):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = round(userForGenerationHours[dev] *100 / 24.0 ,4)
				
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = round(userForGenerationHours[dev] *100 / 9.0 ,4)
			
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getLimPwrRate(self, dispatchStopLostPower, productionTheory_dict_day):
		try:
			all_dict = {}
			
			all_dict[self.project] = 0.0
			
			for company in self.company_keys_dict:
						
				all_dict[company] = 0.0
				
				for farm in self.company_keys_dict[company]:
					
					all_dict[farm] = 0.0
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							all_dict[period] = 0.0
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								all_dict[dev] = round(dispatchStopLostPower[dev] / productionTheory_dict_day[dev], 4) if productionTheory_dict_day[dev] <> 0.0 else 0.0
								
							all_dict[period] = round(dispatchStopLostPower[period] / productionTheory_dict_day[period], 4) if productionTheory_dict_day[period] <> 0.0 else 0.0
						
						all_dict[farm] = round(dispatchStopLostPower[farm] / productionTheory_dict_day[farm], 4) if productionTheory_dict_day[farm] <> 0.0 else 0.0
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							all_dict[period] = 0.0
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								all_dict[dev] = round(dispatchStopLostPower[dev] / productionTheory_dict_day[dev], 4) if productionTheory_dict_day[dev] <> 0.0 else 0.0
								
							all_dict[period] = round(dispatchStopLostPower[period] / productionTheory_dict_day[period], 4) if productionTheory_dict_day[period] <> 0.0 else 0.0
					
						all_dict[farm] = round(dispatchStopLostPower[farm] / productionTheory_dict_day[farm], 4) if productionTheory_dict_day[farm] <> 0.0 else 0.0
				
				all_dict[company] = round(dispatchStopLostPower[company] / productionTheory_dict_day[company], 4) if productionTheory_dict_day[company] <> 0.0 else 0.0
					
			all_dict[self.project] = round(dispatchStopLostPower[self.project] / productionTheory_dict_day[self.project], 4) if productionTheory_dict_day[self.project] <> 0.0 else 0.0
			
			return all_dict
		except:
			raise Exception(traceback.format_exc())
		
	def getUnConnectRatio(self, hours_end, day_date):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						unConnectHours = float(hours_end[dev][day_date]['CMPT_UnConnectHours_Day']) if hours_end[dev][day_date].has_key('CMPT_UnConnectHours_Day') else 0.0
				
						devDict_wt[dev] = round(unConnectHours * 100 / 24.0 , 4)
				
			for dev in self.pv_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						unConnectHours = float(hours_end[dev][day_date]['CMPT_UnConnectHours_Day']) if hours_end[dev][day_date].has_key('CMPT_UnConnectHours_Day') else 0.0
						
						devDict_pv[dev] = round(unConnectHours *100 / 9.0 ,4)
				
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getFullHours(self, ex_dict):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			wt_dict = dict((dev, 0.0) for dev in self.wt_devKey_list)
			pv_dict = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if ex_dict.has_key(dev) and ex_dict[dev].has_key('CMPT_ActPower'):
				
					for date in ex_dict[dev]['CMPT_ActPower']:
						
						if ex_dict[dev]['CMPT_ActPower'][date]  >= self.Caps[dev] or ex_dict[dev]['CMPT_ActPower'][date] <= (1.2 * self.Caps[dev]):
							
							wt_dict[dev] += 1
			
			for dev in self.pv_devKey_list:
				
				if ex_dict.has_key(dev) and ex_dict[dev].has_key('CMPT_ActPower'):
				
					for date in ex_dict[dev]['CMPT_ActPower']:
						
						if ex_dict[dev]['CMPT_ActPower'][date]  >= self.Caps[dev] or ex_dict[dev]['CMPT_ActPower'][date] <= (1.2 * self.Caps[dev]):
							
							pv_dict[dev] += 1
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = round(wt_dict[dev] / 3600.0, 4)
				
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = round(pv_dict[dev] / 3600.0, 4)
				
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getGenerHours(self, ex_dict):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			wt_dict = dict((dev, 0.0) for dev in self.wt_devKey_list)
			pv_dict = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if ex_dict.has_key(dev) and ex_dict[dev].has_key('CMPT_ActPower'):
				
					for date in ex_dict[dev]['CMPT_ActPower']:
						
						if ex_dict[dev]['CMPT_ActPower'][date]  > 0 or ex_dict[dev]['CMPT_ActPower'][date] <= (1.2 * self.Caps[dev]):
							
							wt_dict[dev] += 1
			
			for dev in self.pv_devKey_list:
				
				if ex_dict.has_key(dev) and ex_dict[dev].has_key('CMPT_ActPower'):
				
					for date in ex_dict[dev]['CMPT_ActPower']:
						
						if ex_dict[dev]['CMPT_ActPower'][date]  > 0 or ex_dict[dev]['CMPT_ActPower'][date] <= (1.2 * self.Caps[dev]):
							
							pv_dict[dev] += 1
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = round(wt_dict[dev] / 3600.0, 4)
				
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = round(pv_dict[dev] / 3600.0, 4)
				
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
	def getMTBF(self, hours_end, faultCnt, day_date):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						faultHours = float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
						
						devDict_wt[dev] = round((24.0 - faultHours) / faultCnt[dev] , 4)if faultCnt[dev] <> 0.0 else 24.0
				
			for dev in self.pv_devKey_list:
				
				if hours_end.has_key(dev):
					
					if hours_end[dev].has_key(day_date):
						
						faultHours = float(hours_end[dev][day_date]['CMPT_FaultHours_Day']) if hours_end[dev][day_date].has_key('CMPT_FaultHours_Day') else 0.0
						
						devDict_pv[dev] = round((9.0 - faultHours) / faultCnt[dev] , 4) if faultCnt[dev] <> 0.0 else 9.0
			
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
			
	def getCAH(self, userForGenerationHours, faultCnt):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = round(userForGenerationHours[dev] / faultCnt[dev], 4) if faultCnt[dev] <> 0.0 else userForGenerationHours[dev]
				
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] =  round(userForGenerationHours[dev] / faultCnt[dev], 4) if faultCnt[dev] <> 0.0 else userForGenerationHours[dev]
				
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
		
	def getAvailability(self, faultHours):
		try:
			availability = {}
			
			project_count = 0
			
			for company in self.company_keys_dict:
				
				company_count = 0
				
				for farm in self.company_keys_dict[company]:
					
					farm_count = 0
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
							
							period_count = 0
							
							for dev in self.wt_devKeys_dict[farm][period]:
								
								period_count += 1
								
								if faultHours.has_key(dev):
									
									availability[dev] = round((24.0 - faultHours[dev]) * 100/ 24.0, 4) 
							
							availability[period] =  round((24.0 * period_count - faultHours[period]) * 100/ (24.0* period_count), 4)
							
							farm_count += period_count
							
						availability[farm] =  round((24.0* farm_count - faultHours[farm]) * 100/ (24.0 * farm_count), 4)
						
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							period_count = 0
							
							for dev in self.pv_devKeys_dict[farm][period]:
								
								period_count += 1
								
								if faultHours.has_key(dev):
								
									availability[dev] = round((9.0 - faultHours[dev]) * 100/ 9.0, 4)
								
							availability[period] = round((9.0 * period_count - faultHours[period]) * 100/ (9.0* period_count), 4)
							
							farm_count += period_count
							
						availability[farm] = round((9.0 * farm_count - faultHours[farm]) * 100/ (9.0* farm_count), 4)
				
					company_count += farm_count
				
				availability[company] = round((24.0 * company_count - faultHours[company]) * 100/ (24.0* company_count), 4)
				
				project_count += company_count
				
			availability[self.project] = round((24.0 * project_count - faultHours[self.project]) * 100/ (24.0* project_count), 4)
			
				
			return availability
		except:
			raise Exception(traceback.format_exc())
			
			
	def getAvailability_dev(self, faultHours):
		try:
			availability = {}
			
			for dev in self.devKeys_list:
				
				availability[dev] = round((24.0 - faultHours[dev]) * 100/ 24.0, 4) if faultHours.has_key(dev) else 100.0
			
			return availability
		except:
			raise Exception(traceback.format_exc())
			
			
			
	def getProductionLost(self, dispatchStopLostPower, unConnectLost, faultStopLost):
		try:
			devDict_wt = dict((dev, 0.0) for dev in self.wt_devKey_list)
			
			devDict_pv = dict((dev, 0.0) for dev in self.pv_devKey_list)
			
			for dev in self.wt_devKey_list:
				
				devDict_wt[dev] = round(dispatchStopLostPower[dev] + unConnectLost[dev] + faultStopLost[dev], 4)
			
			for dev in self.pv_devKey_list:
				
				devDict_pv[dev] = round(dispatchStopLostPower[dev] + unConnectLost[dev] + faultStopLost[dev], 4)
			
			return devDict_wt, devDict_pv
		except:
			raise Exception(traceback.format_exc())
		
	def getWindDir_rose(self, ex_dict, windDir_dict):
		try:
			tag_dir = 'CMPT_WindDir'
			
			tag_wind = 'CMPT_WindSpeed_Avg'
			
			wind_dir_list = ['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15']
		
			wind_speed_group_list = ['0-3','3-4','4-5','5-6','6-7','7-8','8-9','9-10','10-11','11-12','12-13','13-14','14-15','15-25','25-99']
			
			wind_speed_group_dict = dict((wind_speed_group.split('-')) for wind_speed_group in wind_speed_group_list )
			
			wt_farms_dict = {}
			
			for farm in self.wt_devKeys_dict:
			
				farm_dict = {}
				
				wind_dir_dict = {}
				
				for wind_dir in wind_dir_list:
					
					wind_dir_dict[wind_dir] = dict((wind_speed , 0) for wind_speed in wind_speed_group_dict)
				
				sum = 0
				
				for period in self.wt_devKeys_dict[farm]:
					
					for devKey in self.wt_devKeys_dict[farm][period]:
						
						count =0
						
						timeList = sorted(windDir_dict[devKey][tag_dir])
						
						for date in timeList:
							
							if (windDir_dict[devKey][tag_dir].has_key(date)) and (windDir_dict[devKey][tag_dir][date] <> '') and (ex_dict[devKey][tag_wind][date] <> ''):
								
								wind_dir = int(float(windDir_dict[devKey][tag_dir][date]))
								
								wind_speed = float(ex_dict[devKey][tag_wind][date])
									
								for wind_speed_min in wind_speed_group_dict:
									
									if wind_speed >= float(wind_speed_min) and wind_speed < float(wind_speed_group_dict[wind_speed_min]):
										
										count += 1
										
										if str(wind_dir) in wind_dir_list:
										
											wind_dir_dict[str(wind_dir)][wind_speed_min] += 1
										
											break
						
						sum += count
						
				for wind_dir in wind_dir_list:
					
					farm_dict[wind_dir] = {}
					
					for wind_speed in wind_speed_group_dict:
						
						ratio = (float(wind_dir_dict[wind_dir][wind_speed])/float(sum)) if sum > 0 else 0.0
						
						farm_dict[wind_dir][str(wind_speed)+'-'+str(wind_speed_group_dict[wind_speed])] = ratio
				
				wt_farms_dict[farm] = str(farm_dict)
				
			return wt_farms_dict
		except:
			raise Exception(traceback.format_exc())
	
	def getLimPwrLostTime(self, agc_dict, ex_dict, keyList):
		try:
			
			
			key_dict = {}
			
			for key in keyList:
				
				timeList = sorted(agc_dict[key]) if agc_dict.has_key(key) else []
				
				key_dict[key] = []
				
				i = 0
					
				while i <= len(timeList) - 2:
					
					if agc_dict.has_key(key) and agc_dict[key].has_key(timeList[i]) and  agc_dict[key][timeList[i]] <> '' and ex_dict.has_key(key) and ex_dict[key].has_key('CMPT_ActPower') and ex_dict[key]['CMPT_ActPower'].has_key(timeList[i]) and ex_dict[key]['CMPT_ActPower'][timeList[i]] <> '':
						
						if int(float(agc_dict[key][timeList[i]])) > ex_dict[key]['CMPT_ActPower'][timeList[i]]:
							
							timeT = timeList[i]
							
							count = 1
							
							for j in range(i, len(timeList)-1, 1):
								
								if agc_dict[key].has_key(timeList[j+1]) and ex_dict[key]['CMPT_ActPower'].has_key(timeList[j+1]):
								
									if agc_dict[key][timeList[j+1]] <> '' and int(float(agc_dict[key][timeList[j+1]])) > ex_dict[key]['CMPT_ActPower'][timeList[j+1]] and j < len(timeList) - 2:
										
										count += 1
										
										i += 1
									
									else :
										
										if count >= 60:
											
											key_dict[key].append(timeT+','+timeList[j])
												
										i += 1
										
										break
								else:
									
									i += 1
						else:
							
							i += 1
					else:
						i += 1
				
			return key_dict
		except:
			raise Exception(traceback.format_exc())
			
	def getLimPwrPowerMax(self, key_dict, keyList, ex_dict):
		try:
			max_power = {}
			
			max_power_data = {}
			
			for key in keyList:
				
				max_power_data[key] = 0.0
				
				max_power[key] = []
				
				timeList = key_dict[key] if key_dict.has_key(key) else []
				
				if timeList <> []:
					
					for time in timeList:
						
						start, end = time.split(',')
						
						value_list = self.max_power(start, end, ex_dict[key])
						
						max_value = max(value_list) if value_list <> [] else 0.0
						
						max_power[key].append(max_value)
						
				max_power_data[key] = max(max_power[key]) if max_power[key] <> [] else 0.0
					
				return max_power_data
		except:
			raise Exception(traceback.format_exc())
			
	def max_power(self, start, end, ex_dict):
		try:
			timeList = sorted(ex_dict['CMPT_ActPower']) if ex_dict.has_key('CMPT_ActPower') else []
			
			data_list = []
			
			i = 0
			
			while i < len(timeList):
				
				if timeList[i] >= start and timeList[i] <= end:
					
					value_act = float(ex_dict['CMPT_ActPower'][timeList[i]]) if ex_dict['CMPT_ActPower'][timeList[i]] <> '' else 0.0
					
					value_the = float(ex_dict['CMPT_ActPowerTheory'][timeList[i]]) if ex_dict['CMPT_ActPowerTheory'][timeList[i]] <> '' else 0.0
					
					data_list.append(value_the - value_act) if (value_the - value_act) >= 0.0 else 0.0
					
				i += 1
			
			return data_list
		except:
			raise Exception(traceback.format_exc())
			
	def limPwrLostFarmPeriod(self, limPwrLostTimeLen, hourCnt_dict, ex_dict):
		try:
			lost_dict = {}
			
			for farm in self.wt_devKeys_dict:
				
				lost_dict[farm] = []
				
				if limPwrLostTimeLen[farm] <> []:
					
					keyList = []
					
					for period in self.wt_devKeys_dict[farm]:
						
						lost_dict[period] = []
						
						keyList.extend(self.wt_devKeys_dict[farm][period])
						
						if limPwrLostTimeLen[period] <> []:
							
							lost_dict[period] = self.limPwrByKeys(period, self.wt_devKeys_dict[farm][period], limPwrLostTimeLen[period], hourCnt_dict, ex_dict)
							
					lost_dict[farm] = self.limPwrByKeys(farm, keyList, limPwrLostTimeLen[farm], hourCnt_dict, ex_dict)
							
			for farm in self.pv_devKeys_dict:
				
				keyList = []
				
				lost_dict[farm] = []
				
				if limPwrLostTimeLen[farm] <> []:
				
					for period in self.pv_devKeys_dict[farm]:
						
						lost_dict[period] = []
						
						keyList.extend(self.pv_devKeys_dict[farm][period])
						
						if limPwrLostTimeLen[period] <> []:
							
							lost_dict[period] = self.limPwrByKeys(period, self.pv_devKeys_dict[farm][period], limPwrLostTimeLen[period], hourCnt_dict, ex_dict)
							
					lost_dict[farm] = self.limPwrByKeys(farm, keyList, limPwrLostTimeLen[farm], hourCnt_dict, ex_dict)
							
			return lost_dict
		except:
			raise Exception(traceback.format_exc())
			
	def limPwrByKeys(self, key, devList, limPwrLostTimeLen, hourCnt_dict, ex_dict):
		try:
			lost_list = {}
			
			for timeT in limPwrLostTimeLen:
				
				starttime, endtime = timeT.split(',')
				
				limPwrTimeLen_wt, limPwrTimeLen_pv = self.getStopTimeLenN(hourCnt_dict, 7, starttime, endtime)
				repairTimeLen_wt, repairTimeLen_pv = self.getStopTimeLenN(hourCnt_dict, 8, starttime, endtime)
				limPwrRun = self.CMPT_LostN(repairTimeLen_wt, repairTimeLen_pv, ex_dict, starttime, endtime)
				limPwrShut = self.CMPT_LostN(limPwrTimeLen_wt, limPwrTimeLen_pv, ex_dict, starttime, endtime)
				limPwrLost = self.CMPT_DispatchStopLostPower(limPwrRun, limPwrShut)
				
				lost_list[timeT] = float(limPwrLost[key])
				
			return lost_list
		except:
			raise Exception(traceback.format_exc())
			
	def subStation(self, start_data, end_data):
		try:
			data = {}
			
			for farm in self.farmKeys_list:
			
				start = start_data[farm]
				
				end = end_data[farm]
				
				data[farm] = end - start if end >= start else 0.0
			
			return data 
		except:
			raise Exception(traceback.format_exc())
			
	def cmpt_actPower_timeLen(self, ex_dict):
		try:
			data = {}
			
			for company in self.company_keys_dict:
			
				for farm in self.company_keys_dict[company]:
					
					if farm in self.wt_devKeys_dict:
						
						for period in self.wt_devKeys_dict[farm]:
						
							data[period] = self.actPower_timeLen(period, ex_dict)
					
					if farm in self.pv_devKeys_dict:
						
						for period in self.pv_devKeys_dict[farm]:
							
							data[period] = self.actPower_timeLen(period, ex_dict)
					
					data[farm] = self.actPower_timeLen(farm, ex_dict)
					
				data[company] = self.actPower_timeLen(company, ex_dict)
					
					
			data[self.project] = self.actPower_timeLen(self.project, ex_dict)
					
			return data
		
		except:
			
			raise Exception(traceback.format_exc())
	
	def actPower_timeLen(self, key, ex_dict):
		
		data = {'100':0.0, '75':0.0, '50':0.0, '25':0.0, '0':0.0}
							
		data_len = {'100':0.0, '75':0.0, '50':0.0, '25':0.0, '0':0.0}
		
		timeList = sorted(ex_dict[key]['CMPT_ActPower']) if ex_dict.has_key(key) and ex_dict[key].has_key('CMPT_ActPower') else []
				
		if timeList <> []:
			
			for timeT in timeList:
				
				value = float(ex_dict[key]['CMPT_ActPower'][timeT]) if ex_dict[key]['CMPT_ActPower'][timeT] <> '' else 0.0
				
				if value >= self.Caps[key]:
					
					data_len['100'] += 1
					
				elif value >= 0.75 * self.Caps[key] and value < self.Caps[key]:
					
					data_len['75'] += 1
					
				elif value >= 0.5 * self.Caps[key] and value < 0.75 *self.Caps[key]:
					
					data_len['50'] += 1
				
				elif value >= 0.25 * self.Caps[key] and value < 0.5 *self.Caps[key]:
					
					data_len['25'] += 1
				
				else:
					
					data_len['0'] += 1
			
		data = {'100':round((data_len['100'] / 3600.0), 4), '75':round((data_len['75'] / 3600.0), 4), '50':round((data_len['50'] / 3600.0), 4), '25':round((data_len['25'] / 3600.0), 4), '0':round((data_len['0'] / 3600.0), 4)}
		
		
		return data
	
	def getRateOfHousePowerTimper(self, timestamp_week, timestamp_month, timestamp_year):
		
		data_dict = {}
		
		week_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_week], self.group, ['CMPT_CompreHouseProduction_Week', 'CMPT_Production_Week'])
		
		month_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_month], self.group, ['CMPT_CompreHouseProduction_Month', 'CMPT_Production_Month'])
		
		year_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_year], self.group, ['CMPT_CompreHouseProduction_Year', 'CMPT_Production_Year'])
		
		mongo_taglist_timePer, mongo_datalist_timePer = [], []
		
		for key in self.group:
			
			if week_value.has_key(key):
				
				if week_value[key].has_key(timestamp_week):
					
					if week_value[key][timestamp_week].has_key('CMPT_CompreHouseProduction_Week') and week_value[key][timestamp_week].has_key('CMPT_Production_Week'):
					
						ongrid_week = float(week_value[key][timestamp_week]['CMPT_CompreHouseProduction_Week']) if week_value[key][timestamp_week]['CMPT_CompreHouseProduction_Week'] <> '' else 0.0
						
						prod_week = float(week_value[key][timestamp_week]['CMPT_Production_Week']) if week_value[key][timestamp_week]['CMPT_Production_Week'] <> '' else 0.0
						
						houseRate = round(ongrid_week * 100 / prod_week, 4) if prod_week <> 0.0 else 0.0
						
						mongo_taglist_timePer.append({'object':key, 'date':timestamp_week})
						mongo_datalist_timePer.append({'CMPT_RateOfHousePower_Week':houseRate})
						
			
			if month_value.has_key(key):
				
				if month_value[key].has_key(timestamp_month):
					
					if month_value[key][timestamp_month].has_key('CMPT_CompreHouseProduction_Month') and month_value[key][timestamp_month].has_key('CMPT_Production_Month'):
					
						ongrid_month= float(month_value[key][timestamp_month]['CMPT_CompreHouseProduction_Month']) if month_value[key][timestamp_month]['CMPT_CompreHouseProduction_Month'] <> '' else 0.0
						
						prod_month = float(month_value[key][timestamp_month]['CMPT_Production_Month'])if month_value[key][timestamp_month]['CMPT_Production_Month'] <> '' else 0.0
						
						houseRate = round(ongrid_month * 100 / prod_month, 4) if prod_month <> 0.0 else 0.0
						
						mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
						mongo_datalist_timePer.append({'CMPT_RateOfHousePower_Month':houseRate})
							
			if year_value.has_key(key):
				
				if year_value[key].has_key(timestamp_year):
					
					if year_value[key][timestamp_year].has_key('CMPT_CompreHouseProduction_Year') and year_value[key][timestamp_year].has_key('CMPT_Production_Year'):
					
						ongrid_year= float(year_value[key][timestamp_year]['CMPT_CompreHouseProduction_Year']) if year_value[key][timestamp_year]['CMPT_CompreHouseProduction_Year'] <> '' else 0.0
						
						prod_year = float(year_value[key][timestamp_year]['CMPT_Production_Year']) if year_value[key][timestamp_year]['CMPT_Production_Year'] <> '' else 0.0
						
						houseRate = round(ongrid_year * 100 / prod_year, 4) if prod_year <> 0.0 else 0.0
						
						mongo_taglist_timePer.append({'object':key, 'date':timestamp_year})
						
						mongo_datalist_timePer.append({'CMPT_RateOfHousePower_Year':houseRate})
				
		self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
		
	def getHouseRateTimper(self, timestamp_week, timestamp_month, timestamp_year):
		
		data_dict = {}
		
		week_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_week], self.group, ['CMPT_HouseProduction_Week', 'CMPT_Production_Week'])
		
		month_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_month], self.group, ['CMPT_HouseProduction_Month', 'CMPT_Production_Month'])
		
		year_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_year], self.group, ['CMPT_HouseProduction_Year', 'CMPT_Production_Year'])
		
		mongo_taglist_timePer, mongo_datalist_timePer = [], []
		
		for key in self.group:
			
			if week_value.has_key(key):
				
				if week_value[key].has_key(timestamp_week):
					
					if week_value[key][timestamp_week].has_key('CMPT_HouseProduction_Week') and week_value[key][timestamp_week].has_key('CMPT_Production_Week'):
					
						ongrid_week = float(week_value[key][timestamp_week]['CMPT_HouseProduction_Week']) if week_value[key][timestamp_week]['CMPT_HouseProduction_Week'] <> '' else 0.0
						
						prod_week = float(week_value[key][timestamp_week]['CMPT_Production_Week']) if week_value[key][timestamp_week]['CMPT_Production_Week'] <> '' else 0.0
						
						houseRate = round(ongrid_week * 100 / prod_week, 4) if prod_week <> 0.0 else 0.0
						
						mongo_taglist_timePer.append({'object':key, 'date':timestamp_week})
						mongo_datalist_timePer.append({'CMPT_HouseRate_Week':houseRate})
						
			
			if month_value.has_key(key):
				
				if month_value[key].has_key(timestamp_month):
					
					if month_value[key][timestamp_month].has_key('CMPT_HouseProduction_Month') and month_value[key][timestamp_month].has_key('CMPT_Production_Month'):
					
						ongrid_month= float(month_value[key][timestamp_month]['CMPT_HouseProduction_Month']) if month_value[key][timestamp_month]['CMPT_HouseProduction_Month'] <> '' else 0.0
						
						prod_month = float(month_value[key][timestamp_month]['CMPT_Production_Month'])if month_value[key][timestamp_month]['CMPT_Production_Month'] <> '' else 0.0
						
						houseRate = round(ongrid_month * 100 / prod_month, 4) if prod_month <> 0.0 else 0.0
						
						mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
						mongo_datalist_timePer.append({'CMPT_HouseRate_Month':houseRate})
							
			if year_value.has_key(key):
				
				if year_value[key].has_key(timestamp_year):
					
					if year_value[key][timestamp_year].has_key('CMPT_HouseProduction_Year') and year_value[key][timestamp_year].has_key('CMPT_Production_Year'):
					
						ongrid_year= float(year_value[key][timestamp_year]['CMPT_HouseProduction_Year']) if year_value[key][timestamp_year]['CMPT_HouseProduction_Year'] <> '' else 0.0
						
						prod_year = float(year_value[key][timestamp_year]['CMPT_Production_Year']) if year_value[key][timestamp_year]['CMPT_Production_Year'] <> '' else 0.0
						
						houseRate = round(ongrid_year * 100 / prod_year, 4) if prod_year <> 0.0 else 0.0
						
						mongo_taglist_timePer.append({'object':key, 'date':timestamp_year})
						
						mongo_datalist_timePer.append({'CMPT_HouseRate_Year':houseRate})
				
		self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
		
	def getAvailabilityTimper(self, timestamp_week, timestamp_month, timestamp_year, date):
		data_dict = {}
		
		#week_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_week], self.group, ['CMPT_UserForGenerationHours_Week'])
		
		month_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_month], self.group, ['CMPT_UserForGenerationHours_Month'])
		
		year_value = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_year], self.group, ['CMPT_UserForGenerationHours_Year'])
		
		mongo_taglist_timePer, mongo_datalist_timePer = [], []
		
		#print date
		year, month, dateT = date.split('/')
		month_days = 0
		if int(month) <> 1:
			
			month_days = calendar.monthrange(int(year), int(month)-1)[1]
			
			month_days += int(dateT)
		else:
			month_days = int(dateT)
		
		year_days = 0
			
		if int(month) <> 1:
			
			for i in range(1, int(month)+1):
			
				year_days += calendar.monthrange(int(year), int(month)-1)[1]
			
			year_days += int(dateT)
		
		else:
			
			year_days = int(dateT)
		
		for key in self.group:
		
			if month_value.has_key(key):
				
				if month_value[key].has_key(timestamp_month):
					
					if month_value[key][timestamp_month].has_key('CMPT_UserForGenerationHours_Month'):
					
						ongrid_month= float(month_value[key][timestamp_month]['CMPT_UserForGenerationHours_Month']) if month_value[key][timestamp_month]['CMPT_UserForGenerationHours_Month'] <> '' else 0.0
						
						prod_month = ongrid_month /(24* month_days)
						
						mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
						mongo_datalist_timePer.append({'CMPT_Availability_Month':prod_month})
			
				if year_value.has_key(key):
					
					if year_value[key].has_key(timestamp_year):
						
						if year_value[key][timestamp_year].has_key('CMPT_UserForGenerationHours_Year'):
						
							ongrid_year= float(year_value[key][timestamp_year]['CMPT_UserForGenerationHours_Year']) if year_value[key][timestamp_year]['CMPT_UserForGenerationHours_Year'] <> '' else 0.0
							
							prod_year = ongrid_year /(24* year_days)
							
							
							mongo_taglist_timePer.append({'object':key, 'date':timestamp_year})
							
							mongo_datalist_timePer.append({'CMPT_Availability_Year':prod_year})
				
		self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
	
	
	
	def getGenerateRateTimper(self, timestamp_week, timestamp_month, timestamp_year):
		
		try:
			week = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_week], self.group, ['CMPT_Production_Week', 'CMPT_WindEnerge_Week', 'CMPT_Radiation_Week'])
			self.getGenerGroup(week, timestamp_week, 'CMPT_Production_Week', 'CMPT_WindEnerge_Week', 'CMPT_Radiation_Week', 'Week')
			
			month = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_month], self.group, ['CMPT_Production_Month', 'CMPT_WindEnerge_Month', 'CMPT_Radiation_Month'])
			self.getGenerGroup(month, timestamp_month, 'CMPT_Production_Month', 'CMPT_WindEnerge_Month', 'CMPT_Radiation_Month', 'Month')
			
			year = self.mongo.getStatisticsByKeyList_DateList(self.project, [timestamp_year], self.group, ['CMPT_Production_Year', 'CMPT_WindEnerge_Year', 'CMPT_Radiation_Year'])
			self.getGenerGroup(year, timestamp_year, 'CMPT_Production_Year', 'CMPT_WindEnerge_Year', 'CMPT_Radiation_Year', 'Year')
			
		except:
			raise Exception(traceback.format_exc())
			
	def getGenerGroup(self, data_dict, date, productionTag, windEnergeTag, raditionTag, dateT):
		try:
			mongo_taglist_timePer, mongo_datalist_timePer = [], []
			
			for company in self.company_keys_dict:
			
				for farm in self.company_keys_dict[company]:
					
					if farm in self.wt_devKeys_dict:
						
						generateRate = self.getGenerDev(data_dict, farm, date, productionTag, windEnergeTag)
						mongo_taglist_timePer.append({'object':farm, 'date':date})
						mongo_datalist_timePer.append({'CMPT_GenerateRate_'+dateT:generateRate})
						
						for period in self.wt_devKeys_dict[farm]:
							generateRate = self.getGenerDev(data_dict, period, date, productionTag, windEnergeTag)
							mongo_taglist_timePer.append({'object':period, 'date':date})
							mongo_datalist_timePer.append({'CMPT_GenerateRate_'+dateT:generateRate})
									
					if farm in self.pv_devKeys_dict:
						
						generateRate = self.getGenerDev(data_dict, farm, date, productionTag, raditionTag)
						mongo_taglist_timePer.append({'object':farm, 'date':date})
						mongo_datalist_timePer.append({'CMPT_GenerateRate_'+dateT:generateRate})
						
						for period in self.pv_devKeys_dict[farm]:
							generateRate = self.getGenerDev(data_dict, period, date, productionTag, raditionTag)
							mongo_taglist_timePer.append({'object':period, 'date':date})
							mongo_datalist_timePer.append({'CMPT_GenerateRate_'+dateT:generateRate})
						
				generateRate = self.getGenerDev(data_dict, company, date, productionTag, windEnergeTag)
				mongo_taglist_timePer.append({'object':company, 'date':date})
				mongo_datalist_timePer.append({'CMPT_GenerateRate_'+dateT:generateRate})
				
				
			generateRate = self.getGenerDev(data_dict, self.project, date, productionTag, windEnergeTag)
			mongo_taglist_timePer.append({'object':self.project, 'date':date})
			mongo_datalist_timePer.append({'CMPT_GenerateRate_'+dateT:generateRate})
			
			self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
			
			
		except:
			raise Exception(traceback.format_exc())
	
	def getGenerDev(self, data_dict, key, date, productionTag, windEnergeTag):
		production = 0.0
		windEnerge = 0.0
		
		if data_dict.has_key(key):
								
			if data_dict[key].has_key(date):
				
				if data_dict[key][date].has_key(productionTag):
				
					production = float(data_dict[key][date][productionTag]) if data_dict[key][date][productionTag] <> '' else 0.0
				
				if data_dict[key][date].has_key(windEnergeTag):
					
					windEnerge = float(data_dict[key][date][windEnergeTag]) if data_dict[key][date][windEnergeTag] <> '' else 0.0
		if windEnerge <> 0.0:
			
			return round(production *100 / windEnerge, 4)
		
		else:
			
			return 100
		
	def cmpt_sum_timeper_1(self, keyList, tagList, today):
		try:
			keyDics, week_dict, month_dict, year_dict  = {}, {}, {}, {}
			yesterday = today-datetime.timedelta(days=1)
			beforeyesterday = (today-datetime.timedelta(days=2)).strftime('%Y/%m/%d')
			#-------------day----------------
			timestamp_week, list_timestamps_week = self.getDate_ByWeek(today, 1)
			timestamp_month, list_timestamps_month = self.getDate_ByMonth(today, 1)
			timestamp_year, list_timestamps_year = self.getDate_ByYear(today, 1)
			keyDicts_week = self.mongo.getStatisticsByKeyList_DateList(self.project, [list_timestamps_week[1]], keyList, tagList)
			#-----------timeper--------
			yeste_timestamp_week, yeste_list_timestamps_week = self.getDate_ByWeek(yesterday, 1)
			yeste_timestamp_month, yeste_list_timestamps_month = self.getDate_ByMonth(yesterday, 1)
			yeste_timestamp_year, yeste_list_timestamps_year = self.getDate_ByYear(yesterday, 1)
			
			allweek_tagList, allmonth_tagList, allyear_tagList= [], [], []
			all_taglist = []
			for tag in tagList:
				week_tag = tag.replace('Day', 'Week')#tag.split('Day')[0]+'Week'
				allweek_tagList.append(week_tag)
				month_tag = tag.replace('Day', 'Month')#tag.split('Day')[0]+'Month'
				allmonth_tagList.append(month_tag)
				year_tag = tag.replace('Day', 'Year')#tag.split('Day')[0]+'Year'
				allyear_tagList.append(year_tag)
				all_taglist.append(week_tag)
				all_taglist.append(month_tag)
				all_taglist.append(year_tag)
			keyDicts_all = self.mongo.getStatisticsByKeyList_DateList(self.project, [yeste_list_timestamps_week[1],yeste_list_timestamps_month[1],yeste_list_timestamps_year[1]], keyList, all_taglist)
			#-------------外部周月年-----------------
			keyDicts_allweek = self.mongo.getStatisticsByKeyList_DateList(self.project, [yeste_list_timestamps_week[0]], keyList, allweek_tagList)
			keyDicts_allmonth = self.mongo.getStatisticsByKeyList_DateList(self.project, [yeste_list_timestamps_month[0]], keyList, allmonth_tagList)
			keyDicts_allyear = self.mongo.getStatisticsByKeyList_DateList(self.project, [yeste_list_timestamps_year[0]], keyList, allyear_tagList)
			
			weekFlag = True
			monthFlag = True
			yearFlag = True
			for key in keyList:
				week_dict[key], month_dict[key], year_dict[key] = {}, {}, {}
				for tag in tagList:
					#-------------timeper----------------
					weekTag = tag.replace('Day', 'Week')#tag.split('Day')[0]+'Week'
					monthTag = tag.replace('Day', 'Month')#tag.split('Day')[0]+'Month'
					yearTag = tag.replace('Day', 'Year')#tag.split('Day')[0]+'Year'
					sum_week, sum_month, sum_year = 0.0, 0.0, 0.0
					lastday_week, lastday_month, lastday_year, own_day = 0.0, 0.0, 0.0, 0.0
					
					beforYear, beforMonth, beforDay = beforeyesterday.split('/')
					WeekDay = datetime.datetime.strptime(beforYear + beforMonth + beforDay,'%Y%m%d').weekday()
					firstday,lastday = calendar.monthrange(int(beforYear),int(beforMonth))
					if WeekDay == 6 :
						lastday_week = 0.0
					else :
						if keyDicts_all.has_key(key):
							if keyDicts_all[key].has_key(beforeyesterday):
								if keyDicts_all[key][beforeyesterday].has_key(weekTag):
									lastday_week = float(keyDicts_all[key][beforeyesterday][weekTag])
									#if lastday_week <> 0 : print 'lastday_week:',lastday_week,'====='
									#print '-----inside-----'
								else:
									weekFlag = False
						if weekFlag == False and keyDicts_allweek.has_key(key):
							if keyDicts_allweek[key].has_key(yeste_timestamp_week):
								if keyDicts_allweek[key][yeste_timestamp_week].has_key(weekTag):
									lastday_week = float(keyDicts_allweek[key][yeste_timestamp_week][weekTag])
									#if lastday_week <> 0 :print '----lastday_week:',lastday_week,'====='
					if lastday == int(beforDay) :
						lastday_month = 0.0
					else :
						if keyDicts_all.has_key(key) :
							if keyDicts_all[key].has_key(beforeyesterday):
								if keyDicts_all[key][beforeyesterday].has_key(monthTag):
									lastday_month = float(keyDicts_all[key][beforeyesterday][monthTag])
									#if lastday_month <> 0 :print 'lastday_month:',lastday_month,'====='
									#print '-----inside-----'
								else:
									monthFlag = False
						if monthFlag == False and keyDicts_allmonth.has_key(key):
							if keyDicts_allmonth[key].has_key(yeste_timestamp_month):
								if keyDicts_allmonth[key][yeste_timestamp_month].has_key(monthTag):
									lastday_month = float(keyDicts_allmonth[key][yeste_timestamp_month][monthTag])
									#if lastday_month <> 0 :print '----lastday_month:',lastday_month,'====='
					if int(beforMonth) == 12 and int(beforDay) == 31 :
						lastday_year = 0.0
					else :
						if keyDicts_all.has_key(key) :
							if keyDicts_all[key].has_key(beforeyesterday):
								if keyDicts_all[key][beforeyesterday].has_key(yearTag):
									lastday_year = float(keyDicts_all[key][beforeyesterday][yearTag])
									#if lastday_year <> 0 : print 'lastday_year:',lastday_year,'====='
									#print '-----inside-----'
								else:
									yearFlag = False
						if yearFlag == False and keyDicts_allyear.has_key(key):
							if keyDicts_allyear[key].has_key(yeste_timestamp_year):
								if keyDicts_allyear[key][yeste_timestamp_year].has_key(yearTag):
									lastday_year = float(keyDicts_allyear[key][yeste_timestamp_year][yearTag])
									#if lastday_year <> 0 :print '----lastday_year:',lastday_year,'====='
									#print '-----outside-----'
					#-----------day--------
					for date in list_timestamps_year:
						if keyDicts_week.has_key(key):
							if keyDicts_week[key].has_key(date):
								if keyDicts_week[key][date].has_key(tag):
									own_day = float(keyDicts_week[key][date][tag])
									#if own_day <> 0 :print 'own_day:',own_day,'====='
					#-----------day--------
					for date in list_timestamps_week:
						sum_week = lastday_week + own_day
						#if sum_week <> 0 :print 'sum_week:',sum_week,'====='
					for date in list_timestamps_month:
						sum_month = lastday_month + own_day
						#if sum_month <> 0 :print 'sum_month:',sum_month,'====='
					for date in list_timestamps_year:
						sum_year = lastday_year + own_day
						#if sum_year <> 0 :print 'sum_year:',sum_year,'====='
					week_dict[key][weekTag] = round(sum_week, 2)
					month_dict[key][monthTag] = round(sum_month, 2)
					year_dict[key][yearTag] = round(sum_year, 2)
			keyDics = {timestamp_week:week_dict, timestamp_month:month_dict, timestamp_year:year_dict}
			#print 'keyDics:',keyDics,'------'
			#print '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
			return keyDics
		except:
			raise Exception(traceback.format_exc())
	def setWeekMonth(self, timestamp_week, timestamp_month, timestamp_year, sum_all_data, sum_farm_data, sum_group_data):
		self.setMonthCycle(self.all_keyList, timestamp_month, timestamp_year, sum_all_data)
		self.setMonthCycle(self.farmKeys_list, timestamp_month, timestamp_year, sum_farm_data)
		self.setMonthCycle(self.group, timestamp_month, timestamp_year, sum_group_data)
		
		self.setWeekCycle(self.all_keyList, timestamp_week, timestamp_month, timestamp_year, sum_all_data)
		self.setWeekCycle(self.farmKeys_list, timestamp_week, timestamp_month, timestamp_year, sum_farm_data)
		self.setWeekCycle(self.group, timestamp_week, timestamp_month, timestamp_year, sum_group_data)
	def setDay(self, yesterday, timestamp_week, timestamp_month, timestamp_year, sum_all_data, sum_farm_data, sum_group_data):
		self.setDayCycie(self.all_keyList, yesterday, timestamp_week, timestamp_month, timestamp_year, sum_all_data)
		self.setDayCycie(self.farmKeys_list, yesterday, timestamp_week, timestamp_month, timestamp_year, sum_farm_data)
		self.setDayCycie(self.group, yesterday, timestamp_week, timestamp_month, timestamp_year, sum_group_data)
	def setDayCycie(self, keyList, date, timestamp_week, timestamp_month, timestamp_year, sum_data):
		Day_taglist_timePer = []
		Day_datalist_timePer = []
		for key in keyList:
			Day_taglist_timePer.append({'object':key, 'date':date})
			Day_datalist_timePer.append(sum_data[timestamp_week][key])
			Day_taglist_timePer.append({'object':key, 'date':date})
			Day_datalist_timePer.append(sum_data[timestamp_month][key])
			Day_taglist_timePer.append({'object':key, 'date':date})
			Day_datalist_timePer.append(sum_data[timestamp_year][key])
			
			Day_taglist_timePer.append({'object':key, 'date':timestamp_month})
			Day_datalist_timePer.append(sum_data[timestamp_month][key])
			Day_taglist_timePer.append({'object':key, 'date':timestamp_year})
			Day_datalist_timePer.append(sum_data[timestamp_year][key])
			
		self.mongo.setData(self.project, Day_taglist_timePer, Day_datalist_timePer)
	def setWeekCycle(self, keyList, timestamp_week, timestamp_month, timestamp_year, sum_data):
		Week_taglist_timePer = []
		Week_datalist_timePer = []
		for key in keyList:
			Week_taglist_timePer.append({'object':key, 'date':timestamp_week})
			Week_datalist_timePer.append(sum_data[timestamp_week][key])
			Week_taglist_timePer.append({'object':key, 'date':timestamp_week})
			Week_datalist_timePer.append(sum_data[timestamp_month][key])
			Week_taglist_timePer.append({'object':key, 'date':timestamp_week})
			Week_datalist_timePer.append(sum_data[timestamp_year][key])
		self.mongo.setData(self.project,Week_taglist_timePer, Week_datalist_timePer)
	def setMonthCycle(self, keyList, timestamp_month, timestamp_year, sum_data):
		mongo_taglist_timePer = []
		mongo_datalist_timePer = []
		for key in keyList:
			mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
			mongo_datalist_timePer.append(sum_data[timestamp_month][key])
			mongo_taglist_timePer.append({'object':key, 'date':timestamp_month})
			mongo_datalist_timePer.append(sum_data[timestamp_year][key])
		self.mongo.setData(self.project, mongo_taglist_timePer, mongo_datalist_timePer)
	
	def controler(self, execcnt, wait):
		begin = datetime.datetime.now()		
					
		#for i in xrange(execcnt):
		self.setData_Day(begin)
		time.sleep(180)
		self.setData_timper(begin, 1)
		
		#	hour = (datetime.datetime.now()-begin).total_seconds()/3600			
		#	if i<>(execcnt-1):
		#		time.sleep(wait)				
		#	if hour>20:
		#		break			
				
	def main(self):
		
		begin = datetime.datetime.now()
		self.setData_Day(begin)
		self.setData_timper(begin, 1)
		
		scheduler = BlockingScheduler()
		scheduler.add_job(self.controler, 'cron', max_instances=2, day='*', args=(3,3600,))
		scheduler.add_job(self.setData_hour, 'cron', max_instances=2, hour='*')
		scheduler.add_job(self.setData_month, 'cron', max_instances=2, month='*')
		scheduler.start()
		
logging.getLogger("requests").setLevel(logging.WARNING)

logging.getLogger("apscheduler").setLevel(logging.WARNING)
	
logging.config.fileConfig("off_cum.config")	

off_logger = logging.getLogger('infoLogger')	

logger = logging.getLogger('errorLogger')
		
		
if __name__ == "__main__":
	off_cum = off()
	off_cum.main()
	

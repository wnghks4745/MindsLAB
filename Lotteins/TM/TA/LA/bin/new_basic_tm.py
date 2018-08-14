#!/usr/bin/python
#-*- coding:euc-kr -*-

import os, sys
import datetime, time
from operator import itemgetter 

def GetlsList(doc_path, endtype):
	if not os.path.exists(doc_path):
		print( 'ERR: ' + doc_path + ' folder not exist') 
		sys.exit(1)
	filelists = os.listdir(doc_path)
	flists = []
	len_etype = -len(endtype)
	for f in filelists:
		if f[len_etype:] == endtype:
			flists.append(doc_path+f)
	return flists

if __name__ == '__main__':
#	if len(sys.argv) != 2:
#		print sys.argv[0], 'project_Name'
#		sys.exit(1)
#
#	proj = sys.argv[1]
#
	f_cfg = '/data1/MindsVOC/TA/LA/bin/DataInterface.cfg'
	dirline = ''
	if os.path.exists(f_cfg) == True:
		dirline= open(f_cfg, 'r').readline().strip().split('=')
	if len(dirline) != 2:
		data_path = '/data1/MindsVOC/TA/data/'
		print( 'using defalut folder_path => ' + data_path )
	else:
		if dirline[1][-1] != '/':
			dirline[1] += '/'
		data_path = dirline[1]
	
	if not os.path.exists(data_path+proj):
		print( 'ERR: ' + data_path+proj + ' folder not exist') 
		sys.exit(1)
	idx_lists = GetlsList(data_path+proj+'/IDX/', '.idx')
	vp_lists = GetlsList(data_path+proj+'/IDXVP/', '.idxvp')

#	idx_lists.sort()
	fw = open(data_path+proj+'/'+proj+'.idx', 'w')
	for idx in idx_lists:
		if os.path.exists(idx) == False:
			print( "ERR: " + idx + " not Exist" )
			continue
		lines = open(idx, 'r').readlines()
		for line in lines:
			line = line.replace('\n','')
			fw.write(line+'\n')
	fw.close()
	os.system( 'python count_idx.py ' + data_path+proj+'/'+proj+'.idx' )

#	vp_lists.sort()
	fw = open(data_path+proj+'/'+proj+'.idxvp', 'w')
	for vp in vp_lists:
		if os.path.exists(vp) == False:
			print( "ERR: " + vp + " not Exist" )
			continue
		lines = open(vp, 'r').readlines()
		for line in lines:
			line = line.replace('\n','')
			fw.write(line+'\n')
	fw.close()
	os.system( 'python count_idx.py ' + data_path+proj+'/'+proj+'.idxvp' )

	os.chdir('/data1/MindsVOC/TA/makeCSV')
	os.system( 'python idxprn_make_csv.py ' + data_path+proj )
	os.system( 'python idxvp_make_csv.py ' + data_path+proj )
	os.chdir('/data1/MindsVOC/TA/LA/bin')

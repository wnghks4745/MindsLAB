#!/usr/bin/python

import sys
import os
#from impala.dbapi import connect

from datetime import datetime

if __name__ == '__main__':

	if len(sys.argv) < 2:
		filelists = os.popen('ls *.idxvp.srt.idxprn.vp')
	else:
		filelists = os.popen('ls '+sys.argv[1]+'/*.idxvp.srt.idxprn.vp')

	print('Start.')
	print(datetime.now())

	#conn = connect(host='voc01', port=21050)
	#cur = conn.cursor()

	#cur.execute('use voc')

	for filelist in filelists:
		os.system('iconv -c -f euc-kr -t utf-8 ' + filelist[:-1] + ' > ' + filelist[:-1]+'.utf')
		input_file = open(filelist[:-1]+'.utf')

		lines = input_file.readlines()

		input_file.close()

		media = filelist[:-1].split('_')[0]

		if media == 'News':
			media = '1'
		elif media == 'Blog':
			media = '2'
		elif media == 'Tweet':
			media = '3'
		else:
#			media = '0'
			vmedia = filelist[:-1].split('.idxvp.srt')[0]
			media = vmedia[len(vmedia)-1]

		output_file_keyword = open(filelist[:-1].split('.idxvp.srt')[0]+'_idxvp_keyword.csv', 'w')
		output_file_sentiment = open(filelist[:-1].split('.idxvp.srt')[0]+'_idxvp_sentiment.csv', 'w')

		for line in lines:
			line = line.replace('\r','').replace('\n','')
			line = line[:8] + '\t' + line[8:]
			line = line.split('\t')
			tmp = {}
			for i in range(6, len(line)):
				if tmp.get(line[i].split(':')[0]):
					tmp[line[i].split(':')[0]][0] += 1
				else:
					tmp[line[i].split(':')[0]] = [1, line[i].split(':')[1], line[i].split(':')[2]]
			for key in tmp.keys():
				#cur.execute("INSERT INTO new_keyword_tbl (channel, doc_id, sent_id, keyword, type, sent_cnt, doc_date, mon) VALUES ("+media+", "+line[4]+", "+key+", '"+line[1]+"', -100, "+str(tmp[key][0])+", "+line[0]+", "+line[0][0:6]+");")
				#cur.execute("INSERT INTO new_sentiment_tbl (channel, doc_id, sent_id, sentiment, degree, doc_date, mon) VALUES ("+media+", "+line[4]+", "+key+", "+tmp[key][1]+", "+tmp[key][2]+", "+line[0]+", "+line[0][0:6]+");")
				#output_file_keyword.write("ivoc\t"+line[4]+"\t"+key+"\t"+line[1]+"\t-100\t"+str(tmp[key][0])+"\t"+line[0]+"\t"+media+"\t"+line[0][0:6]+"\n")
				#output_file_sentiment.write("ivoc\t"+line[4]+"\t"+key+"\t"+tmp[key][1]+"\t"+tmp[key][2]+"\t"+line[0]+"\t"+media+"\t"+line[0][0:6]+"\n")
				output_file_keyword.write(line[4]+"\t"+key+"\t"+line[1]+"\t-100\t"+str(tmp[key][0])+"\t"+line[0]+"\t"+media+"\t"+line[0][0:6]+"\n")
				output_file_sentiment.write(line[4]+"\t"+key+"\t"+line[1]+"\t"+tmp[key][1]+"\t"+tmp[key][2]+"\t"+line[0]+"\t"+media+"\t"+line[0][0:6]+"\n")
		
		#conn.commit()

		output_file_keyword.close()
		output_file_sentiment.close()

		print(filelist[:-1] + ' End.')
		print(datetime.now())

	#conn.commit()
	#cur.close()

	print('End.')
	print(datetime.now())

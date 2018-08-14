#!/usr/bin/python
#-*- coding:euc-kr -*-
import os
import sys
import ConfigParser

if __name__ == '__main__':
	if len(sys.argv) != 2 :
		print("count_idx.py filename")
		sys.exit(1)

	os.system( 'sort ' + sys.argv[1] +' > '+ sys.argv[1] + '.srt' )

	cfg = ConfigParser.ConfigParser()
	cfg.read('LMI.cfg')
	rsc = cfg.get('Section', 'rsc').strip()

#	rsc = ''

	nelist = open( rsc + '/ne_tag.txt', 'r')
	nelines = nelist.readlines()
	nelist.close()

	# Mapping ne_tags/integer
	ne = {}
	for line in nelines:
		vecline = line.strip().split('\t')
		ne[vecline[0]] = vecline[1]

	normal_list = open( rsc + '/Normalization/WikiTitle_Normalization.txt', 'r')
	normal_lines = normal_list.readlines()
	normal_list.close()

	norm = {}
	for line in normal_lines:
		line = line.replace('\n','').replace('\r','').split(' -> ')
		ne_type = 'AT_NOT'
		if line[2] in ne:
			ne_type = line[2]
		norm[line[0]] = [ line[1], ne_type ] 

	# Loading sort file 
	filelist = open( sys.argv[1] + '.srt', 'r' )
	lines = filelist.readlines()
	filelist.close()

	if sys.argv[1][-2:] == 'vp':
		w_file = open( sys.argv[1] + '.srt.idxprn.vp', 'w')
	else:
		w_file = open( sys.argv[1] + '.srt.idxprn', 'w')
	#
	tmpstr = ''
	ne_count = {}
	sa_list = []
	did = ''
	cnt = 0
	# 0:string 1:NE_int 2:weight_default 3:docid 4:cnt 5:sa_list(for)
	for line in lines:
		vecline = line.strip().split('\t')
		str_txt = vecline[0]
		ne_txt = vecline[1]
		if str_txt in norm:
			str_txt = norm[str_txt][0]
			ne_txt = norm[str_txt][1]

		if tmpstr != str_txt or did != vecline[4]:
			if tmpstr != '':
				w_file.write(tmpstr +'\t')
				best = ''
				for key in ne_count.keys():
					if best == '' or ne_count[key] > ne_count[best]:
						best = key
				w_file.write(best+'\t3.160\t'+did+'\t'+str(cnt))
				for sa_line in sa_list:
					w_file.write('\t'+sa_line)
				w_file.write('\n')
			tmpstr = str_txt #0:string
			ne_count = {}	 #1:NE(AT_Type)
			sa_list = []	 #5:SA
			cnt = 0		 #4:cnt
			did = vecline[4] #3:docid
		# 1:NE_tag2int
		if ne_txt in ne:
			ne_txt = ne[ne_txt]
		else:
			ne_txt = '0'
		if ne_txt in ne_count:
			ne_count[ne_txt] += 1 #int(vecline[3]) 
		else:
			ne_count[ne_txt] = 1 #int(vecline[3])		
	
		# 5:SA insert list
		sa_list.append( vecline[5] )
		cnt += 1 

	if tmpstr != '':
		w_file.write(tmpstr +'\t')
		best = ''
		for key in ne_count.keys():
			if best == '' or ne_count[key] > ne_count[best]:
				best = key
		w_file.write(best+'\t3.160\t'+did+'\t'+str(cnt))
		for sa_line in sa_list:
			w_file.write('\t'+sa_line)
		w_file.write('\n')

	w_file.close()

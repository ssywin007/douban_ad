import sys
sys.setrecursionlimit(10000)
import numpy as np
from dpark import DparkContext, optParser
import heapq
from datetime import datetime, timedelta

from config import PLAN_SETTINGS, logger


def loss(context, ad_rank, mt_c, arg_c, ad_heap, limit, m):
	lost = float('inf')
	temp_rank = ad_rank
	init_ad = arg_c[temp_rank]
	curr_ad = arg_c[temp_rank + 1]
	sub_ad = -1
	while len(ad_heap[curr_ad]) == limit[curr_ad] and temp_rank != m:
		if lost > mt_c[init_ad] - mt_c[curr_ad] + ad_heap[curr_ad][0][0]:
			lost = mt_c[init_ad] - mt_c[curr_ad] + ad_heap[curr_ad][0][0]
			sub_ad = curr_ad
		temp_rank += 1
		curr_ad = arg_c[temp_rank]
	if temp_rank != m:
		if lost > mt_c[init_ad] - mt_c[curr_ad]:
			lost = mt_c[init_ad] - mt_c[curr_ad]
			sub_ad = curr_ad
	return (lost, context, ad_rank, sub_ad)


def update(ad, ad_heap, mt, arg, limit, m):
	for i in range(len(ad_heap)):
		for j in range(len(ad_heap[i])):
			if ad_heap[i][j][3] == ad:
				ad_heap[i][j] = loss(ad_heap[i][j][1], ad_heap[i][j][2], mt[ad_heap[i][j][1]], arg[ad_heap[i][j][1]], ad_heap, limit, m)
	return ad_heap


def move(t, ad_heap, ad_context, mt, arg, limit, m):
	ad_context[t[1]] = t[3]
	heapq.heappush(ad_heap[t[3]], loss(t[1], np.where(arg[t[1]] == t[3])[0][0], mt[t[1]], arg[t[1]], ad_heap, limit, m))
	if len(ad_heap[t[3]]) == limit[t[3]] + 1:
		ad_heap, ad_context = move(heapq.heappop(ad_heap[t[3]]), ad_heap, ad_context, mt, arg, limit, m)
	if len(ad_heap[t[3]]) == limit[t[3]]:
		ad_heap = update(t[3], ad_heap, mt, arg, limit, m)		
	return ad_heap, ad_context


def assign(i, mt, arg, ad_heap, limit, ad_context, m):
	print i
	for j in range(m):                  ## j is rank of ad
		curr_ad = arg[i][j]             ## ad
		if len(ad_heap[curr_ad]) < limit[curr_ad]:
			for k in range(j):          ## k is rank of ad
				temp_ad = arg[i][k]     ## ad
				if mt[i,temp_ad] - mt[i,curr_ad] > ad_heap[temp_ad][0][0]:
					ad_context[i] = temp_ad
					heapq.heappush(ad_heap[temp_ad], loss(i, k, mt[i], arg[i], ad_heap, limit, m))
					ad_heap, ad_context = move(heapq.heappop(ad_heap[temp_ad]), ad_heap, ad_context, mt, arg, limit, m)
					if len(ad_heap[temp_ad]) == limit[temp_ad]:
						ad_heap = update(temp_ad, ad_heap, mt, arg, limit, m)
					break
			if ad_context[i] == -1:
				ad_context[i] = curr_ad
				heapq.heappush(ad_heap[curr_ad], loss(i, j, mt[i], arg[i], ad_heap, limit, m))
				if len(ad_heap[curr_ad]) == limit[curr_ad]:
					ad_heap = update(curr_ad, ad_heap, mt, arg, limit, m)
		else:
			for k in range(j):
				temp_ad = arg[i][k]
				if mt[i,temp_ad] - mt[i,curr_ad] > ad_heap[temp_ad][0][0] - ad_heap[curr_ad][0][0]:
					ad_context[i] = temp_ad
					heapq.heappush(ad_heap[temp_ad], loss(i, k, mt[i], arg[i], ad_heap, limit, m))
					ad_heap, ad_context = move(heapq.heappop(ad_heap[temp_ad]), ad_heap, ad_context, mt, arg, limit, m)
					if len(ad_heap[temp_ad]) == limit[temp_ad]:
						ad_heap = update(temp_ad, ad_heap, mt, arg, limit, m)
					break
			if ad_context[i] == -1:
				temp_loss = loss(i, j, mt[i], arg[i], ad_heap, limit, m)
				if temp_loss[0] > ad_heap[curr_ad][0][0]:
					ad_context[i] = curr_ad
					heapq.heappush(ad_heap[curr_ad], temp_loss)
					ad_heap, ad_context = move(heapq.heappop(ad_heap[curr_ad]), ad_heap, ad_context, mt, arg, limit, m)
					if len(ad_heap[curr_ad]) == limit[curr_ad]:
						ad_heap = update(curr_ad, ad_heap, mt, arg, limit, m)
		if ad_context[i] != -1:
			break
	return ad_context, ad_heap


def check(ad_context, ad_heap, mt, arg, limit, m ,n):
	for i in range(n):
		curr_ad = ad_context[i]
		for j in range(m):
			temp_ad = arg[i][j]
			if len(ad_heap[temp_ad]) < limit[temp_ad] and mt[i, curr_ad] >= mt[i, temp_ad]:
				break
			elif len(ad_heap[temp_ad]) < limit[temp_ad]:
				print 'error' + str(i)
				print str(len(ad_heap[temp_ad])) + ' < ' + str(limit[temp_ad]) + ' while current score ' + str(mt[i, curr_ad]) + ' < ' + str(mt[i, temp_ad])
			elif mt[i, curr_ad] < mt[i, temp_ad] - ad_heap[temp_ad][0][0]:
				print 'error' + str(i)
				print str(mt[i, curr_ad]) + ' < ' + str(mt[i, temp_ad]) + ' - ' + str(ad_heap[temp_ad][0][0])
	for i in range(m):
		if ad_context.count(i) > limit[i]:
			print 'overflow!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ' + str(i) + ' ' + str(ad_context.count(i)) + ' ' + str(len(ad_heap[i]))
	for i in range(m):
		for j in range(len(ad_heap[i])):
			if ad_heap[i][j][3] == i:
				print 'error'


def _optimize(dp, feature_domain, model_date_str):
	dest_dir = PLAN_SETTINGS.DATA_PATH + '/models_%s/%s' % (feature_domain, model_date_str)
	mix_dir = dest_dir + '/mix_score'
	test_dir = dest_dir + '/test'
	mt = np.loadtxt(mix_dir + '.txt')
	mt = mt.T                         ## score matrix, col-ad, row-context
	n,m = mt.shape                    ## number of contexts, ads
	print m,n
	arg = dp.parallelize(mt).map(lambda x:np.argsort(-x)).collect()             ## sort index of score matrix
	limit = [500 for i in range(m)]                 ###### LIMIT OF AD. e.g 4
	ad_context = [-1 for i in range(n)]    			## index of ad for n contexts/users, unassigned = -1
	ad_heap = [[] for i in range(m)]       			## heaps-loss of assgined ad-context, in each heap, element=(loss,context,ad_rank,sub)
	for i in range(n):
		ad_context, ad_heap = assign(i, mt, arg, ad_heap, limit, ad_context, m)
	check(ad_context, ad_heap, mt, arg, limit, m ,n)
	#print mt
	#print np.argsort(mt)
	#print ad_context
	#print ad_heap
						

if __name__ == '__main__':
    optParser.add_option('--model_version', dest='model_version')    ##input=yesterday
    optParser.add_option('--feature_domain', dest='feature_domain')
    options, _ = optParser.parse_args()
    dp = DparkContext()

    if not options.model_version:
        model_date_str = (datetime.today() - timedelta(1)).strftime('%Y%m%d')   ##yestoday
    else:
        model_date_str = options.model_version

    # train
    logger.info('optimizinging %s' % options.feature_domain)
    _optimize(dp, options.feature_domain, model_date_str)
    
    logger.info('optimize.py done!')

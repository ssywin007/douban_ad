import sys
sys.setrecursionlimit(10000)
import numpy as np
import scipy.stats as stats
from dpark import DparkContext, optParser
import heapq
from datetime import datetime, timedelta

from config import PLAN_SETTINGS, logger


def loss(context, mt_c, arg_c, ad_heap, limit, m):
	lcas = []
	count = 0
	for curr_rank in range(m):
		curr_ad = arg_c[curr_rank]
		if len(ad_heap[curr_ad]) < limit[curr_ad]:
			temp_l = - mt_c[curr_ad]
			count += 1
		else:
			temp_l = - mt_c[curr_ad] + ad_heap[curr_ad][0][0]
		temp_s = curr_ad
		lcas.append([temp_l, context, curr_ad, curr_ad])
		if count == 2:
			break
	lcas.sort()
	lcas[1][0] = lcas[1][0] - lcas[0][0]
	lcas[1][2] = lcas[0][3]
	return lcas[1]


def _loss(context, init_ad, forbid_ad, mt_c, arg_c, ad_heap, limit, m):
	lcas = []
	if len(ad_heap[init_ad]) < limit[init_ad]:
		init_l = 0
	else:
		init_l = ad_heap[init_ad][0][0]
	for curr_rank in range(m):
		curr_ad = arg_c[curr_rank]
		if curr_ad == init_ad or curr_ad == forbid_ad:
			continue
		if len(ad_heap[curr_ad]) < limit[curr_ad]:
			temp_l = mt_c[init_ad] - mt_c[curr_ad] - init_l
			lcas.append([temp_l, context, init_ad, curr_ad])
			break
		else:
			temp_l = mt_c[init_ad] - mt_c[curr_ad] + ad_heap[curr_ad][0][0] - init_l
			lcas.append([temp_l, context, init_ad, curr_ad])
	lcas.sort()
	return lcas[0]


def update(ad, ad_heap, mt, arg, limit, m):
	for i in range(len(ad_heap)):
		for j in range(len(ad_heap[i])):
			if ad_heap[i][j][3] == ad:
				ad_heap[i][j] = _loss(ad_heap[i][j][1], ad_heap[i][j][2], ad_heap[i][j][2], mt[ad_heap[i][j][1]], arg[ad_heap[i][j][1]], ad_heap, limit, m)
	return ad_heap


def move(t, ad_heap, ad_context, mt, arg, limit, m):                 ##recursion times
	ad_context[t[1]] = t[3]
	temp_loss = _loss(t[1], t[3], t[2], mt[t[1]], arg[t[1]], ad_heap, limit, m)
	ind = 0
	if len(ad_heap[t[3]]) == limit[t[3]]:
		temp = heapq.heappop(ad_heap[t[3]])
		ind = 1
	heapq.heappush(ad_heap[t[3]], temp_loss)
	if len(ad_heap[t[3]]) == limit[t[3]]:
		ad_heap = update(t[3], ad_heap, mt, arg, limit, m)
		if ind == 1:
			ad_heap, ad_context = move(temp, ad_heap, ad_context, mt, arg, limit, m)
	return ad_heap, ad_context


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
	quantiles_dir = dest_dir + '/quantiles'
	mt = np.loadtxt(mix_dir + '.txt')
	mt = mt.T                         ## score matrix, col-ad, row-context
	n,m = mt.shape                    ## number of contexts, ads
	print m,n
	arg = dp.parallelize(mt).map(lambda x:np.argsort(-x)).collect()             ## sort index of score matrix
	limit = [500 for i in range(m)]                   ###### LIMIT OF AD. e.g 4
	ad_context = [-1 for i in range(n)]    			## index of ad for n contexts/users, unassigned = -1
	ad_heap = [[] for i in range(m)]       			## heaps-loss of assgined ad-context, in each heap, element=(loss,context,ad_rank,sub)
	for i in range(n):
		print i
		temp_loss = loss(i, mt[i], arg[i], ad_heap, limit, m)
		ad_context[i] = temp_loss[2]
		heapq.heappush(ad_heap[ad_context[i]], temp_loss)
		if len(ad_heap[ad_context[i]]) == limit[ad_context[i]]:
			ad_heap = update(ad_context[i], ad_heap, mt, arg, limit, m)
		if len(ad_heap[ad_context[i]]) == limit[ad_context[i]] + 1:
			ad_heap, ad_context = move(heapq.heappop(ad_heap[ad_context[i]]), ad_heap, ad_context, mt, arg, limit, m)
	#check(ad_context, ad_heap, mt, arg, limit, m ,n)
	#print mt
	#print np.argsort(-mt)
	#print ad_context
	#print ad_heap
	ad_scores = [[] for i in range(m)]
	quantiles = []
	for i in range(n):
		ad_scores[ad_context[i]].append(mt[i,ad_context[i]])
	for i in range(m):
		quantiles.append(str(stats.mstats.mquantiles(ad_scores[i], prob = 0.25)))
	with open(quantiles_dir + '.txt', 'w') as f:
		f.write('\n'.join(quantiles))
						

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

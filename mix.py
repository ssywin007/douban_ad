import sys, os
import re
import xgboost as xgb
from dpark import DparkContext, optParser
from subprocess import check_call, call
import cPickle
from datetime import datetime, timedelta
import threading
import math
import time
import matplotlib.pyplot as plt

from config import PLAN_SETTINGS, logger

TRAIN_DATA_DAYS = 30
DECAY = pow(0.1, 1.0/TRAIN_DATA_DAYS)


def load_index(index_file):
    with open(index_file, 'r') as f:
        index = f.read().strip().split('\n')
    index = map(lambda x: x.split('\t'), index)
    index = map(lambda x: (x[1], int(x[0])), index)
    index = dict(index)
    return index


def read_data(dp, src, model_date_str):
    rdds = []
    weights = []
    model_date = datetime.strptime(model_date_str, '%Y%m%d').date()
    dates_str = os.listdir(src)
    dates_str = filter(lambda x: re.match(r'^\d{8}', x), dates_str)
    for date_str in dates_str:
        rdd = dp.textFile(src + '/' + date_str, splitSize=16<<20)\
            .map(lambda x: tuple(x.split(',', 2)))\
            .map(lambda x: (x[1], x[2]))\
            .map(lambda (label, features): ('0' if label=='-1' else '1', features.split(',')))\
            .mapValue(lambda features: map(lambda x: x.split(':'), features))
        rdds.append(rdd)
        num = rdd.count()
        date = datetime.strptime(date_str, '%Y%m%d').date()
        w = DECAY ** (model_date - date).days
        weights += [w] * num
    print dp.union(rdds).count()
    data = dp.union(rdds).mergeSplit(numSplits=256) 
    return data, weights


def encode_feature(data, index, dest):
    data.mapValue(lambda features: filter(lambda x: x[0] in index, features))\
        .mapValue(lambda features: map(lambda x: (index[x[0]], x[1]), features))\
        .map(lambda (label, features): label + ' ' + ' '.join([str(x[0]) + ':' + x[1] for x in features]))\
        .saveAsTextFile(dest)
    check_call('cat %s/* > %s.libsvm' % tuple([dest]*2), shell=True)
    check_call('rm -rf %s' % dest, shell=True)


def _mix(dp, feature_domain, model_date_str):
	dest_dir = PLAN_SETTINGS.DATA_PATH + '/models_%s/%s' % (feature_domain, model_date_str)
	model_dir = dest_dir + '/model.gbdt'
	index_dir = dest_dir + '/index'
	sample_dir = dest_dir + '/sample'
	ad_dir = dest_dir + '/sample_ad'
	context_dir = dest_dir + '/sample_context'
	temp_dir = dest_dir + '/sample_temp'
	mix_dir = dest_dir + '/mix_score'

	dates_str = [model_date_str]
	sample_path = PLAN_SETTINGS.DATA_PATH + '/feature_%s/sample' % feature_domain
	sampling_path = PLAN_SETTINGS.DATA_PATH + '/feature_%s/sample_tmp' % feature_domain
	if os.path.exists(sampling_path):
		check_call('rm -rf %s' % sampling_path, shell=True)
	check_call('mkdir %s' % sampling_path, shell=True)
	for d_str in dates_str:
		if os.path.exists('%s/%s' % (sample_path, model_date_str)):
			check_call('ln -s %s/%s %s/%s' % (sample_path, d_str, sampling_path, d_str), shell=True)
	logger.info('copying sampling data done!')
	sample_data, _ = read_data(dp, sampling_path, model_date_str)
	encode_feature(sample_data, load_index(index_dir+'.dic'), sample_dir)
	check_call('rm -rf %s' % sampling_path, shell=True)

	with open(sample_dir + '.libsvm', 'r') as f:
		dsample = f.read().strip().split('\n')
	ad = dp.parallelize(dsample).map(lambda x:' '.join(x.split(' ')[1:7])).uniq().collect()
	context = dp.parallelize(dsample).map(lambda x:(' '.join(x.split(' ')[7:]))).collect()
	with open(ad_dir + '.libsvm', 'w') as f:
		f.write('\n'.join(ad))
	with open(context_dir + '.libsvm', 'w') as f:
		f.write('\n'.join(context))
	## predict
	with open(model_dir, 'r') as f:
		model_bundle = cPickle.load(f)
	bst = model_bundle['bst']
	index = model_bundle['index']
	ypred = []
	l = len(ad)
	print l
	for curr_ad in range(l):
		print str(curr_ad) + 'in' + str(l)
		comb_ad_context = dp.parallelize(context).map(lambda x:('0 ' + ad[curr_ad] + ' ' + x)).collect()
		with open(temp_dir + '.libsvm', 'w') as f:
			f.write('\n'.join(comb_ad_context))
		dtest_sample_temp = xgb.DMatrix(temp_dir + '.libsvm')
		ypred.append(' '.join(dp.parallelize(bst.predict(dtest_sample_temp)).map(lambda x:str(x)).collect()))
	with open(mix_dir + '.txt', 'w') as f:
		f.write('\n'.join(ypred))
	check_call('rm -rf %s' % temp_dir + '.libsvm', shell=True)
	## plot histogram
	#plt.hist(ypred_sample_temp,10)
	#plot_path = '/home2/songsiyu/data/models_%s/%s' % (options.feature_domain, model_date_str) + '/plots'
	#if not os.path.exists(plot_path):
	#	check_call('mkdir %s' % plot_path, shell=True)
	#plt.savefig(plot_path + '/ad%d' % curr_ad)


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
    logger.info('mixing %s' % options.feature_domain)
    _mix(dp, options.feature_domain, model_date_str)
    
    logger.info('mix.py done!')

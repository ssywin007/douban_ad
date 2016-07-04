import os, sys
from subprocess import check_call, call
from datetime import timedelta, datetime
from dpark import DparkContext, optParser
import random
import math

from config import PLAN_SETTINGS, logger
import ctr_rdds
from common.util import GeneralMap, is_spider
from db_tools import DBTools
sys.path.append('%s/user_profile' % os.path.dirname(os.path.realpath(__file__)))

optParser.add_option("--date", dest="date")
optParser.add_option("--collection", dest="collection")
options, _ = optParser.parse_args()
dp = DparkContext()


def feature_gen(current_date, collection):
    db_tools = DBTools()
    if collection == 'CPC':
        AD_TO_TAGS = dp.broadcast(db_tools.get_ad_to_tags())
    elif collection == 'market':
        AD_TO_TAGS = dp.broadcast(db_tools.get_market_ad_to_features())
    else:
        raise Exception
    AD_TO_ITEM = dp.broadcast(db_tools.get_ad_to_item())      ## extract from db
    AD_TO_ORDER = dp.broadcast(db_tools.get_ad_to_order())
    AD_TO_ACCOUNT = dp.broadcast(db_tools.get_ad_to_account())
    AD_TO_UNIT = dp.broadcast(db_tools.get_ad_to_units())
    AD_TO_PRIORITY = dp.broadcast(db_tools.get_ad_to_priority())
    AD_TO_FREQUENCY = dp.broadcast(db_tools.get_ad_to_frequency())
    KEYWORD_TARGETED_ADS = dp.broadcast(db_tools.get_keyword_targeted_ads())
    ALLIANCE_ADS = dp.broadcast(db_tools.get_alliance_ads())
    CPD_ADS = dp.broadcast(db_tools.get_keyword_targeted_ads())
    GROUP_TO_TAGS = dp.broadcast(db_tools.get_group_tags())

    general_map = GeneralMap(need_region=True, unit_name_to_id=db_tools.get_unit_name_to_id())
    general_map = dp.broadcast(general_map)

    fraud = dp.broadcast(ctr_rdds.get_fraud(current_date))

    def feature_extract(line):
        
        url = line['url']                       ## outcome
        if url.startswith('/count'):
            truth = -1
        elif url.startswith('/redirect'):
            truth = 1
        else:
            return None

        ad_id = line['ad_id']
        unit_id = line['unit_id']  # UNIT_NAME_TO_ID.value.get(line['unit_name'])
        region_id = line['region']  # IP_INFO.value.getIPAddr(line['ip'])
        group_id = line['group']
        group_tags = GROUP_TO_TAGS.value.get(group_id, [])
        page_tags = line['page_tags']
        hour = line['hour']
        uid = str(line['uid'])

        if (not ad_id or not unit_id or
            ad_id in KEYWORD_TARGETED_ADS.value or
            ad_id in ALLIANCE_ADS.value or ad_id in CPD_ADS.value):
             return None

        item_id = AD_TO_ITEM.value[ad_id]
        order_id = AD_TO_ORDER.value[ad_id]
        account_id = AD_TO_ACCOUNT.value[ad_id]
        ad_tags = AD_TO_TAGS.value.get(ad_id, [])
        group_tags = GROUP_TO_TAGS.value.get(group_id, [])
        num_units = len(AD_TO_UNIT.value[ad_id])
        priority_id = AD_TO_PRIORITY.value[ad_id]
        frequency_id = AD_TO_FREQUENCY.value[ad_id]

        MAX_TAGS = 5
        if len(ad_tags) > MAX_TAGS:
            import numpy
            ind = numpy.random.choice(range(len(ad_tags)), MAX_TAGS, replace=False)
            ad_tags = [ad_tags[i] for i in ind]
        page_tags = page_tags[:5]
        group_tags = group_tags[:5]
            
        singular_features = [
            ('ad', ad_id),
            ('item', item_id),
            ('order', order_id),
            ('account', account_id),
            ('specificy', int(math.log(num_units + 1, 2))),
            ('frequency', frequency_id),
            ('unit', unit_id),
            ('hour', hour),
            ('group', group_id),
            ('region_current', region_id),
        ]
        singular_features = [('%s?%s' % (name, value), 1.0) for (name, value) in singular_features]   ## format
        tag_features = [
            ('ad_tag', ad_tags),
            ('group_tag', group_tags),
            ('page_tag', page_tags),
        ]
        tag_features = [('%s?%s' % (name, v), 1.0) for (name, values) in tag_features               ## format
                        for v in values]
        features = singular_features + tag_features

        if features:
            return (uid, (truth, features))
        return None


    def sample(line, general_map, rate):
        l = general_map.parse(line, ['url', 'ad_id'])
        if not l:
            return None
        url = l['url']
        ad_id = l['ad_id']
        if not ad_id:
            return None

        if collection == 'CPC':
            priority_id = AD_TO_PRIORITY.value.get(ad_id, '')
            if priority_id <> 'CPC':  # CPC only
                return None
        elif collection == 'market':
            order_id = AD_TO_ORDER.value.get(ad_id, None)
            if order_id not in [1380, 1335]:
                return None
        else:
            raise Exception
            
        if random.random() > rate: # and not url.startswith('/redirect'): #     ## redirect = click ad ---- sample 
            return None
        if not url.startswith('/count') and not url.startswith('/redirect'):
            return None

        return line


    def unravel(((truth, external_features), user_features)):
        user_features = user_features or []
        user_features = [(name.replace('\/', '\?'), value) for (name, value) in user_features]
        features = external_features + user_features                       ## add features together
        if features:
            return (truth, features)
        return None


    def common_gen(spec):
        logs = []
        for host in PLAN_SETTINGS.HOST:
            erebor_path = PLAN_SETTINGS.EREBOR_PATH % (host, current_date.strftime("%Y/%m/%d"))
            if os.path.exists(erebor_path):
                logs.append(dp.textFile(erebor_path, splitSize=16 << 20))
        daily_agg = dp.union(logs)\
            .mergeSplit(numSplits=256)\
            .filter(lambda l: len(l) < 3000)

        #url = daily_agg\
            #.map(lambda l: general_map.value.parse(l, ['url', 'ad_id']))\
            #.filter(lambda x: x and x['url'] and x['ad_id'] and AD_TO_PRIORITY.value.get(x['ad_id'], '')=='CPC')\
            #.map(lambda x: x['url'])
        #pos = url.filter(lambda x: x.startswith('/redirect'))\
            #.count()
        #PRE_RATE = 0.001
        #neg = url.filter(lambda x: random.random() < PRE_RATE and x.startswith('/count'))\
            #.count()
        #ratio = PRE_RATE * pos / neg
        #print 'pos, neg, ratio:', pos, neg/PRE_RATE, ratio
        ratio = 0.0001
        rate = 5 * ratio

        return daily_agg\
            .map(lambda line: sample(line, general_map.value, rate))\
            .filter(lambda x:x)\
            .map(lambda l: general_map.value.parse(l, spec))\
            .filter(lambda x:x)\
            .filter(lambda line: (not is_spider(line) and (line['uid'] or line['bid'])))\
            .filter(lambda l: l['bid'] not in fraud.value and l['uid'] not in fraud.value)


    spec = set(['url', 'uid', 'bid', 'unit_id', 'ad_id', 'status_code', 'user_agent', 'region', 'page_tags', 'hour', 'group'])
    features = common_gen(spec)

    features = features.map(feature_extract)\
        .filter(lambda x:x)\
        .cache()

    user_list = set(features.map(lambda x: x[0]).filter(lambda x: x<>'None').collect())
    user_list_b = dp.broadcast(user_list)

    user_feature = dp.makeRDD([])

    def _parse_list(line):
        uid, features = line.split('\t')
        features = [x.split(':') for x in features.split('|')]
        features = [(x[0], float(x[1])) for x in features]
        features = sorted(features, key=lambda x: x[1], reverse=True)
        return (uid, features)

    for name in ['book_cluster', 'movie_cluster', 'group_cluster', 'text_cluster']:
        fn = '/home2/alg/user_profile/%s/%s' % (current_date, name)
        if not os.path.exists(fn):
            continue
        rdd = dp.textFile(fn, splitSize=16<<20)\
            .filter(lambda x: x.split('\t', 1)[0] in user_list_b.value)\
            .map(_parse_list)\
            .mapValue(lambda x: [('cnt', len(x)), ('hot', sum([y[1] for y in x]))] + x[:2])\
            .mapValue((lambda name: lambda x: [('%s_concise/%s' % (name, k), v) for (k, v) in x])(name))
        user_feature = user_feature.union(rdd)

    for name in ['gender', 'region']:
        fn = '/home2/alg/user_profile/%s/%s' % (current_date, name)
        if not os.path.exists(fn):
            continue
        rdd = dp.textFile('/home2/alg/user_profile/%s/%s' % (current_date, name), splitSize=16<<20)\
            .filter(lambda x: x.split('\t', 1)[0] in user_list_b.value)\
            .map(_parse_list)\
            .mapValue(lambda x: x[:2])\
            .mapValue((lambda name: lambda x: [('%s/%s' % (name, k), v) for (k, v) in x])(name))
        user_feature = user_feature.union(rdd)

    user_feature = user_feature.reduceByKey(lambda x,y: x+y, numSplits=256)

    features = features.leftOuterJoin(user_feature, numSplits=256)\
        .map(lambda (user_id, value): value)\
        .map(unravel)\
        .filter(lambda x: x)\
        .cache()
    print features.count()

    sample_path = PLAN_SETTINGS.DATA_PATH + '/feature_%s/sample' % collection
    if os.path.exists(sample_path):
        check_call('rm -rf %s' % sample_path, shell=True)
    check_call('mkdir %s' % sample_path, shell=True) 
    features.map(lambda (truth, features): '%s,%s' % (truth, ','.join(['%s:%s' % (f,v) for f,v in features])))\
        .mergeSplit(numSplits=256)\
        .saveAsTextFile(sample_path + '/%s' % current_date.strftime('%Y%m%d'))


if __name__ == '__main__':
    if not options.date:
        date = datetime.today().date() - timedelta(1)
    else:
        date = datetime.strptime(options.date, '%Y-%m-%d').date()

    collection = options.collection
    PLAN_SETTINGS.set_debug(True)

    print 'feature_gen', date, collection, '................................'
    feature_gen(date, collection)

    logger.info('feature_gen %s, %s done!' % (date, collection))

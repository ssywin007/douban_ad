import os
from dpark import DparkContext
from glob import glob
from common.util import GeneralMap, is_spider
from config import PLAN_SETTINGS

dpark = DparkContext()


def get_fraud(current_date):
    general_map = GeneralMap()
    general_map = dpark.broadcast(general_map)
    logs = []
    for host in PLAN_SETTINGS.HOST:
        erebor_path = PLAN_SETTINGS.EREBOR_PATH % (host, current_date.strftime("%Y/%m/%d"))
        if os.path.exists(erebor_path):
            logs.append(dpark.textFile(erebor_path, splitSize=16 << 20))
    erebor_log = dpark.union(logs)\
        .mergeSplit(numSplits=256)

    spec = set(['url', 'uid', 'bid', 'status_code', 'user_agent'])
    fraud = erebor_log.filter(lambda l: '/redirect' in l)\
            .map(lambda l: general_map.value.parse(l, spec))\
            .filter(lambda x: x)\
            .filter(lambda line: (not is_spider(line) and (line['uid'] or line['bid'])))\
            .map(lambda l: (l['uid'] if l['uid'] else l['bid'], 1))\
            .reduceByKey(lambda x, y: x + y, numSplits=256)\
            .filter(lambda (k, v): v > 10)\
            .collectAsMap()
    return fraud


def get_agg(agg_path):
    def agg_extract(line):
        items = line.rstrip().split(' ')
        #yield items[0], (float(items[1]), float(items[2]), float(items[3]), float(items[4]))
        yield items[0], (float(items[1]), float(items[2]))

    def agg_transform(agg):
        agg_dict = {}
        for k, v in agg.iteritems():
            try:
                agg_type, agg_ids = k.split('?',1)
            except Exception:
                print k
                exit(0)
            agg_type = tuple(agg_type.split('+'))
            agg_ids = tuple(agg_ids.split('+'))
            #agg_ids = tuple([agg_ids[idx] if val == 'region' else int(agg_ids[idx])
            agg_ids = tuple([agg_ids[idx] 
                            for idx, val in enumerate(agg_type)])
            agg_dict.setdefault(agg_type, {})[agg_ids] = v
        return agg_dict

    def agg_transform_v2(agg):
        k, v = agg
        try:
            agg_type, agg_ids = k.split('?', 1)
        except Exception:
            print k
            exit(0)
        agg_type = tuple(agg_type.split('+'))
        agg_ids = tuple(agg_ids.split('+'))
        yield agg_type, (agg_ids, v)

    def agg_transform_v3(agg):
        k, v = agg
        try:
            agg_type, agg_ids = k.split('?', 1)
        except Exception:
            print k
            exit(0)
        agg_type = tuple(agg_type.split('+'))
        agg_ids = tuple(agg_ids.split('+'))
        if 'cat' not in agg_type and 'page_tag' not in agg_type:
            yield agg_type, (agg_ids, v)
        
    local_agg = dpark.textFile(
        agg_path
    ).flatMap(
        agg_extract
    ).flatMap(
        agg_transform_v3
    ).groupByKey(
        numSplits=200
    ).mapValue(
        dict
    ).collectAsMap()
            
    #agg_log = dpark.textFile(agg_path).flatMap(
        #agg_extract).mergeSplit(4).collectAsMap()
    #local_agg = agg_transform(agg_log)
    return local_agg


def get_user_to_category(value_type='desc'):
    def map_all(line):
        items = line.rstrip().split(' ')
        user = int(items[0])
        for cat in items[1:]:
            yield user, int(cat)

    try:
        #feature_paths = os.listdir(PLAN_SETTINGS.USER_CAT_PATH % '')
        #feature_paths.sort()
        #feature_path = PLAN_SETTINGS.USER_CAT_PATH % feature_paths[-1]
        feature_path = PLAN_SETTINGS.USER_CAT_PATH % ''
        print feature_path
    except:
        raise Exception('user_type file doesn\'t exist.')
    cat = dpark.textFile(
        feature_path, splitSize=16 << 20
    ).flatMap(
        map_all
    ).collectAsMap()
    return cat

def get_user_to_categories(value_type='desc'):
    def map_all(line):
        items = line.rstrip().split('\t',1)
        user = int(items[0])
        cats = items[1].split('|')[:10]
        cats = [cat.split(':',1) for cat in cats]
        cats = [int(cat[0]) for cat in cats]
        yield (user, cats)

    try:
        feature_path = PLAN_SETTINGS.USER_CAT_PATH % ''
        print feature_path
    except:
        raise Exception('user_type file doesn\'t exist.')
    cat = dpark.textFile(
        feature_path, splitSize=16 << 20
    ).flatMap(
        map_all
    ).collectAsMap()
    return cat

def get_converted_users(converted_path):
    def extract(line):
        try:
            registered, id, num_clicks = line.rstrip().split(' ')
            registered, id, num_clicks = int(registered), id, int(num_clicks)
            yield (registered, id), num_clicks
            if registered == 0:
                yield (registered, int(id)), num_clicks  # redundant output to minimize errors when get weight from the map
        except:
            print line

    converted = dpark.textFile(converted_path).flatMap(
        extract).collectAsMap()
    return converted

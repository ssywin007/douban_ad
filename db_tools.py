# -*- coding: utf-8 -*-
import os
import re
from collections import defaultdict
from config import PLAN_SETTINGS
from dpark import DparkContext
from glob import glob
from datetime import datetime, timedelta

import MySQLdb
from douban.sqlstore import store_from_config

class ArkenStore:
    def __init__(self):
        conn = MySQLdb.connect(host='dae_m', port=3320, db='arkenstone', user='arkenstone', passwd='W3hoRArtbM')
        self.cursor = conn.cursor()
    def execute(self, sql):
        code = self.cursor.execute(sql)
        if code:
            return self.cursor.fetchall()

class MarketStore(object):
    def __init__(self):
        self.conn = MySQLdb.connect(host='dae_b', port=3320, db='market', user='market', passwd='KAAjHBw6im')
        self.cursor = self.conn.cursor()

    def execute(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()


class DBTools(object):
    def __init__(self):
        self.dale_store = store_from_config('dale-offline')
        self.shire_store = store_from_config('shire-offline')
        self.arken_store = ArkenStore()
        self.market_store = MarketStore()

    def get_unit_name_to_id(self):
        dale_res = self.dale_store.execute('select name, id from dale_unit where status = 1 and is_archived = 0')
        unit_name_to_id = dict([(name, id) for name, id in dale_res])
        return unit_name_to_id

    def get_region_id_to_guid(self):
        dale_res = self.dale_store.execute("""select dale_region_id, guid from dale_region_guid""")
        id_to_guid = dict([(id, guid) for id, guid in dale_res])
        return id_to_guid

    def get_ip_to_region(self):
        dale_res = self.dale_store.execute("""select a.start, a.end, b.dale_region_id from dale_region_ip as a, dale_region_guid as b where a.region_guid=b.guid""")
        ip_to_region = [(start, end, id) for start, end, id in dale_res]
        return ip_to_region

    def get_region_to_parent(self):
        dale_res = self.dale_store.execute("""select id, parent_id from dale_region where type < 10 and parent_id > 1""")
        region_to_parent = [(id, parent_id) for id, parent_id in dale_res]
        return region_to_parent

    def get_leaf_regions(self):
        ip_to_region = self.get_ip_to_region()
        all_leaf_regions = set([guid for start, end, guid in ip_to_region])
        all_leaf_regions.add(PLAN_SETTINGS.OTHER_AREA_GUID)
        return all_leaf_regions

    def get_region_to_leaves(self):
        ip_to_region = self.get_ip_to_region()
        region_to_leaves = dict([(guid, set([guid])) for start, end, guid in ip_to_region])
        region_to_leaves.setdefault(PLAN_SETTINGS.OTHER_AREA_GUID, set([PLAN_SETTINGS.OTHER_AREA_GUID]))

        dale_res = self.dale_store.execute("""select guid, parent_guid, type \
            from dale_region_guid order by type desc""")
        for id, parent_id, type in dale_res:
            if id in region_to_leaves and parent_id != PLAN_SETTINGS.ROOT_AREA_GUID:
                region_to_leaves.setdefault(parent_id, set()).update(region_to_leaves[id])
        region_to_leaves[PLAN_SETTINGS.ROOT_AREA_GUID] = list(self.get_leaf_regions())
        return region_to_leaves

    def get_ad_to_regions(self):
        ad_to_item = self.get_ad_to_item()
        id_to_guid = self.get_region_id_to_guid()
        id_to_descendents = self.get_region_to_leaves()

        dale_res = self.dale_store.execute('select item_id, region_id from dale_target_region where is_archived = 0')
        crm_res = self.arken_store.execute("SELECT i.id, o.region_id FROM \
                ads_plan_regions AS o, ads_ad AS i WHERE o.plan_id=i.plan_id")
        res = dale_res + crm_res
        item_to_regions = {}
        for iid, rid in res:
            guid = id_to_guid[rid]
            if guid in id_to_descendents:
                item_to_regions.setdefault(iid, set()).update(id_to_descendents[guid])
            else:
                pass
        ad_to_regions = dict([(ad, list(item_to_regions.get(item, set()))) for ad, item in ad_to_item.iteritems()])
        return ad_to_regions

    def get_ad_to_units(self):
        ad_to_item = self.get_ad_to_item()

        dale_res = self.dale_store.execute('select unit_id, order_item_id \
            from dale_unit_order_item where is_archived = 0')
        item_to_units = {}
        for uid, iid in dale_res:
            item_to_units.setdefault(iid, set()).add(uid)

        placement_to_units = {}
        dale_res = self.dale_store.execute('select placement_id, unit_id \
            from dale_placement_unit where is_archived = 0')
        for pid, uid in dale_res:
            placement_to_units.setdefault(pid, set()).add(uid)
        dale_res = self.dale_store.execute('select placement_id, order_item_id \
            from dale_placement_order_item where is_archived = 0')
        crm_res = self.arken_store.execute("SELECT placement_id, ad_id FROM ads_adplacement")
        res = dale_res + crm_res
        for pid, iid in res:
            if pid in placement_to_units:
                item_to_units.setdefault(iid, set()).update(placement_to_units[pid])
        ad_to_units = dict([(ad, list(item_to_units.get(item, set()))) for ad, item in ad_to_item.iteritems()])
        return ad_to_units

    def get_ad_to_tags(self):
        dale_res = self.dale_store.execute('select a.id, ot.tag_id \
            from dale_ad as a, dale_order_item as oi, dale_order_tags as ot \
            where a.item_id=oi.id and oi.order_id=ot.order_id')
        crm_res = self.arken_store.execute('SELECT c.id, d.tag_id \
            FROM ads_creative AS c, ads_ad AS a, ads_plan AS p, defensor_company_tag AS d \
            WHERE c.ad_id=a.id AND a.plan_id=p.id AND p.company_id=d.company_id')
        res = dale_res + crm_res
        ad_to_tags = defaultdict(set)
        for ad_id, tag_id in res:
            ad_to_tags[ad_id].add(tag_id)
        for k in ad_to_tags.keys():
            ad_to_tags[k] = list(ad_to_tags[k])
        return ad_to_tags

    def get_ads(self):
        dale_res = self.dale_store.execute('select id from dale_ad')
        crm_res = self.arken_store.execute('SELECT id FROM ads_creative')
        res = dale_res + crm_res
        ads = set([ad for ad, in res])
        # ad, or just ad?
        return ads

    def get_ad_to_item(self):
        dale_res = self.dale_store.execute('select id, item_id from dale_ad')
        crm_res = self.arken_store.execute("SELECT id, ad_id FROM ads_creative")
        res = dale_res + crm_res
        ad_to_item = dict([(ad_id, item_id) for ad_id, item_id in res])
        return ad_to_item

    def get_item_to_ads(self, item_id):
        res = self.dale_store.execute('select id from dale_ad where item_id = %s' % item_id)
        if not res:
            crm_res = self.arken_store.execute('SELECT id FROM ads_creative WHERE ad_id=%s;' % item_id)
            res = crm_res
        ads = [ad_id for ad_id, in res]

        return ads

    def get_ad_to_priority(self):
        dale_res = self.dale_store.execute('select id, item_id from dale_ad')
        dale_ad_to_item = dict([(ad_id, item_id) for ad_id, item_id in dale_res])

        dale_res = self.dale_store.execute('select item_id, item_type from dale_item_profiles')
        item_type = dict([(id, item_type) for id, item_type in dale_res])
        ad_type = {}
        for ad, item in dale_ad_to_item.iteritems():
            ad_type[ad] = item_type[item]
        crm_res = self.arken_store.execute('SELECT id FROM ads_creative')
        for ad_line in crm_res:
            ad_type[ad_line[0]] = 'CPC'
        return ad_type

    def get_ad_to_frequency(self):
        ad_to_item = self.get_ad_to_item()

        dale_res = self.dale_store.execute('select item_id, frequency_num \
            from dale_item_profiles')
        crm_res = self.arken_store.execute('SELECT i.id, o.view_limit \
            FROM ads_plan AS o, ads_ad AS i WHERE o.id=i.plan_id')
        res = dale_res + crm_res

        item_frequency = dict([(id, frequency_num if frequency_num else 0) for id, frequency_num in res])
        ad_frequency = {}
        for ad, item in ad_to_item.iteritems():
            ad_frequency[ad] = item_frequency[item]
        return ad_frequency

    def get_ad_to_order(self):
        ad_to_item = self.get_ad_to_item()
        dale_res = self.dale_store.execute('select id, order_id from dale_order_item')
        crm_res = self.arken_store.execute('SELECT i.id, o.id FROM ads_plan AS o, ads_ad AS i WHERE o.id=i.plan_id')
        res = dale_res + crm_res

        item_to_order = dict([(id, order_id) for id, order_id in res])
        ad_to_order = dict([(ad_id, item_to_order.get(ad_to_item[ad_id], -1)) for ad_id in ad_to_item])
        return ad_to_order

    def get_ad_to_account(self):
        ad_to_order = self.get_ad_to_order()
        crm_res = self.arken_store.execute('SELECT id, company_id FROM ads_plan')
        dale_res = self.dale_store.execute('select id, account_id from dale_order')
        res = dale_res + crm_res
        order_to_account = dict([(id, account_id) for id, account_id in res])
        ad_to_account = dict([(ad_id, order_to_account.get(ad_to_order[ad_id], -1)) for ad_id in ad_to_order])
        return ad_to_account

    def get_cpd_items(self):
        ad_to_item = self.get_ad_to_item()

        dale_res = self.dale_store.execute("""select item_id from dale_item_profiles
                         where item_type = 'CPD'""")
        cpd_items = set([iid for iid, in dale_res])

        cpd_ads = set()
        for ad, item in ad_to_item.iteritems():
            if item in cpd_items:
                cpd_ads.add(ad)

        return cpd_ads, cpd_items

    def get_click_filtered_items(self):
        ad_to_item = self.get_ad_to_item()

        dale_res = self.dale_store.execute("""select id from dale_order_item
                         where click_filter = 1""")
        cf_items = set([iid for iid, in dale_res])

        cf_ads = set()
        for ad, item in ad_to_item.iteritems():
            if item in cf_items:
                cf_ads.add(ad)

        return cf_ads, cf_items

    def get_qualified_items(self, start_time):
        ad_to_item = self.get_ad_to_item()
        dale_sql = "select item_id from dale_item_profiles \
                    where date(start_date) <= %s and date(end_date) >= %s"
        dale_res = self.dale_store.execute(dale_sql, (str(start_time), str(start_time)))
        crm_sql = "SELECT i.id FROM ads_plan AS o, ads_ad AS i WHERE o.start_time<='%s' \
                    AND o.end_time>='%s' AND i.plan_id=o.id" % (str(start_time), str(start_time))
        crm_res = self.arken_store.execute(crm_sql)

        res = dale_res + crm_res
        qualified_items = set([iid for iid, in res])

        qualified_ads = set()
        for ad, item in ad_to_item.iteritems():
            if item in qualified_items:
                qualified_ads.add(ad)

        return qualified_ads, qualified_items

    def get_keyword_targeted_ads(self):
        dale_res = self.dale_store.execute('select order_item_id \
            from dale_item_criteria where value is not null and value != ""')
        items = set([item_id for item_id, in dale_res])
        ad_to_item = self.get_ad_to_item()
        ads = set([ad for ad, item in ad_to_item.iteritems() if item in items])
        return ads

    def get_alliance_ads(self):
        dale_res = self.dale_store.execute('select id from dale_ad where type = 2')
        ads = set([ad_id for ad_id, in dale_res])
        return ads

    def get_user_agent(self):
        dale_res = self.dale_store.execute('select id, name, type, low, up from dale_useragent')
        clients = [(type,
                    name,
                    low if low and low.strip() != '' else "-inf",
                    up if up and up.strip() != '' else "inf",
                    id) for id, name, type, low, up in dale_res]
        browsers = [t[1:] for t in clients if t[0] == 0]
        browsers.sort()
        devices = [t[1:] for t in clients if t[0] == 1]
        devices.sort()
        return {'browsers': browsers, 'devices': devices}

    def get_unit_life(self, current_date):
        dale_sql = "select unit_id, min(log_time) \
            from dale_unit_report where impression_count > 0 and log_time < '%s' group by unit_id"
        dale_res = self.dale_store.execute(dale_sql % current_date)
        unit_life = dict([(unit_id, (current_date - date).days / 7 + 1.0) for unit_id, date in dale_res])
        return unit_life

    def get_group_tags(self):
        res = self.shire_store.execute('select item_id, category_id from group_category_item')
        group_tags = {}
        for group, tag in res:
            group_tags.setdefault(group, []).append(tag)
        return group_tags

    def get_user_to_tags(self, current_date):
        def parse(line):
            items = line.rstrip().split('\t',1)
            user = int(items[0])
            cats = items[1].split('|')[:10]
            cats = [cat.split(':',1) for cat in cats]
            cats = [int(cat[0]) for cat in cats]
            return (user, cats)
        dp = DparkContext('mesos')
        data_dir = None
        while True:
            data_dir = '/home2/wangqi/data/tfidf/group/uid/%s' % current_date.strftime('%Y%m%d')
            if os.path.exists(data_dir):
                break
            current_date -= timedelta(1)
        if not data_dir:
            print 'warning: no user_tags data!'
            return {}
        else:
            user_tags = dp.textFile(data_dir, splitSize=16<<20)\
                .map(parse)\
                .collectAsMap()
        return user_tags

    def get_user_to_gender(self):
        dp = DparkContext('mesos')
        user_gender = dp.textFile('/home2/alg/gender/gender.csv', splitSize=16<<20)\
            .map(lambda x: tuple(x.split(',')))\
            .map(lambda (user, gender): (int(user), float(gender)))\
            .collectAsMap()
        return user_gender

    # market related
    def get_market_item_category(self):
        sql = 'select a.id, c.id from dianpu_product_sku as a, dianpu_product as b, dianpu_category as c where a.product_id = b.id and b.category_serial_no = c.serial_no'
        res = self.market_store.execute(sql)
        dp = DparkContext('local')
        res = dp.makeRDD(res)\
            .groupByKey()\
            .collectAsMap()
        return res

    def get_market_item_marketing_category(self):
        sql = 'select a.id, b.category_id from dianpu_product_sku as a, dianpu_product_marketing_category as b where a.product_id = b.product_id'
        res = self.market_store.execute(sql)
        dp = DparkContext('local')
        res = dp.makeRDD(res)\
            .groupByKey()\
            .collectAsMap()
        return res

    def get_market_item_tag(self):
        from base64 import urlsafe_b64encode
        sql = 'select a.id, b.tag_name from dianpu_product_sku as a, dianpu_product_tag as b where a.product_id = b.product_id'
        res = self.market_store.execute(sql)
        dp = DparkContext('local')
        res = dp.makeRDD(res)\
            .mapValue(lambda x: urlsafe_b64encode(x))\
            .groupByKey()\
            .collectAsMap()
        return res
        
    def get_market_shop_item(self):
        sql = 'select c.sid, b.id from dianpu_product as a, dianpu_product_sku as b, dianpu_shop as c where a.id=b.product_id and a.shop_id=c.id'
        res = self.market_store.execute(sql)
        dp = DparkContext('local')
        res = dp.makeRDD(res)\
            .groupByKey()\
            .collectAsMap()
        return res

    def get_market_item_features(self):
        tasks = [
            (self.get_market_item_category, 'category'),
            (self.get_market_item_marketing_category, 'marketing_category'),
            (self.get_market_item_tag, 'tag')
        ]
        dp = DparkContext('local')
        rdd = dp.makeRDD([])
        for (getter, name) in tasks:
            new_rdd = dp.makeRDD(getter().items())\
                .mapValue((lambda name: lambda xs: ['%s+%s' % (name, x) for x in xs])(name))
            rdd = rdd.union(new_rdd)
        item_features = rdd.reduceByKey(lambda x,y: x+y)\
            .collectAsMap()
        return item_features

    def get_market_shop_features(self):
        shop_item = self.get_market_shop_item()
        item_features = self.get_market_item_features()
        dp = DparkContext('local')
        item_shop = dp.makeRDD(shop_item.items())\
            .flatMapValue(lambda x: x)\
            .map(lambda (shop, item): (item, shop))
        item_features = dp.makeRDD(item_features.items())\
            .mapValue(set)
        shop_features = item_shop.join(item_features)\
            .map(lambda (k, v): v)\
            .reduceByKey(lambda x,y: x.union(y))\
            .mapValue(list)\
            .collectAsMap()
        return shop_features

    def get_market_ad_to_entity(self):
        item_pat = re.compile(r'(market|shiji)\.douban\.com/item/(\d+)')
        shop_pat = re.compile(r'(market|shiji)\.douban\.com/(\w+)[/$]')
        def parse_url(url):
            x = item_pat.search(url)
            if x:
                return ('item', int(x.group(2)))
            x = shop_pat.search(url)
            if x:
                return ('shop', x.group(2))
            return None

        sql = 'select a.id, a.pic_link from dale_ad as a, dale_order_item as b where a.item_id=b.id and b.order_id in (1335, 1380)'
        dale_res = self.dale_store.execute(sql)
        ad_market = []
        for (ad_id, url) in dale_res:
            if not url:
                continue
            y = parse_url(url)
            if not y:
                continue
            ad_market.append((ad_id, y))
        ad_market = dict(ad_market)
        return ad_market

    def get_market_ad_to_features(self):
        ad_market = self.get_market_ad_to_entity()
        item_features = self.get_market_item_features()
        print len(item_features)
        shop_features = self.get_market_shop_features()
        print len(shop_features)
        ad_features = []
        item_cnt = 0
        shop_cnt = 0
        for (ad_id, (typ, market_id)) in ad_market.iteritems():
            if typ == 'item':
                features = item_features.get(market_id) or []
                features = filter(lambda x: len(x) <= 100, features)
                features = features[:20]
                ad_features.append((ad_id, features))
                item_cnt += 1
            elif typ == 'shop':
                features = shop_features.get(market_id) or []
                features = filter(lambda x: len(x) <= 100, features)
                features = features[:20]
                ad_features.append((ad_id, features))
                shop_cnt += 1
            else:
                raise Exception
        print 'item_cnt', item_cnt
        print 'shop_cnt', shop_cnt
        return dict(ad_features)

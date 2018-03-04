import collections
import contextlib
import glob
import gzip
import logging
import os
import sys
import time
from Queue import Queue, Empty
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from optparse import OptionParser

import memcache

import appsinstalled_pb2

MEMCACHE_MAX_RETRIES = 3
MEMCACHE_RETRY_TIMEOUT = 5
MEMCACHE_CLIENT_TIMEOUT = 30

FILES_PROCESSING_POOL_SIZE = 3
LOAD_FILE_THREADS_COUNT = 10

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


class MemcacheClientPool(object):
    def __init__(self, size, *args, **kwargs):
        self._pool = Queue()
        for i in xrange(size):
            client = memcache.Client(*args, **kwargs)
            self._pool.put(client)

    @contextlib.contextmanager
    def acquire_client(self):
        client = self._pool.get()
        yield client
        self._pool.put(client)


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def create_memc_client_pools(pool_size, idfa, gaid, adid, dvid):
    connections = {
        'idfa': idfa,
        'gaid': gaid,
        'adid': adid,
        'dvid': dvid
    }

    client_pools = {}
    for name, address in connections.iteritems():
        pool = MemcacheClientPool(pool_size, [address], socket_timeout=MEMCACHE_CLIENT_TIMEOUT)
        client_pools[name] = pool

    return client_pools


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def insert_appsinstalled(memc_client, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()

    memc_addr = ', '.join(['{}:{}'.format(s.address[0], s.address[1]) for s in memc_client.servers])
    success = False
    try:
        if dry_run:
            logging.debug("[%s] %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            for i in xrange(MEMCACHE_MAX_RETRIES):
                if i != 0:
                    time.sleep(MEMCACHE_RETRY_TIMEOUT)

                ok = memc_client.set(key, packed)
                if ok:
                    success = True
                    break

    except Exception, e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))

    return success


def insert_records(records_queue, memc_client_pools, dry_run):
    inserted = 0
    while True:
        try:
            appsinstalled = records_queue.get(timeout=1)
        except Empty:
            break

        client_pool = memc_client_pools.get(appsinstalled.dev_type)

        if not client_pool:
            logging.error("Unknown device type: %s" % appsinstalled.dev_type)
            continue

        with client_pool.acquire_client() as client:
            ok = insert_appsinstalled(client, appsinstalled, dry_run)

        if ok:
            inserted += 1

    return inserted


def load_file(file_name, threads_count, idfa, gaid, adid, dvid, dry_run):
    logging.info('Processing {}'.format(file_name))
    records_queue = Queue()
    memc_client_pools = create_memc_client_pools(threads_count, idfa, gaid, adid, dvid)

    thread_pool = ThreadPool(processes=threads_count)
    insert_results = []
    for i in range(threads_count):
        result = thread_pool.apply_async(insert_records, args=(records_queue, memc_client_pools, dry_run))
        insert_results.append(result)
    thread_pool.close()

    total = errors = 0
    fd = gzip.open(file_name)
    for line in fd:
        line = line.strip()
        if not line:
            continue
        total += 1

        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue

        records_queue.put(appsinstalled)

    fd.close()

    successfully_inserted = sum([r.get() for r in insert_results])
    errors += total - errors - successfully_inserted
    processed = total - errors

    if total:
        err_rate = float(errors) / processed if processed else 100.
        if err_rate < NORMAL_ERR_RATE:
            if err_rate < NORMAL_ERR_RATE:
                logging.info("[{}] Acceptable error rate ({}). Load successful".format(file_name, err_rate))
            else:
                logging.error(
                    "[{}] High error rate ({} > {}). Load failed".format(file_name, err_rate, NORMAL_ERR_RATE))
    else:
        logging.error("[{}] Empty".format(file_name))

    return file_name


def main(opts):
    files = sorted(glob.iglob(opts.pattern))

    pool = ThreadPool(processes=FILES_PROCESSING_POOL_SIZE)
    load_file_fn = partial(load_file, threads_count=LOAD_FILE_THREADS_COUNT,
                           idfa=opts.idfa,
                           gaid=opts.gaid,
                           adid=opts.adid,
                           dvid=opts.dvid,
                           dry_run=opts.dry)

    for file_name in pool.imap(load_file_fn, files):
        dot_rename(file_name)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception, e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)

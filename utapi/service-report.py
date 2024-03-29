#!/usr/bin/env python3

import argparse
import concurrent.futures as futures
import datetime
import itertools
import json
import logging
import multiprocessing
import os
import pathlib
import re
import sys
import time
import urllib
from collections import namedtuple

import requests
from requests import ConnectionError, HTTPError, Timeout

_log = logging.getLogger('utapi-svc-compute')
_log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
USERS_BUCKET = 'users..bucket'

def _fatal_error(msg, rc=1):
    _log.error(msg)
    sys.exit(rc)

def path_type(string):
    return pathlib.Path(os.path.expanduser(string)).resolve()

def get_args():
    parser = argparse.ArgumentParser(
        prog=pathlib.Path(sys.argv[0]).name,
        description='Compute service level metrics for Utapiv2',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-c', '--config',
        default='/conf/config.json',
        type=path_type,
        help='Specify an alternate config file')

    parser.add_argument('-r', '--max-retries', default=2, type=int, help='Max retries before failing a request to an external service.')
    parser.add_argument('-p', '--parallel-queries', default=5, type=int, help='Max number of parallel queries to external services.')
    parser.add_argument('-j', '--json', action='store_true', help='Output raw reports in json format.')
    parser.add_argument('-o', '--output', default='.', type=path_type, help='Write report to this directory.')
    parser.add_argument('--debug', action='store_true', help='Enable debug level logging.')
    parser.add_argument('--dry-run', action='store_true', help="Don't do any computation. Only validate and print the configuration.")

    return parser.parse_args()

ScriptConfig = namedtuple('ScriptConfig', ['warp10', 'bucketd', 'vault'])
Warp10Conf = namedtuple('Warp10Conf', ['host', 'port', 'nodeId', 'read_token'])

def get_config(args):
    if not args.config.exists():
        _fatal_error('Config file does not exist: {}'.format(args.config))
    with open(args.config) as f:
        try:
            utapi_conf = json.load(f)
        except Exception as e:
            _log.debug(e)
            _fatal_error('Error reading utapi config file at: {}'.format(args.config))

    try:
        read_token = utapi_conf['warp10']['readToken']
        write_token = utapi_conf['warp10']['writeToken']
        warp10_conf = [Warp10Conf(read_token=read_token, **server) for server in utapi_conf['warp10']['hosts']]
    except Exception as e:
        _log.debug(e)
        _fatal_error('Utapi config does not contain a valid "warp10" section')

    try:
        bucketd_conf = utapi_conf['bucketd'][0]
    except Exception as e:
        _log.debug(e)
        _fatal_error('Utapi config does not contain a valid "bucketd" section')

    try:
        vault_host = utapi_conf['vaultd']['host']
        vault_port = utapi_conf['vaultd']['port']
        vault_addr = 'http://{}:{}'.format(vault_host, vault_port)
    except Exception as e:
        _log.debug(e)
        _fatal_error('Utapi config does not contain a valid "vaultd" section')

    return ScriptConfig(warp10=warp10_conf, bucketd=bucketd_conf, vault=vault_addr)

Bucket = namedtuple('Bucket', ['account', 'name'])
class MaxRetriesReached(Exception):
    def __init__(self, url):
        super().__init__('Max retries reached for request to %s'%url)

class InvalidListing(Exception):
    def __init__(self, bucket):
        super().__init__('Invalid contents found while listing bucket %s'%bucket)

class BucketDClient:

    '''Performs Listing calls against bucketd'''
    __url_format = 'http://{addr}/default/bucket/{bucket}'
    __headers = {'x-scal-request-uids': 'utapi-compute-service-lvl'}

    def __init__(self, bucketd_addr=None, max_retries=2):
        self._bucketd_addr = bucketd_addr
        self._max_retries = max_retries
        self._session = requests.Session()

    def _do_req(self, url, check_500=True, **kwargs):
        # Add 1 for the initial request
        for x in range(self._max_retries + 1):
            try:
                resp = self._session.get(url, timeout=30, verify=False, headers=self.__headers, **kwargs)
                if check_500 and resp.status_code == 500:
                    _log.warning('500 from bucketd, sleeping 15 secs')
                    time.sleep(15)
                    continue
                return resp
            except (Timeout, ConnectionError) as e:
                _log.exception(e)
                _log.error('Error during listing, sleeping 5 secs %s'%url)
                time.sleep(5)

        raise MaxRetriesReached(url)

    def _list_bucket(self, bucket, **kwargs):
        '''
        Lists a bucket lazily until "empty"
        bucket: name of the bucket
        kwargs: url parameters key=value

        To support multiple next marker keys and param encoding, a function can
        be passed as a parameters value. It will be call with the json decode
        response body as its only argument and is expected to return the
        parameters value. On the first request the function will be called with
        `None` and should return its initial value. Return `None` for the param to be excluded.
        '''
        url = self.__url_format.format(addr=self._bucketd_addr, bucket=bucket)
        static_params = {k: v for k, v in kwargs.items() if not callable(v)}
        dynamic_params = {k: v for k, v in kwargs.items() if callable(v)}
        is_truncated = True # Set to True for first loop
        payload = None
        while is_truncated:
            params = static_params.copy() # Use a copy of the static params for a base
            for key, func in dynamic_params.items():
                params[key] = func(payload) # Call each of our dynamic params with the previous payload
            try:
                _log.debug('listing bucket bucket: %s params: %s'%(
                    bucket, ', '.join('%s=%s'%p for p in params.items())))
                resp = self._do_req(url, params=params)
                if resp.status_code == 200:
                    payload = resp.json()
            except ValueError as e:
                _log.exception(e)
                _log.error('Invalid listing response body! bucket:%s params:%s'%(
                    bucket, ', '.join('%s=%s'%p for p in params.items())))
                raise
            except MaxRetriesReached:
                _log.error('Max retries reached listing bucket:%s'%bucket)
                raise
            except Exception as e:
                _log.exception(e)
                _log.error('Unhandled exception during listing! bucket:%s params:%s'%(
                    bucket, ', '.join('%s=%s'%p for p in params.items())))
                raise
            yield resp.status_code, payload
            if isinstance(payload, dict):
                is_truncated = payload.get('IsTruncated', False)
            else:
                is_truncated = len(payload) > 0

    def list_buckets(self):

        def get_next_marker(p):
            if p is None:
                return ''
            return p.get('Contents', [{}])[-1].get('key', '')

        params = {
            'delimiter': '',
            'maxKeys': 1000,
            'marker': get_next_marker
        }
        for _, payload in self._list_bucket(USERS_BUCKET, **params):
            buckets = []
            for result in payload['Contents']:
                match = re.match('(\w+)..\|..(\w+.*)', result['key'])
                bucket = Bucket(*match.groups())
                buckets.append(bucket)
            if buckets:
                yield buckets

def query_warp10(url, payload, retries=5):
    for i in range(1, retries + 1):
        try:
            resp = requests.post(url, payload)
            if resp.status_code != 200:
                _log.error('Error fetching metrics from warp 10')
                if hasattr(resp, 'text'):
                    _log.debug(resp.text)
                    continue
            data = resp.json()
            num_objects = data[0].get('objD')
            bytes_stored = data[0].get('sizeD')
            return num_objects, bytes_stored
        except Exception as e:
            _log.exception('Error during warp 10 request', e)
            continue
    raise MaxRetriesReached(url)

def get_metrics(warp10s, bucket, timestamp, retries=5):
    num_objects = 0
    bytes_stored = 0
    for server in warp10s:
        auth = json.dumps({ 'read': server.read_token })
        op_info = json.dumps({
            'end': timestamp,
            'labels': { 'bck': bucket.name },
            'node': server.nodeId
        })
        payload = "'{}' '{}' @utapi/getMetricsAt".format(auth, op_info).encode('utf-8')
        url = 'http://{}:{}/api/v0/exec'.format(server.host, server.port)
        try:
            node_num_objects, node_bytes_stored = query_warp10(url, payload, retries)
            num_objects += node_num_objects
            bytes_stored += node_bytes_stored
        except MaxRetriesReached as e:
            _log.exception('Error fetching metrics for bucket {} from {}'.format(bucket.name, server.nodeId), e)
            raise e
    return num_objects, bytes_stored

def get_account_data(vault, canon_ids):
    _log.debug('Fetching account info for canonicalId {}'.format(canon_ids) )
    payload = {
        'Action': 'GetAccounts',
        'Version': '2010-05-08',
        'canonicalIds': canon_ids
    }
    resp = requests.get(vault, params=payload)
    return resp.json()


def chunker(iterable, chunksize):
    _iterable = iter(iterable)
    while True:
        chunk = itertools.islice(_iterable, chunksize)
        try:
            item = next(chunk)
        except StopIteration:
            return
        yield itertools.chain([item], chunk)

def print_config(config):
    print('Warp 10 Hosts - NodeId | Address')
    for host in config.warp10:
        print('{} | {}:{}'.format(host.nodeId, host.host, host.port))
    print('\nBucketD Host')
    print(config.bucketd)

html_header = '''<!DOCTYPE html>
<html>
<body>
<style>
    body {
        background: #E7DED9;
        color: #3C3431;
    }

    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
    }

    table {
        width: 95%;
        margin: 0.25em auto;
    }

    thead {
        background: #E1C391;
    }

    tbody {
        background: #FDF4E3;
    }

    tbody > tr:hover {background-color: #D6EEEE;}

    td {
        padding: 0.1em 0.5em;
    }

    h2, h3 {
        margin: auto;
        width: 95%;
        font-weight: normal;
        display: block;
    }

</style>
'''
html_footer = '<span>Generated on {}</span>\n</body></html>'

_heading_tmpl = '<thead><tr>\n{}\n</tr></thead>\n'
def get_heading_row(*args):
    filler = '\n'.join('<td>{}</td>'.format(a) for a in args)
    return _heading_tmpl.format(filler)

_data_tmpl = '<tr>\n{}\n</tr>\n'
def get_data_row(*args):
    filler = '\n'.join('<td>{}</td>'.format(a) for a in args)
    return _data_tmpl.format(filler)

def get_table_lines(heading, data):
    yield '<table>\n'
    yield get_heading_row(*heading)
    yield '<tdata>\n'
    for row in data:
        yield get_data_row(*row)
    yield '</tdata>\n'
    yield '</table>\n'

def get_heading(text, size=2):
    return '<h{}>{}</h{}>\n'.format(size, text, size)

_bucket_reports = {}
_account_reports = {}
_service_report = { 'obj_count': 0, 'bytes_stored': 0 }
# Create a report for the given bucket
# Create/Update the report for the bucket's account canonicalId
# Update the service report
def update_report(bucket, obj_count, bytes_stored):
    if bucket.account not in _bucket_reports:
        _bucket_reports[bucket.account] = dict()
    _bucket_reports[bucket.account][bucket.name] = { 'obj_count': obj_count, 'bytes_stored': bytes_stored }
    if bucket.account not in _account_reports:
        _account_reports[bucket.account] = { 'obj_count': obj_count, 'bytes_stored': bytes_stored }
    else:
        existing = _account_reports[bucket.account]
        _account_reports[bucket.account] = {
            'obj_count': existing['obj_count'] + obj_count,
            'bytes_stored': existing['bytes_stored'] + bytes_stored
        }
    _service_report['obj_count'] = _service_report['obj_count'] + obj_count
    _service_report['bytes_stored'] = _service_report['bytes_stored'] + bytes_stored

def to_human(bytes_stored):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if abs(bytes_stored) < 1024.0:
            return '{0:3.2f}{1}'.format(bytes_stored, unit)
        bytes_stored /= 1024.0
    return "{0:.2f}PiB".format(bytes_stored)

BucketReport = namedtuple('BucketContents', ['name', 'obj_count', 'bytes_stored', 'human'])
AccountReport = namedtuple('AccountReport', ['name', 'arn', 'obj_count', 'bytes_stored', 'human'])
ServiceReport = namedtuple('ServiceReport', ['obj_count', 'bytes_stored', 'human'])

def get_service_report():
    return ServiceReport(human=to_human(_service_report['bytes_stored']), **_service_report)

def get_account_reports(account_info):
    for canonical_id, counters in _account_reports.items():
        name = account_info[canonical_id]['name']
        arn = account_info[canonical_id]['arn']
        human_size = to_human(counters['bytes_stored'])
        yield AccountReport(name=name, arn=arn, human=human_size, **counters)

def get_bucket_reports(canonical_id):
    for name, counters in _bucket_reports[canonical_id].items():
        human_size = to_human(counters['bytes_stored'])
        yield BucketReport(name=name, human=human_size, **counters)

if __name__ == '__main__':
    args = get_args()
    config = get_config(args)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=_log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=_log_format)

    if args.dry_run:
        print_config(config)
        print('\nOutput Path\n{}'.format(args.output))
        sys.exit(0)

    bucket_client = BucketDClient(config.bucketd, args.max_retries)

    generation_timestamp = datetime.datetime.utcnow()
    microtimestamp = int(generation_timestamp.timestamp()) * 1000000
    account_info = {}

    _log.info('Starting report computation')

    # Take the buckets from a listing response from bucketd and submit it to the process pool
    # As the jobs are completed update the reports
    # All buckets are processed before bucketed is queried again
    failed_accounts = set()
    with futures.ProcessPoolExecutor(args.parallel_queries) as executor:
        for batch in bucket_client.list_buckets():
            jobs = { executor.submit(get_metrics, config.warp10, bucket, microtimestamp, args.max_retries): bucket for bucket in batch }
        for job in futures.as_completed(jobs.keys()):
            bucket = jobs[job]
            try:
                num_objects, bytes_stored = job.result()
            except Exception as e:
                _log.exception('Error fetching metrics for bucket {}'.format(bucket.name))
            else:
                _log.info('Updating report for bucket {}'.format(bucket.name))
                update_report(bucket, num_objects, bytes_stored)
            # Account information is fetched lazily on first encounter
            if bucket.account not in account_info:
                try:
                    account_info[bucket.account] = get_account_data(config.vault, [bucket.account])[0]
                except Exception as e:
                    _log.exception('Failed to fetch account information for canonicalId {}'.format(bucket.name))
                    _log.error('Report will not include name and arn for account.')

    ext = 'json' if args.json else 'html'
    output_path = args.output.joinpath('utapi-service-report-{}.{}'.format(generation_timestamp.strftime('%Y-%m-%dT%H-%M-%S'), ext))
    with open(output_path, 'w') as f:
        if args.json:
            _log.debug('writing json report')
            json.dump({
                'service': get_service_report()._asdict(),
                'account': [r._asdict() for r in get_account_reports(account_info)],
                'bucket': {account_info[cid]['arn']: [r._asdict() for r in get_bucket_reports(cid)] for cid in account_info.keys() }
            }, f)
        else:
            _log.debug('writing html report')
            f.write(html_header)
            f.write(get_heading('Service Totals'))
            for line in get_table_lines(['Total Object Count', 'Total Bytes Stored', 'Human Readable Size'], [get_service_report()]):
                f.write(line)
            f.write('<br>')

            _log.debug('Writing per account reports')
            f.write(get_heading('Breakdown by Account'))
            for line in get_table_lines(['Account', 'Arn', 'Objects Count', 'Bytes Stored', 'Human Readable Size'], get_account_reports(account_info)):
                f.write(line)
            f.write('<br>')

            _log.debug('Writing per bucket reports')
            f.write(get_heading('Breakdown by Bucket'))
            for canonical_id, acc_info in account_info.items():
                f.write(get_heading('Account: {}'.format(acc_info['name']), 3))
                for line in get_table_lines(['Bucket', 'Object Count', 'Bytes Stored', 'Human Readable Size'], get_bucket_reports(canonical_id)):
                    f.write(line)
                f.write('<br>')
            f.write(html_footer.format(generation_timestamp.isoformat()))
    _log.info('Finished generating report {}'.format(output_path.as_posix()))

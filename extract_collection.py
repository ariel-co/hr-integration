#!/usr/bin/env python3.12

# DBTITLE 1,imports, utilitles
import logging
import logging.config
import traceback

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from urllib.parse import urlparse, parse_qs, urlencode, urljoin
import xml.etree.ElementTree as ET
from fnmatch import fnmatchcase
import json
import time
import re
from datetime import datetime, timedelta, date, UTC
from pprint import pprint
from contextlib import contextmanager
import uuid
import gzip
import sys
import os
import io
import atexit
from typing import Any, Dict, Optional, Union, Iterable
import subprocess
import shlex
import jsonpath_ng.ext as jp
from jsonpath_ng import JSONPath
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64
from getpass import getpass
from copy import deepcopy
from functools import reduce, partial
from collections import defaultdict
from pathlib import Path
import argparse

# unbuffer stdout, stderr for use with nohup (Python 3.7+)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout = io.TextIOWrapper(
        open(sys.stdout.fileno(), mode='wb', buffering=0))
    sys.stderr = io.TextIOWrapper(
        open(sys.stderr.fileno(), mode='wb', buffering=0))

class _datetime(datetime):
    def __str__(self):
        return self.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        # if (dotidx := txt.index('.')) > -1:
        #     txt = txt[:dotidx+3] + txt[dotidx+6:]
        # return txt

# DBTITLE 1,params, globals
script_path = Path(__file__).resolve()
script_dir = script_path.parent
try:
    config = json.load(open(script_dir / "config.json"))
except Exception as e:
    raise Exception("Cannot find o open the config file %s", script_dir / "config.json")

start_time = _datetime.now(UTC)  #.replace(microsecond=0)

class Context(dict):
    """ Dict subclass;
        allows time/datetime expressions with a delta like 'ts-30d' """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delta_re = re.compile(r"([+-][\d.]+)([dmy]?)$")
    def __missing__(self, key):
        if m := self.delta_re.search(key):
            delta = float(m.group(1))
            unit = m.group(2)
            if unit in ['d', 'D', '']:
                return self[key[:m.start()]] + timedelta(days=delta)
            elif unit in 'mM':
                return add_months(self[key[:m.start()]], int(delta))
            elif unit in 'yY':
                return add_months(self[key[:m.start()]], int(delta)*12)
            else:
                raise ValueError(f"Invalid time unit {unit}")
        else:
            raise KeyError(key)

class HelpfulArgParser(argparse.ArgumentParser):
    def __init__(self, raise_on_error: bool=False, *args, **kwargs):
        self._raise_on_error = raise_on_error
        super().__init__(*args, **kwargs)

    def error(self, message):
        print(f"Error: {message}\n")
        self.print_help()
        if self._raise_on_error:
            raise Exception(message)
        else:
            print(f"FAIL")
            sys.exit(1)

argparser = HelpfulArgParser()
argparser.add_argument('sf', metavar='|'.join(config["sf"].keys()),
                        help="SAP SF connection params (see config.json: sf)")
argparser.add_argument('collection', metavar='CollectionName',
                        help="SF query task (see the keys in collections.json)")
argparser.add_argument('--coll-config', metavar='filename', default='collections.json',
                        help="Collection (task) config file (default SCRIPT_DIR/collections.json)")
argparser.add_argument("--incremental", action="store_true",
                        help="Update the latest collection incrementally and rename")
argparser.add_argument("--mongo-conn",
                        metavar='|'.join(c['name'] for c in config['mongodb']['connections']),
                        help="MongoDB connection (see config.json)")
argparser.add_argument("--output-dir", metavar='dir',
                        help="Local output directory")
argparser.add_argument("--compress", "--gzip", action="store_true")
argparser.add_argument("--azupload",
                        metavar='|'.join(k for k in config['azupload'].keys() if not k.startswith('#')),
                        help="Azure upload destination - see config.json")
argparser.add_argument("--batch-id",
                        help="Correlates all tasks in a batch")
argparser.add_argument('--coll-ts', metavar='iso_time', type=_datetime.fromisoformat,
                        help="Override the collection timestamp")
argparser.add_argument("--max-docs", type=int, metavar='N',
                        help="max docs to output (for testing)")
args = argparser.parse_args(sys.argv[1:])

batch_id = args.batch_id or str(uuid.uuid4())
run_ctx = Context(ts=args.coll_ts or start_time,
                   dt=(args.coll_ts or start_time).date(),
                   start_time=start_time,
                   coll=args.collection, res=args.collection,
                   incr="incr" if args.incremental else "full",
                   batch_id=batch_id, log_id=batch_id[:8],
                   gz=".gz" if args.compress else '')

import shutil
class ShortMessageFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.width = shutil.get_terminal_size((120, 60)).columns
    def format(self, record):
        original_msg = super().format(record)
        return '\n'.join(
            line if len(line) <= self.width else line[:self.width-3] + '...'
            for line in original_msg.splitlines() )

class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        # handle exc_info
        exc_lines = []
        if record.exc_info:
            exc_text = traceback.format_exception(*record.exc_info)
            exc_lines = ''.join(exc_text).splitlines()
            record.exc_info = record.exc_text = None

        msg_lines = record.getMessage().splitlines()

        formatted_lines = []
        for line in msg_lines + exc_lines:
            record.msg = line
            record.args = ()  # Prevent re-formatting args on subsequent loops
            formatted_lines.append(super().format(record))

        return '\n'.join(formatted_lines)

config_logs = config.get("general", {}).get("logs", {})
log_path = script_dir / config_logs.get("path", "logs/{coll}-{incr}-{ts}.log").format_map(run_ctx)
log_path.parent.mkdir(parents=True, exist_ok=True)

logging_cfg = {
    'version': 1,
    'disable_existing_loggers': False,  # otherwise other modules are muted
    'loggers': {
        '': { # root logger
            'level': 'INFO',
            'handlers': ['console', 'file'] } },
    'formatters': {
        'console': {
            '()': ShortMessageFormatter,
            'format': '%(asctime)s %(levelname)-8s {coll} %(message)s'.format_map(run_ctx),
            'datefmt': '%Y-%m-%d %H:%M:%S' },
        'file': {
            '()': MultiLineFormatter,
            'format': config_logs.get("line_fmt", "%(asctime)s %(levelname)-8s {coll} %(message)s").format_map(run_ctx),
            'datefmt': '%Y-%m-%d %H:%M:%S' } },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'console',
            'stream': 'ext://sys.stdout' },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'file',
            'filename': log_path } } }

logging.config.dictConfig(logging_cfg) # this configures the root logger (named '')
logger = logging.getLogger(__name__)   # child of the root logger

logger.info("START %s", 'INCREMENTAL' if args.incremental else 'FULL')
logger.info("argv=%s", ' '.join(map(repr, sys.argv[1:])))
logger.info(f"Logging to {log_path}")

# add a file handler and make all modules use it
# file_handler = logging.FileHandler(log_path)
# file_handler.setLevel(logging.INFO)
# file_handler.setFormatter(logging.MultiLineFormatter(
#     config_logs.get("line_fmt", "%(asctime)s %(message)s").format_map(run_ctx)))
# logging.getLogger().addHandler(file_handler)

# to change the log level later, remember that a handler
# cannot capture logs lower than the Root Logger's level:
# 1. Lower root threshold so handlers can filter individually
# logging.getLogger().setLevel(logging.DEBUG)
# 2. File gets everything (DEBUG and up)
# file_handler.setLevel(logging.DEBUG)
# 3. Console stays cleaner (INFO and up)
# console_handler.setLevel(logging.INFO)

if args.incremental and args.mongo_conn is None:
    raise Exception(f"--incremental requires --mongo-conn")

class Timer():
    def __init__(self):
        self.timer = time.perf_counter
        self._end_time = None
    def __enter__(self):
        self.start = self.timer()
        return self
    def __exit__(self, *args):
        self._end_time = self.timer()
        return False
    @property
    def elapsed(self):
        end = self.timer() if self._end_time is None else self._end_time
        return end - self.start

def json_default_serializer(value: Any):
    return (
        value.isoformat().replace('+00:00', 'Z')
        if isinstance(value, (datetime, _datetime, date))
        else str(value) )

@contextmanager
def open_write_smart(filename):
    if filename is None:
        yield None
    else:
        if str(filename).endswith(".gz"):
            file_obj = gzip.open(filename, mode='wt', encoding='utf-8')
        else:
            file_obj = open(filename, mode='w', encoding='utf-8')
        try:
            yield file_obj
        finally:
            file_obj.close()

def format_object(obj: Union[Dict, list, str], context: Dict[str, Any]):
    """
    Scan operations and expand placeholders strings using the context dict
    and str.format
    """
    if isinstance(obj, str):
        return obj.format(**context)
    elif isinstance(obj, list):
        return [format_object(item, context) for item in obj]
    elif isinstance(obj, dict):
        return {key: format_object(obj, context) for key, obj in obj.items()}
    return obj

def add_months(d: datetime | date, months: int):
    year_delta, new_month = divmod(d.month + int(months), 12)
    last_day = [None, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    try: # not feb or leap year?
        return d.replace(year=d.year+year_delta, month=new_month, day=min(d.day, last_day[new_month]))
    except ValueError:
        return d.replace(year=d.year+year_delta, month=new_month, day=min(d.day, last_day[new_month]-1))

def add_field(data: dict, path: str, value: Any):
    """ Add a value to a dict at a path without arrays (e.g. "$.person.id") """
    try:
        assert isinstance(data, dict)
    except:
        logger.error("data is not dict:\n", repr(data))
        raise
    path_list = path.lstrip("$.").split('.')
    reduce(lambda d, key: d.setdefault(key, {}), path_list[:-1], data)[path_list[-1]] = value
    return data

def copy_path_value(doc: dict, src_path, dest_path, del_src=False):
    """ Takes whatever is found at src_path, copies it to dest_path,
        optionally delete the original ref"""
    if isinstance(src_path, str):
        src_path = jp.parse(src_path)
    matches = src_path.find(doc)
    if matches:
        if isinstance(dest_path, str):
            dest_path = jp.parse(dest_path)
        try:
            dest_path.find_or_create(doc)[0].value = matches[0].value
        except IndexError:
            raise ValueError(f"Cannot set path {dest_path}")
        if del_src:
            src_path.filter(lambda _: True, doc)

def filter_collnames(coll_basename, collnames):
    """ Returns latest, older, partials """
    completed, partials = [], []
    collname_re = "^{coll}-{ts}".format(
        coll=re.escape(coll_basename),
        ts=r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)")
    collname_partial_re = "^{coll}Partial-{ts}".format(
        coll=re.escape(coll_basename),
        ts=r"(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)")
    # print(collname_re)
    # print(collname_partial_re)
    for name in collnames:
        if re.fullmatch(collname_re, name):
            completed.append(name)
        elif re.fullmatch(collname_partial_re, name):
            partials.append(name)
    completed = sorted(completed, reverse=True)
    return next(iter(completed), None), completed[1:], partials

def collname_to_ts(collname):
    if m := re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+Z)?Z?", collname):
        return _datetime.fromisoformat(m.group())
    else:
        return None

def mongofield_to_jsonpath(mongo_field_str):
    segments = mongo_field_str.split(".")
    jsonpath_segments = []
    for segment in segments:
        if segment.isdigit():
            jsonpath_segments.append(f"[{segment}]")
        else:
            jsonpath_segments.append(segment)
    return "$." + ".".join(jsonpath_segments)

def cleanup_file(path):
    logger.info(f"Cleanup: deleting {path}")
    Path(path).unlink()  # convert again in case it's str

# replace default handling of uncaught exceptions
# to log them instead of printing
class ExceptionTracker:
    def __init__(self):
        self.exception_occurred = False
        self.original_excepthook = sys.excepthook

    def hook(self, exc_type, exc_value, exc_traceback):
        self.exception_occurred = True
        logger.error("Exception:", exc_info=(exc_type, exc_value, exc_traceback))
        #self.original_excepthook(exc_type, exc_value, exc_traceback)
exception_tracker = ExceptionTracker()
sys.excepthook = exception_tracker.hook
# final message
def say_goodbye():
    if exception_tracker.exception_occurred:
        globals().get('logger', globals().get('logging')).error("FAIL")
    else:
        globals().get('logger', globals().get('logging')).info("COMPLETED")
atexit.register(say_goodbye)

class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = kwargs.pop("timeout", None)
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        if "timeout" in kwargs and self.timeout is not None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

class ExponentialBackoffSession(requests.Session):
    def __init__(self, *args,
        timeout=None,
        max_retries=0,
        backoff_factor=1.0,
        status_forcelist=(),
        **kwargs ):
        super().__init__(*args, **kwargs)
        self.timeout = tuple(timeout) if timeout is not None else None  # connect, read
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = set(status_forcelist)

    def request(self, *args, **kwargs):
        """ Injects timeout, handles retries """
        if kwargs.get("timeout") is None and self.timeout is not None:
            kwargs["timeout"] = self.timeout
        # logging.info("timeout=%s", kwargs["timeout"])
        kwargs.pop("retries", None)
        attempt = 0
        while True:
            try:
                response = super().request(*args, **kwargs)
                if (self.status_forcelist is not None and self.status_forcelist and
                    response.status_code in self.status_forcelist and
                    attempt < self.max_retries):
                    attempt += 1
                    sleep_time = self.backoff_factor * (2**(attempt-1))
                    logger.warning("Got status code %s for URL: %s",
                                   response.status_code, kwargs.get("url", args[1]) )
                    logger.info("Retrying in %.2f s (retry %d/%d)",
                                sleep_time, attempt, self.max_retries)
                    time.sleep(sleep_time)
                    continue
                return response
            except (requests.ConnectTimeout, requests.ConnectionError, requests.Timeout,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ContentDecodingError,
                    urllib3.exceptions.ProtocolError) as exc:
                # Catch connect timeouts, read timeouts, and proxy/network connection errors
                if attempt < self.max_retries:
                    attempt += 1
                    sleep_time = self.backoff_factor * (2 ** (attempt - 1))
                    logger.warning("Exception %s: %s for URL: %s",
                                   type(exc).__name__, exc, kwargs.get("url", args[1]) )
                    logger.info("Retrying in %.2f s (retry %d/%d)",
                                sleep_time, attempt, self.max_retries)
                    time.sleep(sleep_time)
                    continue
                else:
                    logger.error("Giving up")
                    raise exc

# DBTITLE 1,f: IDP auth
def idp_auth(http_session, url, data: dict, auth: str, corr_id: Optional[str] = None):
    headers = {"Authorization": auth}
    if corr_id:
        headers |= {"X-Request-Correlation-Id": corr_id}
    response = http_session.post(
        url, json=data, headers=headers )
    response.raise_for_status()
    return response.text

# DBTITLE 1,f: OAuth
def oauth(http_session, url, data: dict, auth: str):
    response = http_session.post(url, data=data | {"assertion": auth})
    response.raise_for_status()
    return response.json()["access_token"]

# DBTITLE 1,f: SFAPI login
def sfapi_login(http_session, sfapi_url, assertion):
    login_xml = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
        <soapenv:Header/>
        <soapenv:Body>
            <login xmlns="urn:sfobject.sfapi.successfactors.com">
                <credential>
                    <companyId></companyId>
                    <username></username>
                    <password></password>
                </credential>
            </login>
        </soapenv:Body>
    </soapenv:Envelope>"""
    headers = {
        "Authorization": f"Bearer {assertion}",
        "Content-Type": "text/xml"
    }
    response = http_session.post(sfapi_url, data=login_xml, headers=headers)
    response.raise_for_status()
    # print(response.text)
    et_auth_resp = ET.fromstring(response.text)
    sessionid = et_auth_resp.findtext(".//sfo:sessionId", namespaces={"sfo": "urn:sfobject.sfapi.successfactors.com"})
    # ".//S:Body/ns:loginResponse/ns:result/ns:sessionId"
    return sessionid

# DBTITLE 1,f: OData GET
# https://www.odata.org/documentation/odata-version-2-0/uri-conventions/
def query_odata(http_session, access_token,
                 root_url, resource_path, query_string=None,
                 pagesize=None, results_path='$.d.results[*]', yield_pages=False):
    url = urljoin(root_url, resource_path)
    query = parse_qs(query_string)
    query.update({"$format": "json"})
    if pagesize:
        query.update({"customPageSize": pagesize})
    headers = {"Authorization": f"Bearer {access_token}"}

    results_getter = jp.parse(results_path)
    while True:
        with Timer() as t:
            response = http_session.get(url, headers=headers, params=query)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                logging.error(f"HTTP status code {e.response.status_code}")
                logging.error(f"Response body: {e.response.text}")
                raise RuntimeError("OData query failed") from e
        resp_data = response.json()
        batch = [match.value for match in results_getter.find(resp_data)]
        if yield_pages:
            yield batch
        else:
            yield from batch
        next_url = resp_data["d"].get("__next")
        if next_url:
            skip_token = parse_qs(urlparse(next_url).query)["$skiptoken"][0]
            # print(f"{skip_token=}")
            query["$skiptoken"] = skip_token
        else:
            break

# DBTITLE 1,f: SFAPI query
def query_sfapi(http_session, sessionid, sfapi_url, query_string,
                 doc_xpath, nsmap, pagesize=800, yield_pages=False):
    xml_tpl_query = '\n'.join([
        "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\">",
        "    <soapenv:Header/>",
        "    <soapenv:Body>",
        "        <query xmlns=\"urn:sfobject.sfapi.successfactors.com\">",
        "            <queryString>",
        "            </queryString>",
        "            <!-- <param><name>queryMode</name><value>delta</value></param> -->",
        "            <!-- <param><name>startingRow</name><value>3</value></param> -->",
        "            <!-- <param><name>maxRows</name><value>10</value></param> -->",
        "        </query>",
        "    </soapenv:Body>",
        "</soapenv:Envelope>" ])
    # <soapenv:Header><sfapiSessionId>{api_session_id}</sfapiSessionId></soapenv:Header>
    xml_tpl_querymore = '\n'.join([
        "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\">",
        "    <soapenv:Header/>",
        "    <soapenv:Body>",
        "        <queryMore xmlns=\"urn:sfobject.sfapi.successfactors.com\">",
        "            <querySessionId>{query_session_id}</querySessionId>",
        "        </queryMore>",
        "    </soapenv:Body>",
        "</soapenv:Envelope>" ])

    def set_param(et, nsmap, name, value):
        e_query = et.find(".//sfo:query", namespaces=nsmap)
        updated = False
        for e_param in e_query.findall("sfo:param", nsmap):
            e_name = e_param.find("sfo:name", nsmap)
            if e_name is not None and e_name.text == name:
                e_value = e_param.find("sfo:value", nsmap)
                if e_value is not None:
                    e_value.text = str(value)
                    updated = True
                    break
        if not updated:
            e_param = ET.Element("sfo:param")
            e_name = ET.Element("sfo:name")
            e_value = ET.Element("sfo:value")
            e_name.text = name
            e_value.text = str(value)
            e_param.append(e_name)
            e_param.append(e_value)
            e_query.append(e_param)

    def set_query(et, nsmap, query_string):
        e_query_string = et.find(".//sfo:queryString", nsmap)
        if e_query_string is not None:
            e_query_string.text = query_string

    et_query = ET.fromstring(xml_tpl_query)
    if pagesize:
        set_param(et_query, nsmap, "maxRows", pagesize)
    set_query(et_query, nsmap, query_string)
    # stringify and remove the blank lines created from <!--comments-->
    xml_query = ('\n' if api_style=="sfapi" else '').join(filter(lambda line: line.strip(),
        ET.tostring(et_query).decode('utf-8').splitlines()))
    result_xpath = "./soapenv:Body/sfo:queryResponse/sfo:result"
    has_more, q_session_id = None, None
    while True:
        # print(xml_query)
        with Timer() as t:
            resp_query = http_session.post(sfapi_url,
                data=xml_query,
                headers={"Content-Type": "text/xml" })
        try:
            resp_query.raise_for_status()
        except Exception as e:
            logger.error(resp_query.text)
            raise RuntimeError("SFAPI query failed") from e
        # et_resp_query = ET.fromstring(resp_query.content)
        # if 'str_resp_sample' in globals():
        #     global str_resp_sample
        #     if not str_resp_sample:
        #         str_resp_sample = resp_query.text
        et_result = ET.fromstring(resp_query.content).find(result_xpath, nsmap)
        results = et_result.findall(doc_xpath, nsmap)
        e_num_results = et_result.find("./sfo:numResults", nsmap)
        # print(f"Batch: {e_num_results.text} ({len(results)}) docs in {t.elapsed:.1f} seconds has_more={has_more}")
        if yield_pages:
            yield results
        else:
            yield from results
        e_has_more = et_result.find("./sfo:hasMore", nsmap)
        if e_has_more is not None and e_has_more.text == 'true':
            e_q_session_id = et_result.find("./sfo:querySessionId", nsmap)
            q_session_id = e_q_session_id.text
            xml_query = xml_tpl_querymore.format(api_session_id=sessionid, query_session_id=q_session_id)
            result_xpath = "./soapenv:Body/sfo:queryMoreResponse/sfo:result"
        else:
            break

def paths_match(path, pattern_path):
    return len(path) == len(pattern_path) and all(
        fnmatchcase(seg, seg_pattern) for seg, seg_pattern in zip(path, pattern_path) )

def xml_to_dict(element, list_paths: Optional[list] = None, current_path: Optional[list] = None):
    clean_tag = element.tag.split('}')[-1]
    _current_path = current_path if current_path is not None else [clean_tag]
    # no children
    if len(element)==0:
        return element.text if element.text else ''
    result = {}
    for child in element:
        clean_child_tag = child.tag.split('}')[-1]
        child_path = _current_path + [clean_child_tag]
        child_data = xml_to_dict(child, list_paths, child_path)
        if clean_child_tag in result:
            try:
                result[clean_child_tag].append(child_data)
            except AttributeError:
                logger.warning(f"Unexpected repeated tag at {child_path}")
        else:
            if list_paths and any(paths_match(child_path, list_path) for list_path in list_paths):
                result[clean_child_tag] = [child_data]
            else:
                result[clean_child_tag] = child_data
    # special case for root element
    if False and current_path is None:  # disabled because we want the _contents_ of the root tag
        if list_paths and any(paths_match([clean_tag], list_path) for list_path in list_paths):
            return {clean_tag: [result]}
        else:
            return {clean_tag: result}
    else:
        return result

# DBTITLE 1,main: upload to Azure storage
def azupload(filepath, url, config):
    cmd = [ config.get("cmd", "azcopy"), "cp", filepath, url ]
    retry_delays = config.get("retry_delays", [])
    n_retries = len(retry_delays)
    for attempt_number, retry_delay in enumerate([*retry_delays, None], start=1):
        logger.info(f"Uploading to %s", url)
        result = subprocess.run(cmd, env=config["env"], encoding='utf8',
                                 stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        logging.info(result.stdout)
        if result.returncode == 0:
            return True
        else:
            logger.warning(f"azcopy failed with rc={result.returncode}")
            if retry_delay is not None:
                logger.info(f"Retrying in {retry_delay} s...")
                time.sleep(retry_delay)
            else:
                logger.error("Giving up")
                # logger.error(' '.join(map(shlex.quote, cmd)))
    return False

# DBTITLE 1,f: describe the structure
def merge_skel(skel1, skel2):
    if skel1 == {}: return skel2
    if skel2 == {}: return skel1
    if isinstance(skel1, list) and isinstance(skel2, list):
        return sorted(list(set(skel1 + skel2)))
    if isinstance(skel1, list) or isinstance(skel2, list):
        return skel1 if isinstance(skel1, list) else skel2
    merged = {}
    for key in set(skel1.keys()) | set(skel2.keys()):
        if key in skel1 and key in skel2:
            merged[key] = merge_skel(skel1[key], skel2[key])
        else:
            merged[key] = skel1.get(key) or skel2.get(key)
    return merged

def build_skel(data):
    if isinstance(data, dict):
        skel = {}
        for key, value in data.items():
            skel[key] = build_skel(value)
        return skel
    elif isinstance(data, list):
        merged_item_skel = {}
        for item in data:
            merged_item_skel = merge_skel(merged_item_skel, build_skel(item))
        # default empty arrays to an empty type list
        if not merged_item_skel and not data:
            return {'[]': []}
        else:
            return {'[]': merged_item_skel}
    else:
        return [type(data).__name__]

# main: basic setup, auth

# Decrypt the config
def aesgcm_crypt(data: dict|str, paths: Iterable[JSONPath|str],
                 cipher_suite: AESGCM, do_encrypt=True, encoding='utf-8') -> None:

    def encrypt(value: bytes|str) -> bytes|str:
        value_b = value if isinstance(value, bytes) else value.encode(encoding)
        nonce = os.urandom(12)
        out = nonce + cipher_suite.encrypt(nonce, value_b, associated_data=None)
        return out if isinstance(value, bytes) else base64.b64encode(out).decode('ascii')

    def decrypt(value: bytes|str) -> bytes|str:
        value_b = value if isinstance(value, bytes) else base64.b64decode(value)
        out = cipher_suite.decrypt(value_b[:12], value_b[12:], associated_data=None)
        return out if isinstance(value, bytes) else out.decode(encoding)

    for path in paths:
        expr = path if isinstance(path, JSONPath) else jp.parse(path)
        expr.update(data,
                    lambda value, parent, key:
                        encrypt(value) if do_encrypt else decrypt(value))

config_secret_paths = config.get("general", {}).get("secret_paths", [])
if config_secret_paths:
    aesgcm_encrypt = partial(aesgcm_crypt, do_encrypt=True)
    aesgcm_decrypt = partial(aesgcm_crypt, do_encrypt=False)

    config_key_env_name = config.get("general", {}).get("secret_key_env", "RENISI_CFG_KEY")
    if config_key_env_name in os.environ:
        config_key_b64 = os.environ[config_key_env_name] # -> base64
        logger.info(f"Found ${config_key_env_name} with AESGCM key "
                    f"{config_key_b64[:8]}...{config_key_b64[-4:]}")
    elif sys.stdin.isatty():
        print(f"${config_key_env_name} was not found")
        config_key_b64 = getpass("Enter a Base64 encoded key: ").strip()
    else:
        logger.error(f"${config_key_env_name} was not found and "
                        "standard input is not a terminal - aborting")
        sys.exit(1)

    aesgcm_suite = AESGCM(base64.b64decode(config_key_b64))
    aesgcm_decrypt(config, config_secret_paths, aesgcm_suite) # in place
    logger.info("Config decrypted")
else:
    config.warning("F'$.general.secret_paths is absent or empty in config.json - "
                    "passwords and keys are stored insecurely")

# DBTITLE 1,main: collections, SF config
config_collections = json.load(open(args.coll_config or script_dir / "collections.json"))
try:
    # config_this_coll = next(c for c in config_collections if c['name'] == args.collection)
    config_this_coll = config_collections[args.collection]
except KeyError:
    raise Exception(f"Collection {args.collection!r} was not found in collections.json")
try:
    config_sf = config["sf"][args.sf]
except KeyError:
    raise Exception(f"SAP SF config {args.sf!r} was not found in config.json")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# http_session = requests.Session()
# http_session.verify = False
# http_session.proxies.update(config["general"]["http"]["proxies"])

## IDP auth
# try:
#     http_session.mount(config_sf['auth']['idp']['url'],
#         TimeoutHTTPAdapter(
#             timeout=tuple(config["general"]["http"]["idp"].get("timeout", [])),
#             max_retries=Retry(**config["general"]["http"]["idp"]["retry"],
#                                raise_on_status=False) ) )
# except KeyError:
#     logger.warning(f"No retry logic was found for IdP")
try:
    retry_logic = config["general"]["http"]["retry"]["idp"]
except KeyError:
    logger.warning(f"No retry logic was found for IdP")
    retry_logic = {}
http_session_idp = ExponentialBackoffSession(**retry_logic)
http_session_idp.verify = False
http_session_idp.proxies.update(config["general"]["http"]["proxies"])

with Timer() as t_idp_auth:
    idp_token = idp_auth(http_session_idp, config_sf['auth']['idp']['url'],
                          config_sf['auth']['idp']["payload"], config_sf['auth']['idp']["authorization"],
                          run_ctx['batch_id'])
logger.info(f"IDP auth took {t_idp_auth.elapsed:.1f} s")

# OAuth
# try:
#     http_session.mount(config_sf['auth']['oauth']['url'],
#         TimeoutHTTPAdapter(
#             timeout=tuple(config["general"]["http"]["oauth"].get("timeout", [])),
#             max_retries=Retry(**config["general"]["http"]["oauth"]["retry"],
#                                raise_on_status=False) ) )
# except KeyError:
#     logger.warning(f"No retry logic was found for IdP")
try:
    retry_logic = config["general"]["http"]["retry"]["oauth"]
except KeyError:
    logger.warning(f"No retry logic was found for OAuth")
    retry_logic = {}
http_session_oauth = ExponentialBackoffSession(**retry_logic)
http_session_oauth.verify = False
http_session_oauth.proxies.update(config["general"]["http"]["proxies"])
with Timer() as t_oauth_auth:
    oauth_token = oauth(http_session_oauth,
                         config_sf['auth']['oauth']['url'], config_sf['auth']['oauth']["payload"], idp_token)
logger.info(f"OAuth took {t_oauth_auth.elapsed:.1f} s")

# DBTITLE 1,main: set up outputs
out_dir = args.output_dir or script_dir / "data"
out_dir.mkdir(exist_ok=True)
out_path = out_dir / "{coll}-{incr}-{ts}.json{gz}".format_map(run_ctx)
logger.info("Local data output file: %s", out_path)

# mongodb collection name precedence: cmdline argument > collection config > global config
# unless incremental mode - then we pick the latest completed
if args.mongo_conn:
    from pymongo import MongoClient, ReplaceOne
    from bson.json_util import dumps, loads

    try:
        mongo_conn_params = deepcopy(
            next(c for c in config["mongodb"]['connections'] if c['name']==args.mongo_conn))
    except StopIteration:
        raise Exception(f"Connection {args.mongo_conn!r} was not found in config.json")
    mongo_client = MongoClient(host=mongo_conn_params['uri'],
                                password=mongo_conn_params['password'])
    mongo_db = mongo_client.get_database()  # default db

    final_suf = "PartialCompleted" if config_this_coll.get("mark_completed") else ''
    out_mongo_collname = f"{{coll}}{final_suf}-{{ts}}".format_map(run_ctx)
    out_mongo_collname_tmp = None

    if args.incremental:
        # check the config
        for key in ['query_incremental', 'pk_mongofind', 'pk_jsonpath']:
            if not key in config_this_coll:
                raise Exception(f"{key!r} not found in config")
        logger.info(f"key: {config_this_coll['pk_mongofind']} / {config_this_coll['pk_jsonpath']}")
        # write to the latest collection, then rename
        collection_names = [ n for n in mongo_db.list_collection_names()
                              if not n.startswith("system.") ]

        # find the latest full collection
        latest_coll, older_colls, partial_colls = filter_collnames(
            args.collection, collection_names)
        if not latest_coll:
            raise Exception(f"Cannot find a completed collection - aborting")
        logger.info(f"Latest completed collection: {latest_coll!r}")

        if incr_start_time := collname_to_ts(latest_coll):
            logger.info(f"Retrieved watermark {incr_start_time}")
            run_ctx.update(ts_incr=incr_start_time.replace(microsecond=0, tzinfo=UTC))
        else:
            raise Exception(f"Could not find the timemstamp for {latest_coll} - aborting")

        # upsert with the current collection name
        out_mongo_collname_tmp = latest_coll
    else:
        # (only full mode)
        # new collection with temporary name containing "Partial"
        out_mongo_collname_tmp = ("{coll}Partial-{ts}").format_map(run_ctx)
        logger.info(f"Creating {out_mongo_collname_tmp!r} (empty)")
        mongo_db.create_collection(out_mongo_collname_tmp)

        # v2 FIX (perf): if this collection is going to be upserted (has a
        # key configured), create the index on that key NOW, before the
        # write loop starts. Without an index here, every upsert has to do
        # a full collection scan to check for an existing match, which gets
        # slower and slower as the collection grows during the run - this
        # is what caused throughput to collapse from ~20 docs/s to ~5 docs/s
        # (and dropping further) on a from-scratch full load.
        if config_this_coll.get("pk_mongofind"):
            logger.info(f"Creating index on {config_this_coll['pk_mongofind']!r} "
                        f"(needed for efficient upsert matching)")
            mongo_db[out_mongo_collname_tmp].create_index(config_this_coll["pk_mongofind"])

    # v2 FIX: load the natural key regardless of mode (previously this was
    # only set up for --incremental). When a key is configured, the MongoDB
    # write below uses an idempotent upsert instead of a raw insert - so if
    # the source ever redelivers the same record more than once (e.g. a
    # retry mid-pagination after a transient network error/502), the record
    # is safely overwritten instead of being inserted again as a duplicate.
    # Collections with no key configured keep the old insert_many() behaviour.
    pk_mongofind = config_this_coll.get("pk_mongofind")
    pk_jsonpath = config_this_coll.get("pk_jsonpath")

    # postprocessors for e.g. filtering soft-deleted options out of Picklist
    # "postproc": {
    #     "extraction": {
    #         "array_remove": ["$.path.to.array[?(@.array_path == 'condition')]"],
    #         "move": {"$.from.path": "$.to.path"},
    #         "add": {"$.dest.path": "value_with_placeholder-{ts}"}
    #     },
    #     "file": {},
    #     "mongodb": {}
    # }
    postproc = defaultdict(lambda: defaultdict(list))
    for stage, op_groups in config_this_coll.get("postproc", {}).items():
        if stage not in ["extraction", "file", "mongodb"]:
            logger.warning(f"Unkonwn postprocessing stage {stage} - ignoring this")
            continue
        for op_type, ops in op_groups.items():
            if op_type == "array_remove":
                for path in ops:
                    postproc[stage][op_type].append(
                        lambda doc, p=path: jp.parse(p).filter(lambda _: True, doc))
            elif op_type == "add":
                for path, value in ops.items():
                    postproc[stage][op_type].append(
                        lambda doc, p=path, v=value:
                            add_field(doc, p, format_object(v, run_ctx)) )
            elif op_type == "move":
                for src_path, dest_path in ops.items():
                    postproc[stage][op_type].append(
                        lambda doc, s=src_path, d=dest_path:
                            copy_path_value(doc, s, d, del_src=True) )
            else:
                logger.warning(f"Unkonwn postprocessing op type {op_type} "
                                f"at stage {stage} - ignoring this")
# if no add_fields were specified
if config_this_coll.get("postproc", {}).get("extraction", {}).get("add") is None:
    postproc["extraction"]["add"] = [
        lambda doc: add_field(doc, "$.publishDate", start_time)]

# DBTITLE 1,main: query; dump JSON to file and/or write to MongoDB
doc_samples = []
skel = {}
n_docs = 0
with Timer() as t:
    api_style = config_this_coll.get('api', "odata")

    # prepare the query
    query_key = "query_incremental" if args.incremental else "query"
    query = config_this_coll.get(query_key, '')
    query_string = None
    if query:
        if isinstance(query, list):
            query_string = ('\n' if api_style=="sfapi" else '').join(query)
        elif isinstance(query, str):
            query_string = query

    if not query_string and api_style == "sfapi":
        raise Exception(f"{query_key} not found or has an empty value in the configuration - exiting")

    if api_style == "odata":
        logger.info("OData base URL: %s", urljoin(
            config_sf['url_odata_base'],
            config_this_coll.get("respath", args.collection)))
    else:
        logger.info("SFAPI URL: %s", config_sf["url_sfapi"])
    if query_string:
        if api_style=="sfapi":
            sfapi_ctx = dict(run_ctx)
            sfapi_ctx["ts"] = sfapi_ctx["ts"].strftime("%Y-%m-%dT%H:%M:%SZ")
            if "ts_incr" in sfapi_ctx:
                sfapi_ctx["ts_incr"] = sfapi_ctx["ts_incr"].strftime("%Y-%m-%dT%H:%M:%SZ")
            query_string = query_string.format_map(sfapi_ctx)
        else:
            query_string = query_string.format_map(run_ctx)
        logger.info("Query:")
        for query_line in query_string.splitlines():
            logger.info(f"  {query_line}")

    if api_style == "odata":
        try:
            # http_session.mount(config_sf["url_odata_base"],
            #     TimeoutHTTPAdapter(
            #         timeout=tuple(config["general"]["http"]["odata"].get("timeout", [])),
            #         max_retries=Retry(*config["general"]["http"]["odata"]["retry"],
            #                            raise_on_status= False) ) )
            retry_logic = config["general"]["http"]["retry"]["odata"]
        except KeyError:
            logger.warning(f"No retry logic was found for OData")
            retry_logic = {}
        http_session_query = ExponentialBackoffSession(**retry_logic)
        http_session_query.verify = False
        http_session_query.proxies.update(config["general"]["http"]["proxies"])
        doc_batch_source = query_odata(
            http_session_query, oauth_token,
            config_sf['url_odata_base'], config_this_coll.get("respath", args.collection), query_string,
            results_path=config_this_coll.get("results_path", "$.d.results[*]"),
            yield_pages=True)
    else:
        sfapi_url = config_sf["url_sfapi"]
        nsmap = {"soapenv": "http://schemas.xmlsoap.org/soap/envelope/",
                 "sfo":"urn:sfobject.sfapi.successfactors.com"}
        for ns_name, ns_url in nsmap.items():
            ET.register_namespace(ns_name, ns_url)
        try:
            retry_logic = config["general"]["http"]["retry"]["sfapi"]
            # http_session.mount(config_sf["url_sfapi"],
            #     TimeoutHTTPAdapter(
            #         timeout=tuple(config["general"]["http"]["sfapi"].get("timeout", [])),
            #         max_retries=Retry(*config["general"]["http"]["sfapi"]["retry"],
            #                            raise_on_status= False) ) )
        except KeyError:
            logger.warning(f"No retry logic was found for SFAPI")
            retry_logic = {}
        http_session_query = ExponentialBackoffSession(**retry_logic)
        http_session_query.verify = False
        http_session_query.proxies.update(config["general"]["http"]["proxies"])
        sfapi_sessionid = sfapi_login(http_session_query, sfapi_url, oauth_token)

        doc_batch_source = query_sfapi(
            http_session_query, sfapi_sessionid,
            sfapi_url, query_string,
            doc_xpath=config_this_coll.get("doc_xpath", "./sfo:sfobject"), nsmap=nsmap,
            # v2 FIX: pagesize is now configurable per collection (was always
            # hardcoded to the function's default of 800). Defaults to 800
            # if not set, so this changes nothing for collections that don't
            # specify it.
            pagesize=config_this_coll.get("pagesize", 800),
            yield_pages=True)

    with open_write_smart(out_path) as out_f:
        # register a function to delete the JSON file on exit
        atexit.register(cleanup_file, out_path)

        for doc_batch in doc_batch_source:
            # convert SFAPI's XML to dict
            if api_style == "sfapi":
                for i, doc in enumerate(doc_batch):
                    doc_batch[i] = xml_to_dict(doc, config_this_coll.get("list_paths"))

            # sample and analyse every 10th doc between 0 and 100
            # if doc_idx <= 100 and doc_idx % 10 == 0:
            #     doc_samples.append(doc)
            #     skel = merge_skel(skel, build_skel(doc))

            if postproc.get("extraction"):
                # logger.info("postprocessing in place")
                with Timer() as t_postproc:
                    for op_type, ops in postproc.get("extraction").items():
                        if ops:
                            for f in ops:
                                for doc in doc_batch: f(doc)  # in-place
                # logger.info(f"  {t_postproc.elapsed:.1f}s")

            # file output
            if out_f:
                for doc in doc_batch:
                    if postproc.get("file"):
                        _doc = deepcopy(doc)  # workr on a copy to avoid affecting MongoDB
                        for op_type, ops in postproc.get("file").items():
                            if ops:
                                for f in ops: f(_doc)
                    else:
                        _doc = doc
                    json.dump(_doc, default=json_default_serializer, fp=out_f)
                    out_f.write('\n')

            # MongoDB output (batch mode)
            if args.mongo_conn and doc_batch:
                if postproc.get("mongodb"):
                    for op_type, ops in postproc.get("mongodb").items():
                        if ops:
                            for f in ops:
                                for doc in doc_batch: f(doc)  # in-place
                # v2 FIX: upsert on the configured key whenever one exists,
                # in both full and incremental mode (was: incremental only).
                # A plain insert_many() has no protection against the source
                # redelivering the same batch (e.g. after a retry mid-
                # pagination) - it would just insert the repeat as new,
                # duplicate documents. Upserting on a stable key makes a
                # repeat write a harmless overwrite instead.
                if pk_mongofind and pk_jsonpath:
                    ops = []
                    key_finder = jp.parse(pk_jsonpath)
                    for doc in doc_batch:
                        matches = key_finder.find(doc)
                        if not matches:
                            # v2 FIX: don't let one document with a missing
                            # key crash the whole batch/run - skip and log it.
                            logger.warning("Skipping document with no value at %s", pk_jsonpath)
                            continue
                        filter_key = matches[0].value
                        ops.append(ReplaceOne({pk_mongofind: filter_key},
                                               replacement=doc, upsert=True))
                    if ops:
                        result = mongo_db[out_mongo_collname_tmp].bulk_write(ops)
                        logger.info(f"Modified: {result.modified_count} / Upserted: {result.upserted_count}")
                else:
                    # no natural key configured for this collection -
                    # unchanged, pre-existing behaviour
                    result = mongo_db[out_mongo_collname_tmp].insert_many(doc_batch)

            n_docs += len(doc_batch)
            logger.info(f"{n_docs} in {t.elapsed:.1f} s, avg {(n_docs/t.elapsed):.1f} docs/s")
            if args.max_docs is not None and n_docs >= args.max_docs:
                logging.info(f"max_docs={args.max_docs} reached - stopping")
                break
        out_data_size = out_path.stat().st_size / 1024 / 1024

# DBTITLE 1,main: mongodb: create indexes, rename to actual name
if args.mongo_conn:
    if not args.incremental:
        for idx_keys in config_this_coll.get("indices", []):
            logger.info(f"Creating index on {idx_keys!r}")
            mongo_db[out_mongo_collname_tmp].create_index(idx_keys)
    logger.info("Renaming %r -> %r", out_mongo_collname_tmp, out_mongo_collname)
    mongo_db[out_mongo_collname_tmp].rename(out_mongo_collname)

if args.azupload:
    azupload_success = None
    try:
        config_azupload = (config["azupload"].get("#common", {}) |
                            config["azupload"][args.azupload])
    except KeyError:
        logger.warning(f"Azure config {args.azupload} not found in config.json - ignoring")
    else:
        azure_url = config_azupload['url'].format_map(run_ctx)
        azupload_success = azupload(out_path, azure_url, config_azupload)

logger.info(f"Total docs extracted/updated:  {n_docs}")
logger.info(f"Total time:                    {timedelta(seconds=t.elapsed)}")
if args.mongo_conn:
    logger.info(f"MongoDB collection            {out_mongo_collname}")
# cosmetic FIX (post-transcription, not in the real production line as
# screenshotted): out_data_size (set above, near line 1037) is already in
# MB - this used to divide by 1024/1024 a second time, printing an
# effectively-always-0.0 value regardless of real file size.
logger.info("File size:                     %.1f MB", out_data_size)
if args.azupload:
    if azupload_success:
        logger.info("Azure URL:                     %s", azure_url)
    else:
        raise Exception(f"Uploading to Azure Storage failed")

# DBTITLE 1,sample document
# if doc_samples:
#     s = doc_samples[-1].copy()
#     if '_id' in s:
#         s['_id'] = str(s['_id'])
#     print(json.dumps(s, indent=2))  # everything is a string or null

# DBTITLE 1,inferred structure
# if skel:
#     pprint(skel)

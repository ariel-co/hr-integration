#!/usr/bin/env python3.12

import logging
import logging.config

from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import time
from typing import List, Tuple
import json
from pathlib import Path
import argparse
from datetime import datetime, timezone, timedelta
import sys
import io
import shlex
import uuid
import string

import shutil
class ShortMessageFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.width = shutil.get_terminal_size((120, 60)).columns
    def format(self, record):
        original_msg = super().format(record)
        if len(original_msg) > self.width:
            return original_msg[:self.width-3] + '...'
        else:
            return original_msg

class _datetime(datetime):
    def __str__(self):
        return self.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        # if (dotidx := txt.index('.')) > -1:
        #     txt = txt[:dotidx+3] + txt[dotidx+6:]
        # return txt

logging_cfg = {
    'version': 1,
    'disable_existing_loggers': False, # otherwise other modules are muted
    'loggers': {
        '': { # root logger
            'level': 'INFO',
            'handlers': ['stdout'] } },
    'formatters': {
        'short': {
            '()': ShortMessageFormatter,
            'format': '%(asctime)s %(levelname)-8s (master) %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S' },
        'full': {
            'format': '%(asctime)s %(levelname)-8s (master) %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S' } },
    'handlers': {
        'stdout': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'short',
            'stream': 'ext://sys.stdout' } } }

# unbuffer stdout, stderr for use with nohup (Python 3.7+)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout = io.TextIOWrapper(
        open(sys.stdout.fileno(), mode='wb', buffering=0))
    sys.stderr = io.TextIOWrapper(
        open(sys.stderr.fileno(), mode='wb', buffering=0))
    # sys.stdout.reconfigure(write_through=True)

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

# replace a single field in a string with placeholders for str.format()
def replace_field_by_name(template, field_to_hide, replacement):
    result = []
    for literal, field, fmt, conv in string.Formatter().parse(template):
        # 1. Append the literal text before the field
        result.append(literal)

        # 2. Check if this is the field we want to replace
        if field == field_to_hide:
            result.append(replacement)
        # 3. Keep other fields exactly as they are
        elif field is not None:
            # Reconstruct the original field syntax
            fmt_str = f":{fmt}" if fmt else ""
            conv_str = f"!{conv}" if conv else ""
            result.append(f"{{{field}{conv_str}{fmt_str}}}")

    return "".join(result)

script_path = Path(__file__).resolve()
script_dir = script_path.parent
config = json.load(open(script_dir / "config.json"))

start_time = _datetime.now(timezone.utc)
batch_id = str(uuid.uuid4())

logging.config.dictConfig(logging_cfg) # this configures the root logger (named '')
logger = logging.getLogger(__name__)   # child of the root logger
try:
    log_dir = (script_dir / config["general"]["logs"]["path"]).parent
except KeyError:
    log_dir = script_dir / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
file_handler = logging.FileHandler(log_dir / f"{start_time:%Y-%m-%d_%H-%M-%S}-master-{batch_id}.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(logging_cfg["formatters"]["full"]['format']))
logging.getLogger().addHandler(file_handler)

class HelpfulArgParser(argparse.ArgumentParser):
    def __init__(self, raise_on_error: bool=False, *args, **kwargs):
        self._raise_on_error = raise_on_error
        super().__init__(*args, **kwargs)

    def error(self, message):
        print(f"Error: {message}\n", file=sys.stderr)
        self.print_help()
        if self._raise_on_error:
            raise Exception(message)
        else:
            sys.exit(2)

argparser = HelpfulArgParser()
argparser.add_argument('sf', metavar='|'.join(config["sf"].keys()),
                        help="SAP SF connection params (see config.json: sf)")
argparser.add_argument('resources', metavar='Resource|-Resource', nargs='*',
                        help="SF resources to extract or exclude (see collections.json)")
argparser.add_argument('--coll-config', metavar='filename',
                        help="Collection (task) config file (default SCRIPT_DIR/collections.json)")
argparser.add_argument('--parallel', type=int, default=5, metavar='N',
                        help="How many extractions at the same time")

## default behaviour: run all jobs, passing --incremental to those
## that have "query_incremental" in the config
argparser.add_argument("--incremental", action="store_true",
                        help="Only run incremental updates")
argparser.add_argument("--full", action="store_true",
                        help="Only run jobs without incremental option")
argparser.add_argument("--force-full", action="store_true",
                        help="Run unconditionally in full mode")
argparser.add_argument("--compress", "--gzip", action="store_true")
argparser.add_argument("--mongo-conn",
                        help="MongoDB connection (see config.json)",
                        metavar='|'.join(c['name'] for c in config['mongodb']['connections']) )
argparser.add_argument("--azupload",
                        help="Azure upload destination - see config.json")
argparser.add_argument("--max-docs", type=int, metavar='N',
                        help="max docs to output (for testing)")
argparser.add_argument("--dry-run", action='store_true', help="Just print the commands")

args, extra_args = argparser.parse_known_args(sys.argv[1:])

# check for arg compatibility
if args.incremental and args.full:
    logger.error("Only one of --incremental, --full can be used")
    sys.exit(1)

logger.info(f"START")

config_tasks = json.load(open((args.coll_config or script_dir / "collections.json")))

opt_args = ["--batch-id", batch_id]
if args.coll_config:
    opt_args += ["--coll-config", args.coll_config]
if args.mongo_conn:
    opt_args += ["--mongo-conn", args.mongo_conn]
if args.compress:
    opt_args += ["--compress"]
if args.azupload:
    opt_args += ["--azupload", args.azupload]
if args.max_docs:
    opt_args += ["--max-docs", args.max_docs]

only_these = set(args.resources)
              # set(arg[1:] for arg in extra_args if not arg.startswith('-'))
not_these = set()
for i in range(len(extra_args)-1, -1, -1):
    if extra_args[i].startswith('-') and not extra_args[i].startswith('--'):
        not_these.add(extra_args.pop(i)[1:])

if only_these and not_these:
    logger.error("Cannot use allow and exclude collection arguments together")
    sys.exit(1)

# create collection list
if only_these:
    tasks = { name: task for name, task in config_tasks.items()
              if name in only_these}
else:
    tasks = { name: task for name, task in config_tasks.items()
              if name not in not_these}

if args.incremental:
    tasks = { name: task for name, task in tasks.items()
              if 'query_incremental' in task}
elif args.full:
    tasks = { name: task for name, task in tasks.items()
              if 'query_incremental' not in task}

script_path_str = str(script_dir / "extract_collection.py")
commands = []
for name, task in tasks.items():
    cmd = [script_path_str, args.sf, name, *map(str, opt_args)]
    if 'query_incremental' in task and not args.force_full:
        cmd.append("--incremental")
    cmd.extend(["--coll-ts", str(start_time)])
    commands.append(cmd)

logger.info(f"{len(tasks)} tasks found")

def run_subprocess(cmd_args: List[str], env=None) -> Tuple[List[str], int, float]:
    """Runs the subprocess and returns data to the callback."""
    with Timer() as t_process:
        # v2 FIX: subprocess.run() only takes `args` positionally - every
        # other parameter (including env) is keyword-only. The old call
        # passed env positionally, which raises a TypeError at runtime.
        result = subprocess.run(cmd_args, env=env, text=True)
    return cmd_args, result.returncode, t_process.elapsed

if commands:
    if not args.dry_run:

        results = []
        # callback for job end
        def log_result(result: Tuple[List[str], int, float]):
            """
            This function runs in the MAIN process every time a worker finishes.
            It receives the return value of `run_subprocess`.
            """
            results.append(result)
            cmd_args, exit_code, duration = result
            logger.info(f"Finished: {' '.join(cmd_args)} rc={exit_code}, {duration:.4f}s")

        logger.info("Starting tasks...")
        with Timer() as t_all:
            with ThreadPoolExecutor(args.parallel) as executor:
                futures = [
                    executor.submit(run_subprocess, [*cmd, "--batch-id", batch_id])
                    for cmd in commands ]
                for future in as_completed(futures):
                    cmd_args, exit_code, duration = future.result()
                    if exit_code==0:
                        logger.info("%s finished in %s - OK",
                                    cmd_args[2], timedelta(seconds=duration))
                    else:
                        logger.error("%s finished in %s - FAIL (rc=%d)",
                                     cmd_args[2], timedelta(seconds=duration), exit_code)
                    results.append((cmd_args, exit_code, duration))
        logger.info(f"All tasks have completed in {timedelta(seconds=int(t_all.elapsed))}")
        if any(rc != 0 for cmd_args, rc, duration in results):
            logger.error("Some processes may have failed (result code != 0)")
            logger.error(f"{'Collection':40s} |  RC | log file")
        failed_cmds = 0
        incr_cmds = 0
        for cmd_args, exit_code, duration in results:
            incr = "--incremental" in cmd_args
            incr_cmds += 1 if incr else 0
            if exit_code != 0:
                failed_cmds += 1
                # I'm cheating here because I know that log_id=batch_id[:8]
                # v2 FIX: the log path template uses {ts}, not {start_time} -
                # the old code hid/filled 'start_time', which doesn't exist
                # in the template, causing KeyError: 'ts' whenever this ran
                # (i.e. whenever at least one collection failed).
                log_pat = (replace_field_by_name(config["general"]["logs"]["path"], 'ts', '*')
                           .format(coll=cmd_args[2], batch_id=batch_id, log_id=batch_id[:8],
                                   ts=start_time, incr="incr" if incr else "full"))
                logger.error(f"{cmd_args[2]:40s} | {exit_code:>3} | {log_pat}")
        logger.info("Total collections:  %3d", len(commands))
        logger.info("Incremental loads:  %3d", incr_cmds)
        logger.info("Failed collections: %3d", failed_cmds)
        logger.info("Total batch time:   %s", timedelta(seconds=t_all.elapsed))
        if failed_cmds == 0:
            logger.info("COMPLETED")
        else:
            logger.error("FAIL")
    else:
        for cmd in commands:
            print(shlex.join(cmd))

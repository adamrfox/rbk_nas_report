#!/usr/bin/python

from __future__ import print_function
import rubrik_cdm
import sys
import os
import getopt
import getpass
import urllib3
urllib3.disable_warnings()
import datetime
import pytz
import time
import threading
import random
import io

try:
    import queue
except ImportError:
    import Queue as queue
import shutil
from random import randrange
from pprint import pprint


def python_input(message):
    if int(sys.version[0]) > 2:
        val = input(message)
    else:
        val = raw_input(message)
    return (val)

def file_compare(job_ptr, file, cmp_id):
    f_cur = ""
    f_done = False
    while not f_done:
        if f_cur:
            api_endpoint = '/search?managed_id=' + str(share_id) + '&query_string=' + str(file) + '&cursor=' + f_cur
        else:
            api_endpoint = '/search?managed_id=' + str(share_id) + '&query_string=' + str(file)
        rbk_cmp = rubrik_cluster[job_ptr]['session'].get('internal', api_endpoint, timeout=timeout)
        for sr in rbk_cmp['data']:
#            print("SR: " + sr['path'] + " :: " + file)
            if sr['path'] == file:
                for v in sr['fileVersions']:
                    if v['snapshotId'] == cmp_id:
                        return(True)
            if rbk_cmp['hasMore'] == True:
                f_cur = rbk_cmp['nextCursor']
            else:
                f_done = True
    return(False)

def file_compare_new(job_ptr, path, file_in_base_dir, cmp_id, fh):
    cmp_done = False
    cmp_offset = 0
    file_in_cmp_dir = {}
    while not cmp_done:
        params = {'path': path, 'offset': cmp_offset}
        try:
            cmp_walk = rubrik_cluster[job_ptr]['session'].get('v1','/fileset/snapshot/' + str(cmp_id) + '/browse',
                                                          params=params, timeout=timeout)
        except rubrik_cdm.exceptions.APICallException:
            dprint("Directory not found: " + path)
            oprint(path + ',' + '0,NEW,DIRECTORY', fh)
            for f in file_in_base_dir.keys():
                if f.startswith(path):
                    oprint(f + ',' + str(file_in_base_dir[f]['size']) + ',NEW,FILE', fh)
            return
        for cmp_ent in cmp_walk['data']:
            cmp_offset += 1
            if cmp_ent['fileMode'] == "file":
                file_in_cmp_dir[path + delim + str(cmp_ent['filename'])] = {'size': cmp_ent['size'], 'time': time.mktime(time.strptime(cmp_ent['lastModified'][:-5], '%Y-%m-%dT%H:%M:%S'))}
        if not cmp_walk['hasMore']:
            cmp_done = True
    for base_file in file_in_base_dir.keys():
        try:
            file_in_cmp_dir[base_file]
        except:
            oprint(base_file + ',' + str(file_in_base_dir[base_file]['size']) + ",NEW,FILE", fh)
            continue
        if file_in_base_dir[base_file]['time'] != file_in_cmp_dir[base_file]['time']:
            oprint(base_file + ',' + str(file_in_base_dir[base_file]['size']) + ",UPDATED,FILE", fh)


def walk_tree(rubrik, id, cmp_id, delim, path, parent, files_to_restore, outfile):
    offset = 0
    done = False
    file_count = 0
    files_in_base_dir = {}
    files_in_cmp_dir = {}
    if delim == "\\" and path == "/":
        job_path = path.split(path)
    else:
        job_path = path.split(delim)
    job_path_s = '_'.join(job_path)
    if len(job_path_s) > 128:
        fn = random.randint(0, 1024 * 1000)
        fn = '%x' % fn
        job_path_s = job_path_s[:128] + "_" + str(fn)
    job_path_s = job_path_s.replace(':', '_')
    job_id = str(outfile) + str(job_path_s) + '.part'
    fh = open(job_id, "w")
    while not done:
        job_ptr = randrange(len(rubrik_cluster))
        params = {"path": path, "offset": offset}
        if offset == 0:
            if VERBOSE:
                print("Starting job " + path + " on " + rubrik_cluster[job_ptr]['name'])
            else:
                print(' . ', end='')
        rbk_walk = rubrik_cluster[job_ptr]['session'].get('v1', '/fileset/snapshot/' + str(id) + '/browse',
                                                          params=params, timeout=timeout)
        file_count = 0
        for dir_ent in rbk_walk['data']:
            offset += 1
            file_count += 1
            if dir_ent == parent:
                return
            if dir_ent['fileMode'] == "file":
#               if path == "\\":
#                   path = ""
#                print("FILE: " + path + delim + str(dir_ent['filename']))
                if not FASTER_COMPARE:
                    if not file_compare(job_ptr, path + delim + str(dir_ent['filename']), cmp_id):
                        oprint(path + delim + str(dir_ent['filename'] + "," + str(dir_ent['size'])), fh)
                else:
                    files_in_base_dir[path + delim + str(dir_ent['filename'])] = {'size': dir_ent['size'], 'time': time.mktime(time.strptime(dir_ent['lastModified'][:-5], '%Y-%m-%dT%H:%M:%S'))}
            elif dir_ent['fileMode'] == "directory" or dir_ent['fileMode'] == "drive":
                if dir_ent['fileMode'] == "drive":
                    new_path = dir_ent['filename']
                elif delim == "/":
                    if path == "/":
                        new_path = "/" + dir_ent['path']
                    else:
                        new_path = path + "/" + dir_ent['path']
                else:
                    if path == "\\":
                        new_path = "\\" + dir_ent['path']
                    else:
                        new_path = path + "\\" + dir_ent['path']
                job_queue.put(threading.Thread(name=new_path, target=walk_tree, args=(
                rubrik, id, cmp_id, delim, new_path, dir_ent, files_to_restore, outfile)))
        if not rbk_walk['hasMore']:
            done = True
    if FASTER_COMPARE:
#            pprint(files_in_base_dir)
            file_compare_new(job_ptr, path, files_in_base_dir, cmp_id, fh)
    if file_count == 200000:
        large_trees.put(path)
    fh.close()
    parts.put(job_id)


def generate_report(parts, outfile, LOG_FORMAT):
    if LOG_FORMAT == "log":
        ofh = open(outfile + '.' + LOG_FORMAT, 'wb')
        with open(outfile + '.head', 'rb') as hfh:
            shutil.copyfileobj(hfh, ofh)
        hfh.close()
        ofh.close()
    else:
        ofh = open(outfile + '.' + LOG_FORMAT, 'w')
        print("Base Backup: " + snap_list[int(base_index)][1] + "\nCompared to: " + snap_list[int(compare_index)][1] , file=ofh)
        ofh.close()
    while True:
        if parts.empty():
            time.sleep(10)
            if exit_event.is_set():
                break
            else:
                continue
        name = parts.get()
        dprint("CONSOLIDATING " + name)
        with open(name, 'rb') as rfh:
            with open(outfile + '.' + LOG_FORMAT, 'ab') as wfh:
                shutil.copyfileobj(rfh, wfh)
        rfh.close()
        wfh.close()
        if not DEBUG:
            dprint("Deleting " + name)
            os.remove(name)


def get_job_time(snap_list, id):
    time = ""
    dprint("JOB=" + id)
    for snap in snap_list:
        if snap[0] == id:
            time = snap[1]
            break
    return (time)


def dprint(message):
    if DEBUG:
        if int(sys.version[0]) > 2:
            dfh = open(debug_log, 'a', encoding='utf-8')
            dfh.write(message + "\n")
        else:
            dfh = io.open(debug_log, 'a', encoding='utf-8')
            dfh.write(unicode(message) + "\n")
        dfh.close()
    return ()


def oprint(message, fh):
    if not fh:
        print(message)
    else:
        fh.write(message + "\n")


def log_clean(name):
    files = os.listdir('.')
    for f in files:
        if f.startswith(name) and (f.endswith('.part') or f.endswith('.head')):
            os.remove(f)


def get_rubrik_nodes(rubrik, user, password, token):
    node_list = []
    cluster_network = rubrik.get('internal', '/cluster/me/network_interface')
    dprint("CLUSTER_NETWORK: ")
    dprint(str(cluster_network))
    for n in cluster_network['data']:
        if n['interfaceType'] == "Management":
            if token:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], api_token=token)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ".  Skipping\n")
                    continue
            else:
                try:
                    rbk_session = rubrik_cdm.Connect(n['ipAddresses'][0], user, password)
                except Exception as e:
                    sys.stderr.write("Error on " + n['ipAddresses'][0] + ": " + str(e) + ".  Skipping\n")
                    continue
            try:
                node_list.append({'session': rbk_session, 'name': n['nodeName']})
            except KeyError:
                node_list.append({'session': rbk_session, 'name': n['node']})
    dprint("NODE_LIST: " + str(node_list))
    return (node_list)


def log_job_activity(rubrik, outfile, fs_id, snap_data):
    ev_series_id = ""
    event_series_id_save = ""
    dprint(str(snap_data))
    snap_time_dt = datetime.datetime.strptime(snap_data[1], "%Y-%m-%d %H:%M:%S")
    snap_time_epoch = (snap_time_dt - datetime.datetime(1970, 1, 1)).total_seconds()
    dprint(str(snap_time_epoch))
    events = rubrik.get('v1', '/event/latest?limit=1024&event_type=Backup&object_ids=' + str(fs_id), timeout=timeout)
    for ev in events['data']:
        if ev['latestEvent']['eventType'] != "Backup" or ev['eventSeriesStatus'] not in (
        'Success', 'Failure', 'SuccessWithWarnings'):
            continue
        ev_dt = datetime.datetime.strptime(ev['latestEvent']['time'][:-5], "%Y-%m-%dT%H:%M:%S")
        ev_dt_epoch = (ev_dt - datetime.datetime(1970, 1, 1)).total_seconds()
        dprint("EV_DT: " + str(ev_dt_epoch))
        if ev_dt_epoch < snap_time_epoch:
            ev_series_id = event_series_id_save
            dprint("selected")
            break
        else:
            event_series_id_save = ev['latestEvent']['eventSeriesId']
    if not ev_series_id:
        ev_series_id = event_series_id_save
    dprint("EVENT_SERIES_ID: " + ev_series_id)
    if ev_series_id:
        event_series = rubrik.get('v1', '/event_series/' + str(ev_series_id), timeout=timeout)
        hfp = open(outfile + '.head', "w")
        hfp.write('Backup:' + event_series['location'] + '\n')
        hfp.write('Started: ' + event_series['startTime'][:-5] + '\n')
        hfp.write('Ended: ' + event_series['endTime'][:-5] + '\n')
        hfp.write('Duration: ' + event_series['duration'] + '\n')
        hfp.write('Logical Size: ' + str(event_series['logicalSize']) + '\n')
        hfp.write('Throughput: ' + str(event_series['throughput']) + ' Bps\n\n')
        for e in reversed(event_series['eventDetailList']):
            e_dt = datetime.datetime.strptime(e['time'][:-5], "%Y-%m-%dT%H:%M:%S")
            e_dt_s = datetime.datetime.strftime(e_dt, "%Y-%m-%d %H:%M:%S")
            message_list = e['eventInfo'].split('"')
            message = message_list[3].replace('\\\\', '\\')
            hfp.write(e_dt_s + ' ' + e['eventSeverity'] + ' ' + message + '\n')
    else:
        hfp = open(outfile + '.head', "w")
        hfp.write("No job activity log found.")
    hfp.write('\n')
    hfp.close()

def job_queue_length(thread_list):
    list_check = []
    for thread in threading.enumerate():
        if thread.name in thread_list:
            list_check.append(thread.name)
#    dprint("LIST_CHECK = " + str(list_check))
    dprint("JQD returns " + str(len(list_check)))
    return(len(list_check))

def usage():
    sys.stderr.write(
        "Usage: rbk_fileset_diff.py [-hDrpasl] [-b backup] [-f fileset] [-c creds] [-t token] [-d date] [-m max_threads | -M thread_factor] -o outfile rubrik\n")
    sys.stderr.write("-h | --help : Prints Usage\n")
    sys.stderr.write("-D | --debug : Debug mode.  Prints more information\n")
    sys.stderr.write("-o | --output : Specify an output file.  Don't include an extention. [REQUIRED]\n")
    sys.stderr.write("-b | --backup : Specify backup.  Format is server:share for NAS, host for physical\n")
    sys.stderr.write("-f | --fileset : Specify a fileset for the share\n")
    sys.stderr.write("-c | --creds : Specify cluster credentials.  Not secure.  Format is user:password\n")
    sys.stderr.write("-t | --token : Use an API token instead of credentials\n")
    sys.stderr.write("-M | --thread_factor: Specify the number of threads per node [def:10]\n")
    sys.stderr.write("-m | --max_threads: Specify a maximum number of threads.  Overrides thread factor.\n")
    sys.stderr.write("-p | --physical : Specify a physical fileset backup [default: NAS]\n")
    sys.stderr.write("-s | --single_node : Only use one node of the Rubrik clsuter for API calls\n")
    sys.stderr.write("-l | --latest : Use the latest backup of the fileset\n")
    sys.stderr.write("-d | --date : Specify the exact date of the desired backup\n")
    sys.stderr.write(
        "-a | --all : Report all files in backup.  Default is only files backed up in that specific backkup\n")
    sys.stderr.write("rubrik : Name or IP of the Rubrik Cluster\n")
    exit(0)


if __name__ == "__main__":
    backup = ""
    rubrik = ""
    user = ""
    password = ""
    fileset = ""
    date = ""
    latest = False
    share_id = ""
    restore_job = []
    physical = False
    snap_list = []
    restore_location = ""
    restore_share_id = ""
    restore_host_id = ""
    token = ""
    DEBUG = False
    VERBOSE = False
    REPORT_ONLY = True
    ALL_FILES = False
    FASTER_COMPARE = True
    outfile = ""
    ofh = ""
    timeout = 360
    rubrik_cluster = []
    thread_list = []
    job_queue = queue.Queue()
    max_threads = 0
    thread_factor = 10
    debug_log = "debug_log.txt"
    large_trees = queue.Queue()
    parts = queue.Queue()
    SINGLE_NODE = False
    LOG_FORMAT = "csv"

    optlist, args = getopt.getopt(sys.argv[1:], 'ab:f:c:d:hDst:o:m:M:vplsF:', ["backup=", "fileset=", "creds=", "date=",
                                                                               "help", "debug", "token=", "output=",
                                                                               "max_threads=",
                                                                               "--physical", "--all", "--latest",
                                                                               '--single_node'])
    for opt, a in optlist:
        if opt in ("-b", "--backup"):
            backup = a
        if opt in ("-f", "--fileset"):
            fileset = a
        if opt in ("-c", "--creds"):
            user, password = a.split(":")
        if opt in ("-h", "--help"):
            dprint("Usage called via -h")
            usage()
        if opt in ("-d", "--date"):
            date = a
            date_dt = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
            date_dt_s = datetime.datetime.strftime(date_dt, "%Y-%m-%d %H:%M:%S")
        if opt in ("-D", "--debug"):
            VERBOSE = True
            DEBUG = True
            dfh = open(debug_log, "w")
            dfh.close()
        if opt in ("-t", "--token"):
            token = a
        if opt in ("-o", "--outout"):
            outfile = a
        if opt in ('-s', '--single_node'):
            SINGLE_NODE = True
        if opt in ('-m', '--max_threads'):
            max_threads = int(a)
        if opt in ('-M', '--thread_factor'):
            thread_factor = int(a)
        if opt in ('-v', '--verbose'):
            VERBOSE = True
        if opt in ('-p', '--physical'):
            physical = True
        if opt in ('-a', '--all'):
            ALL_FILES = True
        if opt in ('-l', '--latest'):
            latest = True
        if opt in ('-s', '--single_node'):
            SINGLE_NODE = True
        if opt in ('-F', '--format'):
            if a.lower() == "csv" or a.lower() == "log":
                LOG_FORMAT = a.lower()
            else:
                sys.stderr.write("Invalid log format.  Must be csv,log\n")
                exit(3)
    try:
        rubrik_node = args[0]
    except:
        dprint("Usage called: no Rubrik node")
        usage()
    if not outfile:
        dprint("Usage called: No outfile")
        usage()
    log_clean(outfile)
    if not backup:
        if not physical:
            backup = python_input("Backup (host:share): ")
        else:
            backup = python_input("Backup Host: ")
    if not physical:
        (host, share) = backup.split(':')
    else:
        host = backup
    if not fileset:
        fileset = python_input("Fileset: ")
    if not token:
        if not user:
            user = python_input("User: ")
        if not password:
            password = getpass.getpass("Password: ")
    if not physical:
        host, share = backup.split(":")
        if share.startswith("/"):
            delim = "/"
        else:
            delim = "\\"
        initial_path = delim
    #
    # Find the latest snapshot for the share and  determine the date (2nd newest snap) or use the one provided by the user
    #
    if token:
        rubrik = rubrik_cdm.Connect(rubrik_node, api_token=token)
    else:
        rubrik = rubrik_cdm.Connect(rubrik_node, user, password)
    rubrik_config = rubrik.get('v1', '/cluster/me', timeout=timeout)
    rubrik_tz = rubrik_config['timezone']['timezone']
    local_zone = pytz.timezone(rubrik_tz)
    utc_zone = pytz.timezone('utc')
    if not SINGLE_NODE:
        rubrik_cluster = get_rubrik_nodes(rubrik, user, password, token)
    else:
        rubrik_cluster.append({'session': rubrik, 'name': rubrik_config['name']})
    dprint(str(rubrik_cluster))
    if max_threads == 0:
        max_threads = thread_factor * len(rubrik_cluster)
    print("Using up to " + str(max_threads) + " threads across " + str(len(rubrik_cluster)) + " nodes.")
    if not physical:
        hs_data = rubrik.get('internal', '/host/share', timeout=timeout)
        for x in hs_data['data']:
            if x['hostname'] == host and x['exportPoint'] == share:
                share_id = x['id']
                break
        if share_id == "":
            sys.stderr.write("Share not found\n")
            exit(2)
        fs_data = rubrik.get('v1', str("/fileset?share_id=" + share_id + "&name=" + fileset), timeout=timeout)
    else:
        hs_data = rubrik.get('v1', '/host?name=' + host, timeout=timeout)
        share_id = str(hs_data['data'][0]['id'])
        os_type = str(hs_data['data'][0]['operatingSystemType'])
        dprint("OS_TYPE: " + os_type)
        if os_type == "Windows":
            delim = "\\"
        else:
            delim = "/"
        initial_path = "/"
        if share_id == "":
            sys.stderr.write("Host not found\n")
            exit(2)
        fs_data = rubrik.get('v1', '/fileset?host_id=' + share_id, timeout=timeout)
    fs_id = ""
    for fs in fs_data['data']:
        if fs['name'] == fileset:
            fs_id = fs['id']
            break
    dprint("FS_ID: " + str(fs_id))
    snap_data = rubrik.get('v1', str("/fileset/" + fs_id), timeout=timeout)
    for snap in snap_data['snapshots']:
        s_time = snap['date']
        s_id = snap['id']
        s_time = s_time[:-5]
        snap_dt = datetime.datetime.strptime(s_time, '%Y-%m-%dT%H:%M:%S')
        snap_dt = pytz.utc.localize(snap_dt).astimezone(local_zone)
        snap_dt_s = snap_dt.strftime('%Y-%m-%d %H:%M:%S')
        snap_list.append((s_id, snap_dt_s))
    if latest:
        base_index = len(snap_list) - 1
        base_id = snap_list[-1][0]
        compare_index = len(snap_list) - 2
        compare_id = snap_list[-2][0]
    elif date:
        dprint("TDATE: " + date_dt_s)
        for i, s in enumerate(snap_list):
            dprint(str(i) + ": " + s[1])
            if date_dt_s == s[1]:
                dprint("MATCH!")
                start_index = i
                start_id = snap_list[i][0]
    else:
        for i, snap in enumerate(snap_list):
            print(str(i) + ": " + snap[1] + "  [" + snap[0] + "]")
        valid = False
        while not valid:
            backups_to_diff = python_input("Select Backups [a,b]: ")
            (base_index, compare_index) = backups_to_diff.split(',')
            try:
                base_id = snap_list[int(base_index)][0]
            except (IndexError, TypeError, ValueError) as e:
                print("Invalid Base Index: " + str(e))
                continue
            try:
                compare_id = snap_list[int(compare_index)][0]
            except (IndexError, TypeError, ValueError) as e:
                print("Invalid Compare Index: " + str(e))
                continue
            valid = True
    valid = False
    print("Backup:     " + snap_list[int(base_index)][1] + " [" + base_id + "]")
    print("Compare to: " + snap_list[int(compare_index)][1] + " [" + compare_id + "]")
    if not latest and not date:
        go_s = python_input("Is this correct? (y/n): ")
        if not go_s.startswith('Y') and not go_s.startswith('y'):
            exit(0)
    current_index = int(base_index)
#    if LOG_FORMAT == "log":
#        log_job_activity(rubrik, outfile, fs_id, snap_list[current_index])
    files_to_restore = []
    threading.Thread(name=outfile, target=walk_tree, args=(rubrik, snap_list[int(base_index)][0], snap_list[int(compare_index)][0],
                                                           delim, initial_path, {}, files_to_restore, outfile)).start()
    thread_list.append(outfile)
    print("Waiting for jobs to queue")
    time.sleep(20)
    exit_event = threading.Event()
    threading.Thread(name='report', target=generate_report, args=(parts, outfile, LOG_FORMAT)).start()
    first = True
    while first or not job_queue.empty() or not parts.empty() or (parts.empty() and job_queue_length(thread_list)):
        first = False
        jql = job_queue_length(thread_list)
        if jql < max_threads and not job_queue.empty():
#            dprint(str(list(job_queue.queue)))
            job = job_queue.get()
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(jql))
            dprint("Started job: " + str(job))
            job.start()
            thread_list.append(job.name)
        elif not job_queue.empty():
            time.sleep(10)
            print("\nQueue: " + str(job_queue.qsize()))
            print("Running Threads: " + str(jql))
        else:
            if DEBUG:
                dprint(str(threading.active_count()) + " running:")
                for t in threading.enumerate():
                    dprint("\t " + str(t.name))
                dprint('\n')
            if jql > 0:
                print("\nWaiting on " + str(jql) + " jobs to finish.")
            time.sleep(10)
    print("\nGenerating Report")
    if not large_trees.empty():
        print("NOTE: There is an default API browse limit of 200K files per directory.")
        print("The following directories could have more than 200K files:")
        for d in large_trees.queue:
            print(d)
        print("\nThis value can be raised by Rubrik Support. If you need this, open a case with Rubrik")
    exit_event.set()
    if not DEBUG:
        log_clean(outfile)
    print("done")

##TODO -d option?
#!/usr/bin/python
from __future__ import print_function

import sys
import rubrik_cdm
import getopt
import getpass
import urllib3
urllib3.disable_warnings()
import datetime
import pytz

def usage():
    sys.stderr.write("Usage: rbk_nas_report.py [hDlp] [-b backup] [-f fileset] [-d date] [-c creds] [-t token] [-o file] rubrik\n")
    sys.stderr.write("-h | --help : Prints Usage\n")
    sys.stderr.write("-D | --debug : Prints debug messages\n")
    sys.stderr.write("-l | --latest : Use latest backup\n")
    sys.stderr.write("-p | --physical : Fileset is physical\n")
    sys.stderr.write("-b | --backup= : Specify a share.  Format is host:share\n")
    sys.stderr.write("-f | --fileset= : Specify a fileset name\n")
    sys.stderr.write("-d | --date= : Specify a specific date.  Format is 'YYYY-MM-DD HH:MM'\n")
    sys.stderr.write("-c | --creds= : Specify credentials.  Format is user:password\n")
    sys.stderr.write("-t | --token= | Use an API token instead of user/password\n")
    sys.stderr.write("-o | --outfile= : Write output to a file\n")
    sys.stderr.write("rubrik: Name or IP of Rubrik Cluster\n")
    exit (0)

def dprint(message):
    if DEBUG:
        print(message)

def oprint(message, fh):
    if not fh:
        print(message)
    else:
        fh.write(message + "\n")

def python_input (message):
    if int(sys.version[0]) > 2:
        in_val = input(message)
    else:
        in_val = raw_input(message)
    return (in_val)

def walk_tree(rubrik, id, path, parent, delim, fh):
    offset = 0
    done = False
    while not done:
        params = {"path": path, "offset": offset}
        walk = rubrik.get('v1', '/fileset/snapshot/' + str(id) + '/browse', params = params, timeout=60)
        for dir_ent in walk['data']:
            offset += 1
            if dir_ent == parent:
                return
            if dir_ent['fileMode'] == "file":
                if path == delim:
                   oprint(path + str(dir_ent['filename']) + "," + str(dir_ent['size']), fh)
                else:
                    oprint(path + delim + str(dir_ent['filename']) + "," + str(dir_ent['size']), fh)
            elif dir_ent['fileMode'] == "directory" or dir_ent['fileMode'] == "drive":
                if dir_ent['fileMode'] == "drive":
                    new_path = dir_ent['filename']
                elif path == delim:
                    new_path = delim + dir_ent['path']
                else:
                    new_path = path + delim + dir_ent['path']
                walk_tree(rubrik, id, new_path, dir_ent, delim, fh)
        if not walk['hasMore']:
            done = True
        else:
            dprint("HASMORE: " + str(offset))

if __name__ == "__main__":
    backup = ""
    rubrik = ""
    user = ""
    password = ""
    fileset = ""
    date = ""
    DEBUG = False
    VERBOSE = False
    latest = False
    physical = False
    share_id = ""
    snap_list = []
    outfile = ""
    fh = ""
    share = ""
    os_type = "NAS"
    token = ""

    optlist, args = getopt.getopt(sys.argv[1:], 'b:f:c:d:hDlo:pvt:', ['backup=', 'fileset=', 'creds=' 'date=', 'help', 'debug', 'latest', 'outfile=', 'physical', 'verbose', 'token='])
    for opt, a in optlist:
        if opt in ('-b', '--backup'):
            backup = a
        if opt in ('-f', '--fileset'):
            fileset = a
        if opt in ('-c', '--creds'):
            user, password = a.split(':')
        if opt in ('-h', '--help'):
            usage()
        if opt in ('-d', '--date'):
            date = a
        if opt in ('-D', '--debug'):
            DEBUG = True
        if opt in ('-l', '--latest'):
            latest = True
        if opt in ('-o', '--outfile'):
            outfile = a
        if opt in ('-p', '--physical'):
            physical = True
        if opt in ('-v', '--verbose'):
            VERBOSE = True
        if opt in ('-t', '--token'):
            token = a
    try:
        rubrik_node = args[0]
    except:
        usage()
    if not backup:
        if not physical:
            backup = python_input("Backup (host:share): ")
        else:
            backup = python_input("Backup Host: ")
    if not fileset:
        fileset = python_input("Fileset: ")
    if not user and not token:
        user = python_input ("User: ")
    if not password and not token:
        password = getpass.getpass("Password: ")
    if not physical:
        host, share = backup.split(':')
    else:
        host = backup
    if token != "":
        rubrik = rubrik_cdm.Connect(rubrik_node, api_token=token)
    else:
        rubrik = rubrik_cdm.Connect (rubrik_node, user, password)
    rubrik_config = rubrik.get('v1', '/cluster/me', timeout=60)
    rubrik_tz = rubrik_config['timezone']['timezone']
    local_zone = pytz.timezone(rubrik_tz)
    utz_zone = pytz.timezone('utc')
    if not physical:
        hs_data = rubrik.get('internal', '/host/share', timeout=60)
        for x in hs_data['data']:
            if x['hostname'] == host and x['exportPoint'] == share:
                share_id = str(x['id'])
                break
        if share_id == "":
            sys.stderr.write("Share not found\n")
            exit (2)
        fs_data = rubrik.get('v1', '/fileset?share_id=' + share_id + "&name=" + fileset, timeout=60)
    else:
        hs_data = rubrik.get('v1', '/host?name=' + host, timeout=60)
        share_id = str(hs_data['data'][0]['id'])
        os_type = str(hs_data['data'][0]['operatingSystemType'])
        if share_id == "":
            sys.stderr.write("Host not found\n")
            exit(2)
        fs_data = rubrik.get('v1', '/fileset?host_id=' + share_id, timeout=60)
    dprint("FS_DATA: " + str(fs_data))
    fs_id = ""
    for fs in fs_data['data']:
        if fs['name'] == fileset:
            fs_id = fs['id']
            break
    dprint("FS_ID = " + fs_id)
    snap_data = rubrik.get('v1', '/fileset/' + str(fs_id), timeout=60)
    for snap in snap_data['snapshots']:
        s_time = snap['date']
        s_time = s_time[:-5]
        s_id = snap['id']
        snap_dt = datetime.datetime.strptime(s_time, '%Y-%m-%dT%H:%M:%S')
        snap_dt_s = pytz.utc.localize(snap_dt).astimezone(local_zone)
        snap_list.append((s_id, snap_dt_s))
    if latest:
        snap_index_id = snap_list[-1][0]
    elif date:
        snap_index_id = ""
        for snap in snap_list:
            if date == str(snap[1])[:-9]:
                snap_index_id = snap[0]
                break
        if not snap_index_id:
            sys.stderr.write("Can't find backup at date: " + date + "\n")
            sys.stderr.write("Date format is 'YYYY-MM-DD HH:MM'\n")
            exit(1)
    else:
        for i, snap in enumerate(snap_list):
            print(str(i) + ": " + str(snap[1]) + " [" + str(snap[0]) + "]")
        valid = False
        while not valid:
            snap_index = python_input("Choose a backup: ")
            try:
                snap_index_id = snap_list[int(snap_index)][0]
            except (IndexError, TypeError, ValueError) as e:
                print("Invalid Index: " + str(e))
                continue
            valid = True
    dprint(snap_index_id)
    if outfile:
        fh = open(outfile, "w")
    if os_type == "Windows" or (os_type == "NAS" and not share.startswith("/")):
        delim = "\\"
        if os_type == "NAS":
            walk_tree(rubrik, snap_index_id, '\\', {}, delim, fh)
    else:
        delim = "/"
    walk_tree(rubrik, snap_index_id, '/', {}, delim, fh)
    if outfile:
        fh.close()

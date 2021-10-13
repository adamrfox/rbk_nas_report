# rbk_nas_report
A script to generate a list of file names and sizes in a Rubrik NAS backup

This script creates a CSV file that lists all of the files in a Rubrik fileset backup and the size of each file.  The code should run on either Python 2 or 3 and requires the Rubrik SDK library which can be installed via pip (pip install rubrik-cdm).  While the script was originally written for NAS filesets, it has since been extended to work with physical filesets as well.

The script is multi-threaded.  By default, it runs 10 threads per node in the cluster.  I believe this to be pretty conservative however the user can cap the total number of threads with the -m flag. This cap can be higher or lower than the deafult.  By default, the threads are spread across all nodes of the Rubrik clsuter in order to spread the load.  The user can force it to use one node (the one specified on the CLI) with the -s flag.

Note:  An output file is required.  Don't put an extenstion, just the name you want to use.  Each thread will generate a .part file which will be consolidated into a single .csv file at the end.

By default, the script will only show files that exist in a particular backup.  This means for an incremental backup it will show the files the are contained in that incremental, not files that did not change since the last backup.  This is done by comparing the mtime of the file vs the time of the previous backup.  If the user desires to see all of the files in an incremental including older files that were not baacked up in that particular incremental, use the -a flag.

<pre>
Usage: rbk_nas_report.py [-hDrpasl] [-b backup] [-f fileset] [-c creds] [-t token] [-d date] [-m max_threads] -o outfile rubrik
-h | --help : Prints Usage
-D | --debug : Debug mode.  Prints more information
-o | --output : Specify an output file.  Don't include an extention. [REQUIRED]
-b | --backup : Specify backup.  Format is server:share for NAS, host for physical
-f | --fileset : Specify a fileset for the share
-c | --creds : Specify cluster credentials.  Not secure.  Format is user:password
-t | --token : Use an API token instead of credentials
-m | --max_threads: Specify a maximum number of threads
-p | --physical : Specify a physical fileset backup [default: NAS]
-l | --latest : Use the latest backup of the fileset
-d | --date : Specify the exact date of the desired backup
-a | --all : Report all files in backup.  Default is only files backed up in that specific backkup
rubrik : Name or IP of the Rubrik Cluster
</pre>

Any CLI option that is not specified on the command line but is required to run will be prompted by the script.  Passwords are not echoed to the screen.
If an output file is not specified with -o, the script will print to the screen (stdout).
File sizes are in bytes.  If other units are needed, feel free to file an issue or contibute.
If not time is specified either with -l or -d, the script will prompt the user with a list of backups from which to choose with time stamps.

There is a default limit for the browse API used by this script that stops finding files at 200K.  This limit applies only to files in the immediate parent directory, not subdirectories.  The script will detect this and alert the user at the end of the run since some files could be missing if a directory has more than 200K files in it.  Rubrik support can up this limit if needed.


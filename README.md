# rbk_nas_report
A script to generate a list of file names and sizes in a Rubrik NAS backup

This script creates a CSV file that lists all of the files in a Rubrik fileset backup and the size of each file.  The code should run on either Python 2 or 3 and requires the Rubrik SDK library which can be installed via pip.

<pre>
Usage: rbk_nas_report.py [hDlp] [-b backup] [-f fileset] [-d date] [-c creds] [-t token] [-o file] rubrik
-h | --help : Prints Usage
-D | --debug : Prints debug messages
-l | --latest : Use latest backup
-p | --physical : Fileset is physical
-b | --backup= : Specify a share.  Format is host:share
-f | --fileset= : Specify a fileset name
-d | --date= : Specify a specific date.  Format is 'YYYY-MM-DD HH:MM'
-c | --creds= : Specify credentials.  Format is user:password
-t | --token= | Use an API token instead of user/password
-o | --outfile= : Write output to a file
rubrik: Name or IP of Rubrik Cluster
</pre>

Any CLI option that is not specified on the command line will be prompted by the script.  Passwords are not echoed to the screen.
If an output file is not specified with -o, the script will print to the screen (stdout).
File sizes are in bytes.  If other units are needed, feel free to file an issue or contibute.
If not time is specified either with -l or -d, the script will prompt the user with a list of backups from which to choose with time stamps.


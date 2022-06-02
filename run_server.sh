#!/bin/bash
# Sync local plugin dir to remote server
# Place this script in root of local dir to push.
# Configure 3 variables at the top.
 
set -e
 
#config
SSHUSER="vfast" # user
HOST="222.252.98.29" # remote server
REMOTEPATH="/home/vfast/Documents/VFAST/" # remote path
 
# Push to remote with these options and flags:
 
# -r        recursive
# -v        verbose
# -u        Update files at the destination ONLY if the source copy has been modified more recently
# --exclude Exclude dirs: .git, bin, tests
# --exclude Exclude *-config.json, all hidden files
# --exclude Exclude all files ending with .sh, .xml, .dist, .md
# --delete  Delete files on the destination ONLY if the source copy no longer exists.
rsync -rvue ssh --exclude '__pycache__' --exclude 'venv' --exclude '.git'  --delete ${PWD} ${SSHUSER}@${HOST}:$REMOTEPATH

ssh ${SSHUSER}@${HOST} 'sudo supervisorctl restart all'
 
echo "*** FINISHED ***"
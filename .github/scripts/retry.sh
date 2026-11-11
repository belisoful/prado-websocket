#!/usr/bin/env bash
#
# Runs a command until it succeeds, a bounded number of times, each attempt under a time limit.
#
#   retry.sh <attempts> <seconds-per-attempt> <command> [argument...]
#
# An attempt that outlives its limit receives SIGTERM, and SIGKILL 15 seconds later, so a stalled
# download ends and the next attempt runs.  The signal comes from this script's user: to stop a
# command that runs as root (apt-get), run the script itself under sudo:
#
#   sudo bash retry.sh 3 120 apt-get update
#
# The script waits 10 seconds after the first failed attempt, 20 after the second, and so on.
# Exit status: 0 once an attempt succeeds, 1 when every attempt failed, 2 on a usage error.
set -u

if [ "$#" -lt 3 ] || ! [[ "$1" =~ ^[1-9][0-9]*$ ]] || ! [[ "$2" =~ ^[1-9][0-9]*$ ]]; then
	echo "usage: $0 <attempts> <seconds-per-attempt> <command> [argument...]" >&2
	exit 2
fi
attempts=$1
limit=$2
shift 2

for ((attempt = 1; attempt <= attempts; attempt++)); do
	timeout --kill-after=15 "$limit" "$@"
	status=$?
	if [ "$status" -eq 0 ]; then
		exit 0
	fi
	# timeout exits 124 when the limit passed, and 137 when the command had to be killed.
	if [ "$status" -eq 124 ] || [ "$status" -eq 137 ]; then
		reason="timed out after ${limit} s"
	else
		reason="exit status ${status}"
	fi
	echo "::warning::Attempt ${attempt} of ${attempts} failed (${reason}): $*"
	if [ "$attempt" -lt "$attempts" ]; then
		sleep $((attempt * 10))
	fi
done
echo "::error::All ${attempts} attempts failed: $*"
exit 1

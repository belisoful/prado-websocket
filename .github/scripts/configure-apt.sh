#!/usr/bin/env bash
#
# Writes apt's network settings for a CI runner.  Run it as root before the first step that uses
# apt, the PHP setup included:
#
#   sudo bash configure-apt.sh
#
# apt then retries a failed download five times, gives up on a connection or transfer that stalls
# for 30 seconds, and waits up to two minutes for the dpkg lock.
set -eu

cat > /etc/apt/apt.conf.d/80-ci-network <<'EOF'
Acquire::Retries "5";
Acquire::http::Timeout "30";
Acquire::https::Timeout "30";
DPkg::Lock::Timeout "120";
EOF

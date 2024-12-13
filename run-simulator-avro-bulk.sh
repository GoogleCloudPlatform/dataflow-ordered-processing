#!/bin/bash

# Start date - this is generated data, so can be anytime
START_DATE="2023-01-01 00:00:00 UTC"
START_SECS=$(date -d "${START_DATE}" +%s)
START_MS=$(($START_SECS * 1000))

# Contracts - number of contracts per group
CONTRACTS=50

# Number of groups of contracts to generate in parallel
CONTRACT_GROUPS=10

# Number of days of continuous activity to generate
DAYS=$((7*1))

# Ctrl-C handler
function clean_up {
    echo "Outstanding jobs: $(jobs -p)"
    kill -9 $(jobs -p)
    echo "Exiting.."
    exit
}
trap clean_up SIGHUP SIGINT SIGTERM

# Generate data
for contracts in $(seq ${CONTRACT_GROUPS}); do
  ./run-simulator.sh --simtime ${START_MS}  --contracts ${CONTRACTS} --zero_contract $((contracts * ${CONTRACTS})) --rate 10 --duration P${DAYS}D --avro data_${contracts}b &
done
wait $(jobs -p)




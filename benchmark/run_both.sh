STARTTIME=$(date +%s)
./cpp_mergeRandom_readRandom.sh

ll /home/pshareghi/workspace/rocksdb-bench/cpp/mergerandom_readrandom

rm -rf /home/pshareghi/workspace/rocksdb-bench/cpp/

./cpp_updateRandom_readRandom.sh

ll /home/pshareghi/workspace/rocksdb-bench/cpp/updaterandom_readrandom

ENDTIME=$(date +%s)
echo "\n#########################"
echo "Took $(($ENDTIME - $STARTTIME)) seconds to complete."
echo "#########################\n\n"

num=1000000
reads=$num
bpl=10485760
overlap=10
mcz=0
del=300000000
levels=6
ctrig=4
delay=-1
stop=12
wbn=3
mbc=20
#mb=67108864 #target_file_size
mb=2097152
wbs=134217728
dds=0
sync=false
t=1
ks=16
vs=8
bs=65536
cs=1048576
of=500000
si=$($num / 10)
#merge_opr=org.rocksdb.LongAddMergeOpr
builtin_merge_operator=uint64add
merge_db="/home/pshareghi/workspace/rocksdb-bench/java/mergerandom_readrandom"
update_db="/home/pshareghi/workspace/rocksdb-bench/java/updaterandom_readrandom"

STARTTIME=$(date +%s)
echo "Merging $num keys in database (using RocksJava merge operator) in random order and then reading them back in the same order...."
../jdb_bench.sh  --benchmarks=mergerandom,readrandom  --disable_auto_compactions=true  --mmap_read=false  --statistics=true  --histogram=true  --threads=$t  --key_size=$ks  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --compression_type=snappy  --cache_numshardbits=4  --open_files=$of  --verify_checksum=true  --db=/home/pshareghi/workspace/rocksdb-bench/java/mergerandom_readrandom  --sync=$sync  --disable_wal=true  --stats_interval=$si  --compression_ratio=0.50  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=false  --cache_remove_scan_count_limit=16 --num=$num --reads=$reads --merge_operator=$merge_opr --min_level_to_compress=$mcz --builtin_merge_operator=$builtin_merge_operator
ENDTIME=$(date +%s)
echo "\n#########################"
echo "It takes $(($ENDTIME - $STARTTIME)) seconds to complete Merge then Read task..."
du --block-size=1 $merge_db
#ll $merge_db
echo "#########################\n\n"

#echo "Reading stuff back..."
#./jdb_bench.sh  --benchmarks=readrandom  --disable_auto_compactions=true  --mmap_read=false  --statistics=true  --histogram=true  --threads=$t  --key_size=$ks  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --compression_type=snappy  --cache_numshardbits=4  --open_files=$of  --verify_checksum=true  --db=/home/pshareghi/workspace/rocksdb-bench/java/mergerandom_readrandom  --sync=$sync  --disable_wal=true  --stats_interval=$si  --compression_ratio=0.50  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=true  --cache_remove_scan_count_limit=16 --num=$num --reads=$reads --merge_operator=$merge_opr --min_level_to_compress=$mcz

STARTTIME=$(date +%s)
echo "Merging $num keys in database (using application level Read-Modify-Write approach) in random order and then reading them back in the same order...."
../jdb_bench.sh  --benchmarks=updaterandom,readrandom  --disable_auto_compactions=true  --mmap_read=false  --statistics=true  --histogram=true  --threads=$t  --key_size=$ks  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --compression_type=snappy  --cache_numshardbits=4  --open_files=$of  --verify_checksum=true  --db=/home/pshareghi/workspace/rocksdb-bench/java/updaterandom_readrandom  --sync=$sync  --disable_wal=true  --stats_interval=$si  --compression_ratio=0.50  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=false  --cache_remove_scan_count_limit=16 --num=$num --reads=$reads --min_level_to_compress=$mcz
ENDTIME=$(date +%s)
echo "\n#########################"
echo "It takes $(($ENDTIME - $STARTTIME)) seconds to complete Update then Read task..."
du --block-size=1 $update_db
#ll $update_db
echo "#########################\n\n"

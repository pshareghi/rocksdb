echo "Load 1B keys sequentially into database....."
dir="/home/pshareghi/workspace/rocksdb-bench/java/b5"
num=2200000; r=100000000;  bpl=536870912;  mb=67108864;  overlap=10;  mcz=2;  del=300000000;  levels=6;  ctrig=4; delay=8;  stop=12;  wbn=3;  mbc=20;  wbs=134217728;  dds=false;  sync=false;  vs=800;  bs=4096;  cs=17179869184; of=500000;  wps=0;  si=10000000;
./jdb_bench.sh  --benchmarks=fillseq  --disable_auto_compactions=true  --mmap_read=false  --statistics=true  --histogram=true  --num=$num  --threads=1  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --cache_numshardbits=6  --open_files=$of  --verify_checksum=true  --db=$dir  --sync=$sync  --disable_wal=true  --compression_type=snappy  --stats_interval=$si  --compression_ratio=0.5  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --min_level_to_compress=$mcz  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=false


echo "Reading while writing 100M keys in database in random order...."

num=2200000; r=100000000; bpl=536870912; mb=67108864; overlap=10; mcz=2; del=300000000; levels=6; ctrig=4; delay=8; stop=12; wbn=3; mbc=20; wbs=134217728; dds=false; sync=false; t=32; vs=800; bs=4096; cs=17179869184; of=500000; wps=10000; si=10000000;

./jdb_bench.sh  --benchmarks=readwhilewriting  --disable_auto_compactions=true  --mmap_read=false  --statistics=true  --histogram=true  --num=$num  --reads=$r  --writes_per_second=10000  --threads=$t  --value_size=$vs  --block_size=$bs  --cache_size=$cs  --bloom_bits=10  --cache_numshardbits=6  --open_files=$of  --verify_checksum=true  --db=$dir  --sync=$sync  --disable_wal=false  --compression_type=snappy  --stats_interval=$si  --compression_ratio=0.5  --disable_data_sync=$dds  --write_buffer_size=$wbs  --target_file_size_base=$mb  --max_write_buffer_number=$wbn  --max_background_compactions=$mbc  --level0_file_num_compaction_trigger=$ctrig  --level0_slowdown_writes_trigger=$delay  --level0_stop_writes_trigger=$stop  --num_levels=$levels  --delete_obsolete_files_period_micros=$del  --min_level_to_compress=$mcz  --max_grandparent_overlap_factor=$overlap  --stats_per_interval=1  --max_bytes_for_level_base=$bpl  --use_existing_db=true  --writes_per_second=$wps
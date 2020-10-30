
from psutil import virtual_memory
import tqdm
from configurations import partition_size_gb, partition_size, allocation_unit

mem = virtual_memory()

bar_format_MB = "{desc:30}{percentage:3.0f}%|{bar:35}|  {n_fmt} / {total_fmt} MB {postfix}"
bar_format_Blocks = "{desc:30}{percentage:3.0f}%|{bar:35}|  {n_fmt} / {total_fmt} Blocks {postfix}"
bar_format_percent = "{desc:30}{percentage:3.0f}%|{bar:35}|  {n_fmt} / {total_fmt} % {postfix}"
bar_format_block_with_rate = "{desc:30}{percentage:3.0f}%|{bar:35}|  {n_fmt} / {total_fmt} - {rate_fmt}{postfix}"

space_bar_used = tqdm.tqdm(total=(partition_size_gb//1024//1024), leave=True, unit=' MB', colour='#5adc3d', mininterval=5, bar_format=bar_format_MB)
space_bar_used.set_description("Used Space")
space_bar_savings = tqdm.tqdm(total=(partition_size_gb//1024//1024), leave=True, unit=' MB', colour='#5adc3d', mininterval=5, bar_format=bar_format_MB)
space_bar_savings.set_description("Saved Space (Comp+Dedup)")
space_bar_compression_rate = tqdm.tqdm(total=100, leave=True, unit=' %', colour='#5adc3d', mininterval=5, bar_format=bar_format_percent)
space_bar_compression_rate.set_description("Compression Rate")
space_bar_free_space = tqdm.tqdm(total=(partition_size_gb//1024//1024), leave=True, unit=' MB', colour='#5adc3d', mininterval=5, bar_format=bar_format_MB)
space_bar_free_space.set_description("Free Space")

space_bar_over_used = tqdm.tqdm(total=(partition_size_gb//1024//1024), leave=True, unit=' MB', colour='#5adc3d', mininterval=5, bar_format=bar_format_MB)
space_bar_over_used.set_description("Used over Max Cap:")

frag_bar_space = tqdm.tqdm(total=(partition_size_gb//1024//1024), leave=True, unit=' MB', colour='#c70039', mininterval=5, bar_format=bar_format_MB)
frag_bar_space.set_description("Fragmentation")
frag_bar_blocks = tqdm.tqdm(total=partition_size_gb//allocation_unit, leave=True, unit=' Blocks', colour='#c70039', mininterval=5, bar_format=bar_format_Blocks)
frag_bar_blocks.set_description("Fragmented Blocks (aprox.)")

memory_bar_active = tqdm.tqdm(total=(mem.total//1024//1024), leave=True, unit=' MB', colour='magenta', mininterval=5, bar_format=bar_format_MB)
memory_bar_active.set_description("Used memory - Active")
memory_bar_inactive = tqdm.tqdm(total=(mem.total//1024//1024), leave=True, unit=' MB', colour='magenta', mininterval=5, bar_format=bar_format_MB)
memory_bar_inactive.set_description('Used memory - Resident')
memory_bar_statck = tqdm.tqdm(total=(mem.total//1024//1024), leave=True, unit=' MB', colour='magenta', mininterval=5, bar_format=bar_format_MB)
memory_bar_statck.set_description("Used memory - Stack")

write_bar = tqdm.tqdm(total=0, leave=True, unit=' Blocks', colour='blue',miniters=0, bar_format=bar_format_block_with_rate)
write_bar.set_description("Disk writing (flush)")
dedup_bar = tqdm.tqdm(total=0, leave=True, unit=' Blocks', colour='blue', miniters=0, bar_format=bar_format_block_with_rate)
dedup_bar.set_description("Deduplicating data")

# read_bar = tqdm.tqdm(total=1, leave=True, unit=' Blocks', colour='blue',miniters=0, bar_format=bar_format_block_with_rate)
# read_bar.set_description("Reading")

GC_bar = tqdm.tqdm(total=0, leave=True, unit=' Blocks', colour='yellow',miniters=0, bar_format=bar_format_block_with_rate)
GC_bar.set_description("Garagbe Collecting")
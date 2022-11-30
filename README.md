# parallel_map_reduce

This programs runs a various versions of a parallel map-reduce algorithms to count occurances of each word in a corpus. 

```bash
git clone https://github.com/jehanshah8/parallel_map_reduce.git
```

To build and run the serial version of the program

```bash
cd parallel_map_reduce
make -f Makefile.serial
./serial_count_words files/small_test1.txt files/small_test2.txt serial_wc.txt > serial_out.txt
```

#include <string>
#include <unordered_map>

#include <iostream>
#include <fstream>
#include <sstream>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h> 

#include <omp.h>

#include "../include/utils.hpp"

// make -f Makefile.omp
// ./omp_count_words files/small_test1.txt files/small_test2.txt omp_wc.txt > omp_out.txt

int main(int argc, char *argv[])
{
    if (argc < 3) {
        std::cout << "Usage: count_words <input file 1> ... <input file n> <output file>" << std::endl; 
        exit(0);
    }  

    std::cout << "OpenMP Execution" << std::endl;

    // Get sys info such as number of processors 
    int num_max_threads = omp_get_max_threads(); // max number of threads available 
    std::cout << "\nProgram Configuration" << std::endl;
    std::cout << "Number of threads available = " << num_max_threads << std::endl;

    // Echo arguments
    std::cout << "\nInput file(s): " << std::endl; 
    for (int i = 1; i < argc - 1; i++) {
        std::cout << "  - " << argv[i] << std::endl; 
    }

    std::string output_filename = argv[argc - 1]; 
    std::cout << "\nOutput file: " << output_filename << std::endl; 

    // Go over each file 
    for (int i = 1; i < argc - 1; i++) 
    {
        std::cout << "\nStarting to count words for " << argv[i] << std::endl; 
        
        // Open file using mmap to map file to virtual mem 
        // Open file
        int fd = open(argv[i], O_RDONLY);
        if (fd == -1) {   
            std::cerr << "Unable to open file!" << std::endl;
            exit(1);
        }

        // Get file size
        struct stat file_stat;
        int file_status = fstat (fd, &file_stat);

        if (status < 0) {
            close(fd);
            std::cerr << "Unable to get file status!" << std::endl;
            exit(1);
        }
        size_t file_size = file_stat.st_size;

        // Map file to mempory as a string
        std::string file_buffer = mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (file_buffer == MAP_FAILED) {
	        close(fd);
	        std::cerr << "Error mapping file to memory!" << std::endl;
            exit(1);
        }

        std::cout << file_buffer << std::endl;

        // Unmap file and close it
        if (munmap(file_buffer, file_size) == -1) {
            std::cerr << "Error un-mapping file to memory!" << std::endl;
            exit(1);
        }
        close(fd); 

        std::cout << "Finished counting words for " << argv[i] << std::endl; 
    }

    // Create reader threads 
    // Use multiple threads to read one file at a time before moving on to the next file


    // Each reader thread will read one line 
    
    // OLD WAY and put it into the buffer of its corresponding mapper 
    
    // num_readers = k * num_mappers, where k is an integer
    // There needs to be 1 lock for each mapper's queue

    // Each mapper will acquire a lock on the queue, pop the first line
    
    // Mapper will then process the line and update a local hash map

    // Each time 
    
    // Mappers need to know when all readers are done
    // Reducers need to know when all mappers are done

    return 0; 
}

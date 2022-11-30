#include <iostream>
#include <fstream>
#include <sstream>

#include <string>
#include <unordered_map>

#include <omp.h>

#include "../include/utils.hpp"

// make -f Makefile.omp
// ./omp_count_words files/small_test1.txt files/small_test2.txt omp_wc.txt > omp_out.txt

int main(int argc, char *argv[])
{
    if (argc < 3) {
        std::cout<<"Usage: count_words <input file 1> ... <input file n> <output file>"<< std::endl; 
        exit(0);
    }  

    std::cout<<"OpenMP Execution"<<std::endl;

    // Get sys info such as number of processors 
    int num_max_threads = omp_get_max_threads(); // max number of threads available 
    std::cout<<"\nProgram Configuration"<<std::endl;
    std::cout<<"Number of threads available = "<< num_max_threads<<std::endl;

    // Echo arguments
    std::cout<<"\nInput file(s): "<<std::endl; 
    for (int i = 1; i < argc - 1; i++) {
        std::cout<< "  - "<< argv[i]<<std::endl; 
    }

    std::string output_filename = argv[argc - 1]; 
    std::cout<<"\nOutput file: "<<output_filename<< std::endl; 

    // Create reader threads 
    // Use multiple threads to read one file at a time before moving on to the next file


    // Each reader thread will read one line and put it into the buffer of its corresponding mapper 
    
    // num_readers = k * num_mappers, where k is an integer
    // There needs to be 1 lock for each mapper's queue

    // Each mapper will acquire a lock on the queue, pop the first line
    
    // Mapper will then process the line and update a local hash map

    // Each time 
    
    // Mappers need to know when all readers are done
    // Reducers need to know when all mappers are done

    return 0; 
}

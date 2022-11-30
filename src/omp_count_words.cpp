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

    // Start with one thread-safe queue
    // Readers put lines onto the queue, mappers take lines out of the queue

    // Each mapper 

    // Number of reducers are decided by number of cores 
    // word w goes to the queue of reducer i such that i = h(w) % num_reducers 
    
    // Mappers need to know when all readers are done
    // Reducers need to know when all mappers are done

    return 0; 
}

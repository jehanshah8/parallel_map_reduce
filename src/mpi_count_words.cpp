#include <string>
#include <unordered_map>
#include <vector>

#include <iostream>
#include <fstream>
#include <sstream>

#include <omp.h>
#include <mpi.h>

#include "../include/utils.hpp"

#define SHARDS_PER_THREAD 4
#define NUM_CONCURRENT_FILES 1

struct File
{
    int fd;
    size_t file_size;
    char *file_buffer;
};

struct FileShard
{
    char *data;
    size_t size;
};


std::pair<int, int> GetFilesToMap(int num_input_files, int num_files_already_assigned);

int main(int argc, char *argv[])
{
    int pid; 
    int num_procs; 

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid); // get MPI process id
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs); // get number of MPI processes

    if (argc < 2)
    {
        if (!pid) 
        {
            std::cout << "Usage: count_words <input file 1> ... <input file n>" << std::endl;
            MPI_Finalize(); 
            exit(0);
        }
    }

    // Get sys info such as number of processors
    int num_max_threads = omp_get_max_threads(); // max number of threads available

    char **input_files = &argv[1];
    int num_input_files = argc - 1;

    // Split the entire character array (file_buffer) into (k * num_thread) small
    // character arrays. k * for better load balance
    int num_shards_per_file = SHARDS_PER_THREAD * num_max_threads;

    // Echo arguments
    if (!pid) 
    {
        // Program configuration
        std::cout << "MPI Execution" << std::endl;

        std::cout << "\nInput file(s): " << std::endl;
        for (int i = 0; i < num_input_files; i++)
        {
            std::cout << "  - " << input_files[i] << std::endl;
        }
        
        std::cout << "\nProgram Configuration" << std::endl;
        std::cout << "  - Maximm number of threads available = " << num_max_threads << std::endl;
        std::cout << "  - Shards per thread per file = " << SHARDS_PER_THREAD << std::endl;
        std::cout << "  - Target number of shards per file = " << num_shards_per_file << std::endl;
        std::cout << "  - Number of files to process concurrently = " << NUM_CONCURRENT_FILES << std::endl;
        // std::cout << "  - Mapper scheduling = " << "static, 1" << std::endl;
    }

    // Create a map corresponding to each thread (mapper)
    std::vector<std::unordered_map<std::string, int>> local_maps(num_max_threads);

    MPI_Barrier(); // Wait until all processes are set up
    double parallel_runtime = -MPI_Wtime();// start parallel timer 

    int num_files_already_assigned = 0;
    int mapping_round = 0;




    // std::cout << "\n << "[" << pid << "] " << "Starting to map files" << std::endl;
    /*
    while (true)
    {
        // std::cout << "\n" << "[" << pid << "] " << "  - Round " << mapping_round << " of mapping files" << std::endl;

        std::pair<int, int> file_range = GetFilesToMap(num_input_files, num_files_already_assigned);
        int num_files_to_map = file_range.second - file_range.first;
        // std::cout << "  - Number of files to map in this round = " << num_files_to_map << std::endl;

        num_files_already_assigned += num_files_to_map;

        if (num_files_to_map <= 0)
        {
            // std::cout << "[" << pid << "] " << "  - No more files to map" << std::endl;
            break;
        }

        // std::cout << "[" << pid << "] " << "  - Files(s) to map in this round: " << std::endl;
        // for (int i = file_range.first; i < file_range.second; i++)
        // {
        //     std::cout << "[" << pid << "] " << "  - " << input_files[i] << std::endl;
        // }
        mapping_round++;
    }
    */


    // double sharding_time = -MPI_Wtime(); // start mapping timer 


    // use non-blocking sends?
    // blocking recvs?

    






    MPI_Barrier(); // Wait until all processes are done mapping 
    
    // stop mapping timer
    
    // start reduce timer

    MPI_Barrier(); // Wait until all processes are done reducing  
    
    // stop reduce timer
    
    parallel_runtime = (parallel_runtime + MPI_Wtime()) / MPI_Wick(); // stop parallel timer

    if (!pid) 
    {
        // print out timings
        std::cout << "\nParallel execution time (not including file writing): " << parallel_runtime << " seconds" << std::endl;
        std::cout << "Time spent on sharding: " << sharding_time << " seconds ("
                  << (sharding_time / parallel_runtime) * 100 << "%)" << std::endl;
        std::cout << "Time spent on mapping: " << mapping_time << " seconds ("
                  << (mapping_time / parallel_runtime) * 100 << "%)" << std::endl;
        std::cout << "Time spent on reducing: " << reducing_time << " seconds ("
                  << (reducing_time / parallel_runtime) * 100 << "%)" << std::endl;
        std::cout << std::endl;
    }

    MPI_Finalize(); 
    return 0; 
}

// range of files to process [start, stop) start inclusive, stop exclusive
std::pair<int, int> GetFilesToMap(int num_input_files, int num_files_already_assigned)
{
    int num_files_remaining = num_input_files - num_files_already_assigned;
    if (num_files_remaining <= 0)
    {
        // No more files to assign
        return std::make_pair(0, 0);
    }

    int start = num_files_already_assigned;
    int stop = start + std::min(NUM_CONCURRENT_FILES, num_files_remaining);
    return std::make_pair(start, stop);
}
#include <string>
#include <unordered_map>
#include <vector>

#include <iostream>
#include <fstream>
#include <sstream>

#include <omp.h>
#include <mpi.h>

#include "../include/utils.hpp"

// make -f Makefile.omp
// ./omp_count_words files/small_test1.txt files/small_test2.txt > omp_out.txt
// ./omp_count_words files/1.txt files/2.txt files/3.txt > omp_out.txt

#define SHARDS_PER_THREAD 4
#define NUM_CONCURRENT_FILES 8 // good tradeoff between parallel sharding and mapping

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

std::pair<int, int> GetFilesPerNode(int pid, int num_procs, int num_input_files);

std::pair<int, int> GetFilesToMap(int num_input_files, int num_files_already_assigned);
void OpenAndShardFiles(char **filenames, int num_files, std::vector<File> &files,
                       int num_shards_per_file, std::vector<FileShard> &file_shards);
void SplitBufferToShards(char *file_buffer, size_t file_size, int num_shards_per_file,
                         std::vector<FileShard> &file_shards, int file_num);
void GetWordCountsFromShards(std::vector<FileShard> &file_shards,
                             std::vector<std::unordered_map<std::string, int>> &local_maps);
void CloseFiles(std::vector<File> &files);

void InitLocks(std::vector<omp_lock_t> &locks);
void DestroyLocks(std::vector<omp_lock_t> &locks);
unsigned long Hash(const std::string &str);

int main(int argc, char *argv[])
{
    int pid; 
    int num_procs; 

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid); // get MPI process id
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs); // get number of MPI processes

    if (argc < 3)
    {
        if (!pid) 
        {
            std::cout << "Usage: count_words <num_threads> <input file 1> ... <input file n>" << std::endl;
            MPI_Finalize(); 
            exit(0);
        }
    }

    omp_set_num_threads(std::atoi(argv[1]));
    
    // Get sys info such as number of processors
    int num_max_threads = omp_get_max_threads(); // max number of threads available
    
    char **input_files = &argv[2];
    int num_input_files = argc - 2;

    if (num_input_files < num_procs)
    {
        if (!pid) 
        {
            std::cout << "WARNING: Fewer files than processes!" << std::endl;
            // MPI_Finalize(); 
            // exit(0);
        }
    }

    // Split the entire character array (file_buffer) into (k * num_thread) small
    // character arrays. k * for better load balance
    int num_shards_per_file = SHARDS_PER_THREAD * num_max_threads;

    // Echo arguments
    if (!pid) 
    {
        // Program configuration
        std::cout << "MPI Execution" << std::endl;
        std::cout << "Number of input file(s): " << num_input_files << std::endl;
        // std::cout << "\nInput file(s): " << std::endl;
        // for (int i = 0; i < num_input_files; i++)
        // {
        //     std::cout << "  - " << input_files[i] << std::endl;
        // }
        
        std::cout << "\nProgram Configuration" << std::endl;
        std::cout << "  - Number of MPI processes = " << num_procs << std::endl;
        std::cout << "  - Number of threads per MPI process = " << num_max_threads << std::endl;
        std::cout << "  - Shards per thread per file = " << SHARDS_PER_THREAD << std::endl;
        std::cout << "  - Target number of shards per file = " << num_shards_per_file << std::endl;
        std::cout << "  - Number of files to process concurrently = " << NUM_CONCURRENT_FILES << std::endl;
        // std::cout << "  - Mapper scheduling = " << "static, 1" << std::endl;
    }

    // Sanity check
    MPI_Barrier(MPI_COMM_WORLD); // Wait until all processes are set up
    std::cout << "\n" << "[" << pid << "] " << "Hello!" << std::endl;

    // Create a map corresponding to each thread (mapper)
    std::vector<std::unordered_map<std::string, int>> local_maps(num_max_threads);

    double parallel_runtime = 0;// start parallel timer 
    double global_mapping_time = 0;// start timer for all nodes to finish distribution + local mapping;
    double local_work_time = 0;// start timer for distribution + local mapping;
    double work_distribution_time = 0;// start timer ;
    double local_mapping_time = 0;

    MPI_Barrier(MPI_COMM_WORLD); // Wait until all processes are set up
    parallel_runtime -= MPI_Wtime();// start parallel timer 
    global_mapping_time -= MPI_Wtime();// start timer for all nodes to finish distribution + local mapping;
    local_work_time -= MPI_Wtime();// start timer for distribution + local mapping;
    work_distribution_time -= MPI_Wtime();// start timer ;
    
    std::pair<int, int> local_input_files_range = GetFilesPerNode(pid, num_procs, num_input_files);
    int num_local_input_files = local_input_files_range.second - local_input_files_range.first;
    char **local_input_files = &input_files[local_input_files_range.first];

    // std::cout << "\n" << "[" << pid << "] " << "Number of files to map: " << num_local_input_files << std::endl;
    // for (int i = 0; i < num_local_input_files; i++)
    // {
    //     std::cout << "[" << pid << "] " << "  - " << local_input_files[i] << std::endl;
    // }

    // MPI_Barrier(MPI_COMM_WORLD); // Wait until all processes know what files to work on
    work_distribution_time += MPI_Wtime(); // stop timer
    
    local_mapping_time -= MPI_Wtime();// start timer ;

    int num_files_already_assigned = 0;
    int mapping_round = 0;

    // std::cout << "\n" << "[" << pid << "] " << "Starting to map files" << std::endl;
    while (true)
    {
        // std::cout << "\n" << "[" << pid << "] " << "  - Round " << mapping_round << " of mapping files" << std::endl;

        std::pair<int, int> file_range = GetFilesToMap(num_local_input_files, num_files_already_assigned);
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

        // MapFiles(&input_files[file_range.first], num_files_to_map, num_shards_per_file, local_maps);
        std::vector<File> files(num_files_to_map);                                  // List of files (structs) to map
        std::vector<FileShard> file_shards(num_shards_per_file * num_files_to_map); // Vector to store file shards

        // sharding_time -= omp_get_wtime(); // Start timer
        OpenAndShardFiles(&local_input_files[file_range.first], num_files_to_map, files, num_shards_per_file, file_shards);
        // sharding_time += omp_get_wtime(); // Stop timer

        // mapping_time -= omp_get_wtime(); // Start timer
        GetWordCountsFromShards(file_shards, local_maps);
        // mapping_time += omp_get_wtime(); // Stop timer

        CloseFiles(files);
        
        mapping_round++;
    }
    
    local_mapping_time += MPI_Wtime(); // stop timer
    local_work_time += MPI_Wtime(); // stop timer
    
    MPI_Barrier(MPI_COMM_WORLD); // Wait until all processes are done with local mapping 
    global_mapping_time += MPI_Wtime(); // stop timer

    // std::cout << "\n" << "[" << pid << "] " << "Work distribution time (s): " << work_distribution_time << std::endl;
    // std::cout << "\n" << "[" << pid << "] " << "Local mapping time (s): " << local_mapping_time << std::endl;
    std::cout << "\n" << "[" << pid << "] " << "Work distribution + local mapping time (s): " << local_work_time << std::endl;
    MPI_Barrier(MPI_COMM_WORLD); // To gather after prining   
    
    // Start reducing
    double global_reduction_time = -MPI_Wtime();// start timer for all nodes to finish distribution + local mapping;
    double local_reduction_time = -MPI_Wtime();// start timer for distribution + local mapping;    
    

    
    // reduce 
    std::vector<std::unordered_map<std::string, int>> intermediate_maps(num_procs);
    int num_local_maps = local_maps.size();

    // Create a lock corresponding to each reduced map
    std::vector<omp_lock_t> map_locks(num_procs);
    InitLocks(map_locks);

    #pragma omp parallel for
    for (int i = 0; i < num_local_maps; i++)
    {
        // Each reducer iterates over one local map 
        // and puts words into an intermediate map corresponding to each process
        for (auto &it : local_maps[i])
        {
            int reducer_idx = Hash(it.first) % num_procs;

            omp_set_lock(&(map_locks[reducer_idx]));                                     // get lock for map
            UpdateWordCounts(intermediate_maps[reducer_idx], it.first, it.second); // insert into that map
            omp_unset_lock(&(map_locks[reducer_idx]));                                   // release lock
        }
    }

    // Release locks
    DestroyLocks(map_locks);

    // Post receives to receive intermediate maps from each process


    // Send intermediate maps to corresponding processes
    for (int i = 0; i < num_procs; i++)
    {
        ;
    }

    // As maps are received from each process start processing and reduce them into one shared map






    local_reduction_time += MPI_Wtime(); // Time taken for this node to finish reducing
    MPI_Barrier(MPI_COMM_WORLD); // Wait until all processes are done reducing  
    global_reduction_time += MPI_Wtime(); // Time taken for all nodes to finish reducing

    parallel_runtime += MPI_Wtime(); // stop parallel timer

    std::cout << "\n" << "[" << pid << "] " << "Reduction time: " << local_mapping_time << std::endl;

    MPI_Barrier(MPI_COMM_WORLD); // To gather prints
    
    if (!pid) 
    {
        // print out timings
        std::cout << "\nParallel execution time (not including file writing): " << parallel_runtime << " seconds" << std::endl;
        std::cout << "Time taken for all nodes to finish local mapping: " << global_mapping_time << " seconds ("
                  << (global_mapping_time / parallel_runtime) * 100 << "%)" << std::endl;
        std::cout << "Time taken for all nodes to finish reducing: " << global_reduction_time << " seconds ("
                  << (global_reduction_time / parallel_runtime) * 100 << "%)" << std::endl;
        std::cout << std::endl;
    }

    MPI_Finalize(); 
    return 0; 
}

std::pair<int, int> GetFilesPerNode(int pid, int num_procs, int num_input_files)
{
    int start = (pid * num_input_files) / num_procs; // inclusive
    int stop = ((pid + 1) * num_input_files) / num_procs; // exclusive
    return std::make_pair(start, stop);
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

void OpenAndShardFiles(char **filenames, int num_files, std::vector<File> &files,
                       int num_shards_per_file, std::vector<FileShard> &file_shards)
{
    // Open and split multiple files into shards
    #pragma omp parallel for if (num_files > 3) schedule(dynamic)
    for (int i = 0; i < num_files; i++)
    {
        const char *filename = filenames[i];
        // std::cout << "  - Starting to count words for " << filename << std::endl;

        files.at(i).fd = OpenFile(filename);

        // Get file size
        files.at(i).file_size = GetFileSize(files.at(i).fd);
        // std::cout << "File size = " << files.at(i).file_size << std::endl;

        // Open file using mmap to map file to virtual mem
        files.at(i).file_buffer = (char *)MmapFileToRead(files.at(i).fd, files.at(i).file_size);
        // PrintFileBuffer(files.at(i).file_buffer); // for debugging

        // Split file into multiple shards with ending at word boundries
        SplitBufferToShards(files.at(i).file_buffer, files.at(i).file_size, num_shards_per_file, file_shards, i);
    }
}

void SplitBufferToShards(char *file_buffer, size_t file_size, int num_shards_per_file, std::vector<FileShard> &file_shards, int file_num)
{
    // If the file is very small and too many threads are being used,
    // the intger division file_size / num_shards_per_file will result in shards of size 0
    // min used to protect that
    size_t target_shard_size = std::max((int)(file_size / num_shards_per_file), 1);

    // std::cout << "\nSplitting file into shards" << std::endl;
    // std::cout << "Target shard size: " << target_shard_size << std::endl;

    int start_idx = 0;
    int prev_start_idx = 0;
    for (int j = file_num * num_shards_per_file; j < num_shards_per_file * (file_num + 1); j++)
    {
        if (file_buffer != nullptr && start_idx < file_size)
        {
            // std::cout << "\nStarting to build shard " << j << std::endl;

            file_shards.at(j).data = &file_buffer[start_idx];

            prev_start_idx = start_idx;
            start_idx += std::min(target_shard_size, file_size - prev_start_idx); // move start idx to the end of shard + 1th char

            // std::cout << "\nprev_start_idx: " << prev_start_idx << std::endl;
            // std::cout << "start_idx before fixing: " << start_idx << std::endl;
            // std::cout << "Shard " << j << " before fixing " << std::endl;
            // for (int k = 0; k < start_idx - prev_start_idx; k++)
            // {
            //     std::cout << file_shards[j].data[k];
            // }
            // std::cout << "#" << std::endl;

            // Since the file size (in bytes) will be divided by the number of threads
            // Some words will be split across two small character arrays, and this needs to be fixed
            int k;
            for (k = 0; !IsDelimiter(file_buffer[start_idx + k]); k++)
                ;
            // std::cout << "Number of characters to add: " << k << std::endl;

            start_idx += k;
            file_shards.at(j).size = start_idx - prev_start_idx;

            // std::cout << "\nstart_idx after fixing: " << start_idx << std::endl;
            // std::cout << "Shard " << j << " size: " << file_shards[j].size << std::endl;
            // std::cout << "Shard " << j << " after fixing " << std::endl;
            // for (int k = 0; k < start_idx - prev_start_idx; k++)
            // {
            //     std::cout << file_shards[j].data[k];
            // }
            // std::cout << "#" <<  std::endl;
            // std::cout << "\nEnd shard" << std::endl;
        }
        else
        {
            file_shards.at(j).data = nullptr;
            file_shards.at(j).size = -1;
        }
    }
}

void GetWordCountsFromShards(std::vector<FileShard> &file_shards,
                             std::vector<std::unordered_map<std::string, int>> &local_maps)
{
    // std::cout << "Num shards = " << file_shards.size() << std::endl;

    // parallel for loop to take file shards, tokenize, and update local maps
    int num_maps = local_maps.size();
    int num_shards = file_shards.size();
    
    // #pragma omp parallel for schedule(static, 1)
    #pragma omp parallel for schedule(guided)
    for (int i = 0; i < num_shards; i++)
    {
        if (file_shards.at(i).data != nullptr)
        {
            std::string shard(file_shards.at(i).data, file_shards.at(i).size);

            // std::cout << "Shard " << i << ": \n  - size = " << file_shards.at(i).size << std::endl;
            // std::cout << "  - data: \n"
            //           << file_shards.at(i).data << std::endl;

            GetWordCountsFromString(shard, local_maps[omp_get_thread_num()]);
        }
    }
}

void CloseFiles(std::vector<File> &files)
{
    // Close files
    int num_files = files.size();
    #pragma omp parallel for if (num_files > 3) schedule(dynamic)
    for (int i = 0; i < num_files; i++)
    {
        UnmapAndCloseFile(files.at(i).fd, files.at(i).file_buffer, files.at(i).file_size);
    }
}

// djb2 hash function adopted from https://stackoverflow.com/questions/7700400/whats-a-good-hash-function-for-english-words
unsigned long Hash(const std::string &str)
{
    unsigned long hash = 5381;

    std::string::iterator it;
    for (auto &c : str)
    {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }

    return hash;
}

void InitLocks(std::vector<omp_lock_t> &locks)
{
    // init locks
    for (int i = 0; i < locks.size(); i++)
    {
        omp_init_lock(&(locks[i]));
    }
}

void DestroyLocks(std::vector<omp_lock_t> &locks)
{
    // give up locks
    for (int i = 0; i < locks.size(); i++)
    {
        omp_destroy_lock(&(locks[i]));
    }
}
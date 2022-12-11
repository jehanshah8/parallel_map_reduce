#include <string>
#include <unordered_map>
#include <vector>

#include <iostream>
#include <fstream>
#include <sstream>

#include <omp.h>

#include "../include/utils.hpp"

#define SHARDS_PER_THREAD 4
#define NUM_CONCURRENT_FILES 15

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

// void MapFiles(char **filenames, int num_files, int num_shards_per_file,
//               std::vector<std::unordered_map<std::string, int>> &local_maps);
void OpenAndShardFiles(char **filenames, int num_files, std::vector<File> &files,
                       int num_shards_per_file, std::vector<FileShard> &file_shards);
void SplitBufferToShards(char *file_buffer, size_t file_size, int num_shards_per_file,
                         std::vector<FileShard> &file_shards, int file_num);
void GetWordCountsFromShards(std::vector<FileShard> &file_shards,
                             std::vector<std::unordered_map<std::string, int>> &local_maps, 
                             std::vector<omp_lock_t> &map_locks);
void CloseFiles(std::vector<File> &files);

void ReduceMaps(std::vector<std::unordered_map<std::string, int>> &local_maps,
                std::vector<std::unordered_map<std::string, int>> &reduced_word_count_maps);
void InitLocks(std::vector<omp_lock_t> &locks);
void DestroyLocks(std::vector<omp_lock_t> &locks);
unsigned long Hash(const std::string &str);

void JoinMaps(std::vector<std::unordered_map<std::string, int>> &maps, std::unordered_map<std::string, int> &combined_map);

int main(int argc, char *argv[])
{
    double total_runtime = -omp_get_wtime(); // Start timer
    omp_set_num_threads(20);

    if (argc < 2)
    {
        std::cout << "Usage: count_words <input file 1> ... <input file n>" << std::endl;
        exit(0);
    }

    // Get sys info such as number of processors
    int num_max_threads = omp_get_max_threads(); // max number of threads available

    char **input_files = &argv[1];
    int num_input_files = argc - 1;

    // Split the entire character array (file_buffer) into (k * num_thread) small
    // character arrays. k * for better load balance
    int num_shards_per_file = SHARDS_PER_THREAD * num_max_threads;

    // Program configuration
    std::cout << "OpenMP Execution (concurrent map)" << std::endl;

    // Echo arguments
    std::cout << "\nInput file(s): " << std::endl;
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "  - " << input_files[i] << std::endl;
    }

    std::string sorted_output_filename("sorted_combined_omp_wc.txt");
    std::cout << "\nCombined sorted output file: \n  - " << sorted_output_filename << std::endl;

    std::cout << "\nOutput file(s): " << std::endl;
    std::vector<std::string> output_files(num_max_threads);
    for (int i = 0; i < output_files.size(); i++)
    {
        output_files[i] = "output_files/output" + std::to_string(i) + ".txt";
        std::cout << "  - " << output_files[i] << std::endl;
    }

    // omp_sched_t mapper_schedule_type = omp_sched_guided;
    // omp_set_schedule(mapper_schedule_type, -1);

    std::cout << "\nProgram Configuration" << std::endl;
    std::cout << "  - Maximm number of threads available = " << num_max_threads << std::endl;
    std::cout << "  - Shards per thread per file = " << SHARDS_PER_THREAD << std::endl;
    std::cout << "  - Target number of shards per file = " << num_shards_per_file << std::endl;
    std::cout << "  - Number of files to process concurrently = " << NUM_CONCURRENT_FILES << std::endl;
    // std::cout << "  - Mapper scheduling = " << "static, 1" << std::endl;

    // Create a map corresponding to each thread (mapper)
    std::vector<std::unordered_map<std::string, int>> local_maps(num_max_threads);
    std::vector<omp_lock_t> map_locks(num_max_threads);
    InitLocks(map_locks);

    double parallel_runtime = -omp_get_wtime(); // Start timer
    double sharding_time = 0;
    double mapping_time = 0;

    int num_files_already_assigned = 0;
    int round = 0;
    // std::cout << "\nStarting to process files" << std::endl;
    while (true)
    {
        // std::cout << "\n  - Round " << round << " of processing files" << std::endl;

        std::pair<int, int> file_range = GetFilesToMap(num_input_files, num_files_already_assigned);
        int num_files_to_map = file_range.second - file_range.first;
        // std::cout << "  - Number of files to process in this round = " << num_files_to_map << std::endl;

        num_files_already_assigned += num_files_to_map;

        if (num_files_to_map <= 0)
        {
            // std::cout << "  - No more files to process" << std::endl;
            break;
        }

        // std::cout << "  - Files(s) to map in this round: " << std::endl;
        // for (int i = file_range.first; i < file_range.second; i++)
        // {
        //     std::cout << "  - " << input_files[i] << std::endl;
        // }

        // MapFiles(&input_files[file_range.first], num_files_to_map, num_shards_per_file, local_maps);
        std::vector<File> files(num_files_to_map);                                  // List of files (structs) to map
        std::vector<FileShard> file_shards(num_shards_per_file * num_files_to_map); // Vector to store file shards

        sharding_time -= omp_get_wtime(); // Start timer
        OpenAndShardFiles(&input_files[file_range.first], num_files_to_map, files, num_shards_per_file, file_shards);
        sharding_time += omp_get_wtime(); // Stop timer

        mapping_time -= omp_get_wtime(); // Start timer
        GetWordCountsFromShards(file_shards, local_maps, map_locks);
        mapping_time += omp_get_wtime(); // Stop timer

        CloseFiles(files);

        round++;
    }
    // std::cout << "Finished all files" << std::endl;
    
    parallel_runtime += omp_get_wtime(); // Stop timer

    DestroyLocks(map_locks);

    std::cout << "\nParallel execution time (not including file writing): " << parallel_runtime << " seconds" << std::endl;
    std::cout << "Time spent on sharding: " << sharding_time << " seconds ("
              << (sharding_time / parallel_runtime) * 100 << "%)" << std::endl;
    std::cout << "Time spent on mapping and reducing (one step using concurrent maps): " << mapping_time << " seconds ("
              << (mapping_time / parallel_runtime) * 100 << "%)" << std::endl;
    std::cout << std::endl;

    // double writing_time = -omp_get_wtime(); // Start timer
    // Write to multiple files, one per reducer (thread)
    // #pragma omp parallel for
    // for (int i = 0; i < num_max_threads; i++)
    // {
    //     // if (!SortAndWriteWordCountsToFile(local_maps[i], output_files[i]))
    //     if (!WriteWordCountsToFile(local_maps[i], output_files[i]))
    //     {
    //         std::cerr << "Failed write to " << output_files[i] << "!" << std::endl;
    //         exit(1);
    //     }
    // }
    // writing_time += omp_get_wtime(); // Stop timer

    // Write to one file by appending (produces one file with outputs of previous files by appending)
    // Sorted before writing for convinienence in comparing with serial version
    std::unordered_map<std::string, int> combined_map;
    JoinMaps(local_maps, combined_map);
    if (!SortAndWriteWordCountsToFile(combined_map, sorted_output_filename))
    {
        std::cerr << "Failed write to " << sorted_output_filename << "!" << std::endl;
        exit(1);
    }

    total_runtime += omp_get_wtime(); // Stop timer
    std::cout << "Total program runtime " << total_runtime << " seconds" << std::endl;

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

/*
void MapFiles(char **filenames, int num_files, int num_shards_per_file,
              std::vector<std::unordered_map<std::string, int>> &local_maps)
{
    std::vector<File> files(num_files);                                  // List of files (structs) to map
    std::vector<FileShard> file_shards(num_shards_per_file * num_files); // Vector to store file shards

    sharding_time -= omp_get_wtime(); // Start timer
    OpenAndShardFiles(filenames, num_files, files, num_shards_per_file, file_shards);
    sharding_time += omp_get_wtime(); // Stop timer

    mapping_time -= omp_get_wtime(); // Start timer
    GetWordCountsFromShards(file_shards, local_maps);
    mapping_time += omp_get_wtime(); // Stop timer

    CloseFiles(files);
}
*/

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
                             std::vector<std::unordered_map<std::string, int>> &local_maps, 
                             std::vector<omp_lock_t> &map_locks)
{
    // std::cout << "Num shards = " << file_shards.size() << std::endl;

    // parallel for loop to take file shards, tokenize, and map and reduce using concurrent map approach
    int num_maps = local_maps.size();
    int num_shards = file_shards.size();
    #pragma omp parallel for schedule(static, 1)
    for (int i = 0; i < num_shards; i++)
    {
        if (file_shards.at(i).data != nullptr)
        {
            std::string shard(file_shards.at(i).data, file_shards.at(i).size);

            // std::cout << "Shard " << i << ": \n  - size = " << file_shards.at(i).size << std::endl;
            // std::cout << "  - data: \n"
            //           << file_shards.at(i).data << std::endl;

            int word_start_idx = 0;
            int word_length = 0;
            for (int j = 0; j <= shard.size(); j++) // read including null terminator
            {
                if (IsDelimiter(shard[j]))
                {
                    if (word_length)
                    {
                        std::string word = shard.substr(word_start_idx, word_length);
                        int map_idx = Hash(word) % num_maps;
                        omp_set_lock(&(map_locks[map_idx]));  
                        UpdateWordCounts(local_maps[map_idx], word, 1); 
                        omp_unset_lock(&(map_locks[map_idx])); 

                        // UpdateWordCounts(local_maps[omp_get_thread_num()], word, 1);
                        word_length = 0;
                    }

                    word_start_idx = j + 1;
                }
                else
                {
                    word_length++;
                }
            }
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

void ReduceMaps(std::vector<std::unordered_map<std::string, int>> &local_maps,
                std::vector<std::unordered_map<std::string, int>> &reduced_word_count_maps)
{
    int num_maps = local_maps.size();

    // Create a lock corresponding to each reduced map
    std::vector<omp_lock_t> map_locks(num_maps);
    InitLocks(map_locks);

    #pragma omp parallel for
    for (int i = 0; i < num_maps; i++)
    {
        // Each reducer iterates over one map
        for (auto &it : local_maps[i])
        {
            int reducer_idx = Hash(it.first) % num_maps;

            omp_set_lock(&(map_locks[reducer_idx]));                                     // get lock for map
            UpdateWordCounts(reduced_word_count_maps[reducer_idx], it.first, it.second); // insert into that map
            omp_unset_lock(&(map_locks[reducer_idx]));                                   // release lock
        }
    }

    // Release locks
    DestroyLocks(map_locks);
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

void JoinMaps(std::vector<std::unordered_map<std::string, int>> &maps, std::unordered_map<std::string, int> &combined_map)
{
    for (int i = 0; i < maps.size(); i++)
    {
        combined_map.insert(maps.at(i).begin(), maps.at(i).end());
    }
}
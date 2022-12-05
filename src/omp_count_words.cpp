#include <string>
#include <unordered_map>
#include <vector>

#include <iostream>
#include <fstream>
#include <sstream>

#include <omp.h>

#include "../include/utils.hpp"

// make -f Makefile.omp
// ./omp_count_words files/small_test1.txt files/small_test2.txt > omp_out.txt
// ./omp_count_words files/1.txt files/2.txt files/3.txt > omp_out.txt
// ./omp_count_words files/1.txt files/2.txt files/3.txt files/4.txt files/5.txt files/6.txt files/7.txt files/8.txt files/9.txt files/11.txt files/12.txt files/13.txt files/14.txt files/15.txt files/16.txt > omp_out.txt

#define CHUNKS_PER_THREAD 5
#define NUM_CONCURRENT_FILES 3

struct File
{
    int fd;
    size_t file_size;
    char *file_buffer;
};

struct FileChunk
{
    char *data;
    size_t size;
};

unsigned long Hash(std::string &str);

bool IsDelimiter(char c);

void InitLocks(std::vector<omp_lock_t> &locks);
void DestroyLocks(std::vector<omp_lock_t> &locks);

void SplitBufferToChunks(char *file_buffer, size_t file_size, int target_num_chunks, std::vector<FileChunk> &file_chunks, int file_num);
void GetWordCountsFromChunks(std::vector<FileChunk> &file_chunks,
                             std::vector<std::unordered_map<std::string, int>> &word_count_maps,
                             std::vector<omp_lock_t> &map_locks);
void JoinMaps(std::vector<std::unordered_map<std::string, int>> &maps, std::unordered_map<std::string, int> &combined_map);

double split_to_chunk_runtime = 0; // cumulative time taken to split files to chunks
double map_runtime = 0;            // cumulative time taken for counting words and reducing

int main(int argc, char *argv[])
{
    omp_set_num_threads(16);

    if (argc < 2)
    {
        std::cout << "Usage: count_words <input file 1> ... <input file n>" << std::endl;
        exit(0);
    }

    char **input_files = &argv[1];
    int num_input_files = argc - 1;

    // Program configuration
    std::cout << "OpenMP Execution" << std::endl;

    // Echo arguments
    std::cout << "\nInput file(s): " << std::endl;
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "  - " << input_files[i] << std::endl;
    }

    std::string output_filename("sorted_combined_omp_wc.txt");
    std::cout << "\nCombined Output file: \n  - " << output_filename << std::endl;

    // std::cout << "\nOutput file(s): " << std::endl;
    // std::vector<std::string> output_files(num_max_threads);
    // for (int i = 0; i < output_files.size(); i++)
    // {
    //     output_files[i] = "output_files/output" + std::to_string(i) + ".txt";
    //     std::cout << "  - " << output_files[i] << std::endl;
    // }

    // Get sys info such as number of processors
    int num_max_threads = omp_get_max_threads(); // max number of threads available
    std::cout << "\nProgram Configuration" << std::endl;
    std::cout << "  - Maximm number of threads available = " << num_max_threads << std::endl;

    // Create a map corresponding to each thread, create a lock for each thread
    std::vector<std::unordered_map<std::string, int>> word_count_maps(num_max_threads);
    std::vector<omp_lock_t> map_locks(num_max_threads);
    InitLocks(map_locks);

    // Split the entire character array (file_buffer) into (k * num_thread) small
    // character arrays. k * for better load balance
    std::cout << "  - Chunks per thread = " << CHUNKS_PER_THREAD << std::endl;
    int target_num_chunks = CHUNKS_PER_THREAD * num_max_threads;
    std::cout << "  - Target number of chunks per file = " << target_num_chunks << std::endl;

    int num_concurrent_files = std::min(num_input_files, NUM_CONCURRENT_FILES);
    std::cout << "  - Number of files to process concurrently = " << num_concurrent_files << std::endl;
    std::vector<File> files(num_concurrent_files);

    std::vector<FileChunk> file_chunks(target_num_chunks * num_concurrent_files); // Vector to store file chunks

    double runtime = -omp_get_wtime(); // Start timer

    // Open multiple files at a time, then process that group of files with multiple threads
    int group_num = 0;
    for (int i = 0; i < num_input_files; i += num_concurrent_files)
    {
        std::cout << "\nStarting to count words for group " << group_num << std::endl;
        double group_runtime = -omp_get_wtime(); // Start timer
        double group_chunkify_runtime = -omp_get_wtime(); // Start timer
        // parallelizing here does not give much adv because too little work
        // #pragma omp parallel for num_threads(std::min(num_max_threads, num_concurrent_files))
        for (int j = 0; j < num_concurrent_files; j++)
        {
            if ((i + j) < num_input_files)
            {
                const char *filename = input_files[i + j];
                std::cout << "  - Starting to count words for " << filename << std::endl;

                // int fd = OpenFile(filename);
                files.at(j).fd = OpenFile(filename);

                // Get file size
                // size_t file_size = GetFileSize(fd);
                files.at(j).file_size = GetFileSize(files.at(j).fd);

                // Open file using mmap to map file to virtual mem
                // char *file_buffer = (char *)MmapFileToRead(fd, file_size);
                files.at(j).file_buffer = (char *)MmapFileToRead(files.at(j).fd, files.at(j).file_size);
                // std::cout << "File size = " << file_size << std::endl;

                // PrintFileBuffer(file_buffer); // for debugging
            }
            else
            {
                files.at(j).fd = -1;
                files.at(j).file_size = -1;
                files.at(j).file_buffer = nullptr;
            }

            // Split file into multiple chunks with ending at word boundries
            SplitBufferToChunks(files.at(j).file_buffer, files.at(j).file_size, target_num_chunks, file_chunks, j);
        }
        group_chunkify_runtime += omp_get_wtime(); // Stop timer
        split_to_chunk_runtime += group_chunkify_runtime;
        std::cout << "\n    Chunkifying group took " << group_chunkify_runtime << "seconds" << std::endl;

        GetWordCountsFromChunks(file_chunks, word_count_maps, map_locks);
        std::cout << std::endl;

        for (int j = 0; j < num_concurrent_files && (i + j) < num_input_files; j++)
        {
            UnmapAndCloseFile(files.at(j).fd, files.at(j).file_buffer, files.at(j).file_size);
            const char *filename = input_files[i + j];
            std::cout << "  - Finished counting words for " << filename << std::endl;
        }
        group_runtime += omp_get_wtime(); // Stop timer
        std::cout << "Group took " << group_runtime << "seconds" << std::endl;
        group_num++;
    }

    // Release locks
    DestroyLocks(map_locks);

    runtime += omp_get_wtime(); // Stop timer
    std::cout << "\nParallel execution time " << runtime << " seconds" << std::endl;
    std::cout << "Total time spent on chunkifying " << split_to_chunk_runtime << " seconds ("
              << (split_to_chunk_runtime / runtime) * 100 << "%)" << std::endl;
    std::cout << "Total time spent on mapping " << map_runtime << " seconds ("
              << (map_runtime / runtime) * 100 << "%)" << std::endl;
    std::cout << std::endl;

    // Write to multiple files, one per reducer (thread)
    // #pragma omp parallel for
    // for (int i = 0; i < num_max_threads; i++)
    // {
    //     if (!SortAndWriteWordCountsToFile(word_count_maps[i], output_files[i]))
    //     {
    //         std::cerr << "Failed write to " << output_files[i] << "!" << std::endl;
    //         exit(1);
    //     }
    // }

    // Write to one file by appending (produces one file with outputs of previous files by appending)
    // Sorted before writing for convinienence in comparing with serial version 
    std::unordered_map<std::string, int> combined_map;
    JoinMaps(word_count_maps, combined_map);
    if (!SortAndWriteWordCountsToFile(combined_map, output_filename)) 
    {
        std::cerr << "Failed write to " << output_filename << "!" << std::endl;
        exit(1);
    }

    return 0;
}

void SplitBufferToChunks(char *file_buffer, size_t file_size, int target_num_chunks, std::vector<FileChunk> &file_chunks, int file_num)
{
    double runtime = -omp_get_wtime(); // Start timer

    // If the file is very small and too many threads are being used,
    // the intger division file_size / target_num_chunks will result in chunks of size 0
    // min used to protect that
    size_t target_chunk_size = std::max((int)(file_size / target_num_chunks), 1);

    // std::cout << "\nSplitting file into chunks" << std::endl;
    // std::cout << "Target chunk size: " << target_chunk_size << std::endl;

    int start_idx = 0;
    int prev_start_idx = 0;
    for (int j = file_num * target_num_chunks; j < target_num_chunks * (file_num + 1); j++)
    {
        if (file_buffer != nullptr && start_idx < file_size)
        {
            // std::cout << "\nStarting to build chunk " << j << std::endl;

            file_chunks.at(j).data = &file_buffer[start_idx];

            prev_start_idx = start_idx;
            start_idx += std::min(target_chunk_size, file_size - prev_start_idx); // move start idx to the end of chunk + 1th char

            // std::cout << "\nprev_start_idx: " << prev_start_idx << std::endl;
            // std::cout << "start_idx before fixing: " << start_idx << std::endl;
            // std::cout << "Chunk " << j << " before fixing " << std::endl;
            // for (int k = 0; k < start_idx - prev_start_idx; k++)
            // {
            //     std::cout << file_chunks[j].data[k];
            // }
            // std::cout << "#" << std::endl;

            // Since the file size (in bytes) will be divided by the number of threads
            // Some words will be split across two small character arrays, and this needs to be fixed
            int k;
            for (k = 0; !IsDelimiter(file_buffer[start_idx + k]); k++)
                ;
            // std::cout << "Number of characters to add: " << k << std::endl;

            start_idx += k;
            file_chunks.at(j).size = start_idx - prev_start_idx;

            // std::cout << "\nstart_idx after fixing: " << start_idx << std::endl;
            // std::cout << "Chunk " << j << " size: " << file_chunks[j].size << std::endl;
            // std::cout << "Chunk " << j << " after fixing " << std::endl;
            // for (int k = 0; k < start_idx - prev_start_idx; k++)
            // {
            //     std::cout << file_chunks[j].data[k];
            // }
            // std::cout << "#" <<  std::endl;
            // std::cout << "\nEnd chunk" << std::endl;
        }
        else
        {
            file_chunks.at(j).data = nullptr;
            file_chunks.at(j).size = -1;
        }
    }
    runtime += omp_get_wtime(); // Stop timer
    // split_to_chunk_runtime += runtime;
    // std::cout << "Splitting buffer to chunks took " << runtime << "seconds" << std::endl;
}

void GetWordCountsFromChunks(std::vector<FileChunk> &file_chunks,
                             std::vector<std::unordered_map<std::string, int>> &word_count_maps,
                             std::vector<omp_lock_t> &map_locks)
{
    double runtime = -omp_get_wtime(); // Start timer
    // std::cout << "Num chunks = " << file_chunks.size() << std::endl;

    // parallel for loop to take file chunks, tokenize, and update local maps
    int num_maps = word_count_maps.size();
    // std::cout << "OMP schedule: " << "static" << std::endl;
    #pragma omp parallel for schedule(guided)
    for (int i = 0; i < file_chunks.size(); i++)
    {
        if (file_chunks.at(i).data != nullptr)
        {
            std::string chunk(file_chunks.at(i).data, file_chunks.at(i).size);

            // std::cout << "Chunk " << i << ": \n  - size = " << file_chunks.at(i).size << std::endl;
            // std::cout << "  - data: \n"
            //           << file_chunks.at(i).data << std::endl;

            std::istringstream word_buffer(chunk);
            std::string word;
            while (word_buffer >> word)
            {
                int map_idx = Hash(word) % num_maps;
                // UpdateWordCounts(word_count_maps[omp_get_thread_num()], word);
                omp_set_lock(&(map_locks[map_idx]));              // get lock for map
                UpdateWordCounts(word_count_maps[map_idx], word); // insert into that map
                omp_unset_lock(&(map_locks[map_idx]));            // release lock
            }
        }
    }
    runtime += omp_get_wtime(); // Stop timer
    map_runtime += runtime;
    // std::cout << "Getting word counts took " << runtime << "seconds" << std::endl;
}

bool IsDelimiter(char c)
{
    return c == ' ' || c == '\n' || c == '\0';
}

// djb2 hash function adopted from https://stackoverflow.com/questions/7700400/whats-a-good-hash-function-for-english-words
unsigned long Hash(std::string &str)
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

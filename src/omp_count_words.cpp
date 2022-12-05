#include <string>
#include <unordered_map>
#include <vector>

#include <iostream>
#include <fstream>
#include <sstream>

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#include <omp.h>

#include "../include/utils.hpp"

// make -f Makefile.omp
// ./omp_count_words files/small_test1.txt files/small_test2.txt > omp_out.txt
// ./omp_count_words files/1.txt files/2.txt files/3.txt > omp_out.txt
// ./omp_count_words files/1.txt files/2.txt files/3.txt files/4.txt files/5.txt files/6.txt files/7.txt files/8.txt files/9.txt files/11.txt files/12.txt files/13.txt files/14.txt files/15.txt files/16.txt > omp_out.txt

#define CHUNKS_PER_THREAD 10

struct FileChunk
{
    char *data;
    size_t size;
};

bool IsDelimiter(char c);
int OpenFile(const char *filename);
void UnmapAndCloseFile(int fd, char *file_buffer, size_t file_size);
size_t GetFileSize(int fd);
void *MmapFileToRead(int fd, size_t file_size);
void SplitBufferToChunks(char *file_buffer, size_t file_size, int target_num_chunks, std::vector<FileChunk> &file_chunks);
void InitLocks(std::vector<omp_lock_t> &locks);
void DestroyLocks(std::vector<omp_lock_t> &locks);
unsigned long Hash(std::string &str);
void GetWordCountsFromChunks(std::vector<FileChunk> &file_chunks,
                             std::vector<std::unordered_map<std::string, int>> &word_count_maps,
                             std::vector<omp_lock_t> &map_locks);

int GetReducerIdForWord(std::string &word);

void PrintFileBuffer(const char *file_buffer);

int main(int argc, char *argv[])
{
    omp_set_num_threads(8);

    if (argc < 2)
    {
        std::cout << "Usage: count_words <input file 1> ... <input file n>" << std::endl;
        exit(0);
    }

    char **input_files = &argv[1];
    int num_input_files = argc - 1;

    // Program configuration
    std::cout << "OpenMP Execution" << std::endl;

    // Get sys info such as number of processors
    int num_max_threads = omp_get_max_threads(); // max number of threads available
    std::cout << "\nProgram Configuration" << std::endl;
    std::cout << "\tMaximm number of threads available = " << num_max_threads << std::endl;

    // Echo arguments
    std::cout << "\nInput file(s): " << std::endl;
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "  - " << input_files[i] << std::endl;
    }

    std::cout << "\nOutput file(s): " << std::endl;
    std::vector<std::string> output_files(num_max_threads);
    for (int i = 0; i < output_files.size(); i++)
    {
        output_files[i] = "output_files/output" + std::to_string(i) + ".txt";
        std::cout << "  - " << output_files[i] << std::endl;
    }

    std::vector<std::unordered_map<std::string, int>> word_count_maps(num_max_threads);
    std::vector<omp_lock_t> map_locks(num_max_threads);
    InitLocks(map_locks);

    // Split the entire character array (file_buffer) into (k * num_thread) small
    // character arrays. k * for better load balance
    int target_num_chunks = CHUNKS_PER_THREAD * num_max_threads;
    std::vector<FileChunk> file_chunks(target_num_chunks); // Vector to store file chunks
    std::cout << "Target number of chunks per file: " << target_num_chunks << std::endl;

    double runtime = -omp_get_wtime(); // Start timer

    // Go over each file with multiple threads
    for (int i = 0; i < num_input_files; i++)
    {
        const char *filename = input_files[i];
        std::cout << "\nStarting to count words for " << filename << std::endl;

        int fd = OpenFile(filename);

        // Get file size
        size_t file_size = GetFileSize(fd);

        // Open file using mmap to map file to virtual mem
        char *file_buffer = (char *)MmapFileToRead(fd, file_size);
        // std::cout << "File size = " << file_size << std::endl;

        // PrintFileBuffer(file_buffer); // for debugging

        // Split file into multiple chunks with ending at word boundries
        SplitBufferToChunks(file_buffer, file_size, target_num_chunks, file_chunks);

        GetWordCountsFromChunks(file_chunks, word_count_maps, map_locks);
        // std::cout << "here" << std::endl;

        UnmapAndCloseFile(fd, file_buffer, file_size);

        std::cout << "Finished counting words for " << filename << std::endl;
    }

    // Write the word counts to file

    // Release locks
    DestroyLocks(map_locks);

    runtime += omp_get_wtime(); // Stop timer
    std::cout << "\nParallel execution time " << runtime << "seconds" << std::endl;

// Write to multiple files, one per reducer (thread)
#pragma omp parallel for
    for (int i = 0; i < num_max_threads; i++)
    {
        if (!WriteWordCountsToFile(word_count_maps[i], output_files[i]))
        {
            std::cerr << "Failed write to " << output_files[i] << "!" << std::endl;
            exit(1);
        }
    }

    // Write to one file by appending (produces one file with outputs of previous files by appending)
    // Convinient to compare with serial version
    std::string output_filename("combined_omp_wc.txt");
    std::ofstream out_file{output_filename};
    // Check if file was opened successfully
    if (!out_file)
    {
        std::cerr << "Unable to open file for writing!" << std::endl;
        exit(1);
    }
    for (int i = 0; i < word_count_maps.size(); i++)
    {
        std::unordered_map<std::string, int>::const_iterator it;
        for (it = word_count_maps[i].begin(); it != word_count_maps[i].end(); it++)
        {
            out_file << it->first
                     << ':'
                     << it->second
                     << std::endl;
        }
    }
    out_file.close();

    return 0;
}

bool IsDelimiter(char c)
{
    return c == ' ' || c == '\n' || c == '\0';
}

int OpenFile(const char *filename)
{
    int fd = open(filename, O_RDONLY);
    if (fd == -1)
    {
        std::cerr << "Unable to open file!" << std::endl;
        exit(1);
    }
    return fd;
}

void UnmapAndCloseFile(int fd, char *file_buffer, size_t file_size)
{
    // Unmap file and close it
    if (munmap(file_buffer, file_size) == -1)
    {
        std::cerr << "Error un-mapping file to memory!" << std::endl;
        exit(1);
    }
    close(fd);
}

size_t GetFileSize(int fd)
{
    struct stat file_stat;
    int file_status = fstat(fd, &file_stat);
    if (file_status < 0)
    {
        close(fd);
        std::cerr << "Unable to get file status!" << std::endl;
        exit(1);
    }
    return file_stat.st_size;
}

void *MmapFileToRead(int fd, size_t file_size)
{
    void *file_buffer = mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);
    if (file_buffer == MAP_FAILED)
    {
        close(fd);
        std::cerr << "Error mapping file to memory!" << std::endl;
        exit(1);
    }

    return file_buffer;
}

void PrintFileBuffer(const char *file_buffer)
{
    // Print entire buffer for debugging
    std::cout << "\nFile buffer: " << std::endl;
    std::cout << '\t' << file_buffer << std::endl;
    std::cout << "\nEnd file buffer" << std::endl;
}

void SplitBufferToChunks(char *file_buffer, size_t file_size, int target_num_chunks, std::vector<FileChunk> &file_chunks)
{
    // If the file is very small and too many threads are being used,
    // the intger division file_size / target_num_chunks will result in chunks of size 0
    // min used to protect that
    size_t target_chunk_size = std::max((int)(file_size / target_num_chunks), 1);

    // std::cout << "\nSplitting file into chunks" << std::endl;
    // std::cout << "Target chunk size: " << target_chunk_size << std::endl;

    int start_idx = 0;
    int prev_start_idx = 0;
    for (int j = 0; j < target_num_chunks; j++)
    {
        if (start_idx < file_size)
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

void GetWordCountsFromChunks(std::vector<FileChunk> &file_chunks,
                             std::vector<std::unordered_map<std::string, int>> &word_count_maps,
                             std::vector<omp_lock_t> &map_locks)
{

    // std::cout << "Num chunks = " << file_chunks.size() << std::endl;

    // parallel for loop to take file chunks, tokenize, and update local maps
    int num_maps = word_count_maps.size();
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
                omp_set_lock(&(map_locks[map_idx]));              // get lock for map
                UpdateWordCounts(word_count_maps[map_idx], word); // insert into that map
                omp_unset_lock(&(map_locks[map_idx]));            // release lock
            }
        }
    }
}

/// @brief
/// @param word
/// @return int reducer_id -- the number/id of the reducer corresponding to this word
int GetReducerIdForWord(std::string &word)
{
    // get bucket number
    // mod bucket number by num_reducers

    // proc_id = hash % num_procs;
    // reducer_id_in_proc = hash % num_reducers; s
    return 0;
}

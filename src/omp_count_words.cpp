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
// #include "../include/thread_safe_map.hpp"

// make -f Makefile.omp
// ./omp_count_words files/small_test1.txt files/small_test2.txt omp_wc.txt > omp_out.txt

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
void GetWordCountsFromChunks(std::vector<FileChunk> &file_chunks,
                             std::vector<std::unordered_map<std::string, int>> &thread_local_word_counts);
int GetReducerIdForWord(std::string &word);

void PrintFileBuffer(const char *file_buffer);

int main(int argc, char *argv[])
{
    omp_set_num_threads(1);

    if (argc < 3)
    {
        std::cout << "Usage: count_words <input file 1> ... <input file n> <output file>" << std::endl;
        exit(0);
    }

    char **input_files = &argv[1];
    int num_input_files = argc - 2;
    std::string output_filename = argv[argc - 1];

    std::cout << "OpenMP Execution" << std::endl;

    // Get sys info such as number of processors
    int num_max_threads = omp_get_max_threads(); // max number of threads available
    std::cout << "\nProgram Configuration" << std::endl;
    std::cout << "\tMaximm number of threads available = " << num_max_threads << std::endl;

    // int num_readers = std::min(num_max_threads / 2, num_input_files);
    // std::cout << "Number of reader threads = " << num_readers << std::endl;

    // Echo arguments
    std::cout << "\nInput file(s): " << std::endl;
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "  - " << input_files[i] << std::endl;
    }
    std::cout << "\nOutput file: \n  - " << output_filename << std::endl;

    std::unordered_map<std::string, int> word_counts;
    std::vector<std::unordered_map<std::string, int>> thread_local_word_counts(num_max_threads);

    // Split the entire character array (file_buffer) into (k * num_thread) small
    // character arrays. k * for better load balance
    int target_num_chunks = CHUNKS_PER_THREAD * num_max_threads;
    std::vector<FileChunk> file_chunks(target_num_chunks); // Vector to store file chunks

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
        std::cout << "File size = " << file_size << std::endl;

        // PrintFileBuffer(file_buffer) // for debugging

        // Split file into multiple chunks with ending at word boundries
        SplitBufferToChunks(file_buffer, file_size, target_num_chunks, file_chunks);

        // Do the mapping
        // TODO: mapping
        GetWordCountsFromChunks(file_chunks, thread_local_word_counts);

        UnmapAndCloseFile(fd, file_buffer, file_size);

        std::cout << "Finished counting words for " << filename << std::endl;
    }

    // TODO: Reduce local maps to shared map

    // Write the word counts to file
    if (!WriteWordCountsToFile(thread_local_word_counts[0], output_filename))
    //if (!WriteWordCountsToFile(word_counts, output_filename))
    {
        std::cerr << "Failed write to " << output_filename << "!" << std::endl;
        exit(1);
    }

    runtime += omp_get_wtime(); // Stop timer
    std::cout << "\nParallel execution time " << runtime << "seconds" << std::endl;

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
    size_t target_chunk_size = file_size / target_num_chunks;

    std::cout << "\nSplitting file into chunks" << std::endl;
    int start_idx = 0;
    int prev_start_idx = 0;
    for (int j = 0; j < target_num_chunks && start_idx < file_size; j++)
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
}

void GetWordCountsFromChunks(std::vector<FileChunk> &file_chunks,
                             std::vector<std::unordered_map<std::string, int>> &thread_local_word_counts)
{
    // parallel for loop to take file chunks, tokenize, and update local maps
    #pragma omp parallel for schedule(guided)
    for (int i = 0; i < file_chunks.size(); i++)
    {
        std::string chunk(file_chunks.at(i).data, file_chunks.at(i).size);
        GetWordCountsFromString(chunk, thread_local_word_counts.at(omp_get_thread_num()));
    }
}

/// @brief 
/// @param word 
/// @return int reducer_id -- the number/id of the reducer corresponding to this word 
int GetReducerIdForWord(std::string &word) {
    // get bucket number
    // mod bucket number by num_reducers 
    ;
}


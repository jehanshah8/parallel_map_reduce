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
// ./omp_count_words files/small_test1.txt files/small_test2.txt omp_wc.txt > omp_out.txt

struct file_chunk
{
    char *data;
    size_t size;
};

bool IsDelimiter(char c);
size_t GetFileSize(const int fd);
void *MmapFileToRead(const int fd, const int file_size);

int main(int argc, char *argv[])
{

    // omp_set_num_threads(3);
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
    std::cout << "\tNumber of threads available = " << num_max_threads << std::endl;

    // int num_readers = std::min(num_max_threads / 2, num_input_files);
    // std::cout << "Number of reader threads = " << num_readers << std::endl;

    // Echo arguments
    std::cout << "\nInput file(s): " << std::endl;
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "  - " << input_files[i] << std::endl;
    }
    std::cout << "\nOutput file: \n  - " << output_filename << std::endl;

    // std::unordered_map<std::string, int> word_counts;

    double runtime = -omp_get_wtime(); // Start timer

    // Go over each file with multiple threads
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "\nStarting to count words for " << input_files[i] << std::endl;

        // Open file
        int fd = open(input_files[i], O_RDONLY);
        if (fd == -1)
        {
            std::cerr << "Unable to open file!" << std::endl;
            exit(1);
        }

        // Get file size
        size_t file_size = GetFileSize(fd);

        // TODO: Decide how many threads to give to each file
        int threads_per_file = num_max_threads; // Give all threads to file for now

        // Open file using mmap to map file to virtual mem
        char *file_buffer = (char *)MmapFileToRead(fd, file_size);
        std::cout << "File size = " << file_size << std::endl;

        // Print entire buffer for debugging
        std::cout << "\nFile buffer: " << std::endl;
        std::cout << '\t' << file_buffer << std::endl;
        std::cout << "\nEnd file buffer" << std::endl;

        // Split the entire character array (file_buffer) into (k * num_thread) small
        // character arrays. k * for better load balance
        int chunks_per_thread = 10; // TODO: def const?
        int target_num_chunks = chunks_per_thread * threads_per_file;
        size_t target_chunk_size = file_size / target_num_chunks;

        std::vector<file_chunk> file_chunks(target_num_chunks); // Vector to store file chunks

        std::cout << "\nSplitting file into chunks" << std::endl;
        int start_idx = 0;
        int prev_start_idx = 0;
        for (int j = 0; j < target_num_chunks && start_idx < file_size; j++)
        {
            std::cout << "\nStarting to build chunk " << j << std::endl;

            try
            {
                file_chunks.at(j).data = &file_buffer[start_idx];
            }
            catch (const std::out_of_range &oor)
            {
                std::cerr << "Out of Range error: " << oor.what() << '\n';
            }

            prev_start_idx = start_idx;
            start_idx += std::min(target_chunk_size, file_size - prev_start_idx); // move start idx to the end of chunk + 1th char

            std::cout << "\nprev_start_idx: " << prev_start_idx << std::endl;
            std::cout << "start_idx before fixing: " << start_idx << std::endl;
            std::cout << "Chunk " << j << " before fixing " << std::endl;
            for (int k = 0; k < start_idx - prev_start_idx; k++)
            {
                std::cout << file_chunks[j].data[k];
            }
            std::cout << '\0' << std::endl;

            // Since the file size (in bytes) will be divided by the number of threads
            // Some words will be split across two small character arrays, and this needs to be fixed
            int k;
            for (k = 0; !IsDelimiter(file_buffer[start_idx + k]); k++)
                ;
            std::cout << "Number of characters to add: " << k << std::endl;

            start_idx += k;

            try
            {
                file_chunks.at(j).size = start_idx - prev_start_idx;
            }
            catch (const std::out_of_range &oor)
            {
                std::cerr << "Out of Range error: " << oor.what() << '\n';
            }

            std::cout << "\nstart_idx after fixing: " << start_idx << std::endl;
            std::cout << "Chunk " << j << " size: " << file_chunks[j].size << std::endl;
            std::cout << "Chunk " << j << " after fixing " << std::endl;
            for (int k = 0; k < start_idx - prev_start_idx; k++)
            {
                std::cout << file_chunks[j].data[k];
            }
            std::cout << '\0' << std::endl;

            std::cout << "\nEnd chunk" << std::endl;
        }

// TODO: parallel for loop to take file chunks, tokenize, and update map
#pragma omp parallel for schedule(guided)
        for (int j = 0; j < file_chunks.size(); j++)
        {
            ;
        }

        // Unmap file and close it
        if (munmap(file_buffer, file_size) == -1)
        {
            std::cerr << "Error un-mapping file to memory!" << std::endl;
            exit(1);
        }
        close(fd);

        std::cout << "Finished counting words for " << input_files[i] << std::endl;
    }

    // Write the word counts to file
    // WriteWordCountsToFile(word_counts, output_filename);

    runtime += omp_get_wtime(); // Stop timer
    std::cout << "\nParallel execution time " << runtime << "seconds" << std::endl;

    return 0;
}

bool IsDelimiter(char c)
{
    return c == ' ' || c == '\n' || c == '\0';
}

size_t GetFileSize(const int fd)
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

void *MmapFileToRead(const int fd, const int file_size)
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
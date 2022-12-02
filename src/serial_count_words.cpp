#include <iostream>
#include <fstream>

#include <string>
#include <unordered_map>

#include <omp.h>

#include "../include/utils.hpp"

// make -f Makefile.serial
// ./serial_count_words files/small_test1.txt files/small_test2.txt serial_wc.txt > serial_out.txt

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        std::cout << "Usage: count_words <input file 1> ... <input file n> <output file>" << std::endl;
        exit(0);
    }

    char **input_files = &argv[1];
    int num_input_files = argc - 2;
    std::string output_filename = argv[argc - 1];

    // Program configuration
    std::cout << "Serial Execution" << std::endl;

    // Echo arguments
    std::cout << "\nInput file(s): " << std::endl;
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "  - " << input_files[i] << std::endl;
    }
    std::cout << "\nOutput file: " << output_filename << std::endl;

    // Read files one by one and build a hash table to hold reduced data
    std::unordered_map<std::string, int> word_counts;

    double runtime = -omp_get_wtime(); // Start timer

    // Go over each file
    for (int i = 0; i < num_input_files; i++)
    {
        std::cout << "\nStarting to count words for " << input_files[i] << std::endl;

        std::ifstream in_file;
        in_file.open(input_files[i]);

        // Check if file was opened successfully
        if (!in_file)
        {
            std::cerr << "Unable to open " << input_files[i] << "!" << std::endl;
            exit(1);
        }

        // Read lines from file
        while (in_file)
        {
            std::string line_buffer;
            std::getline(in_file, line_buffer);
            GetWordCountsFromString(line_buffer, word_counts);
        }

        in_file.close();
        std::cout << "Finished counting words for " << input_files[i] << std::endl;
    }

    // Write the word counts to file
    if (!WriteWordCountsToFile(word_counts, output_filename)) 
    {
        std::cerr << "Failed write to " << output_filename << "!" << std::endl;
        exit(1);
    }


    runtime += omp_get_wtime(); // Stop timer
    std::cout << "\nSerial execution time " << runtime << "seconds" << std::endl;

    return 0;
}

/**

/// @brief Reads a string and updates a hash map with the number of appearances for each word
/// @param line_buffer
/// @param word_counts
void GetWordCountsFromString(std::string& line_buffer, std::unordered_map<std::string, int>& word_counts)
{
    std::istringstream buffer_string_stream(line_buffer);
    std::string word;
    while (buffer_string_stream >> word) {
        // Update hash map
        UpdateWordCounts(word_counts, word);
    }
}

/// @brief Increments map entry for the given key
/// @param word_counts
/// @param word
void UpdateWordCounts(std::unordered_map<std::string, int>& word_counts, const std::string& word)
{
    if (word_counts.find(word) == word_counts.end()) {
        word_counts.insert({word, 1});
    } else {
        word_counts[word] += 1;
    }
}
*/
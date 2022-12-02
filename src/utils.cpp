#include "../include/utils.hpp"

/// @brief Reads a string and updates a hash map with the number of appearances for each word
/// @param line_buffer
/// @param word_counts
void GetWordCountsFromLine(std::string &line_buffer, std::unordered_map<std::string, int> &word_counts)
{
    std::istringstream word_buffer(line_buffer);
    std::string word;
    while (word_buffer >> word)
    {
        // Update hash map
        UpdateWordCounts(word_counts, word);
    }
}

/// @brief Increments map entry for the given key
/// @param word_counts
/// @param word
void UpdateWordCounts(std::unordered_map<std::string, int> &word_counts, const std::string &word)
{
    if (word_counts.find(word) == word_counts.end())
    {
        word_counts.insert({word, 1});
    }
    else
    {
        word_counts[word] += 1;
    }
}

/// @brief 
/// @param word_counts 
/// @param filename 
/// @return 
bool WriteWordCountsToFile(const std::unordered_map<std::string, int> &word_counts, const std::string &filename)
{
    std::ofstream out_file{filename};

    // Check if file was opened successfully
    if (!out_file)
    {
        std::cerr << "Unable to open file for writing!" << std::endl;
        exit(1);
    }

    std::unordered_map<std::string, int>::const_iterator it;
    for (it = word_counts.begin(); it != word_counts.end(); it++)
    {
        out_file << it->first
                 << ':'
                 << it->second
                 << std::endl;
    }

    out_file.close();
    return true; 
}

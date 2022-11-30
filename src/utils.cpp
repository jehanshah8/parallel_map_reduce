#include "../include/utils.hpp"


/// @brief Writes hash map to file
/// @param word_counts 
/// @param filename 
void WriteWordCountsToFile(const std::unordered_map<std::string, int>& word_counts, const std::string filename) 
{
    std::ofstream out_file{filename};
  
    // Check if file was opened successfully
    if (!out_file) {   
        std::cerr << "Unable to open file for writing!" <<std::endl;
        exit(1);
    }

    std::unordered_map<std::string, int>::const_iterator it; 
    for (it = word_counts.begin(); it != word_counts.end(); it++) {
    out_file << it->first    
            << ':'
            << it->second
            << std::endl;
    } 

    out_file.close();
}
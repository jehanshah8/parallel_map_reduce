#ifndef UTILS_H_
#define UTILS_H_

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

void GetWordCountsFromLine(std::string& line_buffer, std::unordered_map<std::string, int>& word_counts); 
void UpdateWordCounts(std::unordered_map<std::string, int>& word_counts, const std::string& word);

void WriteWordCountsToFile(const std::unordered_map<std::string, int>& word_counts, const std::string filename); 

#endif /* UTILS_H_ */
#ifndef UTILS_H_
#define UTILS_H_

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

void WriteWordCountsToFile(const std::unordered_map<std::string, int>& word_counts, const std::string filename); 

#endif /* UTILS_H_ */
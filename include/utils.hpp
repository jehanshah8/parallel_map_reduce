#ifndef UTILS_H_
#define UTILS_H_

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
#include <sstream>

#include <string>
#include <unordered_map>
#include <vector>

#include<algorithm>

int OpenFile(const char *filename);
void UnmapAndCloseFile(int fd, char *file_buffer, size_t file_size);
size_t GetFileSize(int fd);
void *MmapFileToRead(int fd, size_t file_size);
void PrintFileBuffer(const char *file_buffer);
bool IsDelimiter(char c);
void GetWordCountsFromString(std::string &line_buffer, std::unordered_map<std::string, int> &word_counts);
void UpdateWordCounts(std::unordered_map<std::string, int> &word_counts, const std::string &word, int count);
bool SortAndWriteWordCountsToFile(const std::unordered_map<std::string, int> &word_counts, const std::string &filename);

bool WriteWordCountsToFile(const std::unordered_map<std::string, int> &word_counts, const std::string &filename);
#endif /* UTILS_H_ */
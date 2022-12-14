#include "../include/utils.hpp"

int OpenFile(const char *filename)
{
    int fd = open(filename, O_RDONLY);
    if (fd == -1)
    {
        std::cerr << "Unable to open file: " << filename << std::endl;
        exit(1);
    }
    return fd;
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

void PrintFileBuffer(const char *file_buffer)
{
    // Print entire buffer for debugging
    std::cout << "\nFile buffer: " << std::endl;
    std::cout << '\t' << file_buffer << std::endl;
    std::cout << "\nEnd file buffer" << std::endl;
}

bool IsDelimiter(char c)
{
    return (c == ' ' || c == '\n' || c == '\0');
}

/// @brief Reads a string and updates a hash map with the number of appearances for each word
/// @param line_buffer
/// @param word_counts

void GetWordCountsFromString(std::string &line_buffer, std::unordered_map<std::string, int> &word_counts)
{
    int word_start_idx = 0;
    int word_length = 0;
    for (int j = 0; j <= line_buffer.size(); j++) // read including null terminator
    {
        if (IsDelimiter(line_buffer[j]))
        {
            if (word_length)
            {
                std::string word = line_buffer.substr(word_start_idx, word_length);
                UpdateWordCounts(word_counts, word, 1);
                word_length = 0;
            }

            word_start_idx = j + 1;
        }
        else
        {
            word_length++;
        }
    }

    // std::istringstream word_buffer(line_buffer);
    // std::string word;
    // while (word_buffer >> word)
    // {
    //     // Update hash map
    //     UpdateWordCounts(word_counts, word, 1);
    // }
}

/// @brief Increments map entry for the given key
/// @param word_counts
/// @param word
void UpdateWordCounts(std::unordered_map<std::string, int> &word_counts, const std::string &word, int count)
{
    if (word_counts.find(word) == word_counts.end())
    {
        word_counts.insert({word, count});
    }
    else
    {
        word_counts[word] += count;
    }
}

/// @brief
/// @param word_counts
/// @param filename
/// @return
bool SortAndWriteWordCountsToFile(const std::unordered_map<std::string, int> &word_counts, const std::string &filename)
{
    std::vector<std::pair<std::string, int>> v;
    for (auto &it : word_counts)
    {
        v.push_back(it);
    }

    std::sort(v.begin(), v.end(), [](auto &left, auto &right)
              { return right.first < left.first; });

    std::ofstream out_file{filename};
    // Check if file was opened successfully
    if (!out_file)
    {
        std::cerr << "Unable to open file for writing!" << std::endl;
        exit(1);
    }

    for (auto &it : v)
    {
        out_file << it.first
                 << ':'
                 << it.second
                 << std::endl;
    }

    out_file.close();
    return true;
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

void JoinMaps(std::vector<std::unordered_map<std::string, int>> &maps, std::unordered_map<std::string, int> &combined_map)
{
    for (int i = 0; i < maps.size(); i++)
    {
        combined_map.insert(maps.at(i).begin(), maps.at(i).end());
    }
}
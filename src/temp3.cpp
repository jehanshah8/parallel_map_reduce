/******************************************************************************

                              Online C++ Compiler.
               Code, Compile, Run and Debug C++ program online.
Write your code in this editor and press "Run" button to compile and execute it.

*******************************************************************************/

#include <iostream>
#include <unordered_map>
#include <stdio.h>
#include <string.h>
#include <sstream>
void SerializeMap (const std::unordered_map < std::string, int >&map, std::string &map_str);
void DeserializeMap(std::unordered_map <std::string, int>&map, char* buffer, size_t buffer_size); 

int main ()
{
  std::cout << "Hello World" << std::endl;

    std::unordered_map < std::string, int >map (
					       {
					       {
					       "orange", 69},
					       {
					       "strawberry", 1000}});

    std::string map_str; 
    SerializeMap (map, map_str);
    std::cout << map_str << std::endl;    
    
    std::unordered_map < std::string, int >new_map;
    DeserializeMap(new_map, map_str.data(), map_str.size());
    
    for (auto &it : new_map)
    {
        std::cout << it.first << " : " << it.second << std::endl; 
    }

    return 0;
}

void SerializeMap (const std::unordered_map < std::string, int >&map, std::string &map_str)
{
    std::ostringstream oss;
    for (auto& it : map) {
        oss << it.first << ":" << it.second << " ";
    }
    map_str = oss.str();
}

void DeserializeMap(std::unordered_map <std::string, int>&map, char* buffer, size_t buffer_size)
{
    int i = 0;
    while (i < buffer_size)
    {
        char* k = &buffer[i]; 
        int key_size = -i;
        while(buffer[i++] != ' ');
        key_size += i; 
        std::string key(k, key_size); 
        
        char* v = &buffer[i]; 
        int val_size = -i;
        while(buffer[i++] != ' ');
        val_size += i; 
        std::string val(v, val_size); 
        
        map[key] = stoi(val); 
    }
}




#include <string>
#include <unordered_map>
#include <vector>

#include <iostream>
#include <fstream>
#include <sstream>

#include <omp.h>
#include <mpi.h>

void SerializeMap (const std::unordered_map < std::string, int >&map, std::string &map_str);
void DeserializeMap(const char* buffer, size_t buffer_size, std::unordered_map <std::string, int>&map); 

int main(int argc, char *argv[])
{
    int pid;
    int num_procs;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);       // get MPI process id
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs); // get number of MPI processes

    if (!pid)
    {
        // Program configuration
        std::cout << "Small MPI Execution" << std::endl;
        std::cout << "  - Number of MPI processes = " << num_procs << std::endl;
    }

    std::vector<std::unordered_map<std::string, int>> intermediate_maps(num_procs);
   
    intermediate_maps[0] = {
                { "Mars", 3000},
                { "Saturn", 60000},
                { "Jupiter", 70000 } };
  
    intermediate_maps[1] = {
                { "Apple", 3000},
                { "Banana", 60000},
                { "Strawberry", 60000},
                { "Orange", 70000 } };

    intermediate_maps[2] = {
                { "Nike", 2000},
                { "Adidas", 5},
                { "Puma", 99},
                { "Hoka", 4 } };

    std::cout << "\n" << "[" << pid << "] " << "Hello!" << std::endl;

    MPI_Barrier(MPI_COMM_WORLD);

    // serialize
    std::vector<std::string> map_strs_to_send (num_procs); 
    std::vector<size_t> send_buffer_sizes(num_procs);
 
    // #pragma omp parallel for
    for (int i = 0; i < intermediate_maps.size(); i++)
    {
        if (i != pid)
        {
            SerializeMap (intermediate_maps[i], map_strs_to_send[i]);
            send_buffer_sizes[i] = map_strs_to_send[i].size() + 1;
            std::cout << "\n" << "[" << pid << "] " << "Serialized map " << i << ": "<< map_strs_to_send[i] << std::endl;
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);


    // Post receives to receive size of intermediate maps from each process
    std::vector<size_t> recv_buffer_sizes(num_procs);
    MPI_Request size_recv_requests[num_procs];
    for (int i = 0; i < num_procs; i++)
    {
        if (i != pid)
        {
            // Use non-blocking recvs so that we can simply post and move on to sending
            // Need to check status later to make sure we got it before processing 
            MPI_Irecv(&recv_buffer_sizes[i], 1, MPI_UNSIGNED_LONG, i, 1, MPI_COMM_WORLD, &size_recv_requests[i]);
        }
    }

    // Send sizes
    MPI_Request size_send_requests[num_procs];
    for (int i = pid + 1; i < pid + num_procs; i++)
    {
        int dest = i % num_procs;
        // Use async non-blocking sends because intermd maps will be unchnaged later, safe to move on
        MPI_Isend(&(send_buffer_sizes[dest]), 1, MPI_UNSIGNED_LONG, dest, 1, MPI_COMM_WORLD, &size_send_requests[dest]);
        std::cout << "\n" << "[" << pid << "] " << "Sent size = " << send_buffer_sizes[dest] << " to node " << dest << std::endl;
    }

    // Check if size recvd and post recv for data
    std::vector<char*> map_strs_recvd (num_procs);
    MPI_Request data_recv_requests[num_procs];
    for (int i = 0; i < num_procs; i++)
    {
        if (i != pid)
        {
            MPI_Wait(&size_recv_requests[i], MPI_STATUS_IGNORE);
            map_strs_recvd[i] = new char[recv_buffer_sizes[i]];
            std::cout << "\n" << "[" << pid << "] " << "Got size = " << recv_buffer_sizes[i] << " from node " << i << std::endl;
            MPI_Irecv(map_strs_recvd[i], recv_buffer_sizes[i], MPI_CHAR, i, 2, MPI_COMM_WORLD, &data_recv_requests[i]);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // Send the data 
    MPI_Request data_send_requests[num_procs];
    for (int i = pid + 1; i < pid + num_procs; i++)
    {
        int dest = i % num_procs;
        // Use async non-blocking sends because intermd maps will be unchnaged later, safe to move on
        MPI_Isend(map_strs_to_send[dest].data(), send_buffer_sizes[dest], MPI_CHAR, dest, 2, MPI_COMM_WORLD, &data_send_requests[dest]);
        std::cout << "\n" << "[" << pid << "] " << "Sent buffer = " << map_strs_to_send[dest].data() << " to node" << dest << std::endl;
    }

    for (int i = 0; i < num_procs; i++)
    {
        if (i != pid)
        {
            MPI_Wait(&data_recv_requests[i], MPI_STATUS_IGNORE);
            std::cout << "\n" << "[" << pid << "] " << "from = " << i << " Got data = " << map_strs_recvd[i] << std::endl;    
        }
    }

    // Free recv buffers
    for (int i = 0; i < map_strs_recvd.size(); i++)
    {
        if (i != pid)
        {
            delete[] map_strs_recvd[i];
        }
    }
    

    // Wait until all sends are complete 
    for (int i = 0; i < num_procs; i++)
    {
        if (i != pid)
        {
            MPI_Wait(&size_send_requests[i], MPI_STATUS_IGNORE);
            MPI_Wait(&data_send_requests[i], MPI_STATUS_IGNORE);
        }
    }

    MPI_Finalize();
    std::cout << "Exiting!" << std::endl; 
    return 0;
}

/*
size_t SerializeMap (const std::unordered_map < std::string, int >&map, char *buffer)
{
    buffer = new char[map.size () * sizeof (std::pair < std::string, int >)];
    if (buffer == nullptr)
    {
      std::cerr << "Not enought memory to serialize buffer" << std::endl;
      // MPI_Finalize();
      exit (1);
    }

    int offset = 0;
    for (auto & it:map)
    {
        size_t key_size = it.first.size();
        memcpy(buffer + offset, &key_size, sizeof(size_t));
        offset += sizeof(size_t);

        memcpy (buffer + offset, it.first.data(), it.first.size());
        offset += it.first.size();

        memcpy (buffer + offset, &it.second, sizeof (int));
        offset += sizeof (int);

        std::cout << offset << std::endl;
        // std::cout << buffer << std::endl;
        for (int i = 0; i < offset; i++)
	    {
	        std::cout << buffer[i];
	    }
        std::cout << std::endl;
        
    }

    return offset;
}
*/

void SerializeMap (const std::unordered_map < std::string, int >&map, std::string &map_str)
{
    std::ostringstream oss;
    for (auto& it : map) {
        oss << it.first << " " << it.second << " ";
    }
    map_str = oss.str();
}

void DeserializeMap(const char* buffer, size_t buffer_size, std::unordered_map <std::string, int>&map)
{
    int i = 0;
    while (i < buffer_size)
    {
        const char* k = &buffer[i]; 
        int key_size = -i;
        while(buffer[i] != ' ')
        {
            i++;    
        }
        
        key_size += i; 
        std::string key(k, key_size); 

        i++; 

        int val = 0; 
        while(buffer[i] != ' ')
        {
            val *= 10; 
            val += buffer[i] - '0'; 
            i++; 
        }
         
        i++; 
        
        map[key] = val;
        
        // std::cout << key << ":" << val; 
        // std::cout << "#" << std::endl;
    }
}
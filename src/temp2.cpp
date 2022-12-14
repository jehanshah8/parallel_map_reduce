#include <unordered_map>
#include <mpi.h>

// The map that will be sent and received
std::unordered_map<std::string, int> map;

// Buffers for sending and receiving the map
char* send_buf = nullptr;
int send_buf_size = 0;
char* recv_buf = nullptr;
int recv_buf_size = 0;

// Request objects for non-blocking send and receive
MPI_Request send_req;
MPI_Request recv_req;

// Initialize the map
map["key1"] = 1;
map["key2"] = 2;
map["key3"] = 3;

// Pack the map into a buffer for sending
MPI_Pack_size(map.size(), MPI_CHAR, MPI_COMM_WORLD, &send_buf_size);
send_buf = new char[send_buf_size];
int pos = 0;
for (const auto& [key, value] : map) {
  int key_len = key.size();
  MPI_Pack(&key_len, 1, MPI_INT, send_buf, send_buf_size, &pos, MPI_COMM_WORLD);
  MPI_Pack(key.data(), key_len, MPI_CHAR, send_buf, send_buf_size, &pos, MPI_COMM_WORLD);
  MPI_Pack(&value, 1, MPI_INT, send_buf, send_buf_size, &pos, MPI_COMM_WORLD);
}

// Allocate a buffer for receiving the map
MPI_Pack_size(map.

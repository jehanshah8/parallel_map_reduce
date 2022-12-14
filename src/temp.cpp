#include <mpi.h>
#include <unordered_map>
#include <string>

// Create an unordered map of strings to integers
std::unordered_map<std::string, int> map;

// Insert some values into the map
map["foo"] = 1;
map["bar"] = 2;

// Get the size of the map
int map_size = map.size();

// Create a buffer to hold the serialized map data
char* buffer = new char[map_size * sizeof(std::pair<std::string, int>)];

// Serialize the map into the buffer
int offset = 0;
for (const auto& kv : map) {
  const std::string& key = kv.first;
  const int& value = kv.second;

  // Serialize the key
  int key_size = key.size();
  std::memcpy(buffer + offset, &key_size, sizeof(int));
  offset += sizeof(int);
  std::memcpy(buffer + offset, key.data(), key_size);
  offset += key_size;

  // Serialize the value
  std::memcpy(buffer + offset, &value, sizeof(int));
  offset += sizeof(int);
}

int rank;
MPI_Comm_rank(MPI_COMM_WORLD, &rank);

if (rank == 0) {
  // Rank 0 sends the map to rank 1 using non-blocking send
  MPI_Request request;
  MPI_Isend(buffer, map_size * sizeof(std::pair<std::string, int>), MPI_CHAR, 1, 0, MPI_COMM_WORLD, &request);

  // Do some other work here...

  // Wait for the send to complete
  MP



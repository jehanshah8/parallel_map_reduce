#ifndef THREAD_SAFE_MAP_H_
#define THREAD_SAFE_MAP_H_

#include <unordered_map>

template <typename K, typename V>
class ThreadSafeMap
{
public:
    ThreadSafeMap();

private:
    std::unordered_map<K, V> map_;
};

template <typename K, typename V>
ThreadSafeMap<K, V>::ThreadSafeMap()
{
    ;
}

#endif /* THREAD_SAFE_MAP_H_ */:
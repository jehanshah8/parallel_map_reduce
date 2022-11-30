#ifndef TS_QUEUE_H_
#define TS_QUEUE_H_

#include <queue>

class TS_QUEUE {
    public: 
        TS_QUEUE();

        size_t size();

        void push()

        bool get_is_active();
        void set_is_active(bool is_active); 

    private: 
        std::queue<std::string> q;
        bool is_active; 
};

#endif /* TS_QUEUE_H_ */
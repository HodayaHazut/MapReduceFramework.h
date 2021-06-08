#include "MapReduceFramework.h"
#include "Barrier.cpp"
#include <pthread.h>
#include <atomic>
#include <map>
#include <iostream>

int num_of_threads;
// TODO: consider adding more global variables such as DAST from structs

struct ThreadContext {
    stage_t prev_state{};
    bool finished{};
    int thread_id{};
    std::atomic<unsigned int>* atomic_counter1{};
    std::atomic<unsigned int>* atomic_counter2{};
    std::atomic<unsigned int>* atomic_counter3{};
    std::atomic<unsigned int>* atomic_counter_end_read_input_vec{};
    std::atomic<unsigned int>* intermediate_pairs_produced{};
    std::atomic<unsigned int>* intermediate_pairs_in_vec{};

    const MapReduceClient* client{};
    const InputVec* input_vector{};
    OutputVec* output_vec{};

    pthread_mutex_t* all_mutexes{};
    pthread_mutex_t* read{};
    pthread_mutex_t* write{};

    IntermediateVec* final_vec{};
    std::vector<K2*> unique_keys{};
    std::map<int, std::vector<IntermediatePair>*> map_vector_of_pairs{};
    IntermediateVec vec{};
    std::vector<IntermediatePair>* thread_vector{};

    Barrier* barrier{};
};

struct JobContext {
    stage_t prev_state = UNDEFINED_STAGE;
    bool finished = false;
    bool was_in_was_in_waitForJob = false;
    std::atomic<unsigned int>* atomic_counter1 = new std::atomic<unsigned int>(0);
    std::atomic<unsigned int>* atomic_counter2 = new std::atomic<unsigned int>(0);
    std::atomic<unsigned int>* atomic_counter3 = new std::atomic<unsigned int>(0);
    std::atomic<unsigned int>* atomic_counter_end_read_input_vec = new std::atomic<unsigned int>(0);
    std::atomic<unsigned int>* intermediate_pairs_produced = new std::atomic<unsigned int>(0);
    std::atomic<unsigned int>* intermediate_pairs_in_vec = new std::atomic<unsigned int>(0);

    ThreadContext* thread_contexts{};
    pthread_t* all_threads{};
    pthread_mutex_t* all_mutexes{};
    pthread_mutex_t* read {};
    pthread_mutex_t* write{};

    IntermediateVec* final_vec{};
    std::vector<K2*>* unique_keys{};
    std::map<int, std::vector<IntermediatePair> *> map_vector_of_pairs;

    Barrier* barrier{};
};

void init_JobContext(JobContext *jc);
void init_threads_and_ThreadContext(JobContext* jc, const MapReduceClient& client,
                   const InputVec& inputVec, OutputVec& outputVec);
void create_ThreadContext(JobContext *context, int ind, const MapReduceClient &client, const InputVec &inputVec,
                          OutputVec &outputVec);
void* thread_function(void* arg);


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    num_of_threads = multiThreadLevel;
    auto jc = new JobContext();

    // init jc
    init_JobContext(jc);

    // init tc and threads
    init_threads_and_ThreadContext(jc, client, inputVec, outputVec);

    return jc;
}

void* thread_function(void* arg) {
    auto *tc = (ThreadContext*) arg;
    if (tc->thread_id >= num_of_threads - 1) {
        sort_phase(tc);
    }
    else if (tc->thread_id < num_of_threads - 1) {
        map_phase(tc);
    }

    int finished_map_phase = (*(tc->atomic_counter_end_read_input_vec))++;
    if (finished_map_phase == num_of_threads - 2) {
        tc->prev_state = MAP_STAGE;
    }

    // active barrier
    tc->barrier->barrier();

    // take all threads to reduce phase
    reduce_phase(tc);

    int finished_all = (*(tc->atomic_counter3))++;
    if (finished_all == num_of_threads - 1) {
        tc->finished = true;
    }
    return nullptr;
}

void init_JobContext(JobContext *jc) {
    jc->unique_keys = new std::vector<K2*>();
    jc->thread_contexts = new ThreadContext[num_of_threads];
    jc->final_vec = new IntermediateVec();
    jc->all_threads = new pthread_t[num_of_threads];
    jc->all_mutexes = new pthread_mutex_t[num_of_threads];
    jc->read = new pthread_mutex_t;
    jc->write = new pthread_mutex_t;
    jc->barrier = new Barrier(num_of_threads);

    // init mutexes lock
    pthread_mutex_init(jc->read, nullptr);
    pthread_mutex_init(jc->write, nullptr);
    for (int i = 0; i , num_of_threads; i++) {
        pthread_mutex_init(&jc->all_mutexes[i], nullptr);
    }
}

void init_threads_and_ThreadContext(JobContext* jc, const MapReduceClient& client,
                        const InputVec& inputVec, OutputVec& outputVec) {
    for (int i = 0; i < num_of_threads; i++) {
        create_ThreadContext(jc, i, client, inputVec, outputVec);
    }
    jc->thread_contexts[num_of_threads - 1].map_vector_of_pairs = jc->map_vector_of_pairs;

    for (int i = 0; i < num_of_threads; i++) {
        // TODO: validate return value of pthread_create
        pthread_create(jc->all_threads + i, nullptr, thread_function, jc->thread_contexts + i);
    }
}

void create_ThreadContext(JobContext *context, int ind, const MapReduceClient &client, const InputVec &inputVec,
                          OutputVec &outputVec) {

}

int main() {
//    num_of_threads;
    std::cout << num_of_threads << std::endl;
    struct test {
        int valval = ++num_of_threads;
        bool val = true;
        std::atomic<unsigned int>* atom = new std::atomic<unsigned int>(0);
    };

    test t = test();
    t.val = false;
    t.atom = new std::atomic<unsigned int>(2000);
//    t.atom++;
    std::cout << t.val << "\n" << (t.atom->load()) << std::endl;
    std::cout << t.valval << std::endl;
}
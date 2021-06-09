//
// Created by hoday on 07/06/2021.
//
#include "MapReduceFramework.h"
#include <pthread.h>
#include <map>
#include <atomic>
#include <iostream>
#include <algorithm>
#include "Barrier.h"

#define FAILURE 1
#define SYS_ERROR "system error: "
#define MUTEX_LOCK_ERR "the mutex lock failed"
#define MUTEX_UNLOCK_ERR "the mutex unlock failed"
#define ALLOC_ERROR "allocation failed"
#define THREAD_CREATE_FAIL "thread creation fail"
#define MUTEX_CREATE_FAIL "mutex create fail"
#define  MUTEX_DESTROY_FAIL "mutex destroy fail"
#define THREAD_JOIN_FAIL "thread join fail"



struct ThreadContext;
void mutexUnlock(pthread_mutex_t* mutex);
void mutexLock(pthread_mutex_t* mutex);
void sortPhase(ThreadContext* context);
void shufflePhase(ThreadContext* context);
void reducePhase(ThreadContext* context);

// TODO: need to update deletes for ALL data structures


/**
 * Define the context of new job that run the MapReduce model.
 */
struct jobContext{
    JobState state{};
    ThreadContext* threadContextsVec{}; // vector of threads contexts
    int MULTI_THREAD_NUM{};

    InputVec * inputVector{};
    //std::vector <IntermediateVec*> shuffleOutputVector{};
    std::map<K2*, IntermediateVec*> shuffleMap{};
    OutputVec* reduceOutputVector{};

    std::atomic<unsigned int> numIntermediaryElements{};
    std::atomic<unsigned int> mapProgressCounter{};
    std::atomic<unsigned int> shuffleProgressCounter{};
    std::atomic<unsigned int> reduceProgressCounter{};

    pthread_mutex_t stateMutex{};
    pthread_mutex_t mapOutputVecMutex{};
    pthread_mutex_t reduceOutputVecMutex{};

    bool wasInWaitForJob{};
    Barrier* barrier{};
};


/**
 * Define the context of each thread.
 */
struct ThreadContext {
    pthread_t threadId{};
    jobContext* job{};
    IntermediateVec* mapOutputVector{};
    const MapReduceClient* client{};
};


/**
* The function gets a mutex and lock it.
*/
void mutexLock(pthread_mutex_t* mutex)
{
    if (pthread_mutex_lock(mutex) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_LOCK_ERR << std::endl;
        exit(FAILURE);
    }
}

/**
 * The function gets a mutex and unlock it.
 */
void mutexUnlock(pthread_mutex_t* mutex)
{
    if (pthread_mutex_unlock(mutex) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_UNLOCK_ERR << std::endl;
        exit(FAILURE);
    }
}


/**
 * The function receives as input intermediary element (K2, V2) and context which contains
 * data structure of the thread that created the intermediary element. The function saves the
 * intermediary element in the context data structures. In addition, the function updates the
 * number of intermediary elements using atomic counter.
 * @param key K2 key
 * @param value V2 value
 * @param context data structure of the thread
 */
void emit2 (K2* key, V2* value, void* context)
{
    auto* currThreadContext = (ThreadContext*) context;
    std::pair <K2*,V2*> pair (key,value);
    currThreadContext->job->numIntermediaryElements++;
    // protect with mutex
    mutexLock(&currThreadContext->job->mapOutputVecMutex);
    currThreadContext->mapOutputVector->push_back(pair);
    mutexUnlock(&currThreadContext->job->mapOutputVecMutex);
}


/**
 * The function receives as input output element (K3, V3) and context which contains data
 * structure of the thread that created the output element. The function saves the output
 * element in the context data structures (output vector). In addition, the function updates the
 * number of output elements using atomic counter.
 * @param key K3 key
 * @param value V3 value
 * @param context context data structure of the thread
 */
void emit3 (K3* key, V3* value, void* context)
{
    auto* jc = (jobContext*) context;
    std::pair <K3*,V3*> pair (key,value);
    // protect with mutex
    mutexLock(&jc->reduceOutputVecMutex);
    jc->reduceOutputVector->push_back(pair);
    mutexUnlock(&jc->reduceOutputVecMutex);
}


/**
 * In this phase each thread reads pairs of (k1, v1) from the input vector and calls the map function
 * on each of them. The map function in turn will produce (k2, v2) and will call the emit2 function to
 * update the framework databases.
 * @param context context of a thread
 */
void mapPhase(ThreadContext* context)
{
    jobContext* jc = context->job;
    unsigned int oldVal = jc->mapProgressCounter;
    while (oldVal < (unsigned int) jc->inputVector->size())
    {
        jc->mapProgressCounter++;
        K1* key = (*jc->inputVector)[oldVal].first;
        V1* val = (*jc->inputVector)[oldVal].second;
        context->client->map(key, val, context);

        mutexLock(&jc->stateMutex);
        jc->state.percentage = ((float)(jc->mapProgressCounter) / (float)(jc->inputVector->size())) * 100 ;
        mutexUnlock(&jc->stateMutex);

        oldVal = jc->mapProgressCounter;
    }

}
/**
 * function sorts vector by key
 * @param left pair
 * @param right pair
 * @return bool if left_key < right_key
 */
bool sortKeys(const IntermediatePair &left, const IntermediatePair &right) {
    return (*left.first) < (*right.first);
}

/**
 * handeling sorting phase hen we want to sort intermidiate vector of each thread
 * @param context thread context of weach we ant to sort its intermediate vec
 */
void sortPhase(ThreadContext *context) {
    std::sort(context->mapOutputVector->begin(), context->mapOutputVector->end(), sortKeys);
}
/**
 * we want that thread[0] will take all intermediate vecs from all threads and shuffle them into one vector
 * @param context to handle
 */
void shufflePhase(ThreadContext *context) {
    auto* jc = context->job;
    for (int i = 0; i < jc->MULTI_THREAD_NUM; ++i) {
        while (! jc->threadContextsVec[i].mapOutputVector->empty())
        {
            IntermediatePair pair = jc->threadContextsVec[i].mapOutputVector->back();
            auto key = pair.first;
            jc->shuffleMap[key]->push_back(pair);
            jc->shuffleProgressCounter++;
            jc->threadContextsVec[i].mapOutputVector->pop_back();
        }
    }
    mutexLock(&jc->stateMutex);
    jc->state.stage = SHUFFLE_STAGE;
    jc->state.percentage = ((float)(jc->shuffleProgressCounter) / (float)(jc->numIntermediaryElements)) * 100;
    mutexUnlock(&jc->stateMutex);
}
/**
 * The reducing threads will wait for the shuffled vectors to be created by the shuffling thread.
 * Once they wake up, they can pop a vector from the back of the queue and run reduce on it
 * The reduce function in turn will produce (k3, v3) pairs and will call emit3 to add them to the
 * framework data structures.
 * @param context context of a thread
 */
void reducePhase(ThreadContext *context) {
    auto* jc = context->job;
    mutexLock(&jc->stateMutex);
    jc->state.stage = REDUCE_STAGE;
    jc->state.percentage = 0;
    mutexUnlock(&jc->stateMutex);

    while (true) {
        mutexLock(&jc->reduceOutputVecMutex);
        auto pair = jc->shuffleMap.begin();
        if (pair == jc->shuffleMap.end()) {
            mutexUnlock(&jc->reduceOutputVecMutex);
            break;
        }
        auto vectorToReduce = pair->second;
        jc->shuffleMap.erase(pair);
        mutexUnlock(&jc->reduceOutputVecMutex);

        context->client->reduce(vectorToReduce, context->job);
        jc->reduceProgressCounter++;

        mutexLock(&jc->stateMutex);
        jc->state.percentage = ((float) (jc->reduceProgressCounter) / (float) (jc->shuffleMap.size())) * 100;
        mutexUnlock(&jc->stateMutex);
    }

}
void* jobManager(void* context)
{
    auto* currThreadContext = (ThreadContext*) context;

    mutexLock(&currThreadContext->job->stateMutex);
    currThreadContext->job->state.stage = MAP_STAGE;
    mutexUnlock(&currThreadContext->job->stateMutex);

    mapPhase(currThreadContext);
    sortPhase(currThreadContext);

    currThreadContext->job->barrier->barrier();

    if (currThreadContext->job->threadContextsVec[0].threadId == currThreadContext->threadId)
    {
        shufflePhase(currThreadContext);
    }
    currThreadContext->job->barrier->barrier();
    reducePhase(currThreadContext);
    return nullptr;
}

void initVectors(const InputVec &inputVec, OutputVec &outputVec, jobContext *jc) {
    for (int i = 0; i < jc->MULTI_THREAD_NUM; ++i) {
        jc->threadContextsVec[i].mapOutputVector = new std::vector <IntermediatePair>();
        if (jc->threadContextsVec[i].mapOutputVector == nullptr)
        {
            std::cerr << SYS_ERROR << ALLOC_ERROR << std::endl;
            exit(FAILURE);
        }
    }

    jc->reduceOutputVector = &outputVec;
    jc->inputVector = (InputVec*)&inputVec;
}

void initCounters(jobContext *jc) {
    jc->numIntermediaryElements = 0;
    jc->mapProgressCounter = 0;
    jc->shuffleProgressCounter = 0;
    jc->reduceProgressCounter = 0;
}

void initState(jobContext *jc) {
    mutexLock(&jc->stateMutex);
    jc->state.stage = UNDEFINED_STAGE;
    jc->state.percentage = 0;
    mutexUnlock(&jc->stateMutex);
}

void initMutexes(jobContext *jc) {
    if(pthread_mutex_init(&jc->stateMutex, nullptr) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_CREATE_FAIL << std::endl;
        exit(FAILURE);
    }
    if(pthread_mutex_init(&jc->mapOutputVecMutex, nullptr) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_CREATE_FAIL << std::endl;
        exit(FAILURE);
    }
    if(pthread_mutex_init(&jc->reduceOutputVecMutex, nullptr) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_CREATE_FAIL << std::endl;
        exit(FAILURE);
    }
}

/**
 * The function starts running the MapReduce algorithm (with several threads) and returns a JobHandle
 * @param client The implementation of MapReduceClient or in other words the task that the
 * framework should run.
 * @param inputVec a vector of type std::vector<InputPair>, the input elements.
 * @param outputVec a vector of type std::vector<OutputPair>, to which the output elements will be added before
 * returning.
 * @param multiThreadLevel the number of worker threads to be used for running the algorithm
 * @return JobHandle that will be used for monitoring the job.
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    auto jc = new jobContext();
    jc->MULTI_THREAD_NUM = multiThreadLevel;

    //init thread contexts vector
    jc->threadContextsVec = new ThreadContext[multiThreadLevel];
    if (jc->threadContextsVec == nullptr) {
        std::cerr << SYS_ERROR << ALLOC_ERROR << std::endl;
        exit(FAILURE);
    }
    initMutexes(jc);
    initState(jc);
    initCounters(jc);
    initVectors(inputVec, outputVec, jc);

    jc->barrier = new Barrier(multiThreadLevel);
    if (jc->barrier == nullptr) {
        std::cerr << SYS_ERROR << ALLOC_ERROR << std::endl;
        exit(FAILURE);
    }

    // create threadContext for each thread
    for (int i = 0; i < multiThreadLevel; ++i) {
        jc->threadContextsVec[i].job = jc;
        jc->threadContextsVec[i].client = &client;
    }

    // create threads
    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(&jc->threadContextsVec[i].threadId, nullptr,
                           jobManager, &jc->threadContextsVec[i]) != 0) {
            std::cerr << SYS_ERROR << THREAD_CREATE_FAIL << std::endl;
            exit(FAILURE);
        }

        return (JobHandle) jc;
    }
}


/**
 * The function gets JobHandle returned by startMapReduceFramework and waits
 * until it is finished.
 * @param job the job we want to waiting for
 */
void waitForJob(JobHandle job) {
    auto* jc = (jobContext*) job;
    if (!jc->wasInWaitForJob) {
        for (int i = 0; i < jc->MULTI_THREAD_NUM; ++i) {
            if (pthread_join(jc->threadContextsVec[i].threadId, nullptr) != 0)
            {
                std::cerr << SYS_ERROR << THREAD_JOIN_FAIL << std::endl;
                exit(FAILURE);
            }
        }
        jc->wasInWaitForJob = true;
    }
}


/**
 * The function gets a JobHandle and updates the state of the job into the given
 * JobState struct.
 * @param job JobHandle object
 * @param state state to update
 */
void getJobState(JobHandle job, JobState *state) {
    auto *currJob = (jobContext *) job;
    mutexLock(&currJob->stateMutex);
    state->stage = currJob->state.stage;
    state->percentage = currJob->state.percentage;
    mutexUnlock(&currJob->stateMutex);
}


/**
 * Releasing all resources of a job. You should prevent releasing resources before the job finished.
 * After this function is called the job handle will be invalid.
 * @param job releasing all resources of this job.
 */
void closeJobHandle(JobHandle job) {
    waitForJob(job);

    auto *currJob = (jobContext *) job;

    if (pthread_mutex_destroy(&currJob->stateMutex) != 0) {
        std::cerr << SYS_ERROR << MUTEX_DESTROY_FAIL << std::endl;
        exit(FAILURE);
    }
    if (pthread_mutex_destroy(&currJob->reduceOutputVecMutex) != 0) {
        std::cerr << SYS_ERROR << MUTEX_DESTROY_FAIL << std::endl;
        exit(FAILURE);
    }
    if (pthread_mutex_destroy(&currJob->mapOutputVecMutex) != 0) {
        std::cerr << SYS_ERROR << MUTEX_DESTROY_FAIL << std::endl;
        exit(FAILURE);
    }
    delete[] currJob->threadContextsVec;
    delete currJob;

}








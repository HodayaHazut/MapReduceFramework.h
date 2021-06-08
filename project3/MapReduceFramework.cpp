//
// Created by hoday on 07/06/2021.
//
#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#define FAILURE 1
#define SYS_ERROR "system error: "
#define MUTEX_LOCK_ERR "the mutex lock failed"
#define MUTEX_UNLOCK_ERR "the mutex unlock failed"
#define ALLOC_ERROR "allocation failed"
#define THREAD_CREATE_FAIL "thread creation fail"
/**
 * Define the context of each thread.
 */
struct ThreadContext {
    pthread_t threadId;
    jobContext* littleJob;
};

/**
 * Define the context of new job that run the MapReduce model.
 */
struct jobContext{
    JobState state{};
    ThreadContext* threadContexts{}; // vector of threads contexts
    int MULTI_THREAD_NUM;

    std::vector <InputPair> inputVector{};
    std::vector <IntermediatePair> mapOutputVector{};
    std::vector <OutputPair> reduceOutputVector{};

    std::atomic<unsigned int> numIntermediaryElements{};
    std::atomic<unsigned int> numOutputElements{};

    pthread_mutex_t stateMutex;
    pthread_mutex_t mapOutputVecMutex;
    pthread_mutex_t reduceOutputVecMutex;
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
    ThreadContext* currThreadContext = (ThreadContext*) context;
    std::pair <K2*,V2*> pair (key,value);
    currThreadContext->littleJob->numIntermediaryElements++;
    // protect with mutex
    mutexLock(currThreadContext->littleJob->mapOutputVecMutex);
    currThreadContext->littleJob->mapOutputVector.push_back(pair);
    mutexUnlock(currThreadContext->littleJob->mapOutputVecMutex);
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
    ThreadContext* currThreadContext = (ThreadContext*) context;
    std::pair <K3*,V3*> pair (key,value);
    currThreadContext->littleJob->numOutputElements++;
    // protect with mutex
    mutexLock(currThreadContext->littleJob->reduceOutputVecMutex);
    currThreadContext->littleJob->reduceOutputVector.push_back(pair);
    mutexUnlock(currThreadContext->littleJob->reduceOutputVecMutex);
}
void* jobManager(void* context)
{
    return nullptr;
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
                            int multiThreadLevel)
{
    jobContext* jc = new jobContext();
    jc->MULTI_THREAD_NUM = multiThreadLevel;

    // init mutexes
    pthread_mutex_init(jc->stateMutex, nullptr);
    pthread_mutex_init(jc->mapOutputVecMutex, nullptr);
    pthread_mutex_init(jc->reduceOutputVecMutex, nullptr);

    // init state
    mutexLock(&jc->stateMutex);
    jc->state.stage = UNDEFINED_STAGE;
    jc->state.percentage = 0;
    mutexUnlock(&jc->stateMutex);

    // init counters
    jc->numIntermediaryElements = 0;
    jc->numOutputElements = 0;

    // init vectors
    jc->mapOutputVector = new std::vector <IntermediatePair>();
    jc->reduceOutputVector = &outputVec;
    jc->inputVector = &inputVec;

    //init thread contexts vector
    jc->threadContexts = new ThreadContext[multiThreadLevel];
    if (jc->threadContexts == nullptr)
    {
        std::cerr << SYS_ERROR << ALLOC_ERROR << std::endl;
        exit(FAILURE);
    }

    // creat threads
    for (int i = 0; i < multiThreadLevel - 1; ++i)
    {
        if (pthread_create(&jc->threadContexts[i].threadId, nullptr, jobManager, &jc->threadContexts[i]) != 0)
        {
            std::cerr << SYS_ERROR << THREAD_CREATE_FAIL << std::endl;
            exit(FAILURE);
        }
    }
    // create threadContext for each thread
    for (int i = 0; i < multiThreadLevel; ++i)
    {
        jc->threadContexts[i].littleJob = jc;
    }
}

void waitForJob(JobHandle job);
/**
 * The function gets a JobHandle and updates the state of the job into the given
 * JobState struct.
 * @param job JobHandle object
 * @param state state to update
 */
void getJobState(JobHandle job, JobState* state)
{
    jobContext* currJob = (jobContext*) job;
    mutexLock(&currJob->stateMutex);
    state->stage = currJob->state.stage;
    state->percentage = currJob->state.percentage;
    mutexUnlock(&currJob->stateMutex);
}

void closeJobHandle(JobHandle job);
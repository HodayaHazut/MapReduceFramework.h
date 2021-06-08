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
#define  MUTEX_DESTROY_FAIL "mutex destroy fail"
#define THREAD_JOIN_FAIL "thread join fail"
/**
 * Define the context of each thread.
 */
struct ThreadContext {
    pthread_t threadId;
    jobContext* job;
    MapReduceClient* client;
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
    std::vector <IntermediatePair> inputSortedandShuffledVec{};

    std::atomic<unsigned int> numIntermediaryElements{};
    std::atomic<unsigned int> numOutputElements{};
    std::atomic<unsigned int> mapCounter{};
    std::atomic<unsigned int> reduceCounter{};

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
    currThreadContext->job->numIntermediaryElements++;
    // protect with mutex
    mutexLock(currThreadContext->job->mapOutputVecMutex);
    currThreadContext->job->mapOutputVector.push_back(pair);
    mutexUnlock(currThreadContext->job->mapOutputVecMutex);
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
    currThreadContext->job->numOutputElements++;
    // protect with mutex
    mutexLock(currThreadContext->job->reduceOutputVecMutex);
    currThreadContext->job->reduceOutputVector.push_back(pair);
    mutexUnlock(currThreadContext->job->reduceOutputVecMutex);
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
    unsigned int oldVal = jc->mapCounter;
    while (oldVal < (unsigned int) jc->inputVector->size())
    {
        jc->mapCounter++;
        context->client->map((*jc->inputVector)[oldVal].first, (*jc->inputVector)[oldVal].second, context);

        mutexLock(jc->stateMutex);
        jc->state.percentage = ((float)(jc->mapCounter) / (float)(jc->inputVector+>size())) * 100 ;
        mutexUnlock(jc->stateMutex);

        oldVal = jc->mapCounter;
    }

}
void jobManager(void* context)
{
    ThreadContext* currThreadContext = (ThreadContext*) context;

    // change the state
    mutexLock(currThreadContext->job->stateMutex);
    currThreadContext->job->state.stage = MAP_STAGE;
    mutexUnlock(currThreadContext->job->stateMutex);

    mapPhase(currThreadContext);
    // manage all the other phases
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
    jc->mapCounter = 0;
    jc->reduceCounter = 0;

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
    for (int i = 0; i < multiThreadLevel; ++i)
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
        jc->threadContexts[i].job = jc;
        jc->threadContexts[i].client = &client;
    }
    return (JobHandle)jc;
}
/**
 * The function gets JobHandle returned by startMapReduceFramework and waits
 * until it is finished.
 * @param job the job we want to waiting for
 */
void waitForJob(JobHandle job)
{
    jobContext* jc = (jobContext*) job;
    for (int i = 0; i < jc->MULTI_THREAD_NUM; ++i) {
        if (pthread_join(jc->threadContexts[i].threadId, nullptr) != 0)
        {
            std::cerr << SYS_ERROR << THREAD_JOIN_FAIL << std::endl;
            exit(FAILURE);
        }
    }
}
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
/**
 * Releasing all resources of a job. You should prevent releasing resources before the job finished.
 * After this function is called the job handle will be invalid.
 * @param job releasing all resources of this job.
 */
void closeJobHandle(JobHandle job)
{
    waitForJob(job);

    jobContext* currJob = (jobContext*) job;

    if (pthread_mutex_destroy(&currJob->stateMutex) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_DESTROY_FAIL << std::endl;
        exit(FAILURE);
    }
    if (pthread_mutex_destroy(&currJob->reduceOutputVecMutex) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_DESTROY_FAIL << std::endl;
        exit(FAILURE);
    }
    if (pthread_mutex_destroy(&currJob->mapOutputVecMutex) != 0)
    {
        std::cerr << SYS_ERROR << MUTEX_DESTROY_FAIL << std::endl;
        exit(FAILURE);
    }
    delete [] currJob->threadContexts;
    delete currJob->mapOutputVector;
    delete currJob;

}

/**
 * The reducing threads will wait for the shuffled vectors to be created by the shuffling thread.
 * Once they wake up, they can pop a vector from the back of the queue and run reduce on it
 * The reduce function in turn will produce (k3, v3) pairs and will call emit3 to add them to the
 * framework data structures.
 * @param context context of a thread
 */
void reducePhase(ThreadContext* context) {
    jobContext* jc = context->job;
    unsigned int old_val = jc->reduceCounter;
    while (old_val < (unsigned int) jc->inputSortedandShuffledVec.size()) {
        jc->reduceCounter++;
        K2* key = (*jc->inputSortedandShuffledVec)[old_val].first;
        V2* val = (*jc->inputSortedandShuffledVec)[old_val].second;
        context->client->reduce(key, val, context);
        
        mutexLock(jc->stateMutex);
        jc->state.precentage = ((float)(jc->reduceCounter) / (float)(jc->inputSortedandShuffledVec->size())) * 100;
        mutexUnlock(jc->stateMutex);
        old_val = jc->reduceCounter;
    }
}


bool sortKeys(const IntermediatePair &left, onst IntermediatePair &right) {
    return (*left.first) < (*right.first)

void sortPhase(ThreadContext* context) {
    jobContext* jc = context->job;
    jc->mapCounter = 0;
    unsigned int old_val = jc->mapCounter;
    while (old_val < jc.mapOutputVector->size()) {
        jc->mapCounter++;
        std::sort((jc->mapOutoutVector)[old_val].begin(), jc->mapOutputVector[old_val].end(), sortKeys);
    }
}



    

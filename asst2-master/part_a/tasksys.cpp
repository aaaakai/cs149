#include "tasksys.h"
#include <stdio.h>
#include <iostream>
#include "thread"
using namespace std;


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads),
            num_thread(num_threads), task_id(0) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::thread tasks[num_thread];
    task_id = 0;
    /**
    for (int i = 1; i < num_total_tasks; i++) {
        tasks[i] = std::thread(&IRunnable::runTask, runnable, i, num_total_tasks);
    }
    **/

    for (int i = 0; i < num_thread; i++) {
        tasks[i] = std::thread([&](){
            while(true){
                if(task_id >= num_total_tasks){
                    return;
                }
                int current_tid = task_id++;
                runnable->runTask(current_tid, num_total_tasks);
            }
        });
    }

    for (int i = 0; i < num_thread; i++) {
        tasks[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads),
        numThread_(num_threads), curTaskId_(0), numTasks_(0), task_(nullptr), stop_(false),
        taskDone_(0) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    threadPool_.reserve(numThread_);
    for (int i = 0; i < numThread_; i++) {
        threadPool_.emplace_back(std::thread([&](){
            int taskToRun;
            int totalTask;
            while(!stop_){
                taskToRun = -1;
                {
                    std::lock_guard<std::mutex> lock(lock_);
                    if(curTaskId_ < numTasks_){
                        taskToRun = curTaskId_;
                        totalTask = numTasks_;
                        curTaskId_++;
                    }
                }
                if (taskToRun != -1) {
                    task_->runTask(taskToRun, totalTask);
                    taskDone_++;
                }
            }
        }));
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop_ = true;
    for (int i = 0; i < numThread_; i++) {
        threadPool_[i].join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    {
        std::lock_guard<std::mutex> lock(lock_);
        task_ = runnable;
        taskDone_ = 0;
        curTaskId_ = 0;
        numTasks_ = num_total_tasks;
    }

    while(true){
        if(taskDone_ == numTasks_){
            return;
        }
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads),
        numThread_(num_threads), curTaskId_(0), numTasks_(0), taskDone_(0), task_(nullptr), stop_(false) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    //cout << "biuld sleep" << endl;
    threadPool_.reserve(numThread_);
    for (int i = 0; i < numThread_; i++) {
        threadPool_.emplace_back(std::thread([&](){
            //cout << "a thread created" << endl;
            std::unique_lock<std::mutex> lock(Lock_);
            while(!stop_){
                //cout << "loop" << endl;
                while(curTaskId_ >= numTasks_){
                    allocSig_.wait(lock);
                    if(stop_){
                        return;
                    }
                }
                int execTask = curTaskId_;
                curTaskId_++;

                lock.unlock();

                task_->runTask(execTask, numTasks_);

                lock.lock();
                taskDone_++;
                if(taskDone_ == numTasks_){
                    finishSig_.notify_all();
                }
            }
            //cout << "threadFinished" << threadFinished_ <<endl;
            //cout << "this" << "thread" << this_thread::get_id() << "bye" << endl;
        }));
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    //cout << "start destruction" << endl;
    stop_ = true;
    Lock_.lock();
    allocSig_.notify_all();
    Lock_.unlock();
    /**
    while(threadPool_[i].joinable()){
        allocSig_.notify_all();
        cout << "notify" << endl;
        //threadPool_[i].join();
    }
    **/
    //cout << " notice once" << endl;
    //cout << "fifnish notice" << endl;
    for (int i = 0; i < numThread_; i++) {
        //cout << "wait t finish" << threadPool_[i].get_id() << endl;
        threadPool_[i].join();
        //cout << "one t finihs" << endl;
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    //cout << "start run" << endl;


    std::unique_lock<std::mutex> lock(Lock_);
    task_ = runnable;
    taskDone_ = 0;
    curTaskId_ = 0;
    numTasks_ = num_total_tasks;

    allocSig_.notify_all();
    

    finishSig_.wait(lock);
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}

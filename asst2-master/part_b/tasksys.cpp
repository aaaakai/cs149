#include "tasksys.h"
#include <iostream>
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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
        numThread_(num_threads), nextBulk_(0), nextBulkToRun_(0), stop_(false) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    //cout << "build a sleep" << endl;
    threadPool_.reserve(numThread_);
    for (int i = 0; i < numThread_; i++) {
        threadPool_.emplace_back(&TaskSystemParallelThreadPoolSleeping::taskExec, this);
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

    for (int i = 0; i < numThread_; i++) {
        threadPool_[i].join();
    }
}

int TaskSystemParallelThreadPoolSleeping::findBulkToRun_() {
    int bulkToRun = -1;
    if (runableBulks_.empty()) {
        return bulkToRun;
    }
    //cout << "runable" << runableBulks_.size() << endl;
    for (int i = 0; i < (int) runableBulks_.size(); i++) {
        int index = (nextBulkToRun_ + i) % runableBulks_.size();
        if(runableBulks_[index].curTaskId_ < runableBulks_[index].numTasks_) {
            bulkToRun = index;
            break;
        }
    }
    nextBulkToRun_ = (nextBulkToRun_ + 1) % runableBulks_.size();
    return bulkToRun;
}

void TaskSystemParallelThreadPoolSleeping::taskExec() {
    std::unique_lock<std::mutex> threadLock(Lock_);
    while(!stop_) {
        int nextBulk = findBulkToRun_();
        while(nextBulk == -1){
            //cout << "thread" << this_thread::get_id() << "sleep" << endl;
            allocSig_.wait(threadLock);
            if(stop_) {
                return;
            }
            //cout << "thread" << this_thread::get_id() << "wakeup" << endl;
            nextBulk = findBulkToRun_();
        }
        int taskId = runableBulks_[nextBulk].curTaskId_;
        IRunnable * taskToRun = runableBulks_[nextBulk].task_; 
        int taskTotal = runableBulks_[nextBulk].numTasks_;
        runableBulks_[nextBulk].curTaskId_++;
        int bulkIdentity = runableBulks_[nextBulk].bulkId;

        threadLock.unlock();

        taskToRun->runTask(taskId, taskTotal);

        threadLock.lock();

        int bulkIndex = -1;
        for (int i = 0; i < (int) runableBulks_.size(); i++){
            if (runableBulks_[i].bulkId == bulkIdentity) {
                bulkIndex = i;
                break;
            }
        }
        std::vector<TaskSystemParallelThreadPoolSleeping::bulkInfo_>::iterator bulk = 
            runableBulks_.begin() + bulkIndex;
        bulk->taskDone_++;
        if(bulk->taskDone_ == bulk->numTasks_){
            doneBulks_.emplace_back(bulk->bulkId);
            runableBulks_.erase(bulk);
            //cout << "finish a task with" << runableBulks_.size() << "left" << endl;
            if (runableBulks_.empty() && unrunBulks_.empty()){
                finishSig_.notify_all();
            }
            updateBulks_();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::updateBulks_() {
    int i = 0;
    while(i < (int) unrunBulks_.size()) {
        bool ready = true;
        for (auto reque : unrunBulks_[i].dependency_) {
            bool inFinish = false;
            for (auto hasdone : doneBulks_) {
                inFinish = inFinish || (reque == hasdone);
                if (inFinish) break;
            }
            ready = ready && inFinish;
            if (!ready) break;
        }
        if (ready) {
            runableBulks_.emplace_back(unrunBulks_[i]);
            unrunBulks_.erase(unrunBulks_.begin() + i);
            allocSig_.notify_all();
        } else {
            i++;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    //cout << "start run" <<endl;
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    //cout << "launch a bulk" << endl;
    std::unique_lock<std::mutex> runAsyncLock(Lock_);
    int bulkIndex = nextBulk_;
    nextBulk_++;
    struct bulkInfo_ newBulk = {};
    newBulk.task_ = runnable;
    newBulk.dependency_ = deps;
    newBulk.curTaskId_ = 0;
    newBulk.numTasks_ = num_total_tasks;
    newBulk.taskDone_ = 0;
    newBulk.bulkId = bulkIndex;
    unrunBulks_.emplace_back(newBulk);
    updateBulks_();


    return bulkIndex;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    //cout << "start sync" << endl;
    std::unique_lock<std::mutex> syncLock(Lock_);
    while((!runableBulks_.empty()) || (!unrunBulks_.empty())){
        //cout << "sync sleep" << "runable" << runableBulks_.size() <<
            // "unrun" << unrunBulks_.size() << endl;
        finishSig_.wait(syncLock);
    }
    //cout << "end sync" << endl;

    return;
}

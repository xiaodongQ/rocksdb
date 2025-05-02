//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/concurrent_task_limiter_impl.h"
#include "rocksdb/concurrent_task_limiter.h"

namespace ROCKSDB_NAMESPACE {

ConcurrentTaskLimiterImpl::ConcurrentTaskLimiterImpl(
    const std::string& name, int32_t max_outstanding_task)
    : name_(name),
      max_outstanding_tasks_{max_outstanding_task},
      outstanding_tasks_{0} {

}

ConcurrentTaskLimiterImpl::~ConcurrentTaskLimiterImpl() {
  assert(outstanding_tasks_ == 0);
}

const std::string& ConcurrentTaskLimiterImpl::GetName() const {
  return name_;
}

void ConcurrentTaskLimiterImpl::SetMaxOutstandingTask(int32_t limit) {
  max_outstanding_tasks_.store(limit, std::memory_order_relaxed);
}

void ConcurrentTaskLimiterImpl::ResetMaxOutstandingTask() {
  // 设置也用了宽松的内存序，因为max初始化好之后一般不会再设置（任务计数则是严格内存序）
  max_outstanding_tasks_.store(-1, std::memory_order_relaxed);
}

int32_t ConcurrentTaskLimiterImpl::GetOutstandingTask() const {
  return outstanding_tasks_.load(std::memory_order_relaxed);
}

std::unique_ptr<TaskLimiterToken> ConcurrentTaskLimiterImpl::GetToken(
    bool force) {
  // 最大限制任务数
  int32_t limit = max_outstanding_tasks_.load(std::memory_order_relaxed);
  // 当前任务数
  int32_t tasks = outstanding_tasks_.load(std::memory_order_relaxed);
  // force = true, bypass the throttle.
  // limit < 0 means unlimited tasks.
  while (force || limit < 0 || tasks < limit) {
    // 任务计数+1，默认memory_order_seq_cst
    if (outstanding_tasks_.compare_exchange_weak(tasks, tasks + 1)) {
      return std::unique_ptr<TaskLimiterToken>(new TaskLimiterToken(this));
    }
  }
  return nullptr;
}

ConcurrentTaskLimiter* NewConcurrentTaskLimiter(
    const std::string& name, int32_t limit) {
  return new ConcurrentTaskLimiterImpl(name, limit);
}

TaskLimiterToken::~TaskLimiterToken() {
  // 析构时自动-1，不同内存序保证？
  // C++标准，没有显式指定内存序则默认就是memory_order_seq_cst
    // operator--对于std::atomic类型默认使用memory_order_seq_cst
  --limiter_->outstanding_tasks_;
  assert(limiter_->outstanding_tasks_ >= 0);
}

}  // namespace ROCKSDB_NAMESPACE

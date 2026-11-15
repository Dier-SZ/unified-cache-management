/**
 * MIT License
 *
 * Copyright (c) 2026 Huawei Technologies Co., Ltd. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * */
#include "view/view_universe.h"
#include <algorithm>
#include <string>

namespace kv {
namespace {

Status ValidateUniverse(const std::unordered_map<KvNsId, GlobalView>& views,
                        std::uint64_t globalEpoch)
{
    if (views.empty()) {
        return Status::Error(StatusCode::INVALID_ARGUMENT,
                             "ViewUniverse must contain at least one namespace");
    }
    for (const auto& [nsId, view] : views) {
        if (view.asuMap.empty()) {
            return Status::Error(
                StatusCode::INVALID_ARGUMENT,
                "ViewUniverse namespace has no routable ASU, nsId=" + std::to_string(nsId));
        }
        if (view.viewEpoch > globalEpoch) {
            return Status::Error(
                StatusCode::INVALID_ARGUMENT,
                "namespace viewEpoch is greater than globalEpoch, nsId=" + std::to_string(nsId));
        }
    }
    return Status::OK();
}

}  // namespace

ViewUniverse::ViewUniverse(std::unordered_map<KvNsId, GlobalView> views, std::uint64_t globalEpoch)
    : views_(std::move(views)), globalEpoch_(globalEpoch)
{
}

Status ViewUniverse::PublishAll(const ViewUniverse& nextUniverse)
{
    if (this == &nextUniverse) { return Status::OK(); }

    std::unordered_map<KvNsId, GlobalView> nextViews;
    std::uint64_t nextGlobalEpoch = 0;
    {
        std::lock_guard sourceLock(nextUniverse.mutex_);
        nextViews = nextUniverse.views_;
        nextGlobalEpoch = nextUniverse.globalEpoch_;
    }

    auto status = ValidateUniverse(nextViews, nextGlobalEpoch);
    if (!status.ok()) { return status; }

    std::lock_guard lock(mutex_);
    if (!views_.empty()) {
        if (nextGlobalEpoch < globalEpoch_) {
            return Status::Error(StatusCode::INVALID_ARGUMENT,
                                 "ViewUniverse globalEpoch moved backwards, current=" +
                                     std::to_string(globalEpoch_) +
                                     ", next=" + std::to_string(nextGlobalEpoch));
        }
        if (nextGlobalEpoch == globalEpoch_) { return Status::OK(); }
    }

    views_ = std::move(nextViews);
    globalEpoch_ = nextGlobalEpoch;
    return Status::OK();
}

Status ViewUniverse::ReplaceAll(const ViewUniverse& nextUniverse)
{
    if (this == &nextUniverse) { return Status::OK(); }

    std::unordered_map<KvNsId, GlobalView> nextViews;
    std::uint64_t nextGlobalEpoch = 0;
    {
        std::lock_guard sourceLock(nextUniverse.mutex_);
        nextViews = nextUniverse.views_;
        nextGlobalEpoch = nextUniverse.globalEpoch_;
    }

    auto status = ValidateUniverse(nextViews, nextGlobalEpoch);
    if (!status.ok()) { return status; }

    std::lock_guard lock(mutex_);
    views_ = std::move(nextViews);
    globalEpoch_ = nextGlobalEpoch;
    return Status::OK();
}

Status ViewUniverse::GetView(KvNsId nsId, GlobalView& view) const
{
    std::lock_guard lock(mutex_);
    auto iter = views_.find(nsId);
    if (iter == views_.end()) {
        return Status::Error(StatusCode::NOT_FOUND,
                             "namespace not found, nsId=" + std::to_string(nsId));
    }
    view = iter->second;
    return Status::OK();
}

std::unordered_map<KvNsId, GlobalView> ViewUniverse::GetAllViews() const
{
    std::lock_guard lock(mutex_);
    return views_;
}

std::vector<KvNsId> ViewUniverse::GetAllNsIds() const
{
    std::lock_guard lock(mutex_);
    std::vector<KvNsId> nsIds;
    nsIds.reserve(views_.size());
    for (const auto& item : views_) { nsIds.emplace_back(item.first); }
    std::sort(nsIds.begin(), nsIds.end());
    return nsIds;
}

std::uint64_t ViewUniverse::GetGlobalEpoch() const
{
    std::lock_guard lock(mutex_);
    return globalEpoch_;
}

bool ViewUniverse::Empty() const
{
    std::lock_guard lock(mutex_);
    return views_.empty();
}

void ViewUniverse::Clear()
{
    std::lock_guard lock(mutex_);
    views_.clear();
    globalEpoch_ = 0;
}

}  // namespace kv

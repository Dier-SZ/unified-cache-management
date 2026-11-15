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
#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include "kv_types.h"

namespace kv {

// GlobalView carries the routing membership and view metadata.
struct GlobalView {
    std::uint64_t viewEpoch{0};
    std::uint64_t viewId{0};
    std::unordered_map<NodeId, NodeInfo> asuMap;
    std::uint64_t createTimeMs{0};
    std::uint64_t expireTimeMs{0};
};

// ViewUniverse owns the complete cluster view and exposes the update/query
// contract used by the refresh path and foreground view consumers.
class ViewUniverse {
public:
    ViewUniverse() = default;
    ViewUniverse(std::unordered_map<KvNsId, GlobalView> views, std::uint64_t globalEpoch);
    ~ViewUniverse() = default;

    ViewUniverse(const ViewUniverse&) = delete;
    ViewUniverse& operator=(const ViewUniverse&) = delete;
    ViewUniverse(ViewUniverse&&) = delete;
    ViewUniverse& operator=(ViewUniverse&&) = delete;

    // Atomically publishes a complete universe. A lower globalEpoch is
    // rejected and the current universe is retained; an equal epoch is a no-op.
    Status PublishAll(const ViewUniverse& nextUniverse);
    // Replaces the contents without the monotonic-globalEpoch check. This is
    // used for a freshly fetched ViewServer result and initial construction.
    Status ReplaceAll(const ViewUniverse& nextUniverse);

    Status GetView(KvNsId nsId, GlobalView& view) const;
    std::unordered_map<KvNsId, GlobalView> GetAllViews() const;
    std::vector<KvNsId> GetAllNsIds() const;
    std::uint64_t GetGlobalEpoch() const;
    bool Empty() const;
    void Clear();

private:
    mutable std::mutex mutex_;
    std::unordered_map<KvNsId, GlobalView> views_;
    std::uint64_t globalEpoch_{0};
};

}  // namespace kv

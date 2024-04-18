//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class SimpleHashJoinTable {
   public:
    explicit SimpleHashJoinTable(
        const std::vector<AbstractExpressionRef>& key_exprs)
        : key_exprs_{key_exprs} {}

    /**
     * Clear the hash table
     */
    void Clear() { ht_.clear(); }

    /** An iterator over the aggregation hash table */
    class Iterator {
       public:
        /** Creates an iterator for the aggregate map. */
        explicit Iterator(
            std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter)
            : iter_{iter} {}

        /** @return The key of the iterator */
        auto Key() -> const HashJoinKey& { return iter_->first; }

        /** @return The value of the iterator */
        auto Val() -> const HashJoinValue& { return iter_->second; }

        /** @return The iterator before it is incremented */
        auto operator++() -> Iterator& {
            ++iter_;
            return *this;
        }

        /** @return `true` if both iterators are identical */
        auto operator==(const Iterator& other) -> bool {
            return this->iter_ == other.iter_;
        }

        /** @return `true` if both iterators are different */
        auto operator!=(const Iterator& other) -> bool {
            return this->iter_ != other.iter_;
        }

       private:
        /** Aggregates map */
        std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter_;
    };

    /** @return Iterator to the start of the hash table */
    auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

    /** @return Iterator to the end of the hash table */
    auto End() -> Iterator { return Iterator{ht_.cend()}; }

    auto Exists(const HashJoinKey& key) -> bool { return ht_.count(key) > 0; }

    auto Get(const HashJoinKey& key) -> const HashJoinValue& {
        return ht_[key];
    }

    void InsertTuple(const Tuple& tuple, const Schema& schema);

   private:
    /** The hash table is just a map from aggregate keys to aggregate values */
    std::unordered_map<HashJoinKey, HashJoinValue> ht_{};
    /** The aggregate expressions that we have */
    const std::vector<AbstractExpressionRef>& key_exprs_;
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
   public:
    /**
     * Construct a new HashJoinExecutor instance.
     * @param exec_ctx The executor context
     * @param plan The HashJoin join plan to be executed
     * @param left_child The child executor that produces tuples for the left
     * side of join
     * @param right_child The child executor that produces tuples for the right
     * side of join
     */
    HashJoinExecutor(ExecutorContext* exec_ctx,
                     const HashJoinPlanNode* plan,
                     std::unique_ptr<AbstractExecutor>&& left_child,
                     std::unique_ptr<AbstractExecutor>&& right_child);

    /** Initialize the join */
    void Init() override;

    /**
     * Yield the next tuple from the join.
     * @param[out] tuple The next tuple produced by the join.
     * @param[out] rid The next tuple RID, not used by hash join.
     * @return `true` if a tuple was produced, `false` if there are no more
     * tuples.
     */
    auto Next(Tuple* tuple, RID* rid) -> bool override;

    auto LeftJoinTuple(const Tuple* left) -> Tuple;

    auto InnerJoinTuple(const Tuple* left, const Tuple* right) -> Tuple;

    /** @return The output schema for the join */
    auto GetOutputSchema() const -> const Schema& override {
        return plan_->OutputSchema();
    };

   private:
    /** The HashJoin plan node to be executed. */
    const HashJoinPlanNode* plan_;
    std::unique_ptr<AbstractExecutor> left_executor_;
    std::unique_ptr<AbstractExecutor> right_executor_;

    SimpleHashJoinTable left_hjt_;
    SimpleHashJoinTable right_hjt_;
    SimpleHashJoinTable::Iterator left_iter_;

    size_t left_tuple_idx_{};
    size_t right_tuple_idx_{};

    const std::vector<Tuple>* right_tuples_{};
};

}  // namespace bustub

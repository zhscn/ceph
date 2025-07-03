// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastar/core/metrics.hh"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/logical_bucket.h"
#include "crimson/os/seastore/transaction_manager.h"

#include <boost/unordered/unordered_flat_map.hpp>

namespace crimson::os::seastore {

SET_SUBSYS(seastore_cache);

class LogicalBucketCache : public LogicalBucket {
public:
  LogicalBucketCache(std::size_t memory_capacity,
                     std::size_t demote_size_per_cycle)
    : memory_capacity(memory_capacity),
      demote_size_per_cycle(demote_size_per_cycle) {
    LOG_PREFIX(LogicalBucketCache);
    INFO("init memory_capacity={}, demote_size_per_cycle={}",
	 memory_capacity, demote_size_per_cycle);
    register_metrics();
  }

  ~LogicalBucketCache() {
    clear();
  }

  void move_to_top(
      laddr_t laddr,
      bool create_if_absent) final {
    LOG_PREFIX(LogicalBucketCache::move_to_top);
    DEBUG("laddr: {}", laddr);
    assert(laddr != L_ADDR_NULL);
    assert(laddr == laddr.get_object_prefix());
    auto iter = index.find(laddr);
    if (iter != index.end()) {
      DEBUG("find bucket: {}", iter->first);
      lru.splice(lru.end(), lru, iter->second);
    } else if (create_if_absent) {
      DEBUG("create bucket: {}", laddr);
      index[laddr] = lru.emplace(lru.end(), laddr);
    } else {
      TRACE("prefix {} doesn't exist, skipping", laddr);
    }
  }

  void remove(laddr_t laddr) final {
    LOG_PREFIX(LogicalBucketCache::remove);
    TRACE("laddr: {}", laddr);
    assert(laddr != L_ADDR_NULL);
    assert(laddr == laddr.get_object_prefix());
    auto iter = index.find(laddr);
    if (iter != index.end()) {
      TRACE("find bucket: {}", laddr);
      lru.erase(iter->second);
      index.erase(iter);
    }
  }

  bool is_cached(laddr_t laddr) final {
    assert(laddr != L_ADDR_NULL);
    assert(laddr == laddr.get_object_prefix());
    return index.contains(laddr);
  }

  void clear() final {
    index.clear();
    lru.clear();
  }

  void set_background_callback(BackgroundListener *l) final {
    listener = l;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) final {
    ecb = cb;
  }

  bool could_demote() const final {
    return !lru.empty();
  }

  bool should_demote() const {
    return (sizeof(laddr_t) * 2 + sizeof(void*) * 3) > memory_capacity;
  }

  seastar::future<> demote() final {
    return repeat_eagain([this] {
      init_state();
      return ecb->with_transaction_intr(
        Transaction::src_t::DEMOTE,
        "demote", cache_hint_t::get_nocache(),
        [this](auto &t) {
          return trans_intr::repeat([this, &t] {
            LOG_PREFIX(LogicalBucketCache::demote);
            auto &bucket = *s.cold_iter;
            DEBUG("start demote {}", bucket);
            assert(demote_size_per_cycle > s.demoted_size);
            return ecb->demote_region(
              t,
              bucket,
              demote_size_per_cycle - s.demoted_size
            ).si_then([this, FNAME](auto &&res) {
              TRACE("demote_size: {}, compelted: {}",
                    res.demote_size, res.completed);
              s.demoted_size += res.demote_size;
              auto &bucket = *s.cold_iter;
              if (res.completed) {
                s.completed_buckets.push_back(bucket);
                s.cold_iter++;
              }
              if (s.cold_iter == s.cold_buckets.end() ||
                  s.demoted_size >= demote_size_per_cycle) {
                return seastar::stop_iteration::yes;
              } else {
                return seastar::stop_iteration::no;
              }
            });
          }).si_then([this, &t] {
            return ecb->submit_transaction_direct(t);
          }).si_then([this] {
            LOG_PREFIX(LogicalBucketCache::demote);
            DEBUG("demote {} bytes in the hot tier", s.demoted_size);
            stat.demoted_bucket_count += s.completed_buckets.size();
            stat.demoted_size += s.demoted_size;
            for (auto &p : s.completed_buckets) {
              remove(p);
            }
            auto old_count = s.init_buckets_count;
            s.update_init_buckets_count(
              demote_size_per_cycle,
              stat.demoted_size,
              stat.demoted_bucket_count);
            TRACE("update init buckets count {} -> {}",
                  old_count, s.init_buckets_count);
            return ExtentCallbackInterface::demote_region_iertr::
                make_ready_future();
          });
        });
    }).handle_error(crimson::ct_error::assert_all{ "impossible" });
  }

private:
  using laddr_lru_t = std::list<laddr_t>;
  laddr_lru_t lru;
  boost::unordered_flat_map<laddr_t, laddr_lru_t::iterator> index;

  struct demote_state_t {
    std::list<laddr_t> cold_buckets;
    std::list<laddr_t> completed_buckets;
    std::list<laddr_t>::iterator cold_iter;

    std::size_t demoted_size;

    int init_buckets_count = 20;

    void reset() {
      cold_buckets.clear();
      completed_buckets.clear();
      cold_iter = cold_buckets.end();
      demoted_size = 0;
    }

    void update_init_buckets_count(
      extent_len_t demote_size_per_cycle,
      double demote_size,
      double demoted_buckets_count) {
      if (demote_size != 0 && demoted_buckets_count != 0) {
	auto demote_ratio = (double)demote_size /
	  (double)demoted_buckets_count;
	assert(!std::isnan(demote_ratio));
	init_buckets_count = (demote_size_per_cycle / demote_ratio) + 1;
      }
    }
  };

  void init_state() {
    s.reset();
    int count = 0;
    assert(s.init_buckets_count > 0);
    for (auto &b : lru) {
      if (count > s.init_buckets_count) {
	break;
      }
      s.cold_buckets.push_back(b);
      count++;
    }
    s.cold_iter = s.cold_buckets.begin();
    ceph_assert(s.cold_iter != s.cold_buckets.end());
  }

  void register_metrics() {
    namespace sm = seastar::metrics;
    metrics.add_group(
      "cache",
      {
	sm::make_gauge(
	  "non_volatile_cache_buckets_count",
	  [this] { return lru.size(); },
	  sm::description("the count of laddr bucket used by non volatile cache")),
	sm::make_counter(
	  "non_volatile_cache_demoted_size",
	  [this] { return stat.demoted_size; },
	  sm::description("total bytes of extents demoted by non volatile cache")),
	sm::make_counter(
	  "non_volatile_cache_demoted_bucket_count",
	  [this] { return stat.demoted_bucket_count; },
	  sm::description("the count of laddr bucket demoted by non volatile cache")),
      });
  }

  demote_state_t s;

  struct {
    uint64_t demoted_size = 0;
    uint64_t demoted_bucket_count = 0;
  } stat;

  seastar::metrics::metric_group metrics;

  const std::size_t memory_capacity;
  const std::size_t demote_size_per_cycle;

  ExtentCallbackInterface *ecb;
  BackgroundListener *listener;
};

LogicalBucketRef create_logical_bucket(
  std::size_t memory_capacity,
  std::size_t demote_size_per_cycle)
{
  return std::make_unique<LogicalBucketCache>(
    memory_capacity, demote_size_per_cycle);
}

}

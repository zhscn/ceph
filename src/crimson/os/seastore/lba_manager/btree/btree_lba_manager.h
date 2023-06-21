// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "include/interval_set.h"
#include "common/interval_map.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/btree/fixed_kv_btree.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/cache.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/btree/btree_range_pin.h"

namespace crimson::os::seastore::lba_manager::btree {

class BtreeLBAMapping : public BtreeNodeMapping<laddr_t, paddr_t> {
// To support cloning, there are two kinds of lba mappings:
// 	1. physical lba mapping: the pladdr in the value of which is the paddr of
// 	   the corresponding extent;
// 	2. indirect lba mapping: the pladdr in the value of which is an laddr pointing
// 	   to the physical lba mapping that's pointing to the actual paddr of the
// 	   extent being searched;
//
// Accordingly, BtreeLBAMapping may also work under two modes: indirect or direct
// 	1. BtreeLBAMappings that come from quering an indirect lba mapping in the lba tree
// 	   are indirect;
// 	2. BtreeLBAMappings that come from quering a physical lba mapping in the lba tree
// 	   are direct.
//
// For direct BtreeLBAMappings, there are two important fields:
//      1. key: the laddr of the lba mapping being queried;
//      2. paddr: the paddr recorded in the value of the lba mapping being queried.
// For indirect BtreeLBAMappings, BtreeLBAMapping has three important fields:
// 	1. key: the laddr key of the lba entry being queried;
// 	2. intermediate_key: the laddr key of the physical lba mapping that
// 	   the current indirect lba mapping points to;
// 	3. paddr: the paddr recorded in the physical lba mapping pointed to by the
// 	   indirect lba mapping being queried;
//
// NOTE THAT, for direct BtreeLBAMappings, their intermediate_keys are the same as
// their keys.
public:
  BtreeLBAMapping(op_context_t<laddr_t> ctx)
    : BtreeNodeMapping(ctx) {}
  BtreeLBAMapping(
    op_context_t<laddr_t> c,
    CachedExtentRef parent,
    uint16_t pos,
    lba_map_val_t &val,
    lba_node_meta_t meta)
    : BtreeNodeMapping(
	c,
	parent,
	pos,
	val.pladdr,
	val.len,
	meta),
      key(meta.begin),
      indirect(val.pladdr.is_laddr() ? true : false),
      intermediate_key(val.pladdr.is_laddr() ? val.pladdr.get_laddr() : meta.begin),
      map_val(val)
  {}

  lba_map_val_t get_map_val() const {
    return map_val;
  }

  bool is_indirect() const final {
    return indirect;
  }

  void set_key_for_indirect(laddr_t laddr) {
    turn_indirect();
    key = laddr;
  }

  laddr_t get_key() const final {
    return key;
  }

  pladdr_t get_raw_val() const {
    return value;
  }

  void set_paddr(paddr_t addr) {
    value = addr;
  }

  laddr_t get_intermediate_key() const {
    assert(is_indirect());
    return intermediate_key;
  }

protected:
  std::unique_ptr<BtreeNodeMapping<laddr_t, paddr_t>> _duplicate(
    op_context_t<laddr_t> ctx) const final {
    auto pin = std::unique_ptr<BtreeLBAMapping>(new BtreeLBAMapping(ctx));
    pin->key = key;
    pin->indirect = indirect;
    pin->intermediate_key = intermediate_key;
    pin->map_val = map_val;
    return pin;
  }
private:
  void turn_indirect() {
    intermediate_key = key;
    indirect = true;
  }
  laddr_t key = L_ADDR_NULL;
  bool indirect = false;
  laddr_t intermediate_key = L_ADDR_NULL;
  lba_map_val_t map_val;
};

using BtreeLBAMappingRef = std::unique_ptr<BtreeLBAMapping>;

using LBABtree = FixedKVBtree<
  laddr_t, lba_map_val_t, LBAInternalNode,
  LBALeafNode, BtreeLBAMapping, LBA_BLOCK_SIZE, true>;

/**
 * BtreeLBAManager
 *
 * Uses a wandering btree to track two things:
 * 1) lba state including laddr_t -> paddr_t mapping
 * 2) reverse paddr_t -> laddr_t mapping for gc (TODO)
 *
 * Generally, any transaction will involve
 * 1) deltas against lba tree nodes
 * 2) new lba tree nodes
 *    - Note, there must necessarily be a delta linking
 *      these new nodes into the tree -- might be a
 *      bootstrap_state_t delta if new root
 *
 * get_mappings, alloc_extent_*, etc populate a Transaction
 * which then gets submitted
 */
class BtreeLBAManager : public LBAManager {
public:
  BtreeLBAManager(Cache &cache)
    : cache(cache)
  {
    register_metrics();
  }

  mkfs_ret mkfs(
    Transaction &t) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_t offset, extent_len_t length) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_list_t &&list) final;

  get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset) final;

  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    pladdr_t addr,
    paddr_t actual_addr,
    LogicalCachedExtent*) final;

  ref_ret decref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, -1);
  }

  ref_ret incref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, 1);
  }

  /**
   * init_cached_extent
   *
   * Checks whether e is live (reachable from lba tree) and drops or initializes
   * accordingly.
   *
   * Returns if e is live.
   */
  init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) final;

  check_child_trackers_ret check_child_trackers(Transaction &t) final;

  scan_mappings_ret scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) final;

  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final;

  update_mapping_ret update_mapping(
    Transaction& t,
    laddr_t laddr,
    paddr_t prev_addr,
    paddr_t paddr,
    LogicalCachedExtent*) final;

  split_mapping_ret split_mapping(
    Transaction &t,
    laddr_t laddr,
    extent_len_t left_len,
    extent_len_t right_len,
    LogicalCachedExtent *lnextent,
    LogicalCachedExtent *rnextent) final;

  get_physical_extent_if_live_ret get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    extent_len_t len) final;
private:
  Cache &cache;


  struct {
    uint64_t num_alloc_extents = 0;
    uint64_t num_alloc_extents_iter_nexts = 0;
  } stats;

  op_context_t<laddr_t> get_context(Transaction &t) {
    return op_context_t<laddr_t>{cache, t};
  }

  seastar::metrics::metric_group metrics;
  void register_metrics();

  /**
   * update_refcount
   *
   * Updates refcount, returns resulting refcount
   */
  using update_refcount_ret = ref_ret;
  update_refcount_ret update_refcount(
    Transaction &t,
    laddr_t addr,
    int delta);

  /**
   * _update_mapping
   *
   * Updates mapping, removes if f returns nullopt
   */
  using _update_mapping_ret_bare =
    std::variant<lba_map_val_t, BtreeLBAMappingRef>;
  using _update_mapping_iertr = ref_iertr;
  using _update_mapping_ret = ref_iertr::future<_update_mapping_ret_bare>;
  using update_func_t = std::function<
    lba_map_val_t(const lba_map_val_t &v)
    >;
  _update_mapping_ret _update_mapping(
    Transaction &t,
    laddr_t addr,
    update_func_t &&f,
    LogicalCachedExtent*);

  using _get_mapping_ret = get_mapping_iertr::future<BtreeLBAMappingRef>;
  _get_mapping_ret _get_mapping(
    Transaction &t,
    laddr_t offset);

  using _get_original_mappings_ret = get_mappings_ret;
  _get_original_mappings_ret _get_original_mappings(
    op_context_t<laddr_t> c,
    std::list<BtreeLBAMappingRef> &pin_list,
    laddr_t offset,
    extent_len_t length);

  alloc_extent_iertr::future<BtreeLBAMappingRef>
  _alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    pladdr_t addr,
    paddr_t actual_addr,
    uint32_t refcnt,
    LogicalCachedExtent*);

  ref_iertr::future<std::list<std::pair<paddr_t, extent_len_t>>>
  _decref_intermediate(
    Transaction &t,
    laddr_t addr,
    extent_len_t len);
};
using BtreeLBAManagerRef = std::unique_ptr<BtreeLBAManager>;

}

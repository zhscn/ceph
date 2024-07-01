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

struct LBALeafNode;

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
// 	2. intermediate_key: the laddr within the scope of the physical lba mapping
// 	   that the current indirect lba mapping points to; although an indirect mapping
// 	   points to the start of the physical lba mapping, it may change to other
// 	   laddr after remap
// 	3. intermediate_base: the laddr key of the physical lba mapping, intermediate_key
// 	   and intermediate_base should be the same when doing cloning
// 	4. intermediate_offset: intermediate_key - intermediate_base
// 	5. intermediate_length: the length of the actual physical lba mapping
// 	6. paddr: the paddr recorded in the physical lba mapping pointed to by the
// 	   indirect lba mapping being queried;
//
// NOTE THAT, for direct BtreeLBAMappings, their intermediate_keys are the same as
// their keys.
//
// Indirect BtreeLBAMapping can be further categrozied into HALF_INDIRECT and
// FULL_INDIRECT.
// 1. HALF_INDIRECT mappings: indirect mappings that don't have information from
//    the intermediate mapping. Specifically, they don't have valid intermediate_base,
//    intermediate_offset or intermediate_length.
// 2. FULL_INDIRECT mappings: ones that have all information about the intermediate
//    mapping

enum indirect_state_t : uint8_t {
  DIRECT,
  HALF_INDIRECT,
  FULL_INDIRECT,
  MAX
};
public:
  BtreeLBAMapping(op_context_t<laddr_t> ctx)
    : BtreeNodeMapping(ctx) {}
  BtreeLBAMapping(
    op_context_t<laddr_t> c,
    LBALeafNodeRef parent,
    uint16_t pos,
    lba_map_val_t &val,
    lba_node_meta_t meta)
    : BtreeNodeMapping(
	c,
	parent,
	pos,
	val.pladdr.is_paddr() ? val.pladdr.get_paddr() : P_ADDR_NULL,
	val.len,
	meta),
      key(meta.begin),
      indirect(val.pladdr.is_laddr() ? indirect_state_t::HALF_INDIRECT
				     : indirect_state_t::DIRECT),
      intermediate_key(indirect > indirect_state_t::DIRECT
	? val.pladdr.build_laddr(key) : L_ADDR_NULL),
      raw_val(val.pladdr),
      map_val(val),
      parent_modifications(parent->modifications)
  {}

  lba_map_val_t get_map_val() const {
    return map_val;
  }

  bool is_indirect() const final {
    return indirect >= indirect_state_t::HALF_INDIRECT;
  }

  bool is_half_indirect() const final {
    return indirect == indirect_state_t::HALF_INDIRECT;
  }

  bool is_full_indirect() const final {
    return indirect == indirect_state_t::FULL_INDIRECT;
  }

  void make_indirect(
    laddr_t new_key,
    extent_len_t length,
    laddr_t interkey = L_ADDR_NULL)
  {
    assert(indirect == indirect_state_t::DIRECT);
    indirect = indirect_state_t::FULL_INDIRECT;
    intermediate_base = key;
    intermediate_length = len;
    adjust_mutable_indirect_attrs(new_key, length, interkey);
  }

  laddr_t get_key() const final {
    return key;
  }

  pladdr_t get_raw_val() const {
    return raw_val;
  }

  laddr_t get_intermediate_key() const final {
    assert(is_indirect());
    assert(intermediate_key != L_ADDR_NULL);
    return intermediate_key;
  }

  laddr_t get_intermediate_base() const final {
    assert(is_full_indirect());
    assert(intermediate_base != L_ADDR_NULL);
    return intermediate_base;
  }

  extent_len_t get_intermediate_offset() const final {
    assert(intermediate_key >= intermediate_base);
    assert((intermediate_key == L_ADDR_NULL)
      == (intermediate_base == L_ADDR_NULL));
    return intermediate_key - intermediate_base;
  }

  extent_len_t get_intermediate_length() const final {
    assert(is_full_indirect());
    assert(intermediate_length);
    return intermediate_length;
  }

  bool is_clone() const final {
    return get_map_val().refcount > 1;
  }

  uint32_t get_checksum() const final {
    return get_map_val().checksum;
  }

  void adjust_mutable_indirect_attrs(
    laddr_t new_key,
    extent_len_t length,
    laddr_t interkey = L_ADDR_NULL)
  {
    assert(is_full_indirect());
    assert(value_is_paddr());
    intermediate_key = (interkey == L_ADDR_NULL ? key : interkey);
    key = new_key;
    len = length;
  }

  uint64_t get_parent_modifications() const {
    return parent_modifications;
  }

  bool parent_modified() const final {
    ceph_assert(parent);
    ceph_assert(is_parent_valid());
    auto &p = static_cast<LBALeafNode&>(*parent);
    return p.modified_since(parent_modifications);
  }

  void maybe_fix_pos() final {
    assert(is_parent_valid());
    if (!parent_modified()) {
      return;
    }
    auto &p = static_cast<LBALeafNode&>(*parent);
    p.maybe_fix_mapping_pos(*this);
  }

  bool has_shadow_mapping() const final {
    return !is_indirect() && map_val.shadow_paddr != P_ADDR_NULL;
  }

  paddr_t get_shadow_val() const final {
    return map_val.shadow_paddr;
  }
protected:
  std::unique_ptr<BtreeNodeMapping<laddr_t, paddr_t>> _duplicate(
    op_context_t<laddr_t> ctx) const final {
    auto pin = std::unique_ptr<BtreeLBAMapping>(new BtreeLBAMapping(ctx));
    pin->key = key;
    pin->intermediate_base = intermediate_base;
    pin->intermediate_key = intermediate_key;
    pin->intermediate_length = intermediate_length;
    pin->indirect = indirect;
    pin->raw_val = raw_val;
    pin->map_val = map_val;
    pin->parent_modifications = parent_modifications;
    return pin;
  }
private:
  void _new_pos(uint16_t pos) {
    this->pos = pos;
  }

  laddr_t key = L_ADDR_NULL;
  indirect_state_t indirect = indirect_state_t::DIRECT;
  laddr_t intermediate_key = L_ADDR_NULL;
  laddr_t intermediate_base = L_ADDR_NULL;
  extent_len_t intermediate_length = 0;
  pladdr_t raw_val;
  lba_map_val_t map_val;
  uint64_t parent_modifications = 0;
  friend struct LBALeafNode;
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


  struct alloc_mapping_info_t {
    laddr_t key = L_ADDR_NULL; // once assigned, the allocation to
			       // key must be exact and successful
    lba_map_val_t value;
    LogicalCachedExtent* extent = nullptr;

    static alloc_mapping_info_t create_zero(extent_len_t len) {
      return {
	L_ADDR_NULL,
	lba_map_val_t{
	  len,
	  pladdr_t(P_ADDR_ZERO),
	  EXTENT_DEFAULT_REF_COUNT,
	  0,
	  P_ADDR_NULL
	},
	nullptr
      };
    }
    static alloc_mapping_info_t create_indirect(
      laddr_t laddr,
      extent_len_t len,
      laddr_t intermediate_key) {
      return {
	laddr,
	lba_map_val_t{
	  len,
	  pladdr_t(intermediate_key),
	  EXTENT_DEFAULT_REF_COUNT,
	  0,    // crc will only be used and checked with LBA direct mappings
		// also see pin_to_extent(_by_type)
	  P_ADDR_NULL
	},
	nullptr
      };
    }
    static alloc_mapping_info_t create_direct(
      laddr_t laddr,
      extent_len_t len,
      paddr_t paddr,
      extent_ref_count_t refcount,
      uint32_t checksum,
      LogicalCachedExtent *extent,
      paddr_t shadow_paddr) {
      return {
	laddr,
	lba_map_val_t{
	  len,
	  pladdr_t(paddr),
	  refcount,
	  checksum,
	  shadow_paddr
	},
	extent
      };
    }
  };

  alloc_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    bool determinsitic) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_zero(len)};
    return seastar::do_with(
      std::move(alloc_infos),
      [&t, hint, this, determinsitic](auto &alloc_infos) {
      return _alloc_extents(
	t,
	hint,
	alloc_infos,
	determinsitic
      ).si_then([](auto mappings) {
	assert(mappings.size() == 1);
	auto mapping = std::move(mappings.front());
	return mapping;
      });
    });
  }

  alloc_extent_ret clone_mapping(
    Transaction &t,
    laddr_t laddr,
    extent_len_t len,
    laddr_t intermediate_key) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_indirect(
	laddr, len, intermediate_key)};
    return alloc_cloned_mappings(
      t,
      laddr,
      std::move(alloc_infos)
    ).si_then([&t, this, intermediate_key](auto imappings) {
      assert(imappings.size() == 1);
      auto &imapping = imappings.front();
      return update_refcount(t, intermediate_key, 1, false, true
      ).si_then([imapping=std::move(imapping)](auto p) mutable {
	auto mapping = std::move(p.mapping);
	ceph_assert(mapping->is_data_stable());
	ceph_assert(imapping->is_half_indirect());
	mapping->make_indirect(
	  imapping->get_key(),
	  imapping->get_length(),
	  imapping->get_intermediate_key());
	return seastar::make_ready_future<
	  LBAMappingRef>(std::move(mapping));
      });
    }).handle_error_interruptible(
      crimson::ct_error::input_output_error::pass_further{},
      crimson::ct_error::assert_all{"unexpect enoent"}
    );
  }

  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    LogicalCachedExtent &ext,
    extent_ref_count_t refcount,
    bool determinsitic) final
  {
    // The real checksum will be updated upon transaction commit
    assert(ext.get_last_committed_crc() == 0);
    assert(!ext.has_laddr());
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_direct(
	L_ADDR_NULL,
	ext.get_length(),
	ext.get_paddr(),
	refcount,
	ext.get_last_committed_crc(),
	&ext,
	P_ADDR_NULL)};
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, hint, determinsitic](auto &alloc_infos) {
      return _alloc_extents(
	t,
	hint,
	alloc_infos,
	determinsitic
      ).si_then([](auto mappings) {
	assert(mappings.size() == 1);
	auto mapping = std::move(mappings.front());
	return mapping;
      });
    });
  }

  alloc_extents_ret alloc_extents(
    Transaction &t,
    laddr_t hint,
    std::vector<LogicalCachedExtentRef> extents,
    extent_ref_count_t refcount,
    bool determinsitic) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos;
    for (auto &extent : extents) {
      alloc_infos.emplace_back(
	alloc_mapping_info_t::create_direct(
	  extent->has_laddr() ? extent->get_laddr() : L_ADDR_NULL,
	  extent->get_length(),
	  extent->get_paddr(),
	  refcount,
	  extent->get_last_committed_crc(),
	  extent.get(),
	  P_ADDR_NULL));
    }
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, hint, determinsitic](auto &alloc_infos) {
      return _alloc_extents(t, hint, alloc_infos, determinsitic);
    });
  }

  move_mappings_ret move_mappings(
    Transaction &t,
    laddr_t src_base,
    laddr_t dst_base,
    extent_len_t length,
    bool direct_mapping_only,
    remap_extent_func_t func) final;

  merge_mappings_ret merge_mappings(
    Transaction &t,
    laddr_t src_base,
    laddr_t dst_base,
    extent_len_t length,
    remap_extent_func_t func) final;

  ref_ret decref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, -1, true, false
    ).si_then([](auto res) {
      return std::move(res.ref_update_res);
    });
  }

  ref_ret incref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, 1, false, false
    ).si_then([](auto res) {
      return std::move(res.ref_update_res);
    });
  }

  remap_ret remap_mappings(
    Transaction &t,
    LBAMappingRef orig_mapping,
    std::vector<remap_entry> remaps,
    std::vector<LogicalCachedExtentRef> extents) final {
    LOG_PREFIX(BtreeLBAManager::remap_mappings);
    assert((orig_mapping->is_indirect())
      == (remaps.size() != extents.size()));
    assert(!orig_mapping->is_half_indirect());
    return seastar::do_with(
      lba_remap_ret_t{},
      std::move(remaps),
      std::move(extents),
      std::move(orig_mapping),
      [&t, FNAME, this](auto &ret, const auto &remaps,
			auto &extents, auto &orig_mapping) {
      return update_refcount(t, orig_mapping->get_key(), -1, false, false
      ).si_then([&ret, this, &extents, &remaps,
		&t, &orig_mapping, FNAME](auto r) {
	ret.ruret = std::move(r.ref_update_res);
	if (!orig_mapping->is_full_indirect()) {
	  ceph_assert(ret.ruret.refcount == 0 &&
	    ret.ruret.addr.is_paddr() &&
	    !ret.ruret.addr.get_paddr().is_zero());
	}
	auto fut = alloc_extent_iertr::make_ready_future<
	  std::vector<LBAMappingRef>>();
	laddr_t orig_laddr = orig_mapping->get_key();
	if (orig_mapping->is_full_indirect()) {
	  std::vector<alloc_mapping_info_t> alloc_infos;
	  for (auto &remap : remaps) {
	    extent_len_t orig_len = orig_mapping->get_length();
	    paddr_t orig_paddr = orig_mapping->get_val();
	    laddr_t intermediate_base = orig_mapping->is_full_indirect()
	      ? orig_mapping->get_intermediate_base()
	      : L_ADDR_NULL;
	    laddr_t intermediate_key = orig_mapping->is_full_indirect()
	      ? orig_mapping->get_intermediate_key()
	      : L_ADDR_NULL;
	    auto remap_offset = remap.offset;
	    auto remap_len = remap.len;
	    auto remap_laddr = remap.dst_laddr;
	    ceph_assert(intermediate_base != L_ADDR_NULL);
	    ceph_assert(intermediate_key != L_ADDR_NULL);
	    ceph_assert(remap_len < orig_len);
	    ceph_assert(remap_offset + remap_len <= orig_len);
	    ceph_assert(remap_len != 0);
	    SUBDEBUGT(seastore_lba,
	      "remap laddr: {}, remap paddr: {}, remap length: {},"
	      " intermediate_base: {}, intermediate_key: {}", t,
	      remap_laddr, orig_paddr, remap_len,
	      intermediate_base, intermediate_key);
	    auto remapped_intermediate_key = intermediate_key + remap_offset;
	    alloc_infos.emplace_back(
	      alloc_mapping_info_t::create_indirect(
		remap_laddr,
		remap_len,
		remapped_intermediate_key));
	  }
	  fut = alloc_cloned_mappings(
	    t,
	    remaps.front().offset + orig_laddr,
	    std::move(alloc_infos)
	  ).si_then([&orig_mapping](auto imappings) mutable {
	    std::vector<LBAMappingRef> mappings;
	    for (auto &imapping : imappings) {
	      auto mapping = orig_mapping->duplicate();
	      auto bmapping = static_cast<BtreeLBAMapping*>(mapping.get());
	      bmapping->adjust_mutable_indirect_attrs(
		imapping->get_key(),
		imapping->get_length(),
		imapping->get_intermediate_key());
	      mappings.emplace_back(std::move(mapping));
	    }
	    return seastar::make_ready_future<std::vector<LBAMappingRef>>(
	      std::move(mappings));
	  });
	} else { // !orig_mapping->is_indirect()
	  assert(remaps.size() == extents.size());
	  std::vector<alloc_mapping_info_t> alloc_infos;
	  for (auto i = 0; i < extents.size(); i++) {
	    auto &extent = extents[i];
	    auto &remap = remaps[i];
	    assert(extent->has_laddr());
	    assert(extent->get_laddr() - orig_mapping->get_key() == remap.offset);
	    assert(extent->get_length() == remap.len);
	    alloc_infos.emplace_back(alloc_mapping_info_t::create_direct(
	      extent->get_laddr(),
	      extent->get_length(),
	      extent->get_paddr(),
	      EXTENT_DEFAULT_REF_COUNT,
	      extent->get_last_committed_crc(),
	      extent.get(),
	      orig_mapping->has_shadow_mapping()
	      ? orig_mapping->get_shadow_val().add_offset(remap.offset)
	      : P_ADDR_NULL));
	  }
	  auto hint = remaps.front().offset + orig_laddr;
	  fut = seastar::do_with(std::move(alloc_infos), [this, &t, hint](auto &infos) {
	    return _alloc_extents(t, hint, infos, true);
	  });
	}

	return fut.si_then([&ret, &remaps, &orig_mapping](auto &&refs) {
	  assert(refs.size() == remaps.size());
#ifndef NDEBUG
	  auto ref_it = refs.begin();
	  auto remap_it = remaps.begin();
	  for (;ref_it != refs.end(); ref_it++, remap_it++) {
	    auto &ref = *ref_it;
	    auto &remap = *remap_it;
	    assert(ref->get_key() == orig_mapping->get_key() + remap.offset);
	    assert(ref->get_length() == remap.len);
	  }
#endif
	  ret.remapped_mappings = std::move(refs);
	  return seastar::now();
	});
      }).si_then([&remaps, &t, &orig_mapping, this] {
	if (remaps.size() > 1 && orig_mapping->is_full_indirect()) {
	  auto intermediate_base = orig_mapping->get_intermediate_base();
	  return _incref_extent(t, intermediate_base, remaps.size() - 1
	  ).si_then([](auto) {
	    return seastar::now();
	  });
	}
	return ref_iertr::now();
      }).si_then([&ret, &remaps] {
	assert(ret.remapped_mappings.size() == remaps.size());
	return seastar::make_ready_future<lba_remap_ret_t>(std::move(ret));
      });
    });
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
    extent_len_t prev_len,
    paddr_t prev_addr,
    extent_len_t len,
    paddr_t paddr,
    uint32_t checksum,
    LogicalCachedExtent*) final;

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

  struct move_mapping_state_t {
    laddr_t src_base;
    laddr_t dst_base;
    extent_len_t length;
    remap_extent_func_t remap_extent;
    lba_pin_list_t res;
    std::vector<alloc_mapping_info_t> mappings;
    std::list<LogicalCachedExtentRef> remapped_extents;
    std::list<LogicalCachedExtentRef> remapped_shadow_extents;

    laddr_t get_src_end() const { return src_base + length; }

    move_mapping_state_t(
      laddr_t src_base,
      laddr_t dst_base,
      extent_len_t length,
      remap_extent_func_t func)
      : src_base(src_base), dst_base(dst_base), length(length),
	remap_extent(std::move(func)), res(), mappings(), remapped_extents()
    {}
  };
  move_mappings_iertr::future<LBABtree::iterator>
  _move_mapping_without_remap(
    op_context_t<laddr_t> c,
    BtreeLBAManager::move_mapping_state_t &state,
    LBABtree::iterator &iter,
    laddr_t laddr,
    lba_map_val_t &map_val,
    LBABtree &btree);

  move_mappings_iertr::future<LBABtree::iterator>
  _move_mapping_with_remap(
    op_context_t<laddr_t> c,
    BtreeLBAManager::move_mapping_state_t &state,
    LBABtree::iterator &iter,
    laddr_t laddr,
    lba_map_val_t &map_val,
    LBABtree &btree);

  move_mappings_iertr::future<LBABtree::iterator>
  _remap_mapping_on_move(
    op_context_t<laddr_t> c,
    LBABtree &btree,
    BtreeLBAManager::move_mapping_state_t &state,
    LBABtree::iterator it,
    bool left);

  struct merge_mapping_state_t {
    laddr_t src_base;
    laddr_t dst_base;
    extent_len_t length;
    remap_extent_func_t remap_extent;
    lba_pin_list_t src_pins;
    lba_pin_list_t res;
    lba_pin_list_t::const_iterator src_it;
    std::list<LogicalCachedExtentRef> remapped_extents;

    laddr_t get_src_end() const { return src_base + length; }
    laddr_t get_dst_end() const { return dst_base + length; }

    merge_mapping_state_t(
      laddr_t src_base, laddr_t dst_base, extent_len_t length,
      remap_extent_func_t func, lba_pin_list_t src_pins)
      : src_base(src_base), dst_base(dst_base), length(length),
	remap_extent(std::move(func)),
	src_pins(std::move(src_pins)),
	src_it(this->src_pins.begin())
    {}
  };

  merge_mappings_iertr::future<>
  _force_remove_mappings_on_merge(
    op_context_t<laddr_t> c,
    merge_mapping_state_t &state,
    LBABtree &btree);

  merge_mappings_iertr::future<LBABtree::iterator>
  _merge_mapping(
    op_context_t<laddr_t> c,
    LBABtree::iterator &&iter,
    merge_mapping_state_t &state,
    laddr_t key,
    lba_map_val_t val,
    laddr_t intermediate_key,
    LBABtree &btree);


  seastar::metrics::metric_group metrics;
  void register_metrics();

  /**
   * update_refcount
   *
   * Updates refcount, returns resulting refcount
   */
  struct update_refcount_ret_bare_t {
    ref_update_result_t ref_update_res;
    BtreeLBAMappingRef mapping;
  };
  using update_refcount_iertr = ref_iertr;
  using update_refcount_ret = update_refcount_iertr::future<
    update_refcount_ret_bare_t>;
  update_refcount_ret update_refcount(
    Transaction &t,
    laddr_t addr,
    int delta,
    bool cascade_remove,
    bool subextent);

  /**
   * _update_mapping
   *
   * Updates mapping, removes if f returns nullopt
   */
  struct update_mapping_ret_bare_t {
    lba_map_val_t map_value;
    BtreeLBAMappingRef mapping;
  };
  using _update_mapping_iertr = ref_iertr;
  using _update_mapping_ret = ref_iertr::future<
    update_mapping_ret_bare_t>;
  using update_func_t = std::function<
    lba_map_val_t(const lba_map_val_t &v)
    >;
  _update_mapping_ret _update_mapping(
    Transaction &t,
    laddr_t addr,
    update_func_t &&f,
    LogicalCachedExtent*,
    bool subextent);

  alloc_extents_ret _alloc_extents(
    Transaction &t,
    laddr_t hint,
    std::vector<alloc_mapping_info_t> &alloc_infos,
    bool determinsitic);

  ref_ret _incref_extent(
    Transaction &t,
    laddr_t addr,
    int delta) {
    ceph_assert(delta > 0);
    return update_refcount(t, addr, delta, false, false
    ).si_then([](auto res) {
      return std::move(res.ref_update_res);
    });
  }

  alloc_extent_iertr::future<std::vector<BtreeLBAMappingRef>> alloc_cloned_mappings(
    Transaction &t,
    laddr_t laddr,
    std::vector<alloc_mapping_info_t> alloc_infos)
  {
#ifndef NDEBUG
    for (auto &alloc_info : alloc_infos) {
      assert(alloc_info.value.pladdr.build_laddr(L_ADDR_NULL) != L_ADDR_NULL);
    }
#endif
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, laddr](auto &alloc_infos) {
      return _alloc_extents(
	t,
	laddr,
	alloc_infos,
	true
      ).si_then([&alloc_infos](auto mappings) {
	assert(alloc_infos.size() == mappings.size());
	std::vector<BtreeLBAMappingRef> rets;
	auto mit = mappings.begin();
	auto ait = alloc_infos.begin();
	for (; mit != mappings.end(); mit++, ait++) {
	  auto mapping = static_cast<BtreeLBAMapping*>(mit->release());
	  auto &alloc_info = *ait;
	  assert(mapping->get_key() == alloc_info.key);
	  assert(mapping->get_raw_val().get_local_clone_id() ==
	    alloc_info.value.pladdr.get_local_clone_id());
	  assert(mapping->get_length() == alloc_info.value.len);
	  rets.emplace_back(mapping);
	}
	return rets;
      });
    });
  }

  using _get_mapping_ret = get_mapping_iertr::future<BtreeLBAMappingRef>;
  _get_mapping_ret _get_mapping(
    Transaction &t,
    laddr_t offset);

  using _get_original_mappings_ret = get_mappings_ret;
  _get_original_mappings_ret _get_original_mappings(
    op_context_t<laddr_t> c,
    std::list<BtreeLBAMappingRef> &pin_list);

  using _decref_intermediate_ret = ref_iertr::future<
    std::optional<ref_update_result_t>>;
  _decref_intermediate_ret _decref_intermediate(
    Transaction &t,
    laddr_t addr,
    extent_len_t len);

  using load_child_ext_ret = base_iertr::future<LogicalCachedExtentRef>;
  load_child_ext_ret load_child_ext(
    Transaction &t,
    const LBABtree::iterator &iter);

  struct insert_pos_t {
    insert_pos_t(LBABtree::iterator iter, laddr_t laddr)
      : iter(iter), laddr(laddr) {}
    LBABtree::iterator iter;
    laddr_t laddr;
  };
  using search_insert_pos_ret = alloc_extent_iertr::future<insert_pos_t>;
  search_insert_pos_ret search_insert_pos(
    Transaction &t,
    LBABtree &btree,
    laddr_t laddr,
    extent_len_t length,
    bool determinsitic);
};
using BtreeLBAManagerRef = std::unique_ptr<BtreeLBAManager>;

}

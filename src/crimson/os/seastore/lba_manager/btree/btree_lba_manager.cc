// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <seastar/core/metrics.hh>

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_lba);
/*
 * levels:
 * - INFO:  mkfs
 * - DEBUG: modification operations
 * - TRACE: read operations, DEBUG details
 */

namespace crimson::os::seastore {

template <typename T>
Transaction::tree_stats_t& get_tree_stats(Transaction &t)
{
  return t.get_lba_tree_stats();
}

template Transaction::tree_stats_t&
get_tree_stats<
  crimson::os::seastore::lba_manager::btree::LBABtree>(
  Transaction &t);

template <typename T>
phy_tree_root_t& get_phy_tree_root(root_t &r)
{
  return r.lba_root;
}

template phy_tree_root_t&
get_phy_tree_root<
  crimson::os::seastore::lba_manager::btree::LBABtree>(root_t &r);

template <>
const get_phy_tree_root_node_ret get_phy_tree_root_node<
  crimson::os::seastore::lba_manager::btree::LBABtree>(
  const RootBlockRef &root_block, op_context_t<laddr_t> c)
{
  auto lba_root = root_block->lba_root_node;
  if (lba_root) {
    ceph_assert(lba_root->is_initial_pending()
      == root_block->is_pending());
    return {true,
	    trans_intr::make_interruptible(
	      c.cache.get_extent_viewable_by_trans(c.trans, lba_root))};
  } else if (root_block->is_pending()) {
    auto &prior = static_cast<RootBlock&>(*root_block->get_prior_instance());
    lba_root = prior.lba_root_node;
    if (lba_root) {
      return {true,
	      trans_intr::make_interruptible(
		c.cache.get_extent_viewable_by_trans(c.trans, lba_root))};
    } else {
      return {false,
	      trans_intr::make_interruptible(
		Cache::get_extent_ertr::make_ready_future<
		  CachedExtentRef>())};
    }
  } else {
    return {false,
	    trans_intr::make_interruptible(
	      Cache::get_extent_ertr::make_ready_future<
		CachedExtentRef>())};
  }
}

template <typename ROOT>
void link_phy_tree_root_node(RootBlockRef &root_block, ROOT* lba_root) {
  root_block->lba_root_node = lba_root;
  ceph_assert(lba_root != nullptr);
  lba_root->root_block = root_block;
}

template void link_phy_tree_root_node(
  RootBlockRef &root_block, lba_manager::btree::LBAInternalNode* lba_root);
template void link_phy_tree_root_node(
  RootBlockRef &root_block, lba_manager::btree::LBALeafNode* lba_root);
template void link_phy_tree_root_node(
  RootBlockRef &root_block, lba_manager::btree::LBANode* lba_root);

template <>
void unlink_phy_tree_root_node<laddr_t>(RootBlockRef &root_block) {
  root_block->lba_root_node = nullptr;
}

}

namespace crimson::os::seastore::lba_manager::btree {

BtreeLBAManager::mkfs_ret
BtreeLBAManager::mkfs(
  Transaction &t)
{
  LOG_PREFIX(BtreeLBAManager::mkfs);
  INFOT("start", t);
  return cache.get_root(t).si_then([this, &t](auto croot) {
    assert(croot->is_mutation_pending());
    croot->get_root().lba_root = LBABtree::mkfs(croot, get_context(t));
    return mkfs_iertr::now();
  }).handle_error_interruptible(
    mkfs_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::mkfs"
    }
  );
}

BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  Transaction &t,
  laddr_t offset, extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::get_mappings);
  TRACET("{}~{}", t, offset, length);
  auto c = get_context(t);
  return with_btree_state<LBABtree, lba_pin_list_t>(
    cache,
    c,
    [c, offset, length, FNAME, this](auto &btree, auto &ret) {
      return seastar::do_with(
	std::list<BtreeLBAMappingRef>(),
	[offset, length, c, FNAME, this, &ret, &btree](auto &pin_list) {
	return LBABtree::iterate_repeat(
	  c,
	  btree.upper_bound_right(c, offset),
	  [&pin_list, offset, length, c, FNAME](auto &pos) {
	    if (pos.is_end() || pos.get_key() >= (offset + length)) {
	      TRACET("{}~{} done with {} results",
		     c.trans, offset, length, pin_list.size());
	      return LBABtree::base_iertr::make_ready_future<
		LBABtree::repeat_indicator_t>(
		  seastar::stop_iteration::yes, std::nullopt);
	    }
	    TRACET("{}~{} got {}, {}, repeat ...",
		   c.trans, offset, length, pos.get_key(), pos.get_val());
	    ceph_assert((pos.get_key() + pos.get_val().len) > offset);
	    pin_list.push_back(pos.get_pin(c));
	    return LBABtree::base_iertr::make_ready_future<
	      LBABtree::repeat_indicator_t>(
		seastar::stop_iteration::no, std::nullopt);
	  }).si_then([this, &ret, c, &pin_list] {
	    return _get_original_mappings(c, pin_list
	    ).si_then([&ret](auto _ret) {
	      ret = std::move(_ret);
	    });
	  });
	});
    });
}

BtreeLBAManager::_get_original_mappings_ret
BtreeLBAManager::_get_original_mappings(
  op_context_t<laddr_t> c,
  std::list<BtreeLBAMappingRef> &pin_list)
{
  return seastar::do_with(
    lba_pin_list_t(),
    [this, c, &pin_list](auto &ret) {
    return trans_intr::do_for_each(
      pin_list,
      [this, c, &ret](auto &pin) {
	LOG_PREFIX(BtreeLBAManager::get_mappings);
	if (pin->get_raw_val().is_paddr()) {
	  ret.emplace_back(std::move(pin));
	  return get_mappings_iertr::now();
	}
	TRACET(
	  "getting original mapping for indirect mapping {}~{}",
	  c.trans, pin->get_key(), pin->get_length());
	auto original_key = pin->get_raw_val().build_laddr(pin->get_key());
	return this->get_mappings(
	  c.trans, original_key, pin->get_length()
	).si_then([&pin, &ret, original_key, c](auto new_pin_list) {
	  LOG_PREFIX(BtreeLBAManager::get_mappings);
	  assert(new_pin_list.size() == 1);
	  auto &new_pin = new_pin_list.front();
	  auto intermediate_key = original_key;
	  assert(!new_pin->is_indirect());
	  assert(new_pin->get_key() <= intermediate_key);
	  assert(new_pin->get_key() + new_pin->get_length() >=
	  intermediate_key + pin->get_length());

	  TRACET("Got mapping {}~{} for indirect mapping {}~{}, "
	    "intermediate_key {}",
	    c.trans,
	    new_pin->get_key(), new_pin->get_length(),
	    pin->get_key(), pin->get_length(),
	    original_key);
	  auto &btree_new_pin = static_cast<BtreeLBAMapping&>(*new_pin);
	  btree_new_pin.make_indirect(
	    pin->get_key(),
	    pin->get_length(),
	    original_key);
	  ret.emplace_back(std::move(new_pin));
	  return seastar::now();
	}).handle_error_interruptible(
	  crimson::ct_error::input_output_error::pass_further{},
	  crimson::ct_error::assert_all("unexpected enoent")
	);
      }
    ).si_then([&ret] {
      return std::move(ret);
    });
  });
}


BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  Transaction &t,
  laddr_list_t &&list)
{
  LOG_PREFIX(BtreeLBAManager::get_mappings);
  TRACET("{}", t, list);
  auto l = std::make_unique<laddr_list_t>(std::move(list));
  auto retptr = std::make_unique<lba_pin_list_t>();
  auto &ret = *retptr;
  return trans_intr::do_for_each(
    l->begin(),
    l->end(),
    [this, &t, &ret](const auto &p) {
      return this->get_mappings(t, p.first, p.second).si_then(
	[&ret](auto res) {
	  ret.splice(ret.end(), res, res.begin(), res.end());
	  return get_mappings_iertr::now();
	});
    }).si_then([l=std::move(l), retptr=std::move(retptr)]() mutable {
      return std::move(*retptr);
    });
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(BtreeLBAManager::get_mapping);
  TRACET("{}", t, offset);
  return _get_mapping(t, offset
  ).si_then([](auto pin) {
    return get_mapping_iertr::make_ready_future<LBAMappingRef>(std::move(pin));
  });
}

BtreeLBAManager::_get_mapping_ret
BtreeLBAManager::_get_mapping(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(BtreeLBAManager::_get_mapping);
  TRACET("{}", t, offset);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, BtreeLBAMappingRef>(
    cache,
    c,
    [FNAME, c, offset, this](auto &btree) {
      return btree.lower_bound(
	c, offset
      ).si_then([FNAME, offset, c](auto iter) -> _get_mapping_ret {
	if (iter.is_end() || iter.get_key() != offset) {
	  ERRORT("laddr={} doesn't exist", c.trans, offset);
	  return crimson::ct_error::enoent::make();
	} else {
	  TRACET("{} got {}, {}",
	         c.trans, offset, iter.get_key(), iter.get_val());
	  auto e = iter.get_pin(c);
	  return _get_mapping_ret(
	    interruptible::ready_future_marker{},
	    std::move(e));
	}
      }).si_then([this, c](auto pin) -> _get_mapping_ret {
	if (pin->get_raw_val().is_laddr()) {
	  return seastar::do_with(
	    std::move(pin),
	    [this, c](auto &pin) {
	    return _get_mapping(
	      c.trans, pin->get_raw_val().build_laddr(pin->get_key())
	    ).si_then([&pin](auto new_pin) {
	      ceph_assert(pin->get_length() == new_pin->get_length());
	      new_pin->make_indirect(
		pin->get_key(),
		pin->get_length());
	      return new_pin;
	    });
	  });
	} else {
	  return get_mapping_iertr::make_ready_future<BtreeLBAMappingRef>(std::move(pin));
	}
      });
    });
}

std::vector<LBAManager::remap_entry> build_remaps(
  Transaction &t,
  const laddr_t src_base,
  const extent_len_t length,
  const laddr_t dst_base,
  const laddr_t pin_key,
  const lba_map_val_t &pin_val)
{
  LOG_PREFIX(build_remap_entries);
  bool split_left = pin_key < src_base;
  bool split_right = src_base + length < pin_key + pin_val.len;
  std::vector<LBAManager::remap_entry> remaps;

  auto offset = 0;
  if (split_left) {
    ceph_assert(pin_val.pladdr.is_paddr());
    offset = src_base - pin_key;
    TRACET("remap left at {}~{}", t, 0, offset);
    remaps.emplace_back(pin_key, 0, offset);
  }

  auto mid_len = std::min(src_base + length, pin_key + pin_val.len) -
    std::max(src_base, pin_key);
  TRACET("remap or move middle at {}~{}", t, offset, mid_len);
  auto dst_laddr = pin_key + offset - src_base + dst_base;
  remaps.emplace_back(dst_laddr, offset, mid_len);

  if (split_right) {
    ceph_assert(pin_val.pladdr.is_paddr());
    ceph_assert(pin_key < src_base + length);
    auto offset = src_base + length - pin_key;
    auto len = pin_val.len - offset;
    TRACET("remap right at {}~{}", t, offset, len);
    remaps.emplace_back(src_base + length, offset, len);
  }
#ifndef NDEBUG
  auto last_end = 0;
  for (auto &remap : remaps) {
    assert(last_end == remap.offset);
    last_end += remap.len;
  }
  assert(last_end == pin_val.len);
#endif
  return remaps;
}

BtreeLBAManager::load_child_ext_ret
BtreeLBAManager::load_child_ext(
  Transaction &t,
  const LBABtree::iterator &iter)
{
  auto c = get_context(t);
  auto map_val = iter.get_val();
  if (map_val.pladdr.is_paddr()) {
    auto pin = iter.get_pin(c);
    auto ext = pin->get_logical_extent(c.trans);
    if (ext.has_child()) {
      return trans_intr::make_interruptible(std::move(ext.get_child_fut()));
    } else {
      return base_iertr::make_ready_future<LogicalCachedExtentRef>(nullptr);
    }
  }
  return base_iertr::make_ready_future<LogicalCachedExtentRef>(nullptr);
}

BtreeLBAManager::move_mappings_iertr::future<LBABtree::iterator>
BtreeLBAManager::_move_mapping_without_remap(
  op_context_t<laddr_t> c,
  BtreeLBAManager::move_mapping_state_t &state,
  LBABtree::iterator &iter,
  laddr_t laddr,
  lba_map_val_t &map_val,
  LBABtree &btree)
{
  auto fut = base_iertr::make_ready_future<
    std::list<LogicalCachedExtentRef>>();
  if (map_val.pladdr.is_paddr() &&
      !map_val.pladdr.get_paddr().is_zero()) {
    fut = load_child_ext(c.trans, iter
    ).si_then([c, &state, &iter, laddr, map_val](auto ext) {
      return state.remap_extent(
	ext.get(),
	iter.get_pin(c),
	build_remaps(
	  c.trans,
	  state.src_base,
	  state.length,
	  state.dst_base,
	  laddr,
	  map_val));
    });
  }
  return fut.si_then([c, &iter, &btree](auto) {
    return btree.remove(c, iter);
  }).si_then([&state, laddr, map_val](auto iter) {
    auto nladdr = state.dst_base + (laddr - state.src_base);
    state.mappings.emplace_back(nladdr, map_val, nullptr);
    return move_mappings_iertr::make_ready_future<
      LBABtree::iterator>(std::move(iter));
  });
}

BtreeLBAManager::move_mappings_iertr::future<LBABtree::iterator>
BtreeLBAManager::_remap_mapping_on_move(
  op_context_t<laddr_t> c,
  LBABtree &btree,
  BtreeLBAManager::move_mapping_state_t &state,
  LBABtree::iterator it,
  bool left)
{
  LOG_PREFIX(remap_left_mapping);
  if (left) {
    ceph_assert(state.remapped_extents.size() > 1);
  } else {
    ceph_assert(state.remapped_extents.size() == 1);
  }
  auto ext = state.remapped_extents.front();
  state.remapped_extents.pop_front();
  auto val = lba_map_val_t{
    ext->get_length(),
    pladdr_t(ext->get_paddr()),
    EXTENT_DEFAULT_REF_COUNT,
    ext->get_last_committed_crc()
  };

  TRACET("insert remapped mapping {} {}",
	 c.trans,
	 ext->get_laddr(),
	 val);
  return btree.insert(
    c,
    it,
    ext->get_laddr(),
    val,
    ext.get()
  ).si_then([c](auto p) {
    ceph_assert(p.second);
    return p.first.next(c);
  });
}

BtreeLBAManager::move_mappings_iertr::future<LBABtree::iterator>
BtreeLBAManager::_move_mapping_with_remap(
  op_context_t<laddr_t> c,
  BtreeLBAManager::move_mapping_state_t &state,
  LBABtree::iterator &iter,
  laddr_t laddr,
  lba_map_val_t &map_val,
  LBABtree &btree)
{
  bool split_left = laddr < state.src_base;
  bool split_right = laddr + map_val.len > state.get_src_end();

  LOG_PREFIX(BtreeLBAManager::_move_mapping_with_remap);
  return load_child_ext(c.trans, iter
  ).si_then([c, &state, &iter, laddr, map_val](auto ext) {
    return state.remap_extent(
      ext.get(),
      iter.get_pin(c),
      build_remaps(
	c.trans,
	state.src_base,
	state.length,
	state.dst_base,
	laddr,
	map_val));
  }).si_then([c, &btree, &state, &iter, FNAME](auto extents) {
    std::swap(state.remapped_extents, extents);
    TRACET("remove mapping {}", c.trans, iter.get_key());
    return btree.remove(c, iter);
  }).si_then([c, split_left, this, &btree, &state](auto it) {
    if (split_left) {
      return _remap_mapping_on_move(c, btree, state, std::move(it), true);
    } else {
      return move_mappings_iertr::make_ready_future<
	LBABtree::iterator>(std::move(it));
    }
  }).si_then([&state, &btree, c, FNAME](auto it) {
    ceph_assert(state.remapped_extents.size() >= 1);
    // insert indirect mapping
    auto ext = state.remapped_extents.front();
    state.remapped_extents.pop_front();

    auto val = lba_map_val_t{
      ext->get_length(),
      pladdr_t(ext->get_paddr()),
      EXTENT_DEFAULT_REF_COUNT,
      ext->get_last_committed_crc()
    };

    state.mappings.emplace_back(ext->get_laddr(), val, ext.get());
    val.pladdr = ext->get_laddr();
    auto laddr = state.src_base + (ext->get_laddr() - state.dst_base);
    TRACET("move mapping from {} to {}, and create indirect mapping {}",
	   c.trans,
	   laddr,
	   ext->get_laddr(),
	   val);
    return btree.insert(c, std::move(it), laddr, val, nullptr
    ).si_then([c, &state](auto p) {
      state.res.emplace_back(p.first.get_pin(c));
      return p.first.next(c);
    });
  }).si_then([split_right, c, &btree, &state, this](auto it) {
    if (split_right) {
      return _remap_mapping_on_move(c, btree, state, std::move(it), false);
    } else {
      return move_mappings_iertr::make_ready_future<
	LBABtree::iterator>(std::move(it));
    }
  });
}

BtreeLBAManager::move_mappings_ret
BtreeLBAManager::move_mappings(
  Transaction &t,
  laddr_t src_base,
  laddr_t dst_base,
  extent_len_t length,
  bool direct_mapping_only,
  remap_extent_func_t func)
{
  LOG_PREFIX(BtreeLBAManager::move_mappings);
  DEBUGT("move {}~{} to {}, direct_mapping_only: {}",
	 t, src_base, length, dst_base, direct_mapping_only);

  auto c = get_context(t);
  return with_btree_state<LBABtree, move_mapping_state_t>(
    cache,
    c,
    move_mapping_state_t(src_base, dst_base, length, std::move(func)),
    [c, direct_mapping_only, FNAME, this](LBABtree &btree, move_mapping_state_t &state) {
      return LBABtree::iterate_repeat(
        c,
	btree.upper_bound_right(c, state.src_base),
	[c, direct_mapping_only, &btree, &state, FNAME, this](LBABtree::iterator &iter) {
	  if (iter.is_end() ||
	      iter.get_key() >= state.get_src_end()) {
	    TRACET("stopping iterate, finished moving {} mappings to dst",
		   c.trans, state.mappings.size());
	    return base_iertr::make_ready_future<LBABtree::repeat_indicator_t>(
	      seastar::stop_iteration::yes, std::nullopt);
	  }

	  auto laddr = iter.get_key();
	  auto map_val = iter.get_val();
	  ceph_assert(map_val.refcount == EXTENT_DEFAULT_REF_COUNT);
	  ceph_assert(laddr + map_val.len > state.src_base);

	  if (direct_mapping_only &&
	      (map_val.pladdr.is_laddr() ||
	       map_val.pladdr.get_paddr().is_zero())) {
	    TRACET("skip {} {}", c.trans, laddr, map_val);
	    state.res.emplace_back(iter.get_pin(c));
	    return base_iertr::make_ready_future<LBABtree::repeat_indicator_t>(
	      seastar::stop_iteration::no, std::nullopt);
	  }

	  if (!direct_mapping_only) {
	    // TODO: we only support mapping-aligned full mappings move for now.
	    ceph_assert(laddr >= state.src_base);
	    ceph_assert(laddr + map_val.len <= state.get_src_end());
	    return _move_mapping_without_remap(c, state, iter, laddr, map_val, btree
	    ).si_then([](auto iter) {
	      return move_mappings_iertr::make_ready_future<
		LBABtree::repeat_indicator_t>(
		  seastar::stop_iteration::no, std::move(iter));
	    });
	  } else {
	    return _move_mapping_with_remap(c, state, iter, laddr, map_val, btree
	    ).si_then([](auto it) {
	      return seastar::make_ready_future<
		LBABtree::repeat_indicator_t>(
		  seastar::stop_iteration::no, std::move(it));
	    });
	  }
	}).si_then([c, &state, direct_mapping_only, this] {
	  auto fut = alloc_extent_iertr::make_ready_future<
	    std::vector<LBAMappingRef>>();
	  if (!state.mappings.empty()) {
	    fut = _alloc_extents(c.trans, state.dst_base, state.mappings, true);
	  }
	  return fut.si_then([&state, direct_mapping_only](auto mappings) {
	    if (!direct_mapping_only) {
	      ceph_assert(state.res.empty());
	    }
	    for (auto &mapping : mappings) {
	      auto start = mapping->get_key();
	      auto end = mapping->get_key() + mapping->get_length();
	      assert(start >= state.dst_base && end <= state.dst_base + state.length);
	      if (!direct_mapping_only) {
		state.res.emplace_back(std::move(mapping));
	      }
	    }
	  });
	});
    }).si_then([](auto state) {
      return move_mappings_iertr::make_ready_future<
	lba_pin_list_t>(std::move(state.res));
    });
}

BtreeLBAManager::alloc_extents_ret
BtreeLBAManager::_alloc_extents(
  Transaction &t,
  laddr_t hint,
  std::vector<alloc_mapping_info_t> &alloc_infos,
  bool determinsitic)
{
  ceph_assert(hint != L_ADDR_NULL);
  extent_len_t total_len = 0;
#ifndef NDEBUG
  bool laddr_null = (alloc_infos.front().key == L_ADDR_NULL);
  laddr_t last_end = hint;
  for (auto &info : alloc_infos) {
    assert((info.key == L_ADDR_NULL) == (laddr_null));
    if (!laddr_null) {
      assert(info.key >= last_end);
      last_end = info.key + info.value.len;
    }
  }
#endif
  if (alloc_infos.front().key == L_ADDR_NULL) {
    for (auto &info : alloc_infos) {
      total_len += info.value.len;
    }
  } else {
    total_len = alloc_infos.back().key + alloc_infos.back().value.len - hint;
  }

  struct state_t {
    laddr_t last_end;

    std::optional<typename LBABtree::iterator> insert_iter;
    std::optional<typename LBABtree::iterator> ret;

    state_t(laddr_t hint) : last_end(hint) {}
  };

  LOG_PREFIX(BtreeLBAManager::_alloc_extents);
  TRACET("{}~{}, hint={}, num of extents: {}",
    t, alloc_infos.front().value.len, total_len, hint, alloc_infos.size());

  auto c = get_context(t);
  stats.num_alloc_extents += alloc_infos.size();
  return seastar::do_with(
    std::vector<LBAMappingRef>(),
    [this, FNAME, &alloc_infos, hint, &t, total_len, c,
    determinsitic](auto &rets) {
    return crimson::os::seastore::with_btree_state<LBABtree, state_t>(
      cache,
      c,
      hint,
      [this, c, hint, total_len, addr=alloc_infos.front().value.pladdr, &rets,
       determinsitic, &t, &alloc_infos, FNAME](auto &btree, auto &state) {
	return search_insert_pos(t, btree, hint, total_len, determinsitic
	).si_then([c, addr, hint, &btree, &state, &alloc_infos,
		   total_len, &rets, FNAME](auto pos) {
	state.last_end = pos.laddr;
	state.insert_iter = pos.iter;
	// the actual inserting pos may be different from hint,
	// we need to calculate the relative position
	auto base = state.last_end;
	return trans_intr::do_for_each(
	  alloc_infos,
	  [c, addr, hint, &btree, &state, FNAME,
	  total_len, &rets, base](auto &alloc_info) {
	  if (alloc_info.key != L_ADDR_NULL) {
	    state.last_end = base + (alloc_info.key - hint);
	  }
	  return btree.insert(
	    c,
	    *state.insert_iter,
	    state.last_end,
	    alloc_info.value,
	    alloc_info.extent
	  ).si_then([&state, c, addr, total_len, hint, FNAME,
		    &alloc_info, &rets](auto &&p) {
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, hint={}, inserted at {}",
		   c.trans, addr, total_len, hint, state.last_end);
	    if (alloc_info.extent) {
	      ceph_assert(alloc_info.value.pladdr.is_paddr());
	      assert(alloc_info.value.pladdr == iter.get_val().pladdr);
	      assert(alloc_info.value.len == iter.get_val().len);
	      if (alloc_info.extent->has_laddr()) {
		assert(alloc_info.key == alloc_info.extent->get_laddr());
		assert(alloc_info.key == iter.get_key());
	      } else {
		alloc_info.extent->set_laddr(iter.get_key());
	      }
	      alloc_info.extent->set_laddr(iter.get_key());
	    }
	    ceph_assert(inserted);
	    rets.emplace_back(iter.get_pin(c));
	    return iter.next(c).si_then([&state, &alloc_info](auto it) {
	      state.insert_iter = it;
	      if (alloc_info.key == L_ADDR_NULL) {
		state.last_end += alloc_info.value.len;
	      }
	    });
	  });
	});
      });
    }).si_then([&rets](auto &&state) {
      return alloc_extent_iertr::make_ready_future<
	std::vector<LBAMappingRef>>(std::move(rets));
    });
  });
}

static bool is_lba_node(const CachedExtent &e)
{
  return is_lba_node(e.get_type());
}

BtreeLBAManager::base_iertr::template future<>
_init_cached_extent(
  op_context_t<laddr_t> c,
  const CachedExtentRef &e,
  LBABtree &btree,
  bool &ret)
{
  if (e->is_logical()) {
    auto logn = e->cast<LogicalCachedExtent>();
    return btree.lower_bound(
      c,
      logn->get_laddr()
    ).si_then([e, c, logn, &ret](auto iter) {
      LOG_PREFIX(BtreeLBAManager::init_cached_extent);
      if (!iter.is_end() &&
	  iter.get_key() == logn->get_laddr() &&
	  iter.get_val().pladdr.is_paddr() &&
	  iter.get_val().pladdr.get_paddr() == logn->get_paddr()) {
	assert(!iter.get_leaf_node()->is_pending());
	iter.get_leaf_node()->link_child(logn.get(), iter.get_leaf_pos());
	logn->set_laddr(iter.get_pin(c)->get_key());
	ceph_assert(iter.get_val().len == e->get_length());
	DEBUGT("logical extent {} live", c.trans, *logn);
	ret = true;
      } else {
	DEBUGT("logical extent {} not live", c.trans, *logn);
	ret = false;
      }
    });
  } else {
    return btree.init_cached_extent(c, e
    ).si_then([&ret](bool is_alive) {
      ret = is_alive;
    });
  }
}

BtreeLBAManager::init_cached_extent_ret
BtreeLBAManager::init_cached_extent(
  Transaction &t,
  CachedExtentRef e)
{
  LOG_PREFIX(BtreeLBAManager::init_cached_extent);
  TRACET("{}", t, *e);
  return seastar::do_with(bool(), [this, e, &t](bool &ret) {
    auto c = get_context(t);
    return with_btree<LBABtree>(
      cache, c,
      [c, e, &ret](auto &btree) -> base_iertr::future<> {
	LOG_PREFIX(BtreeLBAManager::init_cached_extent);
	DEBUGT("extent {}", c.trans, *e);
	return _init_cached_extent(c, e, btree, ret);
      }
    ).si_then([&ret] { return ret; });
  });
}

BtreeLBAManager::check_child_trackers_ret
BtreeLBAManager::check_child_trackers(
  Transaction &t) {
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache, c,
    [c](auto &btree) {
    return btree.check_child_trackers(c);
  });
}

BtreeLBAManager::scan_mappings_ret
BtreeLBAManager::scan_mappings(
  Transaction &t,
  laddr_t begin,
  laddr_t end,
  scan_mappings_func_t &&f)
{
  LOG_PREFIX(BtreeLBAManager::scan_mappings);
  DEBUGT("begin: {}, end: {}", t, begin, end);

  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, f=std::move(f), begin, end](auto &btree) mutable {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, begin),
	[f=std::move(f), begin, end](auto &pos) {
	  if (pos.is_end() || pos.get_key() >= end) {
	    return LBABtree::base_iertr::make_ready_future<
	      LBABtree::repeat_indicator_t>(
		seastar::stop_iteration::yes, std::nullopt);
	  }
	  ceph_assert((pos.get_key() + pos.get_val().len) > begin);
	  if (pos.get_val().pladdr.is_paddr()) {
	    f(pos.get_key(), pos.get_val().pladdr.get_paddr(), pos.get_val().len);
	  }
	  return LBABtree::base_iertr::make_ready_future<
	    LBABtree::repeat_indicator_t>(
	      seastar::stop_iteration::no, std::nullopt);
	});
    });
}

BtreeLBAManager::rewrite_extent_ret
BtreeLBAManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  LOG_PREFIX(BtreeLBAManager::rewrite_extent);
  if (extent->has_been_invalidated()) {
    ERRORT("extent has been invalidated -- {}", t, *extent);
    ceph_abort();
  }
  assert(!extent->is_logical());

  if (is_lba_node(*extent)) {
    DEBUGT("rewriting lba extent -- {}", t, *extent);
    auto c = get_context(t);
    return with_btree<LBABtree>(
      cache,
      c,
      [c, extent](auto &btree) mutable {
	return btree.rewrite_extent(c, extent);
      });
  } else {
    DEBUGT("skip non lba extent -- {}", t, *extent);
    return rewrite_extent_iertr::now();
  }
}

BtreeLBAManager::update_mapping_ret
BtreeLBAManager::update_mapping(
  Transaction& t,
  laddr_t laddr,
  extent_len_t prev_len,
  paddr_t prev_addr,
  extent_len_t len,
  paddr_t addr,
  uint32_t checksum,
  LogicalCachedExtent *nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  TRACET("laddr={}, paddr {} => {}", t, laddr, prev_addr, addr);
  return _update_mapping(
    t,
    laddr,
    [prev_addr, addr, prev_len, len, checksum](
      const lba_map_val_t &in) {
      assert(!addr.is_null());
      lba_map_val_t ret = in;
      ceph_assert(in.pladdr.is_paddr());
      ceph_assert(in.pladdr.get_paddr() == prev_addr);
      ceph_assert(in.len == prev_len);
      ret.pladdr = addr;
      ret.len = len;
      ret.checksum = checksum;
      return ret;
    },
    nextent,
    false
  ).si_then([&t, laddr, prev_addr, addr, FNAME](auto res) {
      auto &result = res.map_value;
      DEBUGT("laddr={}, paddr {} => {} done -- {}",
             t, laddr, prev_addr, addr, result);
      return update_mapping_iertr::make_ready_future<
	extent_ref_count_t>(result.refcount);
    },
    update_mapping_iertr::pass_further{},
    /* ENOENT in particular should be impossible */
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::update_mapping"
    }
  );
}

BtreeLBAManager::get_physical_extent_if_live_ret
BtreeLBAManager::get_physical_extent_if_live(
  Transaction &t,
  extent_types_t type,
  paddr_t addr,
  laddr_t laddr,
  extent_len_t len)
{
  LOG_PREFIX(BtreeLBAManager::get_physical_extent_if_live);
  DEBUGT("{}, laddr={}, paddr={}, length={}",
         t, type, laddr, addr, len);
  ceph_assert(is_lba_node(type));
  auto c = get_context(t);
  return with_btree_ret<LBABtree, CachedExtentRef>(
    cache,
    c,
    [c, type, addr, laddr, len](auto &btree) {
      if (type == extent_types_t::LADDR_INTERNAL) {
	return btree.get_internal_if_live(c, addr, laddr, len);
      } else {
	assert(type == extent_types_t::LADDR_LEAF ||
	       type == extent_types_t::DINK_LADDR_LEAF);
	return btree.get_leaf_if_live(c, addr, laddr, len);
      }
    });
}

void BtreeLBAManager::register_metrics()
{
  LOG_PREFIX(BtreeLBAManager::register_metrics);
  DEBUG("start");
  stats = {};
  namespace sm = seastar::metrics;
  metrics.add_group(
    "LBA",
    {
      sm::make_counter(
        "alloc_extents",
        stats.num_alloc_extents,
        sm::description("total number of lba alloc_extent operations")
      ),
      sm::make_counter(
        "alloc_extents_iter_nexts",
        stats.num_alloc_extents_iter_nexts,
        sm::description("total number of iterator next operations during extent allocation")
      ),
    }
  );
}

BtreeLBAManager::_decref_intermediate_ret
BtreeLBAManager::_decref_intermediate(
  Transaction &t,
  laddr_t addr,
  extent_len_t len)
{
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, addr, len](auto &btree) mutable {
    return btree.upper_bound_right(
      c, addr
    ).si_then([&btree, addr, len, c](auto iter) {
      return seastar::do_with(
	std::move(iter),
	[&btree, addr, len, c](auto &iter) {
	ceph_assert(!iter.is_end());
	ceph_assert(iter.get_key() <= addr);
	auto val = iter.get_val();
	ceph_assert(iter.get_key() + val.len >= addr + len);
	ceph_assert(val.pladdr.is_paddr());
	ceph_assert(val.refcount >= 1);
	val.refcount -= 1;

	LOG_PREFIX(BtreeLBAManager::_decref_intermediate);
	TRACET("decreased refcount of intermediate key {} -- {}",
	  c.trans,
	  iter.get_key(),
	  val);

	if (!val.refcount) {
	  return btree.remove(c, iter
	  ).si_then([val](auto) {
	    auto res = ref_update_result_t{
	      val.refcount,
	      val.pladdr,
	      val.len
	    };
	    return ref_iertr::make_ready_future<
	      std::optional<ref_update_result_t>>(
	        std::make_optional<ref_update_result_t>(res));
	  });
	} else {
	  return btree.update(c, iter, val, nullptr
	  ).si_then([](auto) {
	    return ref_iertr::make_ready_future<
	      std::optional<ref_update_result_t>>(std::nullopt);
	  });
	}
      });
    });
  });
}

BtreeLBAManager::update_refcount_ret
BtreeLBAManager::update_refcount(
  Transaction &t,
  laddr_t addr,
  int delta,
  bool cascade_remove,
  bool subextent)
{
  LOG_PREFIX(BtreeLBAManager::update_refcount);
  TRACET("laddr={}, delta={}", t, addr, delta);
  return _update_mapping(
    t,
    addr,
    [delta](const lba_map_val_t &in) {
      lba_map_val_t out = in;
      ceph_assert((int)out.refcount + delta >= 0);
      out.refcount += delta;
      return out;
    },
    nullptr,
    subextent
  ).si_then([&t, addr, delta, FNAME, this, cascade_remove](auto res) {
    auto &map_value = res.map_value;
    auto &mapping = res.mapping;
    DEBUGT("laddr={}, delta={} done -- {}", t, addr, delta, map_value);
    auto fut = ref_iertr::make_ready_future<
      std::optional<ref_update_result_t>>();
    if (!map_value.refcount && map_value.pladdr.is_laddr() && cascade_remove) {
      fut = _decref_intermediate(
	t,
	map_value.pladdr.build_laddr(addr),
	map_value.len
      );
    }
    return fut.si_then([map_value, mapping=std::move(mapping)]
		       (auto decref_intermediate_res) mutable {
      if (map_value.pladdr.is_laddr()
	  && decref_intermediate_res) {
	return update_refcount_ret_bare_t{
	  *decref_intermediate_res,
	  std::move(mapping)
	};
      } else {
	return update_refcount_ret_bare_t{
	  ref_update_result_t{
	    map_value.refcount,
	    map_value.pladdr,
	    map_value.len
	  },
	  std::move(mapping)
	};
      }
    });
  });
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f,
  LogicalCachedExtent* nextent,
  bool subextent)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, update_mapping_ret_bare_t>(
    cache,
    c,
    [f=std::move(f), c, addr, nextent, subextent](auto &btree) mutable {
      return btree.upper_bound_right(
	c, addr
      ).si_then([&btree, f=std::move(f), c, addr, nextent, subextent](auto iter)
		-> _update_mapping_ret {
	if (iter.is_end() ||
	    (subextent && iter.get_key() > addr) ||
	    (!subextent && iter.get_key() != addr)) {
	  LOG_PREFIX(BtreeLBAManager::_update_mapping);
	  ERRORT("laddr={} doesn't exist", c.trans, addr);
	  return crimson::ct_error::enoent::make();
	}
	assert(iter.get_key() + iter.get_val().len > addr);

	auto ret = f(iter.get_val());
	if (ret.refcount == 0) {
	  return btree.remove(
	    c,
	    iter
	  ).si_then([ret](auto) {
	    return update_mapping_ret_bare_t{
	      std::move(ret),
	      BtreeLBAMappingRef(nullptr)
	    };
	  });
	} else {
	  return btree.update(
	    c,
	    iter,
	    ret,
	    nextent
	  ).si_then([c, ret](auto iter) {
	    return update_mapping_ret_bare_t{
	      std::move(ret),
	      iter.get_pin(c)
	    };
	  });
	}
      });
    });
}

BtreeLBAManager::search_insert_pos_ret
BtreeLBAManager::search_insert_pos(
  Transaction &t,
  LBABtree &btree,
  laddr_t laddr,
  extent_len_t length,
  bool determinsitic)
{
  auto c = get_context(t);
  if (determinsitic) {
    return btree.lower_bound(c, laddr
    ).si_then([c, laddr, length](LBABtree::iterator pos) {
      LOG_PREFIX(BtreeLBAManager::search_insert_pos);
      if (!pos.is_end()) {
	ceph_assert(pos.get_key() != laddr);
	ceph_assert(laddr + length <= pos.get_key());
	DEBUGT("insert {}~{} at {} -- {}", c.trans, laddr, length, pos.get_key(), pos.get_val());
      } else {
	DEBUGT("insert {}~{} at begin", c.trans, laddr, length);
      }

#ifndef NDEBUG
      if (!pos.is_begin()) {
	return pos.prev(c).si_then([laddr, pos](LBABtree::iterator prev) {
	  auto prev_laddr = prev.get_key();
	  auto prev_length = prev.get_val().len;
	  ceph_assert(prev_laddr + prev_length <= laddr);
	  return alloc_extent_iertr::make_ready_future<
	    insert_pos_t>(std::move(pos), laddr);
	});
      } else
#endif
	return alloc_extent_iertr::make_ready_future<
	  insert_pos_t>(std::move(pos), laddr);
    });
  } else {
    struct state_t {
      state_t(laddr_t laddr) : last_end(laddr), iter(std::nullopt) {}
      laddr_t last_end;
      std::optional<LBABtree::iterator> iter;
    };
    auto lookup_attempts = stats.num_alloc_extents;
    return seastar::do_with(
      state_t(laddr),
      [this, c, &btree, laddr, length, lookup_attempts](state_t &state) {
	return trans_intr::repeat([this, c, &btree, &state,
				   laddr, length, lookup_attempts] {
	  return btree.upper_bound_right(c, state.last_end
	  ).si_then([this, &state, c, laddr, length, lookup_attempts](LBABtree::iterator pos) {
	    LOG_PREFIX(BtreeLBAManager::search_insert_pos);
	    ++stats.num_alloc_extents_iter_nexts;
	    if (pos.is_end()) {
	      DEBUGT("{}~{} state end, done with {} attempts, insert at {}",
		     c.trans, laddr, length,
		     stats.num_alloc_extents_iter_nexts - lookup_attempts,
		     state.last_end);
	      state.iter = pos;
	      return typename LBABtree::iterate_repeat_ret_inner(
	        interruptible::ready_future_marker{},
	        seastar::stop_iteration::yes);
	    } else if (pos.get_key() >= (state.last_end + length) &&
		       (!laddr.has_valid_prefix() ||
			pos.get_key().get_object_prefix() !=
			state.last_end.get_object_prefix())) {
	      DEBUGT("{}~{} state {}~{}, done with {} attempts, insert at {}",
		     c.trans, laddr, length, pos.get_key(), pos.get_val().len,
		     stats.num_alloc_extents_iter_nexts - lookup_attempts,
		     state.last_end);
	      state.iter = pos;
	      return typename LBABtree::iterate_repeat_ret_inner(
	        interruptible::ready_future_marker{},
	        seastar::stop_iteration::yes);
	    } else {
	      state.last_end = laddr_t::get_next_hint(state.last_end);
	      DEBUGT("{}~{} state {}~{}, retry with {}",
		     c.trans, laddr, length, pos.get_key(),
		     pos.get_val().len, state.last_end);
	      return typename LBABtree::iterate_repeat_ret_inner(
	        interruptible::ready_future_marker{},
	        seastar::stop_iteration::no);
	    }
	  });
	}).si_then([&state] {
	  return alloc_extent_iertr::make_ready_future<
	    insert_pos_t>(*state.iter, state.last_end);
	});
      });
  }
}

}

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

enum class shadow_mapping_t : uint16_t {
  COLD_MIRROR = 1,
  INVALID = 2,
  MAX = L_ADDR_ALIGNMENT - 1
};

shadow_mapping_t get_shadow_mapping(laddr_t laddr) {
  assert(is_shadow_laddr(laddr));
  auto diff = laddr - p2align(laddr, L_ADDR_ALIGNMENT);
  assert(diff < static_cast<uint16_t>(shadow_mapping_t::INVALID));
  return static_cast<shadow_mapping_t>(diff);
}

laddr_t map_shadow_laddr(laddr_t laddr, shadow_mapping_t mapping) {
  assert(!is_shadow_laddr(laddr));
  assert(mapping < shadow_mapping_t::INVALID);
  return laddr + static_cast<uint16_t>(mapping);
}

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
		seastar::make_ready_future<
		  CachedExtentRef>(CachedExtentRef()))};
    }
  } else {
    return {false,
	    trans_intr::make_interruptible(
	      seastar::make_ready_future<
		CachedExtentRef>(CachedExtentRef()))};
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
	      return LBABtree::iterate_repeat_ret_inner(
		interruptible::ready_future_marker{},
		seastar::stop_iteration::yes);
	    }
	    TRACET("{}~{} got {}, {}, repeat ...",
		   c.trans, offset, length, pos.get_key(), pos.get_val());
	    ceph_assert(pos.get_val_end() > offset);
            if (!is_shadow_laddr(pos.get_key())) {
              pin_list.push_back(pos.get_pin(c));
            }
	    return LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }).si_then([this, &ret, c, &pin_list, length, offset] {
	    return _get_original_mappings(c, pin_list, offset, length
	    ).si_then([&ret](auto _ret) {
	      ret = std::move(_ret);
	    });
	  });
	});
    });
}

BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings_with_shadow(
  Transaction &t,
  laddr_t offset, extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::get_mappings);
  TRACET("{}~{}", t, offset, length);
  auto c = get_context(t);
  return with_btree_state<LBABtree, lba_pin_list_t>(
    cache,
    c,
    [c, offset, length, FNAME](auto &btree, auto &ret) {
      return LBABtree::iterate_repeat(
	c,
        btree.upper_bound_right(c, offset),
        [&ret, offset, length, c, FNAME](auto &pos) {
          if (pos.is_end() || pos.get_key() >= (offset + length)) {
            TRACET("{}~{} done with {} results",
                   c.trans, offset, length, ret.size());
            return LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
              seastar::stop_iteration::yes);
          }
          TRACET("{}~{} got {}, {}, repeat ...",
                 c.trans, offset, length, pos.get_key(), pos.get_val());
          ceph_assert(pos.get_val_end() > offset);
          auto pin = pos.get_pin(c);
          if (!pin->is_indirect()) {
            ret.push_back(std::move(pin));
          }
          return LBABtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
            seastar::stop_iteration::no);
        });
    });
}

BtreeLBAManager::_get_original_mappings_ret
BtreeLBAManager::_get_original_mappings(
  op_context_t<laddr_t> c,
  std::list<BtreeLBAMappingRef> &pin_list,
  laddr_t offset,
  extent_len_t length)
{
  return seastar::do_with(
    lba_pin_list_t(),
    [this, c, &pin_list, length, offset](auto &ret) {
    return trans_intr::do_for_each(
      pin_list,
      [this, c, &ret, length, offset](auto &pin) {
	LOG_PREFIX(BtreeLBAManager::get_mappings);
	if (pin->get_raw_val().is_laddr()) {
	  TRACET(
	    "getting original mapping for indirect mapping {}~{}",
	    c.trans, pin->get_key(), pin->get_length());
	  return this->get_mappings(
	    c.trans, pin->get_raw_val().get_laddr(), pin->get_length()
	  ).si_then([&pin, &ret, c, offset, length](auto new_pin_list) {
	    auto off = 0;
	    for (auto &new_pin : new_pin_list) {
	      if (new_pin->is_shadow_mapping()) {
		continue;
	      }
	      if (pin->get_key() + off + new_pin->get_length() <= offset) {
		// exclude mappings outside `offset`~`length`
		off += new_pin->get_length();
		continue;
	      }
	      if (pin->get_key() + off >= offset + length) {
		break;
	      }

	      LOG_PREFIX(BtreeLBAManager::get_mappings);
	      static_cast<BtreeLBAMapping&>(*new_pin).set_key_for_indirect(
		pin->get_key() + off);
	      off += new_pin->get_length();
	      TRACET("Got mapping {}~{} for indirect mapping {}~{}",
		c.trans,
		new_pin->get_intermediate_key(), new_pin->get_length(),
		pin->get_key(), pin->get_length());
	      ret.emplace_back(new_pin.release());
	    }
	  }).handle_error_interruptible(
	    crimson::ct_error::input_output_error::pass_further{},
	    crimson::ct_error::assert_all("unexpected enoent")
	  );
	} else {
	  ret.emplace_back(pin.release());
	  return get_mappings_iertr::now();
	}
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
    return get_mapping_iertr::make_ready_future<LBAMappingRef>(pin.release());
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
	      c.trans, pin->get_raw_val().get_laddr()
	    ).si_then([&pin](auto new_pin) {
	      ceph_assert(pin->get_length() == new_pin->get_length());
	      new_pin->set_key_for_indirect(pin->get_key());
	      return new_pin;
	    });
	  });
	} else {
	  return get_mapping_iertr::make_ready_future<BtreeLBAMappingRef>(std::move(pin));
	}
      });
    });
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::alloc_extent(
  Transaction &t,
  laddr_t hint,
  extent_len_t len,
  pladdr_t addr,
  paddr_t actual_addr,
  LogicalCachedExtent* nextent)
{
  return _alloc_extent(t, hint, len, addr, actual_addr, 1, nextent
  ).si_then([](auto pin) {
    return alloc_extent_iertr::make_ready_future<LBAMappingRef>(pin.release());
  });
}

BtreeLBAManager::alloc_extent_iertr::future<BtreeLBAMappingRef>
BtreeLBAManager::_alloc_extent(
  Transaction &t,
  laddr_t hint,
  extent_len_t len,
  pladdr_t addr,
  paddr_t actual_addr,
  uint32_t refcnt,
  LogicalCachedExtent* nextent)
{
  struct state_t {
    laddr_t last_end;

    std::optional<typename LBABtree::iterator> insert_iter;
    std::optional<typename LBABtree::iterator> ret;

    state_t(laddr_t hint) : last_end(hint) {}
  };

  assert(!is_shadow_laddr(hint));
  assert(!is_shadow_laddr(hint + len));
  LOG_PREFIX(BtreeLBAManager::_alloc_extent);
  TRACET("{}~{}, hint={}", t, addr, len, hint);
  auto c = get_context(t);
  ++stats.num_alloc_extents;
  auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
  return crimson::os::seastore::with_btree_state<LBABtree, state_t>(
    cache,
    c,
    hint,
    [this, FNAME, c, hint, len, addr, lookup_attempts,
    &t, refcnt, nextent](auto &btree, auto &state) {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, hint),
	[this, &state, len, addr, &t, hint, FNAME, lookup_attempts](auto &pos) {
	  ++stats.num_alloc_extents_iter_nexts;
	  if (pos.is_end()) {
	    DEBUGT("{}~{}, hint={}, state: end, done with {} attempts, insert at {}",
                   t, addr, len, hint,
                   stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end);
	    state.insert_iter = pos;
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else if (pos.get_key() >= (state.last_end + len)) {
	    DEBUGT("{}~{}, hint={}, state: {}~{}, done with {} attempts, insert at {} -- {}",
                   t, addr, len, hint,
                   pos.get_key(), pos.get_val().len,
                   stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end,
                   pos.get_val());
	    state.insert_iter = pos;
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else {
	    if (!is_shadow_laddr(pos.get_key())) {
	      state.last_end = pos.get_key() + pos.get_val().len;
	    }
	    TRACET("{}~{}, hint={}, state: {}~{}, repeat ... -- {}",
                   t, addr, len, hint,
                   pos.get_key(), pos.get_val().len,
                   pos.get_val());
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }
	}).si_then([FNAME, c, addr, len, hint, &btree, &state, refcnt, nextent] {
	  return btree.insert(
	    c,
	    *state.insert_iter,
	    state.last_end,
	    lba_map_val_t{len, pladdr_t(addr), refcnt, 0},
	    nextent
	  ).si_then([&state, FNAME, c, addr, len, hint, nextent](auto &&p) {
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, hint={}, inserted at {}",
	           c.trans, addr, len, hint, state.last_end);
	    if (nextent) {
	      ceph_assert(addr.is_paddr());
	      assert(!is_shadow_laddr(iter.get_key()));
	      nextent->set_laddr(iter.get_key());
	    }
	    ceph_assert(inserted);
	    state.ret = iter;
	  });
	});
    }).si_then([c, actual_addr, addr](auto &&state)
      -> alloc_extent_iertr::future<BtreeLBAMappingRef> {
      auto ret_pin = state.ret->get_pin(c);
      if (actual_addr != P_ADDR_NULL) {
	ceph_assert(addr.is_laddr());
	ret_pin->set_paddr(actual_addr);
      } else {
	ceph_assert(addr.is_paddr());
      }
      return alloc_extent_iertr::make_ready_future<BtreeLBAMappingRef>(
	std::move(ret_pin));
    });
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::alloc_shadow_extent(
  Transaction &t,
  laddr_t laddr,
  extent_len_t len,
  paddr_t paddr,
  LogicalCachedExtent *nextent)
{
  assert(enable_shadow_entry);
  assert(!is_shadow_laddr(laddr));
  assert(!is_shadow_laddr(laddr + len));
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAMappingRef>(
    cache,
    c,
    [c, laddr, len, paddr, nextent](LBABtree &btree) {
      return btree.lower_bound(c, laddr
      ).si_then([c, laddr, len, paddr, nextent, &btree](auto iter) {
        assert(!iter.is_end());
	assert(iter.get_key() == laddr);
        return iter.next(c
        ).si_then([c, laddr, len, paddr, nextent, iter, &btree](auto niter) {
	  auto shadow_laddr = map_shadow_laddr(laddr, shadow_mapping_t::COLD_MIRROR);
	  assert(niter.get_key() != shadow_laddr);
          auto val = iter.get_val();
	  assert(len == val.len);
          val.pladdr = paddr;
          return btree.insert(
	    c,
	    niter,
	    shadow_laddr,
	    val,
	    nextent
          ).si_then([c, nextent](auto iter) {
	    assert(iter.second);
	    if (nextent) {
	      nextent->set_laddr(iter.first.get_key());
	    }
	    return iter.first.get_pin(c);
          });
        });
      });
    });
}

BtreeLBAManager::demote_region_ret
BtreeLBAManager::demote_region(
  Transaction &t,
  laddr_t laddr,
  extent_len_t length,
  extent_len_t max_demote_size,
  retire_promotion_func_t retire_func,
  update_nextent_func_t update_func)
{
  struct state_t {
    struct shadow_info_t {
      paddr_t paddr = P_ADDR_NULL;
      LogicalCachedExtent *nextent = nullptr;
    };
    std::map<laddr_t, shadow_info_t> shadow_entries;
    demote_region_res_t res;
    retire_promotion_func_t retire_func;
    update_nextent_func_t update_func;
  };
  LOG_PREFIX(BtreeLBAManager::demote_region);
  TRACET("demote {}~{} max_demote_size={}",
         t, laddr, length, max_demote_size);
  auto c = get_context(t);
  return with_btree_state<LBABtree, state_t>(
    cache,
    c,
    [c, laddr, length, max_demote_size, &t, FNAME,
     retire_func=std::move(retire_func),
     update_func=std::move(update_func)]
    (LBABtree &btree, state_t &state) {
      state.retire_func = std::move(retire_func);
      state.update_func = std::move(update_func);
      return LBABtree::iterate_repeat(
        c,
        btree.upper_bound_right(c, laddr),
        [laddr, length, max_demote_size,
         &state, &t, FNAME](auto pos) {
          if (pos.is_end() ||
              (pos.get_key() >= laddr + length) ||
              (state.res.demote_size >= max_demote_size)) {
            state.res.completed = pos.is_end() ||
              pos.get_key() >= laddr + length;
            return LBABtree::iterate_repeat_ret_inner(
              interruptible::ready_future_marker{},
              seastar::stop_iteration::yes);
          } else {
	    laddr_t key = pos.get_key();
	    lba_map_val_t val = pos.get_val();
            if (is_shadow_laddr(pos.get_key())) {
	      assert(get_shadow_mapping(pos.get_key()) ==
		     shadow_mapping_t::COLD_MIRROR);
              TRACET("shadow entry: laddr={}, pladdr={}, length={}",
                     t, key, val.pladdr, val.len);
              state.shadow_entries.insert({key, {val.pladdr.get_paddr(), nullptr}});
              state.res.demote_size += val.len;
            } else {
              TRACET("normal entry: laddr={}, pladdr={}, length={}",
                     t, key, val.pladdr, val.len);
              state.res.proceed_size += val.len;
            }
            return LBABtree::iterate_repeat_ret_inner(
              interruptible::ready_future_marker{},
              seastar::stop_iteration::no);
          }
        }).si_then([c, &btree, &state, &t, FNAME] {
          return trans_intr::do_for_each(
            state.shadow_entries,
            [c, &btree, &t, &state, FNAME]
	    (std::pair<const unsigned long,
	     state_t::shadow_info_t> p) mutable {
              return btree.lower_bound(c, p.first
              ).si_then([c, &btree, &t, &state, p, FNAME](auto iter) mutable {
		auto &[laddr, shadow_info] = p;
		auto key = iter.get_key();
		auto val = iter.get_val();
                assert(key == laddr);
		DEBUGT("remove shadow entry: laddr={}, pladdr={}, length={}",
		       t, key, val.pladdr, val.len);

		auto ext = iter.get_pin(c)->get_logical_extent(t);
		if (ext.has_child()) {
		  ceph_assert(val.pladdr.is_paddr());
                  return trans_intr::make_interruptible(
                    std::move(ext.get_child_fut())
                  ).then_interruptible([val, key, &state] (auto extent) mutable {
		    return state.update_func(
		      extent.get(),
		      val.pladdr.get_paddr(),
		      val.len
		    ).si_then([&state, key](auto nextent) mutable {
		      assert(nextent);
		      state.shadow_entries[key].nextent = nextent;
		    });
                  }).si_then([&btree, c, iter] {
		    return btree.remove(c, iter
		    ).si_then([](auto) {
		      return seastar::now();
		    });
		  });
		} else {
		  if (val.pladdr.is_paddr()) {
		    return state.update_func(
		      nullptr,
		      val.pladdr.get_paddr(),
		      val.len
		    ).si_then([&btree, &state, key, c, iter](auto nextent)mutable {
		      assert(nextent);
		      state.shadow_entries[key].nextent = nextent;
		      return btree.remove(c, iter
		      ).si_then([](auto) {
			return seastar::now();
		      });
		    });
		  } else {
		    return base_iertr::now();
		  }
		}
              });
            });
        }).si_then([c, &btree, &state, &t, FNAME,
                    retire_func = std::move(retire_func)] {
          return trans_intr::do_for_each(
            state.shadow_entries,
            [c, &btree, &t, FNAME, &state](auto p) {
              return btree.lower_bound(c, reset_shadow_mapping(p.first)
              ).si_then([c, &btree, &t, FNAME, p, &state] (auto iter) {
                assert(iter.get_key() == reset_shadow_mapping(p.first));
		auto val = iter.get_val();
		auto retired_pladdr = val.pladdr;
		auto retired_length = val.len;
		DEBUGT("update primary entry. laddr: {}, pladdr: {} => {}, length: {}",
		       t, iter.get_key(), val.pladdr, p.second.paddr, val.len);
		val.pladdr = p.second.paddr;
		assert(p.second.nextent);
		return btree.update(c, iter, val, p.second.nextent
		).si_then([&state, retired_pladdr, retired_length,
			   nextent=p.second.nextent](auto niter) mutable {
		  assert(nextent);
		  assert(!is_shadow_laddr(niter.get_key()));
		  nextent->set_laddr(niter.get_key());
		  return state.retire_func(retired_pladdr.get_paddr(), retired_length);
		});
              });
            });
        });
    }).si_then([](auto &&state) {
      return std::move(state.res);
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
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  }
	  ceph_assert(pos.get_val_end() > begin);
	  f(pos.get_key(), pos.get_val().pladdr.get_paddr(), pos.get_val().len);
	  return LBABtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::no);
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
  paddr_t prev_addr,
  paddr_t addr,
  LogicalCachedExtent *nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  TRACET("laddr={}, paddr {} => {}", t, laddr, prev_addr, addr);
  return _update_mapping(
    t,
    laddr,
    [prev_addr, addr](
      const lba_map_val_t &in) {
      assert(!addr.is_null());
      lba_map_val_t ret = in;
      ceph_assert(in.pladdr.is_paddr());
      ceph_assert(in.pladdr.get_paddr() == prev_addr);
      ret.pladdr = addr;
      return ret;
    },
    nextent
  ).si_then([&t, laddr, prev_addr, addr, FNAME](auto result) {
      DEBUGT("laddr={}, paddr {} => {} done -- {}",
             t, laddr, prev_addr, addr,
	     result.val.index() == 0
	      ? std::get<0>(result.val)
	      : std::get<1>(result.val)->get_map_val());
    },
    update_mapping_iertr::pass_further{},
    /* ENOENT in particular should be impossible */
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::update_mapping"
    }
  );
}

BtreeLBAManager::split_mapping_ret
BtreeLBAManager::split_mapping(
  Transaction &t,
  laddr_t laddr,
  paddr_t paddr,
  extent_len_t left_len,
  extent_len_t right_len,
  LogicalCachedExtent *lnextent,
  LogicalCachedExtent *rnextent)
{
  LOG_PREFIX(BtreeLBAManager::split_mapping);
  TRACET("splitting mapping: {}~{} to {}~{} and {}~{}",
    t, laddr, left_len + right_len, laddr, left_len,
    laddr + left_len, right_len);
  return _update_mapping(
    t,
    laddr,
    [left_len, right_len, &t](const lba_map_val_t &in) {
      lba_map_val_t ret = in;
      ceph_assert(left_len + right_len == in.len);
      LOG_PREFIX(BtreeLBAManager::split_mapping);
      TRACET("left val: {}, indirect: {}", t, ret, ret.pladdr.is_laddr());
      ret.len = left_len;
      return ret;
    },
    lnextent
  ).si_then([&t, laddr, left_len, right_len, this, rnextent, paddr](auto result) {
    auto &v = result.val;
    ceph_assert(v.index() == 1);
    auto &mapping = std::get<1>(v);
    auto left_val = mapping->get_map_val();
    auto pladdr = mapping->get_raw_val();
    LOG_PREFIX(BtreeLBAManager::split_mapping);
    TRACET("allocating for pladdr {}, indirect: {}",
      t, pladdr, std::get<1>(v)->is_indirect());

    if (is_shadow_laddr(laddr)) {
      assert(!result.has_shadow_entry);
      assert(paddr == P_ADDR_NULL);
      assert(pladdr.is_paddr());
      return alloc_shadow_extent(
        t,
        reset_shadow_mapping(laddr + left_len),
        right_len,
        (pladdr + left_len).get_paddr(),
        rnextent
      ).si_then([v=std::move(v)](auto ret) mutable {
        return split_mapping_result_t {
          LBAMappingRef(std::move(std::get<1>(v))),
          std::move(ret),
          std::nullopt
        };
      });
    } else {
      return _alloc_extent(
        t,
        laddr + left_len,
        right_len,
        pladdr + left_len,
        (pladdr.is_paddr() ? P_ADDR_NULL : paddr),
        left_val.refcount,
        rnextent
      ).si_then([has_shadow=result.has_shadow_entry, laddr,
	        v=std::move(v)](auto ret) mutable {
        return split_mapping_result_t{
	  LBAMappingRef(std::get<1>(v).release()),
	  std::move(ret),
	  has_shadow
	    ? (std::make_optional<laddr_t>(
	        map_shadow_laddr(laddr, shadow_mapping_t::COLD_MIRROR)))
	    : std::nullopt};
      });
    }
  },
  update_mapping_iertr::pass_further{},
  /* ENOENT in particular should be impossible */
  crimson::ct_error::assert_all{
    "Invalid error in BtreeLBAManager::split_mapping"
  });
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

BtreeLBAManager::ref_iertr::future<std::list<LBAManager::intermediate_mappings_t>>
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
    return btree.lower_bound(
      c, addr
    ).si_then([&btree, addr, len, c](auto iter) {
      return seastar::do_with(
	std::move(iter),
	(extent_len_t)0,
	std::list<intermediate_mappings_t>(),
	[&btree, addr, len, c](auto &iter, auto &scanned, auto &removed) {
	return trans_intr::repeat([&iter, &btree, &scanned,
				  addr, len, c, &removed] {
	  ceph_assert(!iter.is_end() &&
		      (iter.get_key() == addr + scanned ||
		       is_shadow_laddr(iter.get_key())));
	  auto val = iter.get_val();
	  ceph_assert(val.refcount >= 1);
	  val.refcount -= 1;
	  LOG_PREFIX(BtreeLBAManager::_decref_intermediate);
	  TRACET("decreased refcount of intermediate key {} -- {}",
	    c.trans,
	    iter.get_key(),
	    val);

	  auto after_decref = [val, &iter, &scanned, len, c, k=iter.get_key()]
			      (auto it, bool step_forward) {
	    if (!is_shadow_laddr(k)) {
	      scanned += val.len;
	    }
	    if (scanned == len) {
	      return base_iertr::make_ready_future<
		seastar::stop_iteration>(
		  seastar::stop_iteration::yes);
	    } else {
	      ceph_assert(scanned < len);
	      ceph_assert(!it.is_end());
	      if (step_forward) {
		return it.next(c).si_then([&iter](auto it) {
		  iter = std::move(it);
		  return seastar::stop_iteration::no;
		});
	      } else {
		iter = std::move(it);
		return base_iertr::make_ready_future<
		  seastar::stop_iteration>(seastar::stop_iteration::no);
	      }
	    }
	  };
	  if (!val.refcount) {
	    if (is_shadow_laddr(iter.get_key())) {
	      auto &mapping = removed.back();
	      assert(mapping.key == reset_shadow_mapping(iter.get_key())
		&& mapping.shadow_addr == P_ADDR_NULL);
	      mapping.shadow_addr = val.pladdr.get_paddr();
	    } else {
	      removed.emplace_back(
		iter.get_key(),
		val.pladdr.get_paddr(),
		val.len,
		P_ADDR_NULL);
	    }
	    return btree.remove(c, iter
	    ).si_then([f=std::move(after_decref)](auto it) {
	      return f(std::move(it), false);
	    });
	  } else {
	    return btree.update(c, iter, val, nullptr
	    ).si_then([f=std::move(after_decref)](auto it) {
	      return f(std::move(it), true);
	    });
	  }
	}).si_then([&removed] {
	  return std::move(removed);
	});
      });
    });
  });
}

BtreeLBAManager::update_refcount_ret
BtreeLBAManager::update_refcount(
  Transaction &t,
  laddr_t addr,
  int delta)
{
  LOG_PREFIX(BtreeLBAManager::update_refcount);
  TRACET("laddr={}, delta={}", t, addr, delta);
  assert(!is_shadow_laddr(addr));
  auto update_mapping = [this, &t, delta](laddr_t addr) {
    return _update_mapping(
      t,
      addr,
      [delta](const lba_map_val_t &in) {
	lba_map_val_t out = in;
	ceph_assert((int)out.refcount + delta >= 0);
	out.refcount += delta;
	return out;
      },
      nullptr);
  };
  return update_mapping(addr
  ).si_then([&t, addr, delta, FNAME, this,
	     update_mapping=std::move(update_mapping)](auto result)
	     -> update_refcount_ret {
    auto ret_val = (result.val.index() == 0)
      ? std::get<0>(result.val)
      : std::get<1>(result.val)->get_map_val();
    DEBUGT("laddr={}, delta={} done -- {}", t, addr, delta, ret_val);
    if (result.has_shadow_entry) {
      ceph_assert(ret_val.pladdr.is_paddr());
      return update_mapping(
        map_shadow_laddr(addr, shadow_mapping_t::COLD_MIRROR)
      ).si_then([ret_val](auto result) mutable {
	auto rval = (result.val.index() == 0)
	  ? std::get<0>(result.val)
	  : std::get<1>(result.val)->get_map_val();
	return ref_update_result_t{
	  ret_val.refcount,
	  ret_val.pladdr,
	  rval.pladdr.get_paddr(),
	  ret_val.len,
	  {}};
      });
    } else if (!ret_val.refcount && ret_val.pladdr.is_laddr()) {
      return _decref_intermediate(
	t,
	ret_val.pladdr.get_laddr(),
	ret_val.len
      ).si_then([ret_val](auto removed) {
	return ref_update_result_t{
	  ret_val.refcount,
	  ret_val.pladdr,
	  P_ADDR_NULL,
	  ret_val.len,
	  std::move(removed)};
      });
    }
    return ref_iertr::make_ready_future<ref_update_result_t>(
      ret_val.refcount,
      ret_val.pladdr,
      P_ADDR_NULL,
      ret_val.len,
      std::list<intermediate_mappings_t>());
  });
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f,
  LogicalCachedExtent* nextent)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, update_res_t>(
    cache,
    c,
    [f=std::move(f), c, addr, nextent, this](auto &btree) mutable {
      return btree.lower_bound(
	c, addr
      ).si_then([&btree, f=std::move(f), c, addr, nextent, this](auto iter)
		-> _update_mapping_ret {
	if (iter.is_end() || iter.get_key() != addr) {
	  LOG_PREFIX(BtreeLBAManager::_update_mapping);
	  ERRORT("laddr={} doesn't exist", c.trans, addr);
	  return crimson::ct_error::enoent::make();
	}

	auto fun = [&btree, f=std::move(f), c, iter, nextent](bool has_shadow) {
          auto ret = f(iter.get_val());
          if (ret.refcount == 0) {
            return btree.remove(
              c,
              iter
            ).si_then([ret, has_shadow](auto) {
              return update_res_t(ret, has_shadow);
            });
          } else {
            return btree.update(
              c,
              iter,
              ret,
	      nextent
            ).si_then([has_shadow, c](auto iter) {
              return update_res_t(iter.get_pin(c), has_shadow);
            });
          }
	};

	if (enable_shadow_entry &&
	    !is_shadow_laddr(addr)) {
	  return iter.next(c
	  ).si_then([fun=std::move(fun), iter](auto niter) {
	    bool has_shadow = !niter.is_end() &&
	      niter.get_key() == map_shadow_laddr(
	        iter.get_key(), shadow_mapping_t::COLD_MIRROR);
	    return fun(has_shadow);
	  });
	} else {
	  return fun(/*has_shadow=*/ false);
	}
      });
    });
}

}

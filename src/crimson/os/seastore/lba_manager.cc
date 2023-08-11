// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

SET_SUBSYS(seastore_lba);

namespace crimson::os::seastore {

LBAManager::update_mappings_ret
LBAManager::update_mappings(
  Transaction &t,
  const std::list<LogicalCachedExtentRef> &extents)
{
  LOG_PREFIX(LBAManager::update_mappings);
  std::vector<merged_mappings_t> merged_mappings;
  auto merged_mapping = merged_mappings_t();
  auto init_mapping = [&merged_mapping](const LogicalCachedExtentRef &extent) {
    merged_mapping.laddr = extent->get_laddr();
    merged_mapping.length = extent->get_length();
    merged_mapping.mappings.clear();
    merged_mapping.mappings.emplace_back(merged_mappings_t::mapping_t{
	extent->get_laddr(),
        extent->get_prior_paddr_and_reset(),
	extent->get_paddr(),
        extent->get_length()});
  };
  LogicalCachedExtent *last_extent = nullptr;

  int merged_count = 0;
  for (auto &extent : extents) {
    if (last_extent == nullptr) {
      last_extent = extent.get();
      init_mapping(extent);
      continue;
    }

    if (last_extent->get_type() == extent->get_type() &&
        merged_mapping.laddr + merged_mapping.length == extent->get_laddr()) {
      DEBUGT("merge {}~{} ", t, extent->get_laddr(), extent->get_length());
      merged_mapping.length += extent->get_length();
      merged_mapping.mappings.emplace_back(merged_mappings_t::mapping_t{
	  extent->get_laddr(),
          extent->get_prior_paddr_and_reset(),
	  extent->get_paddr(),
          extent->get_length()
	});
    } else {
      DEBUGT("push merged mappings {}~{} {} extens",
	     t, merged_mapping.laddr, merged_mapping.length,
	     merged_mapping.mappings.size());
      merged_count += merged_mapping.mappings.size();
      merged_mappings.emplace_back(std::move(merged_mapping));
      init_mapping(extent);
    }
    last_extent = extent.get();
  }
  if (!merged_mapping.mappings.empty()) {
    DEBUGT("push merged mappings {}~{} {} extens",
	   t, merged_mapping.laddr, merged_mapping.length,
	   merged_mapping.mappings.size());
    merged_count += merged_mapping.mappings.size();
    merged_mappings.emplace_back(std::move(merged_mapping));
  }
  ceph_assert(merged_count == extents.size());

  return seastar::do_with(
      std::move(merged_mappings),
      [this, &t](std::vector<merged_mappings_t> &ms) {
        return trans_intr::do_for_each(ms, [this, &t](const merged_mappings_t &m) {
          ceph_assert(!m.mappings.empty());
          if (m.mappings.size() == 1) {
            return update_mapping(
	      t,
	      m.laddr,
	      m.mappings.front().prior_paddr,
	      m.mappings.front().new_paddr,
	      nullptr);
          } else {
            return update_mappings(t, m);
          }
        });
      });
}

LBAManagerRef lba_manager::create_lba_manager(
  Cache &cache,
  bool enable_shadow_entry) {
  return LBAManagerRef(new btree::BtreeLBAManager(
    cache, enable_shadow_entry));
}

}

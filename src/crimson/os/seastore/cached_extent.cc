// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

namespace crimson::os::seastore {

#ifdef DEBUG_CACHED_EXTENT_REF

void intrusive_ptr_add_ref(CachedExtent *ptr)
{
  intrusive_ptr_add_ref(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
    logger().debug("intrusive_ptr_add_ref: {}", *ptr);
}

void intrusive_ptr_release(CachedExtent *ptr)
{
  logger().debug("intrusive_ptr_release: {}", *ptr);
  intrusive_ptr_release(
    static_cast<boost::intrusive_ref_counter<
    CachedExtent,
    boost::thread_unsafe_counter>*>(ptr));
}

#endif

bool is_backref_mapped_extent_node(const CachedExtentRef &extent) {
  return extent->is_logical()
    || is_lba_node(extent->get_type())
    || extent->get_type() == extent_types_t::TEST_BLOCK_PHYSICAL;
}

std::ostream &operator<<(std::ostream &out, CachedExtent::extent_state_t state)
{
  switch (state) {
  case CachedExtent::extent_state_t::INITIAL_WRITE_PENDING:
    return out << "INITIAL_WRITE_PENDING";
  case CachedExtent::extent_state_t::MUTATION_PENDING:
    return out << "MUTATION_PENDING";
  case CachedExtent::extent_state_t::CLEAN_PENDING:
    return out << "CLEAN_PENDING";
  case CachedExtent::extent_state_t::CLEAN:
    return out << "CLEAN";
  case CachedExtent::extent_state_t::DIRTY:
    return out << "DIRTY";
  case CachedExtent::extent_state_t::EXIST_CLEAN:
    return out << "EXIST_CLEAN";
  case CachedExtent::extent_state_t::EXIST_MUTATION_PENDING:
    return out << "EXIST_MUTATION_PENDING";
  case CachedExtent::extent_state_t::INVALID:
    return out << "INVALID";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const CachedExtent &ext)
{
  return ext.print(out);
}

CachedExtent::~CachedExtent()
{
  if (parent_index) {
    assert(is_linked());
    parent_index->erase(*this, true);
  }
}

void CachedExtent::erase_index_state(extent_types_t t, extent_len_t l) {
  if (parent_index) {
    parent_index->erase_state(t, l);
  }
  get_extent_delta(t) -= deltas.size();
  for (auto &d : deltas) {
    get_extent_delta_size(t) -= d.length();
  }
  get_extent_delta_details(t).adjust(deltas.size(), 0);
}

void CachedExtent::reset_buffer(const char* caller, int delta_size) {
  if (!support_reset_buffer()) {
    return;
  }
  assert(is_dirty());
  assert(is_fully_loaded());
  assert(!need_replay);
  assert(!deltas.empty());
  if (deltas.size() > delta_size) {
    return;
  }

  logger().debug("{} {} {}", __FUNCTION__, caller, *this);

  on_reset_buffer();
  ptr.reset();
  need_replay = true;
  if (parent_index) {
    parent_index->erase_state(get_type(), get_length(), true);
  }
}

void CachedExtent::maybe_replay_delta() {
  assert(support_reset_buffer());
  assert(is_dirty());
  assert(is_fully_loaded());
  assert(need_replay);

  logger().debug("{} {}", __FUNCTION__, *this);

  auto committed_crc = last_committed_crc;
  if (final_crc) {
    committed_crc = *final_crc;
  }
  for (auto &d : deltas) {
    apply_delta_and_adjust_crc_impl(d.record_base, d.delta);
  }
  ceph_assert(committed_crc == last_committed_crc);
  need_replay = false;
  final_crc.reset();
  if (parent_index) {
    parent_index->insert_state(get_type(), get_length());
  }
  on_maybe_replay_done();
}

CachedExtent* CachedExtent::get_transactional_view(Transaction &t) {
  return get_transactional_view(t.get_trans_id());
}

CachedExtent* CachedExtent::get_transactional_view(transaction_id_t tid) {
  auto it = mutation_pendings.find(tid, trans_spec_view_t::cmp_t());
  if (it != mutation_pendings.end()) {
    return (CachedExtent*)&(*it);
  } else {
    return this;
  }
}

std::ostream &operator<<(std::ostream &out, const parent_tracker_t &tracker) {
  return out << "parent_tracker=" << (void*)&tracker
	     << ", parent=" << (void*)tracker.get_parent().get();
}

std::ostream &ChildableCachedExtent::print_detail(std::ostream &out) const {
  if (parent_tracker) {
    out << *parent_tracker;
  } else {
    out << ", parent_tracker=" << (void*)nullptr;
  }
  if (is_latest()) {
    _print_detail(out);
  }
  return out;
}

std::ostream &LogicalCachedExtent::_print_detail(std::ostream &out) const
{
  out << ", laddr=" << laddr;
  return print_detail_l(out);
}

void child_pos_t::link_child(ChildableCachedExtent *c) {
  get_parent<FixedKVNode<laddr_t>>()->link_child(c, pos);
}

void CachedExtent::set_invalid(Transaction &t) {
  state = extent_state_t::INVALID;
  if (trans_view_hook.is_linked()) {
    trans_view_hook.unlink();
  }
  on_invalidated(t);
}

LogicalCachedExtent::~LogicalCachedExtent() {
  if (has_parent_tracker() && is_valid() && !is_pending()) {
    assert(get_parent_node());
    auto parent = get_parent_node<FixedKVNode<laddr_t>>();
    parent->replace_child(laddr, nullptr, this);
  }
}

void LogicalCachedExtent::on_replace_prior(Transaction &t) {
  assert(is_mutation_pending());
  take_prior_parent_tracker();
  assert(get_parent_node());
  auto parent = get_parent_node<FixedKVNode<laddr_t>>();
  parent->replace_child(laddr, this);
}

parent_tracker_t::~parent_tracker_t() {
  // this is parent's tracker, reset it
  auto &p = (FixedKVNode<laddr_t>&)*parent;
  if (p.my_tracker == this) {
    p.my_tracker = nullptr;
  }
}

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs)
{
  return out << "LBAMapping(" << rhs.get_key() << "~" << rhs.get_length()
	     << "->" << rhs.get_val();
}

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (const auto &i: rhs) {
    out << (first ? "" : ",") << *i;
    first = false;
  }
  return out << ']';
}

}

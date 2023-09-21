// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "osd/osd_op_util.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::osd {

class CommonPGPipeline {
protected:
  friend class InternalClientRequest;
  friend class SnapTrimEvent;
  friend class SnapTrimObjSubEvent;

  struct WaitForActive : OrderedExclusivePhaseT<WaitForActive> {
    static constexpr auto type_name = "CommonPGPipeline:::wait_for_active";
  } wait_for_active;
  struct RecoverMissing : OrderedExclusivePhaseT<RecoverMissing> {
    static constexpr auto type_name = "CommonPGPipeline::recover_missing";
  } recover_missing, recover_missing2;
  struct GetOBC : OrderedExclusivePhaseT<GetOBC> {
    static constexpr auto type_name = "CommonPGPipeline::get_obc";
  } get_obc;
  struct Process : OrderedExclusivePhaseT<Process> {
    static constexpr auto type_name = "CommonPGPipeline::process";
  } process;
};

} // namespace crimson::osd

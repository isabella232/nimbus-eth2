# beacon_chain
# Copyright (c) 2022 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [Defect].}

# This implements the pre-release proposal of the libp2p based light client sync
# protocol. See https://github.com/ethereum/consensus-specs/pull/2802

import
  # Status libraries
  stew/[bitops2, objects],
  chronos,
  # Beacon chain internals
  ../spec/datatypes/[phase0, altair, bellatrix],
  "."/[block_pools_types, blockchain_dag]

logScope: topics = "chaindag"

type ThrottledTask = ref object
  isAllowedToResume: LightClientTaskAllowedCallback
  resumeTick: Moment
  activeDur, suspendedDur: Duration
  measureOffset: Duration

proc newThrottledTask(
    isAllowedToResume: LightClientTaskAllowedCallback): ThrottledTask =
  ThrottledTask(
    isAllowedToResume: isAllowedToResume,
    resumeTick: Moment.now())

proc throttle(task: ThrottledTask): Future[void] {.async.} =
  if task.isAllowedToResume == nil:
    return

  const
    chunkDurLimit = chronos.milliseconds(50)
    suspendInterval = chronos.milliseconds(1750)

  let
    tick = Moment.now()
    chunkDur = tick - task.resumeTick
  if chunkDur < chunkDurLimit:
    return
  task.activeDur += chunkDur - task.measureOffset
  task.measureOffset.reset()

  await chronos.sleepAsync(suspendInterval) # yield at least once
  while not task.isAllowedToResume():
    await chronos.sleepAsync(suspendInterval)

  task.resumeTick = Moment.now()
  task.suspendedDur += task.resumeTick - tick

proc measure(task: ThrottledTask): tuple[activeDur, suspendedDur: Duration] =
  let
    tick = Moment.now()
    chunkDur = tick - task.resumeTick
    res = (
      activeDur: task.activeDur + chunkDur - task.measureOffset,
      suspendedDur: task.suspendedDur
    )
  task.activeDur.reset()
  task.suspendedDur.reset()
  task.measureOffset = chunkDur
  res

type
  HashedBeaconStateWithSyncCommittee =
    bellatrix.HashedBeaconState |
    altair.HashedBeaconState

  TrustedSignedBeaconBlockWithSyncAggregate =
    bellatrix.TrustedSignedBeaconBlock |
    altair.TrustedSignedBeaconBlock

template nextEpochBoundarySlot(slot: Slot): Slot =
  ## Compute the first possible epoch boundary state slot of a `Checkpoint`
  ## referring to a block at given slot.
  (slot + (SLOTS_PER_EPOCH - 1)).epoch.start_slot

proc updateExistingState(
    dag: ChainDAGRef, state: var ForkedHashedBeaconState, bsi: BlockSlotId,
    save: bool, cache: var StateCache): bool =
  ## Wrapper around `updateState` for states expected to exist.
  let ok = dag.updateState(state, bsi, save, cache)
  if not ok:
    error "State failed to load unexpectedly", bsi, tail = dag.tail.slot
    doAssert verifyFinalization notin dag.updateFlags
  ok

template asyncUpdateExistingState(
    dag: ChainDAGRef, state: var ForkedHashedBeaconState, bsiParam: BlockSlotId,
    save: bool, cache: var StateCache, periodicBody: untyped): bool =
  ## Wrapper around `updateExistingState` for states expected to exist.
  block:
    # Ideally, an async API for updating state would be available.
    # As a stopgap solution, update to the target state in multiple steps.
    let
      currentSlot = getStateField(state, slot)
      bsi = bsiParam
    if currentSlot > bsi.slot or bsi.slot - currentSlot > 2 * SLOTS_PER_EPOCH:
      let
        targetEpoch = bsi.slot.epoch
        startEpoch = targetEpoch - (targetEpoch mod EPOCHS_PER_STATE_SNAPSHOT)
      for epoch in startEpoch ..< targetEpoch:
        periodicBody
        let stepBsi = dag.atSlot(bsi.bid, epoch.start_slot).valueOr:
          continue
        discard dag.updateState(state, stepBsi, false, cache)
      periodicBody

    dag.updateExistingState(state, bsi, save, cache)

template withUpdatedExistingState(
    dag: ChainDAGRef, stateParam: var ForkedHashedBeaconState,
    bsiParam: BlockSlotId, okBody: untyped, failureBody: untyped): untyped =
  ## Wrapper around `withUpdatedState` for states expected to exist.
  block:
    let bsi = bsiParam
    dag.withUpdatedState(stateParam, bsiParam) do:
      okBody
    do:
      error "State failed to load unexpectedly", bsi, tail = dag.tail.slot
      doAssert verifyFinalization notin dag.updateFlags
      failureBody

template asyncWithUpdatedExistingState(
    dag: ChainDAGRef, stateParam: var ForkedHashedBeaconState,
    bsiParam: BlockSlotId, periodicBody: untyped,
    okBody: untyped, failureBody: untyped): untyped =
  ## Wrapper around `withUpdatedExistingState` with periodic callback.
  block:
    let bsi {.inject.} = bsiParam
    var cache {.inject.} = StateCache()
    if asyncUpdateExistingState(
        dag, stateParam, bsi, false, cache, periodicBody):
      template bid(): BlockId {.inject, used.} = bsi.bid
      template state(): ForkedHashedBeaconState {.inject, used.} = stateParam
      okBody
    else:
      failureBody

proc getExistingBlockIdAtSlot(dag: ChainDAGRef, slot: Slot): Opt[BlockSlotId] =
  ## Wrapper around `getBlockIdAtSlot` for blocks expected to exist.
  let bsi = dag.getBlockIdAtSlot(slot)
  if bsi.isErr:
    error "Block failed to load unexpectedly", slot, tail = dag.tail.slot
    doAssert verifyFinalization notin dag.updateFlags
  bsi

proc existingParent(dag: ChainDAGRef, bid: BlockId): Opt[BlockId] =
  ## Wrapper around `parent` for parents known to exist.
  let parent = dag.parent(bid)
  if parent.isErr:
    error "Parent failed to load unexpectedly", bid, tail = dag.tail.slot
    doAssert verifyFinalization notin dag.updateFlags
  parent

proc getExistingForkedBlock(
    dag: ChainDAGRef, bid: BlockId): Opt[ForkedTrustedSignedBeaconBlock] =
  ## Wrapper around `getForkedBlock` for blocks expected to exist.
  let bdata = dag.getForkedBlock(bid)
  if bdata.isErr:
    error "Block failed to load unexpectedly", bid, tail = dag.tail.slot
    doAssert verifyFinalization notin dag.updateFlags
  bdata

proc existingCurrentSyncCommitteeForPeriod(
    dag: ChainDAGRef,
    tmpState: var ForkedHashedBeaconState,
    period: SyncCommitteePeriod): Opt[SyncCommittee] =
  ## Wrapper around `currentSyncCommitteeForPeriod` for states known to exist.
  let syncCommittee = dag.currentSyncCommitteeForPeriod(tmpState, period)
  if syncCommittee.isErr:
    error "Current sync committee failed to load unexpectedly",
      period, tail = dag.tail.slot
    doAssert verifyFinalization notin dag.updateFlags
  syncCommittee

template syncCommitteeRoot(
    state: HashedBeaconStateWithSyncCommittee): Eth2Digest =
  ## Compute a root to uniquely identify `current_sync_committee` and
  ## `next_sync_committee`.
  withEth2Hash:
    h.update state.data.current_sync_committee.hash_tree_root().data
    h.update state.data.next_sync_committee.hash_tree_root().data

proc syncCommitteeRootForPeriod(
    dag: ChainDAGRef,
    tmpState: var ForkedHashedBeaconState,
    period: SyncCommitteePeriod): Opt[Eth2Digest] =
  ## Compute a root to uniquely identify `current_sync_committee` and
  ## `next_sync_committee` for a given sync committee period.
  ## For non-finalized periods, follow the chain as selected by fork choice.
  let lowSlot = max(dag.tail.slot, dag.cfg.ALTAIR_FORK_EPOCH.start_slot)
  if period < lowSlot.sync_committee_period:
    return err()
  let
    periodStartSlot = period.start_slot
    syncCommitteeSlot = max(periodStartSlot, lowSlot)
    bsi = ? dag.getExistingBlockIdAtSlot(syncCommitteeSlot)
  dag.withUpdatedExistingState(tmpState, bsi) do:
    withState(state):
      when stateFork >= BeaconStateFork.Altair:
        ok state.syncCommitteeRoot
      else: raiseAssert "Unreachable"
  do: err()

func initLightClientDataCollector*(
    serve: bool, importMode: LightClientDataImportMode, maxPeriods: uint64,
    onLCFinalityUpdateCb: OnLightClientFinalityUpdateCallback = nil,
    onLCOptimisticUpdateCb: OnLightClientOptimisticUpdateCallback = nil
): LightClientDataCollector =
  ## Initialize light client data collector.
  LightClientDataCollector(
    serve: serve,
    importMode: importMode,
    maxPeriods: maxPeriods,
    onLightClientFinalityUpdate: onLCFinalityUpdateCb,
    onLightClientOptimisticUpdate: onLCOptimisticUpdateCb)

func targetLightClientTailSlot(dag: ChainDAGRef): Slot =
  ## Earliest slot for which light client data is retained.
  let
    maxPeriods = dag.lcDataCollector.maxPeriods
    headPeriod = dag.head.slot.sync_committee_period
    lowSlot = max(dag.tail.slot, dag.cfg.ALTAIR_FORK_EPOCH.start_slot)
    tail = max(headPeriod + 1, maxPeriods.SyncCommitteePeriod) - maxPeriods
  max(tail.start_slot, lowSlot)

func handleUnexpectedLightClientError(dag: ChainDAGRef, buggedSlot: Slot) =
  ## If there is an unexpected error, adjust `tailSlot` to keep track of the
  ## section for which complete light client data is available, and to avoid
  ## failed lookups of cached light client data.
  doAssert verifyFinalization notin dag.updateFlags
  if buggedSlot >= dag.lcDataCollector.cache.tailSlot:
    dag.lcDataCollector.cache.tailSlot = buggedSlot + 1

proc initLightClientBootstrapForPeriod(
    dag: ChainDAGRef, period: SyncCommitteePeriod,
    task: ThrottledTask): Future[bool] {.async.} =
  ## Compute and cache `LightClientBootstrap` data for all finalized
  ## epoch boundary blocks within a given sync committee period.
  ## Return `true` iff no unexpected errors occurred.
  if not dag.isNextSyncCommitteeFinalized(period):
    return true

  discard task.measure()
  debug "Caching historic LC bootstrap data", period
  defer:
    debug "Historic LC bootstrap data cached", period,
      cacheDur = task.measure()

  let
    periodStartSlot = period.start_slot
    periodEndSlot = periodStartSlot + SLOTS_PER_SYNC_COMMITTEE_PERIOD - 1
    lowSlot = max(periodStartSlot, dag.targetLightClientTailSlot)
    highSlot = min(periodEndSlot, dag.finalizedHead.blck.slot)
    lowBoundarySlot = lowSlot.nextEpochBoundarySlot
    highBoundarySlot = highSlot.nextEpochBoundarySlot
  var
    allOk = true
    tmpState = assignClone(dag.headState)
    tmpCache: StateCache
    nextBoundarySlot = lowBoundarySlot
  while nextBoundarySlot <= highBoundarySlot:
    defer: nextBoundarySlot += SLOTS_PER_EPOCH
    await task.throttle()
    let
      bsi = dag.getExistingBlockIdAtSlot(nextBoundarySlot).valueOr:
        dag.handleUnexpectedLightClientError(nextBoundarySlot)
        allOk = false
        continue
      bid = bsi.bid
      boundarySlot = bid.slot.nextEpochBoundarySlot
    if boundarySlot == nextBoundarySlot and bid.slot >= lowSlot and
        not dag.lcDataCollector.cache.bootstrap.hasKey(bid.slot):
      var cached {.noinit.}: CachedLightClientBootstrap
      let ok =
        dag.asyncUpdateExistingState(
          tmpState[], bid.atSlot, save = false, tmpCache) do:
            await task.throttle
      if not ok:
        dag.handleUnexpectedLightClientError(bid.slot)
        allOk = false
        continue
      withState(tmpState[]):
        when stateFork >= BeaconStateFork.Altair:
          state.data.build_proof(
            altair.CURRENT_SYNC_COMMITTEE_INDEX,
            cached.current_sync_committee_branch)
        else: raiseAssert "Unreachable"
      dag.lcDataCollector.cache.bootstrap[bid.slot] = cached
  await task.throttle()
  return allOk

proc initLightClientUpdateForPeriod(
    dag: ChainDAGRef, period: SyncCommitteePeriod,
    task: ThrottledTask): Future[bool] {.async.} =
  ## Compute and cache the best `LightClientUpdate` within a given
  ## sync committee period up through the finalized head block.
  ## Return `true` iff no unexpected errors occurred.
  ## Non-finalized blocks are processed incrementally by other functions.
  if not dag.isNextSyncCommitteeFinalized(period):
    return true

  discard task.measure()
  debug "Computing best historic LC update", period
  proc logBest() =
    # Using a helper function reduces code size as the `defer` beneath is
    # replicated on every `return`, and the log statement allocates another
    # copy of the arguments on the stack for each instantiation (~1 MB stack!)
    debug "Best historic LC update computed",
      period, update = dag.lcDataCollector.cache.best.getOrDefault(period),
      computeDur = task.measure()
  defer: logBest()

  proc maxParticipantsBlock(
      dag: ChainDAGRef, highBid: BlockId, lowSlot: Slot,
      task: ThrottledTask
  ): Future[tuple[bid: Opt[BlockId], ok: bool]] {.async.} =
    ## Determine the earliest block with most sync committee signatures among
    ## ancestors of `highBid` with at least `lowSlot` as parent block slot.
    ## Return `err` if no block with `MIN_SYNC_COMMITTEE_PARTICIPANTS` exists.
    ## `bool` in result indicates whether no unexpected errors occurred.
    var
      maxParticipants = MIN_SYNC_COMMITTEE_PARTICIPANTS
      maxBid: Opt[BlockId]
      allOk = true
      bid = highBid
    while true:
      let parentBid = dag.parent(bid).valueOr:
        break
      if parentBid.slot < lowSlot:
        break
      await task.throttle()
      let
        bdata = dag.getExistingForkedBlock(bid).valueOr:
          dag.handleUnexpectedLightClientError(bid.slot)
          allOk = false
          break
        numParticipants =
          withBlck(bdata):
            when stateFork >= BeaconStateFork.Altair:
              countOnes(blck.message.body.sync_aggregate.sync_committee_bits)
            else: raiseAssert "Unreachable"
      if numParticipants >= maxParticipants:
        maxParticipants = numParticipants
        maxBid.ok bid
      bid = parentBid
    return (bid: maxBid, ok: allOk)

  func improveUsing(
      existing: var altair.LightClientUpdate,
      update: altair.LightClientUpdate,
      overrideExisting: bool) =
    ## Helper to apply a given `update` if it improves an `existing` update.
    if overrideExisting or is_better_update(update, existing):
      existing = update

  # Determine the block in the period with highest sync committee participation
  let
    periodStartSlot = period.start_slot
    periodEndSlot = periodStartSlot + SLOTS_PER_SYNC_COMMITTEE_PERIOD - 1
    lowSlot = max(periodStartSlot, dag.targetLightClientTailSlot)
    highSlot = min(periodEndSlot, dag.finalizedHead.blck.slot)
    isFullCoverage = (dag.finalizedHead.slot > periodEndSlot)
    highBsi = dag.getExistingBlockIdAtSlot(highSlot).valueOr:
      dag.handleUnexpectedLightClientError(highSlot)
      return false
    highBid = highBsi.bid
    maxParticipantsRes = await dag.maxParticipantsBlock(highBid, lowSlot, task)
    maxParticipantsBid = maxParticipantsRes.bid.valueOr:
      let isSealed = (isFullCoverage and maxParticipantsRes.ok)
      const update = default(altair.LightClientUpdate)
      dag.lcDataCollector.cache.best.mgetOrPut(period, update)
        .improveUsing(update, overrideExisting = isSealed)
      return maxParticipantsRes.ok

  # The block with highest participation may refer to a `finalized_checkpoint`
  # in a different sync committee period. If that is the case, search for a
  # later block with a `finalized_checkpoint` within the given sync committee
  # period, despite it having a lower sync committee participation
  var
    allOk = true
    tmpState = assignClone(dag.headState)
    signatureBid {.noinit.}, finalizedBid {.noinit.}: BlockId
  signatureBid.slot = FAR_FUTURE_SLOT
  finalizedBid.slot = FAR_FUTURE_SLOT
  while true:
    if signatureBid.slot == FAR_FUTURE_SLOT:
      signatureBid = maxParticipantsBid
    else:
      let
        nextLowSlot = signatureBid.slot + 1
        bidRes = await dag.maxParticipantsBlock(highBid, nextLowSlot, task)
      allOk = allOk and bidRes.ok
      signatureBid = bidRes.bid.valueOr:
        signatureBid = maxParticipantsBid
        break
    await task.throttle()
    let
      attestedBid = dag.existingParent(signatureBid).valueOr:
        dag.handleUnexpectedLightClientError(signatureBid.slot)
        allOk = false
        continue
      finalizedEpoch = block:
        dag.asyncWithUpdatedExistingState(tmpState[], attestedBid.atSlot) do:
          await task.throttle()
        do:
          withState(state):
            when stateFork >= BeaconStateFork.Altair:
              state.data.finalized_checkpoint.epoch
            else: raiseAssert "Unreachable"
        do:
          dag.handleUnexpectedLightClientError(attestedBid.slot)
          allOk = false
          continue
      finalizedSlot = finalizedEpoch.start_slot
      finalizedBsi = dag.getBlockIdAtSlot(finalizedSlot).valueOr:
        # Could happen if latest block through finalized slot is before DAG tail
        continue
    if finalizedBid.slot >= lowSlot:
      finalizedBid = finalizedBsi.bid
      break
    if signatureBid == maxParticipantsBid:
      finalizedBid = finalizedBsi.bid # For fallback `break` at start of loop

  # Save best light client data for given period
  await task.throttle()
  var update {.noinit.}: altair.LightClientUpdate
  let attestedBid = dag.existingParent(signatureBid).valueOr:
    dag.handleUnexpectedLightClientError(signatureBid.slot)
    return false
  dag.asyncWithUpdatedExistingState(tmpState[], attestedBid.atSlot) do:
    await task.throttle()
  do:
    let bdata = dag.getExistingForkedBlock(bid).valueOr:
      dag.handleUnexpectedLightClientError(bid.slot)
      return false
    withStateAndBlck(state, bdata):
      when stateFork >= BeaconStateFork.Altair:
        update.attested_header = blck.toBeaconBlockHeader
        update.next_sync_committee = state.data.next_sync_committee
        state.data.build_proof(
          altair.NEXT_SYNC_COMMITTEE_INDEX,
          update.next_sync_committee_branch)
        if finalizedBid.slot == FAR_FUTURE_SLOT:
          update.finality_branch.reset()
        else:
          state.data.build_proof(
            altair.FINALIZED_ROOT_INDEX,
            update.finality_branch)
      else: raiseAssert "Unreachable"
  do:
    dag.handleUnexpectedLightClientError(attestedBid.slot)
    return false
  if finalizedBid.slot == FAR_FUTURE_SLOT or finalizedBid.slot == GENESIS_SLOT:
    update.finalized_header.reset()
  else:
    let bdata = dag.getExistingForkedBlock(finalizedBid).valueOr:
      dag.handleUnexpectedLightClientError(finalizedBid.slot)
      return false
    withBlck(bdata):
      update.finalized_header = blck.toBeaconBlockHeader
  let bdata = dag.getExistingForkedBlock(signatureBid).valueOr:
    dag.handleUnexpectedLightClientError(signatureBid.slot)
    return false
  withBlck(bdata):
    when stateFork >= BeaconStateFork.Altair:
      update.sync_aggregate = blck.asSigned().message.body.sync_aggregate
    else: raiseAssert "Unreachable"
  update.signature_slot = signatureBid.slot

  let isSealed = (isFullCoverage and allOk)
  dag.lcDataCollector.cache.best.mgetOrPut(period, update)
    .improveUsing(update, overrideExisting = isSealed)
  await task.throttle()
  return allOk

proc initLightClientDataForPeriod(
    dag: ChainDAGRef, period: SyncCommitteePeriod,
    task: ThrottledTask): Future[bool] {.async.} =
  ## Import light client data for a given sync committee period.
  ## Return `true` iff no unexpected errors occurred.
  let
    res1 = await dag.initLightClientBootstrapForPeriod(period, task)
    res2 = await dag.initLightClientUpdateForPeriod(period, task)
  return res1 and res2

proc importHistoricLightClientData(dag: ChainDAGRef): Future[void] {.async.} =
  ## Import finalized historic light client data, moving `tailSlot` backwards.
  let task = newThrottledTask(dag.lcDataCollector.importTaskAllowed)

  logScope: lightClientDataMaxPeriods = dag.lcDataCollector.maxPeriods

  var
    anyWorkDone = false
    startTick: Moment
  while true:
    # Delay import of historic light client data until finality advanced
    # to avoid having to deal with potential forking in historic data
    let
      finalizedSlot = dag.finalizedHead.slot
      currentTailSlot = dag.lcDataCollector.cache.tailSlot
    if finalizedSlot < currentTailSlot:
      return

    # If target tail slot has been reached, work is done
    let targetTailSlot = dag.targetLightClientTailSlot
    if currentTailSlot <= targetTailSlot:
      if anyWorkDone:
        info "Historic LC data import complete",
          importDur = Moment.now() - startTick
      return
    if not anyWorkDone:
      anyWorkDone = true
      startTick = Moment.now()
      info "Importing historic LC data"

    # Import next period
    let
      currentPeriod = currentTailSlot.sync_committee_period
      period =
        if currentTailSlot > currentPeriod.start_slot:
          currentPeriod
        else:
          doAssert currentPeriod > 0, "currentTailSlot > targetTailSlot"
          currentPeriod - 1
      periodStartSlot = max(period.start_slot, targetTailSlot)
    if dag.lcDataCollector.cache.isSealed.getOrDefault(period):
      dag.lcDataCollector.cache.tailSlot = periodStartSlot
    else:
      let
        ok = await dag.initLightClientDataForPeriod(period, task)
        isFullCoverage = (dag.finalizedHead.slot >= (period + 1).start_slot)
      if isFullCoverage and ok:
        dag.lcDataCollector.cache.isSealed[period] = true
      if ok and dag.lcDataCollector.cache.tailSlot == currentTailSlot:
        dag.lcDataCollector.cache.tailSlot = periodStartSlot
      else:
        const retryAfterErrorInterval = chronos.seconds(30)
        await sleepAsync(retryAfterErrorInterval)

proc continueImportingHistoricLightClientData(dag: ChainDAGRef) {.gcsafe.} =
  ## Continue importing finalized historic light client data.
  ## This needs to be called whenever `finalized_checkpoint` changes.
  if dag.lcDataCollector.importMode != LightClientDataImportMode.Full:
    return
  if dag.lcDataCollector.importFut != nil:
    return

  dag.lcDataCollector.importFut = dag.importHistoricLightClientData()
  if dag.lcDataCollector.importFut.completed:
    dag.lcDataCollector.importFut = nil
    return

  proc handleFinishedImport(future: pointer) =
    dag.lcDataCollector.importFut = nil
    dag.continueImportingHistoricLightClientData()
  dag.lcDataCollector.importFut.addCallback(handleFinishedImport)

proc getExistingLightClientData(
    dag: ChainDAGRef,
    bid: BlockId): Opt[CachedLightClientData] =
  ## Fetch cached light client data about a given block.
  ## Data must be cached (`cacheLightClientData`) before calling this function.
  try:
    ok dag.lcDataCollector.cache.data[bid]
  except KeyError:
    error "LC data failed to load unexpectedly", bid, tail = dag.tail.slot
    doAssert verifyFinalization notin dag.updateFlags
    err()

proc cacheLightClientData(
    dag: ChainDAGRef,
    state: HashedBeaconStateWithSyncCommittee,
    bid: BlockId) =
  ## Cache data for a given block and its post-state to speed up creating future
  ## `LightClientUpdate` and `LightClientBootstrap` instances that refer to this
  ## block and state.
  var cached {.noinit.}: CachedLightClientData
  state.data.build_proof(
    altair.CURRENT_SYNC_COMMITTEE_INDEX,
    cached.current_sync_committee_branch)
  state.data.build_proof(
    altair.NEXT_SYNC_COMMITTEE_INDEX,
    cached.next_sync_committee_branch)
  cached.finalized_slot =
    state.data.finalized_checkpoint.epoch.start_slot
  state.data.build_proof(
    altair.FINALIZED_ROOT_INDEX,
    cached.finality_branch)
  if dag.lcDataCollector.cache.data.hasKeyOrPut(bid, cached):
    error "Redundant `cacheLightClientData` call", bid
    doAssert verifyFinalization notin dag.updateFlags

proc deleteLightClientData*(dag: ChainDAGRef, bid: BlockId) =
  ## Delete cached light client data for a given block. This needs to be called
  ## when a block becomes unreachable due to finalization of a different fork.
  if dag.lcDataCollector.importMode == LightClientDataImportMode.None:
    return

  dag.lcDataCollector.cache.data.del bid

template lazy_header(name: untyped): untyped {.dirty.} =
  ## `createLightClientUpdates` helper to lazily load a known block header.
  var
    `name _ ptr`: ptr[BeaconBlockHeader]
    `name _ ok` = true
  template `assign _ name`(target: var BeaconBlockHeader, bid: BlockId): bool =
    if `name _ ptr` != nil:
      target = `name _ ptr`[]
    elif `name _ ok`:
      let bdata = dag.getExistingForkedBlock(bid)
      if bdata.isErr:
        dag.handleUnexpectedLightClientError(bid.slot)
        `name _ ok` = false
      else:
        target = bdata.get.toBeaconBlockHeader()
        `name _ ptr` = addr target
    `name _ ok`

template lazy_data(name: untyped): untyped {.dirty.} =
  ## `createLightClientUpdates` helper to lazily load cached light client state.
  var
    `name` {.noinit.}: CachedLightClientData
    `name _ ok` = true
  `name`.finalized_slot = FAR_FUTURE_SLOT
  template `load _ name`(bid: BlockId): bool =
    if `name _ ok` and `name`.finalized_slot == FAR_FUTURE_SLOT:
      `name` = dag.getExistingLightClientData(bid).valueOr:
        dag.handleUnexpectedLightClientError(bid.slot)
        `name _ ok` = false
        default(typeof(`name`))
    `name _ ok`

template lazy_bid(name: untyped): untyped {.dirty.} =
  ## `createLightClientUpdates` helper to lazily load a block id.
  var
    `name` {.noinit.}: BlockId
    `name _ ok` = true
  `name`.slot = FAR_FUTURE_SLOT
  template `load _ name`(slot: Slot): bool =
    if `name _ ok` and `name`.slot == FAR_FUTURE_SLOT:
      let bsi = dag.getBlockIdAtSlot(slot)
      if bsi.isErr:
        # Could happen if latest block through given slot is before DAG tail
        `name _ ok` = false
      else:
        `name` = bsi.get.bid
    `name _ ok`

proc createLightClientUpdates(
    dag: ChainDAGRef,
    state: HashedBeaconStateWithSyncCommittee,
    blck: TrustedSignedBeaconBlockWithSyncAggregate,
    parent_bid: BlockId) =
  ## Create `LightClientUpdate` instances for a given block and its post-state,
  ## and keep track of best / latest ones. Data about the parent block's
  ## post-state must be cached (`cacheLightClientData`) before calling this.

  # Verify sync committee has sufficient participants
  template sync_aggregate(): auto = blck.asSigned().message.body.sync_aggregate
  template sync_committee_bits(): auto = sync_aggregate.sync_committee_bits
  let num_active_participants = countOnes(sync_committee_bits).uint64
  if num_active_participants < MIN_SYNC_COMMITTEE_PARTICIPANTS:
    return

  # Verify attested block (parent) is recent enough and that state is available
  template attested_bid(): auto = parent_bid
  let attested_slot = attested_bid.slot
  if attested_slot < dag.lcDataCollector.cache.tailSlot:
    return

  # Lazy variables to hold historic data
  lazy_header(attested_header)
  lazy_data(attested_data)
  lazy_bid(finalized_bid)
  lazy_header(finalized_header)

  # Update latest light client data
  template latest(): auto = dag.lcDataCollector.cache.latest
  var
    newFinality = false
    newOptimistic = false
  let
    signature_slot = blck.message.slot
    is_later =
      if attested_slot != latest.attested_header.slot:
        attested_slot > latest.attested_header.slot
      else:
        signature_slot > latest.signature_slot
  if is_later and load_attested_data(attested_bid) and
      latest.attested_header.assign_attested_header(attested_bid):
    let finalized_slot = attested_data.finalized_slot
    if finalized_slot == latest.finalized_header.slot:
      latest.finality_branch = attested_data.finality_branch
    elif finalized_slot == GENESIS_SLOT:
      latest.finalized_header.reset()
      latest.finality_branch = attested_data.finality_branch
    elif load_finalized_bid(finalized_slot) and
        latest.finalized_header.assign_finalized_header(finalized_bid):
      latest.finality_branch = attested_data.finality_branch
      newFinality = true
    else:
      latest.finalized_header.reset()
      latest.finality_branch.reset()
    latest.sync_aggregate = sync_aggregate
    latest.signature_slot = signature_slot
    newOptimistic = true

  # Track best light client data for current period
  let
    attested_period = attested_slot.sync_committee_period
    signature_period = signature_slot.sync_committee_period
  if attested_period == signature_period and load_attested_data(attested_bid):
    template next_sync_committee(): auto = state.data.next_sync_committee

    let isCommitteeFinalized = dag.isNextSyncCommitteeFinalized(attested_period)
    var best =
      if isCommitteeFinalized:
        dag.lcDataCollector.cache.best.getOrDefault(attested_period)
      else:
        let key = (attested_period, state.syncCommitteeRoot)
        dag.lcDataCollector.cache.pendingBest.getOrDefault(key)

    let
      finalized_slot = attested_data.finalized_slot
      meta = LightClientUpdateMetadata(
        attested_slot: attested_slot,
        finalized_slot: finalized_slot,
        signature_slot: signature_slot,
        has_sync_committee: true,
        has_finality: load_finalized_bid(finalized_slot),
        num_active_participants: num_active_participants)
      is_better = is_better_data(meta, best.toMeta)
    if is_better and best.attested_header.assign_attested_header(attested_bid):
      best.next_sync_committee = next_sync_committee
      best.next_sync_committee_branch = attested_data.next_sync_committee_branch
      if finalized_slot == best.finalized_header.slot:
        best.finality_branch = attested_data.finality_branch
      elif finalized_slot == GENESIS_SLOT:
        best.finalized_header.reset()
        best.finality_branch = attested_data.finality_branch
      elif meta.has_finality and
          best.finalized_header.assign_finalized_header(finalized_bid):
        best.finality_branch = attested_data.finality_branch
      else:
        best.finalized_header.reset()
        best.finality_branch.reset()
      best.sync_aggregate = sync_aggregate
      best.signature_slot = signature_slot

      if isCommitteeFinalized:
        dag.lcDataCollector.cache.best[attested_period] = best
        debug "Best LC update improved", period = attested_period, update = best
      else:
        let key = (attested_period, state.syncCommitteeRoot)
        dag.lcDataCollector.cache.pendingBest[key] = best
        debug "Best LC update improved", period = key, update = best

  if newFinality and dag.lcDataCollector.onLightClientFinalityUpdate != nil:
    dag.lcDataCollector.onLightClientFinalityUpdate(latest)
  if newOptimistic and dag.lcDataCollector.onLightClientOptimisticUpdate != nil:
    dag.lcDataCollector.onLightClientOptimisticUpdate(latest.toOptimistic)

proc initLightClientData*(dag: ChainDAGRef) =
  ## Initialize cached light client data with the initial head state.
  ## Historic data is imported in the background as soon as finality
  ## advances past the initial head slot.
  if dag.lcDataCollector.importMode == LightClientDataImportMode.None:
    return

  # Prune non-finalized data
  let firstNonFinalizedPeriod = dag.firstNonFinalizedPeriod

  var sealPeriodsToDelete: seq[SyncCommitteePeriod]
  for period in dag.lcDataCollector.cache.isSealed.keys:
    if period >= firstNonFinalizedPeriod:
      sealPeriodsToDelete.add period
  for period in sealPeriodsToDelete:
    dag.lcDataCollector.cache.isSealed.del period

  var periodsToDelete: seq[SyncCommitteePeriod]
  for period in dag.lcDataCollector.cache.best.keys:
    if period >= firstNonFinalizedPeriod:
      periodsToDelete.add period
  for period in periodsToDelete:
    dag.lcDataCollector.cache.best.del period

  # Initialize tail slot
  let altairStartSlot = dag.cfg.ALTAIR_FORK_EPOCH.start_slot
  dag.lcDataCollector.cache.tailSlot = max(dag.head.slot, altairStartSlot)

  # Import head state
  if dag.head.slot >= dag.lcDataCollector.cache.tailSlot:
    withState(dag.headState):
      when stateFork >= BeaconStateFork.Altair:
        dag.cacheLightClientData(state, dag.head.bid)
      else: raiseAssert "Unreachable" # `tailSlot` cannot be before Altair

proc processNewBlockForLightClient*(
    dag: ChainDAGRef,
    state: ForkedHashedBeaconState,
    signedBlock: ForkyTrustedSignedBeaconBlock,
    parentBid: BlockId) =
  ## Update light client data with information from a new block.
  if dag.lcDataCollector.importMode == LightClientDataImportMode.None:
    return
  if signedBlock.message.slot < dag.lcDataCollector.cache.tailSlot:
    return

  when signedBlock is bellatrix.TrustedSignedBeaconBlock:
    dag.cacheLightClientData(state.bellatrixData, signedBlock.toBlockId())
    dag.createLightClientUpdates(state.bellatrixData, signedBlock, parentBid)
  elif signedBlock is altair.TrustedSignedBeaconBlock:
    dag.cacheLightClientData(state.altairData, signedBlock.toBlockId())
    dag.createLightClientUpdates(state.altairData, signedBlock, parentBid)
  elif signedBlock is phase0.TrustedSignedBeaconBlock:
    raiseAssert "Unreachable" # `tailSlot` cannot be before Altair
  else:
    {.error: "Unreachable".}

proc processHeadChangeForLightClient*(dag: ChainDAGRef) =
  ## Update light client data to account for a new head block.
  ## Note that `dag.finalizedHead` is not yet updated when this is called.
  if dag.lcDataCollector.importMode == LightClientDataImportMode.None:
    return
  if dag.head.slot < dag.lcDataCollector.cache.tailSlot:
    return

  # Update `best` from `pendingBest` to ensure light client data
  # only refers to sync committees as selected by fork choice
  let headPeriod = dag.head.slot.sync_committee_period
  if not dag.isNextSyncCommitteeFinalized(headPeriod):
    let
      tailPeriod = dag.lcDataCollector.cache.tailSlot.sync_committee_period
      lowPeriod = max(dag.firstNonFinalizedPeriod, tailPeriod)
    if headPeriod > lowPeriod:
      var tmpState = assignClone(dag.headState)
      for period in lowPeriod ..< headPeriod:
        let
          syncCommitteeRoot =
            dag.syncCommitteeRootForPeriod(tmpState[], period).valueOr:
              dag.handleUnexpectedLightClientError(period.start_slot)
              continue
          key = (period, syncCommitteeRoot)
        dag.lcDataCollector.cache.best[period] =
          dag.lcDataCollector.cache.pendingBest.getOrDefault(key)
    withState(dag.headState): # Common case separate to avoid `tmpState` copy
      when stateFork >= BeaconStateFork.Altair:
        let key = (headPeriod, state.syncCommitteeRoot)
        dag.lcDataCollector.cache.best[headPeriod] =
          dag.lcDataCollector.cache.pendingBest.getOrDefault(key)
      else: raiseAssert "Unreachable" # `tailSlot` cannot be before Altair

proc processFinalizationForLightClient*(
    dag: ChainDAGRef, oldFinalizedHead: BlockSlot) =
  ## Prune cached data that is no longer useful for creating future
  ## `LightClientUpdate` and `LightClientBootstrap` instances.
  ## This needs to be called whenever `finalized_checkpoint` changes.
  if dag.lcDataCollector.importMode == LightClientDataImportMode.None:
    return
  let finalizedSlot = dag.finalizedHead.slot
  if finalizedSlot < dag.lcDataCollector.cache.tailSlot:
    return

  # Cache `LightClientBootstrap` for newly finalized epoch boundary blocks
  let
    firstNewSlot = oldFinalizedHead.slot + 1
    lowSlot = max(firstNewSlot, dag.lcDataCollector.cache.tailSlot)
  var boundarySlot = finalizedSlot
  while boundarySlot >= lowSlot:
    let
      bsi = dag.getExistingBlockIdAtSlot(boundarySlot).valueOr:
        dag.handleUnexpectedLightClientError(boundarySlot)
        break
      bid = bsi.bid
    if bid.slot >= lowSlot:
      let cached = dag.getExistingLightClientData(bid).valueOr:
        dag.handleUnexpectedLightClientError(bid.slot)
        break
      dag.lcDataCollector.cache.bootstrap[bid.slot] =
        CachedLightClientBootstrap(
          current_sync_committee_branch:
            cached.current_sync_committee_branch)
    boundarySlot = bid.slot.nextEpochBoundarySlot
    if boundarySlot < SLOTS_PER_EPOCH:
      break
    boundarySlot -= SLOTS_PER_EPOCH

  # Seal sync committee periods for which data can no longer improve further
  let
    oldFinalizedPeriod = oldFinalizedHead.slot.sync_committee_period
    newFinalizedPeriod = dag.finalizedHead.slot.sync_committee_period
  if newFinalizedPeriod > oldFinalizedPeriod:
    for period in countdown(newFinalizedPeriod - 1, oldFinalizedPeriod):
      if dag.lcDataCollector.cache.tailSlot > period.start_slot:
        break
      debug "Best LC update sealed",
        period, update = dag.lcDataCollector.cache.best.getOrDefault(period)
      dag.lcDataCollector.cache.isSealed[period] = true

  # Prune light client data that is no longer referrable by future updates
  var bidsToDelete: seq[BlockId]
  for bid, data in dag.lcDataCollector.cache.data:
    if bid.slot >= dag.finalizedHead.blck.slot:
      continue
    bidsToDelete.add bid
  for bid in bidsToDelete:
    dag.lcDataCollector.cache.data.del bid

  # Prune seal tracking data that is no longer relevant
  let
    targetTailSlot = dag.targetLightClientTailSlot
    targetTailPeriod = targetTailSlot.sync_committee_period
  var sealPeriodsToDelete: seq[SyncCommitteePeriod]
  for period in dag.lcDataCollector.cache.isSealed.keys:
    if period < targetTailPeriod:
      sealPeriodsToDelete.add period
  for period in sealPeriodsToDelete:
    dag.lcDataCollector.cache.isSealed.del period

  # Prune bootstrap data that is no longer relevant
  var slotsToDelete: seq[Slot]
  for slot in dag.lcDataCollector.cache.bootstrap.keys:
    if slot < targetTailSlot:
      slotsToDelete.add slot
  for slot in slotsToDelete:
    dag.lcDataCollector.cache.bootstrap.del slot

  # Prune best `LightClientUpdate` that are no longer relevant
  var periodsToDelete: seq[SyncCommitteePeriod]
  for period in dag.lcDataCollector.cache.best.keys:
    if period < targetTailPeriod:
      periodsToDelete.add period
  for period in periodsToDelete:
    dag.lcDataCollector.cache.best.del period

  # Prune best `LightClientUpdate` referring to non-finalized sync committees
  # that are no longer relevant, i.e., orphaned or too old
  let firstNonFinalizedPeriod = dag.firstNonFinalizedPeriod
  var keysToDelete: seq[(SyncCommitteePeriod, Eth2Digest)]
  for (period, committeeRoot) in dag.lcDataCollector.cache.pendingBest.keys:
    if period < firstNonFinalizedPeriod:
      keysToDelete.add (period, committeeRoot)
  for key in keysToDelete:
    dag.lcDataCollector.cache.pendingBest.del key

  # Continue importing historic light client data
  dag.continueImportingHistoricLightClientData()

proc getLightClientBootstrap*(
    dag: ChainDAGRef,
    blockRoot: Eth2Digest): Opt[altair.LightClientBootstrap] =
  if not dag.lcDataCollector.serve:
    return err()

  let bdata = dag.getForkedBlock(blockRoot).valueOr:
    debug "LC bootstrap unavailable: Block not found", blockRoot
    return err()

  withBlck(bdata):
    let slot = blck.message.slot
    when stateFork >= BeaconStateFork.Altair:
      if slot < dag.lcDataCollector.cache.tailSlot:
        debug "LC bootstrap unavailable: Block too old", slot
        return err()
      if slot > dag.finalizedHead.blck.slot:
        debug "LC bootstrap unavailable: Not finalized", blockRoot
        return err()
      var cached = dag.lcDataCollector.cache.bootstrap.getOrDefault(slot)
      if cached.current_sync_committee_branch.isZeroMemory:
        debug "LC bootstrap unavailable: Data not cached", slot
        return err()

      let period = slot.sync_committee_period
      var tmpState = assignClone(dag.headState)
      var bootstrap {.noinit.}: altair.LightClientBootstrap
      bootstrap.header =
        blck.toBeaconBlockHeader
      bootstrap.current_sync_committee =
        ? dag.existingCurrentSyncCommitteeForPeriod(tmpState[], period)
      bootstrap.current_sync_committee_branch =
        cached.current_sync_committee_branch
      return ok bootstrap
    else:
      debug "LC bootstrap unavailable: Block before Altair", slot
      return err()

proc getLightClientUpdateForPeriod*(
    dag: ChainDAGRef,
    period: SyncCommitteePeriod): Option[altair.LightClientUpdate] =
  if not dag.lcDataCollector.serve:
    return

  result = some(dag.lcDataCollector.cache.best.getOrDefault(period))
  let numParticipants = countOnes(result.get.sync_aggregate.sync_committee_bits)
  if numParticipants < MIN_SYNC_COMMITTEE_PARTICIPANTS:
    result.reset()

proc getLightClientFinalityUpdate*(
    dag: ChainDAGRef): Option[altair.LightClientFinalityUpdate] =
  if not dag.lcDataCollector.serve:
    return

  result = some(dag.lcDataCollector.cache.latest)
  let numParticipants = countOnes(result.get.sync_aggregate.sync_committee_bits)
  if numParticipants < MIN_SYNC_COMMITTEE_PARTICIPANTS:
    result.reset()

proc getLightClientOptimisticUpdate*(
    dag: ChainDAGRef): Option[altair.LightClientOptimisticUpdate] =
  if not dag.lcDataCollector.serve:
    return

  result = some(dag.lcDataCollector.cache.latest.toOptimistic)
  let numParticipants = countOnes(result.get.sync_aggregate.sync_committee_bits)
  if numParticipants < MIN_SYNC_COMMITTEE_PARTICIPANTS:
    result.reset()

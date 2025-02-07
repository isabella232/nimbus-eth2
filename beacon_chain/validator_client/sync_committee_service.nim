import
  std/[sets, sequtils],
  chronicles,
  "."/[common, api, block_service],
  ../spec/datatypes/[phase0, altair, bellatrix],
  ../spec/eth2_apis/rest_types

type
  ContributionItem* = object
    aggregator_index: uint64
    selection_proof: ValidatorSig
    validator: AttachedValidator
    subcommitteeIdx: SyncSubcommitteeIndex

proc serveSyncCommitteeMessage*(service: SyncCommitteeServiceRef,
                                slot: Slot,
                                beaconBlockRoot: Eth2Digest,
                                duty: SyncDutyAndProof): Future[bool] {.async.} =
  let
    vc = service.client
    fork = vc.forkAtEpoch(slot.epoch)
    genesisValidatorsRoot = vc.beaconGenesis.genesis_validators_root

    vindex = duty.data.validator_index
    subcommitteeIdx = getSubcommitteeIndex(duty.data.validator_sync_committee_index)

    validator =
      block:
        let res = vc.getValidator(duty.data.pubkey)
        if res.isNone():
          return false
        res.get()

    message =
      block:
        let res = await signSyncCommitteeMessage(validator, fork,
                                                 genesisValidatorsRoot,
                                                 slot, beaconBlockRoot)
        if res.isErr():
          error "Unable to sign committee message using remote signer",
                validator = shortLog(validator), slot = slot,
                block_root = shortLog(beaconBlockRoot)
          return
        res.get()

  debug "Sending sync committee message", message = shortLog(message),
        validator = shortLog(validator), validator_index = vindex,
        delay = vc.getDelay(message.slot.sync_committee_message_deadline())

  let res =
    try:
      await vc.submitPoolSyncCommitteeSignature(message)
    except ValidatorApiError:
      error "Unable to publish sync committee message",
            message = shortLog(message),
            validator = shortLog(validator),
            validator_index = vindex
      return false
    except CatchableError as exc:
      error "Unexpected error occurred while publishing sync committee message",
            message = shortLog(message),
            validator = shortLog(validator),
            validator_index = vindex,
            err_name = exc.name, err_msg = exc.msg
      return false

  let delay = vc.getDelay(message.slot.sync_committee_message_deadline())
  if res:
    notice "Sync committee message published",
           message = shortLog(message),
           validator = shortLog(validator),
           validator_index = vindex,
           delay = delay
  else:
    warn "Sync committee message was not accepted by beacon node",
         message = shortLog(message),
         validator = shortLog(validator),
         validator_index = vindex, delay = delay

  return res

proc produceAndPublishSyncCommitteeMessages(service: SyncCommitteeServiceRef,
                                            slot: Slot,
                                            beaconBlockRoot: Eth2Digest,
                                            duties: seq[SyncDutyAndProof]) {.async.} =
  let vc = service.client

  let pendingSyncCommitteeMessages =
    block:
      var res: seq[Future[bool]]
      for duty in duties:
        debug "Serving sync message duty", duty = duty.data, epoch = slot.epoch()
        res.add(service.serveSyncCommitteeMessage(slot,
                                                  beaconBlockRoot,
                                                  duty))
      res

  let statistics =
    block:
      var errored, succeed, failed = 0
      try:
        await allFutures(pendingSyncCommitteeMessages)
      except CancelledError as exc:
        for fut in pendingSyncCommitteeMessages:
          if not(fut.finished()):
            fut.cancel()
        await allFutures(pendingSyncCommitteeMessages)
        raise exc

      for future in pendingSyncCommitteeMessages:
        if future.done():
          if future.read():
            inc(succeed)
          else:
            inc(failed)
        else:
          inc(errored)
      (succeed, errored, failed)

  let delay = vc.getDelay(slot.attestation_deadline())
  debug "Sync committee message statistics", total = len(pendingSyncCommitteeMessages),
        succeed = statistics[0], failed_to_deliver = statistics[1],
        not_accepted = statistics[2], delay = delay, slot = slot,
        duties_count = len(duties)

proc serveContributionAndProof*(service: SyncCommitteeServiceRef,
                                proof: ContributionAndProof,
                                validator: AttachedValidator): Future[bool] {.async.} =
  let
    vc = service.client
    slot = proof.contribution.slot
    validatorIdx = validator.index.get()
    genesisRoot = vc.beaconGenesis.genesis_validators_root
    fork = vc.forkAtEpoch(slot.epoch)
    signedProof = (ref SignedContributionAndProof)(
      message: proof)

  let signature =
    block:
      let res = await validator.sign(signedProof, fork, genesisRoot)
      if res.isErr():
        error "Unable to sign sync committee contribution using remote signer",
              validator = shortLog(validator),
              contribution = shortLog(proof.contribution),
              error_msg = res.error()
        return false
      res.get()

  debug "Sending sync contribution",
        contribution = shortLog(signedProof.message.contribution),
        validator = shortLog(validator), validator_index = validatorIdx,
        delay = vc.getDelay(slot.sync_contribution_deadline())

  let restSignedProof = RestSignedContributionAndProof.init(
    signedProof.message, signedProof.signature)

  let res =
    try:
      await vc.publishContributionAndProofs(@[restSignedProof])
    except ValidatorApiError as err:
      error "Unable to publish sync contribution",
            contribution = shortLog(signedProof.message.contribution),
            validator = shortLog(validator),
            validator_index = validatorIdx,
            err_msg = err.msg
      false
    except CatchableError as err:
      error "Unexpected error occurred while publishing sync contribution",
            contribution = shortLog(signedProof.message.contribution),
            validator = shortLog(validator),
            err_name = err.name, err_msg = err.msg
      false

  if res:
    notice "Sync contribution published",
           validator = shortLog(validator),
           validator_index = validatorIdx
  else:
    warn "Sync contribution was not accepted by beacon node",
         contribution = shortLog(signedProof.message.contribution),
         validator = shortLog(validator),
         validator_index = validatorIdx
  return res

proc produceAndPublishContributions(service: SyncCommitteeServiceRef,
                                    slot: Slot,
                                    beaconBlockRoot: Eth2Digest,
                                    duties: seq[SyncDutyAndProof]) {.async.} =
  let
    vc = service.client
    contributionItems =
      block:
        var res: seq[ContributionItem]
        for duty in duties:
          let validator = vc.attachedValidators.getValidator(duty.data.pubkey)
          if not isNil(validator):
            if duty.slotSig.isSome:
              template slotSignature: auto = duty.slotSig.get
              if is_sync_committee_aggregator(slotSignature):
                res.add(ContributionItem(
                  aggregator_index: uint64(duty.data.validator_index),
                  selection_proof: slotSignature,
                  validator: validator,
                  subcommitteeIdx: getSubcommitteeIndex(
                    duty.data.validator_sync_committee_index)
                ))
        res

  if len(contributionItems) > 0:
    let pendingAggregates =
      block:
        var res: seq[Future[bool]]
        for item in contributionItems:
          let aggContribution =
            try:
              await vc.produceSyncCommitteeContribution(slot,
                                                        item.subcommitteeIdx,
                                                        beaconBlockRoot)
            except ValidatorApiError:
              error "Unable to get sync message contribution data", slot = slot,
                    beaconBlockRoot = shortLog(beaconBlockRoot)
              return
            except CatchableError as exc:
              error "Unexpected error occurred while getting sync message contribution",
                    slot = slot, beaconBlockRoot = shortLog(beaconBlockRoot),
                    err_name = exc.name, err_msg = exc.msg
              return

          let proof = ContributionAndProof(
            aggregator_index: item.aggregator_index,
            contribution: aggContribution,
            selection_proof: item.selection_proof
          )
          res.add(service.serveContributionAndProof(proof, item.validator))
        res

    let statistics =
      block:
        var errored, succeed, failed = 0
        try:
          await allFutures(pendingAggregates)
        except CancelledError as err:
          for fut in pendingAggregates:
            if not(fut.finished()):
              fut.cancel()
          await allFutures(pendingAggregates)
          raise err

        for future in pendingAggregates:
          if future.done():
            if future.read():
              inc(succeed)
            else:
              inc(failed)
          else:
            inc(errored)
        (succeed, errored, failed)

    let delay = vc.getDelay(slot.aggregate_deadline())
    debug "Sync message contribution statistics", total = len(pendingAggregates),
          succeed = statistics[0], failed_to_deliver = statistics[1],
          not_accepted = statistics[2], delay = delay, slot = slot

  else:
    debug "No contribution and proofs scheduled for slot", slot = slot

proc publishSyncMessagesAndContributions(service: SyncCommitteeServiceRef,
                                         slot: Slot,
                                         duties: seq[SyncDutyAndProof]) {.async.} =
  let
    vc = service.client
    startTime = Moment.now()

  try:
    let timeout = syncCommitteeMessageSlotOffset
    await vc.waitForBlockPublished(slot).wait(nanoseconds(timeout.nanoseconds))
    let dur = Moment.now() - startTime
    debug "Block proposal awaited", slot = slot, duration = dur
  except AsyncTimeoutError:
    let dur = Moment.now() - startTime
    debug "Block was not produced in time", slot = slot, duration = dur

  block:
    let delay = vc.getDelay(slot.sync_committee_message_deadline())
    debug "Producing sync committee messages", delay = delay, slot = slot,
          duties_count = len(duties)
  let beaconBlockRoot =
    block:
      try:
        let res = await vc.getHeadBlockRoot()
        res.root
      except CatchableError as exc:
        error "Could not request sync message block root to sign"
        return

  try:
    await service.produceAndPublishSyncCommitteeMessages(slot,
                                                         beaconBlockRoot,
                                                         duties)
  except ValidatorApiError:
    error "Unable to proceed sync committee messages", slot = slot,
           duties_count = len(duties)
    return
  except CatchableError as exc:
    error "Unexpected error while producing sync committee messages",
          slot = slot,
          duties_count = len(duties),
          err_name = exc.name, err_msg = exc.msg
    return

  let contributionTime =
    # chronos.Duration subtraction cannot return a negative value; in such
    # case it will return `ZeroDuration`.
    vc.beaconClock.durationToNextSlot() - OneThirdDuration
  if contributionTime != ZeroDuration:
    await sleepAsync(contributionTime)

  block:
    let delay = vc.getDelay(slot.sync_contribution_deadline())
    debug "Producing contribution and proofs", delay = delay
  await service.produceAndPublishContributions(slot, beaconBlockRoot, duties)

proc spawnSyncCommitteeTasks(service: SyncCommitteeServiceRef, slot: Slot) =
  let vc = service.client

  removeOldSyncPeriodDuties(vc, slot)
  let duties = vc.getSyncCommitteeDutiesForSlot(slot + 1)

  asyncSpawn service.publishSyncMessagesAndContributions(slot, duties)

proc mainLoop(service: SyncCommitteeServiceRef) {.async.} =
  let vc = service.client
  service.state = ServiceState.Running
  try:
    while true:
      let sleepTime =
        syncCommitteeMessageSlotOffset + vc.beaconClock.durationToNextSlot()

      let sres = vc.getCurrentSlot()
      if sres.isSome():
        let currentSlot = sres.get()
        service.spawnSyncCommitteeTasks(currentSlot)
      await sleepAsync(sleepTime)
  except CatchableError as exc:
    warn "Service crashed with unexpected error", err_name = exc.name,
         err_msg = exc.msg

proc init*(t: typedesc[SyncCommitteeServiceRef],
           vc: ValidatorClientRef): Future[SyncCommitteeServiceRef] {.async.} =
  debug "Initializing service"
  var res = SyncCommitteeServiceRef(client: vc,
                                    state: ServiceState.Initialized)
  return res

proc start*(service: SyncCommitteeServiceRef) =
  service.lifeFut = mainLoop(service)

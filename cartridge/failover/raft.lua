local membership = require('membership')
local checks = require('checks')
local topology = require('cartridge.topology')

local vars = require('cartridge.vars').new('cartridge.failover')

vars:new('raft_term')
vars:new('leader_uuid')
vars:new('raft_trigger')

--- Generate appointments according to raft status.
-- Used in 'raft' failover mode.
-- @function get_appointments
local function get_appointments(topology_cfg)
    checks('table')
    local replicasets = assert(topology_cfg.replicasets)

    local appointments = {}

    for replicaset_uuid, _ in pairs(replicasets) do
        local leaders = topology.get_leaders_order(
            topology_cfg, replicaset_uuid
        )
        if replicaset_uuid == vars.replicaset_uuid then
            local my_leader_id = box.info.election.leader
            local my_leader = box.info.replication[my_leader_id]
            if my_leader ~= nil then
                appointments[replicaset_uuid] = my_leader.uuid
                goto next_rs
            end
        end

        local last_leader
        local last_term = 0
        for _, instance_uuid in ipairs(leaders) do
            local server = topology_cfg.servers[instance_uuid]
            local member = membership.get_member(server.uri)

            if member ~= nil
            and member.payload.raft_leader ~= nil
            and member.payload.raft_term >= last_term
            then
                last_leader = member.payload.raft_leader
                last_term = member.payload.raft_term
            end
        end
        appointments[replicaset_uuid] = last_leader
        ::next_rs::
    end

    appointments[vars.replicaset_uuid] = vars.leader_uuid
    return appointments
end

local function on_election_trigger()
    local election = box.info.election
    vars.raft_term = election.term

    local leader = box.info.replication[election.leader] or {}

    if vars.leader_uuid ~= leader.uuid then
        vars.cache.is_leader = vars.leader_uuid == vars.instance_uuid
        vars.leader_uuid = leader.uuid
        membership.set_payload('raft_leader', vars.leader_uuid)
    end
    membership.set_payload('raft_term', vars.raft_term)
end

local function cfg(failover_cfg)
    checks('table')
    box.cfg{
        -- The instance is set to candidate, so it may become leader itself
        -- as well as vote for other instances.
        --
        -- Alternative: set one of instances to `voter`, so that it
        -- never becomes a leader but still votes for one of its peers and helps
        -- it reach election quorum.
        election_mode = os.getenv('TARANTOOL_ELECTION_MODE') or 'candidate',
        -- Quorum for both synchronous transactions and
        -- leader election votes.
        replication_synchro_quorum = failover_cfg.raft_quorum,
        -- Synchronous replication timeout. The transaction will be
        -- rolled back if no quorum is achieved during timeout.
        replication_synchro_timeout = failover_cfg.synchro_timeout,
        -- Heartbeat timeout. A leader is considered dead if it doesn't
        -- send heartbeats for 4 * replication_timeout.
        -- Once the leader is dead, remaining instances start a new election round.
        replication_timeout = failover_cfg.replication_timeout,
        -- Timeout between elections. Needed to restart elections when no leader
        -- emerges soon enough. Equals 4 * replication_timeout
        election_timeout = failover_cfg.election_timeout,
    }

    local election = box.info.election

    vars.raft_term = election.term

    local leader = box.info.replication[election.leader] or {}
    vars.leader_uuid = leader.uuid

    membership.set_payload('raft_term', vars.raft_term)
    membership.set_payload('raft_leader', vars.leader_uuid)

    if vars.raft_trigger == nil then
        vars.raft_trigger = box.ctl.on_election(on_election_trigger)
    end
end

-- disable raft if it was enabled
local function disable()
    if vars.raft_trigger ~= nil then
        box.ctl.on_election(nil, vars.raft_trigger)
        vars.raft_trigger = nil
    end
    box.cfg{ election_mode = 'off' }
    vars.raft_term = nil
    vars.leader_uuid = nil
end

return {
    cfg = cfg,
    disable = disable,
    get_appointments = get_appointments,
}

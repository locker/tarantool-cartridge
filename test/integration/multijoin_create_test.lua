local fio = require('fio')
local t = require('luatest')
local g = t.group()

local helpers = require('test.helper')

g.before_all(function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        use_vshard = false,
        server_command = helpers.entrypoint('srv_basic'),
        cookie = require('digest').urandom(6):hex(),
        replicasets = {
            {
                alias = 'firstling',
                uuid = helpers.uuid('a'),
                roles = {},
                servers = {
                    {instance_uuid = helpers.uuid('a', 'a', 1)},
                },
            }
        },
    })

    g.servers = {}
    for i = 1, 2 do
        local http_port = 8090 + i
        local advertise_port = 13310 + i
        local alias = string.format('twin%d', i)

        g.servers[i] = helpers.Server:new({
            alias = alias,
            command = helpers.entrypoint('srv_basic'),
            workdir = fio.pathjoin(g.cluster.datadir, alias),
            cluster_cookie = g.cluster.cookie,
            http_port = http_port,
            advertise_port = advertise_port,
            instance_uuid = helpers.uuid('b', 'b', i),
            replicaset_uuid = helpers.uuid('b'),
        })
    end

    for _, server in pairs(g.servers) do
        server:start()
    end
    g.cluster:start()
end)

g.after_all(function()
    for _, server in pairs(g.servers or {}) do
        server:stop()
    end
    g.cluster:stop()

    fio.rmtree(g.cluster.datadir)
    g.cluster = nil
    g.servers = nil
end)

g.test_patch_topology = function()
    g.cluster.main_server:eval([[
        local args = ...

        local errors = require('errors')
        local cartridge = require('cartridge')
        local membership = require('membership')

        errors.assert('ProbeError', membership.probe_uri(args.twin1.advertise_uri))
        errors.assert('ProbeError', membership.probe_uri(args.twin2.advertise_uri))

        local topology, err = cartridge.admin_edit_topology({
            replicasets = {
                {
                    uuid = args.twin1.replicaset_uuid,
                    join_servers = {
                        {
                            uuid = args.twin1.instance_uuid,
                            uri = args.twin1.advertise_uri,
                        },
                        {
                            uuid = args.twin2.instance_uuid,
                            uri = args.twin2.advertise_uri,
                        }
                    }
                }
            }
        })
        assert(topology, tostring(err))
    ]], {{
        twin1 = {
            advertise_uri = g.servers[1].advertise_uri,
            instance_uuid = g.servers[1].instance_uuid,
            replicaset_uuid = g.servers[1].replicaset_uuid,
        },
        twin2 = {
            advertise_uri = g.servers[2].advertise_uri,
            instance_uuid = g.servers[2].instance_uuid,
            replicaset_uuid = g.servers[2].replicaset_uuid,
        }},
    })

    g.cluster:retrying({}, function() g.servers[1]:connect_net_box() end)
    g.cluster:retrying({}, function() g.servers[2]:connect_net_box() end)
    g.cluster:wait_until_healthy()
end

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
                servers = {{
                    http_port = 8081,
                    advertise_port = 13301,
                    instance_uuid = helpers.uuid('a', 'a', 1)
                }},
            }
        },
    })

    g.servers = {}
    for i = 2, 3 do
        local http_port = 8080 + i
        local advertise_port = 13300 + i
        local alias = string.format('twin%d', i)

        g.servers[i] = helpers.Server:new({
            alias = alias,
            command = helpers.entrypoint('srv_basic'),
            workdir = fio.pathjoin(g.cluster.datadir, alias),
            cluster_cookie = g.cluster.cookie,
            http_port = http_port,
            advertise_port = advertise_port,
            replicaset_uuid = helpers.uuid('a'),
            instance_uuid = helpers.uuid('a', 'a', i),
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
    -- t.skip("Fails due to https://github.com/tarantool/tarantool/issues/4527")

    g.cluster.main_server:graphql({
        query = [[mutation(
            $replicasets: [EditReplicasetInput!]
        ){
            cluster {
                edit_topology(replicasets:$replicasets) { servers { uri } }
            }
        }]],
        variables = {
            replicasets = {{
                uuid = g.cluster.main_server.replicaset_uuid,
                join_servers = {{
                    uri = g.servers[2].advertise_uri,
                    uuid = g.servers[2].instance_uuid,
                }, {
                    uri = g.servers[3].advertise_uri,
                    uuid = g.servers[3].instance_uuid,
                }}
            }}
        }
    })

    g.cluster:retrying({}, function() g.servers[2]:connect_net_box() end)
    g.cluster:retrying({}, function() g.servers[3]:connect_net_box() end)
    g.cluster:wait_until_healthy()
end

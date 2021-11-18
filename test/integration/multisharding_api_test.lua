local fio = require('fio')
local t = require('luatest')
local g = t.group()

local helpers = require('test.helper')

g.before_all(function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_multisharding'),
        cookie = require('digest').urandom(6):hex(),
        replicasets = {
            {
                alias = 'hot-master',
                uuid = helpers.uuid('a'),
                roles = {'vshard-storage', 'vshard-router'},
                vshard_group = 'hot',
                servers = {{
                    http_port = 8081,
                    advertise_port = 13301,
                    instance_uuid = helpers.uuid('a', 'a', 1)
                }},
            }
        },
    })

    g.cluster:start()
end)

g.after_all(function()
    g.cluster:stop()

    fio.rmtree(g.cluster.datadir)
    g.cluster = nil
end)

g.test_query = function()
    local ret = g.cluster.main_server:graphql({
        query = [[{
            replicasets(uuid: "aaaaaaaa-0000-0000-0000-000000000000") {
                uuid
                vshard_group
            }
        }]]
    })

    t.assert_equals(ret['data']['replicasets'][1], {
        uuid = helpers.uuid('a'),
        vshard_group = 'hot',
    })
end

g.test_mutations = function()
    t.assert_error_msg_contains(
        "replicasets[aaaaaaaa-0000-0000-0000-000000000000].vshard_group" ..
        " can't be modified",
        function()
            return g.cluster.main_server:graphql({
                query = [[mutation {
                    edit_replicaset(
                        uuid: "aaaaaaaa-0000-0000-0000-000000000000"
                        vshard_group: "cold"
                    )
                }]]
            })
        end
    )

    -- Do nothing and check it doesn't raise an error
    g.cluster.main_server:graphql({
        query = [[mutation {
            edit_replicaset(
                uuid: "aaaaaaaa-0000-0000-0000-000000000000"
            )
        }]]
    })
end

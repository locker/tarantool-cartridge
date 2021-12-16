local fio = require('fio')
local t = require('luatest')
local g = t.group()
local helpers = require('test.helper')

g.before_each(function()
    local kuka = 'akuk'
    g.server = helpers.Server:new({
        workdir = fio.tempdir(),
        command = helpers.entrypoint('srv_basic'),
        alias = 'a',
        cluster_cookie = kuka,
        replicaset_uuid = helpers.uuid('a'),
        instance_uuid = helpers.uuid('a', 'a', 1),
        http_port = 8081,
        advertise_port = 13301,
    })

    g.server2 = helpers.Server:new({
        workdir = fio.tempdir(),
        command = helpers.entrypoint('srv_basic'),
        alias = 'b',
        cluster_cookie = kuka,
        replicaset_uuid = helpers.uuid('b'),
        instance_uuid = helpers.uuid('b', 'b', 1),
        http_port = 8082,
        advertise_port = 13302,
    })
    
    g.server3 = helpers.Server:new({
        workdir = fio.tempdir(),
        command = helpers.entrypoint('srv_basic'),
        alias = 'c',
        cluster_cookie = kuka,
        replicaset_uuid = helpers.uuid('c'),
        instance_uuid = helpers.uuid('c', 'c', 1),
        http_port = 8083,
        advertise_port = 13303,
    })
    
    g.server4 = helpers.Server:new({
        workdir = fio.tempdir(),
        command = helpers.entrypoint('srv_basic'),
        alias = 'd',
        cluster_cookie = kuka,
        replicaset_uuid = helpers.uuid('d'),
        instance_uuid = helpers.uuid('d', 'd', 1),
        http_port = 8084,
        advertise_port = 13304,
    })
    g.server:start()
    g.server2:start()
    g.server3:start()
    g.server4:start()

    t.helpers.retrying({timeout = 10}, function()
        g.server:graphql({query = '{ servers { uri } }'})
        g.server2:graphql({query = '{ servers { uri } }'})
        g.server3:graphql({query = '{ servers { uri } }'})
        g.server4:graphql({query = '{ servers { uri } }'})
    end)
end)

g.after_each(function()
    g.server:stop()
    g.server2:stop()
    g.server3:stop()
    g.server4:stop()
    fio.rmtree(g.server.workdir)
    fio.rmtree(g.server2.workdir)
    fio.rmtree(g.server3.workdir)
    fio.rmtree(g.server4.workdir)
end)

function g.test_edit_zone()
    local rc, err = pcall(g.server.exec, g.server, 
        function () 
            local cartridge = require('cartridge')
            cartridge.admin_edit_topology({
            replicasets = {
                {alias = 'r1', join_servers = { {uri = 'localhost:13301'},  {uri = 'localhost:13302'} }},
                {alias = 'r2', join_servers = { {uri = 'localhost:13303'},  {uri = 'localhost:13304'} }},
            }})
            return true
        end
    )
    
    t.assert_equals(rc, true, err)

    local rc, err = pcall(g.server.exec, g.server, 
        function (uuid1, uuid2)
            local cartridge = require('cartridge')
            cartridge.admin_edit_topology({
                servers = {
                    {uuid = uuid1, zone = 'zone-1'},
                    {uuid = uuid2, zone = 'zone-3'},
               },
            })
            return true
        end,
        {g.server.instance_uuid,
        g.server3.instance_uuid}
    )

    t.assert_equals(rc, true, err)
end

local fio = require('fio')
local socket = require('socket')

local t = require('luatest')
local g = t.group()

local helpers = require('test.helper')

g.before_all(function()
    g.tmpdir = fio.tempdir()
    g.server = helpers.Server:new({
        alias = 'server',
        workdir = g.tmpdir,
        command = helpers.entrypoint('srv_basic'),
        advertise_port = 13301,
        http_port = 8080,
        cluster_cookie = 'super-cluster-cookie',
        env = {
            TARANTOOL_CONSOLE_SOCK = fio.pathjoin(g.tmpdir, '/foo.sock'),
            NOTIFY_SOCKET = fio.pathjoin(g.tmpdir, '/notify.sock')
        },
    })
    local notify_socket = socket('AF_UNIX', 'SOCK_DGRAM', 0)
    t.assert(notify_socket, 'Can not create socket')
    t.assert(notify_socket:bind('unix/', g.server.env.NOTIFY_SOCKET), notify_socket:error())
    g.server:start()
    t.helpers.retrying({}, function()
        t.assert(notify_socket:readable(1), "Socket isn't readable")
        t.assert_str_matches(notify_socket:recv(), 'READY=1')
    end)
    notify_socket:close()
end)

g.after_all(function()
    g.server:stop()
    fio.rmtree(g.tmpdir)
    g.server = nil
end)

g.test_console_sock = function()
    local s = socket.tcp_connect('unix/', fio.pathjoin(g.tmpdir, 'foo.sock'))
    t.assert(s)
    local greeting = s:read('\n')
    t.assert(greeting)
    t.assert_str_matches(greeting:strip(), 'Tarantool.*%(Lua console%)')
    s:close()
end

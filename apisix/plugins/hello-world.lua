package.path = package.path .. ";/Applications/ZeroBraneStudio.app/Contents/ZeroBraneStudio/lualibs/?/?.lua;/Applications/ZeroBraneStudio.app/Contents/ZeroBraneStudio/lualibs/?.lua"
package.cpath = package.cpath .. ";/Applications/ZeroBraneStudio.app/Contents/ZeroBraneStudio/bin/?.dylib;/Applications/ZeroBraneStudio.app/Contents/ZeroBraneStudio/bin/clibs/?.dylib"

local core = require("apisix.core")
local log = require("apisix.core.log")
local inspect = require("apisix.plugins.inspect")

local schema = {
    type = "object",
    properties = {
    },
}

local plugin_name = "hello-world"

local _M = {
    version = 0.1,
    priority = 2600,
    name = plugin_name,
    schema = schema,
}

function _M.init_worker()
    --require('mobdebug').start("127.0.0.1", 8172)
    --log.warn("init_worker")
end

function _M.check_schema(conf)
    --require('mobdebug').start("127.0.0.1", 8172)
    --log.warn("check_schema")
end

function _M.rewrite(conf, ctx)
    --require('mobdebug').start("127.0.0.1", 8172)
    log.warn("rewrite")
end

function _M.access(conf, ctx)
    --require('mobdebug').checkcount = 1
    --require('mobdebug').start("127.0.0.1", 8172)
    log.warn("access")
end

--function _M.balancer(conf, ctx)
--    --require('mobdebug').start("127.0.0.1", 8172)
--    log.warn("balancer")
--end

function _M.header_filter(conf, ctx)
    --require('mobdebug').start("127.0.0.1", 8172)
    --log.warn("header_filter")
end

function _M.body_filter(conf, ctx)
    --require('mobdebug').start("127.0.0.1", 8172)
    --log.warn("body_filter")
end

function _M.log(conf, ctx)
    --log.warn("log")
end

return _M
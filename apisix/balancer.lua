--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
local healthcheck
local require     = require
local discovery   = require("apisix.discovery.init").discovery
local balancer    = require("ngx.balancer")
local core        = require("apisix.core")
local ipairs      = ipairs
local tostring    = tostring
local set_more_tries   = balancer.set_more_tries
local get_last_failure = balancer.get_last_failure
local set_timeouts     = balancer.set_timeouts


local module_name = "balancer"
local pickers = {
    roundrobin = require("apisix.balancer.roundrobin"),
    chash = require("apisix.balancer.chash"),
}


local lrucache_server_picker = core.lrucache.new({
    ttl = 300, count = 256
})
local lrucache_checker = core.lrucache.new({
    ttl = 300, count = 256
})
local lrucache_addr = core.lrucache.new({
    ttl = 300, count = 1024 * 4
})


local _M = {
    version = 0.2,
    name = module_name,
}


local function fetch_health_nodes(upstream, checker)
    local nodes = upstream.nodes
    if not checker then
        --没有checker的话，直接从upstream.nodes中剥取ip+port和权重返回
        local new_nodes = core.table.new(0, #nodes)
        for _, node in ipairs(nodes) do
            -- TODO filter with metadata
            new_nodes[node.host .. ":" .. node.port] = node.weight
        end
        return new_nodes
    end

    local host = upstream.checks and upstream.checks.active and upstream.checks.active.host
    local port = upstream.checks and upstream.checks.active and upstream.checks.active.port
    local up_nodes = core.table.new(0, #nodes)
    for _, node in ipairs(nodes) do
        local ok = checker:get_target_status(node.host, port or node.port, host)
        if ok then
            -- TODO filter with metadata
            up_nodes[node.host .. ":" .. node.port] = node.weight
        end
    end

    if core.table.nkeys(up_nodes) == 0 then
        core.log.warn("all upstream nodes is unhealth, use default")
        for _, node in ipairs(nodes) do
            up_nodes[node.host .. ":" .. node.port] = node.weight
        end
    end

    return up_nodes
end


local function create_checker(upstream, healthcheck_parent)
    if healthcheck == nil then
        healthcheck = require("resty.healthcheck")
    end
    local checker = healthcheck.new({
        name = "upstream#" .. healthcheck_parent.key,
        shm_name = "upstream-healthcheck",
        checks = upstream.checks,
    })

    local host = upstream.checks and upstream.checks.active and upstream.checks.active.host
    local port = upstream.checks and upstream.checks.active and upstream.checks.active.port
    for _, node in ipairs(upstream.nodes) do
        local ok, err = checker:add_target(node.host, port or node.port, host)
        if not ok then
            core.log.error("failed to add new health check target: ", node.host, ":",
                    port or node.port, " err: ", err)
        end
    end

    if upstream.parent then
        --在
        core.table.insert(upstream.parent.clean_handlers, function ()
            core.log.info("try to release checker: ", tostring(checker))
            checker:stop()
        end)

    else
        core.table.insert(healthcheck_parent.clean_handlers, function ()
            core.log.info("try to release checker: ", tostring(checker))
            checker:stop()
        end)
    end

    core.log.info("create new checker: ", tostring(checker))
    return checker
end


local function fetch_healthchecker(upstream, healthcheck_parent, version)
    if not upstream.checks then
        return
    end

    if upstream.checker then
        return
    end

    local checker = lrucache_checker(upstream, version,
                                     create_checker, upstream,
                                     healthcheck_parent)
    return checker
end


local function create_server_picker(upstream, checker)
    --picker即负载均衡算法，这里根据upstream.type来选择相应的负载均衡算法
    local picker = pickers[upstream.type]
    if picker then
        --获取健康的节点
        local up_nodes = fetch_health_nodes(upstream, checker)
        core.log.info("upstream nodes: ", core.json.delay_encode(up_nodes))

        return picker.new(up_nodes, upstream)
    end

    return nil, "invalid balancer type: " .. upstream.type, 0
end


local function parse_addr(addr)
    local host, port, err = core.utils.parse_addr(addr)
    return {host = host, port = port}, err
end


local function pick_server(route, ctx)
    core.log.info("route: ", core.json.delay_encode(route, true))
    core.log.info("ctx: ", core.json.delay_encode(ctx, true))

    --获取上游配置
    local up_conf = ctx.upstream_conf
    --service_name应该是跟用户自定义dns解析服务器有关的配置
    --如果存在的话，怎使用用户的自定义dns解析服务
    --todo 在哪里设置service_name未找到
    if up_conf.service_name then
        if not discovery then
            return nil, "discovery is uninitialized"
        end
        up_conf.nodes = discovery.nodes(up_conf.service_name)
    end

    local nodes_count = up_conf.nodes and #up_conf.nodes or 0
    if nodes_count == 0 then
        return nil, "no valid upstream node"
    end

    if up_conf.timeout then
        local timeout = up_conf.timeout
        local ok, err = set_timeouts(timeout.connect, timeout.send,
                                     timeout.read)
        if not ok then
            core.log.error("could not set upstream timeouts: ", err)
        end
    end

    if nodes_count == 1 then
        local node = up_conf.nodes[1]
        ctx.balancer_ip = node.host
        ctx.balancer_port = node.port
        return node
    end

    --upstream_healthcheck_parent在upstream#set_directly中有设置，本质上是启动的时候调用的set_by_route函数
    local healthcheck_parent = ctx.upstream_healthcheck_parent
    local version = ctx.upstream_version
    local key = ctx.upstream_key
    local checker = fetch_healthchecker(up_conf, healthcheck_parent, version)
    ctx.up_checker = checker

    ctx.balancer_try_count = (ctx.balancer_try_count or 0) + 1
    if checker and ctx.balancer_try_count > 1 then
        local state, code = get_last_failure()
        local host = up_conf.checks and up_conf.checks.active and up_conf.checks.active.host
        local port = up_conf.checks and up_conf.checks.active and up_conf.checks.active.port
        if state == "failed" then
            if code == 504 then
                checker:report_timeout(ctx.balancer_ip, port or ctx.balancer_port, host)
            else
                checker:report_tcp_failure(ctx.balancer_ip, port or ctx.balancer_port, host)
            end
        else
            checker:report_http_status(ctx.balancer_ip, port or ctx.balancer_port, host, code)
        end
    end

    if ctx.balancer_try_count == 1 then
        local retries = up_conf.retries
        if not retries or retries < 0 then
            retries = #up_conf.nodes - 1
        end
        if retries > 0 then
            --设置连接失败后的重试次数
            set_more_tries(retries)
        end
    end

    if checker then
        version = version .. "#" .. checker.status_ver
    end

    local server_picker = lrucache_server_picker(key, version,
                            create_server_picker, up_conf, checker)
    if not server_picker then
        return nil, "failed to fetch server picker"
    end

    --调用resty.roundrobin库的#find函数来选取上游
    local server, err = server_picker.get(ctx)
    if not server then
        err = err or "no valid upstream node"
        return nil, "failed to find valid upstream server, " .. err
    end

    --解析上游为固定格式
    local res, err = lrucache_addr(server, nil, parse_addr, server)
    ctx.balancer_ip = res.host
    ctx.balancer_port = res.port
    -- core.log.info("cached balancer peer host: ", host, ":", port)
    if err then
        core.log.error("failed to parse server addr: ", server, " err: ", err)
        return core.response.exit(502)
    end

    return res
end


-- for test
_M.pick_server = pick_server


function _M.run(route, ctx)
    --选取上游节点
    local server, err = pick_server(route, ctx)
    if not server then
        core.log.error("failed to pick server: ", err)
        return core.response.exit(502)
    end

    core.log.info("proxy request to ", server.host, ":", server.port)
    --设置使用的后端服务器，必须是 IP 地址，不能是域名；
    --这里主要用的是ngx.balancer库的相关函数
    local ok, err = balancer.set_current_peer(server.host, server.port)


    local inspect = require("apisix.inspect")
    core.log.warn("ok: " .. inspect(ok))


    if not ok then
        core.log.error("failed to set server peer [", server.host, ":",
                       server.port, "] err: ", err)
        return core.response.exit(502)
    end

    ctx.proxy_passed = true
end


function _M.init_worker()
end

return _M

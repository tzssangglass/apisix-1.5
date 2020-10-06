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
local require = require
local router = require("resty.radixtree")
local core = require("apisix.core")
local plugin = require("apisix.plugin")
local ipairs = ipairs
local type = type
local error = error
local loadstring = loadstring
local user_routes
local cached_version


local _M = {version = 0.2}


    local uri_routes = {}
    local uri_router
local function create_radixtree_router(routes)
    routes = routes or {}

    --api_routes是指通过_M.api()预先定义好(硬编码)的一些api接口，主要是plugins中定义的
    local api_routes = plugin.api_routes()
    core.table.clear(uri_routes)

    for _, route in ipairs(api_routes) do
        if type(route) == "table" then
            core.table.insert(uri_routes, {
                paths = route.uris or route.uri,
                methods = route.methods,
                handler = route.handler,
            })
        end
    end

    --routes即etcd中用户存储的真实业务route
    for _, route in ipairs(routes) do
        if type(route) == "table" then
            local filter_fun, err
            if route.value.filter_func then
                --loadstring(string [,chunkname])
                --loadstring( )函数最典型的用法就是用来执行外部代码
                --这里用loadstring加载route配置中filter_func的函数
                --loadstring函数的返回值是一个function
                filter_fun, err = loadstring(
                                        "return " .. route.value.filter_func,
                                        "router#" .. route.value.id)

                if not filter_fun then
                    core.log.error("failed to load filter function: ", err,
                                   " route id: ", route.value.id)
                    goto CONTINUE
                end

                filter_fun = filter_fun()
            end

            core.log.info("insert uri route: ",
                          core.json.delay_encode(route.value))
            core.table.insert(uri_routes, {
                paths = route.value.uris or route.value.uri,
                methods = route.value.methods,
                priority = route.value.priority,
                hosts = route.value.hosts or route.value.host,
                remote_addrs = route.value.remote_addrs
                               or route.value.remote_addr,
                vars = route.value.vars,
                filter_fun = filter_fun,
                handler = function (api_ctx)
                    api_ctx.matched_params = nil
                    api_ctx.matched_route = route
                end
            })

            ::CONTINUE::
        end
    end

    core.log.info("route items: ", core.json.delay_encode(uri_routes, true))
    uri_router = router.new(uri_routes)
end


    local match_opts = {}
function _M.match(api_ctx)
    --cached_version维护的是本地router的版本号
    --user_routes.conf_version维护的是监听etcd的router的版本变化
    --任何对etcd的router的修改，都会导致user_routes.conf_version的变化
    if not cached_version or cached_version ~= user_routes.conf_version then
        --根据etcd的值重建路由树
        create_radixtree_router(user_routes.values)

        --更新缓存版本号
        cached_version = user_routes.conf_version
    end

    if not uri_router then
        core.log.error("failed to fetch valid `uri` router: ")
        return true
    end

    core.table.clear(match_opts)
    match_opts.method = api_ctx.var.request_method
    match_opts.host = api_ctx.var.host
    match_opts.remote_addr = api_ctx.var.remote_addr
    match_opts.vars = api_ctx.var

    --进行路由匹配
    --这里的api_ctx参数，是传给dispatch成功之后，回调uri_router的handler的参数
    --即上面的api_ctx.matched_route = route
    local ok = uri_router:dispatch(api_ctx.var.uri, match_opts, api_ctx)
    if not ok then
        core.log.info("not find any matched route")
        return true
    end

    return true
end


function _M.routes()
    if not user_routes then
        return nil, nil
    end
    return user_routes.values, user_routes.conf_version
end


function _M.init_worker(filter)
    local err
    --在etcd中新建一条配置，key是"/routes"
    --user_routes获取的实际上是config_etcd.new在调用ngx_timer_at(0, _automatic_fetch, obj)之前的数据
    --因为并没有值显示地接受ngx_timer_at(0, _automatic_fetch, obj)的返回结果
    user_routes, err = core.config.new("/routes", {
            automatic = true,
            item_schema = core.schema.route,
            filter = filter,
        })
    if not user_routes then
        error("failed to create etcd instance for fetching /routes : " .. err)
    end
end


return _M

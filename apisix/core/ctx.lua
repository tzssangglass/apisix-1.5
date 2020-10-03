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
local log          = require("apisix.core.log")
local tablepool    = require("tablepool")
local get_var      = require("resty.ngxvar").fetch
local get_request  = require("resty.ngxvar").request
local ck           = require "resty.cookie"
local setmetatable = setmetatable
local ffi          = require("ffi")
local C            = ffi.C
local sub_str      = string.sub
local rawset       = rawset
local ngx_var      = ngx.var
local re_gsub      = ngx.re.gsub
local type         = type
local error        = error


ffi.cdef[[
int memcmp(const void *s1, const void *s2, size_t n);
]]


local _M = {version = 0.2}

--do-end 代码块主要是解决变量作用域问题
do
    local var_methods = {
        method = ngx.req.get_method,
        cookie = function () return ck:new() end
    }

    local ngx_var_names = {
        upstream_scheme            = true,
        upstream_host              = true,
        upstream_upgrade           = true,
        upstream_connection        = true,
        upstream_uri               = true,

        upstream_mirror_host       = true,

        upstream_cache_zone        = true,
        upstream_cache_zone_info   = true,
        upstream_no_cache          = true,
        upstream_cache_key         = true,
        upstream_cache_bypass      = true,
        upstream_hdr_expires       = true,
        upstream_hdr_cache_control = true,
    }

    local mt = {
        --__index则用来对表访问
        --当你通过键来访问 table 的时候，如果这个键没有值，那么Lua就会寻找该table的metatable（假定有metatable）中的__index 键。
        --如果__index包含一个表格，Lua会在表格中查找相应的键。
        __index = function(t, key)
            if type(key) ~= "string" then
                error("invalid argument, expect string value", 2)
            end

            local val
            local method = var_methods[key]
            if method then
                val = method()

            elseif C.memcmp(key, "cookie_", 7) == 0 then
                local cookie = t.cookie
                if cookie then
                    local err
                    val, err = cookie:get(sub_str(key, 8))
                    if not val then
                        log.warn("failed to fetch cookie value by key: ",
                                 key, " error: ", err)
                    end
                end

            elseif C.memcmp(key, "http_", 5) == 0 then
                key = key:lower()
                key = re_gsub(key, "-", "_", "jo")
                --这里就是从前面获取的_request(cdata)中提取变量了
                val = get_var(key, t._request)

            else
                val = get_var(key, t._request)
            end

            if val ~= nil then
                --rawset(table, index, value)
                --在不调用元表的情况下，给table[index]赋值为value
                rawset(t, key, val)
            end

            return val
        end,

        --_newindex 元方法用来对表更新
        __newindex = function(t, key, val)
            if ngx_var_names[key] then
                ngx_var[key] = val
            end

            -- log.info("key: ", key, " new val: ", val)
            rawset(t, key, val)
        end,
    }

function _M.set_vars_meta(ctx)
    local var = tablepool.fetch("ctx_var", 0, 32)
    --用resty.ngxvar.http.request来返回当前请求的请求对象
    --get_request()用的是lua-var-nginx-module库，从Luajit with FFI 的方式获取nginx变量
    --所以这里var._request是cdata，即 分配的 cdata 对象指向的内存块
    var._request = get_request()

    --设置var的元表为mt，这里是对var的包装，可以从var中提取request变量
    setmetatable(var, mt)
    ctx.var = var
end

function _M.release_vars(ctx)
    if ctx.var == nil then
        return
    end

    tablepool.release("ctx_var", ctx.var)
    ctx.var = nil
end

end -- do


return _M

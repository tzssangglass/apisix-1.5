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
local config_local = require("apisix.core.config_local")
local log          = require("apisix.core.log")
local json         = require("apisix.core.json")
local etcd         = require("resty.etcd")
local new_tab      = require("table.new")
local clone_tab    = require("table.clone")
local check_schema = require("apisix.core.schema").check
local exiting      = ngx.worker.exiting
local insert_tab   = table.insert
local type         = type
local ipairs       = ipairs
local setmetatable = setmetatable
local ngx_sleep    = ngx.sleep
local ngx_timer_at = ngx.timer.at
local ngx_time     = ngx.time
local sub_str      = string.sub
local tostring     = tostring
local tonumber     = tonumber
local pcall        = pcall
local created_obj  = {}


local _M = {
    version = 0.3,
    local_conf = config_local.local_conf,
    clear_local_cache = config_local.clear_cache,
}

local mt = {
    __index = _M,
    __tostring = function(self)
        return " etcd key: " .. self.key
    end
}


local function getkey(etcd_cli, key)
    if not etcd_cli then
        return nil, "not inited"
    end

    local res, err = etcd_cli:get(key)
    if not res then
        -- log.error("failed to get key from etcd: ", err)
        return nil, err
    end

    if type(res.body) ~= "table" then
        return nil, "failed to get key from etcd"
    end

    return res
end


local function readdir(etcd_cli, key)
    if not etcd_cli then
        return nil, nil, "not inited"
    end

    local res, err = etcd_cli:readdir(key, true)
    if not res then
        -- log.error("failed to get key from etcd: ", err)
        return nil, nil, err
    end

    if type(res.body) ~= "table" then
        return nil, "failed to read etcd dir"
    end

    return res
end

local function waitdir(etcd_cli, key, modified_index, timeout)
    if not etcd_cli then
        return nil, nil, "not inited"
    end

    local res, err = etcd_cli:waitdir(key, modified_index, timeout)
    if not res then
        -- log.error("failed to get key from etcd: ", err)
        return nil, err
    end

    if type(res.body) ~= "table" then
        return nil, "failed to read etcd dir"
    end

    return res
end


local function short_key(self, str)
    return sub_str(str, #self.key + 2)
end


function _M.upgrade_version(self, new_ver)
    new_ver = tonumber(new_ver)
    if not new_ver then
        return
    end

    local pre_index = self.prev_index
    if not pre_index then
        self.prev_index = new_ver
        return
    end

    if new_ver <= pre_index then
        return
    end

    self.prev_index = new_ver
    return
end


local function sync_data(self)
    --启动的时候，这里的key有很多个
    --/apisix/ssl  /apisix/proto /apisix/global_rules /apisix/consumers 等

    if not self.key then
        return nil, "missing 'key' arguments"
    end

    if self.need_reload then
        --根据key读取dir
        local res, err = readdir(self.etcd_cli, self.key)
        if not res then
            return false, err
        end

        --获取返回值
        local dir_res, headers = res.body.node, res.headers
        log.debug("readdir key: ", self.key, " res: ",
                  json.delay_encode(dir_res))
        --一系列判空
        if not dir_res then
            return false, err
        end

        if not dir_res.dir then
            return false, self.key .. " is not a dir"
        end

        if not dir_res.nodes then
            dir_res.nodes = {}
        end

        --这一段的目的是，调用clean_handlers函数
        if self.values then
            for i, val in ipairs(self.values) do
                --这里self.values应当是Upstream
                if val and val.clean_handlers then
                    --todo clean_handlers可能跟balancer#create_checker函数有关，里面有对clean_handlers的insert操作
                    for _, clean_handler in ipairs(val.clean_handlers) do
                        clean_handler(val)
                    end
                    val.clean_handlers = nil
                end
            end

            self.values = nil
            self.values_hash = nil
        end

        --看起来values存储的应该是upstreams的nodes
        --table.new(narray, nhash) 两个参数分别代表table里是array还是hash的
        --新建一个数组，存放nodes，长度是#dir_res.nodes
        self.values = new_tab(#dir_res.nodes, 0)
        --新建一个哈希表，存放nodes，长度是#dir_res.nodes
        self.values_hash = new_tab(0, #dir_res.nodes)

        local changed = false
        --这里是遍历etcd中的nodes了
        for _, item in ipairs(dir_res.nodes) do
            local key = short_key(self, item.key)
            local data_valid = true
            if type(item.value) ~= "table" then
                data_valid = false
                log.error("invalid item data of [", self.key .. "/" .. key,
                          "], val: ", tostring(item.value),
                          ", it shoud be a object")
            end

            if data_valid and self.item_schema then
                data_valid, err = check_schema(self.item_schema, item.value)
                if not data_valid then
                    log.error("failed to check item data of [", self.key,
                              "] err:", err, " ,val: ", json.encode(item.value))
                end
            end


            if data_valid then
                changed = true
                --启动的时候，self.values = {} 是空的
                insert_tab(self.values, item)
                --用self.values_hash维护key在self.values中的下标索引
                self.values_hash[key] = #self.values
                --这里不清楚为什么要让item.value.id = key
                item.value.id = key
                item.clean_handlers = {}

                --调用item的filter函数吧
                if self.filter then
                    self.filter(item)
                end
            end
            --让self.prev_index = item.modifiedIndex
            --维持etcd最新的modifiedIndex
            self:upgrade_version(item.modifiedIndex)
        end
        --headers["X-Etcd-Index"]是etcd全局最新的modifiedIndex
        if headers then
            self:upgrade_version(headers["X-Etcd-Index"])
        end

        if changed then
            --启动的时候，self.conf_version = 0
            self.conf_version = self.conf_version + 1
        end

        --从etcd全量同步一个key的数据结束，self.need_reload 置为false，下面不需要再全量同步了
        self.need_reload = false
        return true
    end

    --下面开始监听key目录的变化
    -- for fetch the etcd index
    --在这里get的时候，没有传wait recursive和wait_index，相当之一个直接查询key的请求，目的是为了获取etcd全局最新的modifiedIndex
    --即下面的local key_index = key_res.headers["X-Etcd-Index"]
    local key_res, _ = getkey(self.etcd_cli, self.key)
    --waitdir即向etcd发出wait请求
    --关键参数:
    --wait = true 一次性 watch，每监听到一次实践后，客户端都需要重新发起watch请求
    --dir = true 标识这是一个目录
    --recursive = true 当 watch 一个目录时，可以设定参数：recursive=true，表示 watch 该目录下子目录 "/key" 的变化
    --wait_index = modified_index，self.prev_index始终维持等于etcd当前最新的modified_index
    --self.prev_index + 1即表示watch将要发生的modified_index + 1 事件
    --watch功能
    local dir_res, err = waitdir(self.etcd_cli, self.key, self.prev_index + 1, self.timeout)
    log.info("waitdir key: ", self.key, " prev_index: ", self.prev_index + 1)
    log.info("res: ", json.delay_encode(dir_res, true))
    --err == "timeout" 标识apisix主动关闭了链接，timeout即下面设置的30s
    --err == "timeout"标识在这段时间内，观察的目录下的key没有发生变化
    if err == "timeout" then
        if key_res and key_res.headers then
            --X-Etcd-Index即modifiedIndex
            local key_index = key_res.headers["X-Etcd-Index"]
            local key_idx = key_index and tonumber(key_index) or 0
            --一般情况下，比如通过接口更改key下的数据，或者直接通过etcd客户端来修改
            --key_idx == self.prev_index
            --self.prev_index始终维持等于etcd的modified_index
            if key_idx and key_idx > self.prev_index then
                self:upgrade_version(key_index)
            end
        end
    end

    if not dir_res then
        return false, err
    end

    local res = dir_res.body.node
    --处理etcd事件洪泛场景
    --Etcd v2 server 端只缓存 1000 条事件的历史记录（全局的，不是每个 key），因此若发生事件洪泛，
    --举个例子，如果当前 X-Etcd-Index 为 1005，则 X-Etcd-Index 中为 1 到 5 的时间就会被丢弃。
    --如果 waitIndex=5，则服务器会返回错误信息。
    --例如，瞬间产生超过 1000 条事件而事件监听客户端又处理得比较慢，那么就会发生事件丢失的情况。
    local err_msg = dir_res.body.message
    if err_msg then
        if err_msg == "The event in requested index is outdated and cleared"
           and dir_res.body.errorCode == 401 then
            self.need_reload = true
            log.warn("waitdir [", self.key, "] err: ", err_msg,
                     ", need to fully reload")
            return false
        end
        return false, err
    end

     if not res then
        if err == "The event in requested index is outdated and cleared" then
            self.need_reload = true
            log.warn("waitdir [", self.key, "] err: ", err,
                     ", need to fully reload")
            return false
        end

        return false, err
    end

    local key = short_key(self, res.key)
    if res.value and type(res.value) ~= "table" then
        --如果res.value存在但是不是table，说明etcd中确实有数据变动
        --但是这个变动后的数据无法被apisix解析
        --相当于apisix放弃etcd的这次数据变动，但是要让self.prev_index = modified_index(etcd)
        --即跟上etcd的节奏，为监听下一次的数据变化
        self:upgrade_version(res.modifiedIndex)
        return false, "invalid item data of [" .. self.key .. "/" .. key
                      .. "], val: " .. tostring(res.value)
                      .. ", it shoud be a object"
    end

    if res.value and self.item_schema then
        local ok, err = check_schema(self.item_schema, res.value)
        if not ok then
            self:upgrade_version(res.modifiedIndex)

            return false, "failed to check item data of ["
                          .. self.key .. "] err:" .. err
        end
    end

    self:upgrade_version(res.modifiedIndex)

    if res.dir then
        if res.value then
            return false, "todo: support for parsing `dir` response "
                          .. "structures. " .. json.encode(res)
        end
        return false
    end

    if self.filter then
        self.filter(res)
    end

    --在self.values_hash(是一个hash表)，key对应的value，即此处的pre_index，是一个数字，表示数组下标
    --这个数组下标，是self.values(是一个array数组)中的下标，self.values存放的是key对应的真正的value，
    local pre_index = self.values_hash[key]
    if pre_index then
        --存在的话，直接通过pre_index下标访问self.values数组，获得key在apisix中对应的真正的value
        local pre_val = self.values[pre_index]
        --先执行clean_handlers
        if pre_val and pre_val.clean_handlers then
            for _, clean_handler in ipairs(pre_val.clean_handlers) do
                clean_handler(pre_val)
            end
            pre_val.clean_handlers = nil
        end

        if res.value then
            --这里是真正的用etcd的数据来覆盖apisix本地缓存中的数据
            res.value.id = key
            self.values[pre_index] = res
            res.clean_handlers = {}

        else
            --如果要更新的数据在etcd中不存在了，那么本地也要清除
            --同步次数加一
            self.sync_times = self.sync_times + 1
            self.values[pre_index] = false
        end

    elseif res.value then
        --当从etcd新增一个item时，初始化clean_handlers
        res.clean_handlers = {}
        insert_tab(self.values, res)
        self.values_hash[key] = #self.values
        res.value.id = key
    end

    -- avoid space waste
    -- todo: need to cover this path, it is important.
    --只有在上面key在self.values_hash中存在，但是etcd的body.node.value不存在的条件下
    --self.sync_times = self.sync_times + 1
    --相当于apisix本地有这个key的数据，但是etcd已经没有了，上面已经清理过了self.values
    --这一步相当于给self.values进行缩容，剔除里面的被置为false的元素
    if self.sync_times > 100 then
        local count = 0
        for i = 1, #self.values do
            --val存储的是下标对应的元素，可能是上面置为false，也可能是真的有效值
            local val = self.values[i]
            --上面是把失效位置的下标置为false，但是对于gc来说，是不可回收的
            --这里是置为nil，相当于让gc进行回收
            self.values[i] = nil
            if val then
                --如果val是有效值的话(不是上面置为false)
                count = count + 1
                --存储有效值
                self.values[count] = val
            end
        end

        --对self.values缩容之后，同样要对self.values_hash缩容
        --重新建立key与self.values_hash和self.values之间的对应关系
        for i = 1, count do
            key = short_key(self, self.values[i].key)
            self.values_hash[key] = i
        end
        --self.sync_times归零
        self.sync_times = 0
    end

    --启动的时候reload会+1，有数据同步也+1
    self.conf_version = self.conf_version + 1
    return self.values
end


function _M.get(self, key)
    if not self.values_hash then
        return
    end

    local arr_idx = self.values_hash[tostring(key)]
    if not arr_idx then
        return nil
    end

    return self.values[arr_idx]
end


function _M.getkey(self, key)
    if not self.running then
        return nil, "stoped"
    end

    return getkey(self.etcd_cli, key)
end


local function _automatic_fetch(premature, self)
    if premature then
        return
    end

    local i = 0
    --exiting()即ngx.worker.exiting
    --这个函数返回一个布尔值，指示当前的Nginx工作进程是否已经开始退出。Nginx工作进程退出发生在Nginx服务器退出或配置重新加载(又名HUP重载)。
    --self.running即obj设置的，running = true
    while not exiting() and self.running and i <= 32 do
        i = i + 1
        --调用数据同步函数
        --这里使用了pcall（protected call）来包装需要执行的代码，这是Lua 中处理错误的方式
        --函数第一个返回值是函数的运行状态(true,false)，第二个返回值是pcall中函数的返回值
        --所以在这里，ok是sync_data函数的运行状态
        --ok2和err是sync_data函数的返回值
        local ok, ok2, err = pcall(sync_data, self)
        if not ok then
            --如果函数运行错误，那么ok2的出参位置就是errorinfo
            err = ok2
            log.error("failed to fetch data from etcd: ", err, ", ",
                      tostring(self))
            --使用openresty的lua库，(进程内)协程的切换，但进程还是处于运行状态(其他协程还在运行)
            --此处相当于当前协程沉睡3s
            ngx_sleep(3)
            --沉睡完之后跳出循环，再次尝试
            break
        --如果sync_data主动返回false，并且err有值
        elseif not ok2 and err then
            if err ~= "timeout" and err ~= "Key not found"
               and self.last_err ~= err then
                log.error("failed to fetch data from etcd: ", err, ", ",
                          tostring(self))
            end

            --保存当前最新的异常现场，即让last_err指向当前的err，更新last_err_time为当前时间
            if err ~= self.last_err then
                self.last_err = err
                self.last_err_time = ngx_time()
            else
                if ngx_time() - self.last_err_time >= 30 then
                    --如果当前的异常和前一次的异常一样，并且已经过去了30秒，则置空last_err
                    --出现这种情况一般说明，这么做的目的？可能是相同的异常前面都输出了，再继续执行，相同原因的失败也只是时间问题
                    --走到这里有几个条件
                    --i <= 32
                    --err == self.last_err
                    --ngx_time() - self.last_err_time >= 30
                    self.last_err = nil
                    --执行完之后，还能再次进入循环，会进入err ~= self.last_err这个分支
                end
            end
            ngx_sleep(0.5)
        --如果sync_data主动返回false
        elseif not ok2 then
            ngx_sleep(0.05)
        end
    end

    --走到这里，while循环结束了，相当于本轮fetch结束，递归调用，这里应当是配置更新能做到毫秒级延迟的重点，因为持续地有协程在从etcd同步数据到本地
    if not exiting() and self.running then
        ngx_timer_at(0, _automatic_fetch, self)
    end
end


function _M.new(key, opts)
    local local_conf, err = config_local.local_conf()
    if not local_conf then
        return nil, err
    end

    --获取etcd相关配置
    local etcd_conf = clone_tab(local_conf.etcd)
    local prefix = etcd_conf.prefix
    etcd_conf.http_host = etcd_conf.host
    etcd_conf.host = nil
    etcd_conf.prefix = nil

    --创建etcd连接客户端
    local etcd_cli
    etcd_cli, err = etcd.new(etcd_conf)
    if not etcd_cli then
        return nil, err
    end

    --参数赋值
    local automatic = opts and opts.automatic
    local item_schema = opts and opts.item_schema
    local filter_fun = opts and opts.filter
    local timeout = opts and opts.timeout

    local obj = setmetatable({
        etcd_cli = etcd_cli,
        key = key and prefix .. key,
        automatic = automatic,
        item_schema = item_schema,
        sync_times = 0,
        running = true,
        conf_version = 0,
        values = nil,
        need_reload = true,
        routes_hash = nil,
        prev_index = nil,
        last_err = nil,
        last_err_time = nil,
        timeout = timeout,
        filter = filter_fun,
    }, mt)

    if automatic then
        if not key then
            return nil, "missing `key` argument"
        end
        ngx_timer_at(0, _automatic_fetch, obj)
    end

    --保存已创建的key
    if key then
        created_obj[key] = obj
    end
    return obj
end


function _M.close(self)
    self.running = false
end


function _M.fetch_created_obj(key)
    return created_obj[key]
end


local function read_etcd_version(etcd_cli)
    if not etcd_cli then
        return nil, "not inited"
    end

    local data, err = etcd_cli:version()
    if not data then
        return nil, err
    end

    local body = data.body
    if type(body) ~= "table" then
        return nil, "failed to read response body when try to fetch etcd "
                    .. "version"
    end

    return body
end

function _M.server_version(self)
    if not self.running then
        return nil, "stoped"
    end

    return read_etcd_version(self.etcd_cli)
end


return _M

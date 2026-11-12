-- Execute the ACTUAL shipped Lua script against a deterministic Redis command model.
-- This tests script logic, NOT redis-rs, Redis transport, real Redis atomicity or TTL scheduling.
-- Works with texlua/Lua 5.3 and Lua 5.1. Floating-point conversion emulates Redis Lua numbers.
local source = "src/store/sliding_window.lua"
local passed, assertions = 0, 0
local function eq(actual, expected, note)
    assertions = assertions + 1
    if type(expected) == "table" then
        assert(type(actual) == "table" and #actual == #expected, note or "table size")
        for i, value in ipairs(expected) do eq(actual[i], value, note) end
    else
        assert(actual == expected, (note or "value") .. ": got " .. tostring(actual) .. " expected " .. tostring(expected))
    end
end
local function fixture()
    local f = { now = 100000000.0, sets = {}, expiries = {}, expiry_writes = 0 }
    local function sorted(key, reversed)
        local entries = {}
        for member, score in pairs(f.sets[key] or {}) do entries[#entries + 1] = {member, score} end
        table.sort(entries, function(a, b)
            if a[2] == b[2] then return a[1] < b[1] end
            return a[2] < b[2]
        end)
        if reversed then
            local result = {}
            for i = #entries, 1, -1 do result[#result + 1] = entries[i] end
            return result
        end
        return entries
    end
    local function call(cmd, key, a, b, c)
        if cmd == "TIME" then return {tostring(math.floor(f.now / 1000000)), tostring(f.now % 1000000)} end
        -- Deliberately no wall-clock expiry simulation: compare script expiry writes.
        if cmd == "ZREVRANGE" or cmd == "ZRANGE" then
            local values = sorted(key, cmd == "ZREVRANGE")
            return values[1] and {values[1][1], tostring(values[1][2])} or {}
        elseif cmd == "ZCARD" then return #sorted(key)
        elseif cmd == "ZREMRANGEBYSCORE" then
            local max = tonumber(b)
            for member, score in pairs(f.sets[key] or {}) do
                if score <= max then f.sets[key][member] = nil end
            end
            return 0
        elseif cmd == "ZADD" then
            f.sets[key] = f.sets[key] or {}
            f.sets[key][b] = a
            return 1
        elseif cmd == "PEXPIRE" then
            f.expiries[key] = a
            f.expiry_writes = f.expiry_writes + 1
            return 1
        end
        error("Unexpected Redis command: " .. cmd)
    end
    function f:check(key, window, limit, detailed)
        local env = setmetatable({KEYS = {key}, ARGV = {string.format("%.0f", window), string.format("%.0f", limit), detailed and "details" or nil},
            redis = {call = call}, tonumber = function(v) local n = tonumber(v); return n and n + 0.0 end}, {__index = _G})
        local chunk
        if setfenv then chunk = assert(loadfile(source)); setfenv(chunk, env)
        else chunk = assert(loadfile(source, "t", env)) end
        return chunk()
    end
    function f:count(key) return #sorted(key) end
    return f
end
local function test(name, run)
    run()
    passed = passed + 1
    print("PASS " .. name)
end

test("exact burst and denial does not record", function()
    local f = fixture()
    for _ = 1, 10 do eq(f:check("a", 1000000, 10), 0) end
    for _ = 1, 80 do eq(f:check("a", 1000000, 10), 1) end
    eq(f:count("a"), 10)
end)
test("same-microsecond unique members", function()
    local f = fixture()
    for _ = 1, 300 do f:check("a", 1000000, 300) end
    eq(f:count("a"), 300)
end)
test("half-open lower boundary", function()
    local f = fixture()
    eq(f:check("a", 1000000, 1), 0)
    f.now = f.now + 999999
    eq(f:check("a", 1000000, 1), 1)
    f.now = f.now + 1
    eq(f:check("a", 1000000, 1), 0)
end)
test("rejection does not renew TTL", function()
    local f = fixture()
    eq(f:check("a", 1500001, 1), 0)
    eq(f.expiries.a, 1501)
    f.now = f.now + 500
    eq(f:check("a", 1500001, 1), 1)
    eq(f.expiry_writes, 1)
end)
test("remaining is post-admission", function()
    local f = fixture()
    eq(f:check("a", 60000000, 3, true), {0, 2, 0})
    eq(f:check("a", 60000000, 3, true), {0, 1, 0})
    eq(f:check("a", 60000000, 3, true), {0, 0, 0})
    eq(f:check("a", 60000000, 3, true), {1, 0, 60000000})
end)
test("retry follows oldest expiration not full window", function()
    local f = fixture()
    eq(f:check("a", 60000000, 2, true), {0, 1, 0})
    f.now = 130000000
    eq(f:check("a", 60000000, 2, true), {0, 0, 0})
    f.now = 159250000
    eq(f:check("a", 60000000, 2, true), {1, 0, 750000})
    f.now = 160000000
    eq(f:check("a", 60000000, 2, true), {0, 0, 0})
    eq(f:check("a", 60000000, 2, true), {1, 0, 30000000})
end)
test("clock rollback preserves order and retry", function()
    local f = fixture()
    eq(f:check("a", 60000000, 2, true), {0, 1, 0})
    f.now = 130000000
    eq(f:check("a", 60000000, 2, true), {0, 0, 0})
    f.now = 90000000
    eq(f:check("a", 60000000, 2, true), {1, 0, 70000000})
    eq(f:count("a"), 2)
end)
test("independent keys", function()
    local f = fixture()
    eq(f:check("a", 1000000, 1), 0)
    eq(f:check("b", 1000000, 1), 0)
    eq(f:check("a", 1000000, 1), 1)
end)
test("microsecond windows round TTL up", function()
    local f = fixture()
    eq(f:check("a", 1, 1), 0)
    eq(f.expiries.a, 1)
    f.now = f.now + 1
    eq(f:check("a", 1, 1), 0)
end)
test("maximum exact numeric window", function()
    local f = fixture()
    local max = 9007199254740991.0
    eq(f:check("a", max, max, true), {0, max - 1, 0})
    eq(f.expiries.a, math.ceil(max / 1000))
end)
test("basic and metadata checks share one quota", function()
    local f = fixture()
    eq(f:check("a", 1000000, 2), 0)
    eq(f:check("a", 1000000, 2, true), {0, 0, 0})
    eq(f:check("a", 1000000, 2), 1)
    eq(f:count("a"), 2)
end)
test("generated workload vs independent exact log model", function()
    local f = fixture()
    local queues, seed = {}, 991
    for i = 1, 10000 do
        seed = (seed * 48271) % 2147483647
        f.now = f.now + seed % 127
        local key = tostring(seed % 7)
        local window, limit = 1013, 1 + seed % 9
        -- Include configuration in the key, exactly as the Rust store does.
        key = key .. ":" .. limit
        local q = queues[key] or {}
        local next_q = {}
        for _, stamp in ipairs(q) do if stamp > f.now - window then next_q[#next_q + 1] = stamp end end
        local denied = #next_q >= limit
        if not denied then next_q[#next_q + 1] = f.now end
        queues[key] = next_q
        eq(f:check(key, window, limit), denied and 1 or 0, "model step " .. i)
        eq(f:count(key), #next_q)
    end
end)
print(string.format("RESULT tests=%d assertions=%d status=passed", passed, assertions))

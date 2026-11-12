-- wrk closed-loop HTTP workload. Status collection is enabled for EVERY variant.
-- This is not an open-loop saturation/capacity estimator.
local threads = {}
function setup(thread)
    thread:set("offset", #threads)
    threads[#threads + 1] = thread
end
function init(args)
    local clients = tonumber(args[1]) or 100
    assert(clients >= 1 and clients <= 10000)
    pool = {}
    for i = 0, clients - 1 do
        pool[i + 1] = wrk.format("GET", "/", { ["X-Benchmark-Client"] = tostring(i) })
    end
    cursor = (offset or 0) % clients
    statuses = { ok = 0, limited = 0, other = 0 }
end
function request()
    cursor = cursor % #pool + 1
    return pool[cursor]
end
function response(status, headers, body)
    if status == 200 then statuses.ok = statuses.ok + 1
    elseif status == 429 then statuses.limited = statuses.limited + 1
    else statuses.other = statuses.other + 1 end
end
function done(summary, latency, requests)
    local ok, limited, other = 0, 0, 0
    for _, thread in ipairs(threads) do
        local counts = thread:get("statuses")
        ok, limited, other = ok + counts.ok, limited + counts.limited, other + counts.other
    end
    local errors = summary.errors
    io.write(string.format(
        '\nAXUM_BENCH_JSON {"requests":%d,"duration_us":%d,"rps":%.6f,"p50_us":%.6f,"p95_us":%.6f,"p99_us":%.6f,"ok":%d,"limited":%d,"other":%d,"connect_errors":%d,"read_errors":%d,"write_errors":%d,"timeouts":%d}\n',
        summary.requests, summary.duration, summary.requests / (summary.duration / 1000000),
        latency:percentile(50), latency:percentile(95), latency:percentile(99),
        ok, limited, other, errors.connect, errors.read, errors.write, errors.timeout))
end

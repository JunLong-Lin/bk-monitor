-- A single hash-slot key holds the entire bounded in-flight ledger. Leases
-- NEVER expire entries automatically: TTL is fencing, not proof of I/O exit.
local key = KEYS[1]
local operation = ARGV[1]
if operation == 'rebuild' then
    local entries = cjson.decode(ARGV[3])
    redis.call('DEL', key)
    redis.call('HSET', key, '_epoch', ARGV[2])
    for id, entry in pairs(entries) do
        redis.call('HSET', key, id, cjson.encode(entry))
    end
    return 1
end
if not redis.call('HGET', key, '_epoch') then return -1 end
local id = ARGV[2]
if operation == 'acquire' then
    if redis.call('HGET', key, '_epoch') ~= ARGV[3] then return -1 end
    local candidate = cjson.decode(ARGV[4])
    if redis.call('HEXISTS', key, id) == 1 then return 0 end
    local counts = {}
    local entries = redis.call('HGETALL', key)
    for i = 1, #entries, 2 do
        if entries[i] ~= '_epoch' then
            local entry = cjson.decode(entries[i+1])
            for _, dimension in ipairs(entry.dimensions) do
                counts[dimension] = (counts[dimension] or 0) + 1
            end
        end
    end
    local limits = cjson.decode(ARGV[5])
    for _, dimension in ipairs(candidate.dimensions) do
        if (counts[dimension] or 0) >= limits[dimension] then return 0 end
    end
    redis.call('HSET', key, id, cjson.encode(candidate))
    return 1
end
local value = redis.call('HGET', key, id)
if not value then return 0 end
local entry = cjson.decode(value)
if entry.owner ~= ARGV[3] or tostring(entry.generation) ~= ARGV[4] then return 0 end
if operation == 'release' then
    return redis.call('HDEL', key, id)
end
if operation == 'renew' then
    local now = tonumber(ARGV[5])
    local expiry = tonumber(ARGV[6])
    if entry.expiry <= now or expiry < entry.expiry or expiry <= now then return 0 end
    entry.expiry = expiry
    redis.call('HSET', key, id, cjson.encode(entry))
    return 1
end
return redis.error_reply('unknown export ledger operation')

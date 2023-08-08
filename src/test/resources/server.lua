if package.setsearchroot ~= nil then
    package.setsearchroot()
end

local utils = require('utils')

box.cfg {
    listen = 3301,
    memtx_memory = 128 * 1024 * 1024,
    log_level = 6
}

box.once('schema', function()
    local space = box.schema.space.create('ships', { if_not_exists = true })
    space:format({
        { name = "id", type = "integer" },
        { name = "name", type = "string" },
        { name = "guns_count", type = "unsigned" },
        { name = "crew", type = "unsigned" },
        { name = "created_at", type = "datetime" },
        { name = "breadth", type = "double", is_nullable = true },
    })
    space:create_index("primary", { parts = { { field = "id" } },
                                    if_not_exists = true })
    space:create_index("created_at_index", { parts = { { field = "created_at" } },
                                             if_not_exists = true })

    box.schema.user.grant('guest', 'super')
    utils.create_testcontainers_user()
end)

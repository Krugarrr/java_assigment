local M = {}

function M.create_kv_space(name)
    local space = box.schema.space.create(name, {
        if_not_exists = true,
        format = {
            { 'id', type = 'string' },
            { 'value', type = 'string', is_nullable = true }
        }
    })
    space:create_index('pk', { parts = { 'id' } })
end

function M.create_testcontainers_user()
    -- this user is required for testcontainers
    box.schema.user.create('api_user', { password = 'secret' })
    box.schema.user.grant('api_user', 'read,write,execute', 'universe')
end

return M

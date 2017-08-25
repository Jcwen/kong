-- DB knows about:
-- * Table cache
-- * Propagation of CRUD events
--   * It knows it needs the "old" version of a resource before updating, for propagating events
-- * It knows strategies exist, but doesn't know details
-- It does not know about:
-- * individual table fields - this class deals with records, not individual fields
-- * schemas - use Model if you need to use a schema
-- * update_or_create - the Model knows that

local DB         = {}
local DB_mt      = { __index = DB }

local fmt = string.format

-- TODO add cache here?
-- TODO add rbac hook here?
function DB.new(strategy, events)
  return setmetatable({ strategy = strategy, events = events }, DB_mt)
end

function DB:propagate_crud_event(table_name, event_type, entity, old_entity, options)
  if not options.quiet then
    local ok, err = self.events.post_local("db:crud",
                                           event_type,
                                           {
                                             table_name = table_name,
                                             operation  = event_type,
                                             entity     = entity,
                                             old_entity = old_entity,
                                           })

    if not ok then
      ngx.log(ngx.ERR, "could not propagate CRUD operation: ", err)
    end
  end
end


function DB:get_auto_inserted_value(table_name, field_type)
  return self.strategy:get_auto_inserted_value(table_name, field_type)
end


function DB:get_cache_key(table_name, arg1, arg2, arg3, arg4, arg5)
  return fmt("%s:%s:%s:%s:%s:%s",
             table_name,
             arg1 == nil and "" or arg1,
             arg2 == nil and "" or arg2,
             arg3 == nil and "" or arg3,
             arg4 == nil and "" or arg4,
             arg5 == nil and "" or arg5)
end


function DB:insert_one(table_name, attributes, options)
  local res, err = self.strategy:insert_one(table_name, attributes, options)

  if not err then
    -- TODO insert in table cache here?
    self:propagate_crud_event(table_name, "create", res, nil, options)
  end

  return res, err
end


function DB:find_one(table_name, primary_keys)
  -- TODO retrieve from cache here?
  return self.strategy:find_one(table_name, primary_keys)
end


function DB:select_all(table_name)
  -- TODO can we use the cache for the select_all?
  return self.strategy:select_all(table_name)
end


function DB:count_all(table_name)
  -- TODO can we use the cache for the count_all?
  return self.strategy:count_all(table_name)
end


function DB:update_one(table_name, primary_keys, attributes, options)
  local old, err1 = self:find_one(table_name, primary_keys)
  if err1 then
    return nil, err1
  end

  local res, err2 = self.strategy:update_one(table_name, primary_keys, attributes, options)

  if not err2 then
    -- TODO update table cache here? (add res, remove old)
    self:propagate_crud_event(table_name, "update", res, old, options)
  end

  return res, err2
end


function DB:delete_one(table_name, primary_keys, options)
  local res, err = self.strategy:delete_one(table_name, primary_keys, options)

  if not err then
    -- TODO update cache here?
    self:propagate_crud_event(table_name, "delete", res, nil, options)
  end

  return res, err
end


function DB:truncate(table_name)
  -- TODO invalidate cache for the whole table?
  return self.strategy:truncate(table_name)
end


return DB

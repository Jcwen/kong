-- Model notes:
-- * It has check_arg & company because it is user-facing
-- * It knows about an individual table's schema
-- * It performs crud operations on a single table
-- * It knows how to extract primary keys from groups of attributes
-- * It knows the difference between update and update_or_create
--
-- It does not now about:
-- * Event propagation
-- * Cache
-- * Strategies
--   ^ Use kong.db for those ^
--
-- * Individual table fields
--   ^ Use kong.schema for those ^
--
-- * Validations
--   ^ Use kong.validator for those ^

local Errors = require "kong.db.errors" -- TODO
local utils  = require "kong.tools.utils"

-- model.lua
local Model    = {}


local return_error = Errors.return_error
local RANDOM_VALUE = utils.random_string()


local function check_arg(value, name, order, expected_type)
  if type(value) ~= expected_type then
    local msg = "bad argument #%d to '%s' (%s expected, got %s)"
    error(msg:format(order, name, expected_type, type(value), 3))
  end
end


local function check_not_empty(tbl, name, order)
  if next(tbl) == nil then
    local msg = "bad argument #%d to '%s' (expected table to not be empty)"
    error(msg:format(order, name), 3)
  end
end


local function check_utf8(tbl, arg_n)
  for k, v in pairs(tbl) do
    if not utils.validate_utf8(v) then
      tbl[k] = RANDOM_VALUE -- Force a random string
    end
  end
end


local Model_mt = { __index = Model }
function Model.new(db, schema)
  return setmetatable({ db = database, schema = schema }, Model_mt)
end


function Model:find_one(primary_key_or_keys)
  local t = type(primary_key_or_keys)
  local primary_keys = primary_key_or_keys
  if t == "string" or t == "number" then
    primary_keys = { primary_key_or_keys }
  end
  check_arg(primary_keys, "primary_keys", 1, "table")
  check_utf8(primary_keys, "primary_keys", 1)

  local db          = self.db
  local schema      = self.schema

  local primary_keys, err = self.schema:extract_primary_keys(primary_keys)

  if err then
    return_error(db, nil, err)
  end

  return return_error(db, db:find(schema.table_name, primary_keys))
end


-- No options, no filters. It always returns an iterator
function Model:select_all()
  local db = self.db
  return return_error(db, db:select_all(self.schema.table_name))
end

-- Missing: Paginated results

-- No options, no filters. It always returns a number
function Model:count_all()
  local db = self.db
  return return_error(db, db:count_all(self.schema.table_name))
end


-- POST
-- Doesn't now anything about events - the db layer takes care of that
function Model:insert_one(attributes, options)
  options = options or {}
  check_arg(attributes, "attributes", 1, "table")
  check_arg(options,    "options",    1, "table")

  local db          = self.db
  local schema      = self.schema
  local table_name  = schema.table_name

  for col, field in pairs(schema.fields) do
    if attributes[col] == nil then
      attributes[col] = db:get_auto_inserted_value(table_name, field.type)
    end
  end

  return return_error(db, db:insert_one(table_name, attributes, options))
end


-- PATCH
-- Always updates a single record (not a subset)
-- No "full" option: If the user wants to do a full partial, they have to specify all the files,
-- with ngx.null on the ones they want to nullify.
function Model:update_one(attributes, options)
  check_arg(attributes, "attributes", 1, "table")
  check_not_empty(attributes, "attributes", 1)
  check_arg(options, "options", 2, "table")

  local db = self.db
  local primary_keys, err = self.schema:extract_primary_keys(primary_keys)

  if err then
    return_error(db, nil, err)
  end

  return return_error(db, db:update(self.schema.table_name,
                                    primary_keys,
                                    attributes))
end


-- PUT
function Model:update_or_insert_one(attributes, options)
  check_arg(attributes, "attributes", 1, "table")
  check_not_empty(attributes, "attributes", 1)
  check_arg(options, "options", 2, "table")

  local db = self.db

  local current, err = self:find(attributes)
  if err then
    return return_error(db, nil, err)
  end

  if current then
    return return_error(db, self:update(attributes, options))
  end

  return return_error(db, self:insert(attributes, options))
end


-- DELETE
-- Always deletes a single record. Deleting cascade is not done automatically
function Model:delete_one(primary_keys, options)
  options = options or {}
  check_arg(primary_keys, "primary_keys", 1, "table")
  check_arg(options,      "options",      2, "table")

  local db = self.db
  local primary_keys, err = self.schema:extract_primary_keys(primary_keys)

  if err then
    return_error(db, nil, err)
  end

  return return_error(db:delete_one(self.schema.table_name, primary_keys))
end


return Model


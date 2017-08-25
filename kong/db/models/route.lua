local Model  = require 'kong.db.model'
local schema = require 'kong.db.schema' --TODO
local db     = require 'kong.singletons.db' -- TODO
.
local Route = Model.new(db, schema.routes)

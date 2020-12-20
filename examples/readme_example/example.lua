fiber = require 'fiber'

box.cfg{listen = 3301}
box.schema.space.create('examples', {id = 999})
box.space.examples:create_index('primary', {type = 'hash', parts = {1, 'unsigned'}})
box.schema.user.grant('guest', 'read,write', 'space', 'examples')

if not fiber.self().storage.console then
    require 'console'.start()
    os.exit()
end

# frozen_string_literal: true

require "helper"

require "fluent/plugin/in_static_file"

# unit test for Fluent::Plugin::InStaticFile
class InStaticFileTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end
end

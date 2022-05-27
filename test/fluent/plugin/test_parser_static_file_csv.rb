# frozen_string_literal: true

require "helper"
require "fluent/plugin/parser_static_file_csv"

require "stringio"

# unit test for Fluent::Plugin::ParserStaticFileCsv
class ParserStaticFileCsvTest < Test::Unit::TestCase
  setup do
    Fluent::Test.setup
  end

  CONFIG = %()

  sub_test_case "text type" do
    test "it handles text as String" do
      driver = create_driver

      csv_text = "id,name,description\n" \
                 "1,name1,descr1"

      expected_record = { "description" => "descr1", "id" => "1", "name" => "name1" }

      driver.instance.parse(csv_text) do |time, record|
        assert(time)
        assert_equal(expected_record, record)
      end
    end

    test "it handles text as StringIO" do
      driver = create_driver

      csv_text = "id,name,description\n" \
                 "1,name1,descr1"
      csv_text_io = StringIO.new(csv_text)

      expected_record = { "description" => "descr1", "id" => "1", "name" => "name1" }

      driver.instance.parse(csv_text_io) do |time, record|
        assert(time)
        assert_equal(expected_record, record)
      end
    end
  end

  sub_test_case "CSV delimiter" do
    test "it has a default delimiter set to ," do
      driver = create_driver

      csv_text = "id,name,description\n" \
                 "1,name1,descr1"

      expected_record = { "description" => "descr1", "id" => "1", "name" => "name1" }

      driver.instance.parse(csv_text) do |time, record|
        assert(time)
        assert_equal(expected_record, record)
      end
    end

    test "it can use another delimiter when specified" do
      config = %(delimiter ;)
      driver = create_driver(config)

      csv_text = "id;name;description\n" \
                 "1;name1;descr1"

      expected_record = { "description" => "descr1", "id" => "1", "name" => "name1" }

      driver.instance.parse(csv_text) do |time, record|
        assert(time)
        assert_equal(expected_record, record)
      end
    end
  end

  sub_test_case "CSV header" do
    test "it extracts header by default" do
      driver = create_driver

      csv_text = "col1,col2,col3\n" \
                 "data1,data2,data3"

      expected_record = { "col1" => "data1", "col2" => "data2", "col3" => "data3" }

      driver.instance.parse(csv_text) do |time, record|
        assert(time)
        assert_equal(expected_record, record)
      end
    end

    test "it can use defined header keys" do
      config = %(has_header false
                 keys title_1,title_2,title_3)
      driver = create_driver(config)

      csv_text = "data_1,data_2,data_3\n" \
                 "data_11,data_12,data_13"

      expected_record = [
        { "title_1" => "data_1", "title_2" => "data_2", "title_3" => "data_3" },
        { "title_1" => "data_11", "title_2" => "data_12", "title_3" => "data_13" }
      ]

      index = 0
      driver.instance.parse(csv_text) do |time, record|
        assert(time)
        assert_equal(expected_record[index], record)
        index += 1
      end
    end

    test "it sets column number as keys when no header" do
      config = %(has_header false)
      driver = create_driver(config)

      csv_text = "data_1,data_2,data_3\n" \
                 "data_11,data_12,data_13"

      expected_record = [
        { 1 => "data_1", 2 => "data_2", 3 => "data_3" },
        { 1 => "data_11", 2 => "data_12", 3 => "data_13" }
      ]

      index = 0
      driver.instance.parse(csv_text) do |time, record|
        assert(time)
        assert_equal(expected_record[index], record)
        index += 1
      end
    end
  end

  private

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Parser.new(Fluent::Plugin::ParserStaticFileCsv).configure(conf)
  end
end

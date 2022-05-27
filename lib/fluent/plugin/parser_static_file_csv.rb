# frozen_string_literal: true

require "fluent/plugin/parser"

require "csv"

module Fluent
  module Plugin
    # Fluentd Parser for CSV Text
    #  responsible for parsing CSV content as a whole text
    class ParserStaticFileCsv < Parser
      Plugin.register_parser("static_file_csv", self)

      desc "Names of each fields"
      config_param :keys, :array, value_type: :string, default: []
      desc "The delimiter character (or String) of CSV values"
      config_param :delimiter, :string, default: ","
      desc "Files has header"
      config_param :has_header, :bool, default: true

      def configure(conf)
        super

        @parse_options = { col_sep: @delimiter,
                           headers: @has_header }
      end

      def parse(text)
        csv_content = CSV.parse(text, **@parse_options)
        csv_content.each do |row|
          r = if @has_header && row.respond_to?(:to_h)
                row.to_h
              else
                row_headers = @keys
                row_headers = (1..row.size) if row_headers.empty?

                row_headers.zip(row).to_h
              end

          time, record = convert_values(parse_time(r), r)
          yield time, record
        end
      end
    end
  end
end

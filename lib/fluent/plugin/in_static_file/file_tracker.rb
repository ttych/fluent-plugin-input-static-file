# frozen_string_literal: true

require "fluent/plugin/input"

module Fluent
  module Plugin
    class InStaticFile < Fluent::Plugin::Input
      # FileTracker stores handled file information
      class FileTracker
        FILE_TRACKER_ENTRY_REGEX = /^([^\t]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)\t([0-9a-fA-F]+)/.freeze
        FILE_TRACKER_ENTRY_FORMAT = "%s\t%016x\t%016x\t%016x\n"

        attr_reader :cache

        def initialize(file: nil, follow_inodes: false, logger: nil)
          @file = file
          @follow_inodes = follow_inodes
          @logger = logger

          @file_mutex = Mutex.new
          @cache = {}
        end

        def reload
          @cache = {}
          load_from_file
        end

        def load_from_file
          return unless @file

          @file_mutex.synchronize do
            @file.pos = 0

            @file.each_line do |line|
              m = FILE_TRACKER_ENTRY_REGEX.match(line)
              next if m.nil?

              path = m[1]
              ino = m[2].to_i(16)
              mtime_s = m[3].to_i(16)
              mtime_ns = m[4].to_i(16)

              file_info = FileInfo.new(path, ino, mtime_s, mtime_ns)
              key = @follow_inodes ? file_info.ino : file_info.path

              @cache[key] = file_info
            end
          end
        end

        def has?(file_info)
          return false unless file_info

          key = @follow_inodes ? file_info.ino : file_info.path

          return @cache[key] if @cache[key] == file_info
        end

        def add(file_info)
          return false unless file_info

          key = @follow_inodes ? file_info.ino : file_info.path
          @file_mutex.synchronize do
            @file.seek(0, IO::SEEK_END)
            @file.write(file_info.to_file_tracker_entry_format)
            @cache[key] = file_info
          end
        end

        def remove(file_info)
          return false unless file_info

          key = @follow_inodes ? file_info.ino : file_info.path
          @file_mutex.synchronize do
            @cache.delete(key)

            @file.pos = 0
            @file.truncate(0)
            @file.write(@cache.values.maps(&:to_file_tracker_entry_format).join)
          end
        end
      end

      # file information structure
      FileInfo = Struct.new(:path, :ino, :mtime_s, :mtime_ns) do
        def ==(other)
          return false unless other.is_a?(FileInfo)

          path == other.path && ino == other.ino && mtime_s == other.mtime_s && mtime_ns == other.mtime_ns
        end

        def hash
          path.hash
        end

        def eql?(other)
          self == other
        end

        def to_file_tracker_entry_format(format = FileTracker::FILE_TRACKER_ENTRY_FORMAT)
          format(format, path, ino, mtime_s, mtime_ns)
        end
      end
    end
  end
end

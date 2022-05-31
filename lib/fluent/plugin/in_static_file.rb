# frozen_string_literal: true

require "fluent/plugin/input"
require "fluent/config/error"

require "fluent/plugin/in_static_file/file_tracker"

if Fluent.windows?
  require "fluent/plugin/file_wrapper"
else
  Fluent::FileWrapper = File
end

module Fluent
  module Plugin
    # InStaticFile is an input plugin for file with static content
    class InStaticFile < Fluent::Plugin::Input
      PLUGIN_NAME = "static_file"

      Fluent::Plugin.register_input(PLUGIN_NAME, self)

      helpers :timer, :compat_parameters, :parser

      RESERVED_CHARS = ["/", "*", "%"].freeze

      desc "The paths to read. Multiple paths can be specified, separated by comma."
      config_param :path, :string
      desc "path delimiter used for spliting path config"
      config_param :path_delimiter, :string, default: ","
      desc "The paths to exclude the files from watcher list."
      config_param :exclude_path, :array, default: []

      desc "Limit the watching files that the modification time is within the specified time range (when use '*' in path)."
      config_param :limit_recently_modified, :time, default: nil
      desc "Limit the watching files that the modification time is within the specified time range (when use '*' in path)."
      config_param :limit_oldly_modified, :time, default: 5

      desc "The interval of refreshing the list of watch file."
      config_param :refresh_interval, :time, default: 30

      desc "The tag of the event."
      config_param :tag, :string
      desc "Add the log path being tailed to records. Specify the field name to be used."
      config_param :path_key, :string, default: nil

      desc "Fluentd will record the position it last read into this file."
      config_param :pos_file, :string, default: nil
      desc "Follow inodes instead of following file names. Guarantees more stable delivery and allows to use * in path pattern with rotating files"
      config_param :follow_inodes, :bool, default: false

      desc "When processed move to another location"
      config_param :archive_to, :string, default: nil

      def initialize
        super

        @paths = []
        @pf_file = nil
        @file_tracker = nil
        @ignore_list = []
      end

      def configure(conf)
        @variable_store = Fluent::VariableStore.fetch_or_build(:in_tail)

        compat_parameters_convert(conf, :parser)
        parser_config = conf.elements("parse").first
        raise Fluent::ConfigError, "<parse> section is required." unless parser_config

        super

        if RESERVED_CHARS.include?(@path_delimiter)
          rc = RESERVED_CHARS.join(", ")
          raise Fluent::ConfigError, "#{rc} are reserved words: #{@path_delimiter}"
        end

        @paths = @path.split(@path_delimiter).map(&:strip).uniq
        if @paths.empty?
          raise Fluent::ConfigError,
                "#{PLUGIN_NAME}: 'path' parameter is required on #{PLUGIN_NAME} input"
        end

        if @pos_file
          if @variable_store.key?(@pos_file) && !called_in_test?
            plugin_id_using_this_path = @variable_store[@pos_file]
            raise Fluent::ConfigError,
                  "Other '#{PLUGIN_NAME}' plugin already use same pos_file path: plugin_id = #{plugin_id_using_this_path}, pos_file path = #{@pos_file}"
          end
          @variable_store[@pos_file] = plugin_id
        else
          raise Fluent::ConfigError, "Can't follow inodes without pos_file configuration parameter" if @follow_inodes

          log.warn "'pos_file PATH' parameter is not set to a '#{PLUGIN_NAME}' source."
          log.warn "this parameter is highly recommended to save the file status."
        end

        @file_perm = system_config.file_permission || Fluent::DEFAULT_FILE_PERMISSION
        @dir_perm = system_config.dir_permission || Fluent::DEFAULT_DIR_PERMISSION

        @parser = parser_create(conf: parser_config)
      end

      def start
        super

        if @pos_file
          pos_file_dir = File.dirname(@pos_file)
          FileUtils.mkdir_p(pos_file_dir, mode: @dir_perm) unless Dir.exist?(pos_file_dir)
          @pf_file = File.open(@pos_file, File::RDWR | File::CREAT | File::BINARY, @file_perm)
          @pf_file.sync = true
        end

        @file_tracker = FileTracker.new(file: @pf_file, follow_inodes: @follow_inodes, logger: log)
        @file_tracker.reload

        timer_execute(:in_static_file_lookup, @refresh_interval, &method(:lookup_static_file_in_path))
      end

      def shutdown
        @pf_file&.close

        super
      end

      def resolve_paths
        date = Fluent::EventTime.now
        paths = []

        @paths.each do |path|
          path = date.to_time.strftime(path)
          if path.include?("*")
            paths += Dir.glob(path).select do |p|
              begin
                is_file = !File.directory?(p)
                if (File.readable?(p) || have_read_capability?) && is_file
                  if @limit_recently_modified && File.mtime(p) < (date.to_time - @limit_recently_modified)
                    false
                  else
                    !(@limit_oldly_modified && File.mtime(p) > (date.to_time - @limit_oldly_modified))
                  end
                else
                  if is_file && !@ignore_list.include?(p)
                    log.warn "#{p} unreadable. It is excluded and would be examined next time."
                    @ignore_list << p if @ignore_repeated_permission_error
                  end
                  false
                end
              rescue Errno::ENOENT, Errno::EACCES
                log.debug("#{p} is missing after refresh file list")
                false
              end
            end
          else
            paths << path
          end
        end

        excluded = @exclude_path.map do |path|
          path = date.to_time.strftime(path)
          path.include?("*") ? Dir.glob(path) : path
        end.flatten.uniq

        hash = {}
        (paths - excluded).select do |path|
          FileTest.exist?(path)
        end.each do |path|
          # Even we just checked for existence, there is a race condition here as
          # of which stat() might fail with ENOENT. See #3224.
          begin
            file_stat = Fluent::FileWrapper.stat(path)
            file_info = InStaticFile::FileInfo.new(path, file_stat.ino, file_stat.mtime.to_i, file_stat.mtime.tv_nsec)
            if @follow_inodes
              hash[file_info.ino] = file_info
            else
              hash[file_info.path] = file_info
            end
          rescue Errno::ENOENT, Errno::EACCES => e
            log.warn "expand_paths: stat() for #{path} failed with #{e.class.name}. Skip file."
          end
        end
        hash
      end

      def lookup_static_file_in_path
        detected_files = resolve_paths

        log.debug("detected: #{detected_files}")
        log.debug("cached: #{@file_tracker.cache}")

        to_untrack = @file_tracker.cache.reject do |key, value|
          detected_files[key] && detected_files[key] == value
        end

        untrack_files(to_untrack)
        process_files(detected_files)
      end

      def untrack_files(files_info)
        log.debug("#{PLUGIN_NAME}: untrack files: #{files_info.keys}")
        files_info.each do |_id, file_info|
          @file_tracker.remove(file_info)
        end
      end

      def process_files(files_info)
        log.debug("#{PLUGIN_NAME}: process files: #{files_info.keys}")
        files_info.each do |_id, file_info|
          process_file(file_info)
        end
      end

      def process_file(file_info)
        log.debug("#{PLUGIN_NAME}: process file: #{file_info.path}")

        return if @file_tracker.has?(file_info)

        File.open(file_info.path, "rb") do |f|
          @parser.parse(f) do |time, record|
            record[@path_key] ||= file_info.path unless @path_key.nil?
            router.emit(tag, time, record)
          end
        end

        @file_tracker.add(file_info)

        archive(file_info)
      end

      def archive(file_info)
        return if @archive_to.nil?

        file_name = File.basename(file_info.path)
        target_path = format(@archive_to, file_name)

        target_path_basedir = if target_path.end_with?("/")
                                target_path
                              else
                                File.dirname(target_path)
                              end
        FileUtils.mkdir_p(target_path_basedir, mode: @dir_perm)

        log.debug("#{PLUGIN_NAME}: archiving #{file_info.path} to #{target_path}")
        FileUtils.mv(file_info.path, target_path)
      rescue StandardError => e
        log.warn("#{PLUGIN_NAME}: can't archive #{file_info.path} to #{target_path}: #{e}")
      end
    end
  end
end

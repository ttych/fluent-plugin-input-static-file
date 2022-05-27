# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"
require "bump/tasks"
require "rubocop/rake_task"

RuboCop::RakeTask.new

Rake::TestTask.new(:test) do |t|
  t.libs.push("lib", "test")
  t.test_files = FileList["test/**/test_*.rb"]
  t.verbose = false
  t.warning = false
end

task default: %i[test rubocop]

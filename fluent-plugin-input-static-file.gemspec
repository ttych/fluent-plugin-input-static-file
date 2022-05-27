# frozen_string_literal: true

require_relative "lib/fluent/plugin/input_static_file/version"

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-input-static-file"
  spec.version       = Fluent::Plugin::InputStaticFile::VERSION
  spec.authors       = ["Thomas Tych"]
  spec.email         = ["thomas.tych@gmail.com"]

  spec.summary       = "fluentd plugin to ingest static file"
  spec.description   = "Plugin to manage file as a global block " \
                       "in opposition to a line or multiline block as with in_tail."
  spec.homepage      = "https://gitlab.com/ttych/fluent-plugin-input-static-file"
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.4.0")

  # spec.metadata["allowed_push_host"] = ""

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      f.match(%r{\A(?:test|spec|features)/})
    end
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bump", "~> 0.10.0"
  spec.add_development_dependency "bundler"
  spec.add_development_dependency "byebug", "~> 11.1"
  spec.add_development_dependency "flay", "~> 2.12", ">= 2.12.1"
  spec.add_development_dependency "flog", "~> 4.6", ">= 4.6.4"
  spec.add_development_dependency "parallel", "~> 1.19.2"
  spec.add_development_dependency "rake", "~> 13.0", ">= 13.0.6"
  spec.add_development_dependency "reek", "~> 6.0.6"
  spec.add_development_dependency "rubocop", "~> 1.12.1"
  spec.add_development_dependency "rubocop-ast", "~> 1.4.1"
  spec.add_development_dependency "rubocop-rake", "~> 0.5.1"
  spec.add_development_dependency "test-unit", "~> 3.5.3"

  spec.add_runtime_dependency "fluentd", ">= 0.14.10", "< 2"
end

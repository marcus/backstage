# frozen_string_literal: true

require_relative "lib/backstage/version"

Gem::Specification.new do |spec|
  spec.name = "backstage-agent"
  spec.version = Backstage::VERSION
  spec.summary = "Adapter-driven work orchestration for agents"
  spec.authors = ["Marcus Vorwaller"]
  spec.email = "marcus@vorwaller.net"
  spec.license = "MIT"
  spec.homepage = "https://github.com/marcus/backstage"
  spec.required_ruby_version = ">= 4.0"
  spec.files = Dir["bin/*", "lib/**/*.rb", "schemas/**/*.json"]
  spec.bindir = "bin"
  spec.executables = ["backstage"]
  spec.require_paths = ["lib"]
  spec.add_dependency "base64", "~> 0.3"
end

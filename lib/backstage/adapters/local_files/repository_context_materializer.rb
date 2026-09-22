# frozen_string_literal: true

require "fileutils"

module Backstage::Adapters::LocalFiles
  class RepositoryContextMaterializer
    AuthorityError = Backstage::AuthorityError
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    def initialize(repository_adapter:, workspace_root:)
      @repository_adapter = repository_adapter
      @workspace_root = File.expand_path(workspace_root)
    end

    def materialize(grants)
      Array(grants).map do |grant|
        raise ContractError, "unsupported context grant #{grant["kind"]}" unless grant["kind"] == "repo"

        destination = safe_destination(grant.fetch("mount"))
        @repository_adapter.clone(
          origin: grant.fetch("origin"),
          revision: grant.fetch("revision"),
          destination: destination,
          credential_ref: grant["credential_ref"],
          read_only: true
        )
        make_read_only(destination)
        {
          "kind" => "repo",
          "name" => grant.fetch("name"),
          "origin" => grant.fetch("origin"),
          "revision" => grant.fetch("revision"),
          "mount" => grant.fetch("mount"),
          "host_path" => destination,
          "read_only" => true,
          "materialized_at" => Records.timestamp
        }
      end
    end

    private

    def safe_destination(mount)
      destination = File.expand_path(mount, @workspace_root)
      raise AuthorityError, "context mount escapes workspace" unless destination.start_with?("#{@workspace_root}/")

      destination
    end

    def make_read_only(destination)
      FileUtils.chmod_R("a-w", destination)
    end
  end
end

Backstage::RepositoryContextMaterializer = Backstage::Adapters::LocalFiles::RepositoryContextMaterializer unless defined?(Backstage::RepositoryContextMaterializer)

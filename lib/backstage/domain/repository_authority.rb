# frozen_string_literal: true

module Backstage::Domain
  class RepositoryAuthority
    AuthorityError = Backstage::AuthorityError
    BRANCH = %r{\Abackstage/[a-z0-9][a-z0-9._/-]*\z}

    def initialize(designated_repository:, default_branch:)
      @designated_repository = designated_repository.downcase
      @default_branch = default_branch
    end

    def check_origin!(origin)
      raise AuthorityError, "repository origin must use HTTPS" unless origin.start_with?("https://github.com/")
      raise AuthorityError, "repository is outside configured authority" unless identity(origin) == @designated_repository

      true
    end

    def check_branch!(branch)
      invalid = branch == @default_branch || branch == "refs/heads/#{@default_branch}" || !branch.match?(BRANCH) || ["..", "@{"].any? { |part| branch.include?(part) } || branch.end_with?(".")
      raise AuthorityError, "branch #{branch.inspect} is outside configured authority" if invalid

      true
    end

    def check_action!(action)
      raise AuthorityError, "action #{action} is forbidden" unless %w[clone push_branch create_draft_pr].include?(action.to_s)
    end

    def check_review!(repository:, base:)
      raise AuthorityError, "repository is outside configured authority" unless repository.to_s.downcase == @designated_repository
      raise AuthorityError, "base branch is outside configured authority" unless base == @default_branch

      true
    end

    def check_checkout!(origin:, actual_branch:, expected_branch:)
      check_origin!(origin)
      check_branch!(expected_branch)
      raise AuthorityError, "workspace branch changed outside configured authority" unless actual_branch == expected_branch

      true
    end

    def check_review_response!(repository:, branch:, expected_branch:, base:, draft:)
      check_review!(repository: repository, base: base)
      check_branch!(branch)
      raise AuthorityError, "review head branch is outside configured authority" unless branch == expected_branch
      raise AuthorityError, "existing review change is not a draft" unless draft == true

      true
    end

    private

    def identity(origin)
      origin.delete_prefix("https://github.com/").delete_suffix(".git").downcase
    end
  end
end

Backstage::RepositoryAuthority = Backstage::Domain::RepositoryAuthority unless defined?(Backstage::RepositoryAuthority)

# frozen_string_literal: true

module Backstage::Adapters::GitHub
  class ReviewChange
    AuthorityError = Backstage::AuthorityError
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    def initialize(store:, authority:)
      @store = store
      @authority = authority
    end

    def begin_publication(work_item_id:, repository:, branch:, base:, idempotency_key:)
      validate_expected!(repository: repository, branch: branch, base: base)
      existing = @store.find("external_actions", idempotency_key: idempotency_key)
      validate_existing!(existing, repository: repository, branch: branch, base: base) if existing
      return existing if existing

      @store.save("external_actions", Records.external_action(
        work_item_id: work_item_id,
        kind: "github_draft_pr",
        idempotency_key: idempotency_key,
        status: "pending",
        reference: { "repository" => repository, "branch" => branch, "base" => base }
      ))
    end

    def complete_publication(work_item_id:, repository:, branch:, base:, idempotency_key:, response:)
      validate_expected!(repository: repository, branch: branch, base: base)
      validate_response!(response, repository: repository, branch: branch, base: base)
      action = @store.find("external_actions", idempotency_key: idempotency_key)
      raise ContractError, "draft publication has no pending external action" unless action

      validate_existing!(action, repository: repository, branch: branch, base: base)
      @store.save("external_actions", action.merge("status" => "succeeded", "response" => response, "updated_at" => Records.timestamp))
      response
    end

    private

    def validate_expected!(repository:, branch:, base:)
      @authority.check_action!(:create_draft_pr)
      @authority.check_review!(repository: repository, base: base)
      @authority.check_branch!(branch)
    end

    def validate_response!(response, repository:, branch:, base:)
      raise ContractError, "container publication response must be an object" unless response.is_a?(Hash)

      @authority.check_review_response!(
        repository: response.fetch("repository"),
        branch: response.fetch("branch"),
        expected_branch: branch,
        base: response.fetch("base"),
        draft: response.fetch("draft")
      )
      raise AuthorityError, "publication response repository does not match request" unless response.fetch("repository").downcase == repository.downcase
      raise AuthorityError, "publication response base does not match request" unless response.fetch("base") == base
      response.fetch("url")
    rescue KeyError => error
      raise ContractError, "invalid container publication response: #{error.message}"
    end

    def validate_existing!(action, repository:, branch:, base:)
      reference = action["reference"] || action["response"]
      raise ContractError, "draft publication ledger has no authority reference" unless reference
      raise AuthorityError, "draft publication ledger repository mismatch" unless reference["repository"].to_s.downcase == repository.downcase
      raise AuthorityError, "draft publication ledger branch mismatch" unless reference["branch"] == branch
      raise AuthorityError, "draft publication ledger base mismatch" unless reference["base"] == base
      validate_response!(action["response"], repository: repository, branch: branch, base: base) if action["status"] == "succeeded"
    end
  end
end

Backstage::GitHubReviewChange = Backstage::Adapters::GitHub::ReviewChange unless defined?(Backstage::GitHubReviewChange)

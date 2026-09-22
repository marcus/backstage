# frozen_string_literal: true

require_relative "test_helper"

class ContractValidatorTest < Minitest::Test
  def test_job_bundle_fixture_validates_repository_context_grants
    bundle = JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
    bundle["context_grants"] << {
      "kind" => "repo",
      "name" => "td",
      "origin" => "https://github.com/example/tracker.git",
      "revision" => "main",
      "mount" => "refs/td",
      "read_only" => true,
      "credential_ref" => "github"
    }

    assert_same bundle, Backstage::ContractValidator.new.validate!("job-bundle-v1.json", bundle)
  end

  def test_invalid_bundle_reports_boundary_path
    bundle = JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
    bundle["execution"].delete("image")

    error = assert_raises(Backstage::ContractError) { Backstage::ContractValidator.new.validate!("job-bundle-v1.json", bundle) }
    assert_includes error.message, "$.execution.image"
  end

  def test_context_grant_and_review_provenance_are_fully_validated
    validator = Backstage::ContractValidator.new
    bundle = JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
    bundle["context_grants"] = [{ "unexpected" => "accepted" }]
    assert_raises(Backstage::ContractError) { validator.validate!("job-bundle-v1.json", bundle) }

    outcome = { "schema_version" => 1, "status" => "succeeded", "summary" => "reviewed", "process" => { "exit_code" => 0, "signal" => nil }, "review" => { "verdict" => "approved" } }
    error = assert_raises(Backstage::ContractError) { validator.validate!("outcome-v1.json", outcome) }
    assert_includes error.message, "reviewer_session_id"
  end

  def test_context_grant_rejects_exact_name_and_mount_traversal_boundaries
    validator = Backstage::ContractValidator.new
    valid = JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
    grant = {
      "kind" => "repo",
      "name" => "td",
      "origin" => "https://github.com/example/tracker.git",
      "revision" => "main",
      "mount" => "refs/td",
      "read_only" => true
    }

    escaped_name = JSON.parse(JSON.generate(valid)).merge("context_grants" => [grant.merge("name" => "../../escape")])
    error = assert_raises(Backstage::ContractError) { validator.validate!("job-bundle-v1.json", escaped_name) }
    assert_includes error.message, "$.context_grants[0].name"

    escaped_mount = JSON.parse(JSON.generate(valid)).merge("context_grants" => [grant.merge("mount" => "refs/../../escape")])
    error = assert_raises(Backstage::ContractError) { validator.validate!("job-bundle-v1.json", escaped_mount) }
    assert_includes error.message, "$.context_grants[0].mount"
  end
end

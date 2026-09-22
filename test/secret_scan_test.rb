# frozen_string_literal: true

require_relative "test_helper"
require "open3"

class SecretScanTest < Minitest::Test
  def test_scan_reports_name_and_path_without_echoing_secret
    in_tmpdir do |directory|
      clean = File.join(directory, "clean.jsonl")
      leaked = File.join(directory, "leaked.txt")
      File.write(clean, "safe")
      File.write(leaked, "prefix canary-value suffix")
      script = File.expand_path("../scripts/scan-secrets", __dir__)

      clean_out, clean_error, clean_status = Open3.capture3({ "BACKSTAGE_CANARY_TOKEN" => "canary-value" }, script, clean)
      _leak_out, leak_error, leak_status = Open3.capture3({ "BACKSTAGE_CANARY_TOKEN" => "canary-value" }, script, leaked)

      assert clean_status.success?, clean_error
      assert_includes clean_out, "credential scan clean"
      refute leak_status.success?
      assert_includes leak_error, "BACKSTAGE_CANARY_TOKEN"
      assert_includes leak_error, leaked
      refute_includes leak_error, "canary-value"
    end
  end
end

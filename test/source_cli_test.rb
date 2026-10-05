# frozen_string_literal: true

require_relative "test_helper"

class SourceCLITest < Minitest::Test
  SOURCES_PACK = File.expand_path("../packs/sources-example", __dir__)

  def invoke(directory, *arguments, pack: SOURCES_PACK)
    out, err = StringIO.new, StringIO.new
    code = Backstage::CLI.new(["--state", File.join(directory, "state.jsonl"), "--artifacts", File.join(directory, "artifacts"),
      "--pack", pack, "--json", *arguments], out: out, err: err, env: {}).call
    [code, code.zero? ? JSON.parse(out.string) : JSON.parse(err.string)]
  end

  def json(directory, *arguments, **options)
    code, result = invoke(directory, *arguments, **options)
    assert_equal 0, code, result.inspect
    result
  end

  def test_native_source_admission_queue_review_and_repeatable_delivery
    in_tmpdir do |directory|
      native = json(directory, "source", "list").find { |row| row["connection"] == "native" }
      assert_equal %w[record_result mark_ready], native.fetch("capabilities")
      work = json(directory, "source", "submit", "native", "Widget/Case-17")
      assert_equal "Widget/Case-17", work.dig("source", "ref")
      assert_equal "native", work.dig("source", "connection")
      assert_equal work.fetch("id"), json(directory, "source", "poll", "native").first.fetch("id")
      code, denied = invoke(directory, "deliver", work.fetch("id"), "--operation", "record_result")
      assert_equal 1, code
      assert_match(/independent candidate-bound approval/, denied.dig("error", "message"))
      json(directory, "dispatch", "accept", work.fetch("id"))
      assert_equal 1, json(directory, "dispatch", "pass").fetch("dispatched")
      completed = json(directory, "show", work.fetch("id"))
      assert_equal "completed", completed.fetch("state")
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      bundle = store.list("jobs").first.fetch("bundle")
      assert_equal work.fetch("input"), bundle.dig("work_item", "input")
      first = json(directory, "deliver", work.fetch("id"), "--operation", "record_result", "--operation", "mark_ready")
      assert_equal "succeeded", first.fetch("status")
      assert_equal %w[record_result mark_ready], first.fetch("actions").map { |row| row.fetch("operation") }
      again = json(directory, "deliver", work.fetch("id"), "--operation", "record_result", "--operation", "mark_ready")
      assert_equal first.fetch("actions"), again.fetch("actions")
      assert_equal first.fetch("actions").first, json(directory, "deliver", "show", first.fetch("actions").first.fetch("id"))
    end
  end

  def test_manual_file_input_has_no_source_and_cannot_deliver
    in_tmpdir do |directory|
      document = "{\"custom\": [\"Ä\", 3], \"status\":\"native\"}\n"
      path = File.join(directory, "native.json")
      File.write(path, document)
      work = json(directory, "submit", "--title", "Native document", "--input-file", path, "--media-type", "application/json", pack: PACK)
      assert_equal document, work.dig("input", "content")
      assert_equal "application/json", work.dig("input", "media_type")
      assert_equal Digest::SHA256.hexdigest(document), work.dig("input", "sha256")
      refute work.key?("source")
      assert_empty json(directory, "source", "list", pack: PACK)
      json(directory, "process", work.fetch("id"), pack: PACK)
      code, denied = invoke(directory, "deliver", work.fetch("id"), "--operation", "record_result", pack: PACK)
      assert_equal 1, code
      assert_match(/manual work has no source/, denied.dig("error", "message"))
    end
  end

  def test_refreshed_completion_cannot_deliver_and_replacement_requires_acceptance
    in_tmpdir do |directory|
      pack_path = File.join(directory, "pack")
      FileUtils.cp_r(SOURCES_PACK, pack_path)
      work = json(directory, "source", "submit", "native", "Widget/Case-17", pack: pack_path)
      json(directory, "process", work.fetch("id"), pack: pack_path)
      path = File.join(pack_path, "fixtures", "native-jobs.json")
      document = JSON.parse(File.read(path))
      document.fetch("jobs").first.merge!("body" => "Changed native requirements", "revision" => "v2")
      File.write(path, JSON.generate(document))
      replacement = json(directory, "source", "refresh", work.fetch("id"), pack: pack_path)
      refute_equal work.fetch("id"), replacement.fetch("id")
      assert_equal "ready", replacement.fetch("state")
      assert_equal "Changed native requirements", replacement.dig("input", "content")
      assert_empty json(directory, "dispatch", "list", pack: pack_path)
      code, denied = invoke(directory, "deliver", work.fetch("id"), "--operation", "record_result", pack: pack_path)
      assert_equal 1, code
      assert_match(/refreshed/, denied.dig("error", "message"))
      json(directory, "dispatch", "accept", replacement.fetch("id"), pack: pack_path)
      assert_equal 1, json(directory, "dispatch", "pass", pack: pack_path).fetch("dispatched")
      assert_equal "succeeded", json(directory, "deliver", replacement.fetch("id"), "--operation", "record_result", pack: pack_path).fetch("status")
    end
  end
end

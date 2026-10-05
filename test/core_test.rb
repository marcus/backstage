# frozen_string_literal: true

require_relative "test_helper"

class CoreTest < Minitest::Test
  def test_submit_deduplicates_and_binds_its_workflow
    in_tmpdir do |directory|
      engine = build_engine(directory)
      first = submit_work(engine, key: "same", title: "Do work")
      second = submit_work(engine, key: "same", title: "Ignored")

      assert_equal first["id"], second["id"]
      assert_equal "ready", first.fetch("state")
      assert_equal 0, first.fetch("revision")
      assert_equal "independent-review", first.dig("workflow", "name")
      assert_equal workflow("independent-review").digest, first.dig("workflow", "digest")
      refute first["workflow"].key?("definition"), "the definition belongs in its snapshot, not on every work item"
      assert engine.store.fetch("workflow_snapshots", first.dig("workflow", "digest"))
    end
  end

  def test_work_routing_binding_is_immutable
    in_tmpdir do |directory|
      engine = build_engine(directory)
      first = submit_work(engine, key: "bound", target: "one")
      assert_equal "one", first["target"]
      assert_raises(Backstage::ContractError) do
        submit_work(engine, key: "bound", target: "two")
      end
    end
  end

  def test_native_input_is_opaque_bounded_and_core_digested
    in_tmpdir do |directory|
      engine = build_engine(directory)
      content = '{"password":"task field, not a credential", "custom": [1,2]}'
      work = submit_work(engine, key: "native", input: { content: content, media_type: "application/json" })
      assert_equal content, work.dig("input", "content")
      assert_equal Digest::SHA256.hexdigest(content), work.dig("input", "sha256")
      refute work.key?("description")
      assert_raises(Backstage::ContractError) { submit_work(engine, key: "digest", input: { content: "x", media_type: "text/plain", sha256: "forged" }) }
      assert_raises(Backstage::ContractError) { submit_work(engine, key: "large", input: { content: "x" * (Backstage::Engine::INPUT_BYTE_LIMIT + 1), media_type: "text/plain" }) }
      assert_raises(Backstage::ContractError) { submit_work(engine, key: "invalid", input: { content: "\xff".b, media_type: "text/plain" }) }
    end
  end

  def test_atomic_admission_deduplicates_concurrent_engines
    in_tmpdir do |directory|
      engines = Array.new(4) { build_engine(directory) }
      work = engines.map { |engine| Thread.new { submit_work(engine, key: "concurrent") } }.map(&:value)
      assert_equal 1, work.map { |row| row.fetch("id") }.uniq.length
      assert_equal 1, engines.first.store.list("work_items").length
      assert_equal 1, engines.first.store.read_activity(filters: { type: "work.admitted" }).fetch("events").length
    end
  end

  def test_store_contract_persists_latest_snapshot
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      store.save("things", { "id" => "thing-1", "status" => "new" })
      store.save("things", { "id" => "thing-1", "status" => "done" })

      assert_equal "done", store.fetch("things", "thing-1")["status"]
      assert_equal 1, store.list("things").length
      assert_equal "thing-1", store.find("things", status: "done")["id"]
    end
  end

  def test_commit_is_all_or_nothing_against_an_expected_revision
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      store.save("items", { "id" => "item-1", "revision" => 1 })

      store.commit(
        [["items", { "id" => "item-1", "revision" => 2 }], ["history", { "id" => "row-1" }]],
        expect: [{ collection: "items", id: "item-1", revision: 1 }]
      )
      assert_equal 2, store.fetch("items", "item-1").fetch("revision")
      assert_equal 1, store.list("history").length

      error = assert_raises(Backstage::ConflictError) do
        store.commit(
          [["items", { "id" => "item-1", "revision" => 3 }], ["history", { "id" => "row-2" }]],
          expect: [{ collection: "items", id: "item-1", revision: 1 }]
        )
      end
      assert_match(/revision 2, not 1/, error.message)
      assert_equal 2, store.fetch("items", "item-1").fetch("revision")
      assert_equal 1, store.list("history").length, "a refused commit writes nothing at all"
    end
  end

  def test_state_and_artifacts_reject_secrets
    in_tmpdir do |directory|
      engine = build_engine(directory, secrets: ["super-secret-value"])
      assert_raises(Backstage::ContractError) do
        submit_work(engine, key: "x", title: "super-secret-value")
      end
      assert_raises(Backstage::ContractError) do
        submit_work(engine, key: "x", title: "x", input: { content: "super-secret-value", media_type: "application/json" })
      end
    end
  end
end

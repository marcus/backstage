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
      first = submit_work(engine, key: "bound", target: "one", source_instance: "source-one", source_identity: "/one")
      assert_equal "one", first["target"]
      assert_raises(Backstage::ContractError) do
        submit_work(engine, key: "bound", target: "two", source_instance: "source-two", source_identity: "/two")
      end
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
        submit_work(engine, key: "x", title: "x", source_ref: { "api_token" => "oops" })
      end
    end
  end
end

# frozen_string_literal: true

require_relative "test_helper"
require "timeout"

class JsonlAtomicityTest < Minitest::Test
  def test_every_truncated_batch_is_invisible_and_repaired_before_next_append
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = Backstage::JsonlStore.new(path)
      store.save("jobs", { id: "old", state: "queued" })
      baseline = File.binread(path)
      store.commit([["jobs", { id: "new", state: "running" }], ["runs", { id: "run-new" }]])
      batch = File.binread(path).byteslice(baseline.bytesize..)

      # Includes a syntactically complete JSON object missing its committing newline.
      (1...batch.bytesize).each do |length|
        File.binwrite(path, baseline + batch.byteslice(0, length))
        reopened = Backstage::JsonlStore.new(path)
        assert_nil reopened.fetch("jobs", "new"), "partial batch at byte #{length}"
        assert_empty reopened.list("runs")
        reopened.save("jobs", { id: "recovered" })
        assert_equal %w[old recovered], reopened.list("jobs").map { |record| record["id"] }
        assert_equal 2, File.readlines(path).length
      end
    end
  end

  def test_complete_legacy_events_can_precede_new_transactions
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      File.write(path, JSON.generate(collection: "jobs", record: { id: "legacy" }) + "\n")
      store = Backstage::JsonlStore.new(path)
      store.commit([["jobs", { id: "new" }], ["runs", { id: "run" }]])
      assert_equal %w[legacy new], store.list("jobs").map { |record| record["id"] }
      assert_equal "run", store.fetch!("runs", "run")["id"]
    end
  end

  def test_complete_corruption_is_never_hidden_or_truncated
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = Backstage::JsonlStore.new(path)
      store.save("jobs", { id: "old" })
      valid = File.binread(path)
      ["{broken}\n", "{\"transaction_version\":1,\"events\":{}}\n"].each do |corruption|
        ["", valid, "{incomplete"].each do |tail|
          contents = valid + corruption + tail
          File.binwrite(path, contents)
          assert_raises(Backstage::ContractError) { store.list("jobs") }
          assert_raises(Backstage::ContractError) { store.save("jobs", { id: "new" }) }
          assert_equal contents, File.binread(path)
        end
      end
    end
  end

  def test_field_and_revision_guards_reject_entire_batch
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      store.save("jobs", { id: "job", state: "queued", revision: 2 })
      writes = [["jobs", { id: "job", state: "running", revision: 3 }], ["runs", { id: "run" }]]
      assert_raises(Backstage::ConflictError) do
        store.commit(writes, expect: [{ collection: "jobs", id: "job", revision: 1, fields: { state: "queued" } }])
      end
      assert_raises(Backstage::ConflictError) do
        store.commit(writes, expect: [{ collection: "jobs", id: "job", fields: { state: "running" } }])
      end
      assert_empty store.list("runs")
      store.commit(writes, expect: [{ collection: "jobs", id: "job", revision: 2, fields: { state: "queued", run_id: nil } },
                                   { collection: "runs", id: "run", revision: nil }])
      assert_equal "running", store.fetch!("jobs", "job")["state"]
      assert_equal "run", store.fetch!("runs", "run")["id"]
    end
  end

  def test_concurrent_processes_cannot_both_claim_a_job
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = Backstage::JsonlStore.new(path)
      store.save("jobs", { id: "job", state: "queued" })
      start_read, start_write = IO.pipe
      children = 2.times.map do |number|
        fork do
          start_write.close
          start_read.read(1)
          local = Backstage::JsonlStore.new(path)
          begin
            local.commit([["jobs", { id: "job", state: "running", run_id: "run-#{number}" }],
                          ["runs", { id: "run-#{number}" }]],
                         expect: [{ collection: "jobs", id: "job", fields: { state: "queued", run_id: nil } }])
            exit! 0
          rescue Backstage::ConflictError
            exit! 2
          end
        end
      end
      start_read.close
      start_write.write("xx")
      start_write.close
      statuses = children.map { |pid| Process.wait2(pid).last.exitstatus }
      assert_equal [0, 2], statuses.sort
      assert_equal [store.fetch!("jobs", "job")["run_id"]], store.list("runs").map { |record| record["id"] }
    end
  end

  def test_reader_waits_for_writer_and_crashed_writer_leaves_no_partial_batch
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = Backstage::JsonlStore.new(path)
      store.save("jobs", { id: "old" })
      baseline = File.binread(path)
      ready_read, ready_write = IO.pipe
      child = fork do
        ready_read.close
        File.open(path, "ab") do |file|
          file.flock(File::LOCK_EX)
          file.write('{"transaction_version":1,"events":[{"collection":"jobs","record":{"id":"partial"}}')
          file.flush
          ready_write.write("x")
          ready_write.close
          sleep
        end
        exit! 0
      end
      ready_write.close
      ready_read.read(1)
      ready_read.close
      results = Queue.new
      started = Queue.new
      reader = Thread.new do
        started << true
        results << store.list("jobs")
      end
      started.pop
      assert_raises(Timeout::Error) { Timeout.timeout(0.1) { results.pop } }
      Process.kill("KILL", child)
      Process.wait(child)
      child = nil
      rows = Timeout.timeout(5) { results.pop }
      assert_equal ["old"], rows.map { |record| record["id"] }
      reader.join
      store.save("jobs", { id: "recovered" })
      assert File.binread(path).start_with?(baseline)
      assert_equal %w[old recovered], store.list("jobs").map { |record| record["id"] }
    ensure
      if child
        Process.kill("KILL", child) rescue nil
        Process.wait(child) rescue nil
      end
      reader&.kill if reader&.alive?
    end
  end
end

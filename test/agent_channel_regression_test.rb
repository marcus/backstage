# frozen_string_literal: true

require_relative "test_helper"

class AgentChannelRegressionTest < Minitest::Test
  Channel = Backstage::Adapters::LocalFiles::AgentRequestChannel

  def with_channel
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      job = dispatch(engine, work)
      run = Backstage::Domain::Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"), phase: "implementation", work_revision: job.fetch("work_revision")).merge("status" => "running")
      engine.store.save("runs", run)
      engine.store.save("jobs", job.merge("status" => "running", "run_id" => run.fetch("id")))
      channel = Channel.new(File.join(directory, "channel"), workflow_service: build_workflows(engine), store: engine.store)
      yield engine, work, run, channel
    end
  end

  def append(channel, payload)
    File.open(channel.path, "a") { |file| file.puts(JSON.generate(payload)) }
  end

  def test_live_progress_and_partial_lines_are_applied_once
    with_channel do |engine, work, run, channel|
      line = JSON.generate("transition" => "report_progress", "request_id" => "live", "expected_revision" => 1)
      File.write(channel.path, line)
      assert_empty channel.drain(run: run)
      File.open(channel.path, "a") { |file| file.write("\n") }
      result = channel.drain(run: run)
      assert_equal "applied", result.first["status"]
      assert_equal "running", engine.store.fetch!("runs", run.fetch("id"))["status"]
      assert_equal 2, engine.store.fetch!("work_items", work.fetch("id"))["revision"]
      assert_empty channel.drain(run: run)
    end
  end

  def test_worker_can_supply_a_concrete_human_question_but_cannot_choose_new_authority
    with_channel do |engine, work, run, channel|
      append(channel, "transition" => "escalate", "request_id" => "question", "decision" => {"question" => "May I change the public protocol?", "choices" => ["cancel"], "context" => {"finding" => "protocol mismatch"}})
      assert_equal "applied", channel.drain(run: run).first["status"]
      decision = build_workflows(engine).open_decision(engine.store.fetch!("work_items", work.fetch("id")))
      assert_equal "May I change the public protocol?", decision["question"]
      assert_equal ["cancel"], decision["choices"]
      assert_equal "protocol mismatch", decision.dig("context", "finding")
    end
    with_channel do |_engine, _work, run, channel|
      append(channel, "transition" => "cancel", "actor" => {"role" => "human"})
      assert_equal "rejected", channel.drain(run: run).first["status"]
      append(channel, "transition" => "escalate", "decision" => {"choices" => ["approve"]})
      assert_equal "rejected", channel.drain(run: run).first["status"]
    end
  end

  def test_stale_revision_cancelled_and_replaced_workers_cannot_change_work
    %w[revision cancelled replaced].each do |condition|
      with_channel do |engine, work, run, channel|
        case condition
        when "cancelled"
          engine.store.save("runs", run.merge("cancellation_requested_at" => Backstage::Domain::Records.timestamp))
        when "replaced"
          job = engine.store.fetch!("jobs", run.fetch("job_id"))
          engine.store.save("jobs", job.merge("run_id" => "replacement"))
        end
        append(channel, "transition" => "report_progress", "expected_revision" => (condition == "revision" ? 0 : 1))
        assert_equal "rejected", channel.drain(run: run).first["status"], condition
        assert_equal 1, engine.store.fetch!("work_items", work.fetch("id"))["revision"]
      end
    end
  end

  def test_symlink_to_host_file_is_never_read_or_reported
    with_channel do |engine, _work, run, channel|
      host_file = File.join(File.dirname(File.dirname(channel.path)), "host-private")
      File.write(host_file, "private-host-sentinel\n")
      File.unlink(channel.path)
      File.symlink(host_file, channel.path)
      assert_equal "rejected", channel.drain(run: run).first["status"]
      refute_includes JSON.generate(engine.store.list("agent_requests")), "private-host-sentinel"
      assert_empty channel.drain(run: run)
      assert_equal 1, engine.store.list("agent_requests").length
    end
  end

  def test_replacement_fifo_and_truncation_close_the_channel_visibly
    %w[replacement fifo truncated].each do |condition|
      with_channel do |engine, _work, run, channel|
        append(channel, "transition" => "report_progress")
        channel.drain(run: run)
        if condition == "truncated"
          File.truncate(channel.path, 0)
        else
          File.rename(channel.path, "#{channel.path}.old")
          condition == "fifo" ? File.mkfifo(channel.path) : File.write(channel.path, "{}\n")
        end
        assert_equal "rejected", channel.drain(run: run).first["status"], condition
        assert_empty channel.drain(run: run)
        assert_equal 2, engine.store.list("agent_requests").length
      end
    end
  end

  def test_parser_diagnostics_never_copy_rejected_input
    with_channel do |engine, _work, run, channel|
      File.write(channel.path, "private-input-sentinel\n")
      assert_equal "rejected", channel.drain(run: run).first["status"]
      refute_includes JSON.generate(engine.store.list("agent_requests")), "private-input-sentinel"
    end
  end

  def test_request_flood_has_one_terminal_rejection_and_bounded_persisted_rows
    with_channel do |engine, _work, run, channel|
      File.write(channel.path, "x\n" * (Channel::MAX_REQUESTS + 1000))
      result = channel.drain(run: run)
      assert_equal Channel::MAX_REQUESTS + 1, result.length
      assert_match(/exceeded .* requests/, result.last["error"])
      assert_equal Channel::MAX_REQUESTS + 1, engine.store.list("agent_requests").length
      File.open(channel.path, "a") { |file| file.write("x\n" * 1000) }
      assert_empty channel.drain(run: run)
      assert_equal Channel::MAX_REQUESTS + 1, engine.store.list("agent_requests").length
    end
  end

  def test_file_and_line_limits_close_once_without_processing_excess_contents
    [Channel::MAX_LINE_BYTES + 1, Channel::MAX_FILE_BYTES + 1].each do |size|
      with_channel do |engine, _work, run, channel|
        File.write(channel.path, "x" * size + "\n")
        assert_equal "rejected", channel.drain(run: run).first["status"]
        assert_empty channel.drain(run: run)
        assert_equal 1, engine.store.list("agent_requests").length
      end
    end
  end
end

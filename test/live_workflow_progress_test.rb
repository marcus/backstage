# frozen_string_literal: true
require_relative "controller_test"

class LiveWorkflowProgressTest < Minitest::Test
  def test_live_worker_can_move_between_progress_states_and_still_complete
    in_tmpdir do |directory|
      engine = build_engine(directory)
      definition = workflow("minimal").to_h
      definition["states"]["investigating"] = {}
      definition["transitions"]["investigate"] = { "from" => ["in_progress"], "to" => "investigating", "actors" => ["agent"] }
      definition["transitions"]["report"] = { "from" => ["investigating"], "to" => "investigating", "actors" => ["agent"] }
      definition["transitions"]["finish"]["from"] << "investigating"
      definition["transitions"]["reset"]["from"] << "investigating"
      work = engine.submit(idempotency_key: "live", title: "live progress", description: "", workflow: Backstage::Domain::Workflow.compile(definition))
      service = build_workflows(engine)
      test = self
      runner_factory = lambda do |_phase, bundle, _work|
        runner = Object.new
        runner.define_singleton_method(:run) do |bundle:, secrets: {}, cancellation: nil, capture: nil, &emit|
          test.assert_includes bundle.fetch("workflow_context").fetch("transitions").map { |row| row.fetch("name") }, "investigate"
          path = bundle.dig("agent_request_channel", "path")
          File.open(path, "a") { |file| file.puts(JSON.generate("transition" => "investigate", "request_id" => "first")) }
          emit.call("type" => "heartbeat")
          test.assert_equal "investigating", engine.show_work(work.fetch("id")).fetch("state"), "must become visible before the outcome exists"
          test.assert_equal 1, engine.store.list("jobs").length, "progress dispatches nothing"
          test.assert_nil engine.store.list("runs").first["outcome"]
          test.refute cancellation.call, "the current worker retains authority after its own progress"
          File.open(path, "a") { |file| file.puts(JSON.generate("transition" => "report", "request_id" => "second")) }
          emit.call("type" => "heartbeat")
          test.assert_equal 2, engine.store.list("agent_requests").count { |row| row["status"] == "applied" }
          { "schema_version" => 1, "status" => "succeeded", "summary" => "done", "process" => { "exit_code" => 0, "signal" => nil } }
        end
        runner
      end
      controller = Backstage::Application::Controller.new(engine: engine, workflows: service,
        configuration: ControllerTest::FakeConfiguration.new(work), runner_factory: runner_factory, mode: "fake",
        channel_factory: ->(bundle, _phase) { Backstage::Adapters::LocalFiles::AgentRequestChannel.new(File.join(directory, bundle.fetch("id")), workflow_service: service, store: engine.store) })
      result = controller.process(work_item_id: work.fetch("id"))
      assert_equal "done", result.fetch("state")
      assert_equal %w[start investigate report finish], service.history(work.fetch("id")).map { |row| row.fetch("transition") }
    end
  end
end

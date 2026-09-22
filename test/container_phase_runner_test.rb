# frozen_string_literal: true

require_relative "test_helper"

class ContainerPhaseRunnerTest < Minitest::Test
  class FakeRuntime
    attr_reader :calls

    def initialize(outcomes)
      @outcomes = outcomes
      @calls = []
    end

    # The runner reads sentinels out of the capture stream's close summary now, so this double
    # hands its fixture output to the writer and returns an *empty* `logs`. If the runner were
    # still re-parsing the log string, every one of these tests would fail.
    #
    # It rescues CaptureError the way a real runtime's drain does: a refused chunk stops the run,
    # it does not escape as an exception from the runtime.
    def run(bundle:, secrets:, cancellation:, capture: nil)
      @calls << { bundle: bundle, secrets: secrets, cancellation: cancellation }
      outcome = @outcomes.shift
      logs = outcome.fetch("logs", "")
      if bundle.dig("execution", "command", 1) == "finalize" && outcome["status"] == "succeeded"
        source = bundle.dig("execution", "mounts", 0, "source")
        File.binwrite(File.join(source, "change.patch"), "")
        event = JSON.parse(logs)
        event["branch"] = bundle.dig("repository", "branch")
        logs = JSON.generate(event) << "\n"
      end
      begin
        capture&.write(logs)
      rescue Backstage::CaptureError
        nil
      end
      begin
        capture&.close(reason: outcome.fetch("status") == "cancelled" ? "cancelled" : "close")
      rescue Backstage::CaptureError
        nil
      end
      outcome.merge("logs" => capture ? "" : logs)
    end
  end

  # Refuses the chunks of one named step, so a single stream ends `failed` while everything around
  # it stayed clean — the shape a real partial audit failure has.
  class RefusingSink < Backstage::Application::CaptureSink::Null
    def initialize(step:)
      @step = step
    end

    def open(stream_id:, step:, kind:, phase: nil, opened_at: nil, resume: false)
      step == @step ? Refusing.new(stream_id) : super
    end

    class Refusing < Backstage::Application::CaptureSink::Null::Stream
      def commit(chunk)
        raise Backstage::CaptureError.new("sink refused chunk", stream_id: stream_id,
                                          offset: chunk.fetch("start_offset"))
      end
    end
  end

  def runtime_outcome(logs)
    { "schema_version" => 1, "status" => "succeeded", "summary" => "ok", "process" => { "exit_code" => 0, "signal" => nil }, "cancellation" => { "requested" => false, "timed_out" => false }, "logs" => logs }
  end

  def test_implementation_materializes_context_read_only_and_finalizes_draft
    in_tmpdir do |directory|
      pi_logs = File.read(File.expand_path("fixtures/pi_success.jsonl", __dir__))
      runtime = FakeRuntime.new([
        runtime_outcome(%({"type":"repository_prepared","resolved_revision":"abc","branch":"backstage/work"}\n)),
        runtime_outcome(%({"type":"context_materialized","kind":"repo","name":"td","origin":"https://github.com/example/tracker.git","resolved_revision":"def","mount":"refs/td","read_only":true}\n)),
        runtime_outcome(pi_logs),
        runtime_outcome(%({"type":"repository_materialized","branch":"backstage/work","base_revision":"abc","patch_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","patch_size":0}\n)),
        runtime_outcome(%({"type":"repository_published","url":"https://github.com/example/widgets/pull/1","number":1,"repository":"example/widgets","branch":"backstage/work","base":"main","draft":true,"reconciled":false}\n))
      ])
      broker = example_broker( { "GITHUB_TOKEN" => "github-secret", "OPENROUTER_API_KEY" => "model-secret" })
      config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
      work = { "id" => "work", "title" => "Work", "description" => "Do it", "source" => "td", "source_ref" => "td-work", "target" => "widgets", "source_instance" => "widgets-example", "source_identity" => "/projects/widgets" }
      bundle = config.compile(work_item: work)
      bundle["repository"]["branch"] = "backstage/work"

      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      authority = Backstage::RepositoryAuthority.new(designated_repository: "example/widgets", default_branch: "main")
      review_change = Backstage::GitHubReviewChange.new(store: store, authority: authority)
      harness = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
      outcome = Backstage::ContainerPhaseRunner.new(phase: "implementation", runtime: runtime, harness: harness, credential_broker: broker, workspace_root: directory, review_change: review_change, work_item_id: "work").run(bundle: bundle)

      assert_equal "https://github.com/example/widgets/pull/1", outcome.dig("review_change", "url")
      assert_equal true, outcome.dig("materialized_context_grants", 0, "read_only")
      assert_equal ["backstage-container-repository", "context", "0"], runtime.calls[1][:bundle].dig("execution", "command")
      context_mount = runtime.calls[2][:bundle].dig("execution", "mounts").find { |mount| mount["target"] == "/workspace/refs/td" }
      assert_equal true, context_mount["read_only"]
      assert_equal ["GH_TOKEN", "OPENROUTER_API_KEY"], runtime.calls.map { |call| call[:secrets].keys }.flatten.uniq.sort
      refute_includes JSON.generate(runtime.calls.map { |call| call[:bundle] }), "github-secret"
      refute_includes JSON.generate(runtime.calls.map { |call| call[:bundle] }), "model-secret"
      finalize = runtime.calls.find { |call| call[:bundle].dig("execution", "command", 1) == "finalize" }
      publish = runtime.calls.find { |call| call[:bundle].dig("execution", "command", 1) == "publish" }
      assert_empty finalize[:secrets]
      assert_equal ["backstage-container-repository", "publish"], publish[:bundle].dig("execution", "command")
      refute_equal finalize[:bundle].dig("execution", "mounts", 0, "source"), publish[:bundle].dig("execution", "mounts", 0, "source")
      assert_equal %w[change.json change.patch], Dir.children(publish[:bundle].dig("execution", "mounts", 0, "source")).sort
      assert_equal ["GH_TOKEN"], runtime.calls.last[:secrets].keys
      assert_equal "succeeded", store.list("external_actions").first["status"]
    end
  end

  def test_cancelled_repository_context_and_finalize_phases_remain_cancelled
    %i[prepare context finalize publish].each do |cancelled_phase|
      in_tmpdir do |directory|
        runtime = FakeRuntime.new(outcomes_for_cancellation(cancelled_phase))
        broker = example_broker( { "GITHUB_TOKEN" => "github-secret", "OPENROUTER_API_KEY" => "model-secret" })
        config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
        engine = build_engine(directory)
        work = submit_work(engine, key: "cancel-#{cancelled_phase}", title: "Cancel", source: "td", source_ref: "td-cancel", **config.binding_for("widgets"))
        bundle = config.compile(work_item: work)
        authority = Backstage::RepositoryAuthority.new(designated_repository: bundle.dig("repository", "designated_repository"), default_branch: bundle.dig("repository", "default_branch"))
        review_change = Backstage::GitHubReviewChange.new(store: engine.store, authority: authority)
        harness = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
        runner = Backstage::ContainerPhaseRunner.new(phase: "implementation", runtime: runtime, harness: harness, credential_broker: broker, workspace_root: File.join(directory, "workspaces"), review_change: review_change, work_item_id: work["id"])

        result = engine.execute(dispatch(engine, work), runner: runner, bundle: bundle)

        assert_equal "cancelled", result.dig("outcome", "status"), cancelled_phase
        assert_equal "cancelled", engine.show_work(work["id"]).dig("runs", 0, "status"), cancelled_phase
        assert_equal "running", engine.show_work(work["id"])["state"], cancelled_phase
      end
    end
  end

  def test_sentinels_reach_the_runner_only_through_the_capture_summary
    in_tmpdir do |directory|
      runtime = FakeRuntime.new(succeeding_outcomes)
      runner, bundle = build_runner(directory, runtime)
      capture = Backstage::Application::CaptureDefaults.null(run: "run-sentinel", phase: "implementation")

      outcome = runner.run(bundle: bundle, capture: capture)

      assert_equal "succeeded", outcome.fetch("status")
      assert_equal "", outcome.fetch("logs", ""),
                   "the runner never needs the log string it used to re-parse"
      assert_equal "abc", outcome.dig("change_artifact", "base_revision")
      assert_equal "td", outcome.dig("materialized_context_grants", 0, "name")
      assert_equal "https://github.com/example/widgets/pull/1", outcome.dig("review_change", "url")
      assert_equal "complete", outcome.dig("capture", "status")
      assert_equal %w[prepare context harness finalize publish],
                   outcome.dig("capture", "streams").map { |stream| stream.fetch("step") }
    end
  end

  def test_publication_is_refused_when_a_stream_could_not_be_audited
    in_tmpdir do |directory|
      runtime = FakeRuntime.new(succeeding_outcomes)
      runner, bundle = build_runner(directory, runtime)
      capture = Backstage::Application::RuntimeCapture.new(
        sink: RefusingSink.new(step: "finalize"), clock: Backstage::Adapters::Fake::Clock.new,
        run: { "id" => "run-refused", "phase" => "implementation" }, attempt: 1
      )

      outcome = runner.run(bundle: bundle, capture: capture)

      assert_equal "failed", outcome.fetch("status")
      assert_includes outcome.fetch("summary"), "refusing to publish"
      assert_includes outcome.fetch("summary"), "failed"
      assert_nil outcome["review_change"], "no draft is published on an unaudited run"
      assert_equal "failed", outcome.dig("capture", "status")
      assert_equal 4, runtime.calls.length, "publish never ran"
      assert_equal %w[prepare context pi finalize],
                   runtime.calls.map { |call| call[:bundle].dig("execution", "command").then { |argv| argv.first == "pi" ? "pi" : argv.fetch(1) } }
    end
  end

  # A verdict authorizes the next phase exactly as a finalize does, so it is gated the same way.
  # The review phase used to return before the audited? gate ever ran, so an `approved` was accepted
  # from a run whose harness stream nothing acknowledged — and the evidence the reviewer claims to
  # have read is precisely the part that went missing.
  def test_a_review_verdict_is_refused_when_the_reviewer_stream_could_not_be_audited
    in_tmpdir do |directory|
      runtime = FakeRuntime.new(succeeding_outcomes.take(2) + [runtime_outcome(reviewer_transcript)])
      runner, bundle = build_runner(directory, runtime, review: true)
      capture = Backstage::Application::RuntimeCapture.new(
        sink: RefusingSink.new(step: "harness"), clock: Backstage::Adapters::Fake::Clock.new,
        run: { "id" => "run-review-refused", "phase" => "review" }, attempt: 1
      )

      outcome = runner.run(bundle: bundle, capture: capture)

      assert_equal "failed", outcome.fetch("status")
      assert_includes outcome.fetch("summary"), "refusing to accept the review verdict"
      assert_equal "failed", outcome.dig("capture", "status")
      # The workflow reads a verdict only off a succeeded outcome, so a failed one takes on_failure.
      assert_equal "approved", outcome.dig("review", "verdict"), "what the reviewer said is still recorded"
    end
  end

  # And an audited review still passes through untouched.
  def test_an_audited_review_verdict_is_accepted
    in_tmpdir do |directory|
      runtime = FakeRuntime.new(succeeding_outcomes.take(2) + [runtime_outcome(reviewer_transcript)])
      runner, bundle = build_runner(directory, runtime, review: true)
      capture = Backstage::Application::RuntimeCapture.new(
        sink: Backstage::Application::CaptureSink::Null.new,
        clock: Backstage::Adapters::Fake::Clock.new,
        run: { "id" => "run-review", "phase" => "review" }, attempt: 1
      )

      outcome = runner.run(bundle: bundle, capture: capture)

      assert_equal "succeeded", outcome.fetch("status")
      assert_equal "approved", outcome.dig("review", "verdict")
    end
  end

  # A published pull request is a real external effect, and when it happened is a fact about the
  # worker, not about Backstage's buffer. The sentinel interpreter set no `occurred_at`, so the sink
  # fell back to the chunk's commit time — which put the flush schedule inside the event's canonical
  # fingerprint, so the same announcement split into different chunks was a different fact.
  def test_a_sentinel_is_stamped_when_it_was_announced_not_when_the_chunk_was_flushed
    announced = %({"type":"repository_published","url":"https://github.com/example/widgets/pull/1","number":1,"repository":"example/widgets","branch":"backstage/work","base":"main","draft":true,"reconciled":false,"timestamp":"2026-03-04T05:06:07.000000Z"}\n)
    unstamped = JSON.generate(JSON.parse(announced).reject { |key, _| key == "timestamp" }) + "\n"

    [announced, unstamped].each do |line|
      stamps = [65_536, 8].map { |flush| sentinel_event(line, flush_bytes: flush) }

      assert_equal stamps.fetch(0).fetch("occurred_at"), stamps.fetch(1).fetch("occurred_at"),
                   "the same announcement chunked two ways is one fact, not two"
      # Two separate deployments are what running the same stream id twice costs, so deployment_id
      # is the test's own artefact. `artifact_refs` is the one field still chunk-dependent by
      # design: a semantic event names the chunk artifact its record landed in, which is provenance
      # worth keeping. Everything else — including occurred_at, which decides whether a retry
      # reconciles or is refused — is now a function of the record.
      assert_equal fingerprint(stamps.fetch(0)), fingerprint(stamps.fetch(1))
      refute_equal stamps.fetch(0).fetch("artifact_refs"), stamps.fetch(1).fetch("artifact_refs")
    end
    # And when the worker said so, it is the worker's time that is recorded.
    assert_equal "2026-03-04T05:06:07.000000Z", sentinel_event(announced, flush_bytes: 8).fetch("occurred_at")
    refute_equal "2026-03-04T05:06:07.000000Z", sentinel_event(unstamped, flush_bytes: 8).fetch("occurred_at")
  end

  private

  def fingerprint(event)
    Backstage::Domain::Activity.canonical_fingerprint(
      event.reject { |key, _| %w[deployment_id artifact_refs].include?(key) }
    )
  end

  # Drives one sentinel line through a real durable capture at a given flush size, advancing the
  # clock between writes so a chunk-dependent timestamp would visibly differ, and returns the
  # `artifact.available` the stream committed.
  def sentinel_event(line, flush_bytes:)
    in_tmpdir do |directory|
      clock = Backstage::Adapters::Fake::Clock.new
      run = { "id" => "run-sentinel", "phase" => "implementation" }
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"))
      sink = Backstage::Application::CaptureSink::Durable.new(
        store: store, artifact_store: artifacts, work_item_id: "work", run: run, attempt: 1
      )
      capture = Backstage::Application::RuntimeCapture.new(sink: sink, clock: clock, run: run, attempt: 1,
                                                           flush_bytes: flush_bytes)
      writer = capture.open(step: "publish",
                            interpreter: Backstage::Application::Runners::SentinelInterpreter.new)
      line.b.each_char.each_slice(7) do |slice|
        clock.advance(0.05)
        writer.write(slice.join)
      end
      writer.close
      store.read_activity(filters: { type: "artifact.available" }, limit: 10)
           .fetch("events").find { |event| event.dig("data", "sentinel") == "repository_published" }
    end
  end

  def reviewer_transcript
    verdict = JSON.generate("verdict" => "approved", "summary" => "the change is clean")
    [
      JSON.generate("type" => "message_end",
                    "message" => { "role" => "assistant", "stopReason" => "stop",
                                   "content" => [{ "type" => "text", "text" => verdict }],
                                   "usage" => {} }),
      JSON.generate("type" => "agent_settled")
    ].join("\n") + "\n"
  end

  def build_runner(directory, runtime, review: false)
    broker = example_broker( { "GITHUB_TOKEN" => "github-secret", "OPENROUTER_API_KEY" => "model-secret" })
    config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
    work = { "id" => "work", "title" => "Work", "description" => "Do it", "source" => "td", "source_ref" => "td-work",
             "target" => "widgets", "source_instance" => "widgets-example", "source_identity" => "/projects/widgets" }
    bundle = config.compile(work_item: work)
    bundle["repository"]["branch"] = "backstage/work"
    store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
    authority = Backstage::RepositoryAuthority.new(designated_repository: "example/widgets", default_branch: "main")
    review_change = Backstage::GitHubReviewChange.new(store: store, authority: authority)
    harness = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
    runner = Backstage::ContainerPhaseRunner.new(phase: review ? "review" : "implementation",
                                                 runtime: runtime, harness: harness, review: review,
                                                 credential_broker: broker, workspace_root: directory,
                                                 review_change: review_change, work_item_id: "work")
    [runner, bundle]
  end

  def succeeding_outcomes
    [
      runtime_outcome(%({"type":"repository_prepared","resolved_revision":"abc","branch":"backstage/work"}\n)),
      runtime_outcome(%({"type":"context_materialized","kind":"repo","name":"td","origin":"https://github.com/example/tracker.git","resolved_revision":"def","mount":"refs/td","read_only":true}\n)),
      runtime_outcome(File.read(File.expand_path("fixtures/pi_success.jsonl", __dir__))),
      runtime_outcome(%({"type":"repository_materialized","branch":"backstage/work","base_revision":"abc","patch_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","patch_size":0}\n)),
      runtime_outcome(%({"type":"repository_published","url":"https://github.com/example/widgets/pull/1","number":1,"repository":"example/widgets","branch":"backstage/work","base":"main","draft":true,"reconciled":false}\n))
    ]
  end

  def outcomes_for_cancellation(phase)
    cancelled = { "schema_version" => 1, "status" => "cancelled", "summary" => "container cancelled", "process" => { "exit_code" => 143, "signal" => 15 }, "cancellation" => { "requested" => true, "timed_out" => false }, "logs" => "" }
    prepared = runtime_outcome(%({"type":"repository_prepared","resolved_revision":"abc","branch":"backstage/work"}\n))
    context = runtime_outcome(%({"type":"context_materialized","kind":"repo","name":"td","origin":"https://github.com/example/tracker.git","resolved_revision":"def","mount":"refs/td","read_only":true}\n))
    pi = runtime_outcome(File.read(File.expand_path("fixtures/pi_success.jsonl", __dir__)))
    finalized = runtime_outcome(%({"type":"repository_materialized","branch":"backstage/#{phase}","base_revision":"abc","patch_sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","patch_size":0}\n))
    case phase
    when :prepare then [cancelled]
    when :context then [prepared, cancelled]
    when :finalize then [prepared, context, pi, cancelled]
    when :publish then [prepared, context, pi, finalized, cancelled]
    end
  end
end

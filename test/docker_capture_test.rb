# frozen_string_literal: true

require_relative "test_helper"

# The Docker runtime's output path, proven against a real child process without needing Docker.
#
# The stub is a Ruby script standing in for the `docker` binary: it receives the same argv, writes
# to the same pipe, and answers `stop`/`kill` the way the real one does. That keeps these tests
# about the parts that were actually wrong before — an unbounded String, an unbounded queue, and a
# teardown that joined the reader before draining it — rather than about Docker.
class DockerCaptureTest < Minitest::Test
  RuntimeCapture = Backstage::Application::RuntimeCapture
  CaptureSink = Backstage::Application::CaptureSink

  # A sink that refuses a chunk. The failure a runtime has to survive is not "the disk is full" in
  # the abstract; it is "the audit trail refused this chunk", and what must follow is a stopped
  # container and a failed outcome, never a success with a gap in it.
  class FailingSink < CaptureSink::Null
    def initialize(after: 0)
      @after = after
      @commits = 0
    end

    def open(stream_id:, step:, kind:, phase: nil, opened_at: nil, resume: false)
      Stream.new(stream_id, self)
    end

    def failing?
      @commits += 1
      @commits > @after
    end

    class Stream < CaptureSink::Null::Stream
      def initialize(stream_id, owner)
        super(stream_id)
        @owner = owner
      end

      def commit(chunk)
        if @owner.failing?
          raise Backstage::CaptureError.new("sink refused chunk", stream_id: stream_id,
                                            offset: chunk.fetch("start_offset"))
        end

        super
      end
    end
  end

  def bundle(command = %w[noop])
    JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__))).tap do |value|
      value["execution"]["command"] = command
      value["execution"]["credential_refs"] = []
    end
  end

  # A stub that behaves like `docker` for the three verbs this runtime uses.
  def stub_docker(directory, body)
    path = File.join(directory, "docker-stub")
    File.write(path, <<~SCRIPT)
      #!#{RbConfig.ruby}
      exit 0 unless ARGV.first == "run"
      STDIN.read
      STDOUT.binmode
      STDOUT.sync = true
      #{body}
    SCRIPT
    FileUtils.chmod(0o755, path)
    path
  end

  def null_capture(secrets: [], **options)
    RuntimeCapture.new(sink: options.delete(:sink) || CaptureSink::Null.new,
                       clock: Backstage::Adapters::Fake::Clock.new, run: "run-docker", attempt: 1,
                       secret_guard: Backstage::SecretGuard.new(secret_values: secrets), **options)
  end

  # A capture that actually writes chunk files, so a test can read back what reached the disk.
  def durable_capture(directory, secrets: [], **options)
    guard = Backstage::SecretGuard.new(secret_values: secrets)
    run = { "id" => "run-docker", "phase" => "implementation" }
    store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
    artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
    sink = CaptureSink::Durable.new(store: store, artifact_store: artifacts, work_item_id: "work-1",
                                    run: run, attempt: 1)
    RuntimeCapture.new(sink: sink, clock: Backstage::Adapters::Fake::Clock.new, run: run, attempt: 1,
                       secret_guard: guard, **options)
  end

  def elapsed
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    value = yield
    [value, Process.clock_gettime(Process::CLOCK_MONOTONIC) - started]
  end

  def chunk_files(directory)
    Dir.glob(File.join(directory, "artifacts", "**", "*.log")).sort_by { |path| File.basename(path).to_i }
  end

  def test_output_larger_than_the_tail_keeps_the_end_and_says_it_was_truncated
    in_tmpdir do |directory|
      docker = stub_docker(directory, <<~BODY)
        3000.times { |index| STDOUT.print("line \#{index} " + ("p" * 60) + "\\n") }
        STDOUT.print("final line\\n")
      BODY
      capture = null_capture
      writer = capture.open(step: "harness")
      outcome = Backstage::DockerRuntime.new(docker: docker).run(bundle: bundle, capture: writer)

      assert_equal "succeeded", outcome.fetch("status")
      assert_operator outcome.fetch("logs").bytesize, :<=, 64 * 1024
      assert_equal true, outcome.fetch("logs_truncated")
      assert_equal 64 * 1024, outcome.fetch("log_tail_bytes")
      assert_includes outcome.fetch("logs"), "final line", "a tail keeps the end, which is the part worth reading"
      refute_includes outcome.fetch("logs"), "line 0 ", "and drops the beginning rather than growing"
      # Nothing was lost, it moved: the stream captured every byte the tail could not hold.
      assert_equal "complete", outcome.dig("capture", "status")
      assert_operator outcome.dig("capture", "bytes"), :>, 64 * 1024
      assert_equal 3001, outcome.dig("capture", "streams", 0, "records")
    end
  end

  def test_a_secret_split_across_pipe_reads_reaches_no_chunk_file_and_no_tail
    in_tmpdir do |directory|
      secret = "runtime-secret-value-0123456789"
      docker = stub_docker(directory, %(STDOUT.print("before #{secret} after\\n")))
      capture = durable_capture(directory, secrets: [secret])
      writer = capture.open(step: "harness")
      # Eight bytes per read guarantees the secret straddles several read boundaries.
      outcome = Backstage::DockerRuntime.new(
        docker: docker, secret_guard: Backstage::SecretGuard.new(secret_values: [secret]),
        read_bytes: 8, queue_limit: 4
      ).run(bundle: bundle, capture: writer)

      assert_equal "succeeded", outcome.fetch("status")
      refute_includes outcome.fetch("logs"), secret
      assert_includes outcome.fetch("logs"), "before"
      refute_empty chunk_files(directory)
      chunk_files(directory).each { |path| refute_includes File.binread(path), secret, path }
      assert_includes chunk_files(directory).map { |path| File.binread(path) }.join, "after"
    end
  end

  def test_a_multibyte_character_split_across_pipe_reads_is_captured_intact
    in_tmpdir do |directory|
      docker = stub_docker(directory, %(STDOUT.print("réussi ✅ done\\n")))
      capture = durable_capture(directory)
      writer = capture.open(step: "harness")
      Backstage::DockerRuntime.new(docker: docker, read_bytes: 3, queue_limit: 2)
                              .run(bundle: bundle, capture: writer)

      captured = chunk_files(directory).map { |path| File.binread(path) }.join
      assert_equal "réussi ✅ done\n", captured.force_encoding(Encoding::UTF_8)
    end
  end

  def test_a_refused_chunk_stops_the_container_and_fails_the_outcome
    in_tmpdir do |directory|
      docker = stub_docker(directory, <<~BODY)
        200.times { |index| STDOUT.print("line \#{index}\\n") }
        sleep 30
      BODY
      # A small flush size so a chunk is committed — and refused — while the container is still
      # running, which is the only moment stopping it is worth anything.
      capture = null_capture(sink: FailingSink.new, flush_bytes: 128)
      writer = capture.open(step: "harness")
      outcome, took = elapsed do
        Backstage::DockerRuntime.new(docker: docker, read_bytes: 64, queue_limit: 2, grace_seconds: 1)
                                .run(bundle: bundle, capture: writer)
      end

      assert_equal "failed", outcome.fetch("status")
      assert_equal "failed", outcome.dig("capture", "status")
      assert_includes outcome.fetch("summary"), "could not be captured"
      refute_nil outcome.dig("capture", "error")
      assert_equal true, writer.failed?
      assert_operator took, :<, 25, "a refused chunk stops the container instead of waiting it out"
    end
  end

  def test_teardown_does_not_deadlock_when_the_child_outproduces_the_drain
    in_tmpdir do |directory|
      # The child writes far more than the bounded queue can hold and never stops, so it is blocked
      # on a full pipe throughout teardown. Joining the reader before draining — what this runtime
      # used to do — hangs here forever.
      docker = stub_docker(directory, %(loop { STDOUT.print("x" * 4096) }))
      capture = null_capture(max_run_bytes: 512 * 1024)
      writer = capture.open(step: "harness")
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      cancelled = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) - started > 0.2 }
      outcome, took = elapsed do
        Backstage::DockerRuntime.new(docker: docker, read_bytes: 4096, queue_limit: 2, grace_seconds: 1)
                                .run(bundle: bundle, cancellation: cancelled, capture: writer)
      end

      assert_equal "cancelled", outcome.fetch("status")
      assert_equal true, outcome.dig("cancellation", "requested")
      assert_operator took, :<, 30, "teardown drains while it waits rather than deadlocking on a bounded queue"
      assert_equal "cancelled", capture.summaries.fetch(0).fetch("reason")
      # A byte limit stops persistence; it does not stop the stream from ending honestly.
      assert_includes %w[complete truncated], outcome.dig("capture", "status")
      assert_operator outcome.fetch("logs").bytesize, :<=, 64 * 1024
      # All three places bytes live in flight are still bounded after megabytes went past, which is
      # the whole point of the bounded queue and of draining on the controller thread.
      assert_operator writer.buffered_bytes, :<=, 64 * 1024
      assert_operator writer.pending_bytes, :<=, 64 * 1024
      assert_operator writer.retained_bytes, :<=, 64 * 1024 + 64 * 1024
      assert_operator writer.retained_frames, :<=, 1024
    end
  end

  # The failure that used to be invisible. A container whose whole output fits under one flush
  # commits nothing until it closes, so the close *is* the only commit — and closing was rescued to
  # nil, leaving a run reported "succeeded" whose capture block quietly said "failed" with no error
  # on it. Design 5.2 says the runtime returns failed here, and now it does.
  def test_a_capture_that_only_fails_on_the_final_flush_fails_the_run
    in_tmpdir do |directory|
      docker = stub_docker(directory, %(STDOUT.print("short output line\\n")))
      capture = null_capture(sink: FailingSink.new)
      writer = capture.open(step: "harness")
      outcome = Backstage::DockerRuntime.new(docker: docker, grace_seconds: 1).run(bundle: bundle, capture: writer)

      assert_equal "failed", outcome.fetch("status")
      assert_equal "failed", outcome.dig("capture", "status")
      assert_includes outcome.fetch("summary"), "could not be captured"
      assert_includes outcome.dig("capture", "error"), "sink refused chunk"
      assert_equal true, writer.failed?
    end
  end

  # The tail is bounded by `log_tail_bytes`, and it has to still be bounded after scrubbing: U+FFFD
  # is three bytes, so trimming binary first and replacing invalid bytes afterwards handed back a
  # `logs` field larger than the number `log_tail_bytes` was stating.
  def test_the_log_tail_obeys_its_stated_bound_after_invalid_bytes_are_scrubbed
    in_tmpdir do |directory|
      docker = stub_docker(directory, %(STDOUT.write("\\xFF" * 8192); STDOUT.print("\\n")))
      capture = null_capture
      writer = capture.open(step: "harness")
      outcome = Backstage::DockerRuntime.new(docker: docker, tail_bytes: 4096, grace_seconds: 1)
                                        .run(bundle: bundle, capture: writer)

      assert_operator outcome.fetch("logs").bytesize, :<=, 4096
      assert_equal 4096, outcome.fetch("log_tail_bytes")
      assert_equal true, outcome.fetch("logs_truncated")
      assert_predicate outcome.fetch("logs"), :valid_encoding?
    end
  end

  # And a tail that fits says so with a boolean, not with a nil that happens to be falsy.
  def test_a_log_tail_that_fits_reports_that_nothing_was_dropped
    in_tmpdir do |directory|
      docker = stub_docker(directory, %(STDOUT.print("short output line\\n")))
      capture = null_capture
      writer = capture.open(step: "harness")
      outcome = Backstage::DockerRuntime.new(docker: docker, grace_seconds: 1).run(bundle: bundle, capture: writer)

      assert_equal "short output line\n", outcome.fetch("logs")
      assert_equal false, outcome.fetch("logs_truncated")
    end
  end

  def test_a_run_cancelled_before_launch_still_reports_its_capture
    in_tmpdir do |directory|
      docker = stub_docker(directory, %(STDOUT.print("never\\n")))
      capture = null_capture
      writer = capture.open(step: "harness")
      outcome = Backstage::DockerRuntime.new(docker: docker)
                                        .run(bundle: bundle, cancellation: -> { true }, capture: writer)

      assert_equal "cancelled", outcome.fetch("status")
      assert_equal "complete", outcome.dig("capture", "status")
      assert_equal 0, outcome.dig("capture", "bytes")
      assert_equal "cancelled", capture.summaries.fetch(0).fetch("reason")
    end
  end
end

# frozen_string_literal: true

require "json"
require "open3"
require "securerandom"
require "tempfile"
require "timeout"

module Backstage::Adapters::Docker
  # Runs one container and hands its output to a capture stream.
  #
  # The output path is the part worth reading carefully. It used to be `output.each_line` on a
  # reader thread, appending every redacted line to an unbounded String and yielding each one to
  # the controller. Three things were wrong with that: the String grew without limit, "the bytes
  # were produced" and "the bytes were audited" were the same claim, and an unbounded `Queue`
  # meant a chatty container could outrun its consumer with no backpressure at all.
  #
  # Now the reader thread does one thing — `readpartial` into a bounded `SizedQueue`, which is real
  # backpressure onto the pipe — and everything else happens on the controller thread inside
  # `drain`: redaction, framing, interpretation, durable chunks and their commits, all through the
  # capture writer. No thread is started for capture, so the guarantee that callbacks run on the
  # controller thread still holds.
  #
  # Teardown is `drain.call until reader.join(0.01)`, not `reader.value` — with a bounded queue a
  # child that outproduces the drain blocks on a full pipe, and joining the reader before draining
  # is a deadlock, not a slow test.
  class Runtime
    ContractError = Backstage::ContractError
    ContractValidator = Backstage::Contracts::Validator
    Outcome = Backstage::Domain::Outcome
    Records = Backstage::Domain::Records
    SecretGuard = Backstage::Support::SecretGuard
    DEFAULT_GRACE_SECONDS = 3
    HEARTBEAT_SECONDS = 1
    # One pipe read. Matches the capture's default flush size so an ordinary read is one chunk.
    READ_BYTES = 64 * 1024
    # Bounded queue depth. Beyond this the reader blocks, the pipe fills, and the container is
    # slowed down — which is the correct answer to a worker producing faster than we can audit.
    QUEUE_LIMIT = 64
    # How much output the outcome carries for diagnostics. It is a tail, not a transcript: the
    # complete stream is in the capture artifacts, and `logs_truncated` says when this is partial.
    TAIL_BYTES = 64 * 1024
    EOF = :eof

    def initialize(docker: "docker", secret_guard: SecretGuard.new, clock: Process, poll_interval: 0.05,
                   validator: ContractValidator.new, read_bytes: READ_BYTES, queue_limit: QUEUE_LIMIT,
                   tail_bytes: TAIL_BYTES, grace_seconds: DEFAULT_GRACE_SECONDS)
      @docker = docker
      @secret_guard = secret_guard
      @clock = clock
      @poll_interval = poll_interval
      @validator = validator
      @read_bytes = Integer(read_bytes)
      @queue_limit = Integer(queue_limit)
      @tail_bytes = Integer(tail_bytes)
      # How long a container gets to leave on its own before it is escalated, and how long `docker
      # stop` is told to wait. A keyword like every other bound here, so a test proving the teardown
      # path does not have to spend the production grace period doing it.
      @grace_seconds = Integer(grace_seconds)
    end

    def runtime_identity_before_launch? = true

    # `capture` is a `Ports::RuntimeCapture::StreamWriter` — one already-opened stream, not the
    # capture component. A runtime owns one process's bytes and knows nothing about which streams a
    # phase has or how they should be interpreted; whoever opened the stream chose the interpreter
    # and closes it. This runtime closes it too, with the reason it actually ended for, and `close`
    # is idempotent so the opener still gets the same summary back.
    def run(bundle:, secrets: {}, cancellation: nil, capture: nil)
      @validator.validate!("job-bundle-v1.json", bundle)
      validate_secrets!(bundle, secrets)
      return cancelled_before_launch(capture) if cancellation&.call

      container_name = "backstage-#{bundle.fetch("id").gsub(/[^a-zA-Z0-9_.-]/, "-")}-#{SecureRandom.hex(3)}"
      timeout_seconds = bundle.dig("execution", "timeout_seconds")
      started = monotonic
      started_at = Records.timestamp
      tail = Tail.new(limit: @tail_bytes, redactor: @secret_guard.redactor)
      process = nil
      cancellation_requested = false
      timed_out = false
      capture_error = nil

      with_bundle_file(bundle) do |bundle_path|
        argv = docker_argv(bundle, bundle_path, container_name, secrets.keys)
        child_env = secrets.transform_keys(&:to_s)
        # Announce the runtime identity before the container can produce anything, so an interrupted
        # run leaves recovery something concrete to ask about.
        yield({ "type" => "runtime_started", "container_name" => container_name, "at" => started_at }) if block_given?
        Open3.popen2e(child_env, *argv, pgroup: true) do |stdin, output, wait|
          stdin.write(bundle.dig("harness", "prompt").to_s)
          stdin.close
          process = wait
          output.binmode
          pending = SizedQueue.new(@queue_limit)
          reader = Thread.new do
            begin
              loop { pending << output.readpartial(@read_bytes) }
            rescue EOFError, IOError
              nil
            ensure
              pending << EOF
            end
          end

          # Everything that turns bytes into audited state happens here, on the controller thread.
          drain = lambda do
            bytes = 0
            loop do
              item = begin
                pending.pop(true)
              rescue ThreadError
                break
              end
              break if item == EOF

              tail.push(item)
              next if capture_error

              begin
                capture&.write(item)
                bytes += item.bytesize
              rescue Backstage::CaptureError => error
                # The audit trail failed, so nothing further may be authorized on this run. Stop the
                # container through its own runtime rather than letting it keep working unobserved,
                # and never fall back to an in-memory success trail.
                capture_error = error
                stop_container(container_name)
                break
              end
            end
            capture&.tick unless capture_error
            if bytes.positive? && block_given?
              yield({ "type" => "runtime_progress", "stream" => "combined", "bytes" => bytes,
                      "stream_id" => capture&.id, "at" => Records.timestamp }.compact)
            end
            bytes
          end

          next_heartbeat = monotonic
          begin
            until wait.join(@poll_interval)
              drain.call
              break if capture_error

              if monotonic >= next_heartbeat
                yield({ "type" => "runtime_heartbeat", "container_name" => container_name, "at" => Records.timestamp }) if block_given?
                next_heartbeat = monotonic + HEARTBEAT_SECONDS
              end
              if cancellation&.call
                cancellation_requested = true
                stop_container(container_name)
                break
              end
              if timeout_seconds && monotonic - started >= timeout_seconds
                timed_out = true
                stop_container(container_name)
                break
              end
            end
            # Drain while waiting for the reader, never the other way round: the queue is bounded,
            # so a child that outproduces the drain is blocked on a full pipe until we take from it.
            escalate_at = monotonic + @grace_seconds
            until reader.join(0.01)
              drain.call
              next if monotonic < escalate_at

              force_exit(wait, container_name)
              escalate_at = monotonic + @grace_seconds
            end
            drain.call
            ensure_process_exit(wait, container_name)
          ensure
            # Callback failures must not strand an unattended worker or a reader thread. Discarding
            # rather than draining here is deliberate: we are unwinding, and a blocked reader that
            # can never finish is worse than the bytes we give up on.
            stop_container(container_name) if wait.alive?
            ensure_process_exit(wait, container_name)
            discard(pending) until reader.join(0.01)
          end
        end
      end

      status = process.value
      reached = if capture_error
                  "failed"
                elsif cancellation_requested
                  "cancelled"
                elsif timed_out
                  "timed_out"
                elsif status.success?
                  "succeeded"
                else
                  "failed"
                end
      # The final flush is a commit like any other, and it can be the first one to fail — a
      # container whose whole output fits under one flush has no other. Closing before the status is
      # decided is what lets that failure reach the outcome instead of leaving a `succeeded` run
      # whose capture block quietly says `failed` with no error on it.
      summary, capture_error = close_capture(capture, reached, capture_error)
      normalized_status = capture_error ? "failed" : reached
      outcome = {
        "schema_version" => 2,
        "status" => normalized_status,
        "summary" => capture_error ? "container output could not be captured: #{capture_error.message}" : "container #{normalized_status}",
        "process" => { "exit_code" => status.exitstatus, "signal" => status.termsig },
        "cancellation" => { "requested" => cancellation_requested, "timed_out" => timed_out },
        "logs" => tail.text,
        "logs_truncated" => tail.truncated?,
        "log_tail_bytes" => @tail_bytes,
        "capture" => capture_block(capture, summary, capture_error),
        "container_name" => container_name,
        "started_at" => started_at,
        "finished_at" => Records.timestamp
      }.compact
      Outcome.validate!(outcome)
    end

    private

    def cancelled_before_launch(capture)
      summary, capture_error = close_capture(capture, "cancelled", nil)
      Outcome.validate!({
        "schema_version" => 2, "status" => capture_error ? "failed" : "cancelled",
        "summary" => capture_error ? "container output could not be captured: #{capture_error.message}" : "cancelled before container launch",
        "process" => { "exit_code" => nil, "signal" => nil }, "logs" => "", "logs_truncated" => false,
        "cancellation" => { "requested" => true, "timed_out" => false },
        "capture" => capture_block(capture, summary, capture_error)
      }.compact)
    end

    # A stream is closed by the runtime that produced it, with the reason it actually ended for, so
    # the checkpoint row says `cancelled` rather than the generic `close`. `close` is idempotent, so
    # the opener calling it again gets this same summary.
    #
    # Returns `[summary, capture_error]`. A close that fails is a capture failure like any other and
    # is returned rather than swallowed: it is frequently the *only* failure, because a run short
    # enough to fit in one flush commits nothing until it closes.
    def close_capture(capture, status, capture_error)
      return [nil, capture_error] unless capture

      reason = if capture_error then "failed"
               elsif %w[cancelled timed_out].include?(status) then status
               else "close"
               end
      [capture.close(reason: reason), capture_error]
    rescue Backstage::CaptureError => error
      [nil, capture_error || error]
    end

    def capture_block(capture, summary, capture_error)
      return nil unless capture

      status = "failed" if capture_error || summary.nil?
      block = Outcome.capture_summary(summary ? [summary] : [], status: status)
      block["stream_id"] = capture.id
      block["last_offset"] = summary && summary["last_offset"]
      block["error"] = capture_error.message if capture_error
      block.compact
    end

    def discard(pending)
      loop { pending.pop(true) }
    rescue ThreadError
      nil
    end

    # A bounded, redacted view of what the container printed. The redactor is streaming, so a
    # secret split across two `readpartial` results is withheld rather than half-published, and the
    # tail is trimmed on every push so it never holds more than its limit.
    class Tail
      def initialize(limit:, redactor:)
        @limit = Integer(limit)
        @redactor = redactor
        @buffer = +"".b
        @seen = 0
        @trimmed = false
      end

      def push(bytes)
        @text = nil
        safe = @redactor.push(bytes)
        return if safe.empty?

        @seen += safe.bytesize
        @buffer << safe
        @buffer = @buffer.byteslice(@buffer.bytesize - @limit, @limit) if @buffer.bytesize > @limit
      end

      # True when anything was dropped, whether by the rolling trim or by the one after scrubbing.
      # Reading `text` first is deliberate: the second cause is only known once it has been built.
      def truncated?
        text
        @seen > @limit || @trimmed
      end

      # Flushing the redactor is what releases the bytes it was withholding against a secret
      # straddling a read boundary, so it happens once, here, when there are no more bytes coming.
      #
      # The limit is applied *after* scrubbing, not before. Trimming binary and then replacing each
      # invalid byte with a three-byte U+FFFD grew the result past the bound it had just been cut to
      # — up to three times it, for a container printing binary — and `log_tail_bytes` in the
      # outcome would have been stating a number the field did not obey.
      def text
        @text ||= begin
          remainder = @redactor.finish
          push(remainder) unless remainder.empty?
          scrubbed = @buffer.dup.force_encoding(Encoding::UTF_8).scrub("\u{FFFD}")
          scrubbed = trim(scrubbed) if scrubbed.bytesize > @limit
          scrubbed
        end
      end

      private

      # Cuts to the last `@limit` bytes without leaving a half character at the front.
      def trim(text)
        @trimmed = true
        text.byteslice(text.bytesize - @limit, @limit).scrub("").force_encoding(Encoding::UTF_8)
      end
    end

    def validate_secrets!(bundle, secrets)
      refs = Array(bundle.dig("execution", "credential_refs"))
      unknown = secrets.keys.map(&:to_s) - refs
      missing = refs - secrets.keys.map(&:to_s)
      raise ContractError, "unrequested credentials: #{unknown.join(", ")}" unless unknown.empty?
      raise ContractError, "missing credentials: #{missing.join(", ")}" unless missing.empty?
      raise ContractError, "empty credential value" if secrets.values.any? { |value| value.to_s.empty? }
    end

    def with_bundle_file(bundle)
      Tempfile.create(["backstage-bundle", ".json"]) do |file|
        file.chmod(0o600)
        file.write(JSON.generate(bundle))
        file.flush
        yield file.path
      end
    end

    def docker_argv(bundle, bundle_path, name, secret_names)
      execution = bundle.fetch("execution")
      argv = [@docker, "run", "--rm", "--interactive", "--name", name, "--label", "backstage.job=#{bundle.fetch("id")}"]
      argv.concat(["--mount", "type=bind,source=#{bundle_path},target=/backstage/job-bundle.json,readonly"])
      argv.concat(["--env", "BACKSTAGE_JOB_BUNDLE=/backstage/job-bundle.json"])
      Array(execution["mounts"]).each do |mount|
        source = File.expand_path(mount.fetch("source"))
        spec = "type=bind,source=#{source},target=#{mount.fetch("target")}"
        spec += ",readonly" if mount.fetch("read_only", true)
        argv.concat(["--mount", spec])
      end
      secret_names.each { |name_key| argv.concat(["--env", name_key.to_s]) }
      argv << execution.fetch("image")
      argv.concat(execution.fetch("command"))
      argv
    end

    def stop_container(name)
      system(@docker, "stop", "--time", @grace_seconds.to_s, name, out: File::NULL, err: File::NULL)
    end

    # Escalation that never blocks, for use inside a loop that still has to keep draining.
    def force_exit(wait, container_name)
      return unless wait.alive?

      system(@docker, "kill", container_name, out: File::NULL, err: File::NULL)
      Process.kill("TERM", -wait.pid)
    rescue Errno::ESRCH
      nil
    end

    def ensure_process_exit(wait, container_name)
      return if wait.join(@grace_seconds)

      system(@docker, "kill", container_name, out: File::NULL, err: File::NULL)
      Process.kill("TERM", -wait.pid)
      return if wait.join(@grace_seconds)

      Process.kill("KILL", -wait.pid)
      wait.join
    rescue Errno::ESRCH
      nil
    end

    def monotonic
      @clock.clock_gettime(Process::CLOCK_MONOTONIC)
    end
    def adapter_identifier = "Backstage::DockerRuntime"
  end
end

Backstage::DockerRuntime = Backstage::Adapters::Docker::Runtime unless defined?(Backstage::DockerRuntime)

# frozen_string_literal: true

require "time"

module Backstage::Adapters::Fake
  # Time that only moves when something moves it. The capture's 250 ms flush boundary is decided by
  # a clock, so proving it lands exactly where it should needs a clock that does not drift — and a
  # scripted runtime that says "these bytes arrived 0.3 seconds in" needs somewhere to say it to.
  class Clock < Backstage::Ports::Clock
    EPOCH = Time.utc(2026, 1, 1)

    def initialize(start: EPOCH)
      @now = start.utc
    end

    def now
      @now
    end

    def advance(seconds)
      @now += seconds.to_f
    end

    # Waiting is the same as advancing here: nothing sleeps, and the full delay always "elapses".
    def wait(seconds, interrupt: nil)
      advance(seconds)
      true
    end
  end

  # A runtime that produces exactly the bytes a test says, at exactly the times it says.
  #
  # `script` is `[[offset_seconds, bytes], …]` — the offset is measured from the start of the run,
  # so `[[0.0, "a\n"], [0.3, "b\n"]]` puts a 300 ms gap between two records and makes the capture's
  # time-based flush fire between them, deterministically. It writes through a capture stream just
  # as the Docker runtime does, so the fake journey exercises the real framing, redaction, chunking
  # and commit path rather than a simplified stand-in for it.
  class Runtime
    Outcome = Backstage::Domain::Outcome
    Records = Backstage::Domain::Records

    def initialize(script: [], clock: nil, status: "succeeded", exit_code: 0, tail_bytes: 64 * 1024)
      @script = Array(script)
      @clock = clock
      @status = status
      @exit_code = exit_code
      @tail_bytes = Integer(tail_bytes)
    end

    def runtime_identity_before_launch? = true

    # `capture` is a `Ports::RuntimeCapture::StreamWriter`, the same contract the Docker runtime
    # takes: whoever opened the stream chose its interpreter and gets its summary.
    def run(bundle: nil, secrets: {}, cancellation: nil, capture: nil)
      started_at = timestamp
      yield({ "type" => "runtime_started", "container_name" => "fake", "at" => started_at }) if block_given?
      tail = +"".b
      elapsed = 0.0
      cancelled = false
      capture_error = nil

      @script.each do |entry|
        offset, bytes = entry
        advance(offset.to_f - elapsed)
        elapsed = offset.to_f
        if cancellation&.call
          cancelled = true
          break
        end
        data = bytes.to_s.dup.force_encoding(Encoding::BINARY)
        tail << data
        tail = tail.byteslice(tail.bytesize - @tail_bytes, @tail_bytes) if tail.bytesize > @tail_bytes
        begin
          # Tick first, then write: that is the order a real drain loop has — the poll that finds no
          # bytes still releases anything the flush interval has aged out — and it is what makes the
          # time boundary land between two scripted records instead of merging them.
          capture&.tick
          capture&.write(data)
        rescue Backstage::CaptureError => error
          capture_error = error
          break
        end
        yield({ "type" => "runtime_progress", "stream" => "combined", "bytes" => data.bytesize,
                "stream_id" => capture&.id, "at" => timestamp }.compact) if block_given?
      end

      reached = if capture_error then "failed"
                elsif cancelled then "cancelled"
                else @status
                end
      # Closing before the status is decided, for the same reason the Docker runtime does it: the
      # final flush is a commit, it can be the first one to fail, and a run short enough to fit in
      # one flush has no other. See Adapters::Docker::Runtime#run.
      summary, capture_error = close(capture, reached, capture_error)
      status = capture_error ? "failed" : reached
      logs = tail.dup.force_encoding(Encoding::UTF_8).scrub("\u{FFFD}")
      # After scrubbing, not before: U+FFFD is three bytes, so a binary tail cut to the limit can
      # come back over it. See Adapters::Docker::Runtime::Tail#text.
      trimmed = logs.bytesize > @tail_bytes
      logs = logs.byteslice(logs.bytesize - @tail_bytes, @tail_bytes).scrub("").force_encoding(Encoding::UTF_8) if trimmed
      Outcome.validate!({
        "schema_version" => 2,
        "status" => status,
        "summary" => capture_error ? "fake runtime could not capture its output: #{capture_error.message}" : "fake runtime #{status}",
        "process" => { "exit_code" => status == "succeeded" ? @exit_code : nil, "signal" => nil },
        "cancellation" => { "requested" => cancelled, "timed_out" => false },
        "logs" => logs,
        "logs_truncated" => trimmed,
        "log_tail_bytes" => @tail_bytes,
        "capture" => capture_block(capture, summary, capture_error),
        "started_at" => started_at,
        "finished_at" => timestamp
      }.compact)
    end

    def adapter_identifier = "Backstage::Adapters::Fake::Runtime"

    private

    # Returns `[summary, capture_error]`; a failing close is a capture failure, not a silent nil.
    def close(capture, status, capture_error)
      return [nil, capture_error] unless capture

      [capture.close(reason: status == "cancelled" ? "cancelled" : (status == "failed" ? "failed" : "close")), capture_error]
    rescue Backstage::CaptureError => error
      [nil, capture_error || error]
    end

    def capture_block(capture, summary, capture_error)
      return nil unless capture

      block = Outcome.capture_summary(summary ? [summary] : [],
                                      status: capture_error || summary.nil? ? "failed" : nil)
      block["stream_id"] = capture.id
      block["last_offset"] = summary && summary["last_offset"]
      block["error"] = capture_error.message if capture_error
      block.compact
    end

    def advance(seconds)
      return unless seconds.positive?

      @clock.advance(seconds) if @clock.respond_to?(:advance)
    end

    def timestamp
      @clock.respond_to?(:now) ? @clock.now.utc.iso8601(6) : Records.timestamp
    end
  end
end

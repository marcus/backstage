# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"

module Backstage::Adapters::Fake
  # A local stand-in for a containerized worker. Given a change root it materializes a real patch
  # file, and given a request channel path it files a real progress request mid-run, so the fake
  # journey exercises evidence and live agent visibility without a container or a paid model call.
  class Runner
    CaptureDefaults = Backstage::Application::CaptureDefaults
    Records = Backstage::Domain::Records
    STEP = "fake"

    def initialize(outcome: nil, change_root: nil, branch: nil, request_channel_path: nil,
                   progress_transition: "report_progress", script: nil, clock: nil)
      @outcome = outcome || {
        "schema_version" => 2,
        "status" => "succeeded",
        "summary" => "fake runner completed",
        "process" => { "exit_code" => 0, "signal" => nil },
        "usage" => { "input_tokens" => 0, "output_tokens" => 0, "cost" => nil }
      }
      @change_root = change_root
      @branch = branch
      @request_channel_path = request_channel_path
      @progress_transition = progress_transition
      @script = script
      @clock = clock
    end

    # The fake runner owns no independent process that can outlive its controller.
    def runtime_identity_before_launch? = true

    # `capture` is a `Ports::RuntimeCapture`. With a `script` the fake runner drives a real capture
    # stream through `Fake::Runtime`, so the fake journey exercises the same framing, redaction,
    # chunking and commit path a container's output takes — the point of a fake journey is to prove
    # the machinery, and output capture is now part of the machinery.
    def run(bundle:, secrets: {}, cancellation: nil, capture: nil, &events)
      yield({ "type" => "run_started", "at" => Records.timestamp }) if block_given?
      file_progress_request
      yield({ "type" => "agent_request_filed", "at" => Records.timestamp }) if block_given?
      if cancellation&.call
        return @outcome.merge("status" => "cancelled", "summary" => "cancelled by controller")
      end

      outcome = @outcome
      outcome = outcome.merge(captured(bundle, cancellation, capture, &events)) if @script
      outcome = outcome.merge("change_artifact" => materialize_change(bundle)) if @change_root && outcome["status"] == "succeeded"
      yield({ "type" => "run_finished", "at" => Records.timestamp }) if block_given?
      outcome
    end

    def adapter_identifier = "Backstage::FakeRunner"

    private

    # The scripted output, and what it means for the outcome. A capture failure or a cancellation
    # inside the stream is the runtime's answer, not the fixture's, so those fields win.
    def captured(bundle, cancellation, capture, &events)
      opener = capture || CaptureDefaults.null(run: bundle&.fetch("id", nil) || "fake", clock: @clock)
      writer = opener.open(step: STEP, interpreter: nil)
      produced = Runtime.new(script: @script, clock: @clock).run(
        bundle: bundle, cancellation: cancellation, capture: writer, &events
      )
      fields = produced.slice("logs", "logs_truncated", "log_tail_bytes", "capture")
      return fields if produced.fetch("status") == "succeeded"

      fields.merge("status" => produced.fetch("status"), "summary" => produced.fetch("summary"))
    end

    def file_progress_request
      return if @request_channel_path.to_s.empty?

      request = { "transition" => @progress_transition, "reason" => "fake worker reporting progress mid-run" }
      File.open(@request_channel_path, "a") { |file| file.puts(JSON.generate(request)) }
    end

    def materialize_change(bundle)
      FileUtils.mkdir_p(@change_root)
      path = File.join(@change_root, "change.patch")
      content = "diff --git a/NOTES.md b/NOTES.md\n+fake candidate for #{bundle&.dig("work_item", "id")} run #{bundle&.fetch("id", nil)}\n"
      File.binwrite(path, content)
      {
        "source_path" => path,
        "branch" => @branch || bundle&.dig("repository", "branch") || "backstage/fake",
        "base_revision" => "fake-base",
        "patch_sha256" => Digest::SHA256.hexdigest(content),
        "patch_size" => content.bytesize
      }
    end
  end
end

Backstage::FakeRunner = Backstage::Adapters::Fake::Runner unless defined?(Backstage::FakeRunner)

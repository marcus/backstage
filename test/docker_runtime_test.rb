# frozen_string_literal: true

require_relative "test_helper"

class DockerRuntimeTest < Minitest::Test
  def test_trivial_container_streams_logs_without_secrets
    skip "set BACKSTAGE_DOCKER_TEST=1 to run the local Docker contract" unless ENV["BACKSTAGE_DOCKER_TEST"] == "1"

    bundle = JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
    secret = "runtime-secret-value"
    bundle["execution"]["credential_refs"] = ["EXAMPLE_TOKEN"]
    bundle["execution"]["command"] = ["bash", "-lc", "printf 'container-ok\\n'; printf \"%s\\n\" \"$EXAMPLE_TOKEN\""]
    events = []
    guard = Backstage::SecretGuard.new(secret_values: [secret])
    capture = Backstage::Application::CaptureDefaults.null(run: "docker-contract", secret_guard: guard)
    writer = capture.open(step: "harness")
    outcome = Backstage::DockerRuntime.new(secret_guard: guard).run(bundle: bundle, secrets: { "EXAMPLE_TOKEN" => secret }, capture: writer) { |event| events << event }

    assert_equal "succeeded", outcome["status"]
    assert_includes outcome["logs"], "container-ok"
    refute_includes outcome["logs"], secret
    assert_equal false, outcome["logs_truncated"]
    # Progress carries counts and a stream id now, not the text: the text is in the stream.
    progress = events.select { |event| event["type"] == "runtime_progress" }
    refute_empty progress
    assert progress.all? { |event| event["stream_id"] == writer.id && !event.key?("text") }
    assert_equal "complete", outcome.dig("capture", "status")
    assert_operator outcome.dig("capture", "streams", 0, "records"), :>=, 2
  end

  def test_controller_cancellation_stops_and_removes_container
    docker_test!
    bundle = fixture_bundle
    bundle["id"] = "cancel-fixture"
    bundle["execution"]["command"] = ["bash", "-lc", "sleep 30"]
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    outcome = Backstage::DockerRuntime.new.run(
      bundle: bundle,
      cancellation: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) - started > 0.2 }
    )

    assert_equal "cancelled", outcome["status"]
    assert_equal true, outcome.dig("cancellation", "requested")
    assert_container_removed("cancel-fixture")
  end

  def test_timeout_stops_and_removes_container
    docker_test!
    bundle = fixture_bundle
    bundle["id"] = "timeout-fixture"
    bundle["execution"]["timeout_seconds"] = 1
    bundle["execution"]["command"] = ["bash", "-lc", "sleep 30"]

    outcome = Backstage::DockerRuntime.new.run(bundle: bundle)

    assert_equal "timed_out", outcome["status"]
    assert_equal true, outcome.dig("cancellation", "timed_out")
    assert_container_removed("timeout-fixture")
  end

  def test_container_repository_guard_rejects_default_branch_before_network_or_credentials
    docker_test!
    in_tmpdir do |directory|
      bundle = fixture_bundle
      bundle["id"] = "default-branch-guard"
      bundle["repository"]["branch"] = "main"
      bundle["execution"]["command"] = ["backstage-container-repository", "prepare"]
      bundle["execution"]["mounts"] = [{ "source" => directory, "target" => "/workspace", "read_only" => false }]

      outcome = Backstage::DockerRuntime.new.run(bundle: bundle)

      assert_equal "failed", outcome["status"]
      assert_includes outcome["logs"], "branch \"main\" is outside configured authority"
      assert_empty Dir.children(directory)
      assert_empty containers_for("default-branch-guard")
    end
  end

  def test_worker_shared_authority_rejects_actual_origin_and_returned_preflight_base
    docker_test!
    runner = Backstage::CommandRunner.new

    origin = runner.run([
      "docker", "run", "--rm", "backstage-worker:0.1.0", "backstage-repository-authority",
      "checkout", "example/widgets", "main", "https://github.com/other/repo.git", "backstage/work", "backstage/work"
    ], allow_failure: true)
    refute origin.success?
    assert_includes origin.stderr, "repository is outside configured authority"

    preflight = runner.run([
      "docker", "run", "--rm", "backstage-worker:0.1.0", "backstage-repository-authority",
      "review", "example/widgets", "main", "example/widgets", "backstage/work", "backstage/work", "release", "true"
    ], allow_failure: true)
    refute preflight.success?
    assert_includes preflight.stderr, "base branch is outside configured authority"
  end

  def test_secret_free_materialization_ignores_credential_boundary_and_preserves_file_changes
    docker_test!
    in_tmpdir do |directory|
      repo = File.join(directory, "repo")
      marker = File.join(directory, "filter-marker")
      filter = File.join(directory, "evil-filter")
      FileUtils.mkdir_p(repo)
      run_git(directory, "init", "--quiet", "repo")
      run_git(repo, "config", "user.name", "Fixture")
      run_git(repo, "config", "user.email", "fixture@local")
      run_git(repo, "remote", "add", "origin", "https://github.com/example/widgets.git")
      run_git(repo, "checkout", "-b", "backstage/malicious-config")
      File.write(File.join(repo, "tracked.txt"), "before\n")
      File.write(File.join(repo, "deleted.txt"), "delete me\n")
      run_git(repo, "add", "--all")
      run_git(repo, "commit", "--quiet", "-m", "base")

      File.write(filter, <<~SH)
        #!/bin/sh
        if [ -n "${GH_TOKEN:-}" ]; then
          printf 'saw-secret\n' >>/workspace/filter-marker
        else
          printf 'no-secret\n' >>/workspace/filter-marker
        fi
        cat
      SH
      FileUtils.chmod(0o755, filter)
      run_git(repo, "config", "filter.evil.clean", "/workspace/evil-filter")
      File.write(File.join(repo, ".gitattributes"), "*.txt filter=evil\n")
      File.write(File.join(repo, "tracked.txt"), "after\n")
      File.write(File.join(repo, "untracked.txt"), "new\n")
      File.binwrite(File.join(repo, "untracked.bin"), "\x00\xFFbinary\n".b)
      FileUtils.rm(File.join(repo, "deleted.txt"))

      bundle = fixture_bundle
      bundle["id"] = "malicious-config-materialization"
      bundle["repository"]["branch"] = "backstage/malicious-config"
      bundle["execution"]["credential_refs"] = []
      bundle["execution"]["command"] = ["backstage-container-repository", "finalize"]
      bundle["execution"]["mounts"] = [{ "source" => directory, "target" => "/workspace", "read_only" => false }]
      outcome = Backstage::DockerRuntime.new.run(bundle: bundle, secrets: {})
      assert_equal "succeeded", outcome["status"], outcome["logs"]
      assert_includes outcome["logs"], "repository_materialized"
      assert File.file?(File.join(directory, "change.patch"))
      assert File.file?(File.join(directory, "change.json"))
      assert_equal ["no-secret"], File.readlines(marker, chomp: true).uniq

      verification = File.join(directory, "verification")
      run_git(directory, "clone", "--quiet", "repo", "verification")
      run_git(verification, "config", "filter.evil.clean", "cat")
      run_git(verification, "apply", "--binary", "--index", File.join(directory, "change.patch"))
      assert_equal "after\n", File.read(File.join(verification, "tracked.txt"))
      assert_equal "new\n", File.read(File.join(verification, "untracked.txt"))
      assert_equal "\x00\xFFbinary\n".b, File.binread(File.join(verification, "untracked.bin"))
      refute File.exist?(File.join(verification, "deleted.txt"))
      assert_empty containers_for("malicious-config-materialization")
    end
  end

  private

  def docker_test!
    skip "set BACKSTAGE_DOCKER_TEST=1 to run the local Docker contract" unless ENV["BACKSTAGE_DOCKER_TEST"] == "1"
  end

  def fixture_bundle
    JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
  end

  def containers_for(job_id)
    `docker ps --filter label=backstage.job=#{job_id} --format '{{.ID}}'`.lines.map(&:strip).reject(&:empty?)
  end

  # `--rm` cleanup happens after the container stops, so removal is eventual rather than instant. On
  # a loaded machine a stopped container can still be listed for a moment; waiting for it keeps this
  # about "the container does not survive" rather than about how busy the host was.
  def assert_container_removed(job_id, timeout: 15)
    deadline = Time.now + timeout
    remaining = containers_for(job_id)
    while !remaining.empty? && Time.now < deadline
      sleep(0.2)
      remaining = containers_for(job_id)
    end

    assert_empty remaining, "container for #{job_id} was still running #{timeout}s after the run returned"
  end

  def run_git(directory, *args)
    Backstage::CommandRunner.new.run(["git", *args], chdir: directory)
  end
end

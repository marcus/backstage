# frozen_string_literal: true

require "json"
require "digest"
require "optparse"

module Backstage::Surfaces
  class CLI
    AuthorityError = Backstage::AuthorityError
    Bootstrap = Backstage::Bootstrap
    ContractError = Backstage::ContractError
    DeploymentPack = Backstage::Configuration::DeploymentPack
    Interrupts = Backstage::Support::Interrupts
    USAGE = {
      "submit" => "backstage submit --title TITLE (--description TEXT | --input-file PATH) [--media-type TYPE] [--target NAME] [--workflow NAME] [--json|--jsonl]",
      "list" => "backstage list [--json|--jsonl]",
      "show" => "backstage show WORK_ID [--json|--jsonl]",
      "cancel" => "backstage cancel RUN_ID [--json|--jsonl]",
      "config" => "backstage config check [--pack PATH] [--json|--jsonl]",
      "workflows" => "backstage workflows [NAME] [--pack PATH] [--json|--jsonl]",
      "transitions" => "backstage transitions WORK_ID [--json|--jsonl]",
      "transition" => "backstage transition WORK_ID --to NAME [--actor human|system] [--request-id ID] [--reason TEXT] [--evidence ARTIFACT_ID] [--expect-state NAME] [--expect-revision N] [--decision ID] [--json|--jsonl]",
      "decide" => "backstage decide WORK_ID --choose TRANSITION [--decision ID] [--reason TEXT] [--request-id ID] [--json|--jsonl]",
      "history" => "backstage history WORK_ID [--json|--jsonl]",
      "recover" => "backstage recover [WORK_ID] [--json|--jsonl]",
      "dispatch" => "backstage dispatch accept WORK_ID [--start NAME] [--publish-draft] [--request-id ID] [--max-retries N] [--retry-delay SECONDS] [--retry-backoff fixed|exponential] [--supersede] | " \
                    "dispatch cancel REF [--reason TEXT] | dispatch list [--all] | dispatch show REF [--publish-draft] | dispatch status [--publish-draft] | " \
                    "dispatch pass [--limit N] [--work WORK_ID] [--publish-draft] | dispatch run [--interval SECONDS] [--max-passes N] [--limit N] [--publish-draft]",
      "activity" => "backstage activity list [--after CURSOR] [--limit N] [--work ID] [--run ID] [--target ID] [--kind TYPE]... [--related ID] [--json|--jsonl] | " \
                    "activity show EVENT_ID [--json|--jsonl] | " \
                    "activity follow [--after CURSOR] [--limit N] [--interval SECONDS] [--max-passes N] [--work ID] [--run ID] [--target ID] [--kind TYPE]... [--related ID] [--json|--jsonl]",
      "source" => "backstage source list | source submit CONNECTION REF [--target NAME] | source poll CONNECTION [--target NAME] | source refresh WORK_ID [--json|--jsonl]",
      "deliver" => "backstage deliver WORK_ID --operation OP [--operation OP] | deliver show ACTION_ID | deliver resolve ACTION_ID (--applied|--not-applied) --reason TEXT [--json|--jsonl]",
      "process" => "backstage process WORK_ID [--pack PATH] [--start NAME] [--publish-draft] [--deliver OP] [--json|--jsonl]",
      "version" => "backstage version [--json|--jsonl]",
      "help" => "backstage help [--json|--jsonl]"
    }.freeze
    COMMANDS = USAGE.keys.freeze
    DEFAULT_STATE = ".backstage/state.jsonl"
    DEFAULT_ARTIFACTS = ".backstage/artifacts"

    def initialize(argv, out: $stdout, err: $stderr, env: ENV)
      @argv = argv.dup
      @out = out
      @err = err
      @env = env
    end

    def call
      global = {
        state: @env["BACKSTAGE_STATE"],
        artifacts: @env["BACKSTAGE_ARTIFACTS"],
        state_explicit: @env.key?("BACKSTAGE_STATE"),
        artifacts_explicit: @env.key?("BACKSTAGE_ARTIFACTS"),
        format: :human
      }
      extract_format!(global)
      extract_pack!(global)
      parser = global_parser(global)
      parser.order!(@argv)
      return print_help(parser) if global[:help]
      command = @argv.shift
      return print_help(parser) unless command
      return print_command_help(command, global[:format]) if @argv.delete("--help") || @argv.delete("-h")

      resolve_store_options!(global, command)
      system = %w[config version help].include?(command) ? nil : build_system(global)
      # Activity's three subcommands shape their own output (a page's events plus cursor metadata,
      # or a live series of pages from `follow`) rather than the one generic record/list emission
      # every other command shares, so they handle emission themselves instead of returning a
      # value for the single `emit` call below.
      if command == "activity"
        activity_command(system, global)
      else
        result = dispatch(command, system, global)
        emit(result, global[:format])
      end
      0
    rescue OptionParser::ParseError, Backstage::Error, KeyError, JSON::ParserError => error
      emit_error(error, defined?(global) && global[:format] != :human)
      1
    end

    private

    def global_parser(options)
      OptionParser.new do |parser|
        parser.banner = "Usage: backstage [global options] COMMAND [command options]"
        parser.on("--state PATH", "JSONL state path") { |value| options[:state] = value; options[:state_explicit] = true }
        parser.on("--artifacts PATH", "Artifact directory") { |value| options[:artifacts] = value; options[:artifacts_explicit] = true }
        parser.on("--json", "Emit JSON") { options[:format] = :json }
        parser.on("--jsonl", "Emit newline-delimited JSON") { options[:format] = :jsonl }
        parser.on("--pack PATH", "Deployment pack directory") { |value| options[:pack] = value }
        parser.on("-h", "--help", "Show help") { options[:help] = true }
      end
    end

    def dispatch(command, system, global)
      engine = system&.engine
      case command
      when "submit" then submit(system)
      when "list" then engine.list_work
      when "show" then engine.show_work(required_argument("WORK_ID"))
      when "cancel" then engine.request_cancel(required_argument("RUN_ID"))
      when "version" then { "version" => Backstage::VERSION }
      when "config" then config_command
      when "workflows" then workflows_command(system)
      when "transitions" then system.workflows.allowed_transitions(required_argument("WORK_ID"))
      when "transition" then transition_command(system)
      when "decide" then decide_command(system)
      when "history" then system.workflows.history(required_argument("WORK_ID"))
      when "recover" then system.recovery.reconcile(@argv.shift)
      when "source" then source_command(system)
      when "deliver" then delivery_command(system)
      when "dispatch" then dispatch_command(system)
      when "process" then process_command(system)
      when "help" then help_payload(global)
      else raise OptionParser::InvalidArgument, "unknown command #{command.inspect}"
      end
    end

    def submit(system)
      options = { pack: @active_pack }
      OptionParser.new do |opts|
        opts.on("--title TITLE") { |value| options[:title] = value }
        opts.on("--description TEXT") { |value| options[:description] = value }
        opts.on("--input-file PATH") { |value| options[:input_file] = value }
        opts.on("--media-type TYPE") { |value| options[:media_type] = value }
        opts.on("--idempotency-key KEY") { |value| options[:idempotency_key] = value }
        opts.on("--target NAME") { |value| options[:target] = value }
        opts.on("--workflow NAME") { |value| options[:workflow] = value }
      end.parse!(@argv)
      title = options[:title] || raise(OptionParser::MissingArgument, "--title")
      if options.key?(:description) == options.key?(:input_file)
        raise ContractError, "provide exactly one of --description and --input-file"
      end
      content = options[:input_file] ? read_input(options[:input_file]) : options[:description]
      input = { "content" => content, "media_type" => options[:media_type] || "text/plain" }
      config = system.configuration(options[:pack])
      target = options[:target] || (config.targets.keys.first if config.targets.keys.one?)
      raise ContractError, "--target is required when a pack has multiple targets" unless target
      key = options[:idempotency_key] || "manual:v2:#{Digest::SHA256.hexdigest(JSON.generate([target, title, input]))}"
      workflow = options[:workflow] ? config.workflow(options[:workflow]) : config.workflow_for_target(target)
      system.engine.submit(idempotency_key: key, title: title, input: input, workflow: workflow, **config.binding_for(target))
    end

    def read_input(path)
      File.read(path, encoding: "UTF-8")
    rescue SystemCallError => error
      raise ContractError, "cannot read input document: #{error.message}"
    end

    def workflows_command(system)
      config = system.configuration(@active_pack)
      name = @argv.shift
      return config.workflow(name).to_h if name

      config.workflows.values.map { |workflow| workflow.to_h.slice("name", "version", "description", "initial_state").merge("digest" => workflow.digest) }
    end

    # Trusted operator entry. An operator may act as a human or as the system; nothing here can
    # claim to be a working agent or an independent reviewer.
    def transition_command(system)
      work_id = required_argument("WORK_ID")
      options = { actor: "human", evidence: [] }
      OptionParser.new do |opts|
        opts.on("--to NAME", "Transition to request") { |value| options[:to] = value }
        opts.on("--actor ROLE", "human (default) or system") { |value| options[:actor] = value }
        opts.on("--request-id ID", "Stable request identity for idempotent retries") { |value| options[:request_id] = value }
        opts.on("--reason TEXT") { |value| options[:reason] = value }
        opts.on("--evidence ARTIFACT_ID", "Repeatable") { |value| options[:evidence] << value }
        opts.on("--expect-state NAME") { |value| options[:expected_state] = value }
        opts.on("--expect-revision N", Integer) { |value| options[:expected_revision] = value }
        opts.on("--decision ID", "Decision this answers") { |value| options[:decision_id] = value }
      end.parse!(@argv)
      transition = options[:to] || raise(OptionParser::MissingArgument, "--to")

      system.workflows.request_transition(
        work_item_id: work_id,
        transition: transition,
        actor: { "role" => options[:actor], "id" => @env["USER"], "entry" => "operator_cli" },
        request_id: options[:request_id] || "cli:#{work_id}:#{transition}:#{Backstage::Domain::Records.timestamp}",
        expected_state: options[:expected_state],
        expected_revision: options[:expected_revision],
        reason: options[:reason],
        evidence: options[:evidence],
        decision_id: options[:decision_id]
      )
    end

    def decide_command(system)
      work_id = required_argument("WORK_ID")
      options = { evidence: [] }
      OptionParser.new do |opts|
        opts.on("--decision ID", "Decision being answered") { |value| options[:decision_id] = value }
        opts.on("--choose NAME", "Chosen transition") { |value| options[:choice] = value }
        opts.on("--reason TEXT") { |value| options[:reason] = value }
        opts.on("--request-id ID") { |value| options[:request_id] = value }
        opts.on("--evidence ARTIFACT_ID", "Repeatable") { |value| options[:evidence] << value }
      end.parse!(@argv)
      choice = options[:choice] || raise(OptionParser::MissingArgument, "--choose")
      work = system.engine.store.fetch!("work_items", work_id)
      decision_id = options[:decision_id] || work["open_decision_id"] || raise(ContractError, "work item #{work_id} has no open decision")

      system.workflows.request_transition(
        work_item_id: work_id,
        transition: choice,
        actor: { "role" => "human", "id" => @env["USER"], "entry" => "operator_cli" },
        request_id: options[:request_id] || "decide:#{decision_id}:#{choice}",
        reason: options[:reason],
        evidence: options[:evidence],
        decision_id: decision_id
      )
    end

    def required_argument(name)
      @argv.shift || raise(OptionParser::MissingArgument, name)
    end

    def build_system(options)
      Bootstrap::System.build(state: options[:state], artifacts: options[:artifacts], env: @env, pack: options[:pack])
    end

    def config_command
      subcommand = required_argument("CONFIG_COMMAND")
      raise OptionParser::InvalidArgument, "unknown config command #{subcommand.inspect}" unless subcommand == "check"

      options = { pack: @active_pack }
      OptionParser.new { |opts| opts.on("--pack PATH") { |value| options[:pack] = value } }.parse!(@argv)
      DeploymentPack.new(options[:pack]).check
    end

    def source_command(system)
      command = required_argument("SOURCE_COMMAND")
      config = system.configuration(@active_pack)
      admission = system.source_admission(configuration: config)
      case command
      when "list"
        config.sources.map do |name, source|
          source.slice("kind", "identity", "targets", "default_target", "operations").merge("connection" => name,
            "capabilities" => system.source_adapter(config, name).capabilities)
        end
      when "submit", "poll"
        connection = required_argument("CONNECTION")
        ref = required_argument("REF") if command == "submit"
        options = {}
        OptionParser.new { |opts| opts.on("--target NAME") { |value| options[:target] = value } }.parse!(@argv)
        command == "submit" ? admission.submit(connection: connection, ref: ref, target: options[:target]) :
          admission.poll(connection: connection, target: options[:target])
      when "refresh" then admission.refresh(required_argument("WORK_ID"))
      else raise OptionParser::InvalidArgument, "unknown source command #{command.inspect}"
      end
    end

    def delivery_command(system)
      reference = required_argument("WORK_ID or DELIVERY_COMMAND")
      delivery = system.result_delivery(configuration: system.configuration(@active_pack))
      return delivery.describe(required_argument("ACTION_ID")) if reference == "show"
      if reference == "resolve"
        action = required_argument("ACTION_ID")
        options = {}
        OptionParser.new do |opts|
          opts.on("--applied") { raise ContractError, "choose one resolution" if options.key?(:applied); options[:applied] = true }
          opts.on("--not-applied") { raise ContractError, "choose one resolution" if options.key?(:applied); options[:applied] = false }
          opts.on("--reason TEXT") { |value| options[:reason] = value }
        end.parse!(@argv)
        raise OptionParser::MissingArgument, "--applied or --not-applied" unless options.key?(:applied)
        return delivery.resolve(action, applied: options[:applied], reason: options[:reason] || raise(OptionParser::MissingArgument, "--reason"))
      end
      operations = []
      OptionParser.new { |opts| opts.on("--operation OP") { |value| operations << value } }.parse!(@argv)
      delivery.deliver(work_item_id: reference, operations: operations)
    end

    # The activity projection's operator surface: `list`/`show` are one-shot reads, `follow` is a
    # foreground process like `dispatch run` that blocks between passes and prints as it goes,
    # rather than buffering an unbounded session in memory to hand back at the end.
    def activity_command(system, global)
      subcommand = required_argument("ACTIVITY_COMMAND")
      case subcommand
      when "list" then activity_list(system, global)
      when "show" then activity_show(system, global)
      when "follow" then activity_follow(system, global)
      else raise OptionParser::InvalidArgument, "unknown activity command #{subcommand.inspect}"
      end
    end

    def activity_list(system, global)
      options = { filters: {} }
      activity_filter_parser(options).parse!(@argv)
      page = system.activity_query.list(after: options[:after], filters: options[:filters], limit: options[:limit] || 100)
      emit_activity_page(page, global[:format])
    end

    def activity_show(system, global)
      event_id = required_argument("EVENT_ID")
      event = system.activity_query.show(event_id)
      emit_activity_record(event, global[:format])
    end

    # Blocks, printing each page as it is read, until `--max-passes` reads have happened or a
    # SIGINT/SIGTERM arrives — the same shape `dispatch run` already uses, so an operator or a
    # supervisor scripting either command learns one pattern. `Clock` is the real system clock;
    # tests reach the same polling logic directly through Backstage::Application::ActivityQuery
    # with a fake one instead of signalling a real process.
    def activity_follow(system, global)
      options = { filters: {} }
      parser = activity_filter_parser(options)
      parser.on("--interval SECONDS", Float, "Polling delay while caught up (default 1)") { |value| options[:interval] = value }
      parser.on("--max-passes N", Integer, "Stop after N reads instead of running until signalled") { |value| options[:max_passes] = value }
      parser.parse!(@argv)

      interrupts = Interrupts.new.install
      passes = 0
      begin
        cursor = system.activity_query.follow(
          after: options[:after], filters: options[:filters], limit: options[:limit] || 100,
          interval: options[:interval] || 1, max_passes: options[:max_passes],
          clock: system.clock, interrupt: interrupts
        ) do |page|
          passes += 1
          emit_activity_page(page, global[:format])
        end
        stop_reason = interrupts.stopped? ? "signal:#{interrupts.reason}" : "max_passes"
        emit_activity_follow_summary({ "schema_version" => 1, "passes" => passes, "stop_reason" => stop_reason, "cursor" => cursor }, global[:format])
      ensure
        interrupts.close
      end
    end

    def activity_filter_parser(options)
      OptionParser.new do |opts|
        opts.on("--after CURSOR", "Resume after this opaque cursor") { |value| options[:after] = value }
        opts.on("--limit N", Integer, "Maximum events per page (default 100)") { |value| options[:limit] = value }
        opts.on("--work ID", "Filter to one work item") { |value| options[:filters]["work_item_id"] = value }
        opts.on("--run ID", "Filter to one run") { |value| options[:filters]["run_id"] = value }
        opts.on("--target ID", "Filter to one target") { |value| options[:filters]["target_id"] = value }
        opts.on("--kind TYPE", "Filter to one activity type; repeatable") { |value| (options[:filters]["type"] ||= []) << value }
        opts.on("--related ID", "Filter to anything related to this id") { |value| options[:filters]["related_id"] = value }
      end
    end

    # JSON prints the whole page, as every other command does. JSONL prints one line per matching
    # event followed by one trailing line of cursor metadata, so a consumer can `jq` the events and
    # still see whether it is caught up without parsing a wrapping object. Human output is one
    # compact line per event; a filtered or already-caught-up page may print nothing at all, same
    # as `list` does for any other empty result.
    def emit_activity_page(page, format)
      case format
      when :json then @out.puts(JSON.pretty_generate(page))
      when :jsonl
        page.fetch("events").each { |event| @out.puts(JSON.generate(event)) }
        @out.puts(JSON.generate(page.slice("cursor", "next_cursor", "high_water_mark", "caught_up")))
      else
        page.fetch("events").each { |event| @out.puts(activity_event_line(event)) }
      end
    end

    def emit_activity_record(value, format)
      case format
      when :json then @out.puts(JSON.pretty_generate(value))
      when :jsonl then @out.puts(JSON.generate(value))
      else @out.puts(activity_event_line(value))
      end
    end

    def emit_activity_follow_summary(summary, format)
      case format
      when :json then @out.puts(JSON.pretty_generate(summary))
      when :jsonl then @out.puts(JSON.generate(summary))
      else @out.puts("stopped: #{summary.fetch("stop_reason")} after #{summary.fetch("passes")} passes, cursor=#{summary.fetch("cursor").inspect}")
      end
    end

    def activity_event_line(event)
      [event["sequence"], event["type"], event["work_item_id"] || event["run_id"] || event["target_id"], event["summary"]].compact.join("\t")
    end

    # The durable dispatcher's operator surface. Every subcommand is noninteractive and has the same
    # structured payload under --json as it prints for a human.
    def dispatch_command(system)
      subcommand = required_argument("DISPATCH_COMMAND")
      case subcommand
      when "accept" then dispatch_accept(system)
      when "cancel" then dispatch_cancel(system)
      when "list" then dispatch_list(system)
      when "show" then dispatch_show(system)
      when "status" then dispatch_status(system)
      when "pass" then dispatch_pass(system)
      when "run" then dispatch_run(system)
      else raise OptionParser::InvalidArgument, "unknown dispatch command #{subcommand.inspect}"
      end
    end

    def dispatch_accept(system)
      work_id = required_argument("WORK_ID")
      options = { pack: @active_pack, mode: "fake", retry: {} }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--start NAME", "Transition that starts work when the state offers several") { |value| options[:start] = value }
        opts.on("--publish-draft", "Accept for the real container/GitHub path") { options[:mode] = "publish_draft" }
        opts.on("--request-id ID", "Stable acceptance identity for idempotent retries") { |value| options[:request_id] = value }
        opts.on("--max-retries N", Integer, "Automatic retries after a failure (0 disables them)") { |value| options[:retry]["max_retries"] = value }
        opts.on("--retry-delay SECONDS", Integer) { |value| options[:retry]["delay_seconds"] = value }
        opts.on("--retry-backoff NAME", "fixed or exponential") { |value| options[:retry]["backoff"] = value }
        opts.on("--max-retry-delay SECONDS", Integer) { |value| options[:retry]["max_delay_seconds"] = value }
        opts.on("--supersede", "Cancel the active intent and accept a new generation") { options[:supersede] = true }
      end.parse!(@argv)
      config = system.configuration(options[:pack])
      # The pack default and the acceptance identity are the dispatcher's rules, not the CLI's; only
      # the operator's explicit overrides travel from here.
      system.dispatcher(configuration: config, authorized_mode: options[:mode]).accept(
        work_item_id: work_id,
        request_id: options[:request_id],
        mode: options[:mode],
        start_transition: options[:start],
        retry_policy: options[:retry],
        accepted_by: @env["USER"],
        supersede: options[:supersede]
      )
    end

    def dispatch_cancel(system)
      reference = required_argument("REF")
      options = { pack: @active_pack }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--reason TEXT") { |value| options[:reason] = value }
      end.parse!(@argv)
      dispatcher_for(system, pack: options[:pack]).cancel(reference, reason: options[:reason])
    end

    def dispatch_show(system)
      reference = required_argument("REF")
      options = { pack: @active_pack, mode: "fake" }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--publish-draft", "Report as a dispatcher authorized for real execution") { options[:mode] = "publish_draft" }
      end.parse!(@argv)
      dispatcher_for(system, pack: options[:pack], mode: options[:mode]).describe(reference)
    end

    def dispatch_list(system)
      options = { pack: @active_pack }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--all", "Include completed, cancelled and exhausted intents") { options[:all] = true }
      end.parse!(@argv)
      dispatcher_for(system, pack: options[:pack]).intents(all: options[:all] == true)
    end

    def dispatch_status(system)
      options = { pack: @active_pack, mode: "fake" }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--publish-draft", "Report as a dispatcher authorized for real execution") { options[:mode] = "publish_draft" }
      end.parse!(@argv)
      ownership = system.dispatch_ownership
      held = !ownership.free?
      dispatcher_for(system, pack: options[:pack], mode: options[:mode]).queue_status.merge(
        "owner" => held ? ownership.current : nil,
        "last_owner" => ownership.current
      )
    end

    def dispatch_pass(system)
      options = { pack: @active_pack, mode: "fake" }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--limit N", Integer, "Dispatch at most N intents in this pass") { |value| options[:limit] = value }
        opts.on("--work WORK_ID", "Restrict the pass to one accepted work item") { |value| options[:work] = value }
        opts.on("--publish-draft", "Authorize consuming intents accepted for real execution") { options[:mode] = "publish_draft" }
      end.parse!(@argv)
      worker(system, options).once(limit: options[:limit], work_item_id: options[:work])
    end

    def dispatch_run(system)
      options = { pack: @active_pack, mode: "fake" }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--interval SECONDS", Float, "Polling delay between passes") { |value| options[:interval] = value }
        opts.on("--max-passes N", Integer, "Stop after N passes instead of running until signalled") { |value| options[:passes] = value }
        opts.on("--limit N", Integer, "Dispatch at most N intents per pass") { |value| options[:limit] = value }
        opts.on("--publish-draft", "Authorize consuming intents accepted for real execution") { options[:mode] = "publish_draft" }
      end.parse!(@argv)
      worker(system, options).run(max_passes: options[:passes], limit: options[:limit])
    end

    def worker(system, options)
      config = system.configuration(options[:pack])
      system.worker(
        configuration: config,
        authorized_mode: options[:mode],
        interval: options[:interval] || config.dispatcher_policy.fetch("poll_interval_seconds")
      )
    end

    def dispatcher_for(system, pack: nil, mode: "fake")
      system.dispatcher(configuration: system.configuration(pack || @active_pack), authorized_mode: mode)
    end

    def process_command(system)
      work_id = required_argument("WORK_ID")
      options = { pack: @active_pack, fake: true, deliver: [] }
      OptionParser.new do |opts|
        opts.on("--pack PATH") { |value| options[:pack] = value }
        opts.on("--publish-draft", "Run paid/container/GitHub path") { options[:fake] = false }
        opts.on("--deliver OP", "Deliver a configured source operation after approval; repeatable") { |value| options[:deliver] << value }
        opts.on("--start NAME", "Transition that starts work when the state offers several") { |value| options[:start] = value }
      end.parse!(@argv)
      config = system.configuration(options[:pack])
      # Direct processing stays available for work nobody accepted. It never steps past a
      # dispatcher that owns this item: cancelling or superseding the intent is the owning control.
      system.dispatcher(configuration: config).guard_direct_processing!(work_id)
      runtime = options[:fake] ? nil : system.publish_runtime
      result = system.controller(configuration: config, runtime: runtime).process(work_item_id: work_id, start_transition: options[:start])
      return result if options[:deliver].empty?
      result.merge("delivery" => system.result_delivery(configuration: config).deliver(work_item_id: work_id, operations: options[:deliver]))
    end

    def extract_format!(global)
      if @argv.delete("--json")
        global[:format] = :json
      elsif @argv.delete("--jsonl")
        global[:format] = :jsonl
      end
    end

    def extract_pack!(global)
      index = @argv.index("--pack")
      global[:pack] = index ? @argv.fetch(index + 1) { raise OptionParser::MissingArgument, "--pack" } : "packs/example"
      @argv.slice!(index, 2) if index
      @active_pack = global[:pack]
    end

    def resolve_store_options!(global, command)
      if %w[config version help].include?(command)
        global[:state] ||= DEFAULT_STATE
        global[:artifacts] ||= DEFAULT_ARTIFACTS
        return
      end

      config = DeploymentPack.new(global.fetch(:pack))
      global[:state] = config.state_path unless global[:state_explicit]
      global[:artifacts] = config.artifact_path unless global[:artifacts_explicit]
      global[:state] ||= DEFAULT_STATE
      global[:artifacts] ||= DEFAULT_ARTIFACTS
    end

    def emit(value, format)
      case format
      when :json then @out.puts(JSON.pretty_generate(value))
      when :jsonl
        records(value).each { |item| @out.puts(JSON.generate(item)) }
      else emit_human(value)
      end
    end

    def emit_human(value)
      records(value).each do |item|
        if item.is_a?(Hash)
          @out.puts([item["id"], item["state"] || item["status"], item["title"] || item["summary"] || item["version"]].compact.join("\t"))
        else
          @out.puts(item)
        end
      end
    end

    def records(value)
      value.is_a?(Array) ? value : [value]
    end

    def emit_error(error, json)
      payload = { "error" => { "type" => error.class.name, "message" => error.message } }
      payload["error"]["code"] = error.code if error.respond_to?(:code)
      if json
        @err.puts(JSON.generate(payload))
      else
        suffix = error.respond_to?(:code) ? " (#{error.code})" : ""
        @err.puts("error: #{error.message}#{suffix}")
      end
    end

    def print_help(parser)
      @out.puts(parser)
      @out.puts("Commands: #{COMMANDS.join(", ")}")
      0
    end

    def print_command_help(command, format)
      usage = USAGE.fetch(command) { raise OptionParser::InvalidArgument, "unknown command #{command.inspect}" }
      payload = { "command" => command, "usage" => usage }
      format == :human ? @out.puts("Usage: #{usage}") : emit(payload, format)
      0
    end

    def help_payload(options)
      { "commands" => COMMANDS, "usage" => USAGE, "state" => options[:state], "artifacts" => options[:artifacts] }
    end
  end
end

Backstage::CLI = Backstage::Surfaces::CLI unless defined?(Backstage::CLI)

# frozen_string_literal: true

require "digest"
require "json"

module Backstage::Domain
  # An immutable, validated work lifecycle compiled from ordinary configuration data.
  #
  # Compilation is a state-free function of a plain hash, so any headless caller — the pack
  # compiler, a contract test, a future service host — validates a definition the same way.
  class Workflow
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    ACTORS = %w[system human agent reviewer].freeze
    PHASES = %w[implementation review].freeze
    EVIDENCE = %w[change_candidate review_verdict].freeze
    DEFINITION_KEYS = %w[name version description initial_state max_revisions states transitions].freeze
    DECISION_KEYS = %w[question choices].freeze
    STATE_KEYS = %w[description terminal awaits_decision continue].freeze
    TRANSITION_KEYS = %w[description from to actors requires counts_revision dispatch decision].freeze
    DISPATCH_KEYS = %w[phase on_success on_verdict on_failure].freeze
    VERDICTS = %w[approved changes_requested blocked].freeze

    State = Struct.new(:name, :description, :terminal, :awaits_decision, :continue, keyword_init: true) do
      def terminal? = terminal
      def awaits_decision? = awaits_decision
      def to_h
        { "description" => description, "terminal" => terminal, "awaits_decision" => awaits_decision, "continue" => continue }.compact
      end
    end

    Dispatch = Struct.new(:phase, :on_success, :on_verdict, :on_failure, keyword_init: true) do
      def transition_for_verdict(verdict) = (on_verdict || {})[verdict]

      def to_h
        { "phase" => phase, "on_success" => on_success, "on_verdict" => on_verdict, "on_failure" => on_failure }.compact
      end
    end

    Transition = Struct.new(:name, :description, :from, :to, :actors, :requires, :counts_revision, :dispatch, :decision, keyword_init: true) do
      def allows?(actor_role) = actors.include?(actor_role)
      def counts_revision? = counts_revision

      def to_h
        {
          "description" => description, "from" => from, "to" => to, "actors" => actors,
          "requires" => requires, "counts_revision" => counts_revision,
          "dispatch" => dispatch&.to_h, "decision" => decision
        }.compact
      end
    end

    attr_reader :name, :version, :description, :initial_state, :max_revisions, :states, :transitions, :source

    # Compiles a definition hash into a Workflow, raising ContractError with a concrete
    # diagnostic naming the workflow and the offending key.
    def self.compile(definition, name: nil)
      new(definition, name: name)
    end

    def initialize(definition, name: nil)
      raise ContractError, "workflow definition must be an object" unless definition.is_a?(Hash)

      @source = JSON.parse(JSON.generate(definition))
      @name = (@source["name"] || name).to_s
      raise ContractError, "workflow name is required" if @name.empty?

      reject_unknown!(@source, DEFINITION_KEYS, "definition")
      @version = @source.fetch("version", 1)
      raise ContractError, "workflow #{@name} version must be a positive integer" unless @version.is_a?(Integer) && @version.positive?

      @description = @source["description"]
      @max_revisions = @source.fetch("max_revisions", 0)
      unless @max_revisions.is_a?(Integer) && @max_revisions >= 0
        raise ContractError, "workflow #{@name} max_revisions must be a non-negative integer"
      end

      @states = compile_states(@source["states"])
      @transitions = compile_transitions(@source["transitions"])
      @initial_state = @source["initial_state"].to_s
      validate_graph!
      freeze_deeply
    end

    attr_reader :digest

    def state(name)
      states.fetch(name.to_s) { raise ContractError, "workflow #{@name} has no state #{name.inspect}" }
    end

    def state?(name) = states.key?(name.to_s)

    def transition(name)
      transitions.fetch(name.to_s) { raise ContractError, "workflow #{@name} has no transition #{name.inspect}" }
    end

    def transition?(name) = transitions.key?(name.to_s)

    def terminal?(state_name) = state(state_name).terminal?

    def transitions_from(state_name)
      transitions.values.select { |candidate| candidate.from.include?(state_name.to_s) }
    end

    # What a work item carries: identity plus the digest of the definition it was admitted with.
    # The definition itself is stored once, under that digest, so a later pack edit cannot
    # reinterpret work already in flight.
    def binding
      { "name" => name, "version" => version, "digest" => digest }
    end

    def snapshot_record
      { "schema_version" => 1, "id" => digest, "name" => name, "version" => version, "definition" => to_h, "created_at" => Records.timestamp }
    end

    def to_h
      JSON.parse(JSON.generate({
        "name" => name,
        "version" => version,
        "description" => description,
        "initial_state" => initial_state,
        "max_revisions" => max_revisions,
        "states" => states.transform_values(&:to_h),
        "transitions" => transitions.transform_values(&:to_h)
      }.compact))
    end

    private

    NAME = /\A[a-z][a-z0-9_]*\z/

    # State and transition names become keys in persisted records, where the secret guard reads
    # them. Refusing a bad name here gives a pack author a diagnostic they can act on instead of a
    # store-level surprise later.
    def validate_name!(candidate, kind)
      raise ContractError, "workflow #{name} #{kind} name #{candidate.inspect} must be lower_snake_case" unless candidate.match?(NAME)
      return unless candidate.match?(Backstage::Support::SecretGuard::SECRET_KEY)

      raise ContractError, "workflow #{name} #{kind} name #{candidate.inspect} reads as a credential field; choose another"
    end

    def reject_unknown!(body, allowed, context)
      unknown = body.keys.map(&:to_s) - allowed
      return if unknown.empty?

      raise ContractError, "workflow #{name} #{context} has unknown keys #{unknown.sort.join(", ")}"
    end

    def boolean!(body, key, context)
      value = body.fetch(key, false)
      unless value == true || value == false
        raise ContractError, "workflow #{name} #{context} #{key} must be a boolean"
      end
      value
    end

    def compile_states(raw)
      raise ContractError, "workflow #{name} must define states" unless raw.is_a?(Hash) && !raw.empty?

      raw.to_h do |state_name, body|
        body = {} if body.nil?
        raise ContractError, "workflow #{name} state #{state_name} must be an object" unless body.is_a?(Hash)

        validate_name!(state_name.to_s, "state")
        reject_unknown!(body, STATE_KEYS, "state #{state_name}")
        [state_name.to_s, State.new(
          name: state_name.to_s,
          description: body["description"],
          terminal: boolean!(body, "terminal", "state #{state_name}"),
          awaits_decision: boolean!(body, "awaits_decision", "state #{state_name}"),
          continue: body["continue"]&.to_s
        )]
      end
    end

    def compile_transitions(raw)
      raise ContractError, "workflow #{name} must define transitions" unless raw.is_a?(Hash) && !raw.empty?

      raw.to_h do |transition_name, body|
        raise ContractError, "workflow #{name} transition #{transition_name} must be an object" unless body.is_a?(Hash)

        validate_name!(transition_name.to_s, "transition")
        reject_unknown!(body, TRANSITION_KEYS, "transition #{transition_name}")
        from = Array(body["from"]).map(&:to_s)
        raise ContractError, "workflow #{name} transition #{transition_name} needs at least one from state" if from.empty?

        to = body["to"].to_s
        raise ContractError, "workflow #{name} transition #{transition_name} needs a to state" if to.empty?

        actors = Array(body["actors"]).map(&:to_s)
        raise ContractError, "workflow #{name} transition #{transition_name} needs at least one actor" if actors.empty?

        unknown_actors = actors - ACTORS
        raise ContractError, "workflow #{name} transition #{transition_name} has unknown actors #{unknown_actors.join(", ")}" unless unknown_actors.empty?

        requires = Array(body["requires"]).map(&:to_s)
        unknown_evidence = requires - EVIDENCE
        raise ContractError, "workflow #{name} transition #{transition_name} requires unknown evidence #{unknown_evidence.join(", ")}" unless unknown_evidence.empty?

        [transition_name.to_s, Transition.new(
          name: transition_name.to_s,
          description: body["description"],
          from: from,
          to: to,
          actors: actors,
          requires: requires,
          counts_revision: boolean!(body, "counts_revision", "transition #{transition_name}"),
          dispatch: compile_dispatch(transition_name, body["dispatch"]),
          decision: compile_decision(transition_name, body["decision"])
        )]
      end
    end

    def compile_dispatch(transition_name, body)
      return nil if body.nil?
      raise ContractError, "workflow #{name} transition #{transition_name} dispatch must be an object" unless body.is_a?(Hash)

      reject_unknown!(body, DISPATCH_KEYS, "transition #{transition_name} dispatch")
      phase = body["phase"].to_s
      raise ContractError, "workflow #{name} transition #{transition_name} dispatch phase must be one of #{PHASES.join(", ")}" unless PHASES.include?(phase)

      on_verdict = body["on_verdict"]
      unless on_verdict.nil?
        raise ContractError, "workflow #{name} transition #{transition_name} on_verdict must be an object" unless on_verdict.is_a?(Hash)

        unknown = on_verdict.keys.map(&:to_s) - VERDICTS
        raise ContractError, "workflow #{name} transition #{transition_name} on_verdict has unknown verdicts #{unknown.join(", ")}" unless unknown.empty?

        on_verdict = on_verdict.to_h { |verdict, target| [verdict.to_s, target.to_s] }
      end
      if phase == "review" && on_verdict.nil?
        raise ContractError, "workflow #{name} transition #{transition_name} dispatches a review and must map on_verdict"
      end
      if phase != "review" && body["on_success"].nil?
        raise ContractError, "workflow #{name} transition #{transition_name} dispatch must name on_success"
      end

      Dispatch.new(phase: phase, on_success: body["on_success"]&.to_s, on_verdict: on_verdict, on_failure: body["on_failure"]&.to_s)
    end

    def compile_decision(transition_name, body)
      return nil if body.nil?
      raise ContractError, "workflow #{name} transition #{transition_name} decision must be an object" unless body.is_a?(Hash)

      reject_unknown!(body, DECISION_KEYS, "transition #{transition_name} decision")
      question = body["question"].to_s
      raise ContractError, "workflow #{name} transition #{transition_name} decision needs a question" if question.empty?

      choices = Array(body["choices"]).map(&:to_s)
      raise ContractError, "workflow #{name} transition #{transition_name} decision needs choices" if choices.empty?

      { "question" => question, "choices" => choices }
    end

    def validate_graph!
      raise ContractError, "workflow #{name} initial_state is required" if initial_state.empty?
      raise ContractError, "workflow #{name} initial_state #{initial_state} is not a defined state" unless state?(initial_state)
      raise ContractError, "workflow #{name} initial_state #{initial_state} must not be terminal" if state(initial_state).terminal?
      raise ContractError, "workflow #{name} must define at least one terminal state" if states.values.none?(&:terminal?)

      transitions.each_value do |candidate|
        (candidate.from + [candidate.to]).each do |referenced|
          raise ContractError, "workflow #{name} transition #{candidate.name} references unknown state #{referenced}" unless state?(referenced)
        end
        candidate.from.each do |origin|
          next unless state(origin).terminal?

          unless candidate.actors == ["human"]
            raise ContractError, "workflow #{name} transition #{candidate.name} reopens terminal state #{origin}, which only a human may do"
          end
          if candidate.dispatch
            raise ContractError, "workflow #{name} transition #{candidate.name} reopens terminal state #{origin} and must not dispatch work itself"
          end
        end
        validate_dispatch_targets!(candidate)
        validate_decision_choices!(candidate)
      end

      validate_verdict_requirements!
      validate_ambiguity!
      validate_decision_states!
      validate_continue!
      validate_automatic_cycles!
      validate_reachability!
    end

    def validate_dispatch_targets!(candidate)
      dispatch = candidate.dispatch
      return unless dispatch

      outcomes = []
      outcomes << [dispatch.on_success, dispatch.phase == "review" ? "reviewer" : "agent"] if dispatch.on_success
      outcomes << [dispatch.on_failure, "system"] if dispatch.on_failure
      outcomes.concat((dispatch.on_verdict || {}).values.map { |target| [target, "reviewer"] })
      outcomes.each do |target, actor|
        raise ContractError, "workflow #{name} transition #{candidate.name} dispatches to unknown transition #{target}" unless transition?(target)
        unless transition(target).from.include?(candidate.to)
          raise ContractError, "workflow #{name} transition #{candidate.name} dispatch outcome #{target} is not available from #{candidate.to}"
        end
        unless transition(target).allows?(actor)
          raise ContractError, "workflow #{name} dispatch outcome #{target} must allow #{actor}"
        end
      end
    end

    def validate_decision_choices!(candidate)
      decision = candidate.decision
      return unless decision

      unless state(candidate.to).awaits_decision?
        raise ContractError, "workflow #{name} transition #{candidate.name} carries a decision but #{candidate.to} does not await one"
      end
      decision.fetch("choices").each do |choice|
        raise ContractError, "workflow #{name} transition #{candidate.name} offers unknown choice #{choice}" unless transition?(choice)
        unless transition(choice).from.include?(candidate.to)
          raise ContractError, "workflow #{name} transition #{candidate.name} offers choice #{choice}, which is not available from #{candidate.to}"
        end
        unless transition(choice).allows?("human")
          raise ContractError, "workflow #{name} transition #{candidate.name} offers choice #{choice}, which no human may take"
        end
        unless transition(choice).actors == ["human"]
          raise ContractError, "workflow #{name} transition #{candidate.name} offers choice #{choice}, which must be human-only"
        end
      end
    end

    def validate_verdict_requirements!
      transitions.each_value do |candidate|
        next unless candidate.requires.include?("review_verdict")

        verdicts = transitions.values.flat_map do |origin|
          next [] unless origin.dispatch&.phase == "review"

          origin.dispatch.on_verdict.select { |_verdict, target| target == candidate.name }.keys
        end.uniq
        unless verdicts.length == 1
          raise ContractError, "workflow #{name} transition #{candidate.name} requires review_verdict and must map to exactly one verdict"
        end
      end
    end

    def validate_ambiguity!
      states.each_key do |state_name|
        outgoing = transitions_from(state_name)
        duplicates = outgoing.group_by { |candidate| [candidate.to, candidate.actors.sort, candidate.requires.sort] }
                             .select { |_key, group| group.length > 1 }
        next if duplicates.empty?

        names = duplicates.values.flatten.map(&:name).sort.join(", ")
        raise ContractError, "workflow #{name} state #{state_name} has interchangeable transitions #{names}"
      end
    end

    def validate_decision_states!
      states.each_value do |candidate|
        next unless candidate.awaits_decision?
        if candidate.terminal? || candidate.continue
          raise ContractError, "workflow #{name} state #{candidate.name} awaits_decision cannot be terminal or continue automatically"
        end

        entries = transitions.values.select { |transition| transition.to == candidate.name }
        raise ContractError, "workflow #{name} state #{candidate.name} awaits a decision but nothing enters it" if entries.empty?
        missing = entries.reject(&:decision)
        unless missing.empty?
          raise ContractError, "workflow #{name} transitions #{missing.map(&:name).sort.join(", ")} enter #{candidate.name} without a decision question"
        end
        answers = transitions_from(candidate.name).select { |transition| transition.allows?("human") }
        raise ContractError, "workflow #{name} state #{candidate.name} awaits a decision no human can answer" if answers.empty?
      end
    end

    def validate_continue!
      states.each_value do |candidate|
        next unless candidate.continue

        raise ContractError, "workflow #{name} terminal state #{candidate.name} must not continue automatically" if candidate.terminal?
        raise ContractError, "workflow #{name} state #{candidate.name} continues to unknown transition #{candidate.continue}" unless transition?(candidate.continue)
        unless transition(candidate.continue).from.include?(candidate.name)
          raise ContractError, "workflow #{name} state #{candidate.name} continues to #{candidate.continue}, which is not available from it"
        end
        unless transition(candidate.continue).allows?("system")
          raise ContractError, "workflow #{name} state #{candidate.name} continues to #{candidate.continue}, which the system may not take"
        end
      end
    end

    # Removing revision-counted edges leaves only paths that can run without consuming
    # the finite revision allowance. Any remaining automatic cycle would run unbounded.
    def validate_automatic_cycles!
      edges = Hash.new { |hash, key| hash[key] = [] }
      states.each_value do |candidate|
        next unless candidate.continue

        target = transition(candidate.continue)
        edges[candidate.name] << target.to unless target.counts_revision?
      end
      transitions.each_value do |candidate|
        dispatch = candidate.dispatch
        next unless dispatch

        [dispatch.on_success, dispatch.on_failure, *(dispatch.on_verdict || {}).values].compact.each do |target_name|
          target = transition(target_name)
          edges[candidate.to] << target.to unless target.counts_revision?
        end
      end
      visiting = {}
      visited = {}
      visit = lambda do |state_name|
        if visiting[state_name]
          raise ContractError, "workflow #{name} has an unbounded automatic cycle at #{state_name}; count a revision on a cycle transition"
        end
        return if visited[state_name]

        visiting[state_name] = true
        edges[state_name].each { |target| visit.call(target) }
        visiting.delete(state_name)
        visited[state_name] = true
      end
      states.each_key { |state_name| visit.call(state_name) }
    end

    def validate_reachability!
      reachable = [initial_state]
      queue = [initial_state]
      until queue.empty?
        current = queue.shift
        transitions_from(current).each do |candidate|
          next if reachable.include?(candidate.to)

          reachable << candidate.to
          queue << candidate.to
        end
      end
      unreachable = states.keys - reachable
      raise ContractError, "workflow #{name} has unreachable states #{unreachable.sort.join(", ")}" unless unreachable.empty?

      stranded = states.values.reject(&:terminal?).map(&:name).reject { |state_name| reaches_terminal?(state_name) }
      raise ContractError, "workflow #{name} has states that cannot terminate: #{stranded.sort.join(", ")}" unless stranded.empty?
    end

    def reaches_terminal?(origin)
      seen = [origin]
      queue = [origin]
      until queue.empty?
        current = queue.shift
        return true if state(current).terminal?

        transitions_from(current).each do |candidate|
          next if seen.include?(candidate.to)

          seen << candidate.to
          queue << candidate.to
        end
      end
      false
    end

    def freeze_deeply
      @digest = Digest::SHA256.hexdigest(JSON.generate(to_h))
      instance_variables.each { |variable| deep_freeze(instance_variable_get(variable)) }
      freeze
    end

    def deep_freeze(value)
      case value
      when Hash
        value.each { |key, child| deep_freeze(key); deep_freeze(child) }
      when Array, Struct
        value.each { |child| deep_freeze(child) }
      end
      value.freeze
    end
  end
end

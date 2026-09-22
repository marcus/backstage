# frozen_string_literal: true

require_relative "test_helper"

class WorkflowDefinitionTest < Minitest::Test
  MINIMAL = {
    "name" => "tiny",
    "initial_state" => "new",
    "states" => { "new" => {}, "in_progress" => {}, "done" => { "terminal" => true } },
    "transitions" => {
      "start" => { "from" => ["new"], "to" => "in_progress", "actors" => ["system"] },
      "finish" => { "from" => ["in_progress"], "to" => "done", "actors" => ["system"] }
    }
  }.freeze

  def compile(overrides = {})
    Backstage::Domain::Workflow.compile(deep_merge(MINIMAL, overrides))
  end

  def refuses(message, overrides)
    error = assert_raises(Backstage::ContractError) { compile(overrides) }
    assert_match(message, error.message)
  end

  def test_a_three_state_workflow_needs_no_optional_machinery
    tiny = compile

    assert_equal "new", tiny.initial_state
    assert_equal %w[start], tiny.transitions_from("new").map(&:name)
    assert tiny.terminal?("done")
    assert_equal [], tiny.transition("finish").requires
    refute tiny.state("in_progress").awaits_decision?
    assert_equal 0, tiny.max_revisions
  end

  def test_shipped_pack_workflows_compile_and_differ
    names = pack.workflows.keys.sort
    assert_equal %w[human-gated-change independent-review minimal], names

    review = workflow("independent-review")
    gated = workflow("human-gated-change")
    assert_includes review.transitions.values.flat_map(&:actors), "reviewer"
    refute_includes gated.transitions.values.flat_map(&:actors), "reviewer",
                    "the human-gated lifecycle must not depend on a reviewer at all"
    assert gated.state("awaiting_approval").awaits_decision?
    assert_equal "revise", review.state("changes_requested").continue
    refute_equal review.digest, gated.digest
  end

  def test_definitions_are_frozen_records
    tiny = compile
    assert_predicate tiny, :frozen?
    assert_raises(FrozenError) { tiny.transitions_from("new").first.actors << "human" }
  end

  def test_cycles_are_valid_because_returned_work_continues
    cyclic = compile(
      "states" => { "in_progress" => { "continue" => "retry" } },
      "transitions" => { "retry" => { "from" => ["in_progress"], "to" => "new", "actors" => ["system"] } }
    )
    assert_equal "retry", cyclic.state("in_progress").continue
  end

  def test_it_names_the_missing_reference
    refuses(/references unknown state nowhere/, "transitions" => { "finish" => { "from" => ["in_progress"], "to" => "nowhere", "actors" => ["system"] } })
    refuses(/unknown actors robot/, "transitions" => { "finish" => { "from" => ["in_progress"], "to" => "done", "actors" => ["robot"] } })
    refuses(/requires unknown evidence vibes/, "transitions" => { "finish" => { "from" => ["in_progress"], "to" => "done", "actors" => ["system"], "requires" => ["vibes"] } })
    refuses(/has unknown keys onto/, "transitions" => { "finish" => { "from" => ["in_progress"], "to" => "done", "actors" => ["system"], "onto" => "done" } })
    refuses(/initial_state missing is not a defined state/, "initial_state" => "missing")
  end

  def test_it_rejects_unreachable_and_unterminated_definitions
    refuses(/unreachable states orphan/,
            "states" => { "orphan" => {} })
    refuses(/cannot terminate: stuck/,
            "states" => { "stuck" => {} },
            "transitions" => { "wander" => { "from" => ["new"], "to" => "stuck", "actors" => ["system"] } })
    refuses(/must define at least one terminal state/, "states" => { "done" => { "terminal" => false } })
  end

  def test_it_rejects_interchangeable_transitions
    refuses(/interchangeable transitions finish, wrap/,
            "transitions" => { "wrap" => { "from" => ["in_progress"], "to" => "done", "actors" => ["system"] } })
  end

  def test_dispatch_outcomes_must_be_available_where_they_land
    refuses(/dispatches to unknown transition nope/,
            "transitions" => { "start" => { "from" => ["new"], "to" => "in_progress", "actors" => ["system"], "dispatch" => { "phase" => "implementation", "on_success" => "nope" } } })
    refuses(/dispatch outcome start is not available from in_progress/,
            "transitions" => { "start" => { "from" => ["new"], "to" => "in_progress", "actors" => ["system"], "dispatch" => { "phase" => "implementation", "on_success" => "start" } } })
    refuses(/dispatches a review and must map on_verdict/,
            "transitions" => { "start" => { "from" => ["new"], "to" => "in_progress", "actors" => ["system"], "dispatch" => { "phase" => "review" } } })
  end

  def test_a_decision_state_must_be_answerable_by_a_human
    refuses(/enter waiting without a decision question/,
            "states" => { "waiting" => { "awaits_decision" => true } },
            "transitions" => {
              "hold" => { "from" => ["in_progress"], "to" => "waiting", "actors" => ["system"] },
              "release" => { "from" => ["waiting"], "to" => "done", "actors" => ["human"] }
            })
    refuses(/offers choice release, which no human may take/,
            "states" => { "waiting" => { "awaits_decision" => true } },
            "transitions" => {
              "hold" => { "from" => ["in_progress"], "to" => "waiting", "actors" => ["system"], "decision" => { "question" => "now what?", "choices" => ["release"] } },
              "release" => { "from" => ["waiting"], "to" => "done", "actors" => ["system"] }
            })
  end

  def test_only_an_explicit_human_may_reopen_a_terminal_state
    refuses(/reopens terminal state done, which only a human may do/,
            "transitions" => { "reopen" => { "from" => ["done"], "to" => "new", "actors" => ["system"] } })
    refuses(/must not dispatch work itself/,
            "transitions" => { "reopen" => { "from" => ["done"], "to" => "new", "actors" => ["human"], "dispatch" => { "phase" => "implementation", "on_success" => "start" } } })

    reopenable = compile("transitions" => { "reopen" => { "from" => ["done"], "to" => "new", "actors" => ["human"] } })
    assert_equal %w[reopen], reopenable.transitions_from("done").map(&:name)
  end

  private

  def deep_merge(base, override)
    base.merge(override) do |_key, old, new|
      old.is_a?(Hash) && new.is_a?(Hash) ? deep_merge(old, new) : new
    end
  end
end

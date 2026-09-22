# frozen_string_literal: true

require_relative "test_helper"

class WorkflowValidationRegressionTest < Minitest::Test
  def definition
    {
      "name" => "regression", "initial_state" => "ready", "max_revisions" => 2,
      "states" => { "ready" => {}, "running" => {}, "done" => { "terminal" => true } },
      "transitions" => {
        "start" => { "from" => ["ready"], "to" => "running", "actors" => ["system"],
                     "dispatch" => { "phase" => "review", "on_verdict" => { "approved" => "finish" } } },
        "finish" => { "from" => ["running"], "to" => "done", "actors" => ["reviewer"], "requires" => ["review_verdict"] }
      }
    }
  end

  def compile(body)
    Backstage::Domain::Workflow.compile(body)
  end

  def refuses(body, pattern)
    error = assert_raises(Backstage::ContractError) { compile(body) }
    assert_match pattern, error.message
  end

  def decision_definition
    body = definition
    body["states"]["waiting"] = { "awaits_decision" => true }
    body["transitions"]["ask"] = {
      "from" => ["running"], "to" => "waiting", "actors" => ["agent"],
      "decision" => { "question" => "Continue?", "choices" => ["answer"] }
    }
    body["transitions"]["answer"] = { "from" => ["waiting"], "to" => "done", "actors" => ["human"] }
    body
  end

  def test_dispatch_outcome_must_accept_its_actual_actor
    definition = workflow("minimal").to_h
    definition["transitions"]["finish"]["actors"] = ["human"]
    error = assert_raises(Backstage::ContractError) { Backstage::Domain::Workflow.compile(definition) }
    assert_match(/finish must allow agent/, error.message)
  end

  def test_decision_choices_cannot_also_authorize_agents_or_systems
    %w[agent system reviewer].each do |actor|
      body = decision_definition
      body["transitions"]["answer"]["actors"] << actor
      refuses(body, /human-only/)
    end
    assert compile(decision_definition)
  end

  def test_reopening_cannot_also_authorize_nonhumans
    body = definition
    body["transitions"]["reopen"] = { "from" => ["done"], "to" => "ready", "actors" => ["human", "agent"] }
    refuses(body, /only a human may do/)
  end

  def test_review_evidence_requires_an_unambiguous_expected_verdict
    body = definition
    body["transitions"]["start"]["dispatch"] = { "phase" => "implementation", "on_success" => "finish" }
    body["transitions"]["finish"]["actors"] = ["agent"]
    refuses(body, /exactly one verdict/)

    body = definition
    body["transitions"]["start"]["dispatch"]["on_verdict"]["changes_requested"] = "finish"
    refuses(body, /exactly one verdict/)

    body = definition
    body["transitions"]["start_again"] = Marshal.load(Marshal.dump(body["transitions"]["start"]))
    body["transitions"]["start_again"]["actors"] = ["human"]
    assert compile(body), "multiple dispatch choices can map the same expected verdict"
  end

  def test_unbounded_continue_cycles_are_refused_but_revision_bounded_cycles_work
    body = definition
    body["states"]["ready"]["continue"] = "start"
    body["states"]["running"]["continue"] = "retry"
    body["transitions"]["retry"] = { "from" => ["running"], "to" => "ready", "actors" => ["system"] }
    refuses(body, /unbounded automatic cycle/)
    body["transitions"]["retry"]["counts_revision"] = true
    assert compile(body)
  end

  def test_dispatch_outcomes_cannot_hide_an_unbounded_automatic_cycle
    body = definition
    body["transitions"]["start"]["dispatch"] = { "phase" => "implementation", "on_success" => "retry" }
    body["transitions"]["finish"].delete("requires")
    body["transitions"]["retry"] = {
      "from" => ["running"], "to" => "running", "actors" => ["system", "agent"],
      "dispatch" => { "phase" => "implementation", "on_success" => "retry" }
    }
    refuses(body, /unbounded automatic cycle/)
    body["transitions"]["retry"]["counts_revision"] = true
    assert compile(body)
  end

  def test_waiting_for_a_decision_cannot_also_terminate_or_continue
    [{ "terminal" => true }, { "continue" => "answer" }].each do |extra|
      body = decision_definition
      body["states"]["waiting"].merge!(extra)
      refuses(body, /awaits_decision cannot be terminal or continue/)
    end
  end

  def test_unknown_fields_and_nonboolean_flags_are_rejected
    body = definition.merge("schedule" => "daily")
    refuses(body, /unknown keys schedule/)
    body = decision_definition
    body["transitions"]["ask"]["decision"]["timeout"] = 2
    refuses(body, /unknown keys timeout/)
    %w[terminal awaits_decision].each do |key|
      body = definition
      body["states"]["running"][key] = "false"
      refuses(body, /#{key} must be a boolean/)
    end
    body = definition
    body["transitions"]["start"]["dispatch"]["on_verdict"] = false
    refuses(body, /on_verdict must be an object/)
    body = definition
    body["states"]["running"] = false
    refuses(body, /state running must be an object/)
    body = definition
    body["transitions"]["start"]["counts_revision"] = "true"
    refuses(body, /counts_revision must be a boolean/)
  end

  def test_compiled_graph_is_deeply_immutable_but_export_is_editable
    workflow = compile(decision_definition)
    original_digest = workflow.digest.dup
    assert_raises(FrozenError) { workflow.source["states"]["ready"]["terminal"] = true }
    assert_raises(FrozenError) { workflow.transition("start").dispatch.on_verdict["approved"].replace("ask") }
    assert_raises(FrozenError) { workflow.transition("start").dispatch.phase = "implementation" }
    assert_raises(FrozenError) { workflow.transition("ask").decision["choices"] << "finish" }
    assert_raises(FrozenError) { workflow.transition("answer").actors.first.replace("agent") }
    assert_raises(FrozenError) { workflow.name.replace("other") }
    assert_raises(FrozenError) { workflow.digest.replace("other") }
    exported = workflow.to_h
    exported["transitions"]["ask"]["decision"]["choices"] << "finish"
    exported["transitions"]["start"]["dispatch"]["on_verdict"]["approved"].replace("ask")
    assert_equal ["answer"], workflow.transition("ask").decision["choices"]
    assert_equal "finish", workflow.transition("start").dispatch.on_verdict["approved"]
    assert_equal original_digest, workflow.digest
  end
end

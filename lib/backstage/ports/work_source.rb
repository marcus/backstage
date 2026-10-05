# frozen_string_literal: true

module Backstage::Ports
  # Native task content and operations. The core interprets neither document fields nor source
  # statuses. Configuration owns routing, allowed operations, endpoints and credentials.
  class WorkSource
    # String-keyed ref/title/content/media_type, and optional native version. Content is the
    # complete agent-readable UTF-8 document; ref/version are opaque to the application.
    def snapshot(_ref) = raise(NotImplementedError)

    # Native refs selected by provider rules. Discovery never grants execution authority and must
    # return candidates again on subsequent calls so failed admission can be retried.
    def discover = raise(NotImplementedError)

    # Implemented operation names; deployment configuration separately permits their use.
    def capabilities = raise(NotImplementedError)

    # Produce a JSON-compatible immutable native payload before effects. The operation id is
    # supplied by the host and can be embedded as a provider reconciliation marker.
    def prepare(operation:, ref:, result:, operation_id:) = raise(NotImplementedError)

    # {"status"=>"applied"|"not_applied"|"unknown", "receipt"=>bounded JSON object}.
    # An attempted operation needs trustworthy evidence of absence before being repeated.
    def reconcile(operation:, ref:, payload:, operation_id:, attempted:) = raise(NotImplementedError)

    # {"status"=>"applied"|"unknown", "receipt"=>bounded JSON object}. Credentials and native
    # transport stay here on the host; task prose cannot supply them.
    def execute(operation:, ref:, payload:, operation_id:) = raise(NotImplementedError)
  end
end

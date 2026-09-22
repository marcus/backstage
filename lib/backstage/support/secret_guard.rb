# frozen_string_literal: true

module Backstage
  module Support
    class SecretGuard
      ContractError = Backstage::ContractError
      SECRET_KEY = /(?:^|[_-])(?:token|secret|password|credential)(?:$|[_-])|api[_-]?key|authorization/i
      REDACTED = "[REDACTED]"

      def initialize(secret_values: [])
        @secret_values = secret_values.compact.reject(&:empty?)
      end

      def check!(value, path = "$", allow_secret_keys: false)
        case value
        when Hash
          value.each do |key, nested|
            if !allow_secret_keys && secret_key?(key) && !nested.nil?
              raise ContractError, "secret-like field is forbidden at #{path}.#{key}"
            end
            check!(nested, "#{path}.#{key}", allow_secret_keys: allow_secret_keys)
          end
        when Array
          value.each_with_index { |nested, index| check!(nested, "#{path}[#{index}]", allow_secret_keys: allow_secret_keys) }
        when String
          @secret_values.each do |secret|
            raise ContractError, "secret value is forbidden at #{path}" if contains?(value, secret)
          end
        end
        value
      end

      def redact(text)
        @secret_values.reduce(text.to_s) do |memo, secret|
          comparable?(memo, secret) ? memo.gsub(secret, REDACTED) : memo.b.gsub(secret.b, REDACTED)
        end
      end

      # Bytes of the longest configured secret. Zero when nothing is configured.
      def longest_secret
        @secret_values.map(&:bytesize).max.to_i
      end

      # A redactor for a byte stream that arrives in arbitrary chunks. See Redactor.
      def redactor
        Redactor.new(@secret_values)
      end

      # `redact` can only see the string it is handed, so a secret split across two reads of a
      # pipe would pass through it untouched. This withholds the last `longest_secret - 1` bytes
      # of everything it has been given: any occurrence overlapping a byte it does release is,
      # by that arithmetic, entirely visible and therefore replaceable. `finish` releases the
      # withheld tail at end of stream.
      #
      # It works in binary and returns binary. Callers frame and transcode downstream — running
      # redaction first is what makes a byte offset into the released stream address the same
      # bytes that get persisted.
      class Redactor
        def initialize(secret_values)
          @secrets = secret_values.map { |value| binary(value) }.reject(&:empty?)
          longest = @secrets.map(&:bytesize).max.to_i
          @hold = longest > 1 ? longest - 1 : 0
          @buffer = binary(+"")
        end

        # Returns the safe bytes released by these bytes, which may be empty.
        def push(bytes)
          @buffer << binary(bytes)
          release(@buffer.bytesize - @hold)
        end

        # Releases everything still held. Safe because no further bytes can extend a secret.
        def finish
          release(@buffer.bytesize)
        end

        # Bytes currently withheld, for bounds assertions.
        def held_bytes
          @buffer.bytesize
        end

        private

        # Emits the redacted form of the buffer up to `limit`, a raw-byte boundary no undetectable
        # occurrence can straddle. A replacement that starts before `limit` and ends after it
        # consumes those bytes too — they are already accounted for and must not be released twice.
        def release(limit)
          output = binary(+"")
          return output if limit <= 0

          position = 0
          while position < limit
            found = next_occurrence(position, limit)
            break unless found

            index, length = found
            output << @buffer.byteslice(position, index - position)
            output << REDACTED
            position = index + length
          end
          cut = [position, limit].max
          output << (@buffer.byteslice(position, cut - position) || binary(+"")) if cut > position
          @buffer = @buffer.byteslice(cut, @buffer.bytesize - cut) || binary(+"")
          output
        end

        # The earliest secret occurrence starting at or after `position` and before `limit`,
        # longest first so an overlapping pair does not leave a fragment behind.
        def next_occurrence(position, limit)
          best = nil
          @secrets.each do |secret|
            index = @buffer.index(secret, position)
            next if index.nil? || index >= limit
            next if best && (index > best[0] || (index == best[0] && secret.bytesize <= best[1]))

            best = [index, secret.bytesize]
          end
          best
        end

        def binary(value)
          value.to_s.dup.force_encoding(Encoding::BINARY)
        end
      end

      private

      # Whether a secret occurs in a value, as bytes.
      #
      # The strings this guard is handed are not all text: a chunk of a runtime's output is BINARY
      # and may hold invalid UTF-8, while a configured secret is ordinarily UTF-8 and may hold
      # non-ASCII bytes. Ruby refuses to compare those directly — `Encoding::CompatibilityError`
      # from `include?`, `ArgumentError` from a scan of invalid bytes — and a guard that raises
      # instead of answering is a guard that fails open at exactly the moment a secret is present.
      # Byte containment is the question in every case, so an incompatible or invalid pair is
      # settled in binary.
      def contains?(value, secret)
        comparable?(value, secret) ? value.include?(secret) : value.b.include?(secret.b)
      end

      def comparable?(value, secret)
        !Encoding.compatible?(value, secret).nil? && value.valid_encoding?
      end

      def secret_key?(key)
        name = key.to_s
        return false if name.end_with?("_ref", "_refs", "_reference", "_references")

        name.match?(SECRET_KEY)
      end
    end
  end
end

Backstage::SecretGuard = Backstage::Support::SecretGuard unless defined?(Backstage::SecretGuard)

# frozen_string_literal: true

require_relative "test_helper"

# Framing is where a byte stream becomes records, and it is the only place that decides what a
# record's byte range is. Everything downstream — chunk offsets, checkpoints, deduplication —
# inherits those numbers, so the properties proven here are load-bearing: records tile the stream
# without gaps, a character split across two reads survives, broken bytes are reported rather than
# dropped, and a process that never prints a newline cannot grow the buffer without bound.
class RecordFramerTest < Minitest::Test
  Framer = Backstage::Support::RecordFramer

  def texts(frames) = frames.map { |frame| frame.fetch("text") }

  def test_complete_lines_frame_with_contiguous_byte_ranges
    framer = Framer.new
    frames = framer.push("alpha\nbeta\n")

    assert_equal %w[alpha beta], texts(frames)
    assert_equal [0, 1], frames.map { |frame| frame.fetch("index") }
    assert_equal [0, 6], frames.map { |frame| frame.fetch("start_offset") }
    # end_offset is past the newline, so ranges tile the stream with no gap between records.
    assert_equal [6, 11], frames.map { |frame| frame.fetch("end_offset") }
    assert_equal %w[utf-8 utf-8], frames.map { |frame| frame.fetch("encoding") }
    refute frames.any? { |frame| frame.fetch("truncated") }
  end

  def test_a_multibyte_character_split_across_reads_is_reconstructed
    bytes = "héllo wörld\n".b
    split = bytes.index("\xC3".b) + 1 # between the two bytes of é
    framer = Framer.new

    assert_empty framer.push(bytes.byteslice(0, split))
    frames = framer.push(bytes.byteslice(split, bytes.bytesize - split))

    assert_equal ["héllo wörld"], texts(frames)
    assert_equal "utf-8", frames.fetch(0).fetch("encoding")
    assert_equal Encoding::UTF_8, frames.fetch(0).fetch("text").encoding
  end

  def test_a_line_split_at_every_byte_still_frames_once
    bytes = "first line\nsecond\n".b
    framer = Framer.new
    frames = bytes.each_byte.flat_map { |byte| framer.push(byte.chr) }

    assert_equal ["first line", "second"], texts(frames)
    assert_equal 0, framer.pending_bytes
  end

  def test_crlf_is_a_terminator_not_content
    frames = Framer.new.push("windows\r\nunix\n")

    assert_equal %w[windows unix], texts(frames)
    assert_equal [0, 9], frames.map { |frame| frame.fetch("start_offset") }
    assert_equal [9, 14], frames.map { |frame| frame.fetch("end_offset") }
  end

  def test_a_trailing_line_without_a_newline_arrives_on_finish
    framer = Framer.new

    assert_empty framer.push("no newline yet")
    assert_equal 14, framer.pending_bytes

    frames = framer.finish

    assert_equal ["no newline yet"], texts(frames)
    assert_equal 14, frames.fetch(0).fetch("end_offset")
    assert_empty framer.finish
  end

  def test_invalid_utf8_is_scrubbed_and_flagged_rather_than_dropped
    frames = Framer.new.push("good\xFF\xFEbytes\n".b)

    assert_equal 1, frames.length
    assert_equal "replaced", frames.fetch(0).fetch("encoding")
    assert_includes frames.fetch(0).fetch("text"), "�"
    assert_includes frames.fetch(0).fetch("text"), "bytes"
    assert frames.fetch(0).fetch("text").valid_encoding?
  end

  def test_an_oversized_line_is_truncated_and_the_rest_skipped_to_the_next_newline
    framer = Framer.new(max_record_bytes: 16)
    frames = framer.push("#{"x" * 100}\nafter\n")

    assert_equal 2, frames.length
    assert_equal "x" * 16, frames.fetch(0).fetch("text")
    assert frames.fetch(0).fetch("truncated")
    assert_equal "after", frames.fetch(1).fetch("text")
    refute frames.fetch(1).fetch("truncated")
    # The skipped bytes still count against the stream position: offsets address the stream, not
    # the subset of it that was interesting.
    assert_equal 101, frames.fetch(1).fetch("start_offset")
  end

  def test_a_process_that_never_prints_a_newline_cannot_grow_the_buffer
    framer = Framer.new(max_record_bytes: 1024)
    truncated = 0
    200.times { truncated += framer.push("y" * 4096).length }

    assert_equal 1, truncated
    assert framer.truncating?
    assert_operator framer.pending_bytes, :<=, 1024
    assert_equal 200 * 4096, framer.byte_offset + framer.pending_bytes
  end

  def test_state_round_trips_a_partial_multibyte_line
    bytes = "done\nhalf ✅ pending".b
    framer = Framer.new
    framer.push(bytes.byteslice(0, bytes.bytesize - 4))

    state = JSON.parse(JSON.generate(framer.state))

    assert_equal 1, state.fetch("record_index")
    assert_equal 5, state.fetch("byte_offset")
    refute state.fetch("truncating")

    resumed = Framer.restore(state)

    assert_equal framer.pending_bytes, resumed.pending_bytes
    frames = resumed.push("#{bytes.byteslice(bytes.bytesize - 4, 4)}\n")

    assert_equal ["half ✅ pending"], texts(frames)
    assert_equal 1, frames.fetch(0).fetch("index")
    assert_equal 5, frames.fetch(0).fetch("start_offset")
  end

  def test_state_round_trips_mid_truncation
    framer = Framer.new(max_record_bytes: 8)
    framer.push("z" * 40)

    resumed = Framer.restore(JSON.parse(JSON.generate(framer.state)), max_record_bytes: 8)

    assert resumed.truncating?
    frames = resumed.push("tail\nnext\n")

    assert_equal ["next"], texts(frames)
  end

  # `advance` is how a caller keeps a second framer over the bytes it has actually made durable.
  # If it disagreed with `push` about a single boundary, a checkpoint taken from it would renumber
  # history on resume, so what is proven here is that it cannot: same state, byte for byte, over a
  # fixture with irregular lines, CRLF, an empty line, an oversized record and broken bytes.
  def test_advance_consumes_exactly_what_push_consumes_and_builds_nothing
    fixture = "alpha\r\n\nbroken \xFF\xFE bytes\n#{"o" * 40}\nlast line without a newline".b
    (0..fixture.bytesize).each do |cut|
      framed = Framer.new(max_record_bytes: 16)
      advanced = Framer.new(max_record_bytes: 16)
      [fixture.byteslice(0, cut), fixture.byteslice(cut, fixture.bytesize - cut)].each do |part|
        framed.push(part)

        assert_empty advanced.advance(part).push("")
      end

      assert_equal framed.state, advanced.state
      assert_equal framed.record_index, advanced.record_index
      assert_equal framed.byte_offset, advanced.byte_offset
      assert_equal framed.truncating?, advanced.truncating?
    end
  end

  def test_a_record_index_and_offset_survive_an_empty_push
    framer = Framer.new
    framer.push("one\n")

    assert_empty framer.push("")
    assert_equal 1, framer.record_index
    assert_equal 4, framer.byte_offset
  end
end

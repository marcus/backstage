# frozen_string_literal: true

require "json"
require "fileutils"

module Backstage::Adapters::LocalFiles
  # Dispatcher ownership as an exclusive lock on a file beside the state log.
  #
  # The kernel drops the lock when the holding process dies, so a crashed dispatcher never leaves
  # the queue permanently owned. The file's contents are only a description of the holder for
  # operator inspection; the lock itself is what decides ownership.
  class DispatchOwnership < Backstage::Ports::DispatchOwnership
    Records = Backstage::Domain::Records
    PROBE_RETRIES = 5
    PROBE_BACKOFF_SECONDS = 0.01
    attr_reader :path

    def initialize(path)
      @path = File.expand_path(path)
      FileUtils.mkdir_p(File.dirname(@path))
      @handle = nil
    end

    def acquire(identity = {})
      return lease if held?

      handle = File.open(@path, File::RDWR | File::CREAT, 0o644)
      unless take(handle)
        handle.close
        return nil
      end

      @handle = handle
      @lease = {
        "owner_pid" => Process.pid,
        "owner_host" => Records.host_name,
        "acquired_at" => Records.timestamp,
        "lock_path" => @path
      }.merge(stringify(identity))
      write(@lease)
      @lease
    end

    def release
      return nil unless held?

      write(@lease.merge("released_at" => Records.timestamp))
      @handle.flock(File::LOCK_UN)
      @handle.close
      @handle = nil
      @lease = nil
    end

    def held?
      !@handle.nil? && !@handle.closed?
    end

    def lease
      @lease&.dup
    end

    # Probes the lock without taking or rewriting it, so inspection never disturbs the record of who
    # held it last.
    def free?
      return false if held?
      return true unless File.exist?(@path)

      # A shared probe: it detects an exclusive holder without ever excluding one, so inspecting
      # ownership cannot make a dispatcher that is starting up believe the store is taken.
      File.open(@path, File::RDONLY) do |handle|
        next false unless handle.flock(File::LOCK_SH | File::LOCK_NB)

        handle.flock(File::LOCK_UN)
        true
      end
    rescue SystemCallError, IOError
      false
    end

    # The last recorded holder. A record with no `released_at` from a process that is gone means the
    # dispatcher crashed; the lock is already free and the next `acquire` overwrites this.
    def current
      return nil unless File.exist?(@path)

      content = File.read(@path).strip
      return nil if content.empty?

      JSON.parse(content)
    rescue JSON::ParserError
      nil
    end

    private

    # A brief retry so that another process merely *looking* at ownership cannot make a dispatcher
    # that is starting up believe the store is taken. A real holder still refuses within milliseconds.
    def take(handle, attempts: PROBE_RETRIES, backoff: PROBE_BACKOFF_SECONDS)
      attempts.times do |attempt|
        return true if handle.flock(File::LOCK_EX | File::LOCK_NB)

        sleep(backoff) unless attempt == attempts - 1
      end
      false
    end

    def write(payload)
      @handle.truncate(0)
      @handle.seek(0)
      @handle.write(JSON.generate(payload))
      @handle.flush
      @handle.fsync
    end

    def stringify(identity)
      (identity || {}).to_h { |key, value| [key.to_s, value] }
    end
  end
end

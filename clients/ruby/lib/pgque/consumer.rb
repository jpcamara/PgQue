# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# PgQue includes code derived from PgQ (ISC license,
# Marko Kreen / Skype Technologies OU).

require "logger"

module Pgque
  class Consumer
    DEFAULT_MAX_MESSAGES = 2_147_483_647
    WAIT_SLICE_SECONDS = 0.5
    # A cooperative member that has held a batch this long without a
    # heartbeat is treated as dead, and another member takes the batch.
    DEFAULT_DEAD_INTERVAL = "60 seconds"

    NackFailed = Class.new(StandardError)
    private_constant :NackFailed

    attr_reader :dsn, :queue, :name, :poll_interval, :max_messages,
                :retry_after, :subconsumer, :dead_interval

    attr_accessor :logger

    def initialize(dsn, queue:, name:, poll_interval: 30,
                   max_messages: DEFAULT_MAX_MESSAGES, retry_after: 60,
                   unknown_handler_policy: "nack", subconsumer: nil,
                   dead_interval: nil, logger: nil)
      @dsn = dsn
      @queue = queue
      @name = name
      @poll_interval = poll_interval
      @max_messages = max_messages
      @retry_after = retry_after

      unless ["nack", "ack"].include?(unknown_handler_policy.to_s)
        raise ArgumentError,
              "unknown_handler_policy must be 'nack' or 'ack', " \
              "got #{unknown_handler_policy.inspect}"
      end
      @unknown_handler_policy = unknown_handler_policy.to_s

      if dead_interval && subconsumer.nil?
        raise ArgumentError,
              "dead_interval is only valid in cooperative mode " \
              "(set subconsumer:)"
      end
      @subconsumer = subconsumer
      @dead_interval = dead_interval || (subconsumer && DEFAULT_DEAD_INTERVAL)

      @handlers = {}
      @default_handler = nil
      # @running is a plain boolean. Ruby integer/boolean assignment
      # is atomic, and the only cross-thread interactions are the
      # signal trap and Consumer#stop flipping it false while the
      # main loop polls running? -- no ordering dependencies, so a
      # mutex would be overkill (and unsafe to enter from a signal
      # trap, which raises ThreadError on Mutex#synchronize).
      @running = false
      @stop_signum = nil
      @logger = logger || default_logger
    end

    def on(event_type, &block)
      raise ArgumentError, "block required for Consumer#on" unless block

      if event_type == "*"
        @default_handler = block
      else
        @handlers[event_type] = block
      end
      block
    end

    def start
      @running = true
      @stop_signum = nil

      in_main_thread = (Thread.current == Thread.main)
      original_handlers = {}

      # Signal traps run in a restricted context: Mutex#synchronize,
      # Logger#info, and most blocking code raise ThreadError. Keep
      # this proc to plain instance-variable writes; the main loop
      # logs the signal number after waking up.
      stop_proc = ->(signum) {
        @stop_signum = signum
        @running = false
      }

      if in_main_thread
        ["TERM", "INT"].each do |sig|
          original_handlers[sig] = Signal.trap(sig) { stop_proc.call(sig) }
        end
      end

      begin
        conn = PG.connect(@dsn)
        begin
          channel = "pgque_#{@queue}"
          conn.exec("LISTEN #{conn.escape_identifier(channel)}")
          @logger.info(
            "consumer #{@name} listening on #{@queue} (poll=#{@poll_interval}s)"
          )

          while running?
            handled = poll_once(conn)
            break unless running?
            # More batches may be ready. Their NOTIFYs fired while the
            # handlers ran, so receive again instead of waiting.
            wait_for_notify_or_stop(conn) unless handled
          end

          if @stop_signum
            @logger.info("received signal #{@stop_signum}, shutting down")
          end
        ensure
          conn.close unless conn.finished?
        end
      ensure
        # Clear running? before logging so callers observing the flag
        # see "stopped" by the time the log line is written -- and so
        # an exception during PG.connect, LISTEN, or the poll loop
        # leaves the consumer in a consistent state instead of a
        # ghost "running" with no live worker. Plain instance-var
        # write -- not the trap-context-unsafe pattern.
        @running = false
        if in_main_thread
          original_handlers.each { |sig, h| Signal.trap(sig, h || "DEFAULT") }
        end
        @logger.info("consumer #{@name} stopped")
      end
    end

    def stop
      @running = false
    end

    def running?
      @running
    end

    # Public for testability; not part of the stable API.
    #
    # Returns true when it handled and acked a batch.
    def poll_once(conn)
      client = Client.new(conn)
      # receive runs in its own transaction. In cooperative mode
      # receive_coop locks the logical consumer's row, and committing
      # here releases it before the handlers run, so other subconsumers
      # can claim batches in the meantime. The claimed batch stays open
      # until ack.
      msgs =
        if @subconsumer
          client.receive_coop(
            @queue, @name, @subconsumer,
            max_messages: @max_messages,
            dead_interval: @dead_interval,
          )
        else
          client.receive(@queue, @name, @max_messages)
        end
      return false if msgs.empty?

      batch_id = msgs[0].batch_id
      @logger.debug("batch #{batch_id}: #{msgs.size} message(s)")

      failures = with_heartbeat(conn) { dispatch_batch(msgs) }

      # Nacks and the ack commit together. If a nack fails, the nacks
      # before it roll back and the batch stays open, so the whole batch
      # is delivered again.
      conn.transaction do
        failures.each do |msg, reason|
          raise NackFailed unless nack(client, batch_id, msg, reason)
        end
        rowcount = client.ack(batch_id)
        if rowcount == 0
          @logger.warn(
            "pgque: ack batch #{batch_id} returned 0 -- stale or " \
            "double ack (batch already finished or not found)",
          )
        end
      end
      true
    rescue NackFailed
      false
    end

    private

    # Runs the handlers and returns the messages to nack, each with the
    # nack reason (nil for a handler error).
    def dispatch_batch(msgs)
      failures = []
      msgs.each do |msg|
        handler = @handlers[msg.type] || @default_handler

        if handler.nil?
          if @unknown_handler_policy == "ack"
            @logger.warn(
              "no handler for event type=#{msg.type} ev_id=#{msg.msg_id}; " \
              "acking",
            )
            next
          end
          @logger.warn(
            "no handler for event type=#{msg.type} ev_id=#{msg.msg_id}; " \
            "nacking",
          )
          failures << [msg, "no handler for type=#{msg.type}"]
          next
        end

        begin
          handler.call(msg)
        rescue StandardError => e
          @logger.error(
            "handler failed for msg_id=#{msg.msg_id}: " \
            "#{e.class}: #{e.message}",
          )
          failures << [msg, nil]
        end
      end
      failures
    end

    # The claimed batch is committed before the handlers run, so a crashed
    # member's batch is recovered by dead_interval takeover. While the
    # handlers run, refresh this member's sub_active so a live member's
    # batch is not taken over. The handlers don't use conn, so the
    # heartbeat thread has it to itself until it is joined.
    def with_heartbeat(conn)
      return yield unless @subconsumer

      @heartbeat_seconds ||= conn.exec_params(
        "select extract(epoch from $1::interval) / 4", [@dead_interval]
      ).getvalue(0, 0).to_f
      lock = Mutex.new
      wake = ConditionVariable.new
      finished = false
      beat = Thread.new do
        lock.synchronize do
          until finished
            wake.wait(lock, @heartbeat_seconds)
            touch(conn) unless finished
          end
        end
      end
      yield
    ensure
      if beat
        lock.synchronize do
          finished = true
          wake.signal
        end
        beat.join
      end
    end

    def touch(conn)
      Client.new(conn).touch_subconsumer(@queue, @name, @subconsumer)
    rescue StandardError => e
      @logger.error("heartbeat failed: #{e.class}: #{e.message}")
    end

    def nack(client, batch_id, msg, reason)
      client.nack(batch_id, msg, retry_after: @retry_after, reason: reason)
      true
    rescue StandardError => e
      @logger.error(
        "nack failed for #{reason ? "unhandled " : ""}msg_id=#{msg.msg_id}: " \
        "#{e.class}: #{e.message}",
      )
      false
    end

    def wait_for_notify_or_stop(conn)
      drained = false
      while conn.notifies
        drained = true
      end
      return if drained

      deadline = monotonic + @poll_interval
      while running?
        remaining = deadline - monotonic
        return if remaining <= 0

        slice = [WAIT_SLICE_SECONDS, remaining].min
        notification = conn.wait_for_notify(slice)
        return unless running?

        if notification
          while conn.notifies
            # drain any queued notifications
          end
          return
        end
      end
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # The default logger is effectively silent: it targets $stderr (so
    # messages never collide with application stdout) and ships at level
    # FATAL, which the consumer never emits. Set PGQUE_LOG_LEVEL=warn (or
    # info, debug, error) to see warnings/info from the consumer, or
    # pass logger: Logger.new(...) to Consumer.new for full control.
    def default_logger
      log = Logger.new($stderr)
      log.progname = "pgque.consumer.#{@name}"
      log.level = env_log_level || Logger::FATAL
      log
    end

    def env_log_level
      raw = ENV["PGQUE_LOG_LEVEL"]
      return nil if raw.nil?

      normalized = raw.strip.upcase
      return nil if normalized.empty?

      Logger.const_get(normalized)
    rescue NameError
      nil
    end
  end
end

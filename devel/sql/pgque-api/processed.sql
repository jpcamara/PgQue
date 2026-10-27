-- pgque-api/processed.sql -- Has a consumer processed a transaction's events?
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/*
 * A consumer's cursor only moves past a tick once every event in the batch
 * ending at that tick has been acked or nacked, and each tick stores the
 * snapshot of transactions it covers. So the snapshot at the consumer's
 * last finished tick tells whether a transaction's events have all been
 * processed: they have once the transaction is visible in it.
 *
 * This lets a producer wait for its own sends ("read your writes"), and
 * lets an application know when a group of jobs has been processed
 * without writing anything per event.
 *
 * A nacked event counts as processed. Its retry is a new event, inserted
 * by maint_retry_events() in that function's transaction.
 */

create or replace function pgque.processed_snapshot(
    i_queue_name text, i_consumer_name text)
returns pg_snapshot as $$
-- ----------------------------------------------------------------------
-- Function: pgque.processed_snapshot(2)
--
--      Snapshot of the transactions whose events on the queue the
--      consumer has processed (acked or nacked).
--
--      For a normal consumer that is the snapshot at its last finished
--      tick. For a cooperative consumer it is the oldest of the main
--      cursor and the batches its subconsumers hold. For a partitioned
--      consumer it is the oldest across its slots, and NULL until all of
--      its slots are subscribed.
--
-- Parameters:
--      i_queue_name    - Name of the queue
--      i_consumer_name - Name of the consumer (the main name for a
--                        cooperative or partitioned consumer)
--
-- Returns:
--      The snapshot, or NULL if it isn't known yet.
-- ----------------------------------------------------------------------
declare
    v_queue_id int4;
    v_n int4;
    v_slots int4;
    v_tick_id int8;
    v_snapshot pg_snapshot;
begin
    select queue_id into v_queue_id
    from pgque.queue where queue_name = i_queue_name;
    if not found then
        raise exception 'queue not found: %', i_queue_name;
    end if;

    select n into v_n
    from pgque.partition_consumer
    where queue_id = v_queue_id and co_name = i_consumer_name;

    if found then
        select min(s.sub_last_tick), count(*) into v_tick_id, v_slots
        from generate_series(0, v_n - 1) as k
        join pgque.consumer as c
          on c.co_name = pgque._slot_name(i_consumer_name, k, v_n)
        join pgque.subscription as s
          on s.sub_consumer = c.co_id and s.sub_queue = v_queue_id;
        if v_slots < v_n then
            return null;
        end if;
    else
        -- A cooperative member holding a batch keeps the cursor of the
        -- tick before it; members share the main row's sub_id.
        select min(s.sub_last_tick) into v_tick_id
        from pgque.subscription as main
        join pgque.consumer as c on c.co_id = main.sub_consumer
        join pgque.subscription as s
          on s.sub_queue = main.sub_queue
         and s.sub_id = main.sub_id
         and (s.sub_consumer = main.sub_consumer
              or (s.sub_role = 'coop_member' and s.sub_batch is not null))
        where main.sub_queue = v_queue_id
          and c.co_name = i_consumer_name
          and main.sub_role in ('normal', 'coop_main');
        if v_tick_id is null then
            raise exception 'consumer % is not subscribed to queue %',
                i_consumer_name, i_queue_name;
        end if;
    end if;

    select tick_snapshot into v_snapshot
    from pgque.tick
    where tick_queue = v_queue_id and tick_id = v_tick_id;
    return v_snapshot;
end;
$$ language plpgsql stable security definer set search_path = pgque, pg_catalog;

revoke execute on function pgque.processed_snapshot(text, text) from public;

create or replace function pgque.is_processed(
    i_queue_name text, i_consumer_name text, i_txid xid8)
returns boolean as $$
-- ----------------------------------------------------------------------
-- Function: pgque.is_processed(3)
--
--      Whether the consumer has processed (acked or nacked) every event
--      the transaction sent to the queue. Capture the id in the sending
--      transaction with pg_current_xact_id().
--
-- Parameters:
--      i_queue_name    - Name of the queue
--      i_consumer_name - Name of the consumer
--      i_txid          - Id of the sending transaction
--
-- Returns:
--      true once processed; false while not, or not yet known.
-- ----------------------------------------------------------------------
select coalesce(
    pg_visible_in_snapshot(i_txid, pgque.processed_snapshot(i_queue_name, i_consumer_name)),
    false);
$$ language sql stable security definer set search_path = pgque, pg_catalog;

revoke execute on function pgque.is_processed(text, text, xid8) from public;

/* Producers ask whether their sends were processed, and consumers ask
   too, so both roles get them. */
grant execute on function pgque.processed_snapshot(text, text) to pgque_reader, pgque_writer;
grant execute on function pgque.is_processed(text, text, xid8) to pgque_reader, pgque_writer;

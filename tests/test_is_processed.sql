\set ON_ERROR_STOP on

-- Test: pgque.is_processed() and pgque.processed_snapshot()
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- A transaction's events are processed by a consumer once the consumer
-- has finished (acked or nacked) the batch holding them.

create temp table proc_tx (name text primary key, txid xid8 not null);

-- Plain consumer
do $$
begin
  perform pgque.create_queue('proc_q');
  perform pgque.register_consumer('proc_q', 'c1');
end $$;

begin;
select pgque.send('proc_q', 'a', '{}'::jsonb);
insert into proc_tx values ('a', pg_current_xact_id());
commit;

do $$
declare
  v_txid xid8 := (select txid from proc_tx where name = 'a');
  v_batch bigint;
begin
  assert not pgque.is_processed('proc_q', 'c1', v_txid),
    'not processed before a tick';

  perform pgque.force_next_tick('proc_q');
  perform pgque.ticker();
  assert not pgque.is_processed('proc_q', 'c1', v_txid),
    'not processed before the batch is taken';

  v_batch := pgque.next_batch('proc_q', 'c1');
  assert v_batch is not null, 'expected a batch';
  assert not pgque.is_processed('proc_q', 'c1', v_txid),
    'not processed while the batch is open';

  perform pgque.finish_batch(v_batch);
  assert pgque.is_processed('proc_q', 'c1', v_txid),
    'processed once the batch is finished';
  assert pg_visible_in_snapshot(v_txid, pgque.processed_snapshot('proc_q', 'c1')),
    'processed_snapshot sees the transaction';
  raise notice 'PASS: is_processed() follows a plain consumer''s batches';
end $$;

-- A nacked event counts as processed.
begin;
select pgque.send('proc_q', 'b', '{}'::jsonb);
insert into proc_tx values ('b', pg_current_xact_id());
commit;

do $$
declare
  v_txid xid8 := (select txid from proc_tx where name = 'b');
  v_msg pgque.message;
begin
  perform pgque.force_next_tick('proc_q');
  perform pgque.ticker();
  select * into v_msg from pgque.receive('proc_q', 'c1', 100);
  perform pgque.nack(v_msg.batch_id, v_msg, interval '1 hour', 'test');
  perform pgque.ack(v_msg.batch_id);
  assert pgque.is_processed('proc_q', 'c1', v_txid),
    'a nacked event counts as processed';
  raise notice 'PASS: is_processed() counts a nacked event as processed';
end $$;

-- A transaction still running is not processed.
do $$
begin
  assert not pgque.is_processed('proc_q', 'c1', pg_current_xact_id()),
    'a running transaction is not processed';
  raise notice 'PASS: is_processed() is false for a running transaction';
end $$;

do $$
begin
  begin
    perform pgque.is_processed('proc_q', 'nobody', pg_current_xact_id());
    assert false, 'an unknown consumer must raise';
  exception when raise_exception then
    assert sqlerrm like 'consumer nobody is not subscribed%', sqlerrm;
  end;
  begin
    perform pgque.is_processed('no_such_queue', 'c1', pg_current_xact_id());
    assert false, 'an unknown queue must raise';
  exception when raise_exception then
    assert sqlerrm like 'queue not found%', sqlerrm;
  end;
  raise notice 'PASS: is_processed() raises for an unknown queue or consumer';
end $$;

do $$
begin
  perform pgque.unregister_consumer('proc_q', 'c1');
  perform pgque.drop_queue('proc_q', true);
end $$;

-- Cooperative consumer: a later batch acked by one member is not processed
-- while another member holds an earlier one.
do $$
begin
  perform pgque.create_queue('proc_coop');
  perform pgque.register_subconsumer('proc_coop', 'main_c', 'w1');
  perform pgque.register_subconsumer('proc_coop', 'main_c', 'w2');
end $$;

begin;
select pgque.send('proc_coop', 'first', '{}'::jsonb);
insert into proc_tx values ('coop1', pg_current_xact_id());
commit;
do $$ begin perform pgque.force_next_tick('proc_coop'); perform pgque.ticker(); end $$;

begin;
select pgque.send('proc_coop', 'second', '{}'::jsonb);
insert into proc_tx values ('coop2', pg_current_xact_id());
commit;
do $$ begin perform pgque.force_next_tick('proc_coop'); perform pgque.ticker(); end $$;

do $$
declare
  v_first xid8 := (select txid from proc_tx where name = 'coop1');
  v_second xid8 := (select txid from proc_tx where name = 'coop2');
  v_b1 bigint;
  v_b2 bigint;
begin
  v_b1 := pgque.next_batch('proc_coop', 'main_c', 'w1', null);
  v_b2 := pgque.next_batch('proc_coop', 'main_c', 'w2', null);
  assert v_b1 is not null and v_b2 is not null and v_b1 <> v_b2,
    'each member should hold its own batch';

  perform pgque.finish_batch(v_b2);
  assert not pgque.is_processed('proc_coop', 'main_c', v_second),
    'a later batch is not processed while an earlier one is held';
  assert not pgque.is_processed('proc_coop', 'main_c', v_first),
    'the held batch is not processed';

  perform pgque.finish_batch(v_b1);
  assert pgque.is_processed('proc_coop', 'main_c', v_first),
    'first batch processed once finished';
  assert pgque.is_processed('proc_coop', 'main_c', v_second),
    'second batch processed once the earlier one is finished';
  raise notice 'PASS: is_processed() waits for the oldest batch a member holds';
end $$;

do $$
begin
  perform pgque.unregister_subconsumer('proc_coop', 'main_c', 'w1', 1);
  perform pgque.unregister_subconsumer('proc_coop', 'main_c', 'w2', 1);
  perform pgque.drop_queue('proc_coop', true);
end $$;

-- Partitioned consumer: unknown until every slot is subscribed, then
-- processed once every slot has finished the batch.
do $$
begin
  perform pgque.create_queue('proc_pk');
  perform pgque.subscribe_slot('proc_pk', 'w', 0, 2);
  assert pgque.processed_snapshot('proc_pk', 'w') is null,
    'unknown until every slot is subscribed';
  assert not pgque.is_processed('proc_pk', 'w', pg_current_xact_id()),
    'not processed until every slot is subscribed';
  perform pgque.subscribe_slot('proc_pk', 'w', 1, 2);
end $$;

begin;
select pgque.send('proc_pk', 't', '{}'::jsonb, 'key-1');
select pgque.send('proc_pk', 't', '{}'::jsonb, 'key-2');
insert into proc_tx values ('pk', pg_current_xact_id());
commit;
do $$ begin perform pgque.force_next_tick('proc_pk'); perform pgque.ticker(); end $$;

do $$
declare
  v_txid xid8 := (select txid from proc_tx where name = 'pk');
  v_slot int;
begin
  for v_slot in 0..1 loop
    assert not pgque.is_processed('proc_pk', 'w', v_txid),
      format('not processed before slot %s finishes', v_slot);
    perform pgque.claim_slot('proc_pk', 'w', v_slot, 'wk');
    perform * from pgque.receive_partitioned('proc_pk', 'w', v_slot, 2, 'wk', 100);
    perform pgque.ack_partitioned('proc_pk', 'w', v_slot, 2, 'wk');
    perform pgque.release_slot('proc_pk', 'w', v_slot, 'wk');
  end loop;
  assert pgque.is_processed('proc_pk', 'w', v_txid),
    'processed once every slot has finished';
  raise notice 'PASS: is_processed() waits for every partition slot';
end $$;

do $$
begin
  perform pgque.unsubscribe_slot('proc_pk', 'w', 0);
  perform pgque.unsubscribe_slot('proc_pk', 'w', 1);
  perform pgque.drop_queue('proc_pk', true);
end $$;

drop table proc_tx;
